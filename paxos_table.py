import sys
import time
import random
import sqlalchemy


def paxos(conns, quorum, key, version, value):
    seq = int(time.time())  # Paxos Seq

    # Promise Phase
    success = list()
    random.shuffle(conns)
    for conn in conns:
        try:
            # Insert a row, if does not exist already
            conn.execute('''insert into kvlog
                            (key,version,promised_seq,accepted_seq)
                            values(?,?,0,0)
                         ''', key, version)
        except Exception:
            pass

        try:
            trans = conn.begin()

            # Get the information from the old paxos round
            promised_seq, accepted_seq, accepted_value = list(
                conn.execute('''select promised_seq,accepted_seq,value
                                from kvlog
                                where key=? and version=?
                             ''', key, version))[0]

            # A more recent paxos round has been seen by this node.
            # It should not participate in this round.
            if promised_seq >= seq:
                trans.rollback()
                continue

            # Record seq to reject any old stray paxos rounds
            conn.execute('''update kvlog set promised_seq=?
                            where key=? and version=?
                         ''', seq, key, version)

            trans.commit()

            # This information is used by paxos to decide the proposal value
            success.append((accepted_seq, accepted_value))
        except Exception:
            pass

    if len(success) < quorum:
        return 'no-promise-quorum', len(success)

    # This is the most subtle PAXOS step
    #
    # Find the most recent value accepted by nodes in the ACCEPT phase
    # of previous incomplete PAXOS rounds
    proposal = (0, value)
    for accepted_seq, value in success:
        if accepted_seq > proposal[0]:
            proposal = (accepted_seq, value)

    # Accept Phase
    success = list()
    random.shuffle(conns)
    for conn in conns:
        try:
            # Accept this proposal, iff, this node participated in the
            # promise phase of this round. promised_seq == seq.
            #
            # This is stricter implementation than standard paxos
            # Paxos would allow if seq >= promised_seq, but we don't allow
            # to minimize testing effort for this valid, but rare case.
            result = conn.execute(
                '''update kvlog set accepted_seq=?, value=?
                   where key=? and version=? and promised_seq=?
                ''', seq, proposal[1], key, version, seq)

            if 1 == result.rowcount:
                success.append(True)

        except Exception:
            pass

    if len(success) < quorum:
        return 'no-accept-quorum', len(success)

    # Learn Phase
    success = list()
    random.shuffle(conns)
    for conn in conns:
        try:
            trans = conn.begin()

            # Old versions of this key are not needed anymore
            conn.execute('delete from kvlog where key=? and version < ?',
                         key, version)

            # Mark this value as learned, iff, this node participated in both
            # the promise and accept phase of this round.
            # promised_seq == accepted_seq == seq.
            #
            # Paxos would accept a value without any check. But we don't as
            # we don't even send any value in this phase. We just mark the
            # value accepted in ACCEPT phase as learned, and hence we need
            # the check to ensure this node participated in the promise/accept
            result = conn.execute(
                '''update kvlog set promised_seq=null, accepted_seq=null
                   where key=? and version=? and value is not null and
                         promised_seq=? and accepted_seq=?
                ''', key, version, seq, seq)

            trans.commit()
            if 1 == result.rowcount:
                success.append(True)

        except Exception:
            pass

    if len(success) < quorum:
        return 'no-learn-quorum', len(success)

    # This paxos round completed successfully and our proposed value
    # got accepted and learned.
    if 0 == proposal[0]:
        return 'ok', version

    # Well, the paxos round completed and a value was learned for this
    # key,version. But it was not our value. It was a value picked from
    # response we got from all the nodes in the promise phase.
    return 'resolved', proposal[1]


class PaxosTable():
    def __init__(self, servers):
        self.quorum = int(len(servers)/2) + 1  # A simple majority
        self.engines = dict()

        for s in servers:
            meta = sqlalchemy.MetaData()
            sqlalchemy.Table(
                'kvlog', meta,
                sqlalchemy.Column('rowid', sqlalchemy.Integer,
                                  primary_key=True, autoincrement=True),
                sqlalchemy.Column('promised_seq', sqlalchemy.Integer),
                sqlalchemy.Column('accepted_seq', sqlalchemy.Integer),
                sqlalchemy.Column('key', sqlalchemy.Text),
                sqlalchemy.Column('version', sqlalchemy.Integer),
                sqlalchemy.Column('value', sqlalchemy.LargeBinary),
                sqlalchemy.UniqueConstraint('key', 'version'))

            self.engines[s] = sqlalchemy.create_engine(s)
            meta.create_all(self.engines[s])

    def put(self, key, version, value):
        conns = list()
        for engine in self.engines.values():
            try:
                conns.append(engine.connect())
            except Exception:
                pass

        try:
            return paxos(conns, self.quorum, key, version, value)
        finally:
            [conn.close() for conn in conns]

    def get(self, key):
        conns = list()
        for engine in self.engines.values():
            try:
                conns.append(engine.connect())
            except Exception:
                pass

        versions = list()
        random.shuffle(conns)
        for conn in conns:
            try:
                versions.append(list(conn.execute(
                    '''select version from kvlog
                       where key=? and
                             promised_seq is null and
                             accepted_seq is null
                       order by version desc limit 1
                    ''', key))[0][0])
            except Exception:
                pass

        if len(versions) < self.quorum:
            return 'no-quorum', len(versions), None

        version = max(versions)

        random.shuffle(conns)
        for conn in conns:
            try:
                return 'ok', version, list(conn.execute(
                    '''select value from kvlog
                       where key=? and version=? and
                             promised_seq is null and
                             accepted_seq is null
                    ''', key, version))[0][0]
            except Exception:
                pass


if '__main__' == __name__:
    with open(sys.argv[1]) as fd:
        servers = [s.strip() for s in fd.read().split('\n') if s.strip()]

    ptab = PaxosTable(servers)

    if 5 == len(sys.argv):
        key, version, value = sys.argv[2:]

        status, version = ptab.put(key, int(version), value.encode())

        print('{}: {}'.format(status, version))
    if 4 == len(sys.argv):
        key, version = sys.argv[2:]

        status, version = ptab.put(key, int(version), sys.stdin.buffer.read())

        print('{}: {}'.format(status, version))
    elif 3 == len(sys.argv):
        key = sys.argv[2]

        status, version, value = ptab.get(key)

        print('{}: {}'.format(status, version))
        if 'ok' == status:
            sys.stdout.buffer.write(value)

    exit(0 if 'ok' == status else 1)
