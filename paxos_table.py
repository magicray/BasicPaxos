import time
import random
import argparse
import sqlalchemy


def paxos(conns, quorum, key, version, value):
    seq = int(time.time())            # Paxos Seq

    # Promise Phase
    success = list()
    random.shuffle(conns)
    for conn in conns:
        try:
            conn.execute('''insert into kvlog
                            (key,version,promised_seq,accepted_seq)
                            values(?,?,0,0)
                         ''', key, version)
        except Exception:
            pass

        try:
            trans = conn.begin()

            promised_seq, accepted_seq, accepted_value = list(
                conn.execute('''select promised_seq,accepted_seq,value
                                from kvlog
                                where key=? and version=?
                             ''', key, version))[0]

            if promised_seq is None and accepted_seq is None:
                return 'already-learned', accepted_value

            # Another, more recent, paxos round already started,
            # Reject this round
            if promised_seq >= seq:
                continue

            # To reject any unfinished older PAXOS rounds having seq less
            # than this, record the current seq
            conn.execute('''update kvlog set promised_seq=?
                            where key=? and version=?
                         ''', seq, key, version)

            trans.commit()

            success.append((accepted_seq, accepted_value))
        except Exception:
            pass

    if len(success) < quorum:
        return 'no-promise-quorum', len(success)

    # This is the most subtle PAXOS step
    #
    # Lets find the most recent value accepted by nodes in the ACCEPT phase
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
            # Accept this proposal
            # promised_seq == accepted_seq == seq ensures that this row
            # participated in the current paxos round.
            result = conn.execute('''update kvlog
                                     set accepted_seq=?, value=?
                                     where key=? and version=? and
                                           promised_seq=?
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
            # Finalize this row by setting promised_seq = accepted_seq = null
            # promised_seq == accepted_seq == seq ensures that this row
            # participated in the current paxos round.
            result = conn.execute('''update kvlog
                                     set promised_seq=?, accepted_seq=null
                                     where key=? and version=? and
                                           value is not null and
                                           promised_seq=? and accepted_seq=?
                                  ''', None, key, version, seq, seq)

            if 1 == result.rowcount:
                success.append(True)

        except Exception:
            pass

    if len(success) < quorum:
        return 'no-learn-quorum', len(success)

    if 0 == proposal[0]:
        return 'ok', version

    return 'resolved', proposal[1]


class PaxosTable():
    def __init__(self, servers):
        self.engines = dict()
        self.servers = servers

        for s in self.servers:
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
        # These many nodes should agree before
        # an operation is considered successful
        quorum = int(len(self.engines)/2) + 1

        conns = list()
        for engine in self.engines.values():
            try:
                conns.append(engine.connect())
            except Exception:
                pass

        if version is None:
            versions = list()
            random.shuffle(conns)
            for conn in conns:
                try:
                    trans = conn.begin()
                    v = list(conn.execute('''select max(version)
                                             from kvlog where key=?
                                          ''', key))[0][0]
                    v = v+1 if v else 1
                    conn.execute('''insert into kvlog
                                    (key,version,promised_seq,accepted_seq)
                                    values(?,?,0,0)
                                 ''', key, v)
                    trans.commit()
                    versions.append(v)
                except Exception:
                    pass

            version = max(versions)

        try:
            return paxos(conns, quorum, key, version, value)
        finally:
            [conn.close() for conn in conns]

    def get(self, key, version=None):
        success = list()
        for engine in self.engines.values():
            with engine.begin() as conn:
                rows = list(conn.execute('''select version from kvlog
                                            where key=? and
                                                  promised_seq is null and
                                                  accepted_seq is null
                                            order by version desc
                                            limit 1
                                         ''', key))
                if rows:
                    success.append((engine, rows[0][0]))

        if len(success) < int(len(self.engines)/2) + 1:
            return 'no-quorum', len(success), None

        version = max([v for e, v in success])
        for engine, v in success:
            if v != version:
                continue

            with engine.begin() as conn:
                rows = conn.execute('''select value from kvlog
                                       where key=? and version=? and
                                             promised_seq is null and
                                             accepted_seq is null
                                    ''', key, version)

                return 'ok', version, list(rows)[0][0]


if '__main__' == __name__:
    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--key', dest='key')
    ARGS.add_argument('--value', dest='value')
    ARGS.add_argument('--version', dest='version', type=int)

    ARGS.add_argument('--servers', dest='servers')
    ARGS = ARGS.parse_args()
    with open(ARGS.servers) as fd:
        ARGS.servers = [s.strip() for s in fd.read().split('\n') if s.strip()]

    ptab = PaxosTable(ARGS.servers)

    if ARGS.value and ARGS.version:
        print(ptab.put(ARGS.key, ARGS.version, ARGS.value.encode()))
    elif ARGS.value:
        print(ptab.put(ARGS.key, None, ARGS.value.encode()))
    else:
        print(ptab.get(ARGS.key))
