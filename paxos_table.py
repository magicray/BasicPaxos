import sys
import time
import random
import sqlalchemy


def paxos(conns, quorum, key, version, value):
    assert(version > 0)

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

            # Value for this key,version has already been learned
            if promised_seq is None and accepted_seq is None:
                return dict(status='already-learned', value=accepted_value)

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
        return dict(status='no-promise-quorum', nodes=len(success))

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
        return dict(status='no-accept-quorum', nodes=len(success))

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
        return dict(status='no-learn-quorum', nodes=len(success))

    # This paxos round completed successfully and our proposed value
    # got accepted and learned.
    if 0 == proposal[0]:
        return dict(status='ok', version=version)

    # Well, the paxos round completed and a value was learned for this
    # key,version. But it was not our value. It was a value picked from
    # response we got from all the nodes in the promise phase.
    return dict(status='resolved', value=proposal[1])


class PaxosTable():
    def __init__(self, servers):
        self.quorum = int(len(servers)/2) + 1  # A simple majority
        self.engines = dict()
        self.conns = list()

        for s in servers:
            meta = sqlalchemy.MetaData()
            sqlalchemy.Table(
                'kvlog', meta,
                sqlalchemy.Column('key', sqlalchemy.Text),
                sqlalchemy.Column('version', sqlalchemy.Integer),
                sqlalchemy.Column('promised_seq', sqlalchemy.Integer),
                sqlalchemy.Column('accepted_seq', sqlalchemy.Integer),
                sqlalchemy.Column('value', sqlalchemy.LargeBinary),
                sqlalchemy.PrimaryKeyConstraint('key', 'version'))

            self.engines[s] = sqlalchemy.create_engine(s)
            meta.create_all(self.engines[s])

    def connect(self):
        if not self.conns:
            for engine in self.engines.values():
                try:
                    self.conns.append(engine.connect())
                except Exception:
                    pass

        random.shuffle(self.conns)
        return self.conns

    def disconnect(self):
        if self.conns:
            [conn.close() for conn in self.conns]
            self.conns = list()

    def put(self, key, version, value):
        try:
            return paxos(self.connect(), self.quorum, key, version, value)
        finally:
            self.disconnect()

    def get(self, key):
        # Find out the latest version for this key on each node
        versions = dict()
        for conn in self.connect():
            try:
                v = list(conn.execute(
                    '''select version from kvlog
                       where key=? and
                             promised_seq is null and
                             accepted_seq is null
                       order by version desc limit 1
                    ''', key))[0][0]

                versions.setdefault(v, list()).append(conn)
            except Exception:
                versions.setdefault(0, list()).append(conn)

        # Find out the latest version for this key on the cluster
        version = max(versions.keys())

        # Read the value for the latest version of this key
        for conn in versions[version]:
            try:
                value = list(conn.execute(
                    '''select value from kvlog
                       where key=? and version=? and
                             promised_seq is null and
                             accepted_seq is null
                    ''', key, version))[0][0]
                break
            except Exception:
                pass

        # Some nodes don't have the latest version of the value for this key.
        #
        # It is important to ensure that the a mojority has the latest learned
        # version. If that is not done, and the node with the latest learned
        # value goes down, we might have to return an older version of the
        # value and that wouldbreak the promise of being a strongly consistent
        # KeyValue store.
        for v, conn_list in versions.items():
            if v == version:
                continue

            for conn in conn_list:
                try:
                    trans = conn.begin()
                    conn.execute(
                        '''delete from kvlog
                           where key=? and version <= ?
                        ''', key, version)
                    result = conn.execute(
                        '''insert into kvlog
                           (key,version,promised_seq,accepted_seq,value)
                           values(?,?,null,null,?)
                        ''', key, version, value)
                    trans.commit()

                    if 1 == result.rowcount:
                        versions[version].append(conn)
                except Exception:
                    pass

        self.disconnect()

        # We do not have a majority with the latest value
        if len(versions[version]) < self.quorum:
            return dict(status='no-quorum', replicas=len(versions[version]))

        if version > 0:
            return dict(status='ok', version=version,
                        replicas=len(versions[version]),
                        value=value)

        return dict(status='not-found')


if '__main__' == __name__:
    sys.argv.extend((None, None, None))
    server_file, key, version, value = sys.argv[1:5]

    with open(server_file) as fd:
        servers = [s.strip() for s in fd.read().split('\n') if s.strip()]

    ptab = PaxosTable(servers)

    if value:
        r = ptab.put(key, int(version), value.encode())
        print(r, file=sys.stderr)
    elif version:
        r = ptab.put(key, int(version), sys.stdin.buffer.read())
        print(r, file=sys.stderr)
    elif key:
        r = ptab.get(key)

        print('status({}) version({}) replicas({})'.format(
            r['status'], r.get('version', ''), r.get('replicas', '')),
            file=sys.stderr)

        if r.get('value', ''):
            sys.stdout.buffer.write(r['value'])
            sys.stdout.flush()
            sys.stderr.write('\n')

    exit(0 if 'ok' == r['status'] else 1)
