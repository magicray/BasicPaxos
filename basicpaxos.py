import sys
import time
import random
import hashlib
import sqlalchemy


def paxos(conns, quorum, key, version, value):
    if not key or not value or version < 1:
        return dict(status='invalid-input')

    seq = int(time.time())  # Paxos Seq

    keyhash = hashlib.sha256(key).hexdigest()

    # Ensure only next version number is being used
    existing_version = 0
    random.shuffle(conns)
    for conn in conns:
        try:
            # Find out the max version
            rows = list(conn.execute(sqlalchemy.text(
                '''select max(version) from paxostable
                   where keyhash=:keyhash and
                         promised_seq is null and accepted_seq is null
                ''').params(keyhash=keyhash)))

            if rows and rows[0][0] and rows[0][0] > existing_version:
                existing_version = rows[0][0]
        except Exception:
            pass

    if version != existing_version + 1:
        return dict(status='invalid-version', version=existing_version)

    # Promise Phase
    success = list()
    random.shuffle(conns)
    for conn in conns:
        try:
            # Insert a row, if does not exist already
            conn.execute(sqlalchemy.text(
                '''insert into paxostable
                   (keyhash,version,promised_seq,accepted_seq,keyblob)
                   values(:keyhash,:version,0,0,:keyblob)
                ''').params(keyhash=keyhash, version=version, keyblob=key))
        except Exception:
            pass

        try:
            trans = conn.begin()

            # Get the information from the old paxos round
            promised_seq, accepted_seq, accepted_value = list(
                conn.execute(sqlalchemy.text(
                    '''select promised_seq,accepted_seq,valueblob
                       from paxostable
                       where keyhash=:keyhash and version=:version and
                             keyblob is not null
                    ''').params(keyhash=keyhash, version=version)))[0]

            # Value for this key,version has already been learned
            if promised_seq is None and accepted_seq is None:
                return dict(status='already-learned', value=accepted_value)

            # A more recent paxos round has been seen by this node.
            # It should not participate in this round.
            if promised_seq >= seq:
                trans.rollback()
                continue

            # Record seq to reject any old stray paxos rounds
            conn.execute(sqlalchemy.text(
                '''update paxostable set promised_seq=:seq
                   where keyhash=:keyhash and version=:version and
                         keyblob is not null
                ''').params(seq=seq, keyhash=keyhash, version=version))

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

    valuehash = hashlib.sha256(proposal[1]).hexdigest()

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
            result = conn.execute(sqlalchemy.text(
                '''update paxostable
                   set accepted_seq=:seq, valuehash=:valuehash,
                       valueblob=:valueblob
                   where keyhash=:keyhash and version=:version and
                         promised_seq=:seq and keyblob is not null
                ''').params(seq=seq, valuehash=valuehash,
                            valueblob=proposal[1],
                            keyhash=keyhash, version=version))

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

            # Remove old versions
            conn.execute(sqlalchemy.text(
                '''delete from paxostable
                   where keyhash=:keyhash and version < :version
                ''').params(keyhash=keyhash, version=version))

            # Mark this value as learned, iff, this node participated in both
            # the promise and accept phase of this round.
            # promised_seq == accepted_seq == seq.
            #
            # Paxos would accept a value without any check. But we don't as
            # we don't even send any value in this phase. We just mark the
            # value accepted in ACCEPT phase as learned, and hence we need
            # the check to ensure this node participated in the promise/accept
            result = conn.execute(sqlalchemy.text(
                '''update paxostable set promised_seq=null, accepted_seq=null
                   where keyhash=:keyhash and version=:version and
                         keyblob is not null and
                         valuehash is not null and valueblob is not null and
                         promised_seq=:seq and accepted_seq=:seq
                ''').params(keyhash=keyhash, version=version, seq=seq))

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


def read(conns, quorum, key, cache_expiry):
    keyhash = hashlib.sha256(key).hexdigest()
    valuehash = None

    # Find out latest version for this key
    versions = list()
    for conn in conns:
        try:
            rows = list(conn.execute(sqlalchemy.text(
                '''select max(version) from paxostable
                   where keyhash=:keyhash and
                         promised_seq is null and accepted_seq is null
                ''').params(keyhash=keyhash)))

            if rows and rows[0][0]:
                versions.append((rows[0][0], conn))
            else:
                versions.append((0, conn))
        except Exception:
            pass

    version = max([v for v, c in versions]) if versions else 0
    if 0 == version:
        return dict(status='not-found')

    in_sync = [conn for ver, conn in versions if ver == version]
    out_of_sync = [conn for ver, conn in versions if ver != version]

    for conn in in_sync:
        try:
            value = list(conn.execute(sqlalchemy.text(
                '''select valueblob from paxostable
                   where keyhash=:keyhash and version=:version and
                         promised_seq is null and accepted_seq is null
                ''').params(keyhash=keyhash, version=version)))[0][0]
            break
        except Exception:
            pass

    valuehash = hashlib.sha256(value).hexdigest()

    for conn in out_of_sync:
        try:
            trans = conn.begin()

            conn.execute(sqlalchemy.text(
                '''delete from paxostable
                   where keyhash=:keyhash and version=:version
                ''').params(keyhash=keyhash, version=version))

            result = conn.execute(sqlalchemy.text(
                '''insert into paxostable
                   (keyhash,version,promised_seq,accepted_seq,valuehash,
                    keyblob,valueblob)
                   values(:keyhash,:version,null,null,:valuehash,
                          :keyblob,:valueblob)
                ''').params(keyhash=keyhash, version=version,
                            valuehash=valuehash, keyblob=key, valueblob=value))

            trans.commit()

            if 1 == result.rowcount:
                in_sync.append(conn)
        except Exception:
            pass

    # We do not have a majority with the latest value
    if len(in_sync) < quorum:
        return dict(status='no-quorum', replicas=len(in_sync))

    # All good
    return dict(status='ok', version=version,
                replicas=len(in_sync), value=value)


class PaxosTable():
    def __init__(self, servers):
        self.quorum = int(len(servers)/2) + 1  # A simple majority
        self.engines = dict()
        self.conns = list()

        for s in servers:
            meta = sqlalchemy.MetaData()
            sqlalchemy.Table(
                'paxostable', meta,
                sqlalchemy.Column('keyhash', sqlalchemy.VARCHAR(64)),
                sqlalchemy.Column('version', sqlalchemy.Integer),
                sqlalchemy.Column('promised_seq', sqlalchemy.Integer),
                sqlalchemy.Column('accepted_seq', sqlalchemy.Integer),
                sqlalchemy.Column('valuehash', sqlalchemy.VARCHAR(64)),
                sqlalchemy.Column('keyblob', sqlalchemy.LargeBinary),
                sqlalchemy.Column('valueblob', sqlalchemy.LargeBinary),
                sqlalchemy.PrimaryKeyConstraint('keyhash', 'version'))

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
        try:
            return read(self.connect(), self.quorum, key, cache_expiry=30)
        finally:
            self.disconnect()


def main():
    sys.argv.extend((None, None, None))
    server_file, key, version, value = sys.argv[1:5]

    with open(server_file) as fd:
        servers = [s.strip() for s in fd.read().split('\n') if s.strip()]

    key = key.encode()
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


if '__main__' == __name__:
    main()
