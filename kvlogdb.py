import time
import random
import argparse
import sqlalchemy


def paxos(db_engines, key, version, value):
    seq = int(time.time())                 # Paxos Seq
    quorum = int(len(db_engines)/2) + 1    # Quorum

    def shuffle(inlist):
        # Pick servers in random order to invokde as many conflicts
        # This helps us find out any corner cases quickly
        random.shuffle(inlist)
        return inlist

    def get_seq(conn, key, version):
        # Non null rowid means a row exists
        # promised_seq and accepted_seq are the key elements in PAXOS
        rows = conn.execute('''select rowid, promised_seq, accepted_seq
                               from kvlog
                               where key=? and version=?
                            ''', key, version)
        rows = list(rows)

        return rows[0] if rows else (None, 0, 0)

    # Promise Phase
    success = list()
    for engine in shuffle(db_engines):
        with engine.begin() as conn:
            rowid, promised_seq, accepted_seq = get_seq(conn, key, version)

            # A row for this key,version exists
            if rowid is not None:
                # Value for this key,version is already learned
                if promised_seq is None and accepted_seq is None:
                    return 'already-learned', 0

                # Another, more recent, paxos round is in progress. Back out.
                if promised_seq >= seq:
                    continue

            if rowid is None:
                # Insert a row for this key,version as none exists
                conn.execute('''insert into kvlog(key,version,promised_seq)
                                values(?,?,?)
                             ''', key, version, seq)
            else:
                # A row exists and has a lower promised_seq than seq
                # we can participate in this round.
                #
                # To reject any PAXOS round having seq less than this,
                # need to record the current seq
                conn.execute('update kvlog set promised_seq=? where rowid=?',
                             seq, rowid)

            if accepted_seq:
                # This node accepted a value in a previous paxos round,
                # return this value as required by PAXOS
                rows = conn.execute('select value from kvlog where rowid=?',
                                    rowid)
                success.append((accepted_seq, list(rows)[0][0]))
            else:
                # Though this node participated in a (or many) previous paxos
                # rounds, it never reached the accept phase
                success.append((0, None))

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
    for engine in shuffle(db_engines):
        with engine.begin() as conn:
            rowid, promised_seq, accepted_seq = get_seq(conn, key, version)

            # Back out as this node has already particiapted
            # in a more recent paxos round
            if rowid is not None and promised_seq > seq:
                continue

            if rowid is None:
                # This node is participating in paxos round for this
                # key,version for the first time. Lets insert in new row.
                conn.execute('''insert into kvlog(key,version,promised_seq,
                                                  accepted_seq,value)
                                values(?,?,?,?,?)
                             ''', key, version, seq, seq, proposal[1])
            else:
                # Though this node participated in earlier PAXOS rounds,
                # this is the most recent one. Lets accept this value
                # and promise to ignore any older rounds (seq values)
                # in future
                conn.execute('''update kvlog set promised_seq=?,
                                                 accepted_seq=?, value=?
                                where rowid=?
                             ''', seq, seq, proposal[1], rowid)

            success.append(True)

    if len(success) < quorum:
        return 'no-accept-quorum', len(success)

    # Learn Phase
    for engine in shuffle(db_engines):
        with engine.begin() as conn:
            # promised_seq = accepted_seq = null means the value
            # for this key,version pair has been learned

            # Insert a new row. A new rowid is set and
            # promised_seq and accepted_seq are set to null
            #
            # Do this only if this row participated in the promise/accept
            # round. It should have promised_seq = accepted_seq = seq
            conn.execute('''insert into kvlog(key, version, value)
                            select key, version, value from kvlog
                            where key=? and version=? and
                                  value is not null and
                                  promised_seq=? and accepted_seq=?
                         ''', key, version, seq, seq)

            # Delete the row that participated in promise/accept phase
            conn.execute('''delete from kvlog
                            where key=? and version=? and
                            value is not null and
                            promised_seq=? and accepted_seq=?
                    ''', key, version, seq, seq)

            # We should now have exactly one row for this key,version pair,
            # with value column set to something non null.
            #
            # promised_seq and accepted_seq should be null to indicate that
            # this value is finalized
            rows = conn.execute('''select count(*) from kvlog
                              where key=? and version=? and
                                    value is not null and
                                    promised_seq is null and
                                    accepted_seq is null
                           ''', key, version)

            # This node learned this value if there was exactly one row
            if 1 == list(rows)[0][0]:
                success.append(True)

    if len(success) < quorum:
        return 'no-learn-quorum', len(success)

    return ('ok' if proposal[0] == 0 else 'updated', version)


class Table():
    def __init__(self, servers):
        self.meta = dict()
        self.tables = dict()
        self.engines = dict()
        self.servers = servers

        for s in self.servers:
            self.meta[s] = sqlalchemy.MetaData()
            self.tables[s] = sqlalchemy.Table(
                'kvlog', self.meta[s],
                sqlalchemy.Column('rowid', sqlalchemy.Integer,
                                  primary_key=True, autoincrement=True),
                sqlalchemy.Column('promised_seq', sqlalchemy.Integer),
                sqlalchemy.Column('accepted_seq', sqlalchemy.Integer),
                sqlalchemy.Column('key', sqlalchemy.Text),
                sqlalchemy.Column('version', sqlalchemy.Integer),
                sqlalchemy.Column('value', sqlalchemy.LargeBinary))

            self.engines[s] = sqlalchemy.create_engine(s)
            self.meta[s].create_all(self.engines[s])

    def put(self, key, version, value):
        return paxos(list(self.engines.values()), key, version, value)

    def append(self, topic, value):
        return None

    def get(self, key):
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

    tab = Table(ARGS.servers)

    if ARGS.value and ARGS.version:
        print(tab.put(ARGS.key, ARGS.version, ARGS.value.encode()))
    elif ARGS.value:
        print(tab.put(ARGS.key, ARGS.value.encode()))
    else:
        print(tab.get(ARGS.key))
