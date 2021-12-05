import time
import random
import logging
import argparse
import sqlalchemy
from logging import critical as log


def paxos(db_engines, key, version, value):
    seq = int(time.time())                 # Paxos Seq
    quorum = int(len(db_engines)/2) + 1    # Quorum

    def shuffle(inlist):
        random.shuffle(inlist)
        return inlist

    def get_seq(conn, key, version):
        rows = conn.execute('''select rowid, promised_seq, accepted_seq
                               from kvlog
                               where key=? and version=?
                            ''', key, version)
        rows = list(rows)

        return rows[0] if rows else (None, 0, 0)

    # Promise
    success = list()
    for engine in shuffle(db_engines):
        with engine.begin() as conn:
            execute = conn.execute

            rowid, promised_seq, accepted_seq = get_seq(conn, key, version)

            if rowid is not None and promised_seq is None and accepted_seq is None:
                log('already-learned')
                return 'already-learned'

            if rowid is not None and promised_seq >= seq:
                continue

            if rowid is None:
                execute('''insert into kvlog(rowid,key,version,promised_seq)
                           values(null,?,?,?)
                        ''', key, version, seq)
            else:
                execute('update kvlog set promised_seq=? where rowid=?',
                        seq, rowid)

            if accepted_seq:
                rows = execute('select value from kvlog where rowid=?', rowid)
                success.append((accepted_seq, list(rows)[0][0]))
            else:
                success.append((0, None))

    if len(success) < quorum:
        log('no-promise-quorum {}'.format(len(success)))
        return 'no-promise-quorum'

    proposal = (0, value)
    for accepted_seq, value in success:
        if accepted_seq > proposal[0]:
            proposal = (accepted_seq, value)

    # Accept
    success = list()
    for engine in shuffle(db_engines):
        with engine.begin() as conn:
            execute = conn.execute

            rowid, promised_seq, accepted_seq = get_seq(conn, key, version)

            if rowid is not None and promised_seq > seq:
                continue

            if rowid is None:
                execute('''insert into kvlog(rowid,key,version,promised_seq,accepted_seq,value)
                           values(null,?,?,?,?,?)
                        ''', key, version, seq, seq, proposal[1])
            else:
                execute('''update kvlog
                           set promised_seq=?, accepted_seq=?, value=?
                           where rowid=?
                        ''', seq, seq, proposal[1], rowid)

            success.append(True)

    if len(success) < quorum:
        log('no-accept-quorum {}'.format(len(success)))
        return 'no-accept-quorum'

    # Learn
    for engine in shuffle(db_engines):
        with engine.begin() as conn:
            execute = conn.execute

            execute('''insert into kvlog(key, version, value)
                       select key, version, value from kvlog
                       where key=? and version=? and value is not null and
                             promised_seq=? and accepted_seq=?
                    ''', key, version, seq, seq)
            execute('''delete from kvlog
                       where key=? and version=? and value is not null and
                             promised_seq=? and accepted_seq=?
                    ''', key, version, seq, seq)
            rows = execute('''select count(*) from kvlog
                              where key=? and version=? and
                                    value is not null and
                                    promised_seq is null and
                                    accepted_seq is null
                           ''', key, version)

            if 1 == list(rows)[0][0]:
                success.append(True)

    if len(success) < quorum:
        log('no-learn-quorum {}'.format(len(success)))
        return 'no-learn-quorum'

    log('ok' if proposal[0] == 0 else 'updated')
    return 'ok'


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


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--key', dest='key')
    ARGS.add_argument('--value', dest='value')
    ARGS.add_argument('--version', dest='version', type=int)

    ARGS.add_argument('--table', dest='table', default='demo')
    ARGS.add_argument('--servers', dest='servers')
    ARGS = ARGS.parse_args()
    with open(ARGS.servers) as fd:
        ARGS.servers = [s.strip() for s in fd.read().split('\n') if s.strip()]

    tab = Table(ARGS.servers)
    tab.put(ARGS.key, ARGS.version, ARGS.value.encode())
