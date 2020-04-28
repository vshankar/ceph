"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import cephfs
import errno
import rados
from contextlib import contextmanager
import re
from mgr_util import CephfsClient, CephfsConnectionException, \
        open_filesystem
from collections import OrderedDict
from datetime import datetime, timezone
import logging
from threading import Timer
import sqlite3


SNAP_SCHEDULE_NAMESPACE = 'cephfs-snap-schedule'
SNAP_DB_PREFIX = 'snap_db'
# increment this every time the db schema changes and provide upgrade code
SNAP_DB_VERSION = '0'
SNAP_DB_OBJECT_NAME = f'{SNAP_DB_PREFIX}_v{SNAP_DB_VERSION}'
SNAPSHOT_TS_FORMAT = '%Y-%m-%d-%H_%M_%S'

log = logging.getLogger(__name__)


@contextmanager
def open_ioctx(self, pool):
    try:
        if type(pool) is int:
            with self.mgr.rados.open_ioctx2(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
        else:
            with self.mgr.rados.open_ioctx(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
    except rados.ObjectNotFound:
        log.error("Failed to locate pool {}".format(pool))
        raise


def updates_schedule_db(func):
    def f(self, fs, *args):
        func(self, fs, *args)
        self.refresh_snap_timers(fs)
    return f


class Schedule(object):
    '''
    Wrapper to work with schedules stored in sqlite
    '''
    def __init__(self,
                 path,
                 schedule,
                 retention_policy,
                 start,
                 fs_name,
                 subvol,
                 rel_path,
                 created=None,
                 first=None,
                 last=None,
                 last_pruned=None,
                 created_count=0,
                 pruned_count=0,
                 ):
        self.fs = fs_name
        self.subvol = subvol
        self.path = path
        self.rel_path = rel_path
        self.schedule = schedule
        self.retention = retention_policy
        if start is None:
            now = datetime.now(timezone.utc)
            self.start = datetime(now.year,
                                  now.month,
                                  now.day,
                                  tzinfo=now.tzinfo)
        else:
            self.start = datetime.fromisoformat(start).astimezone(timezone.utc)
        if created is None:
            self.created = datetime.now(timezone.utc)
        if first:
            self.first = datetime.fromisoformat(first)
        else:
            self.first = first
        if last:
            self.last = datetime.fromisoformat(last)
        else:
            self.last = last
        if last_pruned:
            self.last_pruned = datetime.fromisoformat(last_pruned)
        else:
            self.last_pruned = last_pruned
        self.last = last
        self.last_pruned = last_pruned
        self.created_count = created_count
        self.pruned_count = pruned_count

    def __str__(self):
        return f'''{self.path}: {self.schedule}; {self.retention}'''

    CREATE_TABLES = '''CREATE TABLE schedules(
        id integer PRIMARY KEY ASC,
        path text NOT NULL UNIQUE,
        subvol text,
        rel_path text NOT NULL,
        active int NOT NULL
    );
    CREATE TABLE schedules_meta(
        id INTEGER PRIMARY KEY ASC,
        schedule_id INT,
        start TEXT NOT NULL,
        first TEXT,
        last TEXT,
        last_pruned TEXT,
        created TEXT,
        repeat BIGINT NOT NULL,
        schedule TEXT NOT NULL,
        created_count INT,
        pruned_count INT,
        retention TEXT,
        FOREIGN KEY(schedule_id) REFERENCES schedules(id) ON DELETE CASCADE,
        UNIQUE (start, repeat)
    );'''

    GET_SCHEDULES = '''SELECT
        s.path, s.subvol, s.rel_path, s.active,
        sm.schedule, sm.retention, sm.start, sm.first, sm.last,
        sm.last_pruned, sm.created, sm.created_count, sm.pruned_count
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        WHERE s.path = ?'''

    @classmethod
    def get_schedules(cls, path, db, fs):
        with db:
            c = db.execute(cls.GET_SCHEDULES, (path,))
        return [Schedule._from_get_query(row, fs) for row in c.fetchall()]

    @classmethod
    def _from_get_query(cls, table_row, fs):
        return cls(table_row['path'],
                   table_row['schedule'],
                   table_row['retention'],
                   table_row['start'],
                   fs,
                   table_row['subvol'],
                   table_row['rel_path'],
                   table_row['created'],
                   table_row['first'],
                   table_row['last'],
                   table_row['last_pruned'],
                   table_row['created_count'],
                   table_row['pruned_count'])

    LIST_SCHEDULES = '''SELECT
        s.path, sm.schedule, sm.retention
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        WHERE'''

    @classmethod
    def list_schedules(cls, path, db, fs, recursive):
        with db:
            if recursive:
                c = db.execute(cls.LIST_SCHEDULES + ' path LIKE ?',
                               (f'{path}%',))
            else:
                c = db.execute(cls.LIST_SCHEDULES + ' path = ?',
                               (f'{path}',))
        return [row for row in c.fetchall()]

    INSERT_SCHEDULE = '''INSERT INTO
        schedules(path, subvol, rel_path, active)
        Values(?, ?, ?, ?);'''
    INSERT_SCHEDULE_META = '''INSERT INTO
        schedules_meta(schedule_id, start, created, repeat, schedule, retention)
        SELECT ?, ?, ?, ?, ?, ?'''

    def store_schedule(self, db):
        sched_id = None
        with db:
            try:
                c = db.execute(self.INSERT_SCHEDULE,
                               (self.path,
                                self.subvol,
                                self.rel_path,
                                1))
                sched_id = c.lastrowid
            except sqlite3.IntegrityError:
                # might be adding another schedule, retrieve sched id
                log.debug(f'found schedule entry for {self.path}, trying to add meta')
                c = db.execute('SELECT id FROM schedules where path = ?',
                               (self.path,))
                sched_id = c.fetchone()[0]
                pass
            db.execute(self.INSERT_SCHEDULE_META,
                       (sched_id,
                        self.start.isoformat(),
                        self.created,
                        self.repeat_in_s(),
                        self.schedule,
                        self.retention))

    @classmethod
    def rm_schedule(cls, db, path, repeat, start):
        with db:
            cur = db.execute('SELECT id FROM schedules WHERE path = ?',
                             (path,))
            row = cur.fetchone()

            if len(row) == 0:
                log.info(f'no schedule for {path} found')
                raise ValueError('SnapSchedule for {} not found'.format(path))

            id_ = tuple(row)

            if repeat or start:
                meta_delete = 'DELETE FROM schedules_meta WHERE schedule_id = ?'
                delete_param = id_
                if repeat:
                    meta_delete += ' AND schedule = ?'
                    delete_param += (repeat,)
                if start:
                    meta_delete += ' AND start = ?'
                    delete_param += (start,)
                # maybe only delete meta entry
                log.debug(f'executing {meta_delete}, {delete_param}')
                res = db.execute(meta_delete + ';', delete_param).rowcount
                if res < 1:
                    raise ValueError(f'No schedule found for {repeat} {start}')
                db.execute('COMMIT;')
                # now check if we have schedules in meta left, if not delete
                # the schedule as well
                meta_count = db.execute(
                    'SELECT COUNT() FROM schedules_meta WHERE schedule_id = ?',
                    id_)
                if meta_count.fetchone() == (0,):
                    log.debug(
                        f'no more schedules left, cleaning up schedules table')
                    db.execute('DELETE FROM schedules WHERE id = ?;', id_)
            else:
                # just delete the schedule CASCADE DELETE takes care of the
                # rest
                db.execute('DELETE FROM schedules WHERE id = ?;', id_)

    def report(self):
        import pprint
        return pprint.pformat(self.__dict__)

    def repeat_in_s(self):
        mult = self.schedule[-1]
        period = int(self.schedule[0:-1])
        if mult == 'm':
            return period * 60
        elif mult == 'h':
            return period * 60 * 60
        elif mult == 'd':
            return period * 60 * 60 * 24
        elif mult == 'w':
            return period * 60 * 60 * 24 * 7
        else:
            raise Exception('schedule multiplier not recognized')


def parse_retention(retention):
    ret = {}
    matches = re.findall(r'\d+[a-z]', retention)
    for m in matches:
        ret[m[-1]] = int(m[0:-1])
    matches = re.findall(r'\d+[A-Z]', retention)
    for m in matches:
        ret[m[-1]] = int(m[0:-1])
    return ret


class SnapSchedClient(CephfsClient):

    def __init__(self, mgr):
        super(SnapSchedClient, self).__init__(mgr)
        # TODO maybe iterate over all fs instance in fsmap and load snap dbs?
        self.sqlite_connections = {}
        self.active_timers = {}

    # TODO limit query to all schedules with start in the past
    EXEC_QUERY = '''SELECT
        s.path, sm.retention,
        sm.repeat - (strftime("%s", "now") - strftime("%s", sm.start)) % sm.repeat "until"
        FROM schedules s
            INNER JOIN schedules_meta sm ON sm.schedule_id = s.id
        ORDER BY until;'''

    def refresh_snap_timers(self, fs):
        try:
            log.debug(f'SnapDB on {fs} changed, updating next Timer')
            db = self.get_schedule_db(fs)
            rows = []
            with db:
                cur = db.execute(self.EXEC_QUERY)
                rows = cur.fetchmany(1)
            timers = self.active_timers.get(fs, [])
            for timer in timers:
                timer.cancel()
            timers = []
            for row in rows:
                log.debug(f'adding timer for {row}')
                log.debug(f'Creating new snapshot timer')
                t = Timer(row[2],
                          self.create_scheduled_snapshot,
                          args=[fs, row[0], row[1]])
                t.start()
                timers.append(t)
                log.debug(f'Will snapshot {row[0]} in fs {fs} in {row[2]}s')
            self.active_timers[fs] = timers
        except Exception as e:
            log.error(f'refresh raised {e}')

    def get_schedule_db(self, fs):
        if fs not in self.sqlite_connections:
            self.sqlite_connections[fs] = sqlite3.connect(
                ':memory:',
                check_same_thread=False)
            with self.sqlite_connections[fs] as con:
                con.row_factory = sqlite3.Row
                con.execute("PRAGMA FOREIGN_KEYS = 1")
                pool = self.get_metadata_pool(fs)
                with open_ioctx(self, pool) as ioctx:
                    try:
                        size, _mtime = ioctx.stat(SNAP_DB_OBJECT_NAME)
                        db = ioctx.read(SNAP_DB_OBJECT_NAME,
                                        size).decode('utf-8')
                        con.executescript(db)
                    except rados.ObjectNotFound:
                        log.info(f'No schedule DB found in {fs}')
                        con.executescript(Schedule.CREATE_TABLES)
        return self.sqlite_connections[fs]

    def store_schedule_db(self, fs):
        # only store db is it exists, otherwise nothing to do
        metadata_pool = self.get_metadata_pool(fs)
        if not metadata_pool:
            raise CephfsConnectionException(
                -errno.ENOENT, "Filesystem {} does not exist".format(fs))
        if fs in self.sqlite_connections:
            db_content = []
            db = self.sqlite_connections[fs]
            with db:
                for row in db.iterdump():
                    db_content.append(row)
        with open_ioctx(self, metadata_pool) as ioctx:
            ioctx.write_full(SNAP_DB_OBJECT_NAME,
                             '\n'.join(db_content).encode('utf-8'))

    def create_scheduled_snapshot(self, fs_name, path, retention):
        log.debug(f'Scheduled snapshot of {path} triggered')
        try:
            time = datetime.now(timezone.utc).strftime(SNAPSHOT_TS_FORMAT)
            with open_filesystem(self, fs_name) as fs_handle:
                fs_handle.mkdir(f'{path}/.snap/scheduled-{time}', 0o755)
            log.info(f'created scheduled snapshot of {path}')
            # TODO change last snap timestamp in db, maybe first
        except cephfs.Error as e:
            log.info(f'scheduled snapshot creating of {path} failed: {e}')
            # TODO set inactive if path doesn't exist
        except Exception as e:
            # catch all exceptions cause otherwise we'll never know since this
            # is running in a thread
            log.error(f'ERROR create_scheduled_snapshot raised{e}')
        finally:
            log.info(f'finally branch')
            self.refresh_snap_timers(fs_name)
            log.info(f'calling prune')
            self.prune_snapshots(fs_name, path, retention)

    def prune_snapshots(self, fs_name, path, retention):
        log.debug('Pruning snapshots')
        ret = parse_retention(retention)
        if not ret:
            # TODO prune if too many (300?)
            log.debug(f'schedule on {path} has no retention specified')
            return
        try:
            prune_candidates = set()
            with open_filesystem(self, fs_name) as fs_handle:
                with fs_handle.opendir(f'{path}/.snap') as d_handle:
                    dir_ = fs_handle.readdir(d_handle)
                    while dir_:
                        if dir_.d_name.startswith(b'scheduled-'):
                            log.debug(f'add {dir_.d_name} to pruning')
                            ts = datetime.strptime(
                                dir_.d_name.lstrip(b'scheduled-').decode('utf-8'),
                                SNAPSHOT_TS_FORMAT)
                            prune_candidates.add((dir_, ts))
                        else:
                            log.debug(f'skipping dir entry {dir_.d_name}')
                        dir_ = fs_handle.readdir(d_handle)
                to_keep = self.get_prune_set(prune_candidates, ret)
                for k in prune_candidates - to_keep:
                    dirname = k[0].d_name.decode('utf-8')
                    log.debug(f'rmdir on {dirname}')
                    fs_handle.rmdir(f'{path}/.snap/{dirname}')
                log.debug(f'keeping {to_keep}')
        except Exception as e:
            log.debug(f'prune_snapshots threw {e}')

    def get_prune_set(self, candidates, retention):
        PRUNING_PATTERNS = OrderedDict([
            #TODO remove M for release
            ("M", '%Y-%m-%d-%H_%M'),
            ("h", '%Y-%m-%d-%H'),
            ("d", '%Y-%m-%d'),
            ("w", '%G-%V'),
            ("m", '%Y-%m'),
            ("y", '%Y'),
        ])
        keep = set()
        log.debug(retention)
        for period, date_pattern in PRUNING_PATTERNS.items():
            period_count = retention.get(period, 0)
            if not period_count:
                log.debug(f'skipping period {period}')
                continue
            last = None
            for snap in sorted(candidates, key=lambda x: x[0].d_name,
                               reverse=True):
                snap_ts = snap[1].strftime(date_pattern)
                log.debug(f'{snap_ts} : {last}')
                if snap_ts != last:
                    last = snap_ts
                    if snap not in keep:
                        log.debug(f'keeping {snap[0].d_name} due to {period_count}{period}')
                        keep.add(snap)
                        if len(keep) == period_count:
                            log.debug(f'found enough snapshots for {period_count}{period}')
                            break
            # TODO maybe do candidates - keep here? we want snaps counting it
            # hours not be considered for days and it cuts down on iterations
        return keep

    def get_snap_schedules(self, fs, path):
        db = self.get_schedule_db(fs)
        return Schedule.get_schedules(path, db, fs)

    def list_snap_schedules(self, fs, path, recursive):
        db = self.get_schedule_db(fs)
        return Schedule.list_schedules(path, db, fs, recursive)

    @updates_schedule_db
    def store_snap_schedule(self, fs, sched):
        log.debug(f'attempting to add schedule {sched}')
        db = self.get_schedule_db(fs)
        sched.store_schedule(db)
        self.store_schedule_db(sched.fs)

    @updates_schedule_db
    def rm_snap_schedule(self, fs, path, repeat, start):
        db = self.get_schedule_db(fs)
        Schedule.rm_schedule(db, path, repeat, start)
