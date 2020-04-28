"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import errno
import sqlite3
from .fs.schedule import SnapSchedClient, Schedule
from mgr_module import MgrModule, CLIReadCommand, CLIWriteCommand
from mgr_util import CephfsConnectionException
from rados import ObjectNotFound
from threading import Event
try:
    import queue as Queue
except ImportError:
    import Queue


class Module(MgrModule):

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self._initialized = Event()
        self.client = SnapSchedClient(self)

        self._background_jobs = Queue.Queue()

    @property
    def default_fs(self):
        fs_map = self.get('fs_map')
        if fs_map['filesystems']:
            return fs_map['filesystems'][0]['mdsmap']['fs_name']
        else:
            self.log.error('No filesystem instance could be found.')
            raise CephfsConnectionException(
                -errno.ENOENT, "no filesystem found")

    def serve(self):
        self._initialized.set()

    def handle_command(self, inbuf, cmd):
        self._initialized.wait()
        return -errno.EINVAL, "", "Unknown command"

    @CLIReadCommand('fs snap-schedule ls',
                    'name=path,type=CephString,req=false '
                    'name=subvol,type=CephString,req=false '
                    'name=fs,type=CephString,req=false',
                    'List current snapshot schedules')
    def snap_schedule_ls(self, path='/', subvol=None, fs=None):
        use_fs = fs if fs else self.default_fs
        try:
            ret_scheds = self.client.list_snap_schedule(use_fs, path)
        except CephfsConnectionException as e:
            return e.to_tuple()
        return 0, ' '.join(str(ret_scheds)), ''

    @CLIReadCommand('fs snap-schedule get',
                    'name=path,type=CephString '
                    'name=subvol,type=CephString,req=false '
                    'name=fs,type=CephString,req=false',
                    'Get current snapshot schedule for <path>')
    def snap_schedule_get(self, path, subvol=None, fs=None):
        try:
            use_fs = fs if fs else self.default_fs
            sched = self.client.get_snap_schedule(use_fs, path)
        except CephfsConnectionException as e:
            return e.to_tuple()
        if not sched:
            return -1, '', f'SnapSchedule for {path} not found'
        return 0, str(sched), ''

    prune_schedule_options = ('name=keep-minutely,type=CephString,req=false '
                              'name=keep-hourly,type=CephString,req=false '
                              'name=keep-daily,type=CephString,req=false '
                              'name=keep-weekly,type=CephString,req=false '
                              'name=keep-monthly,type=CephString,req=false '
                              'name=keep-yearly,type=CephString,req=false '
                              'name=keep-last,type=CephString,req=false '
                              'name=keep-within,type=CephString,req=false')

    @CLIWriteCommand('fs snap-schedule set',
                     'name=path,type=CephString '
                     'name=snap-schedule,type=CephString '
                     'name=retention-policy,type=CephString,req=false '
                     'name=fs,type=CephString,req=false '
                     'name=subvol,type=CephString,req=false',
                     'Set a snapshot schedule for <path>')
    def snap_schedule_set(self,
                          path,
                          snap_schedule,
                          retention_policy='',
                          fs=None,
                          subvol=None):
        try:
            use_fs = fs if fs else self.default_fs
            # TODO should we allow empty retention policies?
            sched = Schedule(path, snap_schedule, retention_policy,
                             use_fs, subvol)
            # TODO allow schedules on non-existent paths?
            # self.client.validate_schedule(None, sched=sched, fs_name=fs)
            self.client.store_snap_schedule(use_fs, sched)
            suc_msg = f'Schedule set for path {path}'
        except sqlite3.IntegrityError:
            existing_sched = self.client.get_snap_schedule(use_fs, path)
            self.log.info(f'Found existing schedule {existing_sched}...updating')
            self.client.update_snap_schedule(use_fs, sched)
            suc_msg = f'Schedule set for path {path}, updated existing schedule {existing_sched}'
        except CephfsConnectionException as e:
            return e.to_tuple()
        return 0, suc_msg, ''

    @CLIWriteCommand('fs snap-schedule rm',
                     'name=path,type=CephString '
                     'name=subvol,type=CephString,req=false '
                     'name=fs,type=CephString,req=false',
                     'Remove a snapshot schedule for <path>')
    def snap_schedule_rm(self, path, subvol=None, fs=None):
        try:
            use_fs = fs if fs else self.default_fs
            self.client.rm_snap_schedule(use_fs, path)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except ObjectNotFound as e:
            return e.errno, '', 'SnapSchedule for {} not found'.format(path)
        return 0, 'Schedule removed for path {}'.format(path), ''
