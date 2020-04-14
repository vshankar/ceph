import os
import json
import time
import errno
import random
import logging
import collections

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

def extract_schedule_and_retention_spec(spec=[]):
    schedule = set([s[0] for s in spec])
    retention = set([s[1] for s in spec])
    return (schedule, retention)

def seconds_upto_next_schedule(time_from, timo):
    ts = int(time_from)
    return ((int(ts / 60) * 60) + timo) - ts

class TestSnapSchedules(CephFSTestCase):
    CLIENTS_REQUIRED = 1

    TEST_VOLUME_NAME = "snap_vol"
    TEST_DIRECTORY = "snap_test_dir15"

    def _fs_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", *args)

    def _fs_snap_schedule_cmd(self, *args):
        return self._fs_cmd("snap-schedule", "--fs={0}".format(self.volname), *args)

    def _create_or_reuse_test_volume(self):
        result = json.loads(self._fs_cmd("volume", "ls"))
        if len(result) == 0:
            self.vol_created = True
            self.volname = TestSnapSchedules.TEST_VOLUME_NAME
            self._fs_cmd("volume", "create", self.volname)
        else:
            self.volname = result[0]['name']

    def _enable_snap_schedule(self):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable", "snap_schedule")

    def _disable_snap_schedule(self):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "disable", "snap_schedule")

    def setUp(self):
        super(TestSnapSchedules, self).setUp()
        self.volname = None
        self.vol_created = False
        self._create_or_reuse_test_volume()
        self._enable_snap_schedule()

    def tearDown(self):
        if self.vol_created:
            self._delete_test_volume()
        #self._disable_snap_schedule()
        super(TestSnapSchedules, self).tearDown()

    def _schedule_to_timeout(self, schedule):
        # copied from pybind/mgr/snap_schedule/fs/schedule.py::repeat_in_s()
        mult = schedule[-1]
        period = int(schedule[0:-1])
        if mult == 'm':
            return period * 60
        elif mult == 'h':
            return period * 60 * 60
        elif mult == 'd':
            return period * 60 * 60 * 24
        elif mult == 'w':
            return period * 60 * 60 * 24 * 7
        else:
            raise RuntimeError('schedule multiplier not recognized')

    def _verify_schedule(self, dir_path, schedule_spec):
        expected_schedule, expected_retention = extract_schedule_and_retention_spec(schedule_spec)
        print("expected_schedule: {0}, expected_retention: {1}".format(expected_schedule, expected_retention))

        result = json.loads(self._fs_snap_schedule_cmd("list", dir_path))
        schedule, retention = extract_schedule_and_retention_spec(schedule_spec)
        print("schedule: {0}, retention: {1}".format(schedule, retention))

        self.assertEquals(expected_schedule, schedule)
        self.assertEquals(expected_retention, retention)

    def _verify_snapshot(self, dir_path, schedule, exec_time, existing_snap_count=0):
        snap_path = "{0}/.snap".format(dir_path)
        timo = self._schedule_to_timeout(schedule)
        wait_timo = seconds_upto_next_schedule(exec_time, timo)

        print("waiting for {0}s for snapshot...".format(wait_timo))
        time.sleep(wait_timo)

        def check_snapshot():
            snapshots = self.mount_a.ls(path=snap_path)
            return len(snapshots) == existing_snap_count + 1
        self.wait_until_true(check_snapshot, timeout=10)

    def _remove_snapshots(self, dir_path):
        snap_path = "{0}/.snap".format(dir_path)

        snapshots = self.mount_a.ls(path=snap_path)
        for snapshot in snapshots:
            snapshot_path = os.path.join(snap_path, snapshot)
            log.debug("removing snapshot: {0}".format(snapshot_path))
            self.mount_a.run_shell(["rmdir", snapshot_path])

    def test_snap_schedule(self):
        self.mount_a.run_shell(["mkdir", "-p", TestSnapSchedules.TEST_DIRECTORY])

        # set a schedule on the dir
        self._fs_snap_schedule_cmd("add", TestSnapSchedules.TEST_DIRECTORY, '2m')
        exec_time = time.time()

        # verify snapshot schedule
        self._verify_schedule(TestSnapSchedules.TEST_DIRECTORY, [['2m', '']])

        # wait for the snapshot
        self._verify_snapshot(TestSnapSchedules.TEST_DIRECTORY, '2m', exec_time)

        # remove snapshot schedule
        self._fs_snap_schedule_cmd("remove", TestSnapSchedules.TEST_DIRECTORY)
        try:
            self._fs_snap_schedule_cmd("list", TestSnapSchedules.TEST_DIRECTORY)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EPERM:
                raise RuntimeError("incorrect errno when listing a non-existing snap schedule")

        # remove all scheduled snapshots
        self._remove_snapshots(TestSnapSchedules.TEST_DIRECTORY)

        self.mount_a.run_shell(["rmdir", TestSnapSchedules.TEST_DIRECTORY])

    def test_multi_snap_schedule(self):
        self.mount_a.run_shell(["mkdir", "-p", TestSnapSchedules.TEST_DIRECTORY])

        # set schedules on the dir
        self._fs_snap_schedule_cmd("add", TestSnapSchedules.TEST_DIRECTORY, '2m')
        self._fs_snap_schedule_cmd("add", TestSnapSchedules.TEST_DIRECTORY, '3m')
        exec_time = time.time()

        # verify snapshot schedule
        self._verify_schedule(TestSnapSchedules.TEST_DIRECTORY, [['2m', ''], ['3m', '']])

        # wait for the snapshot
        self._verify_snapshot(TestSnapSchedules.TEST_DIRECTORY, '2m', exec_time)
        self._verify_snapshot(TestSnapSchedules.TEST_DIRECTORY, '3m', exec_time, existing_snap_count=1)

        # remove snapshot schedule
        self._fs_snap_schedule_cmd("remove", TestSnapSchedules.TEST_DIRECTORY)

        # remove all scheduled snapshots
        self._remove_snapshots(TestSnapSchedules.TEST_DIRECTORY)

        self.mount_a.run_shell(["rmdir", TestSnapSchedules.TEST_DIRECTORY])

    def test_snap_schedule_on_multiple_directory(self):
        test_dir0 = "{0}.0".format(TestSnapSchedules.TEST_DIRECTORY)
        test_dir1 = "{0}.1".format(TestSnapSchedules.TEST_DIRECTORY)

        self.mount_a.run_shell(["mkdir", "-p", test_dir0])
        self.mount_a.run_shell(["mkdir", "-p", test_dir1])
        exec_time = time.time()

        # set schedule on the dirs
        self._fs_snap_schedule_cmd("add", test_dir0, '1m')
        self._fs_snap_schedule_cmd("add", test_dir1, '2m')

        # veridy snapshot schedules
        self._verify_schedule(test_dir0, [['1m', '']])
        self._verify_schedule(test_dir1, [['2m', '']])

        # wait for the snapshot
        self._verify_snapshot(TestSnapSchedules.TEST_DIRECTORY, '2m', exec_time)
        self._verify_snapshot(TestSnapSchedules.TEST_DIRECTORY, '3m', exec_time)

        # remove snapshot schedule
        self._fs_snap_schedule_cmd("remove", test_dir0)
        self._fs_snap_schedule_cmd("remove", test_dir1)

        self.mount_a.run_shell(["rmdir", test_dir0])
        self.mount_a.run_shell(["rmdir", test_dir1])
