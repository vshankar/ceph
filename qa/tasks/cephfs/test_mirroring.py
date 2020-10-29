import os
import json
import errno
import logging
import time

from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)

class TestMirroring(CephFSTestCase):
    MDSS_REQUIRED = 5
    CLIENTS_REQUIRED = 2
    REQUIRE_BACKUP_FILESYSTEM = True

    MODULE_NAME = "mirroring"

    def setUp(self):
        super(TestMirroring, self).setUp()
        self.primary_fs_name = self.fs.name
        self.primary_fs_id = self.fs.id
        self.secondary_fs_name = self.backup_fs.name
        self.enable_mirroring_module()

    def tearDown(self):
        self.disable_mirroring_module()
        super(TestMirroring, self).tearDown()

    def enable_mirroring_module(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "enable", TestMirroring.MODULE_NAME)

    def disable_mirroring_module(self):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("mgr", "module", "disable", TestMirroring.MODULE_NAME)

    def enable_mirroring(self, fs_name, fs_id):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "enable", fs_name)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {})
        self.assertTrue(res['snap_dirs']['dir_count'] == 0)

    def disable_mirroring(self, fs_name, fs_id):
        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "disable", fs_name)
        time.sleep(10)
        # verify via asok
        try:
            self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                       'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        except CommandFailedError as ce:
            pass
        else:
            raise RuntimeError('expected admin socket to be unavailable')

    def peer_add(self, fs_name, fs_id, peer_spec, remote_fs_name=None):
        if remote_fs_name:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec, remote_fs_name)
        else:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_add", fs_name, peer_spec)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        peer_uuid = self.get_peer_uuid(peer_spec)
        self.assertTrue(peer_uuid in res['peers'])
        client_name = res['peers'][peer_uuid]['remote']['client_name']
        cluster_name = res['peers'][peer_uuid]['remote']['cluster_name']
        self.assertTrue(peer_spec == f'{client_name}@{cluster_name}')
        if remote_fs_name:
            self.assertTrue(self.secondary_fs_name == res['peers'][peer_uuid]['remote']['fs_name'])
        else:
            self.assertTrue(self.fs_name == res['peers'][peer_uuid]['remote']['fs_name'])

    def peer_remove(self, fs_name, fs_id, peer_spec):
        peer_uuid = self.get_peer_uuid(peer_spec)
        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_remove", fs_name, peer_uuid)
        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        self.assertTrue(res['peers'] == {} and res['snap_dirs']['dir_count'] == 0)

    def add_directory(self, fs_name, fs_id, dir_name):
        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "add", fs_name, dir_name)

        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        new_dir_count = res['snap_dirs']['dir_count']
        log.debug(f'new dir_count={new_dir_count}')
        self.assertTrue(new_dir_count > dir_count)

    def remove_directory(self, fs_name, fs_id, dir_name):
        # get initial dir count
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        dir_count = res['snap_dirs']['dir_count']
        log.debug(f'initial dir_count={dir_count}')

        self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "remove", fs_name, dir_name)

        time.sleep(10)
        # verify via asok
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        new_dir_count = res['snap_dirs']['dir_count']
        log.debug(f'new dir_count={new_dir_count}')
        self.assertTrue(new_dir_count < dir_count)

    def check_peer_status(self, fs_name, fs_id, peer_spec, dir_name, expected_snap_name,
                          expected_snap_count=1, expected_delete_count=0):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        self.assertTrue(dir_name in res)
        self.assertTrue(res[dir_name]['last_synced_snap']['name'] == expected_snap_name)
        self.assertTrue(res[dir_name]['snaps_synced'] == expected_snap_count)
        self.assertTrue(res[dir_name]['snaps_deleted'] == expected_delete_count)

    def check_peer_snap_in_progress(self, fs_name, fs_id,
                                    peer_spec, dir_name, snap_name):
        peer_uuid = self.get_peer_uuid(peer_spec)
        res = self.mirror_daemon_command(f'peer status for fs: {fs_name}',
                                         'fs', 'mirror', 'peer', 'status',
                                         f'{fs_name}@{fs_id}', peer_uuid)
        self.assertTrue('syncing' == res[dir_name]['state'])
        self.assertTrue(res[dir_name]['current_sycning_snap']['name'] == snap_name)

    def verify_snapshot(self, dir_name, snap_name):
        snap_list = self.mount_b.ls(path=f'{dir_name}/.snap')
        self.assertTrue(snap_name in snap_list)

        source_res = self.mount_a.dir_checksum(path=f'{dir_name}/.snap/{snap_name}')
        log.debug(f'source snapshot checksum {snap_name} {source_res}')

        dest_res = self.mount_b.dir_checksum(path=f'{dir_name}/.snap/{snap_name}')
        log.debug(f'destination snapshot checksum {snap_name} {dest_res}')
        self.assertTrue(source_res == dest_res)

    def get_peer_uuid(self, peer_spec):
        status = self.fs.status()
        fs_map = status.get_fsmap_byname(self.primary_fs_name)
        peers = fs_map['mirror_info']['peers']
        for peer_uuid, mirror_info in peers.items():
            client_name = mirror_info['remote']['client_name']
            cluster_name = mirror_info['remote']['cluster_name']
            remote_peer_spec = f'{client_name}@{cluster_name}'
            if peer_spec == remote_peer_spec:
                return peer_uuid
        return None

    def get_daemon_admin_socket(self):
        """overloaded by teuthology override (fs/mirror/clients/mirror.yaml)"""
        return "/var/run/ceph/cephfs-mirror.asok"

    def get_mirror_daemon_pid(self):
        """pid file overloaded in fs/mirror/clients/mirror.yaml"""
        return self.mount_a.run_shell(['cat', '/var/run/ceph/cephfs-mirror.pid']).stdout.getvalue().strip()

    def get_mirror_rados_addr(self, fs_name, fs_id):
        """return the rados addr used by cephfs-mirror instance"""
        res = self.mirror_daemon_command(f'mirror status for fs: {fs_name}',
                                         'fs', 'mirror', 'status', f'{fs_name}@{fs_id}')
        return res['rados_inst']

    def get_blocklisted_instances(self):
        return json.loads(self.mds_cluster.mon_manager.raw_cluster_cmd(
            "osd", "dump", "--format=json-pretty"))['blocklist']

    def mirror_daemon_command(self, cmd_label, *args):
        asok_path = self.get_daemon_admin_socket()
        try:
            # use mount_a's remote to execute command
            p = self.mount_a.client_remote.run(args=
                     ['ceph', '--admin-daemon', asok_path] + list(args),
                     stdout=StringIO(), stderr=StringIO(), timeout=30,
                     check_status=True, label=cmd_label)
            p.wait()
        except CommandFailedError as ce:
            log.warn(f'mirror daemon command with label "{cmd_label}" failed: {ce}')
            raise
        res = p.stdout.getvalue().strip()
        log.debug(f'command returned={res}')
        return json.loads(res)

    def test_basic_mirror_commands(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_mirror_peer_commands(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        # add peer
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)
        # remove peer
        self.peer_remove(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph")

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_mirror_disable_with_peer(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        # add peer
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_matching_peer(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        try:
            self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError('invalid errno when adding a matching remote peer')
        else:
            raise RuntimeError('adding a peer matching local spec should fail')

        # verify via asok -- nothing should get added
        res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                         'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
        self.assertTrue(res['peers'] == {})

        # and explicitly specifying the spec (via filesystem name) should fail too
        try:
            self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.primary_fs_name)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError('invalid errno when adding a matching remote peer')
        else:
            raise RuntimeError('adding a peer matching local spec should fail')

        # verify via asok -- nothing should get added
        res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                         'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
        self.assertTrue(res['peers'] == {})

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_mirror_peer_add_existing(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        # add peer
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        # adding the same peer should be idempotent
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        # remove peer
        self.peer_remove(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph")

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_peer_commands_with_mirroring_disabled(self):
        # try adding peer when mirroring is not enabled
        try:
            self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a peer')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected peer_add to fail')

        # try removing peer
        try:
            self.mgr_cluster.mon_manager.raw_cluster_cmd("fs", "snapshot", "mirror", "peer_remove", self.primary_fs_name, 'dummy-uuid')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when removing a peer')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected peer_remove to fail')

    def test_add_directory_with_mirroring_disabled(self):
        # try adding a directory when mirroring is not enabled
        try:
            self.add_directory(self.primary_fs_name, self.primary_fs_id, "/d1")
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')

    def test_directory_commands(self):
        self.mount_a.run_shell(["mkdir", "d1"])
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d1')
        try:
            self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d1')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EEXIST:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when re-adding a directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.remove_directory(self.primary_fs_name, self.primary_fs_id, '/d1')
        try:
            self.remove_directory(self.primary_fs_name, self.primary_fs_id, '/d1')
        except CommandFailedError as ce:
            if ce.exitstatus not in (errno.ENOENT, errno.EINVAL):
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when re-deleting a directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory removal to fail')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.mount_a.run_shell(["rmdir", "d1"])

    def test_add_non_existing_directory(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        try:
            self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d1')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.ENOENT:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a non-existing directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_add_relative_directory_path(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        try:
            self.add_directory(self.primary_fs_name, self.primary_fs_id, './d1')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a relative path dir')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_add_non_directory(self):
        self.mount_a.run_shell(["touch", "test"])
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        try:
            self.add_directory(self.primary_fs_name, self.primary_fs_id, '/test')
        except CommandFailedError as ce:
            if ce.exitstatus != errno.EINVAL:
                raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a non directory')
        else:
            raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.mount_a.run_shell(["rm", "test"])

    def test_add_directory_path_normalization(self):
        self.mount_a.run_shell(["mkdir", "-p", "d1/d2/d3"])
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d1/d2/d3')
        def check_add_command_failure(dir_path):
            try:
                self.add_directory(self.primary_fs_name, self.primary_fs_id, dir_path)
            except CommandFailedError as ce:
                if ce.exitstatus != errno.EEXIST:
                    raise RuntimeError(-errno.EINVAL, 'incorrect error code when re-adding a directory')
            else:
                raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')

        # everything points for /d1/d2/d3
        check_add_command_failure('/d1/d2/././././././d3')
        check_add_command_failure('/d1/d2/././././././d3//////')
        check_add_command_failure('/d1/d2/../d2/././././d3')
        check_add_command_failure('/././././d1/./././d2/./././d3//////')
        check_add_command_failure('/./d1/./d2/./d3/../../../d1/d2/d3')

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.mount_a.run_shell(["rm", "-rf", "d1"])

    def test_add_ancestor_and_child_directory(self):
        self.mount_a.run_shell(["mkdir", "-p", "d1/d2/d3"])
        self.mount_a.run_shell(["mkdir", "-p", "d1/d4"])
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d1/d2/')
        def check_add_command_failure(dir_path):
            try:
                self.add_directory(self.primary_fs_name, self.primary_fs_id, dir_path)
            except CommandFailedError as ce:
                if ce.exitstatus != errno.EINVAL:
                    raise RuntimeError(-errno.EINVAL, 'incorrect error code when adding a directory')
            else:
                raise RuntimeError(-errno.EINVAL, 'expected directory add to fail')

        # cannot add ancestors or a subtree for an existing directory
        check_add_command_failure('/')
        check_add_command_failure('/d1')
        check_add_command_failure('/d1/d2/d3')

        # obviously, one can add a non-ancestor or non-subtree
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d1/d4/')

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.mount_a.run_shell(["rm", "-rf", "d1"])

    def test_cephfs_mirror_blocklist(self):
        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)

        # add peer
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                         'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
        peers_1 = set(res['peers'])

        # fetch rados address for blacklist check
        rados_inst = self.get_mirror_rados_addr(self.primary_fs_name, self.primary_fs_id)

        # simulate non-responding mirror daemon by sending SIGSTOP
        pid = self.get_mirror_daemon_pid()
        log.debug(f'SIGSTOP to cephfs-mirror pid {pid}')
        self.mount_a.run_shell(['kill', '-SIGSTOP', pid])

        # wait for blocklist timeout -- the manager module would blocklist
        # the mirror daemon
        time.sleep(40)

        # wake up the mirror daemon -- at this point, the daemon should know
        # that it has been blocklisted
        log.debug(f'SIGCONT to cephfs-mirror')
        self.mount_a.run_shell(['kill', '-SIGCONT', pid])

        # check if the rados addr is blocklisted
        blocklist = self.get_blocklisted_instances()
        self.assertTrue(rados_inst in blocklist)

        # wait enough so that the mirror daemon restarts blocklisted instances
        time.sleep(40)
        rados_inst_new = self.get_mirror_rados_addr(self.primary_fs_name, self.primary_fs_id)

        # and we should get a new rados instance
        self.assertTrue(rados_inst != rados_inst_new)

        # along with peers that were added
        res = self.mirror_daemon_command(f'mirror status for fs: {self.primary_fs_name}',
                                         'fs', 'mirror', 'status', f'{self.primary_fs_name}@{self.primary_fs_id}')
        peers_2 = set(res['peers'])
        self.assertTrue(peers_1, peers_2)

        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_cephfs_mirror_stats(self):
        log.debug('reconfigure client auth caps')
        self.mds_cluster.mon_manager.raw_cluster_cmd_result(
            'auth', 'caps', "client.{0}".format(self.mount_b.client_id),
                'mds', 'allow rw',
                'mon', 'allow r',
                'osd', 'allow rw pool={0}, allow rw pool={1}'.format(
                    self.backup_fs.get_data_pool_name(), self.backup_fs.get_data_pool_name()))

        log.debug(f'mounting filesystem {self.secondary_fs_name}')
        self.mount_b.umount_wait()
        self.mount_b.mount(cephfs_name=self.secondary_fs_name)

        # create a bunch of files in a directory to snap
        self.mount_a.run_shell(["mkdir", "d0"])
        self.mount_a.create_n_files('d0/file', 50, sync=True)

        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d0')
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        # take a snapshot
        self.mount_a.run_shell(["mkdir", "d0/.snap/snap0"])

        time.sleep(30)
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               "client.mirror_remote@ceph", '/d0', 'snap0', expected_snap_count=1)
        self.verify_snapshot('d0', 'snap0')

        # some more IO
        self.mount_a.run_shell(["mkdir", "d0/d00"])
        self.mount_a.run_shell(["mkdir", "d0/d01"])

        self.mount_a.create_n_files('d0/d00/more_file', 20, sync=True)
        self.mount_a.create_n_files('d0/d01/some_more_file', 75, sync=True)

        # take another snapshot
        self.mount_a.run_shell(["mkdir", "d0/.snap/snap1"])

        time.sleep(60)
        self.check_peer_status(self.primary_fs_name, self.primary_fs_id,
                               "client.mirror_remote@ceph", '/d0', 'snap1', expected_snap_count=2)
        self.verify_snapshot('d0', 'snap1')

        # delete a snapshot
        self.mount_a.run_shell(["rmdir", "d0/.snap/snap0"])

        time.sleep(10)
        snap_list = self.mount_b.ls(path='d0/.snap')
        self.assertTrue('snap0' not in snap_list)

        # rename a snapshot
        self.mount_a.run_shell(["mv", "d0/.snap/snap1", "d0/.snap/snap2"])

        time.sleep(10)
        snap_list = self.mount_b.ls(path='d0/.snap')
        self.assertTrue('snap1' not in snap_list)
        self.assertTrue('snap2' in snap_list)

        self.remove_directory(self.primary_fs_name, self.primary_fs_id, '/d0')
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)

    def test_cephfs_mirror_cancel_sync(self):
        log.debug('reconfigure client auth caps')
        self.mds_cluster.mon_manager.raw_cluster_cmd_result(
            'auth', 'caps', "client.{0}".format(self.mount_b.client_id),
                'mds', 'allow rw',
                'mon', 'allow r',
                'osd', 'allow rw pool={0}, allow rw pool={1}'.format(
                    self.backup_fs.get_data_pool_name(), self.backup_fs.get_data_pool_name()))

        log.debug(f'mounting filesystem {self.secondary_fs_name}')
        self.mount_b.umount_wait()
        self.mount_b.mount(cephfs_name=self.secondary_fs_name)

        # create a bunch of files in a directory to snap
        self.mount_a.run_shell(["mkdir", "d0"])
        for i in range(8):
            filename = f'file.{i}'
            self.mount_a.write_n_mb(os.path.join('d0', filename), 1024)

        self.enable_mirroring(self.primary_fs_name, self.primary_fs_id)
        self.add_directory(self.primary_fs_name, self.primary_fs_id, '/d0')
        self.peer_add(self.primary_fs_name, self.primary_fs_id, "client.mirror_remote@ceph", self.secondary_fs_name)

        # take a snapshot
        self.mount_a.run_shell(["mkdir", "d0/.snap/snap0"])

        time.sleep(10)
        self.check_peer_snap_in_progress(self.primary_fs_name, self.primary_fs_id,
                                         "client.mirror_remote@ceph", '/d0', 'snap0')

        self.remove_directory(self.primary_fs_name, self.primary_fs_id, '/d0')

        snap_list = self.mount_b.ls(path='d0/.snap')
        self.assertTrue('snap0' not in snap_list)
        self.disable_mirroring(self.primary_fs_name, self.primary_fs_id)
