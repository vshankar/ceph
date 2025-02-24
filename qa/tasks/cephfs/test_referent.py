import logging

log = logging.getLogger(__name__)

from tasks.cephfs.cephfs_test_case import CephFSTestCase

class TestReferentInode(CephFSTestCase):
    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1

    def test_referent(self):
        """

        """

        self.mount_a.run_shell(["mkdir", "dir0"])
        self.mount_a.run_shell(["touch", "dir0/file1"])
        self.mount_a.run_shell(["ln", "dir0/file1", "dir0/hardlink_file1"])
        file_ino = self.mount_a.path_to_ino("dir0/file1")

        # write out the backtrace - this would writeout the backrace
        # of the newly introduced referent inode to the data pool.
        self.fs.mds_asok(["flush", "journal"])

        # read the primary inode
        dir_ino = self.mount_a.path_to_ino("dir0")
        file1_inode = self.fs.read_meta_inode(dir_ino, "file1")

        # read the referent inode
        referent_inode = self.fs.read_meta_inode(dir_ino, "hardlink_file1")

        self.assertFalse(file1_inode['ino'] == referent_inode['ino'])

        # the real inode should track the ereferent inode number
        self.assertIn(referent_inode['ino'], file1_inode['referent_inodes'])

    def test_referent_reintegration(self):
        """

        """

        self.mount_a.run_shell(["mkdir", "dir0"])
        self.mount_a.run_shell(["touch", "dir0/file1"])
        self.mount_a.run_shell(["ln", "dir0/file1", "dir0/hardlink_file1"])
        self.mount_a.run_shell(["ln", "dir0/file1", "dir0/hardlink_file2"])

        # remove the primary link
        self.mount_a.run_shell(["rm", "dir0/file1"])

        # verify that the refrent is now a real inode - the referent list
        # should have the other other hardlink ino number and not its own.

    def test_multiple_referent_post_reintegration(self):
        pass

    def test_rename_a_referent_dentry(self):
        pass

    def test_referent_with_mds_killpoints(self):
        pass

    def test_referent_with_snapshot(self):
        pass

    def test_referent_with_mdlog_replay(self):
        pass

    def test_referent_no_caps(self):
        pass
