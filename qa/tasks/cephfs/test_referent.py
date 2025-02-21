import logging

log = logging.getLogger(__name__)

from tasks.cephfs.cephfs_test_case import CephFSTestCase

class TestReferentInode(CephFSTestCase):
    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1

    def test_referent(self):
        """

        """

        data_pool_name = self.fs.get_data_pool_name()

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
        log.debug(f'real inode={file1_inode}, referent_inode={referent_inode}')
