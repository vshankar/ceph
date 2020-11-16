import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestFSTop(CephFSTestCase):
    # TODO: invoke fstop with --selftest and verify exit code
    def test_fstop(self):
        rv = self.mount_a.run_shell(["cephfs-top", "--selftest"]).stdout.getvalue().strip()
        self.assertTrue(rv == "selftest ok")
