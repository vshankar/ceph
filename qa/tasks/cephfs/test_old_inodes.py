"""
Investigation harness for old_inodes growth on the ancestors of subvolumes.

Trackers:
  #67102 - mds: read-only file system due to large old_inodes
  #76455 - src/mds/CDir.cc: FAILED ceph_assert(header || !_set.empty() || !_rm.empty())
  #70794 - mds: use subvolume directory snap realm for COW'ing old_inodes

CHARACTERIZATION test: it asserts the CURRENT behaviour, which is the buggy
behaviour.  It is meant to answer one question and then be inverted once a
real fix lands.

Why the buildup is expected, so the assertions below are readable:

  * MDCache::predirty_journal_parents() calls CInode::pre_cow_old_inode() on
    every ancestor up to root (MDCache.cc:2392).
  * With mds_use_global_snaprealm_seq_for_subvol=true (the DEFAULT),
    pre_cow_old_inode() takes `follows` from the global snaprealm seq, which
    is max(last_created, last_destroyed) over the WHOLE filesystem
    (SnapRealm.cc:128-133).  So a snapshot created anywhere makes every
    dirtied ancestor mint an old_inode.
  * A subvolume's snapids never enter /volumes' snaprealm - realms inherit
    downward, not upward - so every one of those old_inodes preserves a
    version that no snapshot can ever reference.

Run against vstart with:

  cd build && ../src/vstart.sh -n -d --nolockdep
  python3 ../qa/tasks/vstart_runner.py \
      tasks.cephfs.test_old_inodes.TestOldInodeGrowth
"""

import json
import logging
import collections
from io import BytesIO
from pathlib import Path

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.test_volumes import TestVolumesHelper

log = logging.getLogger(__name__)

# root inode 0x1 has a single dirfrag; /volumes' dentry lives in it, so
# this object's fnode carries the snap_purged_thru that gates the purge
# of /volumes' old_inodes.
ROOT_DIRFRAG_OBJECT = "1.00000000"


class TestOldInodeGrowth(TestVolumesHelper):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    # 90 snapshots on one subvolume, under mds_max_snaps_per_dir (default 100)
    SNAPS_PER_BATCH = 10
    BATCHES = 9

    def setUp(self):
        super(TestOldInodeGrowth, self).setUp()

        # rstat propagation up the tree is throttled by mds_dirstat_min_interval
        # (default 1s).  With the throttle on, whether a given ancestor gets
        # CoW'd for a given snapshot is a race; turn it off so one snapshot
        # means one propagation attempt.
        self.config_set('mds', 'mds_dirstat_min_interval', '0')

        # NOTE: journal trim policy is set per-test, not here.  A journal
        # trim commits dirfrags, and the dirfrag commit is the only thing that
        # runs purge_stale_snap_data() - so whether trimming is suppressed or
        # forced is the independent variable, not a fixture detail.

    # ------------------------------------------------------------------
    # observation helpers
    # ------------------------------------------------------------------

    def _set_global_seq_config(self, enabled):
        self.config_set('mds', 'mds_use_global_snaprealm_seq_for_subvol',
                        enabled)
        self.assertEqual(
            self.config_get('mds', 'mds_use_global_snaprealm_seq_for_subvol'),
            'true' if enabled else 'false')

    def _paths(self, group, subvol):
        """Ordered subvol -> root chain, as filesystem-absolute paths."""
        sv = Path(self._fs_cmd("subvolume", "getpath", self.volname,
                               subvol, group).strip())
        # sv == /volumes/<group>/<subvol>/<uuid>
        return collections.OrderedDict([
            ("subvol", sv.parent),
            ("group", sv.parent.parent),
            ("volumes", sv.parent.parent.parent),
            ("root", Path("/")),
        ])

    def _ino(self, path):
        rel = str(path).lstrip("/") or "."
        return self.mount_a.path_to_ino(rel)

    def _old_inode_counts(self, paths):
        """
        len(old_inodes) for each inode in the chain.

        Reads the MDS's IN-MEMORY state on purpose: purge_stale_snap_data()
        runs during the dirfrag commit (CDir::_parse_dentry), so flushing the
        journal before measuring would hide the peak we are hunting.
        """
        counts = collections.OrderedDict()
        for label, path in paths.items():
            try:
                dump = self.fs.mds_asok(['dump', 'inode', hex(self._ino(path))])
            except CommandFailedError:
                counts[label] = None       # not in cache
                continue
            counts[label] = len(dump["old_inodes"]) if dump else None
        return counts

    def _snap_table(self):
        d = self.fs.mds_asok(['dump', 'snaps', '--server'])
        return int(d['last_created']), int(d['last_destroyed'])

    def _observe(self, label, paths, gate=False):
        counts = self._old_inode_counts(paths)
        last_created, last_destroyed = self._snap_table()
        suffix = ""
        if gate:
            purged_thru = self._snap_purged_thru()
            if purged_thru is None:
                suffix = " snap_purged_thru=?"
            else:
                suffix = (" snap_purged_thru=%d gate=%s"
                          % (purged_thru,
                             "OPEN" if purged_thru < last_destroyed
                             else "SHUT"))
        log.info("OLDINO %-28s old_inodes=%s last_created=%d "
                 "last_destroyed=%d%s",
                 label, json.dumps(counts), last_created, last_destroyed,
                 suffix)
        return counts

    def _snap_purged_thru(self):
        """
        snap_purged_thru out of the ROOT dirfrag's on-disk fnode.

        This is the left-hand side of the purge gate at CDir.cc:2615.

        CAREFUL: the gate tests the IN-MEMORY fnode.  _omap_fetched() sets
        snap_purged_thru in memory and only calls log_mark_dirty()
        (CDir.cc:2119,2301); the value does not reach RADOS until the next
        commit.  CDir::dump does not expose the field, so there is no asok
        route to the live value - which means this on-disk read is only
        meaningful AFTER a commit has flushed the fnode.  Read it any earlier
        and it reports the previous value.

        Returns None if the object or ceph-dencoder is unavailable; the gate
        reading is diagnostic, never load bearing for an assertion.
        """
        try:
            blob = self.fs.radosmo(["getomapheader", ROOT_DIRFRAG_OBJECT, "-"],
                                   stdout=BytesIO())
            fnode = json.loads(self.fs.dencoder('fnode_t', blob))
            return int(fnode['snap_purged_thru'])
        except Exception as e:
            log.warning("could not read snap_purged_thru from %s: %s",
                        ROOT_DIRFRAG_OBJECT, e)
            return None

    def _commit_dirfrags(self):
        """
        Force the dirfrag commit that gives purge_stale_snap_data() a chance
        to run.  'flush journal' reaches CDir::commit() by exactly the same
        path as ordinary trimming: MDSRank.cc:150 trim_to() -> expire_segments()
        -> LogSegment::try_to_expire() (journal.cc:126) -> CDir::commit().

        Flushed twice on purpose: the first flush commits (and purges), but the
        flush itself drives predirty_journal_parents() and can immediately
        re-CoW.  Same reasoning as TestVolumesHelper._verify_old_inodes().
        """
        self.fs.mds_asok(["flush", "journal"])
        self.fs.mds_asok(["flush", "journal"])

    def _restart_mds_to_shut_gate(self):
        """
        Force root's dirfrag to be re-fetched from RADOS, which is the only
        thing that advances snap_purged_thru (CDir.cc:2119).

        Why this is needed: CephFSTestCase.setUp() destroys and recreates the
        filesystem for every test (cephfs_test_case.py:154).  On a brand new
        fs, root's dirfrag is built in memory by mkfs and is already complete,
        so MDCache::open_root() never takes the rootdir->fetch() branch
        (MDCache.cc:743).  snap_purged_thru therefore stays at its default 0
        while last_destroyed sits at its birth value 1 (SnapServer.cc:47,74),
        and the gate `0 < 1` is OPEN - which is NOT the state a long-lived
        cluster is in.

        Flushing first matters: it commits and trims the log so that replay
        does not simply repopulate root's dirfrag from the journal, which
        would again skip the fetch.
        """
        self._commit_dirfrags()

        self.fs.fail()
        self.mount_a.umount_wait(force=True)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        # _omap_fetched() only dirtied the fnode in memory (CDir.cc:2119,2301).
        # Commit so the new watermark actually lands on disk, otherwise
        # _snap_purged_thru() returns the pre-restart value and we misread the
        # gate as OPEN while the MDS is really treating it as SHUT.
        self._commit_dirfrags()
        log.info("OLDINO post-restart snap_purged_thru=%s",
                 self._snap_purged_thru())

    def _assert_gate(self, expected_open):
        """
        Assert the purge gate is in the state this test needs BEFORE drawing
        any conclusion from it.  A test that silently runs in the wrong gate
        state produces a confidently wrong answer.
        """
        purged_thru = self._snap_purged_thru()
        _, last_destroyed = self._snap_table()
        if purged_thru is None:
            self.skipTest("could not read snap_purged_thru from %s; cannot "
                          "establish the gate state" % ROOT_DIRFRAG_OBJECT)
        is_open = purged_thru < last_destroyed
        log.info("OLDINO GATE snap_purged_thru=%d last_destroyed=%d -> %s "
                 "(wanted %s)", purged_thru, last_destroyed,
                 "OPEN" if is_open else "SHUT",
                 "OPEN" if expected_open else "SHUT")
        self.assertEqual(
            is_open, expected_open,
            "precondition not met: wanted the purge gate %s but "
            "snap_purged_thru=%d, last_destroyed=%d gives %s. "
            "If SHUT was wanted and this says OPEN, either root's dirfrag was "
            "never re-fetched on restart (MDCache.cc:743 found it already "
            "complete, e.g. journal replay repopulated it), or the fetched "
            "watermark reached memory but not disk (CDir.cc:2301)."
            % ("OPEN" if expected_open else "SHUT", purged_thru,
               last_destroyed, "OPEN" if is_open else "SHUT"))
        return purged_thru, last_destroyed

    def _snapshot_and_commit_loop(self, group, subvol, snapnames):
        """
        Take snapshots in batches, forcing a dirfrag commit after each batch,
        and record /volumes before and after each commit.  Returns
        (pre_series, post_series, root_post_series).
        """
        paths = self._paths(group, subvol)
        pre_series, post_series, root_post_series = [], [], []

        for b in range(self.BATCHES):
            names = ["s_%d_%d" % (b, i) for i in range(self.SNAPS_PER_BATCH)]
            for name in names:
                self._fs_cmd("subvolume", "snapshot", "create", self.volname,
                             subvol, name, group)
            snapnames.extend(names)
            taken = (b + 1) * self.SNAPS_PER_BATCH

            pre = self._observe("%d snaps, pre-commit" % taken, paths)
            self._commit_dirfrags()
            post = self._observe("%d snaps, post-commit" % taken, paths,
                                 gate=True)

            pre_series.append(self._volumes_count(pre))
            post_series.append(self._volumes_count(post))
            root_post_series.append(post["root"])

        log.info("OLDINO SERIES /volumes pre-commit : %s", pre_series)
        log.info("OLDINO SERIES /volumes post-commit: %s", post_series)
        log.info("OLDINO SERIES root     post-commit: %s", root_post_series)
        return pre_series, post_series, root_post_series

    def _assert_root_was_reclaimed(self, root_post_series):
        """
        Control: root accumulates at the same rate as /volumes but reclaims by
        a different route.  A dirty base inode goes to CInode::store()
        (journal.cc:151), which purges UNCONDITIONALLY (CInode.cc:1267-1268).
        If root does not come back down, the commits are not happening and
        nothing else in the run is interpretable.
        """
        for i, n in enumerate(root_post_series):
            self.assertIsNotNone(n, "root not in cache at batch %d" % i)
            self.assertLessEqual(
                n, 2,
                "root still has %d old_inodes after a commit (series=%s). "
                "CInode::store() purges unconditionally (CInode.cc:1268), so "
                "either the flush did not commit anything or root's realm is "
                "not empty - the rest of this test is not interpretable until "
                "that is explained." % (n, root_post_series))

    def _volumes_count(self, counts):
        """old_inodes on /volumes, asserting the inode was actually cached."""
        self.assertIsNotNone(
            counts["volumes"],
            "/volumes was not in the MDS cache, cannot measure old_inodes "
            "(counts=%s)" % counts)
        return counts["volumes"]

    # ------------------------------------------------------------------
    # fixture
    # ------------------------------------------------------------------

    def _make_subvolume(self):
        group = self._gen_subvol_grp_name()
        subvol = self._gen_subvol_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolume", "create", self.volname, subvol, group,
                     "--mode=777")
        return group, subvol

    def _cleanup(self, group, subvol, snapnames):
        for name in snapnames:
            try:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             subvol, name, group, "--force")
            except CommandFailedError:
                pass
        self._fs_cmd("subvolume", "rm", self.volname, subvol, group, "--force")
        self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")
        self._wait_for_trash_empty()

    # ------------------------------------------------------------------
    # A: does the buildup happen with subvolume snapshots alone?
    # ------------------------------------------------------------------

    def test_a_growth_with_subvolume_snapshots_only(self):
        """
        Pure subvolume use case.  Subvolume snapshots only, nothing snapshotted
        outside /volumes/<group>/<subvol>, and NOTHING ever deleted.  Default
        config (mds_use_global_snaprealm_seq_for_subvol = true).

        Claim under test: old_inodes on /volumes grows roughly linearly with
        the number of subvolume snapshots, even though /volumes' snaprealm can
        never reference any of them.

        PASS => subvolume snapshots alone are sufficient to drive the buildup,
                with no snapshot anywhere outside the subvolume.
        FAIL => either nothing accumulates (the CoW does not reach /volumes),
                or something reclaims mid-run (the series dips).  Both are
                informative; read the OLDINO lines either way.
        """
        self._set_global_seq_config(True)
        # suppress trimming: we want the in-memory peak with no commit at all
        self.config_set('mds', 'mds_log_max_segments', '1024')

        group, subvol = self._make_subvolume()
        paths = self._paths(group, subvol)
        snapnames = []

        try:
            self._observe("baseline", paths, gate=True)

            series = []
            for b in range(self.BATCHES):
                names = ["s_%d_%d" % (b, i)
                         for i in range(self.SNAPS_PER_BATCH)]
                for name in names:
                    self._fs_cmd("subvolume", "snapshot", "create",
                                 self.volname, subvol, name, group)
                snapnames.extend(names)
                taken = (b + 1) * self.SNAPS_PER_BATCH
                counts = self._observe("after %d snapshots" % taken, paths)
                series.append(self._volumes_count(counts))

            total = self.BATCHES * self.SNAPS_PER_BATCH
            log.info("OLDINO SERIES /volumes old_inodes after each batch of "
                     "%d snapshots: %s", self.SNAPS_PER_BATCH, series)

            # Nothing should reclaim while we are only creating: the purge runs
            # from the dirfrag commit and is gated on
            # snap_purged_thru < last_destroyed (CDir.cc:2615), and we have
            # never deleted a snapshot.
            for prev, cur in zip(series, series[1:]):
                self.assertGreaterEqual(
                    cur, prev,
                    "old_inodes on /volumes went DOWN during a create-only "
                    "phase (series=%s). Something committed the dirfrag and "
                    "purged; check whether a journal trim slipped through "
                    "despite mds_log_max_segments." % series)

            self.assertGreaterEqual(
                series[-1], total // 2,
                "expected /volumes to accumulate on the order of one old_inode "
                "per subvolume snapshot (%d taken), got %d (series=%s)"
                % (total, series[-1], series))
        finally:
            self._cleanup(group, subvol, snapnames)

    # ------------------------------------------------------------------
    # B: does it survive a normally-trimming journal?
    #
    # Two arms running the identical snapshot loop, differing only in the
    # state of the purge gate at CDir.cc:2615.  Comparing them demonstrates
    # the gate directly instead of inferring it.
    # ------------------------------------------------------------------

    def _run_arm(self, shut_the_gate):
        self._set_global_seq_config(True)
        # trim as aggressively as the MDS allows (min is 8), so this is the
        # shape a live cluster has rather than (A)'s suppressed-trim fixture
        self.config_set('mds', 'mds_log_max_segments', '8')

        group, subvol = self._make_subvolume()
        snapnames = []

        try:
            if shut_the_gate:
                self._restart_mds_to_shut_gate()
            self._assert_gate(expected_open=not shut_the_gate)

            self._observe("baseline", self._paths(group, subvol), gate=True)
            pre, post, root_post = self._snapshot_and_commit_loop(
                group, subvol, snapnames)
            self._assert_root_was_reclaimed(root_post)
            return pre, post
        finally:
            self._cleanup(group, subvol, snapnames)

    def test_b1_gate_shut_growth_survives_commits(self):
        """
        Gate SHUT arm - the production regime.

        An MDS restart re-fetches root's dirfrag, so _omap_fetched() sets
        snap_purged_thru = last_destroyed (CDir.cc:2119).  Nothing is ever
        deleted here, so last_destroyed never moves past that and the gate
        stays shut for the whole run.

        Claim: with the gate shut, committing the dirfrag over and over
        reclaims nothing, and /volumes keeps climbing.  Trimming does not
        save you.

        PASS => the buildup is real on a live cluster; #67102 is reachable
                with subvolume snapshots alone.
        FAIL => /volumes was reclaimed even with the gate shut, so the gate is
                not what governs this and the model is wrong.
        """
        pre, post = self._run_arm(shut_the_gate=True)
        self.assertGreaterEqual(
            post[-1], self.BATCHES * self.SNAPS_PER_BATCH // 2,
            "/volumes old_inodes did NOT survive repeated dirfrag commits "
            "with the purge gate SHUT (post=%s, pre=%s). The gate is not what "
            "governs reclaim here." % (post, pre))

    def test_b2_gate_open_growth_is_reclaimed(self):
        """
        Gate OPEN arm - the control.

        No MDS restart, so on this freshly created filesystem root's dirfrag
        was never fetched and snap_purged_thru is still 0 while last_destroyed
        is 1: the gate is open.

        Claim: with the gate open, every commit purges with
        snaps = root_realm->get_snaps() = {} - an empty set makes every
        old_inode stale (CInode.cc:3268-3273) - so /volumes is driven back to
        zero on each commit and never accumulates.

        Together with (b1) this isolates the gate as the single variable that
        decides whether the buildup happens.
        """
        pre, post = self._run_arm(shut_the_gate=False)
        self.assertLessEqual(
            max(post), 2,
            "/volumes retained old_inodes across commits even with the purge "
            "gate OPEN (post=%s, pre=%s). Either the purge is not running or "
            "root's realm snap set is not empty." % (post, pre))
        self.assertGreater(
            max(pre), 2,
            "/volumes never accumulated even before a commit (pre=%s); the "
            "CoW is not happening at all, so this arm proves nothing." % pre)
