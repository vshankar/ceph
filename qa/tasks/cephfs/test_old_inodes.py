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
from pathlib import Path

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.test_volumes import TestVolumesHelper

log = logging.getLogger(__name__)


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

        # A journal trim commits dirfrags, and the dirfrag commit is what runs
        # purge_stale_snap_data().  We are measuring the in-memory peak, so
        # keep trimming out of the way for the duration of the test.
        self.config_set('mds', 'mds_log_max_segments', '1024')

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

    def _observe(self, label, paths):
        counts = self._old_inode_counts(paths)
        last_created, last_destroyed = self._snap_table()
        log.info("OLDINO %-28s old_inodes=%s last_created=%d last_destroyed=%d",
                 label, json.dumps(counts), last_created, last_destroyed)
        return counts

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

        group, subvol = self._make_subvolume()
        paths = self._paths(group, subvol)
        snapnames = []

        try:
            self._observe("baseline", paths)

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
