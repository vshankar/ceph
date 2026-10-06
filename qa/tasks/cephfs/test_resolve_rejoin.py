"""
Exercise MDS replay -> resolve -> reconnect -> rejoin with a namespace whose
auth is split across three ranks, and verify that recovery leaves consistent
auth/replica state behind.

Namespace (pins give the subtree layout; inode vs. dirfrag auth differ at
each pinned directory):

  /                                   rank 0
  |-- data/            (pin 0)        rank 0
  |   |-- f1..fN
  |   `-- d/
  `-- home/                           inode auth rank 0
      `-- [home dirfrag] (pin 1)      rank 1
          |-- u1/
          |   |-- a.txt, f1..fN
          |   `-- lnk  -> hard link to home/big/far/zebra (inode auth rank 2)
          `-- big/                    inode auth rank 1
              `-- far/                inode auth rank 1
                  `-- [far dirfrag] (pin 2)   rank 2
                      |-- zebra, g1..gN
                      `-- sub/

The workload also does cross-rank renames, a cross-rank hard link and a
snapshot of /home, so that every rank holds replicas of the others' objects,
journals peer updates, and carries dirty scatterlock (dirstat/rstat) state
for split-auth directories.

After recovery, the cache of every rank is dumped and cross-checked:
every non-auth inode/dirfrag a rank holds must be registered in its auth's
replica_map with the same nonce. The dumps and subtree maps are logged so
the debug MDS logs can be correlated with them.

Related: https://tracker.ceph.com/issues/64717
"""

import json
import logging
import time

from teuthology import contextutil

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

NFILES = 64

EXPECTED_SUBTREES = [('/data', 0), ('/home', 1), ('/home/big/far', 2)]


class TestResolveRejoin(CephFSTestCase):
    # 3 active ranks + 1 standby to take over a failed rank.
    MDSS_REQUIRED = 4
    CLIENTS_REQUIRED = 2

    def setUp(self):
        super().setUp()
        self.fs.set_allow_new_snaps(True)
        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()

    def tearDown(self):
        # A test may leave mount_b's network suspended.
        try:
            self.mount_b.resume_netns()
        except Exception:
            log.exception("resume_netns failed")
        super().tearDown()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _marker(self, msg):
        log.info("=" * 20 + " %s " + "=" * 20, msg)

    def _build_tree(self):
        self._marker("building namespace")
        m = self.mount_a
        m.run_shell(["mkdir", "-p", "data/d", "home/u1", "home/big/far/sub"])
        m.run_shell_payload("echo zebra > home/big/far/zebra; "
                            "echo a > home/u1/a.txt")

        m.setfattr("data", "ceph.dir.pin", "0")
        m.setfattr("home", "ceph.dir.pin", "1")
        m.setfattr("home/big/far", "ceph.dir.pin", "2")
        self._wait_subtrees(EXPECTED_SUBTREES, rank="all", timeout=60)

        # Creates under each subtree: dirty fragstat/rstat on dirfrags
        # whose inodes are auth on another rank (scatterlocks).
        m.run_shell_payload(
            f"for i in $(seq 1 {NFILES}); do "
            f"  echo $i > data/f$i; echo $i > home/u1/f$i; "
            f"  echo $i > home/big/far/g$i; "
            f"done")

        # Cross-rank hard link: dentry on rank 1, inode auth on rank 2.
        m.run_shell(["ln", "home/big/far/zebra", "home/u1/lnk"])

        # Cross-rank renames (peer updates in the journals).
        m.run_shell(["mv", "data/f1", "home/u1/moved_from_data"])         # 0 -> 1
        m.run_shell(["mv", "home/u1/a.txt", "home/big/far/a.txt"])        # 1 -> 2
        m.run_shell(["mv", "home/big/far/g1", "data/moved_from_far"])     # 2 -> 0
        m.run_shell(["mkdir", "home/big/far/sub/dir1"])
        m.run_shell(["mv", "home/big/far/sub/dir1", "home/u1/dir1"])      # dir 2 -> 1

        # Snapshot /home, then modify: snapped inodes on ranks 1 and 2.
        m.run_shell(["mkdir", "home/.snap/s1"])
        m.run_shell_payload("echo more >> home/u1/f2; "
                            "echo more >> home/big/far/zebra; "
                            "rm home/u1/f3 home/big/far/g3")

        # Hold files open in every subtree so caps are reconnected.
        for path in ("data/held", "home/u1/held", "home/big/far/held"):
            m.open_background(path)

        # Deliberately no journal flush: replay should rebuild all of this.

    def _listing(self):
        # run_shell_payload wraps the payload in single quotes: use double
        # quotes only. Directory sizes are rbytes, which propagate lazily,
        # so only record sizes (and link counts) for non-directories.
        out = self.mount_a.run_shell_payload(
            "find . -path ./home/.snap -prune -o "
            "-type d -printf \"%p d\\n\" -o "
            "-printf \"%p %y %n %s\\n\" | sort; "
            "echo \"--- snap s1\"; "
            "cd home/.snap/s1 && find . "
            "-type d -printf \"%p d\\n\" -o "
            "-printf \"%p %y %s\\n\" | sort")
        return out.stdout.getvalue()

    def _dump_rank(self, rank, status):
        subtrees = self.fs.rank_asok(["get", "subtrees"], rank=rank,
                                     status=status)
        cache = self.fs.rank_asok(["dump", "cache"], rank=rank,
                                  status=status, timeout=600)
        log.info("rank %d subtrees:\n%s", rank, json.dumps(subtrees, indent=1))
        log.info("rank %d cache (%d inodes):\n%s", rank, len(cache),
                 json.dumps(cache, indent=1))
        return subtrees, cache

    @staticmethod
    def _authority(obj):
        a = obj['replica_state']['authority']
        if isinstance(a, dict):
            return a['first']
        return a[0]

    def _index_cache(self, caches):
        """
        Build {rank: {'inode': {ino: obj}, 'dirfrag': {df: obj}}}.
        Inodes whose ino appears more than once in any rank's dump (head +
        snapped inodes) are excluded everywhere: the dump has no snapid to
        tell them apart.
        """
        dup = set()
        for cache in caches.values():
            seen = set()
            for ino_obj in cache:
                ino = ino_obj.get('ino')
                if ino in seen:
                    dup.add(ino)
                seen.add(ino)

        index = {}
        for rank, cache in caches.items():
            inodes, dirfrags = {}, {}
            for ino_obj in cache:
                ino = ino_obj.get('ino')
                if ino not in dup:
                    inodes[ino] = ino_obj
                for df in ino_obj.get('dirfrags', []):
                    dirfrags[df['dirfrag']] = df
            index[rank] = {'inode': inodes, 'dirfrag': dirfrags}
        return index, dup

    def _check_replicas(self, caches):
        """
        Every non-auth object must be registered at its auth with the same
        nonce. Auth-side entries for objects the replica no longer holds are
        reported but tolerated (a cache expire may be in flight).
        """
        index, dup = self._index_cache(caches)
        errors, stale = [], []
        for rank, objs in index.items():
            for kind in ('inode', 'dirfrag'):
                for key, obj in objs[kind].items():
                    if obj['is_auth']:
                        reps = obj['auth_state']['replicas']
                        for r, nonce in reps.items():
                            r = int(r)
                            held = index.get(r, {}).get(kind, {}).get(key)
                            if held is None:
                                stale.append(f"{kind} {key}: mds.{rank} lists "
                                             f"mds.{r} (nonce {nonce}) but "
                                             f"mds.{r} does not hold it")
                        continue
                    auth = self._authority(obj)
                    nonce = obj['replica_state']['replica_nonce']
                    where = f"{kind} {key} on mds.{rank}"
                    if auth < 0:
                        errors.append(f"{where}: undefined authority "
                                      f"{obj['replica_state']['authority']}")
                        continue
                    if auth not in index:
                        errors.append(f"{where}: auth mds.{auth} not active")
                        continue
                    aobj = index[auth][kind].get(key)
                    if aobj is None:
                        errors.append(f"{where}: auth mds.{auth} does not "
                                      f"have it in cache")
                    elif not aobj['is_auth']:
                        errors.append(f"{where}: mds.{auth} copy is not auth")
                    else:
                        anonce = aobj['auth_state']['replicas'].get(str(rank))
                        if anonce is None:
                            errors.append(f"{where}: not in mds.{auth} "
                                          f"replica_map "
                                          f"{aobj['auth_state']['replicas']}")
                        elif anonce != nonce:
                            errors.append(f"{where}: nonce {nonce} != "
                                          f"mds.{auth} replica_map nonce "
                                          f"{anonce}")
        log.info("replica check: %d inodes skipped (snapped/duplicate ino): %s",
                 len(dup), sorted(dup))
        for s in stale:
            log.info("replica check (tolerated): %s", s)
        for e in errors:
            log.error("replica check: %s", e)
        return errors

    def _verify_state(self, label):
        """
        Dump subtrees + cache on every rank and cross-check replica state.
        Retried a few times so that in-flight expires/discovers settle.
        """
        self._marker(f"verifying state: {label}")
        errors = None
        try:
            with contextutil.safe_while(sleep=10, tries=3) as proceed:
                while proceed():
                    status = self.fs.status()
                    caches = {}
                    for info in self.fs.get_ranks(status=status):
                        rank = info['rank']
                        subtrees, cache = self._dump_rank(rank, status)
                        caches[rank] = cache
                        for s in subtrees:
                            self.assertGreaterEqual(
                                s['auth_first'], 0,
                                f"rank {rank}: subtree {s['dir']['path']} "
                                f"has no authority: {s}")
                    errors = self._check_replicas(caches)
                    if not errors:
                        return
        except contextutil.MaxWhileTries:
            pass
        self.fail(f"{label}: inconsistent replica state:\n" +
                  "\n".join(errors))

    def _verify_namespace(self, before, label):
        self._marker(f"verifying namespace: {label}")
        self._wait_subtrees(EXPECTED_SUBTREES, rank="all", timeout=120)
        after = self._listing()
        if before != after:
            log.error("listing before:\n%s", before)
            log.error("listing after:\n%s", after)
        self.assertEqual(before, after, f"{label}: namespace changed")

        out = self.fs.run_scrub(["start", "/", "recursive"])
        self.assertEqual(out['return_code'], 0)
        self.assertTrue(self.fs.wait_until_scrub_complete(
            tag=out["scrub_tag"], sleep=5, timeout=600))
        for info in self.fs.get_ranks():
            damage = self.fs.rank_tell(["damage", "ls"], rank=info['rank'])
            self.assertEqual(damage, [], f"{label}: damage on rank "
                                         f"{info['rank']}: {damage}")

    def _rank_gid(self, rank, status=None):
        return self.fs.get_rank(rank=rank, status=status)['gid']

    def _wait_rank_replaced(self, rank, old_gid, timeout=120):
        """Wait until a daemon other than old_gid holds the rank."""
        with contextutil.safe_while(sleep=1, tries=timeout) as proceed:
            while proceed():
                status = self.fs.status()
                try:
                    info = self.fs.get_rank(rank=rank, status=status)
                except Exception:
                    continue
                if info and info['gid'] != old_gid:
                    return info, status

    def _wait_rank_state(self, rank, state, gid=None, timeout=300):
        with contextutil.safe_while(sleep=1, tries=timeout) as proceed:
            while proceed():
                status = self.fs.status()
                try:
                    info = self.fs.get_rank(rank=rank, status=status)
                except Exception:
                    continue
                if not info or (gid is not None and info['gid'] != gid):
                    continue
                log.debug("rank %d (gid %s) in %s", rank, info['gid'],
                          info['state'])
                if info['state'] == state:
                    return info, status

    # ------------------------------------------------------------------
    # tests
    # ------------------------------------------------------------------

    def test_single_rank_failover(self):
        """
        Rank 1 fails over to the standby; ranks 0 and 2 survive.

        Exercises: survivor strong rejoins (ranks 0 and 2 replicate rank 1's
        objects), weak rejoin from the new rank 1 with the survivor-side
        scour (stale replica_map entries for the old rank 1, e.g. /data and
        zebra), scatterlock state for /home (inode on 0, dirfrag on 1) and
        /home/big/far (inode on 1, dirfrag on 2).
        """
        self._build_tree()
        before = self._listing()
        self._verify_state("before failover")

        self._marker("failing rank 1")
        old_gid = self._rank_gid(1)
        self.fs.rank_fail(rank=1)
        self._wait_rank_replaced(1, old_gid)
        self.fs.wait_for_daemons(timeout=300)

        self._verify_state("after rank 1 failover")
        self._verify_namespace(before, "after rank 1 failover")

    def test_all_ranks_fail(self):
        """
        All ranks fail and recover together (the #64717 pattern).

        Every rank is in up:rejoin and receives a weak rejoin from each of
        the others, so rejoin_scour_survivor_replicas() runs on the
        rejoining side (MDCache::handle_cache_rejoin_weak(), !survivor)
        where no rank holds registered replicas yet.
        """
        self._build_tree()
        before = self._listing()
        self._verify_state("before fs fail")

        self._marker("failing the file system")
        self.fs.fail()
        self.fs.set_joinable()
        self.fs.wait_for_daemons(timeout=300)

        self._verify_state("after all ranks recovered")
        self._verify_namespace(before, "after all ranks recovered")

    def test_rank_restart_during_rejoin(self):
        """
        Rank 1 restarts while rank 0 is in up:rejoin and has already
        processed rank 1's weak rejoin.

        Rank 0 must then scour the replica_map entries created for the old
        rank 1 when the new rank 1's weak rejoin arrives. This is the case
        where the rejoining-side scour has real work to do.

        Sequencing: mount_b's network is suspended so it never reconnects,
        and mds_reconnect_timeout is set high cluster-wide. That holds every
        rank in up:reconnect. Ranks 0 and 1 are then released (timeout
        lowered at runtime via the admin socket) into up:rejoin, where they
        exchange weak rejoins and wait for rank 2, still held in reconnect.
        Rank 1 is failed and replaced; once the new rank 1 reaches up:rejoin
        and has sent its weak rejoin, rank 2 is released.
        """
        if getattr(self.mount_b, 'nsid', -1) == -1:
            self.skipTest("mount_b has no netns; cannot suspend its network")

        self._build_tree()

        # mount_b needs sessions on all three ranks.
        self.mount_b.run_shell_payload(
            "touch data/from_b home/u1/from_b home/big/far/from_b; "
            "stat data/f2 home/u1/f2 home/big/far/g2 > /dev/null; sync")
        before = self._listing()
        self._verify_state("before fs fail")

        self.config_set('mds', 'mds_reconnect_timeout', 600)
        self.mount_b.suspend_netns()

        self._marker("failing the file system")
        self.fs.fail()
        self.fs.set_joinable()

        for rank in (0, 1, 2):
            self._wait_rank_state(rank, 'up:reconnect')
        self._marker("all ranks held in up:reconnect")

        status = self.fs.status()
        for rank in (0, 1):
            self.fs.rank_asok(["config", "set", "mds_reconnect_timeout", "1"],
                              rank=rank, status=status)
        for rank in (0, 1):
            self._wait_rank_state(rank, 'up:rejoin')
        self._marker("ranks 0 and 1 in up:rejoin, rank 2 held in reconnect")
        # Let ranks 0 and 1 send and process each other's weak rejoins.
        time.sleep(10)
        status = self.fs.status()
        self.assertEqual(self.fs.get_rank(rank=0, status=status)['state'],
                         'up:rejoin')
        self.assertEqual(self.fs.get_rank(rank=2, status=status)['state'],
                         'up:reconnect')

        self._marker("failing rank 1 during rejoin")
        old_gid = self._rank_gid(1, status=status)
        self.fs.rank_fail(rank=1)
        info, status = self._wait_rank_replaced(1, old_gid)
        new_gid = info['gid']
        # The replacement reads mds_reconnect_timeout=600 from the config
        # database; release it (the asok works in any state).
        self.fs.mds_asok(["config", "set", "mds_reconnect_timeout", "1"],
                         mds_id=info['name'])
        self._wait_rank_state(1, 'up:rejoin', gid=new_gid)
        self._marker("new rank 1 in up:rejoin")
        time.sleep(10)
        status = self.fs.status()
        self.assertEqual(self.fs.get_rank(rank=0, status=status)['state'],
                         'up:rejoin')

        self._marker("releasing rank 2")
        self.fs.rank_asok(["config", "set", "mds_reconnect_timeout", "1"],
                          rank=2, status=status)
        self.fs.wait_for_daemons(timeout=300)

        self.mount_b.resume_netns()
        self.mount_b.umount_wait(force=True)

        self._verify_state("after rank 1 restart during rejoin")
        self._verify_namespace(before, "after rank 1 restart during rejoin")
