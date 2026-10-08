"""
Measure where MDS failover time goes when the cache is large.

This is a measurement harness, not a functional test: it builds a large
cache on every rank, fails ranks, and waits for recovery. The phase timings
are read afterwards from the MDS logs (debug_mds 10, debug_ms 1), e.g. with
recovery-phase-times.py.

Layout, with three active ranks:

  /r<rank>/cached/d*/f*     created, then flushed out of the journal. Some of
                            these are held open by a client, so the open file
                            table records them and the rejoin prefetch has to
                            load them (they are not in the replayed journal).
  /r<rank>/journaled/d*/f*  created after the flush and left in the journal,
                            so replay rebuilds a large cache (recalc_auth_bits,
                            rejoin_send_acks, the rejoining-side scour).

Survivors keep both sets in cache, so the survivor-side scour and
rejoin_walk() walk a large cache too.

The test checks that the flush wrote the held-open files to each rank's open
file table, so the rejoin prefetch has them to load. With drop_client_cache,
the client then drops its caches and releases caps on everything it does not
hold open, so reconnect does not bring back caps on the whole cached set
(opening those inodes for the cap imports would hide the prefetch).

Sizes can be set from the job yaml:

  recovery_timing:
    cached_files_per_rank: 500000
    journaled_files_per_rank: 1500000
    open_files_per_rank: 100000
    files_per_dir: 5000
    create_workers: 48
    drop_client_cache: true

See https://tracker.ceph.com/issues/81449
"""

import logging
import time
from io import StringIO
from textwrap import dedent

from teuthology.contextutil import safe_while

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

OPENED_MARKER = "/tmp/recovery_timing_opened"


class TestRecoveryTiming(CephFSTestCase):
    MDSS_REQUIRED = 4
    CLIENTS_REQUIRED = 1

    RANKS = 3

    def setUp(self):
        super().setUp()

        params = self.ctx.config.get('recovery_timing', None) or {}
        self.cached_files = int(params.get('cached_files_per_rank', 500000))
        self.journaled_files = int(params.get('journaled_files_per_rank', 1500000))
        self.open_files = int(params.get('open_files_per_rank', 100000))
        self.files_per_dir = int(params.get('files_per_dir', 5000))
        self.create_workers = int(params.get('create_workers', 48))
        self.drop_client_cache = bool(params.get('drop_client_cache', True))
        self.assertLessEqual(self.open_files, self.cached_files)

        self.fs.set_max_mds(self.RANKS)
        self.status = self.fs.wait_for_daemons()

    def _set_debug_mds(self, level):
        # runtime override: ceph.conf sets debug mds for the job, and a
        # config-db setting would not take precedence over it
        for mds_id in self.fs.mds_ids:
            self.fs.mds_tell(["config", "set", "debug_mds", str(level)],
                             mds_id=mds_id)

    def _create_files(self, subdir, count):
        """
        Create count files under /r<rank>/<subdir> for every rank, all ranks
        in parallel.
        """
        bases = [f"{self.mount_a.hostfs_mntpt}/r{rank}/{subdir}"
                 for rank in range(self.RANKS)]
        ndirs = -(-count // self.files_per_dir)
        pyscript = dedent(f"""
            import os
            from multiprocessing import Pool

            bases = {bases!r}
            count = {count}
            per = {self.files_per_dir}

            def create(job):
                base, d = job
                dpath = os.path.join(base, "d" + str(d))
                os.makedirs(dpath, exist_ok=True)
                n = min(per, count - d * per)
                for i in range(n):
                    fd = os.open(os.path.join(dpath, "f" + str(i)),
                                 os.O_CREAT | os.O_WRONLY, 0o644)
                    os.close(fd)
                return n

            jobs = [(base, d) for d in range({ndirs}) for base in bases]
            with Pool({self.create_workers}) as p:
                print(sum(p.imap_unordered(create, jobs)))
            """)
        start = time.time()
        created = self.mount_a.run_python(pyscript, timeout=4*3600)
        log.info(f"created {created} files under /r*/{subdir} "
                 f"in {time.time() - start:.1f}s")
        self.assertEqual(int(created), count * self.RANKS)

    def _hold_open(self):
        """
        Open the first open_files_per_rank files of every rank's cached set,
        read-only, and keep them open in a background process.
        """
        bases = [f"{self.mount_a.hostfs_mntpt}/r{rank}/cached"
                 for rank in range(self.RANKS)]
        total = self.open_files * self.RANKS
        pyscript = dedent(f"""
            import fcntl
            import os
            import resource
            import sys
            import time

            fcntl.fcntl(sys.stdin, fcntl.F_SETFL,
                        fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)

            nofile = {total} + 1024
            resource.setrlimit(resource.RLIMIT_NOFILE, (nofile, nofile))

            per = {self.files_per_dir}
            fds = []
            for base in {bases!r}:
                for i in range({self.open_files}):
                    path = os.path.join(base, "d" + str(i // per),
                                        "f" + str(i % per))
                    fds.append(os.open(path, os.O_RDONLY))

            with open("{OPENED_MARKER}", "w") as f:
                f.write(str(len(fds)))

            while True:
                try:
                    if os.read(0, 4096) == b"":
                        break
                except BlockingIOError:
                    pass
                time.sleep(2)
            """)
        # sudo: an earlier test's (root-owned) marker in sticky /tmp
        self.mount_a.client_remote.run(args=["sudo", "rm", "-f", OPENED_MARKER])
        # sudo: raising the hard RLIMIT_NOFILE needs root
        proc = self.mount_a._run_python(pyscript, sudo=True)
        self.mount_a.background_procs.append(proc)

        with safe_while(sleep=5, tries=720,
                        action="wait for files to be opened") as proceed:
            while proceed():
                opened = self.mount_a.client_remote.sh(
                    f"cat {OPENED_MARKER} 2>/dev/null || true").strip()
                if opened == str(total):
                    break
        log.info(f"holding {total} files open")

    def _check_open_file_table(self):
        """
        After the flush, each rank's open file table objects should have an
        entry for every file held open on that rank (plus their ancestors).
        """
        objects = self.fs.radosmo(["ls"], stdout=StringIO()).split()
        for rank in range(self.RANKS):
            prefix = f"mds{rank}_openfiles."
            keys = 0
            for obj in sorted(o for o in objects if o.startswith(prefix)):
                out = self.fs.radosmo(["listomapkeys", obj], stdout=StringIO())
                keys += len(out.split())
            log.info(f"rank {rank} open file table has {keys} entries")
            self.assertGreaterEqual(keys, self.open_files,
                                    f"rank {rank} open file table is missing "
                                    f"held-open files")

    def _wait_caps_released(self, timeout=1800):
        """
        Wait for the client to release the caps it no longer needs after
        dropping its caches. Recovery is measured whatever happens; this only
        logs how far the release got.
        """
        limit = self.open_files + self.open_files // 10 + 1000
        start = time.time()
        while True:
            caps = [self.fs.rank_asok(["perf", "dump", "mds_mem"], rank=rank)
                    ['mds_mem']['cap'] for rank in range(self.RANKS)]
            log.info(f"caps per rank after dropping client caches: {caps}")
            if all(c <= limit for c in caps):
                return
            if time.time() - start > timeout:
                log.warning(f"client still holds more than {limit} caps on "
                            f"some rank; reconnect will bring them back")
                return
            time.sleep(10)

    def _populate(self):
        # the balancer does not export a pinned directory while it is empty,
        # so create the subdirectories before pinning
        self.mount_a.run_shell(["mkdir", "-p"] +
                               [f"r{rank}/{subdir}" for rank in range(self.RANKS)
                                for subdir in ("cached", "journaled")])
        for rank in range(self.RANKS):
            self.mount_a.setfattr(f"r{rank}", "ceph.dir.pin", str(rank))
        self._wait_subtrees([(f"/r{rank}", rank) for rank in range(self.RANKS)],
                            status=self.status, rank="all", timeout=120)

        # creating this many files at debug_mds 10 would take far longer
        # and fill the log disks
        self._set_debug_mds(1)

        self._create_files("cached", self.cached_files)
        self._hold_open()

        # push the cached set out of the journal; trimming also commits the
        # open file table with the files held open above
        for rank in range(self.RANKS):
            self.fs.rank_tell(["flush", "journal"], rank=rank, timeout=1800)
        self._check_open_file_table()

        self._create_files("journaled", self.journaled_files)

        if self.drop_client_cache:
            # release caps on everything that is not held open, so reconnect
            # does not bring back caps on the whole cached set
            self.mount_a.client_remote.run(
                args=["sudo", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])
            self._wait_caps_released()

        self._set_debug_mds(10)
        for rank in range(self.RANKS):
            perf = self.fs.rank_asok(["perf", "dump", "mds_mem"], rank=rank)
            log.info(f"rank {rank} before failover: {perf}")

    def _log_ranks(self, what):
        status = self.fs.status()
        ranks = {r['rank']: r['name'] for r in self.fs.get_ranks(status=status)}
        log.info(f"RECOVERY_TIMING {what} at {time.time():.3f}: rank -> mds {ranks}")
        return ranks

    def test_single_rank_failover(self):
        """
        Fail rank 1 with ranks 0 and 2 as survivors. Survivors send strong
        rejoins (rejoin_walk), scour on the weak rejoin and wake waiters in
        handle_mds_recovery.
        """
        self._populate()

        self._log_ranks("before single rank failover")
        start = time.time()
        self.fs.rank_fail(rank=1)
        self.fs.wait_for_daemons(timeout=3600)
        log.info(f"RECOVERY_TIMING single rank failover took "
                 f"{time.time() - start:.1f}s")
        self._log_ranks("after single rank failover")

    def test_all_ranks_fail(self):
        """
        Fail every rank together. All ranks send weak rejoins to each other,
        so each rank runs the rejoining-side scour once per peer.
        """
        self._populate()

        self._log_ranks("before all ranks fail")
        start = time.time()
        self.fs.fail()
        with safe_while(sleep=2, tries=60,
                        action="wait for all ranks to fail") as proceed:
            while proceed():
                if not list(self.fs.get_ranks()):
                    break
        self.fs.set_joinable()
        self.fs.wait_for_daemons(timeout=3600)
        log.info(f"RECOVERY_TIMING all ranks recovery took "
                 f"{time.time() - start:.1f}s")
        self._log_ranks("after all ranks fail")
