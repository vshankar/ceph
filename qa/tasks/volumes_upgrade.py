"""
mgr/volumes upgrade sub-tasks: verify that auto-upgrade of v1 and v2
subvolumes to the v3 layout does not disrupt subvolume clients doing IO, that
async clones and purges started before an upgrade finish successfully after
it, that v1/v2 subvolumes keep working as such while only ceph-mgr is
upgraded, that subvolume (and snapshot) attributes, auth IDs and the
subvolume boundary survive auto-upgrade, that sessions mounted at the
pre-v3 data dir can be evicted, that snapshots taken before and after
auto-upgrade are all listed, and that subvolumes removed with retained
snapshots (one of them with a pending clone) can be upgraded.

Run these in order (see qa/suites/fs/upgrade/volumes):

  - volumes_upgrade.setup             (on the old release)
  - volumes_upgrade.verify_mgr_only   (only after a mgr-only upgrade)
  - volumes_upgrade.check_io          (any time after setup)
  - volumes_upgrade.post_upgrade      (after the whole cluster is upgraded)
"""

import contextlib
import functools
import json
import logging
import re
import time
import uuid
from configparser import ConfigParser
from textwrap import dedent

from teuthology import misc
from teuthology.contextutil import safe_while
from teuthology.orchestra import run

log = logging.getLogger(__name__)

VOLNAME = 'cephfs'
SNAPNAME = 'snap0'
SNAPNAME_2 = 'snap1'
# taken (on the clone source) by the new mgr while the MDS is still old
SNAP_MGR_ONLY = 'snap_mgr_only'
# taken on every subvolume after auto-upgrade
SNAP_V3 = 'snap_v3'
TRASH_DIR = 'volumes/_deleting'
# mgr/volumes/snapshot_clone_delay the old ceph-mgr picks up the clones with,
# so that they're still pending when it gets upgraded (pause_cloning cancels
# clones, see https://tracker.ceph.com/issues/81383).
CLONE_DELAY = 86400
# mgr/volumes/max_concurrent_clones for the old ceph-mgr: each delayed clone
# holds a cloner thread, and with mgr/volumes/snapshot_clone_no_wait (the
# default) ceph-mgr rejects a clone (EAGAIN) once as many clones as there are
# cloner threads are pending.
MAX_CONCURRENT_CLONES = 8
STATE_KEY = 'volumes-upgrade-state'

# (name, group) of the subvolumes this task creates
IO_SUBVOLS = [('sv_io_0', None), ('sv_io_1', None), ('sv_io_2', 'grp_io')]
CLONE_SRC = ('sv_clone_src', 'grp_src')
CLONES = [('sv_clone_0', None), ('sv_clone_1', None), ('sv_clone_2', 'grp_clone')]
PURGES = [('sv_purge_0', None), ('sv_purge_1', None), ('sv_purge_2', 'grp_purge')]
# number of pre-v3 snapshots taken on each IO subvolume, while its client
# is writing
IO_NSNAPS = [0, 1, 2]
# a clone (pending through the upgrade) of the first snapshot of an IO
# subvolume: (clone, index of the IO subvolume)
IO_CLONE = (('sv_clone_io', 'grp_clone'), 1)
# v2 subvolumes removed with --retain-snapshots (v1 subvolumes don't support
# it), with their data dirs caught mid-purge: one with a clone (of its
# retained snapshot) pending through the upgrade, and one without.
RETAINED_CLONE_SRC = ('sv_retained_src', 'grp_retained')
RETAINED_CLONE = ('sv_clone_retained', 'grp_clone')
RETAINED_PURGE = ('sv_retained_purge', 'grp_retained')
STATE_RETAINED = 'snapshot-retained'

# non-default attributes the v1/v2 (IO and clone source) subvolumes are
# created with, which must survive auto-upgrade. the user xattr (as well as
# the earmark, being one) is to be copied over by auto-upgrade, unlike
# ceph.* vxattrs (quota and layout) which it must carry over otherwise.
SV_SIZE = 10 * 1024 ** 3
SV_UID = SV_GID = 1000
SV_MODE = 0o750
SV_EARMARK = 'nfs'
SV_METADATA = {'vu_key_0': 'vu_val_0', 'vu_key_1': 'vu_val_1'}
SV_XATTR = ('user.volumes_upgrade', 'vu_xattr_val')
SV_MAX_FILES = 1000000
# an ACL (also copied over, as xattrs) that leaves the mode as is
SV_ACL = 'u:1001:r-x,d:u:1001:r-x'
# a file in the data dir for probing the subvolume boundary
BOUNDARY_FILE = 'vu_boundary'
# user metadata of each pre-v3 snapshot (the value being the snapshot name)
SNAP_METADATA_KEY = 'vu_snap_key'
# `fs subvolume snapshot info` fields that must be as the old ceph-mgr
# reported them
SNAP_INFO_KEPT = ('created_at', 'data_pool')
EARMARK_XATTR = 'user.ceph.subvolume.earmark'
# `fs subvolume info` fields that must be as the old ceph-mgr reported them
INFO_KEPT = ('type', 'uid', 'gid', 'mode', 'data_pool', 'pool_namespace',
             'bytes_quota', 'created_at', 'earmark')

# Runs on the client node as root, from within the subvolume mount. Writes
# files until the stop file appears, periodically re-verifying recent writes
# and doing namespace operations. Any failure (e.g., EACCES/ESTALE after the
# subvolume's data dir gets renamed during auto-upgrade) makes it exit
# non-zero. The checksum manifest is kept outside CephFS on purpose.
WRITER_SCRIPT = dedent('''\
    set -u
    mnt=$1
    state=$2
    manifest=$state/manifest
    fail() { echo "$(date -u +%FT%T) writer: $*" >&2; exit 1; }
    : > "$manifest" || fail "creating manifest"
    cd "$mnt" || fail "cd $mnt"
    mkdir -p io || fail "mkdir io"
    i=0
    while [ ! -e "$state/stop" ]; do
        i=$((i + 1))
        f=io/f.$i
        dd if=/dev/urandom of=$f.tmp bs=4k count=4 conv=fsync status=none || fail "write $f.tmp"
        mv $f.tmp $f || fail "rename $f.tmp"
        md5sum $f >> "$manifest" || fail "checksum $f"
        if [ $((i % 20)) -eq 0 ]; then
            tail -n 20 "$manifest" | md5sum -c --quiet || fail "verify at $i"
            mkdir io/d.$i && rmdir io/d.$i || fail "mkdir/rmdir io/d.$i"
            ls io > /dev/null || fail "readdir"
            stat . > /dev/null || fail "stat"
        fi
        echo $i > "$state/progress"
        sleep 0.5
    done
    md5sum -c --quiet "$manifest" || fail "final verify"
    n=$(find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l)
    [ "$n" -eq "$i" ] || fail "found $n files, wrote $i"
    echo "writer: wrote and verified $i files"
''')


class Subvol:
    def __init__(self, name, group):
        self.name = name
        self.group = group
        self.base = f'volumes/{group or "_nogroup"}/{name}'
        # subvolume version before auto-upgrade to v3
        self.version = 2
        # path to the pre-v3 (v1/v2) data dir, relative to fs root
        self.data_path = None
        # created with the SV_* attributes
        self.attrs = False
        # pool namespace it is isolated in (if attrs)
        self.namespace = None
        # `fs subvolume info` by the old ceph-mgr (if attrs and it could be
        # queried)
        self.info = None
        # getfacl of the data dir, as set at creation (if attrs)
        self.acl = None
        # removed with --retain-snapshots
        self.retained = False

    @property
    def grp_arg(self):
        return f' --group_name {self.group}' if self.group else ''

    def __str__(self):
        return f'{self.group or "_nogroup"}/{self.name}'


class State:
    def __init__(self, ctx, config):
        self.manager = ctx.managers['ceph']
        self.admin = _get_mount(ctx, config.get('admin_client', 'client.3'))
        self.io_mounts = [_get_mount(ctx, c) for c in
                          config.get('io_clients',
                                     ['client.0', 'client.1', 'client.2'])]
        assert len(self.io_mounts) == len(IO_SUBVOLS)
        # version of the (non-clone, non-purged) subvolumes before upgrade
        self.version = int(config.get('version', 2))
        assert self.version in (1, 2), f'unsupported version {self.version}'
        self.testdir = misc.get_testdir(ctx)

        self.io_svs = [Subvol(*s) for s in IO_SUBVOLS]
        self.src = Subvol(*CLONE_SRC)
        # clones pending through the upgrade
        self.clones = [Subvol(*s) for s in CLONES]
        # clone name -> (source subvolume, snapshot) of each of self.clones
        self.clone_of = {c.name: (self.src, SNAPNAME) for c in self.clones}
        self.purges = [Subvol(*s) for s in PURGES]
        # subvolumes removed with --retain-snapshots: a clone source and a
        # purge
        self.retained = [Subvol(*RETAINED_CLONE_SRC), Subvol(*RETAINED_PURGE)]

        self.clone_src_files = config.get('clone_src_files', 5000)
        self.purge_files = config.get('purge_files', 5000)
        # subvolume name -> names of snapshots it is expected to have
        self.snaps = {}
        # (subvolume name, snapshot name) -> manifest of the snapshot's data
        self.snap_manifests = {}
        # clones issued after setup: (clone, source, snapshot)
        self.late_clones = []
        # one entry per io_svs: dict(mount, sv, statedir, proc)
        self.writers = []
        # (subvolume name, snapshot name) -> `fs subvolume snapshot info` by
        # the old ceph-mgr
        self.snap_info = {}
        # subvolume name -> dict(auth_id, keyring, mnt, root) of an auth ID
        # authorized by the old ceph-mgr for an extra (ceph-fuse) session
        # mounted at the pre-v3 data dir, for checking authorized_list,
        # deauthorize and evict after auto-upgrade
        self.evict = {}


def _get_mount(ctx, role):
    _, _, id_ = misc.split_role(role)
    return ctx.mounts[id_]


def _state(ctx):
    return ctx[STATE_KEY]


def _ceph(st, cmd, check_status=True):
    p = st.manager.ceph(cmd, check_status=check_status)
    return p.exitstatus, p.stdout.getvalue()


def _admin_sh(st, payload, timeout=900):
    """
    Run payload as root via bash, from the root of the admin mount. The
    payload must not contain single quotes.
    """
    p = st.admin.run_shell_payload(payload, sudo=True, timeout=timeout)
    return p.stdout.getvalue()


def _wait_for(desc, pred, timeout, sleep=5):
    with safe_while(sleep=sleep, tries=max(1, timeout // sleep),
                    action=desc) as proceed:
        while proceed():
            if pred():
                return


def _set_mgr_config(st, name, value):
    _ceph(st, f'config set mgr mgr/volumes/{name} {value}')
    # config_notify() is asynchronous
    time.sleep(5)


def _set_purging_paused(st, paused):
    _set_mgr_config(st, 'pause_purging', str(paused).lower())


def _count_files(st, path):
    out = _admin_sh(st, f'test -d {path} && find {path} -type f | wc -l || echo 0')
    return int(out.strip())


def _read_meta(st, sv, meta='.meta'):
    out = _admin_sh(st, f'cat {sv.base}/{meta}')
    cp = ConfigParser()
    cp.read_string(out)
    return cp


def _getpath(st, sv, check_status=True):
    rc, out = _ceph(st, f'fs subvolume getpath {VOLNAME} {sv.name}{sv.grp_arg}',
                    check_status=check_status)
    return rc, out.strip()


def _manifest(st, path):
    return _admin_sh(st, f'cd {path} && find . -type f -print0 | sort -z | '
                         'xargs -0 -r md5sum')


def _populate(st, path, nfiles, files_per_dir=100):
    ndirs = max(1, nfiles // files_per_dir)
    _admin_sh(st, dedent(f'''\
        set -e
        cd {path}
        for d in $(seq 1 {ndirs}); do
            mkdir -p d.$d
            for f in $(seq 1 {files_per_dir}); do
                head -c 4096 /dev/urandom > d.$d/f.$f
            done
        done
    '''), timeout=3600)


# ----- subvolume layout checks -----


def _assert_pre_v3(st, sv, strict=False):
    """
    The subvolume is in its pre-v3 (v1/v2) layout and untouched by any
    (failed) auto-upgrade attempt.

    ceph-mgr has long been upgrading (the metadata of) v1 subvolumes without
    snapshots to v2 on access, the layout being the same. Unless strict, such
    a v1 subvolume may be either.
    """
    ftype = _admin_sh(st, f'stat -c %F {sv.base}/.meta').strip()
    assert ftype == 'regular file', f'{sv}: .meta is a {ftype}'
    meta = _read_meta(st, sv)
    version = meta.get('GLOBAL', 'version')
    expected = {str(sv.version)}
    if sv.version == 1 and not strict and not st.snaps.get(sv.name):
        expected.add('2')
    assert version in expected, f'{sv}: version = {version}, expected {expected}'
    path = meta.get('GLOBAL', 'path')
    assert path == f'/{sv.data_path}', f'{sv}: meta path = {path}'
    out = _admin_sh(st, f'test -d {sv.data_path} && echo yes; '
                        f'test -e {sv.base}/roots && echo yes || echo no')
    assert out.split() == ['yes', 'no'], \
        f'{sv}: half-upgraded subvolume (v2 dir, roots dir present?: {out.split()})'


def _assert_v3(st, sv):
    """
    Trigger auto-upgrade (if not done already) through getpath and verify the
    subvolume is in v3 layout. A v1/v2 subvolume, with or without snapshots,
    is upgraded in place: its data dir is moved under roots/ keeping its uuid
    (so that clients need not remount), along with any v1 snapshots (taken
    in the data dir), while v2 snapshots stay where they were
    (<subvolume>/.snap).
    """
    _, path = _getpath(st, sv)
    m = re.fullmatch(rf'/{re.escape(sv.base)}/roots/([0-9a-f-]{{36}})/mnt', path)
    assert m, f'{sv}: getpath returned {path}, expected a v3 path'
    sv_uuid = m.group(1)

    link = _admin_sh(st, f'readlink {sv.base}/.meta').strip()
    assert link == f'.meta.{sv_uuid}', f'{sv}: .meta -> {link}, uuid = {sv_uuid}'

    meta = _read_meta(st, sv)
    glob = dict(meta.items('GLOBAL'))
    log.info(f'{sv}: v3 meta = {glob}')
    assert glob.get('version') == '3', f'{sv}: meta = {glob}'
    assert glob.get('path') == path, f'{sv}: meta = {glob}'
    assert glob.get('state') == 'complete', f'{sv}: meta = {glob}'

    if sv.data_path:
        old_uuid = sv.data_path.rsplit('/', 1)[1]
        assert sv_uuid == old_uuid, f'{sv}: uuid changed {old_uuid} -> {sv_uuid}'
        old_dir = _admin_sh(st, f'test -e {sv.data_path} && echo yes || echo no').strip()
        assert old_dir == 'no', \
            f'{sv}: v{sv.version} data dir {sv.data_path} still exists'
    return path.lstrip('/')


def _assert_usable(st, sv):
    """
    The subvolume is still v1/v2 and subvolume commands on it work as usual.
    """
    _, path = _getpath(st, sv)
    assert path == f'/{sv.data_path}', f'{sv}: getpath returned {path}'
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    info = json.loads(out)
    assert info['path'] == f'/{sv.data_path}', f'{sv}: info = {info}'
    _assert_snaps(st, sv)
    _assert_pre_v3(st, sv)
    _assert_attrs(st, sv, sv.data_path)


# ----- subvolume attributes -----


def _assert_attrs(st, sv, path):
    """
    The subvolume (whose data dir is at path) has the attributes it was
    created with: as reported by `fs subvolume info` (also compared with what
    the old ceph-mgr reported, if it could be queried), user metadata, and
    the user xattr on its data dir.
    """
    if not sv.attrs:
        return
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    info = json.loads(out)
    log.info(f'{sv}: info = {info}')
    actual = {k: info.get(k) for k in INFO_KEPT}
    actual['mode'] &= 0o7777
    expected = dict(actual, uid=SV_UID, gid=SV_GID, mode=SV_MODE,
                    data_pool=_data_pool(st), pool_namespace=sv.namespace,
                    bytes_quota=SV_SIZE, earmark=SV_EARMARK)
    if sv.info:
        expected.update({k: sv.info.get(k) for k in INFO_KEPT})
        expected['mode'] &= 0o7777
        # v1 subvolumes gain features on getting upgraded
        if sv.version == 2:
            actual['features'] = info.get('features')
            expected['features'] = sv.info.get('features')
    diff = {k: (expected[k], actual[k]) for k in expected
            if expected[k] != actual[k]}
    assert not diff, f'{sv}: attributes changed (expected, actual): {diff}'

    _, out = _ceph(st, f'fs subvolume metadata ls {VOLNAME} {sv.name}{sv.grp_arg}')
    metadata = json.loads(out)
    assert metadata == SV_METADATA, f'{sv}: metadata = {metadata}'

    name, val = SV_XATTR
    out = _admin_sh(st, f'getfattr --only-values -n {name} {path} || true')
    assert out == val, f'{sv}: xattr {name} = "{out}" on {path}, expected "{val}"'

    out = _admin_sh(st, f'getfattr --only-values -n ceph.quota.max_files {path} || true')
    assert out == str(SV_MAX_FILES), f'{sv}: ceph.quota.max_files = "{out}" on {path}'

    acl = _getfacl(st, path)
    assert acl == sv.acl, f'{sv}: ACL on {path}:\n{acl}\nexpected:\n{sv.acl}'


def _getfacl(st, path):
    return _admin_sh(st, f'getfacl -cpn {path}')


def _set_common_attrs(st, sv):
    """
    Set the attributes that are set on the data dir by hand regardless of
    the subvolume version (no ceph-mgr command sets them).
    """
    _admin_sh(st, dedent(f'''\
        set -e
        setfattr -n ceph.quota.max_files -v {SV_MAX_FILES} {sv.data_path}
        setfacl -m {SV_ACL} {sv.data_path}
        touch {sv.data_path}/{BOUNDARY_FILE}
    '''))
    sv.acl = _getfacl(st, sv.data_path)
    assert 'user:1001:r-x' in sv.acl and 'default:user:1001:r-x' in sv.acl, \
        f'{sv}: ACL not set:\n{sv.acl}'


def _probe_boundary(st, sv, path):
    """
    Hard link a file in the data dir (at path) to the subvolume group dir,
    the subvolume dir, and (for v3) the incarnation dir, returning whether
    each is allowed ('ok') or crosses a subvolume boundary ('EXDEV'), so as
    to find where the subvolume is marked (ceph.dir.subvolume).
    """
    targets = {'group': sv.base.rsplit('/', 1)[0], 'subvolume': sv.base}
    if path.endswith('/mnt'):
        targets['incarnation'] = path.rsplit('/', 1)[0]
    result = {}
    for what, d in targets.items():
        dst = f'{d}/{BOUNDARY_FILE}.{sv.name}.link'
        out = _admin_sh(st, f'ln {path}/{BOUNDARY_FILE} {dst} 2>&1 && '
                            f'rm -f {dst} && echo ok || true').strip()
        if out != 'ok':
            assert 'cross-device' in out, f'{sv}: ln to {d}: {out}'
            out = 'EXDEV'
        result[what] = out
    log.info(f'{sv}: subvolume boundary probe from {path}: {result}')
    return result


def _record_attrs(st, sv):
    """
    Record the attributes of the subvolume as reported by the (old)
    ceph-mgr.
    """
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    sv.info = json.loads(out)


# ----- snapshots -----


def _pre_v3_snap_path(sv, snap):
    if sv.version == 1:
        # v1 snapshots are taken in the data (uuid) dir
        return f'{sv.data_path}/.snap/{snap}'
    # v2 snapshots are taken a level above the data dir
    sv_uuid = sv.data_path.rsplit('/', 1)[1]
    return f'{sv.base}/.snap/{snap}/{sv_uuid}'


def _snap_create(st, sv, snap, pre_v3=False, direct=False):
    """
    Snapshot the subvolume through ceph-mgr, or directly (pre_v3 only), the
    way ceph-mgr would have.
    """
    if direct:
        assert pre_v3
        _admin_sh(st, f'mkdir {_pre_v3_snap_path(sv, snap)}')
    else:
        _ceph(st, f'fs subvolume snapshot create {VOLNAME} {sv.name} {snap}{sv.grp_arg}')
    st.snaps.setdefault(sv.name, set()).add(snap)
    if pre_v3:
        path = _pre_v3_snap_path(sv, snap)
        out = _admin_sh(st, f'test -d {path} && echo yes || echo no').strip()
        assert out == 'yes', f'{sv}: v{sv.version} snapshot {snap} not at {path}'
        st.snap_manifests[(sv.name, snap)] = _manifest(st, path)


def _assert_snaps(st, sv):
    """
    Snapshot listing has exactly the snapshots taken so far (v1/v2 as well
    as v3 ones), and each of them can be queried.
    """
    _, out = _ceph(st, f'fs subvolume snapshot ls {VOLNAME} {sv.name}{sv.grp_arg}')
    names = {s['name'] for s in json.loads(out)}
    expected = st.snaps.get(sv.name, set())
    assert names == expected, f'{sv}: snapshots = {names}, expected {expected}'
    for snap in names:
        info = _snap_info(st, sv, snap)
        recorded = st.snap_info.get((sv.name, snap))
        if recorded:
            diff = {k: (recorded.get(k), info.get(k)) for k in SNAP_INFO_KEPT
                    if recorded.get(k) != info.get(k)}
            assert not diff, \
                f'{sv}: snapshot {snap} info changed (expected, actual): {diff}'
        if (sv.name, snap) in st.snap_manifests and not sv.retained:
            # a pre-v3 snapshot (snapshot metadata of a subvolume with
            # retained snapshots can't be queried)
            _, out = _ceph(st, f'fs subvolume snapshot metadata ls {VOLNAME} '
                               f'{sv.name} {snap}{sv.grp_arg}')
            metadata = json.loads(out)
            assert metadata == {SNAP_METADATA_KEY: snap}, \
                f'{sv}: snapshot {snap} metadata = {metadata}'


def _snap_info(st, sv, snap):
    _, out = _ceph(st, f'fs subvolume snapshot info {VOLNAME} {sv.name} {snap}{sv.grp_arg}')
    return json.loads(out)


def _record_snap_info(st, sv):
    for snap in st.snaps.get(sv.name, set()):
        st.snap_info[(sv.name, snap)] = _snap_info(st, sv, snap)


def _assert_pending_clones(st, sv, snap, expected):
    info = _snap_info(st, sv, snap)
    assert info.get('has_pending_clones') == expected, \
        f'{sv}: snapshot {snap} info = {info}, expected has_pending_clones = {expected}'


# ----- IO writers -----


def _start_writer(st, mount, sv):
    statedir = f'{st.testdir}/volumes_upgrade.io.{sv.name}'
    script = f'{st.testdir}/volumes_upgrade.writer.sh'
    remote = mount.client_remote
    remote.run(args=['sudo', 'rm', '-rf', statedir])
    remote.run(args=['mkdir', '-p', statedir])
    remote.write_file(script, WRITER_SCRIPT)
    proc = remote.run(
        args=['sudo', 'stdin-killer', '--timeout=60', '--',
              'bash', script, mount.mountpoint, statedir],
        wait=False, stdin=run.PIPE, label=f'writer {sv}')
    return dict(mount=mount, sv=sv, statedir=statedir, proc=proc)


def _writer_progress(w):
    if w['proc'].finished:
        # surfaces the writer's exit status
        w['proc'].wait()
        raise RuntimeError(f'writer for {w["sv"]} exited prematurely')
    remote = w['mount'].client_remote
    out = remote.sh(f'cat {w["statedir"]}/progress 2>/dev/null || echo 0')
    return int(out.strip())


def _wait_for_writer_progress(w, nfiles, timeout=300):
    start = _writer_progress(w)
    _wait_for(f'writer for {w["sv"]} to write {nfiles} files past {start}',
              lambda: _writer_progress(w) >= start + nfiles, timeout)


def _remount(st, mount, sv, path, **kwargs):
    """
    Remount the client at the subvolume's data dir (path), and restore the
    data dir's mode, which the mount helpers set to 1777 (on the mount root)
    after mounting. The five digits clear the sticky bit too.
    """
    mount.remount(cephfs_mntpt=f'/{path}', **kwargs)
    _admin_sh(st, f'chmod {SV_MODE:05o} {path}')


def _remount_and_verify(st, w, path, client_id, **creds):
    """
    Remount the client at the subvolume's (new) v3 path with the given
    credentials (client_keyring or client_keyring_path), and verify that all
    the data written by the writer is accessible and intact, and that the
    subvolume is writable.
    """
    mount, sv = w['mount'], w['sv']
    log.info(f'{sv}: remounting as client.{client_id} at /{path}')
    _remount(st, mount, sv, path, client_id=client_id, **creds)
    manifest = f'{w["statedir"]}/manifest'
    mount.run_shell(['sudo', 'md5sum', '-c', '--quiet', manifest], timeout=1800)
    nlines = int(mount.client_remote.sh(f'wc -l < {manifest}').strip())
    found = int(mount.get_shell_stdout(
        'sudo find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l').strip())
    assert found == nlines, f'{sv}: {found} files after remount, expected {nlines}'
    mount.run_shell(['sudo', 'bash', '-c',
                     f'echo {client_id} > io/post-remount && '
                     f'grep -q {client_id} io/post-remount && '
                     'rm io/post-remount'])


def _kill_writers(st):
    for w in st.writers:
        if not w['proc'].finished:
            log.info(f'killing writer for {w["sv"]}')
            w['mount'].client_remote.run(
                args=['sudo', 'pkill', '-KILL', '-f', 'volumes_upgrade.writer.sh'],
                check_status=False)


def _kill_writers_on_failure(func):
    """
    Writers left running (in the subvolume mounts) make the client mounts
    fail to unmount on teardown, as do the extra (ceph-fuse) sessions'
    mounts the test dir removal.
    """
    @functools.wraps(func)
    def wrapper(ctx, config):
        try:
            return func(ctx, config)
        except Exception:
            try:
                _kill_writers(_state(ctx))
            except Exception:
                log.exception('failed to kill writers')
            try:
                _cleanup_evict_mounts(_state(ctx))
            except Exception:
                log.exception('failed to unmount extra sessions')
            raise
    return wrapper


def _stop_writer(w):
    remote = w['mount'].client_remote
    remote.run(args=['sudo', 'touch', f'{w["statedir"]}/stop'])
    _wait_for(f'writer for {w["sv"]} to stop',
              lambda: w['proc'].finished, timeout=900)
    w['proc'].stdin.close()
    w['proc'].wait()
    return int(remote.sh(f'cat {w["statedir"]}/progress').strip())


# ----- auth IDs and evict -----


def _authorized_ids(st, sv):
    _, out = _ceph(st, f'fs subvolume authorized_list {VOLNAME} {sv.name}{sv.grp_arg}')
    return {k for e in json.loads(out) for k in e}


def _mgr_auth_ids(st, i, post=False):
    """
    Auth IDs authorized for IO subvolume i through ceph-mgr (with
    `fs subvolume authorize`): those of the client (v2 only, v1 credentials
    being made by hand), of the extra session (if any), and the client's
    new one (post auto-upgrade).
    """
    sv = st.io_svs[i]
    ids = set()
    if st.version == 2:
        ids.add(f'svio{i}')
    if sv.name in st.evict:
        ids.add(st.evict[sv.name]['auth_id'])
    if post:
        ids.add(f'svio{i}new')
    return ids


def _assert_authorized(st, post=False):
    for i, sv in enumerate(st.io_svs):
        if st.version == 1 and not st.snaps.get(sv.name) and not post:
            # nothing authorized through ceph-mgr, see _setup_evict()
            continue
        ids = _authorized_ids(st, sv)
        expected = _mgr_auth_ids(st, i, post)
        assert ids == expected, f'{sv}: authorized_list = {ids}, expected {expected}'


def _sessions(st, auth_id):
    """
    Return the roots of the sessions of auth_id across all active ranks.
    """
    _, out = _ceph(st, f'fs get {VOLNAME} --format=json')
    max_mds = json.loads(out)['mdsmap']['max_mds']
    roots = []
    for rank in range(max_mds):
        _, out = _ceph(st, f'tell mds.{VOLNAME}:{rank} client ls --format=json')
        for session in json.loads(out):
            md = session.get('client_metadata', {})
            if md.get('entity_id') == auth_id:
                roots.append(md.get('root'))
    return roots


def _fuse_mount(st, auth_id, keyring_path, root, mnt, check_status=True):
    remote = st.admin.client_remote
    remote.run(args=['mkdir', '-p', mnt])
    p = remote.run(args=['sudo', 'timeout', '300', 'ceph-fuse',
                         '-c', '/etc/ceph/ceph.conf', '--id', auth_id,
                         '-k', keyring_path, '-r', f'/{root}', mnt],
                   check_status=check_status)
    return p.exitstatus


def _fuse_umount(st, mnt):
    remote = st.admin.client_remote
    remote.run(args=['sudo', 'umount', '-l', mnt], check_status=False)
    remote.run(args=['sudo', 'pkill', '-KILL', '-f', f'ceph-fuse.*{mnt}'],
               check_status=False)
    remote.run(args=['rmdir', mnt], check_status=False)


def _cleanup_evict_mounts(st):
    for e in st.evict.values():
        _fuse_umount(st, e['mnt'])


def _setup_evict(st):
    """
    Authorize an auth ID for each IO subvolume through the old ceph-mgr, and
    mount the subvolume (pre-v3 data dir) with it using ceph-fuse on the
    admin client's node. A v1 subvolume without snapshots is skipped, as
    the old ceph-mgr would upgrade it to v2.
    """
    remote = st.admin.client_remote
    for i, sv in enumerate(st.io_svs):
        if sv.version == 1 and not st.snaps.get(sv.name):
            log.info(f'{sv}: v1 without snapshots, not authorizing through ceph-mgr')
            continue
        auth_id = f'svev{i}'
        keyring = _authorize(st, sv, auth_id)
        keyring_path = f'{st.testdir}/volumes_upgrade.{auth_id}.keyring'
        remote.write_file(keyring_path, keyring)
        mnt = f'{st.testdir}/volumes_upgrade.evict.{i}'
        st.evict[sv.name] = dict(auth_id=auth_id, keyring=keyring_path,
                                 mnt=mnt, root=sv.data_path)
        _fuse_mount(st, auth_id, keyring_path, sv.data_path, mnt)
        remote.run(args=['sudo', 'stat', f'{mnt}/{BOUNDARY_FILE}'])
        roots = set(_sessions(st, auth_id))
        assert roots == {f'/{sv.data_path}'}, f'{sv}: sessions of {auth_id}: {roots}'


def _verify_deauthorize_evict(st, i, path):
    """
    After auto-upgrade, deauthorizing the auth ID of the extra session must
    revoke its access to the subvolume (both the pre-v3 and v3 paths), and
    evicting it must evict the session, mounted at the pre-v3 data dir.
    """
    sv = st.io_svs[i]
    e = st.evict.get(sv.name)
    if not e:
        return
    auth_id, remote = e['auth_id'], st.admin.client_remote

    # the session survived the upgrade, and the subvolume is accessible
    roots = set(_sessions(st, auth_id))
    assert roots == {f'/{e["root"]}'}, f'{sv}: sessions of {auth_id}: {roots}'
    remote.run(args=['sudo', 'timeout', '300', 'stat', f'{e["mnt"]}/{BOUNDARY_FILE}'])

    _ceph(st, f'fs subvolume deauthorize {VOLNAME} {sv.name} {auth_id}{sv.grp_arg}')
    ids = _authorized_ids(st, sv)
    assert auth_id not in ids, f'{sv}: {auth_id} still in authorized_list {ids}'
    rc, out = _ceph(st, f'auth get client.{auth_id} --format=json', check_status=False)
    if rc == 0:
        mds_caps = json.loads(out)[0]['caps'].get('mds', '')
        for p in (e['root'], path):
            assert f'path=/{p}' not in mds_caps, \
                f'{sv}: deauthorized {auth_id} has caps for /{p}: {mds_caps}'

    _ceph(st, f'fs subvolume evict {VOLNAME} {sv.name} {auth_id}{sv.grp_arg}')
    _wait_for(f'sessions of {auth_id} to be evicted',
              lambda: not _sessions(st, auth_id), timeout=120)
    _fuse_umount(st, e['mnt'])

    # the old key must not get access to the subvolume any more
    rc = _fuse_mount(st, auth_id, e['keyring'], path, e['mnt'], check_status=False)
    if rc == 0:
        _fuse_umount(st, e['mnt'])
    assert rc != 0, f'{sv}: deauthorized {auth_id} could mount /{path}'
    _fuse_umount(st, e['mnt'])
    del st.evict[sv.name]


# ----- async jobs -----


def _clone_state(st, sv):
    """
    Read the clone state from the .meta file directly: "fs clone status"
    itself opens the subvolume, triggering auto-upgrade.
    """
    return _read_meta(st, sv).get('GLOBAL', 'state')


def _clone(st, src, snap, c):
    cmd = f'fs subvolume snapshot clone {VOLNAME} {src.name} {snap} {c.name}{src.grp_arg}'
    if c.group:
        _ceph(st, f'fs subvolumegroup create {VOLNAME} {c.group}')
        cmd += f' --target_group_name {c.group}'
    _ceph(st, cmd)


def _wait_for_clone(st, c):
    def done():
        _, out = _ceph(st, f'fs clone status {VOLNAME} {c.name}{c.grp_arg}')
        status = json.loads(out)['status']
        assert status['state'] in ('pending', 'in-progress', 'complete'), \
            f'clone {c}: status = {status}'
        return status['state'] == 'complete'
    _wait_for(f'clone {c} to complete', done, timeout=1800, sleep=10)


def _assert_clone_data(st, c, src, snap, path=None):
    if path is None:
        path = _getpath(st, c)[1].lstrip('/')
    assert _manifest(st, path) == st.snap_manifests[(src.name, snap)], \
        f'clone {c}: data does not match snapshot {snap} of {src}'


def _trash_entries(st):
    out = _admin_sh(st, f'test -d {TRASH_DIR} && '
                        f'find {TRASH_DIR} -mindepth 1 -maxdepth 1 | wc -l || echo 0')
    return int(out.strip())


def _setup_clones(st):
    sv = st.src
    _create_pre_v3(st, sv)
    _populate(st, sv.data_path, st.clone_src_files)
    _pre_v3_snap_create(st, sv, SNAPNAME)
    # a second snapshot, with different contents
    _admin_sh(st, f'mkdir {sv.data_path}/extra && touch {sv.data_path}/extra/file')
    _pre_v3_snap_create(st, sv, SNAPNAME_2)

    # the old ceph-mgr's cloner threads pick up the clones and sleep on the
    # delay (with the clones pending) until it gets upgraded. the delay is
    # reset at the end of setup (by then, the clones have long been picked
    # up), so that the new ceph-mgr starts them right away.
    _set_mgr_config(st, 'max_concurrent_clones', MAX_CONCURRENT_CLONES)
    _set_mgr_config(st, 'snapshot_clone_delay', CLONE_DELAY)
    clones, st.clones = st.clones, []
    for c in clones:
        _clone(st, sv, SNAPNAME, c)
        _register_clone(st, c, sv, SNAPNAME)

    # a clone of an IO subvolume, whose client keeps writing to it
    (name, group), i = IO_CLONE
    io_sv, io_snap = st.io_svs[i], f'snap_v{st.version}_0'
    c = Subvol(name, group)
    _clone(st, io_sv, io_snap, c)
    _register_clone(st, c, io_sv, io_snap)

    # a clone of a subvolume that is then removed with --retain-snapshots
    # (in _setup_purges())
    rsv = st.retained[0]
    _create_retained(st, rsv, 500)
    c = Subvol(*RETAINED_CLONE)
    _clone(st, rsv, SNAPNAME, c)
    _register_clone(st, c, rsv, SNAPNAME)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'yes')
    assert len(st.clones) <= MAX_CONCURRENT_CLONES


def _register_clone(st, c, src, snap):
    """
    Record a clone (issued with snapshot_clone_delay set) to be held pending
    through the upgrade.
    """
    c.data_path = _read_meta(st, c).get('GLOBAL', 'path').lstrip('/')
    assert _clone_state(st, c) == 'pending'
    st.clones.append(c)
    st.clone_of[c.name] = (src, snap)


def _wait_for_clones(st, verify):
    """
    Wait for the clones held pending through the upgrade to finish, and
    verify their data (verify(c) returns the clone's data dir).
    """
    for c in st.clones:
        _wait_for_clone(st, c)
        src, snap = st.clone_of[c.name]
        _assert_clone_data(st, c, src, snap, path=verify(c))
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'no')


def _assert_clones_pending(st):
    for c in st.clones:
        state = _clone_state(st, c)
        copied = _count_files(st, c.data_path)
        log.info(f'clone {c}: state = {state}, copied {copied}')
        assert state == 'pending' and copied == 0, \
            f'clone {c} was not held pending (state = {state}, copied {copied})'


def _setup_purges(st):
    for sv in st.purges:
        if sv.group:
            _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
        _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}')
        _, path = _getpath(st, sv)
        sv.data_path = path.lstrip('/')
        _populate(st, sv.data_path, st.purge_files)
    _create_retained(st, st.retained[1], st.purge_files)

    _set_purging_paused(st, True)
    for sv in st.purges:
        _ceph(st, f'fs subvolume rm {VOLNAME} {sv.name}{sv.grp_arg}')
    # moves their data dirs (but not their snapshots) to the trash; the
    # (first) one has a clone of its snapshot pending
    for sv in st.retained:
        _ceph(st, f'fs subvolume rm {VOLNAME} {sv.name}{sv.grp_arg} '
                  '--retain-snapshots')
        sv.retained = True
        _assert_retained_pre_v3(st, sv)
    total = _count_files(st, TRASH_DIR)
    assert _trash_entries(st) == len(st.purges) + len(st.retained)

    # as with clones, catch the purges mid-way
    _set_purging_paused(st, False)
    _wait_for('purges to make progress',
              lambda: _count_files(st, TRASH_DIR) <= total * 0.9,
              timeout=600, sleep=1)
    _set_purging_paused(st, True)
    left = _count_files(st, TRASH_DIR)
    log.info(f'purges paused with {left}/{total} files and '
             f'{_trash_entries(st)} trash entries left')
    assert left > 0, 'purges were not caught mid-way, increase purge_files'


def _create_retained(st, sv, nfiles):
    """
    Create a (v2, regardless of the version of the other subvolumes) subvolume
    with a snapshot, to be removed with --retain-snapshots.
    """
    if sv.group:
        _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
    _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}')
    sv.data_path = _getpath(st, sv)[1].lstrip('/')
    _populate(st, sv.data_path, nfiles)
    _pre_v3_snap_create(st, sv, SNAPNAME)
    _record_snap_info(st, sv)


def _assert_retained(st, sv):
    """
    The subvolume, removed with --retain-snapshots, is listed (with its
    snapshots) and reported as such.
    """
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    info = json.loads(out)
    assert info.get('state') == STATE_RETAINED, f'{sv}: info = {info}'
    _, out = _ceph(st, f'fs subvolume ls {VOLNAME}{sv.grp_arg}')
    names = {s['name'] for s in json.loads(out)}
    assert sv.name in names, f'{sv}: not in subvolume ls: {names}'
    _assert_snaps(st, sv)


def _assert_retained_pre_v3(st, sv):
    """
    The subvolume, removed with --retain-snapshots, is still v2 (its data dir
    gone, its snapshots in place), and is reported as such.
    """
    ftype = _admin_sh(st, f'stat -c %F {sv.base}/.meta').strip()
    assert ftype == 'regular file', f'{sv}: .meta is a {ftype}'
    glob = dict(_read_meta(st, sv).items('GLOBAL'))
    assert (glob.get('version'), glob.get('state'), glob.get('path')) == \
        ('2', STATE_RETAINED, ''), f'{sv}: meta = {glob}'
    out = _admin_sh(st, f'test -e {sv.data_path} && echo yes || echo no; '
                        f'test -e {sv.base}/roots && echo yes || echo no')
    assert out.split() == ['no', 'no'], \
        f'{sv}: data dir, roots dir present?: {out.split()}'
    for snap in st.snaps[sv.name]:
        path = _pre_v3_snap_path(sv, snap)
        out = _admin_sh(st, f'test -d {path} && echo yes || echo no').strip()
        assert out == 'yes', f'{sv}: retained snapshot {snap} not at {path}'
    _assert_retained(st, sv)


def _verify_retained(st, sv, clone=None):
    """
    After auto-upgrade: the retained snapshot can still be cloned (into
    clone, if given), and removing it, the last one, removes the subvolume.
    """
    _assert_retained(st, sv)
    log.info(f'{sv}: after auto-upgrade:\n' +
             _admin_sh(st, f'ls -la {sv.base} {sv.base}/roots 2>&1 || true; '
                           f'cat {sv.base}/.meta'))
    if clone:
        _clone(st, sv, SNAPNAME, clone)
        _wait_for_clone(st, clone)
        _assert_clone_data(st, clone, sv, SNAPNAME, path=_assert_v3(st, clone))
        _assert_pending_clones(st, sv, SNAPNAME, 'no')
    _ceph(st, f'fs subvolume snapshot rm {VOLNAME} {sv.name} {SNAPNAME}{sv.grp_arg}')
    st.snaps[sv.name].discard(SNAPNAME)
    rc, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                    check_status=False)
    assert rc != 0, f'{sv}: still exists after removing its last snapshot: {out}'
    _, out = _ceph(st, f'fs subvolume ls {VOLNAME}{sv.grp_arg}')
    names = {s['name'] for s in json.loads(out)}
    assert sv.name not in names, f'{sv}: still listed: {names}'


def _authorize(st, sv, auth_id):
    _ceph(st, f'fs subvolume authorize {VOLNAME} {sv.name} {auth_id}'
              f'{sv.grp_arg} --access_level rw')
    # kernel clients may not support the default key type of newer
    # releases; same as the kclient task.
    _ceph(st, f'auth rotate client.{auth_id} --key-type=aes', check_status=False)
    _, keyring = _ceph(st, f'auth get client.{auth_id}')
    return keyring


def _setup_io(st):
    for i, (mount, sv) in enumerate(zip(st.io_mounts, st.io_svs)):
        _create_pre_v3(st, sv)
        # mount the subvolume using its own (path restricted) credentials
        keyring = _pre_v3_authorize(st, sv, f'svio{i}')
        _remount(st, mount, sv, sv.data_path, client_id=f'svio{i}',
                 client_keyring=keyring)
        st.writers.append(_start_writer(st, mount, sv))

    for w, nsnaps in zip(st.writers, IO_NSNAPS):
        _wait_for_writer_progress(w, 20)
        for j in range(nsnaps):
            _pre_v3_snap_create(st, w['sv'], f'snap_v{st.version}_{j}')
            _wait_for_writer_progress(w, 20)


# ----- pre-v3 subvolumes -----
#
# v1 subvolumes can't be created by any release that can be upgraded from,
# so they are fabricated in the layout ceph-mgr used to create them in. A
# v1 subvolume without snapshots must not be accessed through the old
# ceph-mgr before the upgrade, as it would upgrade it to v2 (the new
# ceph-mgr does the same when only it is upgraded, which is fine).


def _data_pool(st):
    return _admin_sh(st, 'getfattr --only-values -n ceph.dir.layout.pool .').strip()


def _create_v1(st, sv):
    """
    Create a v1 subvolume in the layout ceph-mgr used to (see
    SubvolumeV1.create()): data in <subvolume>/<uuid> (with the pool layout
    set on it), and a .meta file (version 1) in <subvolume>. It gets the
    SV_* attributes the way ceph-mgr would set them (`fs subvolume create`
    options, `fs subvolume metadata set`), and is marked a subvolume (on its
    data dir, see SubvolumeV1.mark_subvolume()).
    """
    sv.version = 1
    sv.data_path = f'{sv.base}/{uuid.uuid4()}'
    # as ceph-mgr names it (SubvolumeBase.namespace)
    sv.namespace = f'fsvolumens__{sv.group or "_nogroup"}_{sv.name}'
    meta = (f'[GLOBAL]\\nversion = 1\\ntype = subvolume\\n'
            f'path = /{sv.data_path}\\nstate = complete\\n\\n'
            '[USER_METADATA]\\n' +
            ''.join(f'{k} = {v}\\n' for k, v in SV_METADATA.items()))
    _admin_sh(st, dedent(f'''\
        set -e
        mkdir -p -m 755 {sv.data_path}
        setfattr -n ceph.dir.layout.pool -v {_data_pool(st)} {sv.data_path}
        setfattr -n ceph.dir.subvolume -v 1 {sv.data_path}
        setfattr -n ceph.dir.layout.pool_namespace -v {sv.namespace} {sv.data_path}
        setfattr -n ceph.quota.max_bytes -v {SV_SIZE} {sv.data_path}
        setfattr -n {EARMARK_XATTR} -v {SV_EARMARK} {sv.data_path}
        setfattr -n {SV_XATTR[0]} -v {SV_XATTR[1]} {sv.data_path}
        chown {SV_UID}:{SV_GID} {sv.data_path}
        chmod {SV_MODE:o} {sv.data_path}
        printf "{meta}" > {sv.base}/.meta
    '''))
    log.info(f'{sv}: created v1 subvolume at {sv.data_path}:\n'
             f'{_admin_sh(st, f"cat {sv.base}/.meta")}')


def _create_pre_v3(st, sv):
    if sv.group:
        _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
    sv.attrs = True
    if st.version == 1:
        _create_v1(st, sv)
    else:
        _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg} '
                  f'--size {SV_SIZE} --uid {SV_UID} --gid {SV_GID} '
                  f'--mode {SV_MODE:o} --namespace-isolated '
                  f'--earmark {SV_EARMARK}')
        for k, v in SV_METADATA.items():
            _ceph(st, f'fs subvolume metadata set {VOLNAME} {sv.name} {k} {v}'
                      f'{sv.grp_arg}')
        sv.data_path = _getpath(st, sv)[1].lstrip('/')
        sv.namespace = _admin_sh(
            st, 'getfattr --only-values -n ceph.dir.layout.pool_namespace '
                f'{sv.data_path}').strip()
        assert sv.namespace, f'{sv}: no pool namespace'
        _admin_sh(st, f'setfattr -n {SV_XATTR[0]} -v {SV_XATTR[1]} {sv.data_path}')
    assert sv.version == st.version
    _set_common_attrs(st, sv)
    # the data dir must be in a subvolume
    probe = _probe_boundary(st, sv, sv.data_path)
    assert probe['group'] == 'EXDEV', f'{sv}: not marked a subvolume: {probe}'


def _pre_v3_authorize(st, sv, auth_id):
    if st.version == 2:
        return _authorize(st, sv, auth_id)
    # same caps as `fs subvolume authorize` (on a v1 subvolume) gives
    _ceph(st, f'auth get-or-create client.{auth_id} mon "allow r" '
              f'mds "allow rw path=/{sv.data_path}" '
              f'osd "allow rw pool={_data_pool(st)} namespace={sv.namespace}"')
    _ceph(st, f'auth rotate client.{auth_id} --key-type=aes', check_status=False)
    _, keyring = _ceph(st, f'auth get client.{auth_id}')
    return keyring


def _pre_v3_snap_create(st, sv, snap):
    """
    Snapshot a pre-v3 subvolume through the old ceph-mgr, except for the
    first snapshot of a v1 subvolume (taken directly in its data dir, as v1
    does), which keeps the old ceph-mgr from upgrading it to v2.
    """
    direct = sv.version == 1 and not st.snaps.get(sv.name)
    _snap_create(st, sv, snap, pre_v3=True, direct=direct)
    _ceph(st, f'fs subvolume snapshot metadata set {VOLNAME} {sv.name} {snap} '
              f'{SNAP_METADATA_KEY} {snap}{sv.grp_arg}')


def _teardown(st):
    """
    Stop whatever setup left running (writers, extra sessions) and remove the
    files it created in the test dir, so that the client mounts can be
    unmounted and the test dir removed, whether or not the job failed.
    """
    try:
        _kill_writers(st)
    except Exception:
        log.exception('failed to kill writers')
    try:
        _cleanup_evict_mounts(st)
    except Exception:
        log.exception('failed to unmount extra sessions')
    remotes = {m.client_remote for m in st.io_mounts + [st.admin]}
    for remote in remotes:
        # --one-file-system: don't descend into an extra session's mount
        # that failed to unmount
        remote.run(args=['sudo', 'bash', '-c',
                         f'rm -rf --one-file-system {st.testdir}/volumes_upgrade.*'],
                   check_status=False)


# ----- sub-tasks -----


@contextlib.contextmanager
def setup(ctx, config):
    """
    On the old release, create v1 or v2 (per `version`) subvolumes with
    clients doing IO in them and a v1/v2 clone source, pending (delayed)
    clones (of the clone source, of an IO subvolume and of a v2 subvolume
    removed with retained snapshots), and in-progress (paused) purges (of v2
    subvolumes, and of the data dirs of v2 subvolumes removed with retained
    snapshots, as the old ceph-mgr creates clones as v2, upgrades a v1
    subvolume to v2 before removing it, and can't retain v1 snapshots).

    Example::

        tasks:
        - volumes_upgrade.setup:
            version: 1
            io_clients: [client.0, client.1, client.2]
            admin_client: client.3
    """
    config = config or {}
    st = State(ctx, config)
    ctx[STATE_KEY] = st
    try:
        _setup(st)
        yield
    finally:
        _teardown(st)


def _setup(st):
    _setup_io(st)
    _setup_evict(st)
    _setup_clones(st)
    _setup_purges(st)
    _assert_clones_pending(st)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'yes')
    _set_mgr_config(st, 'snapshot_clone_delay', 0)

    for sv in st.io_svs + [st.src]:
        if sv.version == 1 and not st.snaps.get(sv.name):
            _assert_pre_v3(st, sv, strict=True)
        else:
            # the old ceph-mgr must handle a v1 subvolume with snapshots as v1
            # (checks the attributes too)
            _assert_usable(st, sv)
            _record_attrs(st, sv)
            _record_snap_info(st, sv)
    _assert_authorized(st)
    for c in st.clones:
        _assert_pre_v3(st, c)
    _ceph(st, f'fs subvolume ls {VOLNAME}')


@_kill_writers_on_failure
def check_io(ctx, config):
    """
    Verify that the subvolume clients are still doing IO.
    """
    st = _state(ctx)
    for w in st.writers:
        _wait_for_writer_progress(w, 10)


@_kill_writers_on_failure
def verify_mgr_only(ctx, config):
    """
    With only ceph-mgr upgraded, auto-upgrade can't set
    ceph.dir.subvolume.prevpath on the old MDS. The v1/v2 subvolumes must
    keep working as such: client IO, subvolume commands, clones and purges.
    """
    st = _state(ctx)
    check_io(ctx, config)

    for sv in st.io_svs + [st.src]:
        _assert_usable(st, sv)
    _assert_authorized(st)

    # a v2 snapshot taken by the new mgr
    _snap_create(st, st.src, SNAP_MGR_ONLY)
    _assert_usable(st, st.src)

    # the new mgr does not honor pause_purging on startup, so the purges may
    # already be running. resume them explicitly anyway.
    _set_purging_paused(st, False)

    _wait_for('purges to finish with old MDS',
              lambda: _trash_entries(st) == 0, timeout=1800, sleep=10)
    for sv in st.purges:
        rc, _ = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                      check_status=False)
        assert rc != 0, f'purged subvolume {sv} still exists'

    def verify(c):
        _assert_usable(st, c)
        return c.data_path
    _wait_for_clones(st, verify)
    for sv in st.retained:
        _assert_retained_pre_v3(st, sv)

    # a new clone of a v1/v2 snapshot, by the new mgr
    c = Subvol('sv_clone_mgr_only', 'grp_clone')
    _clone(st, st.src, SNAPNAME_2, c)
    _wait_for_clone(st, c)
    _assert_clone_data(st, c, st.src, SNAPNAME_2)
    _assert_snaps(st, c)
    st.late_clones.append((c, st.src, SNAPNAME_2))
    for snap in (SNAPNAME, SNAPNAME_2):
        _assert_pending_clones(st, st.src, snap, 'no')

    for sv in st.io_svs:
        _assert_usable(st, sv)
    check_io(ctx, config)


@_kill_writers_on_failure
def post_upgrade(ctx, config):
    """
    With the whole cluster upgraded, verify auto-upgrade of the subvolumes
    while their clients do IO, that the clones and purges finish, and that
    snapshots taken before and after auto-upgrade are all listed.
    """
    st = _state(ctx)
    check_io(ctx, config)
    _set_purging_paused(st, False)

    # first subvolume command on each subvolume triggers its auto-upgrade;
    # the writers must carry on regardless.
    for w in st.writers:
        sv = w['sv']
        _assert_attrs(st, sv, _assert_v3(st, sv))
        _wait_for_writer_progress(w, 20)
    _assert_authorized(st)

    _wait_for_clones(st, lambda c: _assert_v3(st, c))
    for c, src, snap in st.late_clones:
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))

    _assert_attrs(st, st.src, _assert_v3(st, st.src))

    # where the upgraded subvolumes are marked as such must be the same as
    # for a subvolume created as v3
    fresh = Subvol('sv_fresh_v3', 'grp_io')
    _ceph(st, f'fs subvolume create {VOLNAME} {fresh.name}{fresh.grp_arg}')
    fresh_path = _assert_v3(st, fresh)
    _admin_sh(st, f'touch {fresh_path}/{BOUNDARY_FILE}')
    expected = _probe_boundary(st, fresh, fresh_path)
    for sv in st.io_svs + [st.src]:
        probe = _probe_boundary(st, sv, _getpath(st, sv)[1].lstrip('/'))
        assert probe == expected, \
            f'{sv}: subvolume boundary {probe}, expected {expected} (as for v3)'

    # v1/v2 snapshots must still be listed after auto-upgrade (with their
    # metadata and info intact), and listed along with v3 snapshots once
    # those are taken.
    late = [c for c, _, _ in st.late_clones]
    for sv in st.io_svs + [st.src] + st.clones + late:
        _assert_snaps(st, sv)
        _snap_create(st, sv, SNAP_V3)
        _assert_snaps(st, sv)

    # clones of v1/v2 snapshots (taken pre-upgrade) must still work
    io_src = st.io_svs[-1]
    io_snap = f'snap_v{st.version}_{IO_NSNAPS[-1] - 1}'
    for c, src, snap in [(Subvol('sv_clone_post_0', None), st.src, SNAPNAME),
                         (Subvol('sv_clone_post_1', 'grp_clone'), st.src, SNAPNAME_2),
                         (Subvol('sv_clone_post_2', 'grp_clone'), io_src, io_snap)]:
        _clone(st, src, snap, c)
        _wait_for_clone(st, c)
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))
    for snap in (SNAPNAME, SNAPNAME_2):
        _assert_pending_clones(st, st.src, snap, 'no')
    _assert_pending_clones(st, io_src, io_snap, 'no')

    _wait_for('purges to finish', lambda: _trash_entries(st) == 0,
              timeout=1800, sleep=10)
    for sv in st.purges:
        rc, _ = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                      check_status=False)
        assert rc != 0, f'purged subvolume {sv} still exists'

    # the subvolumes removed with --retain-snapshots (whose data dirs have
    # been purged by now): clone the retained snapshot again, then remove it
    # (along with the subvolume)
    _verify_retained(st, st.retained[0], Subvol('sv_clone_post_retained', 'grp_clone'))
    _verify_retained(st, st.retained[1])
    _wait_for('subvolumes with retained snapshots to be purged',
              lambda: _trash_entries(st) == 0, timeout=1800, sleep=10)

    for w in st.writers:
        sv = w['sv']
        written = _stop_writer(w)
        path = _getpath(st, sv)[1].lstrip('/')
        found = _count_files(st, f'{path}/io')
        log.info(f'{sv}: writer wrote {written} files, found {found} at {path}')
        assert found == written, f'{sv}: wrote {written} files, found {found}'

    # clients may choose to remount, in which case they mount the new v3
    # path, with either the credentials they had been using (authorized for
    # the v2 path) or with ones authorized after the upgrade.
    for i, w in enumerate(st.writers):
        sv, mount = w['sv'], w['mount']
        path = _getpath(st, sv)[1].lstrip('/')
        _remount_and_verify(st, w, path, mount.client_id,
                            client_keyring_path=mount.client_keyring_path)

        auth_id = f'svio{i}new'
        keyring = _authorize(st, sv, auth_id)
        _, out = _ceph(st, f'auth get client.{auth_id} --format=json')
        mds_caps = json.loads(out)[0]['caps']['mds']
        assert f'path=/{path}' in mds_caps, \
            f'{sv}: caps of client.{auth_id} not for the v3 path: {mds_caps}'
        _remount_and_verify(st, w, path, auth_id, client_keyring=keyring)
    _assert_authorized(st, post=True)

    for i, sv in enumerate(st.io_svs):
        _verify_deauthorize_evict(st, i, _getpath(st, sv)[1].lstrip('/'))

    _ceph(st, f'fs subvolume ls {VOLNAME}')
