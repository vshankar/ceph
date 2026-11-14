"""
check that removing an export releases its CephFS mount on every ganesha

Every ganesha daemon of an nfs cluster mounts CephFS (libcephfs) for an
export, whether or not NFS clients use the export through that daemon.
When the export is removed, each daemon has to unmount it once nothing
references the export any more, which closes its CephFS session. A
daemon that keeps a reference keeps the mount and the session. The
session outlives the cephx user that mgr/nfs removes with the export, so
the client can no longer get monitor maps, misses MDS failovers, and the
MDS holds the ranks it has sessions on until it autocloses the session.

The task creates an export, writes to it over NFS through one ganesha
daemon for io_seconds, removes the export and checks that every ganesha
daemon closes the export's CephFS session within release_timeout
seconds. The nfs cluster needs at least two ganesha daemons, on
different hosts.

With cluster_qos, Per_Export bandwidth control is enabled for the nfs
cluster and for the export before the writes. Ganesha's Cluster QoS then
sends the export's bandwidth use from the daemon serving the writes to
the other daemons. That needs Cluster_Members (see ganesha-reconf) and a
ganesha built with ENABLE_QOS and ENABLE_CLUSTER_QOS; the task fails
early if Cluster QoS is not running.

If a daemon keeps a session, the task evicts the session and restarts
that daemon so that the teardown finds a clean cluster, and then fails.

  - ganesha-export-rm:
      cluster_id: <nfs cluster id>
      nfs_client: client.0       # host that mounts the export
      fs_name: cephfs
      cluster_qos: false         # enable Per_Export Cluster QoS first
      qos_bw: 10GiB              # export bandwidth limit with cluster_qos
      io_seconds: 30             # seconds of NFS writes before the removal
      release_timeout: 120       # seconds for every session to go
      timeout: 300               # seconds for restarts and export loading
"""

import json
import logging
import os
import re
import time
from io import StringIO

from teuthology import misc
from teuthology.task import Task

from tasks.ganesha_thrash import GaneshaDaemon, _host_remote, _nfs_daemons, _nfs_port

log = logging.getLogger(__name__)

PSEUDO = '/export-rm'
# ganesha's default Cqos_Program
CQOS_PROGRAM = 100062

class GaneshaExportRm(Task):
    # for overrides.ganesha-export-rm
    name = 'ganesha-export-rm'

    def __init__(self, ctx, config):
        super(GaneshaExportRm, self).__init__(ctx, config)
        self.log = log
        self.mounted = None     # NFS mount point on the nfs client

    def _ceph(self, args, stdin=None):
        proc = self.mon0_remote.run(args=['ceph', *args], stdin=stdin, stdout=StringIO())
        return proc.stdout.getvalue()

    def _wait(self, cond, timeout, what):
        start = time.time()
        while time.time() - start < timeout:
            if cond():
                return
            time.sleep(2)
        raise RuntimeError(f'timed out after {timeout}s waiting for {what}')

    def _active_ranks(self):
        mdsmap = json.loads(self._ceph(['fs', 'get', self.fs_name, '--format=json']))['mdsmap']
        return sorted(info['rank'] for info in mdsmap['info'].values()
                      if info['state'] == 'up:active')

    def _session_host(self, session):
        addr = session.get('entity', {}).get('addr', {}).get('addr')
        if addr:
            addr = addr.rsplit(':', 1)[0].strip('[]')
        else:
            m = re.search(r'([0-9.]+):\d+/\d+', session.get('inst', ''))
            addr = m.group(1) if m else None
        if addr in self.hostnames:
            return self.hostnames[addr]
        hostname = session.get('client_metadata', {}).get('hostname')
        return hostname.split('.')[0] if hostname else addr

    def _sessions(self):
        """session id -> (rank, host) of the CephFS sessions of the export's cephx user"""
        found = {}
        for rank in self._active_ranks():
            sessions = json.loads(self._ceph(['tell', f'mds.{self.fs_name}:{rank}',
                                              'session', 'ls', '--format=json']))
            for s in sessions:
                if s.get('client_metadata', {}).get('entity_id') == self.user_id:
                    found[s['id']] = (rank, self._session_host(s))
        return found

    def _wait_for_restart(self, before):
        started = {d['daemon_name']: d.get('started') for d in before}

        def restarted():
            daemons = _nfs_daemons(self.ctx, self.mon0_remote, self.cluster_id)
            return daemons and all(d.get('status_desc') == 'running' and
                                   d.get('started') != started.get(d['daemon_name'])
                                   for d in daemons)
        self._wait(restarted, self.timeout, f'nfs.{self.cluster_id} to restart')
        for g in self.ganeshas:
            self._wait(lambda g=g: g.is_active() and g.is_listening(self.port),
                       self.timeout, f'{g.name} to listen on {self.port}')

    def _enable_cluster_qos(self):
        before = _nfs_daemons(self.ctx, self.mon0_remote, self.cluster_id)
        # mgr/nfs restarts the nfs service to apply this
        self._ceph(['nfs', 'cluster', 'qos', 'enable', 'bandwidth_control', self.cluster_id,
                    'PerShare', '--combined-rw-bw-ctrl', '--max_export_combined_bw', self.qos_bw])
        self._wait_for_restart(before)
        log.info('cluster qos: ' +
                 self._ceph(['nfs', 'cluster', 'qos', 'get', self.cluster_id]).strip())
        for g in self.ganeshas:
            path = os.path.join('/var/lib/ceph', self.fsid, g.name, 'etc/ganesha/ganesha.conf')
            conf = g.remote.run(args=['sudo', 'cat', path], stdout=StringIO()).stdout.getvalue()
            members = re.search(r'Cluster_Members\s*=\s*([^;]*);', conf)
            if not members:
                raise RuntimeError(f'{g.name}: no Cluster_Members in ganesha.conf, '
                                   f'set cluster_members in ganesha-reconf')
            port = re.search(r'Cqos_Port\s*=\s*(\d+)', conf)
            if not port:
                raise RuntimeError(f'{g.name}: no Cqos_Port in ganesha.conf')
            port = int(port.group(1))
            # ganesha opens the port only once Cluster QoS has started
            try:
                self._wait(lambda g=g, port=port: g.is_listening(port), 60,
                           f'{g.name} to listen on {port}')
            except RuntimeError:
                raise RuntimeError(f'{g.name} does not listen on its Cluster QoS port {port}: '
                                   f'ganesha is built without ENABLE_CLUSTER_QOS or Cluster '
                                   f'QoS did not start, see the ganesha log')
            log.info(f'{g.name}: Cluster QoS on port {port}, '
                     f'members {members.group(1).strip()}')

    def _create_export(self):
        self._ceph(['nfs', 'export', 'create', 'cephfs', '--cluster-id', self.cluster_id,
                    '--pseudo-path', PSEUDO, '--fsname', self.fs_name])
        info = json.loads(self._ceph(['nfs', 'export', 'info', self.cluster_id, PSEUDO]))
        self.export_id = info['export_id']
        self.user_id = info['fsal']['user_id']
        if self.cluster_qos:
            self._ceph(['nfs', 'export', 'qos', 'enable', 'bandwidth_control', self.cluster_id,
                        PSEUDO, '--combined-rw-bw-ctrl', '--max_export_combined_bw', self.qos_bw])
        log.info(f'export {PSEUDO}: id {self.export_id}, cephx user {self.user_id}')

        hosts = {self.daemon_host[g.name] for g in self.ganeshas}
        seen = {}

        def mounted_everywhere():
            seen.clear()
            seen.update(self._sessions())
            return hosts <= {host for _, host in seen.values()}
        try:
            self._wait(mounted_everywhere, self.timeout,
                       f'a CephFS session for {self.user_id} from each of {sorted(hosts)}')
        except RuntimeError as e:
            raise RuntimeError(f'{e}, found {seen}') from e

    def _umount(self):
        proc = self.nfs_remote.run(args=['sudo', 'timeout', '-s', 'KILL', '60', 'umount',
                                         self.mounted], check_status=False)
        if proc.exitstatus != 0:
            log.warning(f'umount of {self.mounted} failed ({proc.exitstatus}), forcing it')
            self.nfs_remote.run(args=['sudo', 'umount', '-f', '-l', self.mounted])
        self.nfs_remote.run(args=['rmdir', self.mounted], check_status=False)
        self.mounted = None

    def _write_load(self, ganesha):
        """write to the export over NFS through one ganesha daemon"""
        host = self.daemon_host[ganesha.name]
        mnt = os.path.join(misc.get_testdir(self.ctx), 'mnt.export-rm')
        self.nfs_remote.run(args=['mkdir', '-p', mnt])
        self.nfs_remote.run(args=['sudo', 'mount', '-t', 'nfs', '-o',
                                  f'port={self.port},vers=4.1',
                                  f'{self.host_addrs[host]}:{PSEUDO}', mnt])
        self.mounted = mnt
        log.info(f'writing to {PSEUDO} through {ganesha.name} for {self.io_seconds}s')
        proc = self.nfs_remote.run(
            args=['sudo', 'timeout', str(self.io_seconds), 'sh', '-c',
                  f'while :; do dd if=/dev/zero of={mnt}/load bs=1M count=64 '
                  f'conv=fsync status=none || exit 1; done'],
            check_status=False)
        if proc.exitstatus != 124:
            raise RuntimeError(f'NFS writes through {ganesha.name} failed ({proc.exitstatus})')
        self.nfs_remote.run(args=['sudo', 'rm', '-f', f'{mnt}/load'])
        self._umount()

    def _ganesha_log(self, ganesha):
        """what the debug log of a ganesha daemon says about the export"""
        path = os.path.join('/var/log/ceph', self.fsid, f'ganesha.{self.cluster_id}.log')
        # every request is logged with its RPC program; every export
        # reference taken or dropped with the count it left
        ref = f'export ref for id {self.export_id} '
        out = ganesha.remote.run(
            args=['sudo', 'sh', '-c',
                  f'test -f {path} || exit 0; '
                  f'echo $(grep -c "for Program {CQOS_PROGRAM}," {path}) '
                  f'$(grep -E "(get|put) {ref}" {path} | tail -n 1 | '
                  f'sed -n "s/.*exp_refcount = //p")'],
            stdout=StringIO(), check_status=False).stdout.getvalue().split()
        if not out:
            return 'no ganesha debug log'
        refcnt = out[1] if len(out) > 1 else 'not logged'
        return (f'its debug log has {out[0]} Cluster QoS requests and a last '
                f'exp_refcount of {refcnt} for export {self.export_id}')

    def _drop_kept(self, kept):
        """evict the kept sessions and restart the ganesha daemons that held them"""
        notes = {}
        for sid, (rank, _) in kept.items():
            self._ceph(['tell', f'mds.{self.fs_name}:{rank}', 'client', 'evict', f'id={sid}'])
        for g in self.ganeshas:
            if self.daemon_host[g.name] not in {host for _, host in kept.values()}:
                continue
            g.stop()
            status = g.remote.run(args=['sudo', 'systemctl', 'show', '-p', 'ExecMainStatus',
                                        '--value', g.unit],
                                  stdout=StringIO(), check_status=False).stdout.getvalue()
            notes[g.name] = f'was restarted (ExecMainStatus {status.strip()} after the stop)'
            g.start()
            self._wait(lambda g=g: g.is_active() and g.is_listening(self.port),
                       self.timeout, f'{g.name} to come back')
        return notes

    def begin(self):
        super(GaneshaExportRm, self).begin()

        self.cluster_id = self.config['cluster_id']
        self.fs_name = self.config.get('fs_name', 'cephfs')
        self.cluster_qos = bool(self.config.get('cluster_qos', False))
        self.qos_bw = str(self.config.get('qos_bw', '10GiB'))
        self.io_seconds = int(self.config.get('io_seconds', 30))
        self.release_timeout = int(self.config.get('release_timeout', 120))
        self.timeout = int(self.config.get('timeout', 300))
        nfs_client = self.config.get('nfs_client', 'client.0')

        first_mon = misc.get_first_mon(self.ctx, None)
        (self.mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()
        (self.nfs_remote,) = self.ctx.cluster.only(nfs_client).remotes.keys()
        self.fsid = self.ctx.ceph['ceph'].fsid

        hosts = json.loads(self._ceph(['orch', 'host', 'ls', '--format', 'json']))
        self.host_addrs = {h['hostname']: h['addr'] for h in hosts}
        self.hostnames = {h['addr']: h['hostname'] for h in hosts}

        daemons = sorted(_nfs_daemons(self.ctx, self.mon0_remote, self.cluster_id),
                         key=lambda d: d['daemon_name'])
        self.daemon_host = {d['daemon_name']: d['hostname'] for d in daemons}
        if len(set(self.daemon_host.values())) < 2:
            raise RuntimeError(f'need ganesha daemons on two hosts in nfs cluster '
                               f'{self.cluster_id}, found {self.daemon_host}')
        self.ganeshas = [GaneshaDaemon(self.fsid, d, _host_remote(self.ctx, d['hostname']))
                         for d in daemons]
        self.port = _nfs_port(self.mon0_remote, self.cluster_id)

        if self.cluster_qos:
            self._enable_cluster_qos()
        self._create_export()

        writer = self.ganeshas[0]
        self._write_load(writer)

        before = self._sessions()
        log.info(f'CephFS sessions of {self.user_id} before the removal: {before}')
        start = time.time()
        self._ceph(['nfs', 'export', 'rm', self.cluster_id, PSEUDO])
        while True:
            kept = self._sessions()
            elapsed = time.time() - start
            if not kept or elapsed >= self.release_timeout:
                break
            time.sleep(2)
        if not kept:
            log.info(f'all {len(before)} CephFS sessions of {self.user_id} closed '
                     f'{elapsed:.0f}s after the export was removed')
            return

        writer_host = self.daemon_host[writer.name]
        held = {}
        for sid, (rank, host) in kept.items():
            held.setdefault(host, []).append(f'{sid} on rank {rank}')
        details = []
        for g in self.ganeshas:
            host = self.daemon_host[g.name]
            if host in held:
                role = 'served the writes' if host == writer_host else 'had no NFS clients'
                details.append(f'{g.name} ({role}) kept session(s) '
                               f'{", ".join(held[host])}; {self._ganesha_log(g)}')
        notes = self._drop_kept(kept)
        for name, note in notes.items():
            details.append(f'{name} {note}')
        qos = 'with Per_Export Cluster QoS' if self.cluster_qos else 'without QoS'
        raise RuntimeError(f'export {PSEUDO} (id {self.export_id}, cephx user {self.user_id}) '
                           f'was removed {qos}, but {len(kept)} CephFS session(s) were still '
                           f'open after {self.release_timeout}s: ' + '; '.join(details))

    def end(self):
        super(GaneshaExportRm, self).end()
        if self.mounted:
            self._umount()

task = GaneshaExportRm
