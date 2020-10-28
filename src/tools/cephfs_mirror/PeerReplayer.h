// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_PEER_REPLAYER_H
#define CEPHFS_MIRROR_PEER_REPLAYER_H

#include "common/Formatter.h"
#include "common/Thread.h"
#include "mds/FSMap.h"
#include "Types.h"

class CephContext;

namespace cephfs {
namespace mirror {

class FSMirror;
class PeerReplayerAdminSocketHook;

class PeerReplayer {
public:
  PeerReplayer(CephContext *cct, FSMirror *fs_mirror,
               const Filesystem &filesystem, const Peer &peer,
               const std::set<std::string, std::less<>> &directories,
               MountRef mount);
  ~PeerReplayer();

  // initialize replayer for a peer
  int init();

  // shutdown replayer for a peer
  void shutdown();

  // add a directory to mirror queue
  void add_directory(string_view dir_path);

  // remove a directory from queue
  void remove_directory(string_view dir_path);

  // admin socket helpers
  void peer_status(Formatter *f);

private:
  inline static const std::string PRIMARY_SNAP_ID_KEY = "primary_snap_id";

  bool is_stopping() {
    return m_stopping;
  }

  struct Replayer;
  class SnapshotReplayerThread : public Thread {
  public:
    SnapshotReplayerThread(PeerReplayer *peer_replayer)
      : m_peer_replayer(peer_replayer) {
    }

    void *entry() override {
      m_peer_replayer->run(this);
      return 0;
    }

  private:
    PeerReplayer *m_peer_replayer;
  };

  struct DirRegistry {
    int fd;
    SnapshotReplayerThread *replayer;
  };

  struct SyncEntry {
    std::string epath;
    ceph_dir_result *dirp; // valid for directories
    struct ceph_statx stx;

    SyncEntry(std::string_view path,
              const struct ceph_statx &stx)
      : epath(path),
        stx(stx) {
    }
    SyncEntry(std::string_view path,
              ceph_dir_result *dirp,
              const struct ceph_statx &stx)
      : epath(path),
        dirp(dirp),
        stx(stx) {
    }

    bool is_directory() const {
      return S_ISDIR(stx.stx_mode);
    }
  };

  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  struct SnapSyncStat {
    boost::optional<std::pair<uint64_t, std::string>> last_synced_snap;
    boost::optional<std::pair<uint64_t, std::string>> current_syncing_snap;
    uint64_t synced_snap_count = 0;
    uint64_t deleted_snap_count = 0;
    time last_synced = clock::zero();
    boost::optional<double> last_sync_duration;
  };

  void _set_last_synced_snap(std::string_view dir_path, uint64_t snap_id,
                            const std::string &snap_name) {
    auto &sync_stat = m_snap_sync_stats.at(std::string(dir_path));
    sync_stat.last_synced_snap = std::make_pair(snap_id, snap_name);
    sync_stat.current_syncing_snap = boost::none;
  }
  void set_last_synced_snap(std::string_view dir_path, uint64_t snap_id,
                            const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_path, snap_id, snap_name);
  }
  void set_current_syncing_snap(std::string_view dir_path, uint64_t snap_id,
                                const std::string &snap_name) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(std::string(dir_path));
    sync_stat.current_syncing_snap = std::make_pair(snap_id, snap_name);
  }
  void inc_deleted_snap(std::string_view dir_path) {
    std::scoped_lock locker(m_lock);
    auto &sync_stat = m_snap_sync_stats.at(std::string(dir_path));
    ++sync_stat.deleted_snap_count;
  }
  void set_last_synced_stat(std::string_view dir_path, uint64_t snap_id,
                            const std::string &snap_name, double duration) {
    std::scoped_lock locker(m_lock);
    _set_last_synced_snap(dir_path, snap_id, snap_name);
    auto &sync_stat = m_snap_sync_stats.at(std::string(dir_path));
    sync_stat.last_synced = clock::now();
    sync_stat.last_sync_duration = duration;
    ++sync_stat.synced_snap_count;
  }

  typedef std::vector<std::unique_ptr<SnapshotReplayerThread>> SnapshotReplayers;

  CephContext *m_cct;
  FSMirror *m_fs_mirror;
  Peer m_peer;
  // probably need to be encapsulated when supporting cancelations
  std::map<std::string, DirRegistry> m_registered;
  std::vector<std::string> m_directories;
  std::map<std::string, SnapSyncStat> m_snap_sync_stats;
  MountRef m_local_mount;
  PeerReplayerAdminSocketHook *m_asok_hook = nullptr;

  ceph::mutex m_lock;
  ceph::condition_variable m_cond;
  RadosRef m_remote_cluster;
  MountRef m_remote_mount;
  bool m_stopping = false;
  SnapshotReplayers m_replayers;

  void run(SnapshotReplayerThread *replayer);

  boost::optional<std::string> pick_directory();
  int register_directory(std::string_view dir_path, SnapshotReplayerThread *replayer);
  void unregister_directory(std::string_view dir_path);
  int try_lock_directory(std::string_view dir_path, SnapshotReplayerThread *replayer,
                         DirRegistry *registry);
  void unlock_directory(std::string_view dir_path, const DirRegistry &registry);
  void sync_snaps(std::string_view dir_path, std::unique_lock<ceph::mutex> &locker);

  int do_sync_snaps(std::string_view dir_path);
  int build_snap_map(std::string_view, std::map<uint64_t, std::string> *snap_map,
                     bool is_remote=false);
  int propagate_snap_deletes(std::string_view dir_name, const std::set<std::string> &snaps);
  int propagate_snap_renames(std::string_view dir_name,
                             const std::set<std::pair<std::string,std::string>> &snaps);
  int synchronize(std::string_view dir_path, uint64_t snap_id, std::string_view snap_name);
  int do_synchronize(const std::string &path, const std::string &snap_name);

  int cleanup_remote_dir(const std::string &dir_path);
  int remote_mkdir(const std::string &local_path, const std::string &remote_path,
                   const struct ceph_statx &stx);
  int remote_file_op(const std::string &local_path, const std::string &remote_path,
                     const struct ceph_statx &stx);
  int remote_copy(const std::string &local_path,const std::string &remote_path,
                  const struct ceph_statx &local_stx);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H
