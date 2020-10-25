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
    bool is_dir = false;

    SyncEntry(std::string_view path)
      : epath(path) {
    }
    SyncEntry(std::string_view path,
              ceph_dir_result *dirp,  bool is_dir)
      : epath(path),
        dirp(dirp),
        is_dir(is_dir) {
      ceph_assert(is_dir);
    }

    bool is_directory() const {
      return is_dir;
    }
  };

  typedef std::vector<std::unique_ptr<SnapshotReplayerThread>> SnapshotReplayers;

  CephContext *m_cct;
  FSMirror *m_fs_mirror;
  Peer m_peer;
  // probably need to be encapsulated when supporting cancelations
  std::map<std::string, DirRegistry> m_registered;
  std::vector<std::string> m_directories;
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
  int remote_mkdir(const std::string &local_path, const std::string &remote_path);
  int remote_file_op(const std::string &local_path, const std::string &remote_path);

  int remote_copy(const std::string &local_path,const std::string &remote_path,
                  const struct ceph_statx &local_stx);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_PEER_REPLAYER_H
