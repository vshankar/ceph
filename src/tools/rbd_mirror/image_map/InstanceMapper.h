// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAPPER_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAPPER_H

#include <map>
#include <list>
#include <vector>
#include <memory>

#include "common/Mutex.h"
#include "common/AsyncOpTracker.h"
#include "include/rados/librados.hpp"

#include "Policy.h"
#include "tools/rbd_mirror/ImageDeleter.h"

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;
template <typename> class InstanceWatcher;
template <typename> class InstanceReplayer;

namespace image_map {

typedef rbd::mirror::RadosRef RadosRef;

template <typename ImageCtxT = librbd::ImageCtx>
class InstanceMapper {
public:
  static InstanceMapper *create(
    rbd::mirror::Threads<ImageCtxT> *threads,
    std::shared_ptr<rbd::mirror::ImageDeleter> image_deleter,
    rbd::mirror::ImageSyncThrottlerRef<ImageCtxT> image_sync_throttler,
    const rbd::mirror::peer_t &peer, RadosRef local_rados, RadosRef remote_rados,
    int64_t local_pool_id, librados::IoCtx &local_io_ctx, librados::IoCtx &remote_io_ctx) {
    return new InstanceMapper(
      threads, image_deleter, image_sync_throttler, peer, local_rados,
      remote_rados, local_pool_id, local_io_ctx, remote_io_ctx);
  }

  int init();
  void add_instances(std::vector<std::string> &instance_ids, Context *on_finish);
  void remove_instances(std::vector<std::string> &instance_ids, Context *on_finish);
  void handle_update(const std::string &mirror_uuid, ImageIds &&added_image_ids,
                     ImageIds &&removed_image_ids, Context *on_finish);

private:
  typedef std::list<Context *> Contexts;

  InstanceMapper(
    rbd::mirror::Threads<ImageCtxT> *threads,
    std::shared_ptr<rbd::mirror::ImageDeleter> image_deleter,
    rbd::mirror::ImageSyncThrottlerRef<ImageCtxT> image_sync_throttler,
    const rbd::mirror::peer_t &peer, RadosRef local_rados, RadosRef remote_rados,
    int64_t local_pool_id, librados::IoCtx &local_io_ctx, librados::IoCtx &remote_io_ctx);

  struct C_FinishUpdate : public Context {
    InstanceMapper<ImageCtxT> *m_instance_mapper;
    Context *m_on_finish;

    C_FinishUpdate(InstanceMapper<ImageCtxT> *instance_mapper,
                   Context *on_finish)
      : m_instance_mapper(instance_mapper), m_on_finish(on_finish) {
    }

    void finish(int r) override {
      m_on_finish->complete(r);
      m_instance_mapper->handle_update_finished();
    }
  };

  rbd::mirror::Threads<ImageCtxT> *m_threads;
  std::shared_ptr<rbd::mirror::ImageDeleter> m_image_deleter;
  rbd::mirror::ImageSyncThrottlerRef<ImageCtxT> m_image_sync_throttler;
  rbd::mirror::peer_t m_peer;
  RadosRef m_local_rados;
  RadosRef m_remote_rados;
  int64_t m_local_pool_id;
  librados::IoCtx &m_local_io_ctx;
  librados::IoCtx &m_remote_io_ctx;

  std::unique_ptr<rbd::mirror::InstanceReplayer<ImageCtxT> > m_instance_replayer;
  std::unique_ptr<rbd::mirror::InstanceWatcher<ImageCtxT> > m_instance_watcher;
  AsyncOpTracker m_update_op_tracker;

  Mutex m_lock;
  bool m_bootstrap;
  bool m_update_in_progress;
  Contexts m_blocked_requests;

  bool m_peer_uuid_available;
  std::string m_cached_peer_uuid;

  std::unique_ptr<InstanceMap> m_instance_map;

  // instances that failed mirror uuid update notifications
  // should not receive (re)mapped images
  std::list<std::string> m_ignore_instance_ids;

  void handle_update_finished();
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAPPER_H
