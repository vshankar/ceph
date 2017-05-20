// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"

#include "librbd/Utils.h"
#include "InstanceMapper.h"
#include "AddPeerRequest.h"
#include "InstanceMapUpdateRequest.h"
#include "InstanceMapShuffleRequest.h"

#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/InstanceReplayer.h"

#include "SimplePolicy.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::InstanceMapper: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::unique_lock_name;

template<typename I>
InstanceMapper<I>::InstanceMapper(
  rbd::mirror::Threads<I> *threads,
  std::shared_ptr<rbd::mirror::ImageDeleter> image_deleter,
  rbd::mirror::ImageSyncThrottlerRef<I> image_sync_throttler,
  const rbd::mirror::peer_t &peer, RadosRef local_rados,
  RadosRef remote_rados, int64_t local_pool_id,
  librados::IoCtx &local_io_ctx, librados::IoCtx &remote_io_ctx)
  : m_threads(threads),
    m_image_deleter(image_deleter),
    m_image_sync_throttler(image_sync_throttler),
    m_peer(peer),
    m_local_rados(local_rados),
    m_remote_rados(remote_rados),
    m_local_pool_id(local_pool_id),
    m_local_io_ctx(local_io_ctx),
    m_remote_io_ctx(remote_io_ctx),
    m_lock(unique_lock_name
           ("rbd::mirror::image_map::InstanceMapper::m_lock", this)),
    m_bootstrap(true),
    m_update_in_progress(false) {
}

template<typename I>
int InstanceMapper<I>::init() {
  dout(20) << dendl;

  int r;
  std::string local_mirror_uuid;

  r = librbd::cls_client::mirror_uuid_get(&m_local_io_ctx,
                                          &local_mirror_uuid);
  if (r < 0) {
    derr << "failed to retrieve local mirror uuid from pool "
         << m_local_io_ctx.get_pool_name() << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  r = m_remote_rados->ioctx_create(m_local_io_ctx.get_pool_name().c_str(),
                                   m_remote_io_ctx);
  if (r < 0) {
    derr << "error accessing remote pool " << m_local_io_ctx.get_pool_name()
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  dout(20) << "connected to " << m_peer << dendl;
  m_cached_peer_uuid = "";

  m_instance_map.reset(InstanceMap::create(m_local_io_ctx, "simple"));

  m_instance_replayer.reset(InstanceReplayer<>::create(
                              m_remote_io_ctx, m_threads, m_image_deleter,
                              m_image_sync_throttler, m_local_rados,
                              local_mirror_uuid, m_local_pool_id));
  m_instance_replayer->init();
  m_instance_replayer->add_peer(m_peer.uuid);

  m_instance_watcher.reset(InstanceWatcher<>::create(
                             m_local_io_ctx, m_threads->work_queue,
                             m_instance_replayer.get()));
  r = m_instance_watcher->init();
  if (r < 0) {
    derr << "error initializing instance watcher: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template<typename I>
void InstanceMapper<I>::handle_update_finished() {
  dout(20) << dendl;

  Context *ctx = nullptr;
  {
    Mutex::Locker locker(m_lock);
    assert(m_update_in_progress);

    if (m_blocked_requests.empty()) {
      m_update_in_progress = false;
      return;
    }

    ctx = m_blocked_requests.front();
    m_blocked_requests.pop_front();
  }

  if (ctx != nullptr) {
    m_threads->work_queue->queue(ctx, 0);
  }
}

template<typename I>
void InstanceMapper<I>::add_instances(std::vector<std::string> &instance_ids,
                                      Context *on_finish) {
  dout(20) << dendl;

  on_finish = new FunctionContext([this, instance_ids, on_finish](int r) {
      InstanceMapShuffleRequest<I> *req = InstanceMapShuffleRequest<I>::create(
        m_local_io_ctx, m_instance_map.get(), m_peer.uuid,
        m_instance_watcher.get(), instance_ids,
        InstanceMap::INSTANCES_ADDED, m_bootstrap,
        m_threads->work_queue, new C_FinishUpdate(this, on_finish));

      req->send();
    });

  on_finish = new FunctionContext([this, instance_ids, on_finish](int r) {
      if (m_cached_peer_uuid == "") {
        on_finish->complete(0);
        return;
      }

      AddPeerRequest<I> *req = AddPeerRequest<I>::create(
        m_instance_watcher.get(), instance_ids, m_cached_peer_uuid,
        m_peer.uuid, &m_ignore_instance_ids, on_finish);
      req->send();
    });

  Mutex::Locker locker(m_lock);
  if (m_update_in_progress) {
    dout(20) << " update in progress, delaying request" << dendl;
    m_blocked_requests.push_back(on_finish);
  } else {
    m_update_in_progress = true;
    m_threads->work_queue->queue(on_finish, 0);
  }
}

template<typename I>
void InstanceMapper<I>::remove_instances(std::vector<std::string> &instance_ids,
                                         Context *on_finish) {
  dout(20) << dendl;

  on_finish = new FunctionContext(
    [this, instance_ids, on_finish](int r) {
      InstanceMapShuffleRequest<I> *req = InstanceMapShuffleRequest<I>::create(
        m_local_io_ctx, m_instance_map.get(), m_peer.uuid,
        m_instance_watcher.get(), instance_ids,
        InstanceMap::INSTANCES_REMOVED, m_bootstrap,
        m_threads->work_queue, new C_FinishUpdate(this, on_finish));

      req->send();
    });

  Mutex::Locker locker(m_lock);
  if (m_update_in_progress) {
    dout(20) << " update in progress, delaying request" << dendl;
    m_blocked_requests.push_back(on_finish);
  } else {
    m_update_in_progress = true;
    m_threads->work_queue->queue(on_finish, 0);
  }
}

template<typename I>
void InstanceMapper<I>::handle_update(const std::string &mirror_uuid,
                                      ImageIds &&added_image_ids,
                                      ImageIds &&removed_image_ids,
                                      Context *on_finish) {
  dout(20) << dendl;

  on_finish = new FunctionContext(
    [this, mirror_uuid, added_image_ids, removed_image_ids, on_finish](int r) {
      m_bootstrap = false;
      InstanceMapUpdateRequest<I> *req = InstanceMapUpdateRequest<I>::create(
        m_local_io_ctx, m_instance_map.get(), mirror_uuid,
        m_instance_watcher.get(), added_image_ids, removed_image_ids,
        &m_ignore_instance_ids, m_threads->work_queue,
        new C_FinishUpdate(this, on_finish));

      req->send();
    });

  if (!mirror_uuid.empty() && m_peer.uuid != mirror_uuid) {
    on_finish = new FunctionContext([this, mirror_uuid, on_finish](int r) {
        m_cached_peer_uuid = m_peer.uuid;
        m_peer.uuid = mirror_uuid;
        on_finish->complete(r);
      });

    on_finish = new FunctionContext(
      [this, mirror_uuid, on_finish](int r) {

        std::vector<std::string> instance_ids;
        m_instance_map->get_instance_ids(&instance_ids);

        AddPeerRequest<I> *req = AddPeerRequest<I>::create(
          m_instance_watcher.get(), instance_ids, m_peer.uuid,
          mirror_uuid, &m_ignore_instance_ids, on_finish);
        req->send();
      });
  }

  Mutex::Locker locker(m_lock);
  if (m_update_in_progress) {
    dout(20) << " update in progress, delaying request" << dendl;
    m_blocked_requests.push_back(on_finish);
  } else {
    m_update_in_progress = true;
    m_threads->work_queue->queue(on_finish, 0);
  }
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::InstanceMapper<librbd::ImageCtx>;
