// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"

#include "librbd/Utils.h"

#include "tools/rbd_mirror/InstanceWatcher.h"

#include "MapImageRequest.h"
#include "RemapImageRequest.h"
#include "InstanceMapShuffleRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::InstanceMapShuffleRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::create_context_callback;

template <typename I>
struct C_ShuffleImage : public Context {
  IoCtx &ioctx;
  InstanceMap *instance_map;
  InstanceWatcher<I> *instance_watcher;
  bool bootstrap;
  std::string mirror_uuid;
  std::string global_id;
  std::string from_instance_id;
  std::string to_instance_id;
  Context *on_finish;

  std::string id;

  C_ShuffleImage<I>(
    IoCtx &ioctx,
    InstanceMap *instance_map,
    InstanceWatcher<I> *instance_watcher,
    bool bootstrap,
    const std::string &mirror_uuid,
    const std::string &global_id,
    const std::string &from_instance_id,
    const std::string &to_instance_id,
    Context *on_finish)
    : ioctx(ioctx),
      instance_map(instance_map),
      instance_watcher(instance_watcher),
      bootstrap(bootstrap),
      mirror_uuid(mirror_uuid),
      global_id(global_id),
      from_instance_id(from_instance_id),
      to_instance_id(to_instance_id),
      on_finish(on_finish) {
  }

  void send() {
    dout(20) << dendl;

    if (bootstrap) {
      remap_image();
    } else {
      lookup_image();
    }
  }

  void lookup_image() {
    dout(20) << dendl;

    ImageSpecs::iterator it;
    instance_map->lookup(global_id, &it);

    const ImageSpec &entry = *it;
    // choose peer uuid and image id based on image state
    if (entry.remote_id.empty()) {
      id = entry.local_id;
      mirror_uuid = "";
    } else {
      // peer uuid should be known
      assert(mirror_uuid != "");
      id = entry.remote_id;
     }

    remap_image();
  }

  void remap_image() {
    dout(20) << dendl;

    dout(20) << ": global_id=" << global_id << " shuffling from instance="
             << from_instance_id << " to instance=" << to_instance_id
             << dendl;

    Context *ctx = create_context_callback<
      C_ShuffleImage,
      &C_ShuffleImage::handle_remap_image>(this);

    if (from_instance_id == to_instance_id) {
      // we'd still need to start the replayer
      MapImageRequest<I> *req = MapImageRequest<I>::create(
        ioctx, mirror_uuid, to_instance_id, global_id, id,
        instance_watcher, bootstrap, ctx);
      req->send();
    } else {
      RemapImageRequest<I> *req = RemapImageRequest<I>::create(
        ioctx, mirror_uuid, from_instance_id, to_instance_id,
        global_id, id, instance_watcher, bootstrap, ctx);
      req->send();
    }
  }

  void handle_remap_image(int r) {
    if (r < 0 && r != -ENOENT && r != -EINVAL) {
      // TODO: move to failed list
      derr << ": failed to shuffle: " << cpp_strerror(r) << dendl;
      complete(r);
      return;
    }

    complete(0);
  }

  void finish(int r) override {
    on_finish->complete(r);
  }
};

template<typename I>
InstanceMapShuffleRequest<I>::InstanceMapShuffleRequest(
  IoCtx &ioctx,
  InstanceMap *instance_map,
  std::string &mirror_uuid,
  InstanceWatcher<I> *instance_watcher,
  const std::vector<std::string> &instance_ids,
  InstanceMap::ShuffleType type, bool bootstrap,
  ContextWQ *op_work_queue, Context *on_finish)
  : m_ioctx(ioctx),
    m_instance_map(instance_map),
    m_mirror_uuid(mirror_uuid),
    m_instance_watcher(instance_watcher),
    m_instance_ids(instance_ids),
    m_shuffle_type(type),
    m_bootstrap(bootstrap),
    m_op_work_queue(op_work_queue),
    m_on_finish(on_finish) {
}

template<typename I>
void InstanceMapShuffleRequest<I>::send() {
  dout(20) << dendl;

  if (m_shuffle_type == InstanceMap::INSTANCES_ADDED) {
    refresh_instance_map();
  } else {
    shuffle_images();
  }
}

template<typename I>
void InstanceMapShuffleRequest<I>::refresh_instance_map() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    InstanceMapShuffleRequest,
    &InstanceMapShuffleRequest::handle_refresh_instance_map>(this);
  m_instance_map->load(m_instance_ids, ctx);
}

template<typename I>
void InstanceMapShuffleRequest<I>::handle_refresh_instance_map(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    finish(r);
    return;
  }

  shuffle_images();
}

template<typename I>
void InstanceMapShuffleRequest<I>::shuffle_images() {
  dout(20) << dendl;

  m_instance_map->shuffle(
    m_instance_ids, &m_remapped, m_shuffle_type);
  if (m_remapped.empty()) {
    finish(0);
    return;
  }

  Context *ctx = create_context_callback<
    InstanceMapShuffleRequest,
    &InstanceMapShuffleRequest::handle_shuffle_images>(this);

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  for (auto &iter : m_remapped) {
    C_ShuffleImage<I> *ctx = new C_ShuffleImage<I>(
      m_ioctx, m_instance_map, m_instance_watcher, m_bootstrap,
      m_mirror_uuid, iter.first, iter.second.first,
      iter.second.second, gather_ctx->new_sub());
    ctx->send();
  }

  gather_ctx->activate();
}

template<typename I>
void InstanceMapShuffleRequest<I>::handle_shuffle_images(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to refresh image map: " << cpp_strerror(r) << dendl;
  } else {
    dout(20) << ": map refreshed (map size: " << m_instance_map->size() << ")"
             << dendl;
  }

  finish(r);
}

template<typename I>
void InstanceMapShuffleRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::InstanceMapShuffleRequest<librbd::ImageCtx>;
