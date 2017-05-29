// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"

#include "common/WorkQueue.h"
#include "librbd/Utils.h"

#include "tools/rbd_mirror/InstanceWatcher.h"

#include "MapImageRequest.h"
#include "InstanceMapGetRequest.h"
#include "InstanceMapRemoveRequest.h"
#include "InstanceMapUpdateRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::InstanceMapUpdateRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::create_context_callback;

template<typename I>
struct C_Update : public Context {
  IoCtx &ioctx;
  InstanceMap *instance_map;
  InstanceWatcher<I> *instance_watcher;
  ImageId image_id;
  std::string mirror_uuid;
  Context *on_finish;

  std::string instance_id;

  C_Update<I>(
    IoCtx &ioctx,
    InstanceMap *instance_map,
    InstanceWatcher<I> *instance_watcher,
    const ImageId &image_id,
    const std::string &mirror_uuid,
    Context *on_finish)
    : ioctx(ioctx),
      instance_map(instance_map),
      instance_watcher(instance_watcher),
      image_id(image_id),
      mirror_uuid(mirror_uuid),
      on_finish(on_finish) {
  }

  void send() {
    dout(20) << dendl;

    ImageSpecs::iterator it;
    instance_id = instance_map->lookup_or_map(image_id.global_id, &it);
    assert(instance_id != InstanceMap::UNMAPPED_INSTANCE_ID);

    const ImageSpec &entry = *it;
    if (mirror_uuid == "") {
      entry.local_id = image_id.id;
    } else {
      entry.remote_id = image_id.id;
    }

    dout(20) << ": global_id=" << image_id.global_id
             << " maping to instance=" << instance_id << dendl;

    map_image();
  }

  void map_image() {
    dout(20) << dendl;

    Context *ctx = create_context_callback<
      C_Update, &C_Update::handle_map_image>(this);

    MapImageRequest<I> *req = MapImageRequest<I>::create(
      ioctx, mirror_uuid, instance_id, image_id.global_id,
      image_id.id, instance_watcher, false, ctx);
    req->send();
  }

  void handle_map_image(int r) {
    dout(20) << ": r=" << r << dendl;

    if (r < 0 && r != -ENOENT && r != -EINVAL) {
      // TODO: move to failed list
      derr << ": failed to map image: " << cpp_strerror(r) << dendl;
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
InstanceMapUpdateRequest<I>::InstanceMapUpdateRequest(
  IoCtx &ioctx,
  InstanceMap *instance_map,
  const std::string &mirror_uuid,
  InstanceWatcher<I> *instance_watcher,
  const ImageIds &added_image_ids, const ImageIds &removed_image_ids,
  std::list<std::string> *ignore_instance_ids, ContextWQ *op_work_queue,
  Context *on_finish)
  : m_ioctx(ioctx),
    m_instance_map(instance_map),
    m_mirror_uuid(mirror_uuid),
    m_instance_watcher(instance_watcher),
    m_added_image_ids(added_image_ids),
    m_removed_image_ids(removed_image_ids),
    m_ignore_instance_ids(ignore_instance_ids),
    m_op_work_queue(op_work_queue),
    m_on_finish(on_finish) {
}

template<typename I>
void InstanceMapUpdateRequest<I>::send() {
  dout(20) << dendl;

  if (m_removed_image_ids.empty()) {
    add_images();
  } else {
    finish(0);
  }
}

template<typename I>
void InstanceMapUpdateRequest<I>::add_images() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    InstanceMapUpdateRequest,
    &InstanceMapUpdateRequest::handle_add_images>(this);

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);

  for (auto const &image_id : m_added_image_ids) {
    C_Update<I> *ctx = new C_Update<I>(
      m_ioctx, m_instance_map, m_instance_watcher, image_id,
      m_mirror_uuid, gather_ctx->new_sub());
    ctx->send();
  }

  gather_ctx->activate();
}

template<typename I>
void InstanceMapUpdateRequest<I>::handle_add_images(int r) {
  dout(20) << ": r=" << r << dendl;

  finish(r);
}

template<typename I>
void InstanceMapUpdateRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::InstanceMapUpdateRequest<librbd::ImageCtx>;
