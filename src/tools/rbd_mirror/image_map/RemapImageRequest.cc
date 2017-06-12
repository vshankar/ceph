// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "Policy.h"
#include "librbd/Utils.h"
#include "RemapImageRequest.h"
#include "tools/rbd_mirror/InstanceWatcher.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::RemapImageRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::create_rados_callback;
using librbd::util::create_context_callback;

template<typename I>
RemapImageRequest<I>::RemapImageRequest(
  IoCtx &ioctx,
  const std::string &mirror_uuid,
  const std::string &from_instance_id,
  const std::string &to_instance_id,
  const std::string &global_id,
  const std::string &image_id,
  InstanceWatcher<I> *instance_watcher,
  bool bootstrap, Context *on_finish)
  : m_ioctx(ioctx),
    m_mirror_uuid(mirror_uuid),
    m_from_instance_id(from_instance_id),
    m_to_instance_id(to_instance_id),
    m_global_id(global_id),
    m_image_id(image_id),
    m_instance_watcher(instance_watcher),
    m_bootstrap(bootstrap),
    m_on_finish(on_finish) {
}

template<typename I>
void RemapImageRequest<I>::send() {
  dout(20) << dendl;

  dout(20) << ": global_id=" << m_global_id << " from instance="
           << m_from_instance_id << " to instance="
           << m_to_instance_id << dendl;

  get_image_map();
}

template<typename I>
void RemapImageRequest<I>::get_image_map() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::image_map_get_start(&op, m_global_id);

  librados::AioCompletion *aio_comp = create_rados_callback<
    RemapImageRequest,
    &RemapImageRequest::handle_get_image_map>(this);
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op, &m_out_bl);
  assert(r == 0);
}

template<typename I>
void RemapImageRequest<I>::handle_get_image_map(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::image_map_get_finish(&it, &m_image_map);
  }

  if (r < 0 && r != -ENOENT) {
    derr << ": failed to get image map: " << cpp_strerror(r)
         << dendl;
    finish(-EIO);
    return;
  }

  if (r == 0) {
    cls::rbd::ImageMapState state = m_image_map.state;

    if (state == cls::rbd::IMAGE_MAP_STATE_UNMAPPING ||
        state == cls::rbd::IMAGE_MAP_STATE_MAPPING ||
        m_image_map.instance_id == m_from_instance_id) {
      update_image_map_set_unmapping();
    } else {
      finish(-EINVAL);
    }

    return;
  }

  // -ENOENT
  m_image_map.instance_id = m_from_instance_id;
  update_image_map_set_unmapping();
}

template<typename I>
void RemapImageRequest<I>::update_image_map_set_unmapping() {
  dout(20) << dendl;

  m_image_map.state = cls::rbd::IMAGE_MAP_STATE_UNMAPPING;

  dout(20) << ": image map=" << m_image_map << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::image_map_update(&op, m_global_id, m_image_map);

  librados::AioCompletion *aio_comp = create_rados_callback<
    RemapImageRequest,
    &RemapImageRequest::handle_update_image_map_set_unmapping>(this);
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
}

template<typename I>
void RemapImageRequest<I>::handle_update_image_map_set_unmapping(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update on-disk image map to UNMAPPING: "
         << cpp_strerror(r) << dendl;
    finish(-EIO);
    return;
  }

  stop_image_replayer();
}

template<typename I>
void RemapImageRequest<I>::stop_image_replayer() {
  dout(20) << dendl;

  if (m_bootstrap) {
    update_image_map_set_mapping();
    return;
  }

  Context *ctx = create_context_callback<
    RemapImageRequest,
    &RemapImageRequest::handle_stop_image_replayer>(this);

  m_instance_watcher->notify_image_release(
    m_image_map.instance_id, m_global_id, m_mirror_uuid, m_image_id,
    false, ctx);
}

template<typename I>
void RemapImageRequest<I>::handle_stop_image_replayer(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0 && r != -ENOENT && r != -EINVAL) {
    derr << ": failed to stop image replayer: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  update_image_map_set_mapping();
}

template<typename I>
void RemapImageRequest<I>::update_image_map_set_mapping() {
  dout(20) << dendl;

  m_image_map.instance_id = m_to_instance_id;
  m_image_map.state = cls::rbd::IMAGE_MAP_STATE_MAPPING;

  dout(20) << ": image map=" << m_image_map << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::image_map_update(&op, m_global_id, m_image_map);

  librados::AioCompletion *aio_comp = create_rados_callback<
    RemapImageRequest,
    &RemapImageRequest::handle_update_image_map_set_mapping>(this);
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
}

template<typename I>
void RemapImageRequest<I>::handle_update_image_map_set_mapping(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update on-disk image map to MAPPING: "
         << cpp_strerror(r) << dendl;
    finish(-EIO);
    return;
  }

  start_image_replayer();
}

template<typename I>
void RemapImageRequest<I>::start_image_replayer() {
  dout(20) << dendl;

  if (m_bootstrap) {
    update_image_map_set_mapped();
    return;
  }

  Context *ctx = create_context_callback<
    RemapImageRequest,
    &RemapImageRequest::handle_start_image_replayer>(this);

  m_instance_watcher->notify_image_acquire(
    m_to_instance_id, m_global_id, m_mirror_uuid, m_image_id, ctx);
}

template<typename I>
void RemapImageRequest<I>::handle_start_image_replayer(int r) {
  dout(2) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to start image replayer: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  update_image_map_set_mapped();
}

template<typename I>
void RemapImageRequest<I>::update_image_map_set_mapped() {
  dout(20) << dendl;

  m_image_map.state = cls::rbd::IMAGE_MAP_STATE_MAPPED;

  dout(20) << ": image map=" << m_image_map << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::image_map_update(&op, m_global_id, m_image_map);

  librados::AioCompletion *aio_comp = create_rados_callback<
    RemapImageRequest,
    &RemapImageRequest::handle_update_image_map_set_mapped>(this);
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
}

template<typename I>
void RemapImageRequest<I>::handle_update_image_map_set_mapped(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to update on-disk image map to MAPPED: "
         << cpp_strerror(r) << dendl;
    r = -EIO;
  }

  finish(r);
}

template<typename I>
void RemapImageRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::RemapImageRequest<librbd::ImageCtx>;
