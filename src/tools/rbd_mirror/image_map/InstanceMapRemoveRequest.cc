// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "librbd/Utils.h"
#include "InstanceMapRemoveRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::InstanceMapRemoveRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::create_rados_callback;

static const uint32_t MAX_REMOVE = 1024;

template<typename I>
InstanceMapRemoveRequest<I>::InstanceMapRemoveRequest(
  IoCtx &ioctx, ImageIds &image_ids, Context *on_finish)
  : m_ioctx(ioctx),
    m_image_ids(image_ids),
    m_on_finish(on_finish) {
}

template<typename I>
void InstanceMapRemoveRequest<I>::send() {
  dout(20) << dendl;

  m_iter = m_image_ids.begin();
  remove_image_ids();
}

template<typename I>
void InstanceMapRemoveRequest<I>::remove_image_ids() {
  dout(20) << dendl;

  m_global_ids.clear();

  uint32_t count = 0;
  while (m_iter != m_image_ids.end() && count < MAX_REMOVE) {
    m_global_ids.push_back((*m_iter).global_id);

    ++m_iter;
    ++count;
  }

  if (!count) {
    finish(0);
    return;
  }

  librados::ObjectWriteOperation op;
  librbd::cls_client::image_map_remove(&op, m_global_ids);
  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceMapRemoveRequest,
    &InstanceMapRemoveRequest::handle_remove_image_ids>(this);
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op);
  assert(r == 0);
}

template<typename I>
void InstanceMapRemoveRequest<I>::handle_remove_image_ids(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to remove image map: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_image_ids();
}

template<typename I>
void InstanceMapRemoveRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::InstanceMapRemoveRequest<librbd::ImageCtx>;
