// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"

#include "librbd/Utils.h"

#include "tools/rbd_mirror/InstanceWatcher.h"
#include "PeerUpdateRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::PeerUpdateRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::create_context_callback;

template<typename I>
PeerUpdateRequest<I>::PeerUpdateRequest(
  InstanceWatcher<I> *instance_watcher,
  const std::vector<std::string> &instance_ids,
  const std::string &old_mirror_uuid,
  const std::string &new_mirror_uuid, Context *on_finish)
  : m_instance_watcher(instance_watcher),
    m_instance_ids(instance_ids),
    m_old_mirror_uuid(old_mirror_uuid),
    m_new_mirror_uuid(new_mirror_uuid),
    m_on_finish(on_finish) {
}

template<typename I>
void PeerUpdateRequest<I>::send() {
  dout(20) << dendl;

  notify_peer_update();
}

template<typename I>
void PeerUpdateRequest<I>::notify_peer_update() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    PeerUpdateRequest,
    &PeerUpdateRequest::handle_notify_peer_update>(this);

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);

  for (auto const &instance : m_instance_ids) {
    // TODO: on failure, we would need to disallow mappings to
    // this instance (already mapped images would get remapped
    // when the instance schedule task times out).
    m_instance_watcher->notify_peer_update(
      instance, m_old_mirror_uuid, m_new_mirror_uuid,
      gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template<typename I>
void PeerUpdateRequest<I>::handle_notify_peer_update(int r) {
  dout(20) << ": r=" << r << dendl;

  finish(r);
}

template<typename I>
void PeerUpdateRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::PeerUpdateRequest<librbd::ImageCtx>;
