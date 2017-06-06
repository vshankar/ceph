// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"

#include "librbd/Utils.h"

#include "tools/rbd_mirror/InstanceWatcher.h"
#include "AddPeerRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::AddPeerRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::create_context_callback;

template<typename I>
AddPeerRequest<I>::AddPeerRequest(
  InstanceWatcher<I> *instance_watcher,
  const std::vector<std::string>& instance_ids,
  const std::string& old_mirror_uuid, const std::string& new_mirror_uuid,
  std::list<std::string> *missed_instance_ids, Context *on_finish)
  : m_instance_watcher(instance_watcher),
    m_instance_ids(instance_ids),
    m_old_mirror_uuid(old_mirror_uuid),
    m_new_mirror_uuid(new_mirror_uuid),
    m_missed_instance_ids(missed_instance_ids),
    m_on_finish(on_finish) {
}

template<typename I>
void AddPeerRequest<I>::send() {
  dout(20) << dendl;

  notify_add_peer();
}

template<typename I>
void AddPeerRequest<I>::notify_add_peer() {
  dout(20) << dendl;

  Context *ctx = create_context_callback<
    AddPeerRequest, &AddPeerRequest::handle_notify_add_peer>(this);

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);

  for (auto const instance : m_instance_ids) {
    Context *on_finish = gather_ctx->new_sub();
    on_finish = new FunctionContext([this, on_finish, instance](int r) {
        if (r < 0) {
          dout(20) << ": instance: " << instance << ", r=" << r << dendl;
          m_missed_instance_ids->push_back(instance);
        }

        on_finish->complete(0); // gather_ctx clobbers retval anyway
      });

    m_instance_watcher->notify_add_peer(instance, m_old_mirror_uuid,
                                        m_new_mirror_uuid, on_finish);
  }

  gather_ctx->activate();
}

template<typename I>
void AddPeerRequest<I>::handle_notify_add_peer(int r) {
  dout(20) << ": r=" << r << dendl;

  finish(r);
}

template<typename I>
void AddPeerRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::AddPeerRequest<librbd::ImageCtx>;
