// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "librbd/Utils.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"

#include "InstanceMapGetRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::InstanceMapGetRequest: "   \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

static const uint32_t MAX_RETURN = 1024;

using librbd::util::create_rados_callback;

template<typename I>
InstanceMapGetRequest<I>::InstanceMapGetRequest(
  IoCtx &ioctx, InstanceMap::InMap *inmap,
  std::vector<std::string> &instance_ids, Context *on_finish)
  : m_ioctx(ioctx),
    m_inmap(inmap),
    m_instance_ids(instance_ids),
    m_on_finish(on_finish) {
}

template<typename I>
void InstanceMapGetRequest<I>::send() {
  dout(20) << dendl;

  for (auto const &instance : m_instance_ids) {
    assert(m_inmap->find(instance) == m_inmap->end());
    m_new_map.insert(std::make_pair(instance, ImageSpecs{}));
  }

  image_map_list();
}

template<typename I>
void InstanceMapGetRequest<I>::image_map_list() {
  dout(20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::image_map_list_start(&op, m_start_after, MAX_RETURN);

  librados::AioCompletion *aio_comp = create_rados_callback<
    InstanceMapGetRequest,
    &InstanceMapGetRequest::handle_image_map_list>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(RBD_MIRROR_LEADER, aio_comp, &op, &m_out_bl);
  assert(r == 0);
  aio_comp->release();
}

template<typename I>
void InstanceMapGetRequest<I>::handle_image_map_list(int r) {
  dout(20) << ": r=" << r << dendl;

  std::map<std::string, cls::rbd::ImageMap> image_map;
  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = librbd::cls_client::image_map_list_finish(&it, &image_map);
  }

  if (r < 0) {
    derr << ": failed to get image map: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  for (auto const &it: image_map) {
    auto inst_it = m_new_map.find(it.second.instance_id);
    if (inst_it != m_new_map.end()) {
      inst_it->second.insert(ImageSpec{it.first, it.second.state});
    }
  }

  if (image_map.size() == MAX_RETURN) {
    m_start_after = image_map.rbegin()->first;
    image_map_list();
    return;
  }

  apply();
}

template<typename I>
void InstanceMapGetRequest<I>::apply() {
  dout(20) << dendl;

  m_inmap->insert(m_new_map.begin(), m_new_map.end());
  finish(0);
}

template<typename I>
void InstanceMapGetRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_map::InstanceMapGetRequest<librbd::ImageCtx>;
