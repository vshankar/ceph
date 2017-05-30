// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_REMOVE_REQUEST
#define CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_REMOVE_REQUEST

#include <list>

#include "include/rados/librados.hpp"
#include "tools/rbd_mirror/types.h"

class Context;

using librados::IoCtx;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_map {

template<typename ImageCtxT = librbd::ImageCtx>
class InstanceMapRemoveRequest {
public:
  static InstanceMapRemoveRequest *create(
    IoCtx &ioctx, ImageIds &image_ids, Context *on_finish) {
    return new InstanceMapRemoveRequest(ioctx, image_ids, on_finish);
  }

  void send();

private:
  InstanceMapRemoveRequest(
    IoCtx &ioctx, ImageIds &image_ids, Context *on_finish);

  IoCtx &m_ioctx;
  ImageIds m_image_ids;
  Context *m_on_finish;

  std::list<std::string> m_global_ids;
  ImageIds::iterator m_iter;

  void remove_image_ids();
  void handle_remove_image_ids(int r);

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_INSTNACE_MAP_REMOVE_REQUEST
