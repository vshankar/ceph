// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_GET_REQUEST
#define CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_GET_REQUEST

#include "InstanceMap.h"
#include "include/rados/librados.hpp"

class Context;
using librados::IoCtx;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_map {

template<typename ImageCtxT = librbd::ImageCtx>
class InstanceMapGetRequest {
public:
  static InstanceMapGetRequest *create(
    IoCtx &ioctx, InstanceMap::InMap *inmap,
    std::vector<std::string> &instance_ids, Context *on_finish) {
    return new InstanceMapGetRequest(
      ioctx, inmap, instance_ids, on_finish);
  }

  void send();

private:
  InstanceMapGetRequest(
    IoCtx &ioctx, InstanceMap::InMap *inmap,
    std::vector<std::string> &instance_ids, Context *on_finish);

  IoCtx& m_ioctx;
  InstanceMap::InMap *m_inmap;
  std::vector<std::string> m_instance_ids;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_start_after;
  InstanceMap::InMap m_new_map;

  void image_map_list();
  void handle_image_map_list(int r);

  void apply();

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_GET_REQUEST
