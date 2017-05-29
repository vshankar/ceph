// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_UPDATE_REQUEST
#define CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_UPDATE_REQUEST

#include <vector>

#include "Policy.h"
#include "include/rados/librados.hpp"

class Context;
class ContextWQ;

using librados::IoCtx;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class InstanceWatcher;

namespace image_map {

using rbd::mirror::InstanceWatcher;

template<typename ImageCtxT = librbd::ImageCtx>
class InstanceMapUpdateRequest {
public:
  static InstanceMapUpdateRequest *create(
    IoCtx &ioctx,
    InstanceMap *instance_map,
    const std::string &mirror_uuid,
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const ImageIds &added_image_ids, const ImageIds &removed_image_ids,
    std::list<std::string> *ignore_instance_ids, ContextWQ *op_work_queue,
    Context *on_finish) {
    return new InstanceMapUpdateRequest(
      ioctx, instance_map, mirror_uuid, instance_watcher, added_image_ids,
      removed_image_ids, ignore_instance_ids, op_work_queue, on_finish);
  }

  void send();

private:
  InstanceMapUpdateRequest(
    IoCtx &ioctx,
    InstanceMap *instance_map,
    const std::string &mirror_uuid,
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const ImageIds &added_image_ids, const ImageIds &removed_image_ids,
    std::list<std::string> *ignore_instance_ids, ContextWQ *op_work_queue,
    Context *on_finish);

  IoCtx &m_ioctx;
  InstanceMap *m_instance_map;
  std::string m_mirror_uuid;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  ImageIds m_added_image_ids;
  ImageIds m_removed_image_ids;
  std::list<std::string> *m_ignore_instance_ids;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  void add_images();
  void handle_add_images(int r);

  void refresh_instance_map_done();
  void handle_refresh_instance_map_done(int r);

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_UPDATE_REQUEST
