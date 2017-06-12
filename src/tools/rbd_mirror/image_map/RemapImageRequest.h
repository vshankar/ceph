// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_REMAP_IMAGE_REQUEST
#define CEPH_RBD_MIRROR_IMAGE_MAP_REMAP_IMAGE_REQUEST

#include "InstanceMap.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"

class Context;

using librados::IoCtx;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class InstanceWatcher;

namespace image_map {

using rbd::mirror::InstanceWatcher;

template<typename ImageCtxT = librbd::ImageCtx>
class RemapImageRequest {
public:
  static RemapImageRequest *create(
    IoCtx &ioctx,
    const std::string &mirror_uuid,
    const std::string &from_instance_id,
    const std::string &to_instance_id,
    const std::string &global_id,
    const std::string &image_id,
    InstanceWatcher<ImageCtxT> *instance_watcher,
    bool bootstrap, Context *on_finish) {
    return new RemapImageRequest(
      ioctx, mirror_uuid, from_instance_id, to_instance_id,
      global_id, image_id, instance_watcher, bootstrap, on_finish);
  }

  void send();

private:
  RemapImageRequest(
    IoCtx &ioctx,
    const std::string &mirror_uuid,
    const std::string &from_instance_id,
    const std::string &to_instance_id,
    const std::string &global_id,
    const std::string &image_id,
    InstanceWatcher<ImageCtxT> *instance_watcher,
    bool bootstrap, Context *on_finish);

  IoCtx &m_ioctx;
  std::string m_mirror_uuid;
  std::string m_from_instance_id;
  std::string m_to_instance_id;
  std::string m_global_id;
  std::string m_image_id;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  bool m_bootstrap;
  Context *m_on_finish;

  bufferlist m_out_bl;
  cls::rbd::ImageMap m_image_map;

  void get_image_map();
  void handle_get_image_map(int r);

  void update_image_map_set_unmapping();
  void handle_update_image_map_set_unmapping(int r);

  void stop_image_replayer();
  void handle_stop_image_replayer(int r);

  void update_image_map_set_mapping();
  void handle_update_image_map_set_mapping(int r);

  void start_image_replayer();
  void handle_start_image_replayer(int r);

  void update_image_map_set_mapped();
  void handle_update_image_map_set_mapped(int r);

  void finish(int r);
};

} // namsspace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_REMAP_IMAGE_REQUEST
