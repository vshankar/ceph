// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_SHUFFLE_REQUEST
#define CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_SHUFFLE_REQUEST

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
class InstanceMapShuffleRequest {
public:
  static InstanceMapShuffleRequest *create(
    IoCtx &ioctx,
    InstanceMap *instance_map, std::string &mirror_uuid,
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const std::vector<std::string> &instance_ids,
    InstanceMap::ShuffleType type, bool bootstrap,
    ContextWQ *op_work_queue, Context *on_finish) {
    return new InstanceMapShuffleRequest(
      ioctx, instance_map, mirror_uuid, instance_watcher,
      instance_ids, type, bootstrap, op_work_queue, on_finish);
  }

  void send();

private:
  InstanceMapShuffleRequest(
    IoCtx &ioctx,
    InstanceMap *instance_map, std::string &mirror_uuid,
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const std::vector<std::string> &instance_ids,
    InstanceMap::ShuffleType type, bool bootstrap,
    ContextWQ *op_work_queue, Context *on_finish);

  IoCtx &m_ioctx;
  InstanceMap *m_instance_map;
  std::string m_mirror_uuid;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  std::vector<std::string> m_instance_ids;
  InstanceMap::ShuffleType m_shuffle_type;
  bool m_bootstrap;
  ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  Remap m_remapped;

  std::vector<std::string> m_all_instance_ids;
  std::vector<std::string> m_inactive_instance_ids;

  void refresh_instance_map();
  void handle_refresh_instance_map(int r);

  void shuffle_images();
  void handle_shuffle_images(int r);

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_SHUFFLE_REQUEST
