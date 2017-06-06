// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_PEER_UPDATE_REQUEST
#define CEPH_RBD_MIRROR_IMAGE_MAP_PEER_UPDATE_REQUEST

#include "include/rados/librados.hpp"

class Context;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class InstanceWatcher;

namespace image_map {

using rbd::mirror::InstanceWatcher;

template<typename ImageCtxT = librbd::ImageCtx>
class PeerUpdateRequest {
public:
  static PeerUpdateRequest *create(
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const std::vector<std::string> &instance_ids,
    const std::string &old_mirror_uuid,
    const std::string &new_mirror_uuid, Context *on_finish) {
    return new PeerUpdateRequest(
      instance_watcher, instance_ids, old_mirror_uuid,
      new_mirror_uuid, on_finish);
  }

  void send();

private:
  PeerUpdateRequest(
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const std::vector<std::string> &instance_ids,
    const std::string &old_mirror_uuid,
    const std::string &new_mirror_uuid, Context *on_finish);

  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  std::vector<std::string> m_instance_ids;
  std::string m_old_mirror_uuid;
  std::string m_new_mirror_uuid;
  Context *m_on_finish;

  void notify_peer_update();
  void handle_notify_peer_update(int r);

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_PEER_UPDATE_REQUEST
