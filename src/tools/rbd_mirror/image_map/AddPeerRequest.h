// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_ADD_PEER_REQUEST
#define CEPH_RBD_MIRROR_IMAGE_MAP_ADD_PEER_REQUEST

#include <vector>

#include "Policy.h"
#include "include/rados/librados.hpp"

class Context;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class InstanceWatcher;

namespace image_map {

using rbd::mirror::InstanceWatcher;

template<typename ImageCtxT = librbd::ImageCtx>
class AddPeerRequest {
public:
  static AddPeerRequest *create(
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const std::vector<std::string>& instance_ids,
    const std::string& old_mirror_uuid, const std::string& new_mirror_uuid,
    std::list<std::string> *missed_instance_ids, Context *on_finish) {
    return new AddPeerRequest(
      instance_watcher, instance_ids, old_mirror_uuid, new_mirror_uuid,
      missed_instance_ids, on_finish);
  }

  void send();

private:
  AddPeerRequest(
    InstanceWatcher<ImageCtxT> *instance_watcher,
    const std::vector<std::string>& instance_ids,
    const std::string& old_mirror_uuid, const std::string& new_mirror_uuid,
    std::list<std::string> *missed_instance_ids, Context *on_finish);

  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  std::vector<std::string> m_instance_ids;
  std::string m_old_mirror_uuid;
  std::string m_new_mirror_uuid;
  std::list<std::string> *m_missed_instance_ids;
  Context *m_on_finish;

  void notify_add_peer();
  void handle_notify_add_peer(int r);

  void finish(int r);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_ADD_PEER_REQUEST
