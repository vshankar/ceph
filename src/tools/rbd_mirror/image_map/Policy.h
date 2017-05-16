// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H

#include <map>
#include <string>
#include <tuple>

#include "common/Mutex.h"
#include "InstanceMap.h"

namespace rbd {
namespace mirror {
namespace image_map {

class Policy {
public:
  Policy();
  virtual ~Policy() {
  }

  std::string lookup(
    InstanceMap::InMap &inmap,
    const std::string &global_id, ImageSpecs::iterator *iter);
  std::string map(
    InstanceMap::InMap *inmap,
    const ImageSpec &image_spec, ImageSpecs::iterator *iter);
  bool unmap(InstanceMap::InMap *inmap,
             const std::string &global_id);
  bool remap(InstanceMap::InMap *inmap,
             const std::string &from_instance_id,
             const std::string &to_instance_id,
             const std::string &global_id);
  void shuffle(
    InstanceMap::InMap *inmap,
    std::vector<std::string> &instance_ids,
    Remap *remapped, InstanceMap::ShuffleType type);

private:
  // policies needs to implement these..
  virtual std::string do_map(
    InstanceMap::InMap &inmap, const std::string &global_id) = 0;
  virtual void do_shuffle(
    InstanceMap::InMap &inmap,
    std::vector<std::string> &instance_ids,
    Remap *remapped, InstanceMap::ShuffleType type) = 0;
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H
