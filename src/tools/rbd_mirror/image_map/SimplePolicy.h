// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_SIMPLE_POLICY_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_SIMPLE_POLICY_H

#include "Policy.h"

namespace rbd {
namespace mirror {
namespace image_map {

class SimplePolicy : public Policy {
public:
  static SimplePolicy *create() {
    return new SimplePolicy();
  }

private:
  SimplePolicy();

  virtual std::string do_map(
    InstanceMap::InMap &inmap, const std::string &global_id);
  virtual void do_shuffle(
    InstanceMap::InMap &inmap,
    std::vector<std::string> &instance_ids,
    Remap *remapped, InstanceMap::ShuffleType type);

  void shuffle_grow_map(
    InstanceMap::InMap &inmap,
    std::vector<std::string> &instance_ids, Remap *remapped);
  void shuffle_shrink_map(
    InstanceMap::InMap &inmap,
    std::vector<std::string> &instance_ids, Remap *remapped);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_SIMPLE_POLICY_H
