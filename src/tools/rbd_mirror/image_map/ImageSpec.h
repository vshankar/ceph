// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_IMAGE_SPEC_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_IMAGE_SPEC_H

#include <map>
#include <string>

#include "cls/rbd/cls_rbd_types.h"

namespace rbd {
namespace mirror {
namespace image_map {

struct ImageSpec {
  std::string global_id;
  mutable std::string local_id;
  mutable std::string remote_id;
  mutable cls::rbd::ImageMapState state;

  ImageSpec(const std::string &global_id)
    : global_id(global_id) {
  }

  ImageSpec(const std::string &global_id,
            cls::rbd::ImageMapState state)
    : global_id(global_id), state(state) {
  }

  inline bool operator==(const ImageSpec &rhs) const {
    return (global_id == rhs.global_id);
  }

  inline bool operator<(const ImageSpec &rhs) const {
    return global_id < rhs.global_id;
  }

  void update(const std::string &id,
              const std::string &mirror_uuid,
              cls::rbd::ImageMapState new_state) const;
  void update_state(cls::rbd::ImageMapState new_state) const;
};

std::ostream &operator<<(
  std::ostream &os, const ImageSpec &image_spec);

typedef std::set<ImageSpec> ImageSpecs;

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_IMAGE_SPEC_H
