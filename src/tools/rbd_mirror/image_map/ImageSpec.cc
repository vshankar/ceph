// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageSpec.h"

namespace rbd {
namespace mirror {
namespace image_map {

void ImageSpec::update_state(
  cls::rbd::ImageMapState new_state) const {
  if (state != new_state) {
    state = new_state;
  }
}

void ImageSpec::update(
  const std::string &id,
  const std::string &mirror_uuid,
  cls::rbd::ImageMapState new_state) const {

  if (mirror_uuid == "") {
    local_id = id;
  } else {
    remote_id = id;
  }

  update_state(new_state);
}

std::ostream &operator<<(
  std::ostream &os, const ImageSpec &image_spec) {
  return os << "[global_id=" << image_spec.global_id
            << ", remote_id=" << image_spec.remote_id
            << ", local_id=" << image_spec.local_id
            << ", state=" << image_spec.state << "]";
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
