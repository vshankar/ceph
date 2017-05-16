// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Policy.h"

namespace rbd {
namespace mirror {
namespace image_map {

Policy::Policy() {
}

std::string Policy::lookup(
  InstanceMap::InMap &inmap,
  const std::string &global_id, ImageSpecs::iterator *iter) {

  for (auto it = inmap.begin(); it != inmap.end(); ++it) {
    auto const &spec_it = it->second.find(global_id);
    if (spec_it != it->second.end()) {
      if (iter != nullptr) {
        *iter = spec_it;
      }

      return it->first;
    }
  }

  return InstanceMap::UNMAPPED_INSTANCE_ID;
}

std::string Policy::map(
  InstanceMap::InMap *inmap,
    const ImageSpec &image_spec, ImageSpecs::iterator *iter) {

  std::string instance_id = do_map(*inmap, image_spec.global_id);
  auto ins = (*inmap)[instance_id].insert(image_spec);
  assert(ins.second);

  if (iter != nullptr) {
    *iter = ins.first;
  }

  return instance_id;
}

bool Policy::unmap(
  InstanceMap::InMap *inmap,
  const std::string &global_id) {

  ImageSpecs::iterator it;
  std::string instance_id = lookup(*inmap, global_id, &it);
  if (instance_id == InstanceMap::UNMAPPED_INSTANCE_ID) {
    return false;
  }

  (*inmap)[instance_id].erase(it);
  return true;
}

bool Policy::remap(
  InstanceMap::InMap *inmap,
  const std::string &from_instance_id,
  const std::string &to_instance_id,
  const std::string &global_id) {

  if (from_instance_id == to_instance_id) {
    return true;
  }

  // lookup source to verify
  ImageSpecs::iterator spec_it;
  std::string source = lookup(*inmap, global_id, &spec_it);
  if (source != from_instance_id) {
    return false;
  }

  auto it = inmap->find(to_instance_id);
  if (it == inmap->end()) {
    return false;
  }

  auto ins = it->second.emplace(*spec_it);
  assert(ins.second);

  (*inmap)[from_instance_id].erase(spec_it);
  return true;
}

void Policy::shuffle(
  InstanceMap::InMap *inmap,
  std::vector<std::string> &instance_ids,
  Remap *remapped, InstanceMap::ShuffleType type) {

  do_shuffle(*inmap, instance_ids, remapped, type);
  for (auto const &iter : *remapped) {
    bool b = remap(inmap, iter.second.first,
                   iter.second.second, iter.first);
    assert(b);
  }

  if (type == InstanceMap::INSTANCES_REMOVED) {
    for (auto const &instance : instance_ids) {
      assert((*inmap)[instance].empty());
      (*inmap).erase(instance);
    }
  }
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
