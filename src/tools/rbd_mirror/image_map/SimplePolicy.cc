// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SimplePolicy.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::SimplePolicy: " << this \
                           << " " << __func__
namespace rbd {
namespace mirror {
namespace image_map {

namespace {

uint64_t calc_images_per_instance(
  InstanceMap::InMap &inmap, int nr_instances) {

  assert(nr_instances > 0);

  uint64_t nr_images = 0;
  for (auto const &instance: inmap) {
    nr_images += instance.second.size();
  }

  uint64_t images_per_instance = nr_images / nr_instances;
  if (nr_images % nr_instances) {
    ++images_per_instance;
  }

  return images_per_instance;
}


bool instance_present(
  std::vector<std::string> &instance_ids, const std::string &instance) {

  return !(std::find(instance_ids.begin(),
                     instance_ids.end(), instance) == instance_ids.end());
}

void shuffle_images(
  ImageSpecs::iterator from_it,
  const std::string &from_instance,
  const std::string &to_instance, uint64_t nr_moves, Remap *remapped) {

  while (nr_moves > 0) {
    remapped->insert(
      std::make_pair((*from_it).global_id,
                     std::make_pair(from_instance, to_instance)));
    ++from_it;
    --nr_moves;
  }
}

} // anonymous namespace

SimplePolicy::SimplePolicy()
  : Policy() {
}

void SimplePolicy::shuffle_shrink_map(
  InstanceMap::InMap &inmap,
  std::vector<std::string> &instance_ids, Remap *remapped) {

  if (instance_ids.empty()) {
    return;
  }

  uint64_t images_per_instance = calc_images_per_instance(
    inmap, inmap.size() - instance_ids.size());
  dout(5) << ": images per instance: " << images_per_instance
          << dendl;

  auto to_it = inmap.begin();
  for (auto const &from_instance : instance_ids) {
    uint64_t from_size = inmap[from_instance].size();
    auto from_it = inmap[from_instance].begin();

    for (; to_it != inmap.end(); ) {
      if (instance_present(instance_ids, to_it->first)) {
        ++to_it;
        continue;
      }

      uint64_t to_size = to_it->second.size();
      uint64_t nr_moves = min(images_per_instance - to_size, from_size);

      dout(5) << ": shuffling " << nr_moves << " (out of " << from_size
              << ") images from " << from_instance << " to "
              << to_it->first << dendl;
      shuffle_images(
        from_it, from_instance, to_it->first, nr_moves, remapped);
      from_size -= nr_moves;
      if (from_size > 0) {
        std::advance(from_it, nr_moves);
        ++to_it;
      } else {
        break;
      }
    }
  }
}

void SimplePolicy::shuffle_grow_map(
  InstanceMap::InMap &inmap,
  std::vector<std::string> &instance_ids, Remap *remapped) {

  if (inmap.empty() && instance_ids.empty()) {
    return;
  }

  uint64_t images_per_instance =
    calc_images_per_instance(inmap, inmap.size());
  dout(5) << ": images per instance: " << images_per_instance
          << dendl;

  // record which instances are unbalanced
  std::set<std::string> remap_instances;
  for (auto const &instance : inmap) {
    if (instance.second.size() > images_per_instance) {
      remap_instances.insert(instance.first);
    }
  }

  auto to_it = inmap.begin();
  for (auto const &from_instance : remap_instances) {
    uint64_t from_size =
      inmap[from_instance].size() - images_per_instance;
    auto from_it = inmap[from_instance].begin();

    for (; to_it != inmap.end(); ) {
      uint64_t to_size = to_it->second.size();
      if (from_instance == to_it->first ||
          to_size >= images_per_instance) {
        ++to_it;
        continue;
      }

      uint64_t nr_moves = min(from_size, images_per_instance - to_size);
      dout(5) << ": shuffling " << nr_moves << " (out of " << from_size
              << ") images from " << from_instance << " to "
              << to_it->first << dendl;

      shuffle_images(
        from_it, from_instance, to_it->first, nr_moves, remapped);
      from_size -= nr_moves;
      if (from_size > 0) {
        ++to_it;
        std::advance(from_it, nr_moves);
      } else {
        break;
      }
    }
  }

  for (auto const &instance : instance_ids) {
    auto it = inmap.find(instance);
    for (auto const &spec : it->second) {
      remapped->insert(
        std::make_pair(spec.global_id,
                       std::make_pair(instance, instance)));
    }
  }
}

void SimplePolicy::do_shuffle(
  InstanceMap::InMap &inmap,
  std::vector<std::string> &instance_ids,
  Remap *remapped, InstanceMap::ShuffleType type) {

  if (type == InstanceMap::INSTANCES_ADDED) {
    shuffle_grow_map(inmap, instance_ids, remapped);
  } else {
    shuffle_shrink_map(inmap, instance_ids, remapped);
  }
}

std::string SimplePolicy::do_map(
  InstanceMap::InMap &inmap, const std::string &global_id) {

  auto min_it = inmap.begin();
  for (auto it = min_it; it != inmap.end(); ++it) {
    assert(it->second.find(global_id) == it->second.end());
    if (it->second.size() < min_it->second.size()) {
      min_it = it;
    }
  }

  dout(20) << ": mapping global_id=" << global_id
           << " to instance=" << min_it->first << dendl;
  return min_it->first;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
