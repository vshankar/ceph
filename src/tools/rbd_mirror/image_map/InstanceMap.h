// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_H

#include <map>
#include <tuple>
#include <vector>
#include <string>

#include "ImageSpec.h"
#include "common/RWLock.h"
#include "cls/rbd/cls_rbd_types.h"
#include "tools/rbd_mirror/types.h"

class Context;

using librados::IoCtx;

namespace rbd {
namespace mirror {
namespace image_map {

class Policy;

// TODO: move this to somwhere where everyone doesn't know
// about it
typedef std::map<std::string,
                 std::pair<std::string, std::string> > Remap;

class InstanceMap {
public:
  static InstanceMap *create(
    IoCtx &ioctx, const std::string &policy) {
    return new InstanceMap(ioctx, policy);
  }

  enum ShuffleType {
    INSTANCES_ADDED = 0,
    INSTANCES_REMOVED,
  };

  typedef std::map<std::string, ImageSpecs> InMap;
  static const std::string UNMAPPED_INSTANCE_ID;

  // size of the map : number of instances tracked
  size_t size() const {
    RWLock::RLocker locker(m_lock);
    return m_map.size();
  }

  // get the instances tracked by the map
  void get_instance_ids(std::vector<std::string> *instance_ids);

  // load the instance map
  void load(
    std::vector<std::string> &instance_ids, Context *on_finish);

  // lookup
  std::string lookup(
    const std::string &global_id, ImageSpecs::iterator *iter);

  // map an image
  std::string map(
    ImageSpec &image_spec, ImageSpecs::iterator *iter);

  // unmap an image
  bool unmap(const std::string &global_id);

  // grow/shrink the map
  void shuffle(
    std::vector<std::string> &instance_ids, Remap *remap,
    ShuffleType type);

  // lookup an image and map if its not present
  std::string lookup_or_map(
    const std::string &global_id, ImageSpecs::iterator *iter);

  // NOTE: there is not interface to *directly* remap an entry
  //       in the map

private:
  InstanceMap(IoCtx &ioctx, const std::string &policy);

  std::string _lookup(
    const std::string &global_id, ImageSpecs::iterator *iter);
  std::string _map(
    ImageSpec &image_spec, ImageSpecs::iterator *iter);

  IoCtx &m_ioctx;

  RWLock m_lock;
  std::unique_ptr<Policy> m_mapping_policy;

  InMap m_map;          // our instance_id -> images map
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_INSTANCE_MAP_H
