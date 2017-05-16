// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Utils.h"
#include "InstanceMap.h"
#include "SimplePolicy.h"
#include "InstanceMapGetRequest.h"

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::unique_lock_name;

const std::string InstanceMap::UNMAPPED_INSTANCE_ID("");

InstanceMap::InstanceMap(IoCtx &ioctx, const std::string &policy)
  : m_ioctx(ioctx),
    m_lock(unique_lock_name
           ("rbd::mirror::image_map::InstanceMap::m_lock", this)) {
  if (policy == "simple") {
    m_mapping_policy.reset(SimplePolicy::create());
  } else {
    assert("unknown policy" == 0);
  }
}

void InstanceMap::load(
  std::vector<std::string> &instance_ids, Context *on_finish) {

  RWLock::WLocker locker(m_lock);

  InstanceMapGetRequest<> *req = InstanceMapGetRequest<>::create(
    m_ioctx, &m_map, instance_ids, on_finish);
  req->send();
}

void InstanceMap::get_instance_ids(
  std::vector<std::string> *instance_ids) {

  RWLock::WLocker locker(m_lock);

  for (auto const &it : m_map) {
    instance_ids->push_back(it.first);
  }
}

std::string InstanceMap::_lookup(
  const std::string &global_id, ImageSpecs::iterator *iter) {

  assert(m_lock.is_locked());

  return m_mapping_policy->lookup(m_map, global_id, iter);
}

std::string InstanceMap::lookup(
  const std::string &global_id, ImageSpecs::iterator *iter) {

  RWLock::RLocker locker(m_lock);

  return _lookup(global_id, iter);
}

std::string InstanceMap::_map(
  ImageSpec &image_spec, ImageSpecs::iterator *iter) {

  assert(m_lock.is_wlocked());

  return m_mapping_policy->map(&m_map, image_spec, iter);
}

std::string InstanceMap::map(
  ImageSpec &image_spec, ImageSpecs::iterator *iter) {

  RWLock::WLocker locker(m_lock);

  return _map(image_spec, iter);
}

bool InstanceMap::unmap(const std::string &global_id) {

  RWLock::WLocker locker(m_lock);

  return m_mapping_policy->unmap(&m_map, global_id);
}

void InstanceMap::shuffle(
  std::vector<std::string> &instance_ids,
  Remap *remap, ShuffleType type) {

  RWLock::WLocker locker(m_lock);

  m_mapping_policy->shuffle(
    &m_map, instance_ids, remap, type);
}

std::string InstanceMap::lookup_or_map(
  const std::string &global_id, ImageSpecs::iterator *iter) {

  RWLock::WLocker locker(m_lock);

  std::string instance_id = _lookup(global_id, iter);
  if (instance_id != UNMAPPED_INSTANCE_ID) {
    return instance_id;
  }

  ImageSpec image_spec{
    global_id, cls::rbd::IMAGE_MAP_STATE_UNASSIGNED};
  return _map(image_spec, iter);
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
