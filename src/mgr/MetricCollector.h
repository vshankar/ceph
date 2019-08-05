// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_METRIC_COLLECTOR_H
#define CEPH_MGR_METRIC_COLLECTOR_H

#include <map>
#include <set>
#include <vector>
#include <utility>

#include "common/ceph_mutex.h"
#include "msg/Message.h"
#include "mgr/Types.h"
#include "mgr/MetricTypes.h"

class MMgrReport;

template <typename Query, typename Limit, typename Key, typename Report>
class MetricCollector {
public:
  virtual ~MetricCollector() {
  }

  typedef std::set<Limit> Limits;

  MetricCollector(MetricListener &listener);

  MetricQueryID add_query(const Query &query, const std::optional<Limit> &limit);

  int remove_query(MetricQueryID query_id);

  void remove_all_queries();

  int get_counters(MetricQueryID query_id, std::map<Key, PerformanceCounters> *counters);

  std::map<Query, Limits> get_queries() const {
    std::lock_guard locker(lock);

    std::map<Query, Limits> result;
    for (auto &it : queries) {
      auto &query = it.first;
      auto &limits = it.second;
      auto result_it = result.insert({query, {}}).first;
      if (is_limited(limits)) {
        for (auto &iter : limits) {
          result_it->second.insert(*iter.second);
        }
      }
    }

    return result;
  }

  virtual void process_reports(const MetricPayload &payload) = 0;

protected:
  typedef std::optional<Limit> OptionalLimit;
  typedef std::map<Query, std::map<MetricQueryID, OptionalLimit>> Queries;
  typedef std::map<MetricQueryID, std::map<Key, PerformanceCounters>> Counters;
  typedef std::function<void(PerformanceCounter *, const PerformanceCounter &)> UpdateCallback;

  mutable ceph::mutex lock = ceph::make_mutex("mgr::metric::collector::lock");

  Queries queries;
  Counters counters;

  void process_reports_generic(const std::map<Query, Report> &reports, UpdateCallback callback);

private:
  MetricListener &listener;
  MetricQueryID next_query_id = 0;

  bool is_limited(const std::map<MetricQueryID, OptionalLimit> &limits) const {
    for (auto &it : limits) {
      if (!it.second) {
        return false;
      }
    }
    return true;
  }
};

#endif // CEPH_MGR_METRIC_COLLECTOR_H
