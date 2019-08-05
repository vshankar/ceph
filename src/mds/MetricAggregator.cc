// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MetricAggregator.h"
#include "mgr/MgrClient.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.metric.aggregator" << " " << __func__

MetricAggregator::MetricAggregator(CephContext *cct, MgrClient *mgrc)
  : Dispatcher(cct),
    mgrc(mgrc) {
}

int MetricAggregator::init() {
  dout(20) << dendl;
  return 0;
}

void MetricAggregator::shutdown() {
  dout(20) << dendl;
}

bool MetricAggregator::ms_can_fast_dispatch2(const cref_t<Message> &m) const {
  return m->get_type() == MSG_MDS_METRICS;
}

void MetricAggregator::ms_fast_dispatch2(const ref_t<Message> &m) {
  bool handled = ms_dispatch2(m);
  ceph_assert(handled);
}

bool MetricAggregator::ms_dispatch2(const ref_t<Message> &m) {
  if (m->get_type() == MSG_MDS_METRICS) {
    if (m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MDS) {
      handle_mds_metrics(ref_cast<MMDSMetrics>(m));
    }

    return true;
  }

  return false;
}

void MetricAggregator::refresh_metrics_for_rank(const entity_inst_t &client,
                                                mds_rank_t rank, const Metrics &metrics) {
  dout(20) << ": client=" << client << ", rank=" << rank << ", metrics="
           << metrics << dendl;

  auto p = clients_by_rank.find(rank);
  ceph_assert(p != clients_by_rank.end());

  bool ins = p->second.insert(client).second;
  if (ins) {
    dout(20) << ": rank=" << rank << " has " << p->second.size() << " connected"
             << " client(s)" << dendl;
  }

  auto update_counter_func = [&metrics](const MDSPerformanceCounterDescriptor &d,
                                        PerformanceCounter *c) {
    ceph_assert(d.is_supported());

    dout(20) << ": performance_counter_descriptor=" << d << dendl;

    switch (d.type) {
    case MDSPerformanceCounterType::CAP_HIT_METRIC:
      c->first = metrics.cap_hit_metric.hits;
      c->second = metrics.cap_hit_metric.misses;
      break;
    case MDSPerformanceCounterType::OSDC_CACHE_METRIC:
      c->first = metrics.osdc_cache_metric.hits;
      c->second = metrics.osdc_cache_metric.misses;
      break;
    case MDSPerformanceCounterType::IOPS_METRIC:
      c->first = (int)metrics.iops_metric.read_iops;
      c->second = (int)metrics.iops_metric.write_iops;
      break;
    default:
      ceph_abort_msg("unknown counter type");
    }
  };

  auto sub_key_func = [client, rank](const MDSPerfMetricSubKeyDescriptor &d,
                                     MDSPerfMetricSubKey *sub_key) {
    ceph_assert(d.is_supported());

    dout(20) << ": sub_key_descriptor=" << d << dendl;

    std::string match_string;
    switch (d.type) {
    case MDSPerfMetricSubKeyType::MDS_RANK:
      match_string = stringify(rank);
      break;
    case MDSPerfMetricSubKeyType::CLIENT_ID:
      match_string = stringify(client);
      break;
    default:
      ceph_abort_msg("unknown counter type");
    }

    dout(20) << ": match_string=" << match_string << dendl;

    std::smatch match;
    if (!std::regex_search(match_string, match, d.regex)) {
      return false;
    }
    if (match.size() <= 1) {
      return false;
    }
    for (size_t i = 1; i < match.size(); ++i) {
      sub_key->push_back(match[i].str());
    }
    return true;
  };

  for (auto &it : query_metrics_map) {
    auto &query = it.first;
    MDSPerfMetricKey key;
    if (query.get_key(sub_key_func, &key)) {
      query.update_counters(update_counter_func, &it.second[key]);
    }
  }
}

void MetricAggregator::remove_metrics_for_rank(const entity_inst_t &client,
                                               mds_rank_t rank, bool remove) {
  dout(20) << ": client=" << client << ", rank=" << rank << dendl;

  if (remove) {
    auto p = clients_by_rank.find(rank);
    ceph_assert(p != clients_by_rank.end());

    bool rm = p->second.erase(client) != 0;
    ceph_assert(rm);
    dout(20) << ": rank=" << rank << " has " << p->second.size() << " connected"
             << " client(s)" << dendl;
  }

  auto sub_key_func = [client, rank](const MDSPerfMetricSubKeyDescriptor &d,
                                     MDSPerfMetricSubKey *sub_key) {
    ceph_assert(d.is_supported());

    dout(20) << ": sub_key_descriptor=" << d << dendl;

    std::string match_string;
    switch (d.type) {
    case MDSPerfMetricSubKeyType::MDS_RANK:
      match_string = stringify(rank);
      break;
    case MDSPerfMetricSubKeyType::CLIENT_ID:
      match_string = stringify(client);
      break;
    default:
      ceph_abort_msg("unknown counter type");
    }

    dout(20) << ": match_string=" << match_string << dendl;

    std::smatch match;
    if (!std::regex_search(match_string, match, d.regex)) {
      return false;
    }
    if (match.size() <= 1) {
      return false;
    }
    for (size_t i = 1; i < match.size(); ++i) {
      sub_key->push_back(match[i].str());
    }
    return true;
  };

  for (auto &it : query_metrics_map) {
    auto &query = it.first;
    MDSPerfMetricKey key;
    if (query.get_key(sub_key_func, &key)) {
      if (it.second.erase(key)) {
        dout(10) << ": removed metric for key=" << key << dendl;
      }
    }
  }
}

void MetricAggregator::handle_mds_metrics(const cref_t<MMDSMetrics> &m) {
  const MetricsMessage &metrics_message = m->metrics_message;

  auto rank = metrics_message.rank;
  auto &client_metrics_map = metrics_message.client_metrics_map;

  dout(20) << ": applying " << client_metrics_map.size() << " updates for rank="
           << rank << dendl;

  std::scoped_lock locker(lock);

  for (auto &p : client_metrics_map) {
    auto &client = p.first;
    auto &metrics = p.second;

    switch (metrics.update_type) {
    case UpdateType::UPDATE_TYPE_REFRESH:
      refresh_metrics_for_rank(client, rank, metrics);
      break;
    case UpdateType::UPDATE_TYPE_REMOVE:
      remove_metrics_for_rank(client, rank, true);
      break;
    default:
      ceph_abort();
    }
  }
}
