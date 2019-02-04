// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "MDSRank.h"
#include "SessionMap.h"
#include "MetricsHandler.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": mds.metrics"

MetricsHandler::MetricsHandler(MDSRank *mds) : mds(mds) {
  schedule_update();
}

void MetricsHandler::add_session(Session *session) {
  dout(20) << ": session=" << session << dendl;
  ceph_assert(session != nullptr);

  auto client = session->info.inst;
  auto it1 = client_metrics_map.find(client);
  ceph_assert(it1 == client_metrics_map.end() ||
              it1->second.update_type == UpdateType::UPDATE_TYPE_REMOVE);

  auto it2 = client_metrics_map.emplace(client, Metrics());
  it2.first->second.update_type = UPDATE_TYPE_REFRESH;
}

void MetricsHandler::remove_session(Session *session) {
  dout(20) << ": session=" << session << dendl;
  ceph_assert(session != nullptr);

  auto it = client_metrics_map.find(session->info.inst);
  ceph_assert(it != client_metrics_map.end());

  auto &metrics = it->second;

  metrics.cap_hit_metric = { };
  metrics.osdc_cache_metric = { };
  metrics.iops_metric = { };
  metrics.update_type = UPDATE_TYPE_REMOVE;
}

}

void MetricsHandler::handle_payload(Session *session, const CapInfoPayload &payload) {
  dout(20) << ": session=" << session << ", hits=" << payload.cap_hits << ", misses="
           << payload.cap_misses << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  ceph_assert(it != client_metrics_map.end());

  auto &metrics = it->second;
  metrics.update_type = UPDATE_TYPE_REFRESH;

  metrics.cap_hit_metric.hits = payload.cap_hits;
  metrics.cap_hit_metric.misses = payload.cap_misses;
}

void MetricsHandler::handle_payload(Session *session, const OSDCCacheInfoPayload &payload) {
  dout(20) << ": session=" << session << ", hits=" << payload.osdc_cache_hits << ", misses="
           << payload.osdc_cache_misses << ", size=" << payload.osdc_cache_size << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  ceph_assert(it != client_metrics_map.end());

  auto &metrics = it->second;
  metrics.update_type = UPDATE_TYPE_REFRESH;

  metrics.osdc_cache_metric.hits = payload.osdc_cache_hits;
  metrics.osdc_cache_metric.misses = payload.osdc_cache_misses;
  metrics.osdc_cache_metric.size = payload.osdc_cache_size;
}

void MetricsHandler::handle_payload(Session *session, const ReadWriteIOPSPayload &payload) {
  dout(20) << ": session=" << session << ", read_iops=" << payload.read_iops
           << ", write_iops=" << payload.write_iops << dendl;

  auto it = client_metrics_map.find(session->info.inst);
  ceph_assert(it != client_metrics_map.end());

  auto &metrics = it->second;
  metrics.update_type = UPDATE_TYPE_REFRESH;

  metrics.iops_metric.read_iops = payload.read_iops;
  metrics.iops_metric.write_iops = payload.write_iops;
}

void MetricsHandler::handle_payload(Session *session, const UnknownPayload &payload) {
  ceph_abort();
}

void MetricsHandler::update_rank0() {
  dout(20) << dendl;
  ceph_assert(timer_task == nullptr);
  ceph_assert(ceph_mutex_is_locked_by_me(mds->mds_lock));

  MetricsMessage metrics_message(mds->get_nodeid());
  auto &update_client_metrics_map = metrics_message.client_metrics_map;

  for (auto p = client_metrics_map.begin(); p != client_metrics_map.end();) {
    update_client_metrics_map.emplace(p->first, p->second);
    if (p->second.update_type == UPDATE_TYPE_REFRESH) {
      p->second = {};
      ++p;
    } else {
      p = client_metrics_map.erase(p);
    }
  }

  dout(20) << ": sending metric updates for " << update_client_metrics_map.size()
           << " clients to rank 0" << dendl;
  mds->send_message_mds(make_message<MMDSMetrics>(metrics_message), (mds_rank_t)0);

  schedule_update();
}

void MetricsHandler::schedule_update() {
  dout(20) << dendl;
  ceph_assert(timer_task == nullptr);

  timer_task = new FunctionContext([this](int _) {
      timer_task = nullptr;
      update_rank0();
    });

  double after = g_conf().get_val<double>("mds_metrics_update_interval");
  mds->timer.add_event_after(after, timer_task);
}
