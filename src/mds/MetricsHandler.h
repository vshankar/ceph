// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_METRICS_HANDLER_H
#define CEPH_MDS_METRICS_HANDLER_H

#include "common/Timer.h"
#include "messages/MMDSMetrics.h"
#include "metrics/client/Types.h"

class MDSRank;
class Session;

class MetricsHandler {
public:
  MetricsHandler(MDSRank *mds);

  void add_session(Session *session);
  void remove_session(Session *session);

  void handle_payload(Session *session, const CapInfoPayload &payload);
  void handle_payload(Session *session, const OSDCCacheInfoPayload &payload);
  void handle_payload(Session *session, const ReadWriteIOPSPayload &payload);
  void handle_payload(Session *session, const UnknownPayload &payload);

private:
  MDSRank *mds;
  Context *timer_task = nullptr;
  std::map<entity_inst_t, Metrics> client_metrics_map;

  void update_rank0();
  void schedule_update();
};

#endif // CEPH_MDS_METRICS_HANDLER_H
