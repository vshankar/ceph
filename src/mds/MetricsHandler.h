// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_METRICS_HANDLER_H
#define CEPH_MDS_METRICS_HANDLER_H

#include <thread>
#include <utility>
#include <boost/variant.hpp>

#include "msg/Dispatcher.h"
#include "include/spinlock.h"
#include "include/cephfs/metrics/Types.h"

#include "messages/MMDSPing.h"
#include "messages/MClientMetrics.h"

#include "MDSPerfMetricTypes.h"

class MDSRank;
class Session;
class CephContext;

class MetricsHandler : public Dispatcher {
public:
  MetricsHandler(CephContext *cct, MDSRank *mds);

  bool ms_can_fast_dispatch_any() const override {
    return true;
  }
  bool ms_can_fast_dispatch2(const cref_t<Message> &m) const override;
  void ms_fast_dispatch2(const ref_t<Message> &m) override;
  bool ms_dispatch2(const ref_t<Message> &m) override;

  void ms_handle_connect(Connection *c) override {
  }
  bool ms_handle_reset(Connection *c) override {
    return false;
  }
  void ms_handle_remote_reset(Connection *c) override {
  }
  bool ms_handle_refused(Connection *c) override {
    return false;
  }

  void add_session(Session *session);
  void remove_session(Session *session);

  void init();
  void shutdown();

private:
  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    MetricsHandler *metrics_handler;
    Session *session;

    HandlePayloadVisitor(MetricsHandler *metrics_handler, Session *session)
      : metrics_handler(metrics_handler), session(session) {
    }

    template <typename ClientMetricPayload>
    inline void operator()(const ClientMetricPayload &payload) const {
      metrics_handler->handle_payload(session, payload);
    }
  };

  MDSRank *mds;
  // drop this lock when calling ->send_message_mds() else mds might
  // deadlock
  ceph::spinlock lock;

  // ISN sent by rank0 pinger is 1
  version_t next_seq = 0;

  // sequence number incremented on each update sent to rank 0.
  // this is nowhere related to next_seq and is completely used
  // locally to figure out if a session got added and removed
  // within an update to rank 0. note that, this is initialized
  // with the incoming sequence number sent by rank 0.
  version_t last_updated_seq = 0;

  std::thread updater;
  std::map<entity_inst_t, std::pair<version_t, Metrics>> client_metrics_map;

  bool stopping = false;

  void handle_payload(Session *session, const CapInfoPayload &payload);
  void handle_payload(Session *session, const ReadLatencyPayload &payload);
  void handle_payload(Session *session, const WriteLatencyPayload &payload);
  void handle_payload(Session *session, const MetadataLatencyPayload &payload);
  void handle_payload(Session *session, const UnknownPayload &payload);

  void set_next_seq(version_t seq);

  void handle_client_metrics(const cref_t<MClientMetrics> &m);
  void handle_mds_ping(const cref_t<MMDSPing> &m);

  void update_rank0(std::unique_lock<ceph::spinlock> &locker);
};

#endif // CEPH_MDS_METRICS_HANDLER_H
