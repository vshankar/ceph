// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_CLIENT_METRICS_H
#define CEPH_MDS_CLIENT_METRICS_H

#include <vector>

#include "msg/Message.h"
#include "mds/metrics/client/Types.h"

class MClientMetrics : public Message {
public:
  std::vector<ClientMetricMessage> updates;

protected:
  MClientMetrics() : MClientMetrics(std::vector<ClientMetricMessage>{}) { }
  MClientMetrics(const std::vector<ClientMetricMessage> &updates)
    : Message(CEPH_MSG_CLIENT_METRICS), updates(updates) {
  }
  ~MClientMetrics() { }

public:
  std::string_view get_type_name() const override {
    return "client_metrics";
  }

  void print(ostream &out) const override {
    out << "client_metrics";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(updates, payload);
  }

  void decode_payload() override {
    using ceph::decode;
    auto iter = payload.cbegin();
    decode(updates, iter);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // CEPH_MDS_CLIENT_METRICS_H
