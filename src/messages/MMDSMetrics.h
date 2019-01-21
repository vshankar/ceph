// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_METRICS_H
#define CEPH_MDS_METRICS_H

#include "msg/Message.h"
#include "mds/MDSPerfMetricTypes.h"

class MMDSMetrics : public Message {
public:
  // metrics messsage (client -> metrics map, rank, etc..)
  MetricsMessage metrics_message;

protected:
  MMDSMetrics() : Message(MSG_MDS_METRICS) {
  }
  MMDSMetrics(const MetricsMessage &metrics_message)
    : Message(MSG_MDS_METRICS), metrics_message(metrics_message) {
  }
  ~MMDSMetrics() { }

public:
  std::string_view get_type_name() const override {
    return "mds_metrics";
  }

  void print(ostream &out) const override {
    out << "mds_metrics";
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(metrics_message, payload, features);
  }

  void decode_payload() override {
    using ceph::decode;
    auto iter = payload.cbegin();
    decode(metrics_message, iter);
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif // CEPH_MDS_METRICS_H
