// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_METRICS_CLIENT_TYPES_H
#define CEPH_MDS_METRICS_CLIENT_TYPES_H

#include <string>
#include <boost/variant.hpp>

#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "include/int_types.h"

namespace ceph { class Formatter; }

enum ClientMetricType {
  CLIENT_METRIC_TYPE_CAP_INFO,
  CLIENT_METRIC_TYPE_OSDC_CACHE_INFO,
  CLIENT_METRIC_TYPE_READ_WRITE_IOPS,
};
std::ostream &operator<<(std::ostream &os, const ClientMetricType &type);

struct CapInfoPayload {
  static const ClientMetricType METRIC_TYPE = ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO;

  uint64_t cap_hits = 0;
  uint64_t cap_misses = 0;
  uint64_t nr_caps = 0;

  CapInfoPayload() { }
  CapInfoPayload(uint64_t cap_hits, uint64_t cap_misses, uint64_t nr_caps)
    : cap_hits(cap_hits), cap_misses(cap_misses), nr_caps(nr_caps) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct OSDCCacheInfoPayload {
  static const ClientMetricType METRIC_TYPE = ClientMetricType::CLIENT_METRIC_TYPE_OSDC_CACHE_INFO;

  uint64_t osdc_cache_hits = 0;
  uint64_t osdc_cache_misses = 0;
  uint64_t osdc_cache_size = 0;

  OSDCCacheInfoPayload() { }
  OSDCCacheInfoPayload(uint64_t osdc_cache_hits, uint64_t osdc_cache_misses,
                       uint64_t osdc_cache_size)
    : osdc_cache_hits(osdc_cache_hits), osdc_cache_misses(osdc_cache_misses),
      osdc_cache_size(osdc_cache_size) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct ReadWriteIOPSPayload {
  static const ClientMetricType METRIC_TYPE = ClientMetricType::CLIENT_METRIC_TYPE_READ_WRITE_IOPS;

  double read_iops = 0.0;
  double write_iops = 0.0;

  ReadWriteIOPSPayload() { }
  ReadWriteIOPSPayload(double read_iops, double write_iops)
    : read_iops(read_iops), write_iops(write_iops) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct UnknownPayload {
  static const ClientMetricType METRIC_TYPE = static_cast<ClientMetricType>(-1);

  UnknownPayload() { }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

typedef boost::variant<CapInfoPayload,
                       OSDCCacheInfoPayload,
                       ReadWriteIOPSPayload,
                       UnknownPayload> ClientMetricPayload;

// metric update message sent by clients
struct ClientMetricMessage {
public:
  ClientMetricMessage(const ClientMetricPayload &payload = UnknownPayload())
    : payload(payload) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;

  ClientMetricPayload payload;
};
WRITE_CLASS_ENCODER(ClientMetricMessage);

#endif // CEPH_MDS_METRICS_CLIENT_TYPES_H
