// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_PERF_METRIC_TYPES_H
#define CEPH_MDS_PERF_METRIC_TYPES_H

#include <iostream>

#include "include/encoding.h"
#include "mdstypes.h"

enum UpdateType {
  UPDATE_TYPE_REFRESH = 0,
  UPDATE_TYPE_REMOVE,
};

struct CapHitMetric {
  uint64_t hits = 0;
  uint64_t misses = 0;

  void encode(bufferlist &bl) const {
    using ceph::encode;

    ENCODE_START(1, 1, bl);
    encode(hits, bl);
    encode(misses, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;

    DECODE_START(1, iter);
    decode(hits, iter);
    decode(misses, iter);
    DECODE_FINISH(iter);
  }

  friend std::ostream& operator<<(std::ostream& os, const CapHitMetric &metric) {
    os << "{hits=" << metric.hits << ", misses=" << metric.misses << "}";
    return os;
  }
};

struct OSDCCacheMetric {
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t size = 0;

  void encode(bufferlist &bl) const {
    using ceph::encode;

    ENCODE_START(1, 1, bl);
    encode(hits, bl);
    encode(misses, bl);
    encode(size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;

    DECODE_START(1, iter);
    decode(hits, iter);
    decode(misses, iter);
    decode(size, iter);
    DECODE_FINISH(iter);
  }

  friend std::ostream& operator<<(std::ostream& os, const OSDCCacheMetric &metric) {
    os << "{hits=" << metric.hits << ", misses=" << metric.misses << ", size="
       << metric.size << "}";
    return os;
  }
};

struct IOPSMetric {
  double read_iops = 0.0;
  double write_iops = 0.0;

  void encode(bufferlist &bl) const {
    using ceph::encode;

    ENCODE_START(1, 1, bl);
    encode(read_iops, bl);
    encode(write_iops, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;

    DECODE_START(1, iter);
    decode(read_iops, iter);
    decode(write_iops, iter);
    DECODE_FINISH(iter);
  }

  friend std::ostream& operator<<(std::ostream& os, const IOPSMetric &metric) {
    os << "{read_iops=" << metric.read_iops << ", write_iops=" << metric.write_iops << "}";
    return os;
  }
};

WRITE_CLASS_ENCODER(CapHitMetric);
WRITE_CLASS_ENCODER(OSDCCacheMetric);
WRITE_CLASS_ENCODER(IOPSMetric);

// TODO: forward only those metrics that have changed.
struct Metrics {
  // metrics
  CapHitMetric cap_hit_metric;
  OSDCCacheMetric osdc_cache_metric;
  IOPSMetric iops_metric;

  // metric update type
  UpdateType update_type = UpdateType::UPDATE_TYPE_REFRESH;

  void encode(bufferlist &bl) const {
    using ceph::encode;

    ENCODE_START(1, 1, bl);
    encode(static_cast<uint32_t>(update_type), bl);

    encode(cap_hit_metric, bl);
    encode(osdc_cache_metric, bl);
    encode(iops_metric, bl);

    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;

    DECODE_START(1, iter);
    uint32_t type;
    decode(type, iter);
    update_type = static_cast<UpdateType>(type);

    decode(cap_hit_metric, iter);
    decode(osdc_cache_metric, iter);
    decode(iops_metric, iter);

    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("update_type", static_cast<uint32_t>(update_type));
  }

  friend std::ostream& operator<<(std::ostream& os, const Metrics& metrics) {
    os << "[update_type=" << metrics.update_type << ", metrics={"
       << "cap_hit_metric=" << metrics.cap_hit_metric
       << ", osdc_cache_metric" << metrics.osdc_cache_metric
       << ", iops_metric=" << metrics.iops_metric
       << "}]";
    return os;
  }
};

struct MetricsMessage {
  mds_rank_t rank = MDS_RANK_NONE;
  std::map<entity_inst_t, Metrics> client_metrics_map;

  MetricsMessage() {
  }
  MetricsMessage(mds_rank_t rank)
    : rank(rank) {
  }

  void encode(bufferlist &bl, uint64_t features) const {
    using ceph::encode;
    encode(rank, bl);
    encode(client_metrics_map, bl, features);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    decode(rank, iter);
    decode(client_metrics_map, iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("rank", rank);
    for (auto &p : client_metrics_map) {
      f->dump_stream("client") << p.first;
      f->dump_object("metrics", p.second);
    }
  }

  friend std::ostream& operator<<(std::ostream& os, const MetricsMessage& metrics_message) {
    os << "[rank=" << metrics_message.rank << ", metrics=" << metrics_message.client_metrics_map
       << "]";
    return os;
  }
};

WRITE_CLASS_ENCODER(Metrics)
WRITE_CLASS_ENCODER_FEATURES(MetricsMessage);

#endif // CEPH_MDS_PERF_METRIC_TYPES_H
