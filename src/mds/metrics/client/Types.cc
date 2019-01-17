// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "include/stringify.h"

#include "Types.h"

std::ostream &operator<<(std::ostream &os, const ClientMetricType &type) {
  switch(type) {
  case ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO:
    os << "CAP_INFO";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_OSDC_CACHE_INFO:
    os << "OSDC_CACHE_INFO";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_READ_WRITE_IOPS:
    os << "READ_WRITE_IOPS";
    break;
  default:
    ceph_abort();
  }

  return os;
}

class EncodePayloadVisitor : public boost::static_visitor<void> {
public:
  explicit EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename ClientMetricPayload>
  inline void operator()(const ClientMetricPayload &payload) const {
    using ceph::encode;
    encode(static_cast<uint32_t>(ClientMetricPayload::METRIC_TYPE), m_bl);
    payload.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class DecodePayloadVisitor : public boost::static_visitor<void> {
public:
  DecodePayloadVisitor(bufferlist::const_iterator &iter) : m_iter(iter) {
  }

  template <typename ClientMetricPayload>
  inline void operator()(ClientMetricPayload &payload) const {
    using ceph::decode;
    payload.decode(m_iter);
  }

private:
  bufferlist::const_iterator &m_iter;
};

class DumpPayloadVisitor : public boost::static_visitor<void> {
public:
  explicit DumpPayloadVisitor(Formatter *formatter) : m_formatter(formatter) {
  }

  template <typename ClientMetricPayload>
  inline void operator()(const ClientMetricPayload &payload) const {
    ClientMetricType metric_type = ClientMetricPayload::METRIC_TYPE;
    m_formatter->dump_string("client_metric_type", stringify(metric_type));
    payload.dump(m_formatter);
  }

private:
  Formatter *m_formatter;
};

// vairant : cap info
void CapInfoPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(cap_hits, bl);
  encode(cap_misses, bl);
  encode(nr_caps, bl);
  ENCODE_FINISH(bl);
}

void CapInfoPayload::decode(bufferlist::const_iterator &iter) {
  using ceph::decode;
  DECODE_START(1, iter);
  decode(cap_hits, iter);
  decode(cap_misses, iter);
  decode(nr_caps, iter);
  DECODE_FINISH(iter);
}

void CapInfoPayload::dump(Formatter *f) const {
  f->dump_int("cap_hits", cap_hits);
  f->dump_int("cap_misses", cap_misses);
  f->dump_int("num_caps", nr_caps);
}

// variant : osdc cache info
void OSDCCacheInfoPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(osdc_cache_hits, bl);
  encode(osdc_cache_misses, bl);
  encode(osdc_cache_size, bl);
  ENCODE_FINISH(bl);
}

void OSDCCacheInfoPayload::decode(bufferlist::const_iterator &iter) {
  using ceph::decode;
  DECODE_START(1, iter);
  decode(osdc_cache_hits, iter);
  decode(osdc_cache_misses, iter);
  decode(osdc_cache_size, iter);
  DECODE_FINISH(iter);
}

void OSDCCacheInfoPayload::dump(Formatter *f) const {
  f->dump_int("osdc_cache_hits", osdc_cache_hits);
  f->dump_int("osdc_cache_misses", osdc_cache_misses);
  f->dump_int("osdc_cache_size", osdc_cache_size);
}

// variant : read write iops
void ReadWriteIOPSPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  ENCODE_START(1, 1, bl);
  encode(read_iops, bl);
  encode(write_iops, bl);
  ENCODE_FINISH(bl);
}

void ReadWriteIOPSPayload::decode(bufferlist::const_iterator &iter) {
  using ceph::decode;
  DECODE_START(1, iter);
  decode(read_iops, iter);
  decode(write_iops, iter);
  DECODE_FINISH(iter);
}

void ReadWriteIOPSPayload::dump(Formatter *f) const {
  f->dump_float("read_iops", read_iops);
  f->dump_float("write_iops", write_iops);
}

// variant : unknown
void UnknownPayload::encode(bufferlist &bl) const {
  ceph_abort();
}

void UnknownPayload::decode(bufferlist::const_iterator &iter) {
  ceph_abort();
}

void UnknownPayload::dump(Formatter *f) const {
  ceph_abort();
}

// metric update message
void ClientMetricMessage::encode(bufferlist &bl) const {
  boost::apply_visitor(EncodePayloadVisitor(bl), payload);
}
void ClientMetricMessage::decode(bufferlist::const_iterator &iter) {
  using ceph::decode;

  uint32_t metric_type;
  decode(metric_type, iter);

  switch (metric_type) {
  case ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO:
    payload = CapInfoPayload();
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_OSDC_CACHE_INFO:
    payload = OSDCCacheInfoPayload();
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_READ_WRITE_IOPS:
    payload = ReadWriteIOPSPayload();
    break;
  default:
    payload = UnknownPayload();
    break;
  }

  boost::apply_visitor(DecodePayloadVisitor(iter), payload);
}
void ClientMetricMessage::dump(Formatter *f) const {
  apply_visitor(DumpPayloadVisitor(f), payload);
}
