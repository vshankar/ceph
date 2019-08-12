// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_PINGER_H
#define CEPH_MDS_PINGER_H

#include <map>

#include "include/types.h"
#include "include/spinlock.h"

#include "common/ceph_time.h"
#include "messages/MMDSPing.h"

#include "mdstypes.h"

class MDSRank;

class MDSPinger {
public:
  MDSPinger(MDSRank *mds);

  // send a ping message to an mds rank. initialize ping state if
  // required.
  void send_ping(mds_rank_t rank);

  // check if a pong response is valid. a pong reponse from an
  // mds is valid if at least one ping message was sent to the
  // mds and the sequence number in the pong is outstanding.
  bool pong_received(mds_rank_t rank, version_t seq);

  // reset the ping state for a given rank
  void reset_ping(mds_rank_t rank);

  // check if a rank is lagging (based on pong response) responding
  // to a ping message.
  bool is_rank_lagging(mds_rank_t rank);

private:
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  struct PingState {
    version_t last_seq = 0;
    std::map<version_t, time> seq_time_map;
    time last_acked_time = clock::now();
  };

  MDSRank *mds;
  ceph::spinlock lock;
  std::map<mds_rank_t, PingState> ping_state_by_rank;
};

#endif // CEPH_MDS_PINGER_H
