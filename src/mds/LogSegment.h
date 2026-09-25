// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_LOGSEGMENT_H
#define CEPH_LOGSEGMENT_H

#include "include/elist.h"
#include "include/interval_set.h"
#include "include/Context.h"
#include "include/types.h" // for version_t
#include "include/utime.h"

#include <unordered_set>

#include <cstdint>
#include <map>
#include <iosfwd>
#include <set>
#include <string_view>
#include <vector>

namespace ceph { class Formatter; }

struct inodeno_t;
struct dirfrag_t;
struct metareqid_t;
class CDir;
class CInode;
class CDentry;
class MDSContext;
class C_MDSInternalNoop;
using MDSGather = C_GatherBase<MDSContext, C_MDSInternalNoop>;
using MDSGatherBuilder = C_GatherBuilderBase<MDSContext, MDSGather>;
class MDSRank;
struct MDPeerUpdate;

class LogSegment {
 public:
  using seq_t = uint64_t;

  LogSegment(uint64_t _seq, loff_t off=-1);
  ~LogSegment() noexcept;

  /*
   * Things a segment still owes before it can be expired, in the order
   * try_to_expire() evaluates them.  Kept in lockstep with try_to_expire():
   * every branch there that adds a sub to the gather has a counterpart here,
   * so that dump_expiry_obligations() can report the same set without side
   * effects.
   *
   * These are obligations, not necessarily blockers: the lists are filled
   * when events are journaled, so a segment carries them from birth whether
   * or not expiry has ever been attempted on it.
   */
  enum {
    EXPIRY_DIRTY_DIRFRAGS = 0,
    EXPIRY_UNCOMMITTED_LEADERS,
    EXPIRY_UNCOMMITTED_PEERS,
    EXPIRY_UNCOMMITTED_FRAGMENTS,
    EXPIRY_DIRTY_DIRFRAG_DIR,
    EXPIRY_DIRTY_DIRFRAG_DIRFRAGTREE,
    EXPIRY_DIRTY_DIRFRAG_NEST,
    EXPIRY_OPEN_FILES,
    EXPIRY_OPEN_FILE_TABLE,
    EXPIRY_DIRTY_PARENT_INODES,
    EXPIRY_INOTABLE,
    EXPIRY_SESSIONMAP,
    EXPIRY_TOUCHED_SESSIONS,
    EXPIRY_MDSTABLE_CLIENT,
    EXPIRY_MDSTABLE_SERVER,
    EXPIRY_TRUNCATING_INODES,
    EXPIRY_PURGING_INODES,
    EXPIRY_NUM_OBLIGATIONS
  };
  static std::string_view get_expiry_obligation_name(int obligation);
  static std::string_view get_expiry_obligation_reason(int obligation);

  struct expiry_obligations_t {
    unsigned count[EXPIRY_NUM_OBLIGATIONS] = {0};
    /*
     * Of those, how many cannot currently make progress -- frozen, mid
     * export, or sitting on an unstable lock.  try_to_expire() issues work
     * for every item and the gather waits on all of them, so one stuck item
     * among thousands is what actually holds the segment.  That is the
     * number worth looking at.
     */
    unsigned blocked[EXPIRY_NUM_OBLIGATIONS] = {0};
    /* dirfrags try_to_expire() will actually commit, after de-duplication */
    unsigned num_dirfrags_to_commit = 0;
    uint64_t oft_committed_log_seq = 0;

    unsigned num_categories() const {
      unsigned n = 0;
      for (int i = 0; i < EXPIRY_NUM_OBLIGATIONS; ++i) {
        if (count[i]) {
          ++n;
        }
      }
      return n;
    }
    bool empty() const {
      return num_categories() == 0;
    }
  };

  /*
   * Count what this segment still owes, without emitting anything.  Shared
   * by "dump log segments" and by the MDS_TRIM health metric so the two
   * cannot disagree.
   */
  void count_expiry_obligations(MDSRank *mds, expiry_obligations_t &out);

  void try_to_expire(MDSRank *mds, MDSGatherBuilder &gather_bld, int op_prio);

  /*
   * Report, without side effects, what this segment still owes.  Returns
   * the number of distinct obligation categories found.  The lists walked
   * here are self-clearing -- an object removes itself once its obligation
   * is discharged -- so what remains is what is still owed.
   */
  int dump_expiry_obligations(ceph::Formatter *f, MDSRank *mds, bool detail);
  void dump(ceph::Formatter *f, MDSRank *mds, bool detail);

  void purge_inodes_finish(interval_set<inodeno_t>& inos);
  void set_purged_cb(MDSContext* c){
    ceph_assert(purged_cb == NULL);
    purged_cb = c;
  }
  void wait_for_expiry(MDSContext *c)
  {
    ceph_assert(c != NULL);
    expiry_waiters.push_back(c);
  }

  const seq_t seq;
  uint64_t offset, end;
  uint64_t num_events = 0;

  // dirty items
  elist<CDir*>    dirty_dirfrags, new_dirfrags;
  elist<CInode*>  dirty_inodes;
  elist<CDentry*> dirty_dentries;

  elist<CInode*>  open_files;
  elist<CInode*>  dirty_parent_inodes;
  elist<CInode*>  dirty_dirfrag_dir;
  elist<CInode*>  dirty_dirfrag_nest;
  elist<CInode*>  dirty_dirfrag_dirfragtree;

  std::set<CInode*> truncating_inodes;
  interval_set<inodeno_t> purging_inodes;
  MDSContext* purged_cb = nullptr;

  std::map<int, std::unordered_set<version_t>> pending_commit_tids;  // mdstable
  std::set<metareqid_t> uncommitted_leaders;
  std::set<metareqid_t> uncommitted_peers;
  std::set<dirfrag_t> uncommitted_fragments;

  // client request ids
  std::map<int, ceph_tid_t> last_client_tids;

  // potentially dirty sessions
  std::set<entity_name_t> touched_sessions;

  // table version
  version_t inotablev = 0;
  version_t sessionmapv = 0;
  std::map<int,version_t> tablev;

  std::vector<MDSContext*> expiry_waiters;

  /*
   * Expiry attempt bookkeeping, maintained by MDLog::try_expire().  These
   * cannot be derived from the obligation lists, which only say what is
   * outstanding now, not how long it has been outstanding.
   */
  unsigned expiry_attempts = 0;
  int last_expiry_subs = 0;     // gather subs created by the last attempt
  utime_t first_expiry_attempt;
  utime_t last_expiry_attempt;
};

std::ostream& operator<<(std::ostream& out, const LogSegment& ls);

#endif
