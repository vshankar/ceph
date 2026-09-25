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
#ifndef CEPH_MDLOG_H
#define CEPH_MDLOG_H

#include "common/fair_mutex.h"
#include "include/common_fwd.h"

enum {
  l_mdl_first = 5000,
  l_mdl_evlrg,
  l_mdl_evadd,
  l_mdl_evex,
  l_mdl_evtrm,
  l_mdl_ev,
  l_mdl_evexg,
  l_mdl_evexd,
  l_mdl_segadd,
  l_mdl_segex,
  l_mdl_segtrm,
  l_mdl_seg,
  l_mdl_segmjr,
  l_mdl_segexg,
  l_mdl_segexd,
  l_mdl_expos,
  l_mdl_wrpos,
  l_mdl_rdpos,
  l_mdl_jlat,
  l_mdl_replayed,
  l_mdl_last,
};

#include "include/fs_types.h" // for inodeno_t
#include "include/types.h"
#include "include/Context.h"

#include "common/DecayCounter.h"
#include "common/Thread.h"

#include "LogSegment.h"
#include "SegmentBoundary.h"
#include "LogSegmentRef.h"

#include <atomic>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace ceph { class Formatter; }

struct EstimatedReplayTime;
class Journaler;
class JournalPointer;
class LogEvent;
class MDSContext;
class MDSLogContextBase;
class MDSMap;
class MDSRank;
class LogSegment;
class ESubtreeMap;

class MDLog {
public:

  MDLog(MDSRank *m);
  ~MDLog();

  const std::set<LogSegmentRef> &get_expiring_segments() const
  {
    return expiring_segments;
  }

  void create_logger();
  void set_write_iohint(unsigned iohint_flags);

  LogSegmentRef const& peek_current_segment() {
    static LogSegmentRef const nullsegment = nullptr;
    return segments.empty() ? nullsegment : segments.rbegin()->second;
  }

  LogSegmentRef const& get_current_segment() { 
    ceph_assert(!segments.empty());
    return segments.rbegin()->second;
  }

  LogSegmentRef const& get_segment(LogSegment::seq_t seq) {
    static LogSegmentRef const nullsegment = nullptr;
    auto it = segments.find(seq);
    if (it != segments.end()) {
      return it->second;
    } else {
      return nullsegment;
    }
  }

  bool have_any_segments() const {
    return !segments.empty();
  }

  bool is_segment_expiring(LogSegment::seq_t seq) const {
    auto it = segments.find(seq);
    return it != segments.end() && expiring_segments.count(it->second);
  }
  bool is_segment_expired(LogSegment::seq_t seq) const {
    auto it = segments.find(seq);
    return it != segments.end() && expired_segments.count(it->second);
  }
  bool is_major_segment(LogSegment::seq_t seq) const {
    return major_segments.count(seq);
  }

  /*
   * Is this segment fully flushed, as trim() judges it?  Uses MDLog's
   * cached safe_pos rather than asking the Journaler: that avoids taking
   * the Journaler lock under submit_mutex, and it makes the answer agree
   * with the test trim() itself applies before trying to expire.
   */
  bool is_segment_flushed(const LogSegment& ls) const {
    return !pending_events.count(ls.seq) && ls.end <= safe_pos;
  }

  bool is_current_segment(LogSegment::seq_t seq) const {
    return !segments.empty() && segments.rbegin()->first == seq;
  }

  /*
   * The segment _trim_expired_segments() breaks on: the oldest one not yet
   * expired.  This -- not segments.begin() -- is what holds expire_pos, and
   * therefore removal, in place.
   *
   * Deliberately not called "blocking": being unexpired is not the same as
   * being stuck.  It may be untouched (trim() never asked), and it may be
   * the current segment, which _expired() refuses to expire while it is
   * still being written to -- in which case expire_pos resting at its start
   * means the journal is trimmed as far as it can be.  Null only when the
   * journal has no segments at all.
   */
  LogSegmentRef get_first_unexpired_segment() const {
    static LogSegmentRef const nullsegment = nullptr;
    for (const auto& p : segments) {
      if (!expired_segments.count(p.second)) {
        return p.second;
      }
    }
    return nullsegment;
  }

  void dump_segments(ceph::Formatter *f, std::optional<LogSegment::seq_t> seq,
                     bool all, bool detail);

  /*
   * Formatter-free summary of what is holding trimming back, for the
   * MDS_TRIM health metric.  Cheap enough to compute on the beacon path:
   * it walks one segment's obligation lists, not the whole journal.
   */
  struct trim_blocker_info_t {
    bool have_segment = false;      // false => the journal has no segments
    LogSegment::seq_t seq = 0;
    bool is_current = false;        // the segment still being written to
    bool expiring = false;
    unsigned expiry_attempts = 0;
    double blocked_for = 0.0;       // seconds since the first expiry attempt
    unsigned num_categories = 0;
    std::string summary;            // e.g. "dirty_dirfrag_dir(1), open_file_table(1)"
    std::string categories;         // e.g. "dirty_dirfrag_dir+open_file_table"
  };
  trim_blocker_info_t get_trim_blocker_info();

  void flush_logger();

  uint64_t get_num_events() const { return num_events; }
  uint64_t get_num_segments() const { return segments.size(); }

  auto get_debug_subtrees() const {
    return events_per_segment;
  }
  auto get_max_segments() const {
    return max_segments;
  }

  uint64_t get_read_pos() const;
  uint64_t get_write_pos() const;
  uint64_t get_safe_pos() const;
  Journaler *get_journaler() { return journaler; }
  bool empty() const { return segments.empty(); }

  uint64_t get_last_major_segment_seq() const {
    ceph_assert(!major_segments.empty());
    return *major_segments.rbegin();
  }
  uint64_t get_last_segment_seq() const {
    ceph_assert(!segments.empty());
    return segments.rbegin()->first;
  }

  bool is_capped() const { return mds_is_shutting_down; }
  void cap();

  void kick_submitter();
  void shutdown();

  void finish_head_waiters();

  LogSegment::seq_t submit_entry(LogEvent *e, MDSLogContextBase* c = 0) {
    std::lock_guard l(submit_mutex);
    auto seq = _submit_entry(e, c);
    _segment_upkeep();
    submit_cond.notify_all();
    return seq;
  }

  void wait_for_safe(Context* c);
  void flush();
  bool is_flushed() const {
    return unflushed == 0;
  }

  void trim_expired_segments(MDSContext* ctx=nullptr) {
    std::unique_lock locker(submit_mutex);
    _trim_expired_segments(locker, ctx);
  }
  int trim_all() {
    return trim_to(0);
  }
  int trim_to(SegmentBoundary::seq_t);

  void create(MDSContext *onfinish);  // fresh, empty log! 
  void open(MDSContext *onopen);      // append() or replay() to follow!
  void reopen(MDSContext *onopen);
  void append();
  void replay(MDSContext *onfinish);
  EstimatedReplayTime get_estimated_replay_finish_time();

  void standby_trim_segments();

  void handle_conf_change(const std::set<std::string>& changed, const MDSMap& mds_map);

  void dump_replay_status(Formatter *f) const;

  MDSRank *mds;
  // replay state
  std::map<inodeno_t, std::set<inodeno_t>> pending_exports;

  // beacon needs me too
  bool is_trim_slow() const;

protected:
  struct PendingEvent {
    PendingEvent(LogEvent *e, Context* c, bool f=false) : le(e), fin(c), flush(f) {}
    LogEvent *le;
    Context* fin;
    bool flush;
  };

  // -- replay --
  class ReplayThread : public Thread {
  public:
    explicit ReplayThread(MDLog *l) : log(l) {}
    void* entry() override {
      log->_replay_thread();
      return 0;
    }
  private:
    MDLog *log;
  } replay_thread;

  // Journal recovery/rewrite logic
  class RecoveryThread : public Thread {
  public:
    explicit RecoveryThread(MDLog *l) : log(l) {}
    void set_completion(MDSContext *c) {completion = c;}
    void* entry() override {
      log->_recovery_thread(completion);
      return 0;
    }
  private:
    MDLog *log;
    MDSContext *completion = nullptr;
  } recovery_thread;

  class SubmitThread : public Thread {
  public:
    explicit SubmitThread(MDLog *l) : log(l) {}
    void* entry() override {
      log->_submit_thread();
      return 0;
    }
  private:
    MDLog *log;
  } submit_thread;

  friend class ReplayThread;
  friend class C_MDL_Replay;
  friend class MDSLogContextBase;
  friend class SubmitThread;
  // -- subtreemaps --
  friend class ESubtreeMap;
  friend class MDCache;

  void _replay();         // old way
  void _replay_thread();  // new way

  void _recovery_thread(MDSContext *completion);
  void _reformat_journal(JournalPointer const &jp, Journaler *old_journal, MDSContext *completion);

  void set_safe_pos(uint64_t pos)
  {
    std::lock_guard l(submit_mutex);
    ceph_assert(pos >= safe_pos);
    safe_pos = pos;
  }

  void _submit_thread();

  LogSegmentRef const& get_oldest_segment() {
    return segments.begin()->second;
  }
  void remove_oldest_segment() {
    ceph_assert(!segments.empty());
    segments.erase(segments.begin());
  }

  uint64_t num_events = 0; // in events
  uint64_t unflushed = 0;
  bool mds_is_shutting_down = false;

  // Log position which is persistent *and* for which
  // submit_entry wait_for_safe callbacks have already
  // been called.
  uint64_t safe_pos = 0;

  inodeno_t ino;
  Journaler *journaler = nullptr;

  PerfCounters *logger = nullptr;

  bool already_replayed = false;

  std::vector<MDSContext*> waitfor_replay;

  // -- segments --
  std::map<uint64_t,LogSegmentRef> segments;
  std::size_t pre_segments_size = 0;            // the num of segments when the mds finished replay-journal, to calc the num of segments growing
  LogSegment::seq_t event_seq = 0;
  uint64_t expiring_events = 0;
  uint64_t expired_events = 0;

  int64_t mdsmap_up_features = 0;
  std::map<uint64_t,std::list<PendingEvent> > pending_events; // log segment -> event list
  ceph::fair_mutex submit_mutex{"MDLog::submit_mutex"};
  std::condition_variable_any submit_cond;

private:
  friend class C_MaybeExpiredSegment;
  friend class C_MDL_Flushed;
  friend class C_OFT_Committed;

  void try_to_commit_open_file_table(uint64_t last_seq);
  LogSegmentRef const& _start_new_segment(SegmentBoundary* sb);
  void _segment_upkeep();
  LogSegment::seq_t _submit_entry(LogEvent* e, MDSLogContextBase* c);

  void try_expire(LogSegmentRef const& ls, int op_prio);
  void _maybe_expired(LogSegmentRef const& ls, int op_prio);
  void _expired(LogSegmentRef const& ls);
  void _trim_expired_segments(auto& locker, MDSContext* ctx=nullptr);
  void write_head(MDSContext *onfinish);

  void trim();
  void log_trim_upkeep(void);

  bool debug_subtrees;
  std::atomic_uint64_t event_large_threshold; // accessed by submit thread
  uint64_t events_per_segment;
  int64_t max_events;
  uint64_t max_segments;
  uint64_t minor_segments_per_major_segment;
  bool pause;
  bool skip_corrupt_events;
  bool skip_unbounded_events;

  std::set<uint64_t> major_segments;
  std::set<LogSegmentRef> expired_segments;
  std::set<LogSegmentRef> expiring_segments;
  uint64_t minor_segments_since_last_major_segment = 0;
  double log_warn_factor;

  // log trimming decay counter
  DecayCounter log_trim_counter;

  // log trimming upkeeper thread
  std::thread upkeep_thread;
  // guarded by mds_lock
  std::condition_variable_any cond;
  std::atomic<bool> upkeep_log_trim_shutdown{false};

  std::map<uint64_t, std::vector<Context*>> waiting_for_expire; // protected by mds_lock

  ceph::coarse_mono_time replay_start_time = ceph::coarse_mono_clock::zero();
};
#endif
