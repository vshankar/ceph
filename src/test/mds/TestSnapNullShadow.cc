// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

// Reproduces the snap-lookup ENOENT that remains after
// https://github.com/ceph/ceph/pull/70525 (tracker 78548).
//
// A head negative lookup installs a null dentry [2, CEPH_NOSNAP]. That
// range covers every historical snapid, so CDir::lookup() returns the null
// for a snapshot of the same name. The patch fetches the dirfrag when that
// happens, but CDir::_load_dentry() uses the same lookup, sees the null, and
// does not call add_primary_dentry(). _omap_fetched() then mark_complete()s
// the frag. The next lookup hits the null in a complete directory and
// path_traverse returns ENOENT without fetching again.
//
// LoadSnapDentry installs the on-disk primary when the cache has no dentry.
// That case passes. ShadowingNullHidesSnapDentry is the bug: it fails while
// the null is left in place.

#include <sys/stat.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "include/ceph_features.h"
#include "include/Context.h"

#include "common/LogClient.h"
#include "common/Timer.h"
#include "common/fair_mutex.h"
#include "global/global_context.h"
#include "mds/Beacon.h"
#include "mds/CDentry.h"
#include "mds/CDir.h"
#include "mds/CInode.h"
#include "mds/MDCache.h"
#include "mds/MDLog.h"
#include "mds/MDSMap.h"
#include "mds/MDSRank.h"
#include "mgr/MgrClient.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"

#include "gtest/gtest.h"

#include <boost/asio/io_context.hpp>

namespace {

class ExposedDir : public CDir {
public:
  using CDir::CDir;

  CDentry *load_dentry(std::string_view key,
                       std::string_view dname,
                       snapid_t last,
                       ceph::buffer::list &bl) {
    bool force_dirty = false;
    return _load_dentry(key, dname, last, bl, 0, nullptr, 0.0, &force_dirty);
  }

  // Drop dentries so MDCache's LRU is empty before the rank is destroyed.
  void purge() {
    std::vector<CDentry*> dns;
    dns.reserve(items.size());
    for (auto &p : items)
      dns.push_back(p.second);
    std::vector<CInode*> children;
    for (auto *dn : dns) {
      if (dn->get_linkage()->is_primary())
        children.push_back(dn->get_linkage()->get_inode());
      remove_dentry(dn);
    }
    for (auto *in : children)
      mdcache->remove_inode(in);
  }
};

class C_Noop : public Context {
public:
  void finish(int) override {}
};

class TestRank : public MDSRank {
public:
  using MDSRank::MDSRank;

  void init_loggers() { create_logger(); }
  void shutdown_threads() {
    mdlog->shutdown();
    mdcache->shutdown();
  }
};

ceph::buffer::list encode_snap_primary(snapid_t first, inodeno_t ino)
{
  InodeStore store;
  auto *pi = store.get_inode();
  pi->ino = ino;
  pi->version = 1;
  pi->mode = S_IFREG | 0644;
  pi->nlink = 1;

  ceph::buffer::list inode_bl;
  store.encode(inode_bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

  ceph::buffer::list bl;
  using ceph::encode;
  encode(first, bl);
  bl.append('i');
  ENCODE_START(2, 1, bl);
  encode(std::string(), bl);
  bl.claim_append(inode_bl);
  ENCODE_FINISH(bl);
  return bl;
}

} // namespace

class SnapNullShadow : public ::testing::Test {
protected:
  boost::asio::io_context ioctx;
  std::unique_ptr<MonClient> monc;
  Messenger *msgr = nullptr;
  std::unique_ptr<MgrClient> mgrc;
  std::unique_ptr<LogClient> log_client;
  LogChannelRef clog;
  std::unique_ptr<Beacon> beacon;
  ceph::fair_mutex mds_lock{"mds_lock"};
  CommonSafeTimer<ceph::fair_mutex> timer{g_ceph_context, mds_lock};
  std::unique_ptr<MDSMap> mdsmap;
  TestRank *rank = nullptr;
  bool locked = false;
  bool timer_started = false;
  std::vector<ExposedDir*> dirs;

  void SetUp() override {
    std::stringstream err;
    ASSERT_EQ(0, g_conf().set_val("mds_bal_export_pin", "false", &err)) << err.str();
    err.str("");
    ASSERT_EQ(0, g_conf().set_val("mds_task_status_update_interval", "3600", &err)) << err.str();

    monc = std::make_unique<MonClient>(g_ceph_context, ioctx);
    msgr = Messenger::create_client_messenger(g_ceph_context, "mds");
    ASSERT_NE(nullptr, msgr);
    mgrc = std::make_unique<MgrClient>(g_ceph_context, msgr, &monc->monmap);
    log_client = std::make_unique<LogClient>(g_ceph_context, msgr, &monc->monmap,
                                             LogClient::NO_FLAGS);
    clog = log_client->create_channel("mds");
    beacon = std::make_unique<Beacon>(g_ceph_context, monc.get(), "mds.a");
    mdsmap = std::make_unique<MDSMap>();

    mds_lock.lock();
    locked = true;
    timer.init();
    timer_started = true;
    rank = new TestRank(mds_rank_t(0), mds_lock, clog, timer, *beacon, mdsmap,
                        msgr, monc.get(), mgrc.get(),
                        new C_Noop, new C_Noop, ioctx);
    rank->init_loggers();
  }

  void TearDown() override {
    if (rank && locked) {
      for (auto *dir : dirs) {
        CInode *diri = dir->get_inode();
        dir->purge();
        rank->mdcache->remove_inode(diri);
      }
      dirs.clear();
      rank->shutdown_threads();
    }
    if (timer_started && locked) {
      timer.shutdown();
      timer_started = false;
    }
    if (locked) {
      mds_lock.unlock();
      locked = false;
    }
    delete rank;
    rank = nullptr;
    delete msgr;
    msgr = nullptr;
  }

  ExposedDir *make_dir(inodeno_t ino) {
    auto *diri = new CInode(rank->mdcache);
    auto *pi = diri->_get_inode();
    pi->ino = ino;
    pi->mode = S_IFDIR | 0755;
    pi->version = 1;
    pi->nlink = 1;
    rank->mdcache->add_inode(diri);
    auto *dir = new ExposedDir(diri, frag_t(), rank->mdcache, true);
    diri->add_dirfrag(dir);
    dirs.push_back(dir);
    return dir;
  }
};

TEST_F(SnapNullShadow, LoadSnapDentry)
{
  const snapid_t first(2);
  const snapid_t snap(10);
  const std::string name = "file";
  const inodeno_t file_ino(0x100000001);

  ExposedDir *dir = make_dir(inodeno_t(2));
  ASSERT_TRUE(dir->is_auth());
  ASSERT_FALSE(dir->is_complete());

  ceph::buffer::list bl = encode_snap_primary(first, file_ino);
  CDentry *dn = dir->load_dentry("0_file", name, snap, bl);
  ASSERT_NE(nullptr, dn);
  ASSERT_TRUE(dn->get_linkage()->is_primary());
  EXPECT_EQ(file_ino, dn->get_linkage()->get_inode()->ino());
  EXPECT_EQ(dn, dir->lookup(name, snap));
}

TEST_F(SnapNullShadow, ShadowingNullHidesSnapDentry)
{
  const snapid_t first(2);
  const snapid_t snap(10);
  const std::string name = "file";
  const inodeno_t file_ino(0x100000002);

  ExposedDir *dir = make_dir(inodeno_t(3));
  // Same null a complete-directory miss inserts: [2, CEPH_NOSNAP].
  CDentry *null_dn = dir->add_null_dentry(name, first, CEPH_NOSNAP);
  ASSERT_TRUE(null_dn->get_linkage()->is_null());
  ASSERT_EQ(null_dn, dir->lookup(name, snap));
  // Preconditions of the fetch added by the patch.
  ASSERT_TRUE(dir->is_auth());
  ASSERT_FALSE(dir->is_complete());
  ASSERT_LT(snap, snapid_t(CEPH_NOSNAP));

  ceph::buffer::list bl = encode_snap_primary(first, file_ino);
  CDentry *loaded = dir->load_dentry("0_file", name, snap, bl);
  // _omap_fetched() marks the frag complete after the keys are loaded.
  dir->mark_complete();

  EXPECT_EQ(null_dn, loaded);
  CDentry *found = dir->lookup(name, snap);
  ASSERT_NE(nullptr, found);
  EXPECT_TRUE(found->get_linkage()->is_primary())
      << "lookup(\"" << name << "\", snapid " << uint64_t(snap)
      << ") still returns null dentry [" << uint64_t(found->first) << ","
      << found->last << "] after _load_dentry of on-disk primary ["
      << uint64_t(first) << "," << uint64_t(snap) << "] ino " << file_ino
      << ". The directory is complete, so the fetch in path_traverse is skipped "
      << "and the snap lookup returns ENOENT.";
}
