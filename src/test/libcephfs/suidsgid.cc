// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "include/buffer.h"
#include "include/stringify.h"
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>
#include <iostream>
#include <vector>
#include "json_spirit/json_spirit.h"

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

using namespace std;
struct ceph_mount_info *admin;
struct ceph_mount_info *cmount;
char c_path[128];

void run_fallocate_test_case(int mode, int result, bool with_admin=false)
{
  struct ceph_statx stx;
  int flags = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;

  ASSERT_EQ(0, ceph_chmod(admin, c_path, mode));

  struct ceph_mount_info *_cmount = cmount;
  if (with_admin) {
    _cmount = admin;
  }
  int fd = ceph_open(_cmount, c_path, O_RDWR, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_fallocate(_cmount, fd, flags, 1024, 40960));
  ASSERT_EQ(ceph_statx(_cmount, c_path, &stx, CEPH_STATX_MODE, 0), 0);
  std::cout << "After ceph_fallocate, mode: 0" << oct << mode << " -> 0"
            << (stx.stx_mode & 07777) << dec << std::endl;
  ASSERT_EQ(stx.stx_mode & (S_ISUID|S_ISGID), result);
  ceph_close(_cmount, fd);
}

TEST(SuidsgidTest, Fallocate) {
  string user = "libcephfs_fallocate_test_" + stringify(rand());
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  sprintf(c_path, "/suidsgid_test_file_%d", getpid());
  int fd = ceph_open(admin, c_path, O_CREAT|O_RDWR, 0766);
  ASSERT_GE(ceph_ftruncate(admin, fd, 10000000), 0);
  ceph_close(admin, fd);

  ASSERT_EQ(0, ceph_create(&cmount, user.c_str()));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_init(cmount), 0);
  UserPerm *perms = ceph_userperm_new(123, 456, 0, NULL);
  ASSERT_NE(nullptr, perms);
  ASSERT_EQ(0, ceph_mount_perms_set(cmount, perms));
  ceph_userperm_destroy(perms);
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  // 1, Commit to a non-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06666, 0); // a+rws

  // 2, Commit to a group-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06676, 0); // g+x,a+rws

  // 3, Commit to a user-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06766, 0); // u+x,a+rws,g-x

  // 4, Commit to a all-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06777, 0); // a+rwxs

  // 5, Commit to a non-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06666, S_ISUID|S_ISGID, true); // a+rws

  // 6, Commit to a group-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06676, S_ISUID|S_ISGID, true); // g+x,a+rws

  // 7, Commit to a user-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06766, S_ISUID|S_ISGID, true); // u+x,a+rws,g-x

  // 8, Commit to a all-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06777, S_ISUID|S_ISGID, true); // a+rwxs

  // 9, Commit to a group-exec file by an unprivileged user clears sgid
  run_fallocate_test_case(02676, 0); // a+rw,g+rwxs

  // 10, Commit to a all-exec file by an unprivileged user clears sgid.
  run_fallocate_test_case(02777, 0); // a+rwx,g+rwxs

  // clean up
  ceph_shutdown(cmount);
  ceph_shutdown(admin);
}
