// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/ceph_context.h"
#include "include/assert.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "librbd/ImageState.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/MirroringWatcher.h"
#include "journal/Settings.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/journal/cls_journal_client.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/journal/RemoveRequest.h"
#include "librbd/operation/TrimRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RemoveRequest: " << this << " " \
                           << __func__ << " "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_ack_callback;

template<typename I>
RemoveRequest<I>::RemoveRequest(IoCtx &ioctx, const std::string &image_name, const std::string &image_id,
                                bool force, ProgressContext &prog_ctx, ContextWQ *op_work_queue,
                                Context *on_finish) :
  m_image_name(image_name), m_image_id(image_id), m_force(force), m_prog_ctx(prog_ctx),
  m_snap_lock("RemoveRequest::m_snap_lock"), m_op_work_queue(op_work_queue), m_on_finish(on_finish) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());

  m_image_ctx = new I((m_image_id.empty() ? m_image_name : std::string()),
                      m_image_id, nullptr, m_ioctx, false);
}

template<typename I>
void RemoveRequest<I>::send() {
  ldout(m_cct, 20) << dendl;

  open_image();
}

template<typename I>
void RemoveRequest<I>::open_image() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_image>(this);

  m_image_ctx->state->open(ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_open_image(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error opening image: " << cpp_strerror(*result) << dendl;
    switch_thread_context();
    return nullptr;
  }

  m_image_id = m_image_ctx->id;
  m_image_name = m_image_ctx->name;
  m_header_oid = m_image_ctx->header_oid;
  m_old_format = m_image_ctx->old_format;
  m_unknown_format = false;

  check_exclusive_lock();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::check_exclusive_lock() {
  ldout(m_cct, 20) << dendl;

  if (m_image_ctx->exclusive_lock == nullptr) {
    validate_image_removal();
  } else {
    acquire_exclusive_lock();
  }
}

template<typename I>
void RemoveRequest<I>::acquire_exclusive_lock() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  if (m_force) {
    Context *ctx = create_context_callback<klass, &klass::handle_exclusive_lock_force>(this);
    m_image_ctx->exclusive_lock->shut_down(ctx);
  } else {
    Context *ctx = create_context_callback<klass, &klass::handle_exclusive_lock>(this);
    RWLock::WLocker owner_lock(m_image_ctx->owner_lock);
    m_image_ctx->exclusive_lock->try_lock(ctx);
  }
}

template<typename I>
Context *RemoveRequest<I>::handle_exclusive_lock_force(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error shutting down exclusive lock" << cpp_strerror(*result)
                 << dendl;
    send_close_image(*result);
    return nullptr;
  }

  assert(m_image_ctx->exclusive_lock == nullptr);
  validate_image_removal();
  return nullptr;
}

template<typename I>
Context *RemoveRequest<I>::handle_exclusive_lock(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) || !m_image_ctx->exclusive_lock->is_lock_owner()) {
    lderr(m_cct) << "cannot obtain exclusive lock - not removing" << dendl;
    send_close_image(-EBUSY);
    return nullptr;
  }

  validate_image_removal();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::validate_image_removal() {
  ldout(m_cct, 20) << dendl;

  check_image_snaps();
}

template<typename I>
void RemoveRequest<I>::check_image_snaps() {
  ldout(m_cct, 20) << dendl;

  if (m_image_ctx->snaps.size()) {
    lderr(m_cct) << "image has snapshots - not removing" << dendl;
    send_close_image(-ENOTEMPTY);
    return;
  }

  check_image_watchers();
}

template<typename I>
void RemoveRequest<I>::check_image_watchers() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  op.list_watchers(&m_watchers, &m_retval);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_check_image_watchers>(this);

  int r = m_image_ctx->md_ctx.aio_operate(m_header_oid,
                                          rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_check_image_watchers(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error listing image watchers: " << cpp_strerror(*result) << dendl;
    send_close_image(*result);
    return nullptr;
  }
  if (m_watchers.size() > 1) {
    lderr(m_cct) << "image has watchers - not removing" << dendl;
    send_close_image(-EBUSY);
    return nullptr;
  }

  check_image_consistency_group();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::check_image_consistency_group() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::image_get_group_start(&op);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_check_image_consistency_group>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(m_header_oid,
                                          rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_check_image_consistency_group(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error fetching consistency group for image" << cpp_strerror(*result)
                 << dendl;
    send_close_image(*result);
    return nullptr;
  }

  cls::rbd::GroupSpec s;
  *result = librbd::cls_client::image_get_group_finish(m_out_bl, s);
  if (*result < 0) {
    send_close_image(*result);
    return nullptr;
  }
  if (s.is_valid()) {
    lderr(m_cct) << "image is in a consistency group - not removing" << dendl;
    send_close_image(-EMLINK);
    return nullptr;
  }

  trim_image();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::trim_image() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_trim_image>(this);

  RWLock::RLocker owner_lock(m_image_ctx->owner_lock);
  librbd::operation::TrimRequest<I> *req = librbd::operation::TrimRequest<I>::create(
    *m_image_ctx, ctx, m_image_ctx->size, 0, m_prog_ctx);
  req->send();
}

template<typename I>
Context *RemoveRequest<I>::handle_trim_image(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "warning: failed to remove some object(s): " << cpp_strerror(*result)
                 << dendl;
  }

  remove_child();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::remove_child() {
  ldout(m_cct, 20) << dendl;

  m_image_ctx->parent_lock.get_read();
  parent_info parent_info = m_image_ctx->parent_md;
  m_image_ctx->parent_lock.put_read();

  librados::ObjectWriteOperation op;
  librbd::cls_client::remove_child(&op, parent_info.spec, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_remove_child>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_CHILDREN, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_remove_child(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "error removing child from children list" << cpp_strerror(*result)
                 << dendl;
    send_close_image(*result);
    return nullptr;
  }

  if (!m_old_format) {
    fetch_mirror_image();
  } else {
    send_close_image(0);
  }

  return nullptr;
}

template<typename I>
void RemoveRequest<I>::fetch_mirror_image() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::mirror_image_get_start(&op, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_fetch_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING,
                                          rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_fetch_mirror_image(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && !((*result == -EOPNOTSUPP) || (*result == -ENOENT))) {
    lderr(m_cct) << "cannot disable mirroring: " << cpp_strerror(*result) << dendl;
    send_close_image(*result);
    return nullptr;
  }

  if (*result < 0) {
    send_close_image(0); // continue removal
    return nullptr;
  }

  bufferlist::iterator it = m_out_bl.begin();
  *result = librbd::cls_client::mirror_image_get_finish(&it, &m_mirror_image_internal);
  if (*result < 0) {
    lderr(m_cct) << "cannot disable mirroring: " << cpp_strerror(*result) << dendl;
    send_close_image(*result);
    return nullptr;
  }

  open_journal();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::open_journal(){
  ldout(m_cct, 20) << dendl;

  m_journal = m_image_ctx->create_journal();

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_journal>(this);

  m_journal->open(ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_open_journal(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to open journal: " << cpp_strerror(*result) << dendl;
    close_journal(*result);
    return nullptr;
  }

  check_tag_owner();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::check_tag_owner() {
  ldout(m_cct, 20) << dendl;

  int r = 0;
  if (!m_journal->is_tag_owner()&& !m_force) {
    lderr(m_cct) << "Mirrored image is not primary, add force option to"
                 << " disable mirroring" << dendl;
    r = -EINVAL;
  }

  close_journal(r);
}

template<typename I>
void RemoveRequest<I>::close_journal(int r) {
  ldout(m_cct, 20) << dendl;

  m_retval = r;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_journal>(this);

  m_journal->close(ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_close_journal(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to close journal: " << cpp_strerror(*result) << dendl;
  }

  assert(m_journal != nullptr);
  delete m_journal;
  m_journal = nullptr;
  
  if (m_retval < 0) {
    send_close_image(m_retval);
    return nullptr;
  }

  mirror_image_disable();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::mirror_image_disable() {
  ldout(m_cct, 20) << dendl;

  m_mirror_image_internal.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_image_id, m_mirror_image_internal);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_mirror_image_disable>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_mirror_image_disable(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "cannot disable mirroring: " << cpp_strerror(*result) << dendl;
    send_close_image(*result);
    return nullptr;
  }

  send_watcher_notification();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::send_watcher_notification(){
  ldout(m_cct, 20) << dendl;

  Context *ctx = new FunctionContext([this](int r) {
      r = MirroringWatcher<>::notify_image_updated(
        m_ioctx, cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
        m_image_id, m_mirror_image_internal.global_image_id);
      handle_watcher_notify(r);
    });

  m_op_work_queue->queue(ctx, 0);
}

template<typename I>
void RemoveRequest<I>::handle_watcher_notify(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    // non fatal error -- watchers would cope up upon noticing missing
    // updates. just log and move on...
    ldout(m_cct, 10) << "failed to send update notification: " << cpp_strerror(r)
                     << dendl;
  } else {
    ldout(m_cct, 20) << "image mirroring is disabled: global_id=" <<
      m_mirror_image_internal.global_image_id << dendl;
  }

  get_clients();
}

template<typename I>
void RemoveRequest<I>::get_clients() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_get_clients>(this);

  assert(m_clients.empty());
  cls::journal::client::client_list(m_image_ctx->md_ctx,
                                    ::journal::Journaler::header_oid(m_image_id),
                                    &m_clients, ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_get_clients(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to fetch client list: " << cpp_strerror(*result) << dendl;
    send_close_image(*result);
    return nullptr;
  }

  assert(m_clients.size() >= 1);
  if (m_clients.size() == 1) {
    send_close_image(0);
    return nullptr;
  }

  remove_client_snaps();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::remove_client_snaps() {
  ldout(m_cct, 20) << dendl;

  assert(m_snaps.empty() && (m_snap_count == 0));

  std::set<cls::journal::Client>::iterator client = m_clients.begin();
  while (client != m_clients.end()) {
    m_client = *client;

    journal::ClientData client_data;
    bufferlist::iterator it = m_client.data.begin();
    ::decode(client_data, it);
    journal::ClientMetaType type = client_data.get_client_meta_type();

    if (type != journal::ClientMetaType::MIRROR_PEER_CLIENT_META_TYPE) {
      client = m_clients.erase(client);
      continue;
    }

    journal::MirrorPeerClientMeta client_meta =
      boost::get<journal::MirrorPeerClientMeta>(client_data.client_meta);
    for (const auto &sync : client_meta.sync_points) {
      m_snaps.push_back(sync.snap_name);
    }

    break;
  }

  if (client == m_clients.end()) {
    get_clients();
    return;
  }

  m_clients.erase(client);
  if (m_snaps.empty()) {
    unregister_client();
  } else {
    remove_snaps();
  }
}

template<typename I>
void RemoveRequest<I>::remove_snaps() {
  ldout(m_cct, 20) << dendl;

  m_retval = 0; // use @m_retval as error indicator
  m_snap_count = m_snaps.size();

  using klass = RemoveRequest<I>;
  for (const auto& snap : m_snaps) {
    Context *ctx = create_context_callback<klass, &klass::handle_remove_snaps>(this);
    m_image_ctx->operations->snap_remove(snap.c_str(), ctx);
  }
}

template<typename I>
Context *RemoveRequest<I>::handle_remove_snaps(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to remove temporary snapshot created by remote peer"
                 << cpp_strerror(*result) << dendl;
    m_retval = *result;
  }

  bool unregister = false;
  {
    Mutex::Locker l(m_snap_lock);
    m_snap_count--;
    if (!m_snap_count) {
      m_snaps.clear();
      unregister = true;
    }
  }

  if (!unregister) {
    return nullptr;
  }

  if (m_retval < 0) {
    send_close_image(m_retval);
  } else {
    unregister_client();
  }

  return nullptr;
}

template<typename I>
void RemoveRequest<I>::unregister_client() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectWriteOperation op;
  cls::journal::client::client_unregister(&op, m_client.id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_unregister_client>(this);
  int r = m_image_ctx->md_ctx.aio_operate(
    ::journal::Journaler::header_oid(m_image_id), rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_unregister_client(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to unregister remote journal client: "
                 << cpp_strerror(*result) << dendl;
    send_close_image(*result);
    return nullptr;
  }

  remove_client_snaps(); // remove snap for next client
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::send_close_image(int r) {
  ldout(m_cct, 20) << dendl;

  m_retval = r;
  assert(!m_image_ctx->owner_lock.is_locked());

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_send_close_image>(this);

  m_image_ctx->state->close(ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_send_close_image(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error encountered while closing image: " << cpp_strerror(*result)
                 << dendl;
  }

  switch_thread_context();
  return nullptr;
}

template <typename I>
void RemoveRequest<I>::switch_thread_context() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;

  Context *ctx = create_context_callback<klass, &klass::handle_switch_thread_context>(this);
  m_op_work_queue->queue(ctx, 0);
}

template <typename I>
void RemoveRequest<I>::handle_switch_thread_context(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  delete m_image_ctx;
  m_image_ctx = nullptr;

  if (m_retval < 0) {
    delete this;
    m_on_finish->complete(m_retval);
    return;
  }

  remove_header();
  return;
}

template<typename I>
void RemoveRequest<I>::remove_header() {
  ldout(m_cct, 20) << dendl;

  if (m_header_oid.empty()) {
    remove_image();
    return;
  }

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_remove_header>(this);
  int r = m_ioctx.aio_remove(m_header_oid, rados_completion);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_remove_header(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "error removing header: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  remove_image();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::remove_image() {
  ldout(m_cct, 20) << dendl;

  if (m_old_format || m_unknown_format) {
    remove_v1_image();
  } else {
    remove_v2_image();
  }
}

template<typename I>
void RemoveRequest<I>::remove_v1_image() {
  ldout(m_cct, 20) << dendl;

  Context *ctx = new FunctionContext([this] (int r) {
      r = tmap_rm(m_ioctx, m_image_name);
      handle_remove_v1_image(r);
    });

  m_op_work_queue->queue(ctx, 0);
}

template<typename I>
void RemoveRequest<I>::handle_remove_v1_image(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  m_old_format = (r == 0);
  if ((r < 0) && !m_unknown_format) {
    if (r != -ENOENT) {
      lderr(m_cct) << "error removing image from v1 directory: " << cpp_strerror(r) << dendl;
    }

    delete this;
    m_on_finish->complete(r);
    return;
  }

  if (!m_old_format) {
    remove_v2_image();
    return;
  }

  delete this;
  m_on_finish->complete(0);
}

template<typename I>
void RemoveRequest<I>::remove_v2_image() {
  ldout(m_cct, 20) << dendl;

  if (m_image_id.empty()) {
    dir_get_image_id();
    return;
  } else if (m_image_name.empty()) {
    dir_get_image_name();
    return;
  }

  remove_journal();
}

template<typename I>
void RemoveRequest<I>::dir_get_image_id() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_id_start(&op, m_image_name);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_dir_get_image_id>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_dir_get_image_id(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "error fetching image id: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  if (!*result) {
    bufferlist::iterator iter = m_out_bl.begin();
    *result = librbd::cls_client::dir_get_id_finish(&iter, &m_image_id);
  }
  if (*result < 0) {
    return m_on_finish;
  }

  remove_journal();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::dir_get_image_name() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_dir_get_image_name>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_dir_get_image_name(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "error fetching image name: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  if (!*result) {
    bufferlist::iterator iter = m_out_bl.begin();
    *result = librbd::cls_client::dir_get_name_finish(&iter, &m_image_name);
  }
  if (*result < 0) {
    return m_on_finish;
  }

  remove_journal();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::remove_journal() {
  ldout(m_cct, 20) << dendl;

  if (m_image_id.empty()) {
    remove_id_object();
    return;
  }

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_journal>(this);

  librbd::journal::RemoveRequest<I> *req = librbd::journal::RemoveRequest<I>::create(
    m_ioctx, m_image_id, librbd::Journal<I>::IMAGE_CLIENT_ID, m_op_work_queue, ctx);
  req->send();
}

template<typename I>
Context *RemoveRequest<I>::handle_remove_journal(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if (*result < 0) { // journal::RemoveRequest return 0 on -ENOENT
    lderr(m_cct) << "error removing image journal: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  remove_object_map();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::remove_object_map() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_remove_object_map>(this);
  int r = m_ioctx.aio_remove(
    ObjectMap::object_map_name(m_image_id, CEPH_NOSNAP), rados_completion);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_remove_object_map(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "error removing object map: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  mirror_image_remove();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::mirror_image_remove() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_remove(&op, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_mirror_image_remove>(this);
  int r = m_ioctx.aio_operate(RBD_MIRRORING, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_mirror_image_remove(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to remove image from mirroring directory: "
                 << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  ldout(m_cct, 20) << "removed image state from mirroring directory" << dendl;

  remove_id_object();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::remove_id_object() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_remove_id_object>(this);
  int r = m_ioctx.aio_remove(util::id_obj_name(m_image_name), rados_completion);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_remove_id_object(int *result) {
  ldout(m_cct, 20) << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "error removing id object: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  dir_remove_image();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::dir_remove_image() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::dir_remove_image(&op, m_image_name, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_dir_remove_image>(this);
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template<typename I>
Context *RemoveRequest<I>::handle_dir_remove_image(int *result) {
  ldout(m_cct, 20) << ":r =" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "error removing image from v2 directory: "
                 << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  *result = 0;
  return m_on_finish;
}

} // namespace image
} // namespace librbd

template class librbd::image::RemoveRequest<librbd::ImageCtx>;
