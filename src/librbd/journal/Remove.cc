// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Remove.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/assert.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Journal::remove: "

namespace librbd {

using util::create_context_callback;

namespace journal {

template<typename I>
RemoveJournal<I>::RemoveJournal(IoCtx &ioctx, const std::string &image_id,
                                Journaler *journaler, Context *on_finish)
  : m_imageid(image_id), m_journaler(journaler), m_on_finish(on_finish) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

template<typename I>
void RemoveJournal<I>::send() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  stat_journal();
}

template<typename I>
void RemoveJournal<I>::stat_journal() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = RemoveJournal<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_stat_journal>(this);

  m_journaler->exists(ctx);
}

template<typename I>
Context *RemoveJournal<I>::handle_stat_journal(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to stat journal header: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  if (*result == -ENOENT) {
    *result = 0;
    return m_on_finish;
  }

  remove_journal();
  return nullptr;
}

template<typename I>
void RemoveJournal<I>::remove_journal() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = RemoveJournal<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_journal>(this);

  m_journaler->remove(true, ctx);
}

template<typename I>
Context *RemoveJournal<I>::handle_remove_journal(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to remove journal: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  complete(0);
  return nullptr;
}

template<typename I>
void RemoveJournal<I>::complete(int r) {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (r == 0) {
    ldout(m_cct, 20) << "done." << dendl;
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::RemoveJournal<librbd::ImageCtx>;
