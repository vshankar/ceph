// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/Create.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/assert.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Journal::create: "

namespace librbd {

using util::create_context_callback;

namespace journal {

template<typename I>
CreateJournal<I>::CreateJournal(IoCtx &ioctx, const std::string &imageid, uint8_t order,
                                uint8_t splay_width, const std::string &object_pool,
                                uint64_t tag_class, TagData &tag_data, Journaler *journaler,
                                Context *on_finish)
  : m_imageid(imageid), m_order(order), m_splay_width(splay_width),
    m_object_pool(object_pool), m_tag_class(tag_class), m_tag_data(tag_data),
    m_journaler(journaler), m_on_finish(on_finish), m_pool_id(-1) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

template<typename I>
void CreateJournal<I>::send() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (m_order > 64 || m_order < 12) {
    lderr(m_cct) << "order must be in the range [12, 64]" << dendl;
    complete(-EDOM);
    return;
  }
  if (m_splay_width == 0) {
    complete(-EINVAL);
    return;
  }

  get_pool_id();
}

template<typename I>
void CreateJournal<I>::get_pool_id() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (m_object_pool.empty()) {
    create_journal();
    return;
  }

  librados::Rados rados(m_ioctx);
  IoCtx data_ioctx;
  int r = rados.ioctx_create(m_object_pool.c_str(), data_ioctx);
  if (r != 0) {
    lderr(m_cct) << "failed to create journal: "
                 << "error opening journal object pool '" << m_object_pool
                 << "': " << cpp_strerror(r) << dendl;
    complete(r);
    return;
  }

  m_pool_id = data_ioctx.get_id();
  create_journal();
}

template<typename I>
void CreateJournal<I>::create_journal() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateJournal<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_create_journal>(this);

  m_journaler->create(m_order, m_splay_width, m_pool_id, ctx);
}

template<typename I>
Context *CreateJournal<I>::handle_create_journal(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to create journal: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  allocate_journal_tag();
  return nullptr;
}

template<typename I>
void CreateJournal<I>::allocate_journal_tag() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateJournal<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journal_tag>(this);

  ::encode(m_tag_data, m_bl);
  m_journaler->allocate_tag(m_tag_class, m_bl, &m_tag, ctx);
}

template<typename I>
Context *CreateJournal<I>::handle_journal_tag(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to allocate tag: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  register_client();
  return nullptr;
}

template<typename I>
void CreateJournal<I>::register_client() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  m_bl.clear();
  ::encode(ClientData{ImageClientMeta{m_tag.tag_class}}, m_bl);

  using klass = CreateJournal<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_register_client>(this);

  m_journaler->register_client(m_bl, ctx);
}

template<typename I>
Context *CreateJournal<I>::handle_register_client(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to register client: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  complete(0);
  return nullptr;
}

template<typename I>
void CreateJournal<I>::complete(int r) {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (r == 0) {
    ldout(m_cct, 20) << "done." << dendl;
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::CreateJournal<librbd::ImageCtx>;
