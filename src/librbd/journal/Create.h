// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_CREATE_H
#define CEPH_LIBRBD_JOURNAL_CREATE_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "journal/Journaler.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "cls/journal/cls_journal_types.h"

using librados::IoCtx;
using journal::Journaler;

class Context;

namespace journal {
  class Journaler;
}

namespace librbd {

class ImageCtx;

namespace journal {

template<typename ImageCtxT = ImageCtx>
class CreateJournal {
public:
  static CreateJournal *create(IoCtx &ioctx, const std::string &imageid, uint8_t order,
                               uint8_t splay_width, const std::string &object_pool,
                               uint64_t tag_class, TagData &tag_data, Journaler *journaler,
                               Context *on_finish) {
    return new CreateJournal(ioctx, imageid, order, splay_width, object_pool,
                             tag_class, tag_data, journaler, on_finish);
  }

  void send();

private:
  typedef typename TypeTraits<ImageCtxT>::Journaler Journaler;

  CreateJournal(IoCtx &ioctx, const std::string &imageid, uint8_t order,
                uint8_t splay_width, const std::string &object_pool,
                uint64_t tag_class, TagData &tag_data, Journaler *journaler,
                Context *on_finish);

  IoCtx m_ioctx;
  std::string m_imageid;
  uint8_t m_order;
  uint8_t m_splay_width;
  std::string m_object_pool;
  uint64_t m_tag_class;
  TagData m_tag_data;
  Journaler *m_journaler;
  Context *m_on_finish;

  CephContext *m_cct;
  int64_t m_pool_id;
  cls::journal::Tag m_tag;
  bufferlist m_bl;

  void get_pool_id();

  void create_journal();
  Context *handle_create_journal(int *result);

  void allocate_journal_tag();
  Context *handle_journal_tag(int *result);

  void register_client();
  Context *handle_register_client(int *result);

  void shut_down_journaler();
  Context *handle_journaler_shut_down(int *result);

  void complete(int r);
};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::CreateJournal<librbd::ImageCtx>;

#endif /* CEPH_LIBRBD_JOURNAL_CREATE_H */
