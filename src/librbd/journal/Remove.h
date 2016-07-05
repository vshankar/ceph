// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_REMOVE_H
#define CEPH_LIBRBD_JOURNAL_REMOVE_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "journal/Journaler.h"
#include "librbd/journal/TypeTraits.h"

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
class RemoveJournal {
public:
  static RemoveJournal *create(IoCtx &ioctx, const std::string &image_id,
                               Journaler *journaler, Context *on_finish) {
    return new RemoveJournal(ioctx, image_id, client_id, journaler, on_finish);
  }

  void send();

private:
  typedef typename TypeTraits<ImageCtxT>::Journaler Journaler;

  RemoveJournal(IoCtx &ioctx, const std::string &image_id,
                Journaler *journaler, Context *on_finish);

  IoCtx m_ioctx;
  std::string m_imageid;
  Journaler *m_journaler;
  Context *m_on_finish;

  CephContext *m_cct;

  void stat_journal();
  Context *handle_stat_journal(int *result);

  void remove_journal();
  Context *handle_remove_journal(int *result);

  void complete(int r);
};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::RemoveJournal<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_REMOVE_H
