// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "gutil/ref_counted.h"

#include <common/logging.h>

#include "gutil/atomic_refcount.h"

namespace starrocks::subtle {

RefCountedBase::RefCountedBase()
#ifndef NDEBUG
        : in_dtor_(false) {
}
#else
        = default;
#endif

RefCountedBase::~RefCountedBase() { // NOLINT
#ifndef NDEBUG
    DCHECK(in_dtor_) << "RefCounted object deleted without calling Release()";
#endif
}

void RefCountedBase::AddRef() const {
    // TODO(maruel): Add back once it doesn't assert 500 times/sec.
    // Current thread books the critical section "AddRelease" without release it.
    // DFAKE_SCOPED_LOCK_THREAD_LOCKED(add_release_);
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    ++ref_count_;
}

bool RefCountedBase::Release() const {
    // TODO(maruel): Add back once it doesn't assert 500 times/sec.
    // Current thread books the critical section "AddRelease" without release it.
    // DFAKE_SCOPED_LOCK_THREAD_LOCKED(add_release_);
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    if (--ref_count_ == 0) {
#ifndef NDEBUG
        in_dtor_ = true;
#endif
        return true;
    }
    return false;
}

bool RefCountedThreadSafeBase::HasOneRef() const {
    return base::RefCountIsOne(&const_cast<RefCountedThreadSafeBase*>(this)->ref_count_);
}

RefCountedThreadSafeBase::RefCountedThreadSafeBase() { // NOLINT
#ifndef NDEBUG
    in_dtor_ = false;
#endif
}

RefCountedThreadSafeBase::~RefCountedThreadSafeBase() { // NOLINT
#ifndef NDEBUG
    DCHECK(in_dtor_) << "RefCountedThreadSafe object deleted without "
                        "calling Release()";
#endif
}

void RefCountedThreadSafeBase::AddRef() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
#endif
    base::RefCountInc(&ref_count_);
}

bool RefCountedThreadSafeBase::Release() const {
#ifndef NDEBUG
    DCHECK(!in_dtor_);
    DCHECK(!base::RefCountIsZero(&ref_count_));
#endif
    if (!base::RefCountDec(&ref_count_)) {
#ifndef NDEBUG
        in_dtor_ = true;
#endif
        return true;
    }
    return false;
}

} // namespace starrocks::subtle
