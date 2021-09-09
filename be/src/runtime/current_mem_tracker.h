// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "common/logging.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"

namespace starrocks {
class CurrentMemTracker {
public:
    inline static void consume(int64_t size) {
        MemTracker* tracker = CurrentThread::mem_tracker();
        if (tracker != nullptr && size != 0) {
            tracker->consume(size);
        }
    }

    inline static void release(int64_t size) {
        MemTracker* tracker = CurrentThread::mem_tracker();
        if (tracker != nullptr && size != 0) {
            tracker->release(size);
        }
    }
};
} // namespace starrocks
