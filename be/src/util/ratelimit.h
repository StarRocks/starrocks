// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>

#include "util/time.h"

#define RATE_LIMIT(func, interval_ms)           \
    {                                           \
        static std::atomic<int64_t> last;       \
        static int64_t RATE_LIMIT_SKIP_CNT = 0; \
        int64_t now = starrocks::UnixMillis();  \
        if (now > last.load() + interval_ms) {  \
            func;                               \
            RATE_LIMIT_SKIP_CNT = 0;            \
            last.store(now);                    \
        } else {                                \
            RATE_LIMIT_SKIP_CNT++;              \
        }                                       \
    }

#define RATE_LIMIT_BY_TAG(tag, func, interval_ms)        \
    {                                                    \
        static std::atomic<int64_t> last[64];            \
        int64_t now = starrocks::UnixMillis();           \
        if (now > last[tag & 63].load() + interval_ms) { \
            func;                                        \
            last[tag & 63].store(now);                   \
        }                                                \
    }
