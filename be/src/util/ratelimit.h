// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
