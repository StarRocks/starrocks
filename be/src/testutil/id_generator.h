// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>

namespace starrocks {

inline int64_t next_id() {
    static std::atomic<int64_t> id{1};
    return id.fetch_add(1, std::memory_order_relaxed);
}

} // namespace starrocks
