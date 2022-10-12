// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>

namespace starrocks::pipeline {
// Manage the memory usage for local exchange
// TODO(KKS): Should use the real chunk memory usage, not chunk row number
// Use row number because it's hard to control very big bitmap column memory usage
class LocalExchangeMemoryManager {
public:
    LocalExchangeMemoryManager(int32_t max_row_count) : _max_row_count(max_row_count) {}
    void update_row_count(int32_t row_count) { _row_count += row_count; }
    bool is_full() const { return _row_count >= _max_row_count; }

private:
    int32_t _max_row_count;
    std::atomic<int32_t> _row_count{0};
};
} // namespace starrocks::pipeline