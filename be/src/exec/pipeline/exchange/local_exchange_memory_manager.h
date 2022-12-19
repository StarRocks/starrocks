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
