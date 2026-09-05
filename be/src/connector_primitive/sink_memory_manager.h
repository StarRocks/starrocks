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

#include <cstdint>
#include <memory>
#include <vector>

namespace starrocks {

class MemTracker;

namespace connector {

/// Interface for operator-level sink memory managers.
class SinkOperatorMemoryManager {
public:
    virtual ~SinkOperatorMemoryManager() = default;

    // return true if a victim is found and killed, otherwise return false
    virtual bool kill_victim() = 0;

    virtual int64_t update_releasable_memory() = 0;

    virtual int64_t update_writer_occupied_memory() = 0;

    // thread-safe
    virtual int64_t releasable_memory() const = 0;

    // thread-safe
    virtual int64_t writer_occupied_memory() const = 0;
};

/// 1. manage all sink operators in a query
/// 2. calculates releasable memory across all
/// 3. kill (early-close) writers to enlarge releasable memory, which are flushed to remote storage and freed
/// asynchronously
class SinkMemoryManager {
public:
    SinkMemoryManager(MemTracker* query_pool_tracker, MemTracker* query_tracker);

    SinkOperatorMemoryManager* register_child_manager(std::unique_ptr<SinkOperatorMemoryManager> child_manager);

    // thread-safe
    // may lower frequency if overhead is significant
    bool can_accept_more_input(SinkOperatorMemoryManager* child_manager);

private:
    bool _apply_on_mem_tracker(SinkOperatorMemoryManager* child_manager, MemTracker* mem_tracker);

    int64_t _total_releasable_memory();
    int64_t _total_writer_occupied_memory();

    double _high_watermark_ratio = 0;
    double _low_watermark_ratio = 0;
    double _urgent_space_ratio = 0;
    MemTracker* _process_tracker = nullptr;
    MemTracker* _query_pool_tracker = nullptr;
    MemTracker* _query_tracker = nullptr;
    std::vector<std::unique_ptr<SinkOperatorMemoryManager>> _children;
};

} // namespace connector
} // namespace starrocks
