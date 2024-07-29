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

#include "async_flush_stream_poller.h"
#include "common/config.h"
#include "connector/connector_chunk_sink.h"
#include "formats/file_writer.h"
#include "runtime/mem_tracker.h"

namespace starrocks::connector {

/// manage memory of a single sink operator
/// not thread-safe except `releasable_memory()`
class SinkOperatorMemoryManager {
public:
    SinkOperatorMemoryManager() = default;

    void init(std::unordered_map<std::string, WriterStreamPair>* writer_stream_pairs, AsyncFlushStreamPoller* io_poller,
              CommitFunc commit_func);

    // return true if a victim is found and killed, otherwise return false
    bool kill_victim();

    int64_t update_releasable_memory();

    int64_t update_writer_occupied_memory();

    // thread-safe
    int64_t releasable_memory() { return _releasable_memory.load(); }

    // thread-safe
    int64_t writer_occupied_memory() { return _writer_occupied_memory.load(); }

private:
    std::unordered_map<std::string, WriterStreamPair>* _candidates = nullptr; // reference, owned by sink operator
    CommitFunc _commit_func;
    AsyncFlushStreamPoller* _io_poller;
    std::atomic_int64_t _releasable_memory{0};
    std::atomic_int64_t _writer_occupied_memory{0};
};

/// 1. manage all sink operators in a query
/// 2. calculates releasable memory across all
/// 3. kill (early-close) writers to enlarge releasable memory, which are flushed to remote storage and freed asynchronously
class SinkMemoryManager {
public:
    SinkMemoryManager(MemTracker* query_pool_tracker, MemTracker* query_tracker);

    SinkOperatorMemoryManager* create_child_manager();

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

} // namespace starrocks::connector
