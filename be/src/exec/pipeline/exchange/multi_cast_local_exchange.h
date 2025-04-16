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

#include <deque>
#include <utility>

#include "column/chunk.h"
#include "exec/pipeline/schedule/observer.h"
#include "exec/pipeline/source_operator.h"
#include "exec/spill/dir_manager.h"
#include "fs/fs.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

// For uni cast stream sink, we just add a exchange sink operator
// at the end of the pipeline. It works like
// [source pipeline]  -> exchange_sink_operator

// But for multi cast stream sink, extra pipelines are added with exchanger
// It works like
// [source pipeline] -> mcast_local_sink   -> mcast_local_exchanger  -> [new pipeline1]
//                                                     |             -> [new pipeline2]
//                                                     |             -> [new pipeline3]
// and for each new pipeline, the internal structure is
// [new pipeline]: mcast_local_source -> exchange_sink_operator

// The dataflow works like:
// 1. mcast_local_sink push chunks to exchanger
// 2. mcast_local_source pull chunks from exchanger

// The exchanger should take care of several things:
// 1. can accept chunk or not. we don't want to block any consumer. we can accept chunk only when a any consumer needs chunk.
// 2. can throw chunk or not. we can only throw any chunk when all consumers have consumed that chunk.
// 3. can pull chunk. we maintain the progress of consumers.

class MultiCastLocalExchangeSinkOperator;
// ===== exchanger =====
class MultiCastLocalExchanger {
public:
    virtual ~MultiCastLocalExchanger() = default;
    virtual bool support_event_scheduler() const = 0;

    virtual Status init_metrics(RuntimeProfile* profile, bool is_first_sink_driver) = 0;
    virtual Status prepare(RuntimeState* state) { return Status::OK(); }
    virtual void close(RuntimeState* state) {}

    virtual bool can_pull_chunk(int32_t mcast_consumer_index) const = 0;
    virtual bool can_push_chunk() const = 0;
    virtual Status push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence) = 0;
    virtual StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t mcast_consuemr_index) = 0;
    virtual void open_source_operator(int32_t mcast_consumer_index) = 0;
    virtual void close_source_operator(int32_t mcast_consumer_index) = 0;
    virtual void open_sink_operator() = 0;
    virtual void close_sink_operator() = 0;

    virtual bool releaseable() const { return false; }
    virtual void enter_release_memory_mode() {}

    PipeObservable& observable() { return _observable; }

private:
    PipeObservable _observable;
};

class InMemoryMultiCastLocalExchanger final : public MultiCastLocalExchanger {
public:
    InMemoryMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number);
    ~InMemoryMultiCastLocalExchanger() override;
    bool support_event_scheduler() const override { return true; }

    Status init_metrics(RuntimeProfile* profile, bool is_first_sink_driver) override;
    bool can_pull_chunk(int32_t mcast_consumer_index) const override;
    bool can_push_chunk() const override;
    Status push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t mcast_consuemr_index) override;
    void open_source_operator(int32_t mcast_consumer_index) override;
    void close_source_operator(int32_t mcast_consumer_index) override;
    void open_sink_operator() override;
    void close_sink_operator() override;

#ifndef BE_TEST
private:
#endif

    struct Cell {
        ChunkPtr chunk = nullptr;
        Cell* next = nullptr;
        size_t memory_usage = 0;
        size_t accumulated_row_size = 0;
        // how many consumers have used this chunk
        int32_t used_count = 0;
    };
    void _update_progress(Cell* fast = nullptr);
    void _closer_consumer(int32_t mcast_consumer_index);
    RuntimeState* _runtime_state;
    mutable std::mutex _mutex;
    size_t _consumer_number;
    size_t _current_accumulated_row_size = 0;
    std::vector<Cell*> _progress;
    std::vector<int32_t> _opened_source_opcount;
    // the fast consumer has consumed how many chunks.
    size_t _fast_accumulated_row_size = 0;
    int32_t _opened_source_number = 0;
    int32_t _opened_sink_number = 0;
    Cell* _head = nullptr;
    Cell* _tail = nullptr;
    size_t _current_memory_usage = 0;
    size_t _current_row_size = 0;
    RuntimeProfile::HighWaterMarkCounter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_buffer_row_size_counter = nullptr;
};

class MemLimitedChunkQueue;

class SpillableMultiCastLocalExchanger : public MultiCastLocalExchanger {
public:
    SpillableMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number, int32_t plan_node_id);
    ~SpillableMultiCastLocalExchanger() override = default;
    bool support_event_scheduler() const override { return false; }

    Status init_metrics(RuntimeProfile* profile, bool is_first_sink_driver) override;
    bool can_pull_chunk(int32_t mcast_consumer_index) const override;
    bool can_push_chunk() const override;
    Status push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state, int32_t mcast_consumer_index) override;
    void open_source_operator(int32_t mcast_consumer_index) override;
    void close_source_operator(int32_t mcast_consumer_index) override;
    void open_sink_operator() override;
    void close_sink_operator() override;
    bool releaseable() const override { return true; }
    void enter_release_memory_mode() override;

private:
    std::shared_ptr<MemLimitedChunkQueue> _queue;
};

} // namespace starrocks::pipeline
