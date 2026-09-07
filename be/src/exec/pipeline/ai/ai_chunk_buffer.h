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

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <type_traits>
#include <vector>

#include "base/statusor.h"
#include "column/chunk.h"
#include "exec_primitive/pipeline/primitives/pipeline_observer.h"

namespace starrocks::pipeline {

// A fragment-shared, non-blocking buffer between AI sink and source operators.
//
// Each driver owns one lane. Chunks stay in that lane and retain FIFO order,
// while chunk and memory limits apply to the whole fragment.
class AIChunkBuffer {
public:
    static StatusOr<std::shared_ptr<AIChunkBuffer>> create(int64_t capacity, int64_t max_retained_bytes);

    static StatusOr<size_t> capacity_for_dop(int64_t dop);
    static StatusOr<size_t> memory_limit_for_query(int64_t query_memory_limit);

    ~AIChunkBuffer() noexcept;

    // Adaptive pipelines may not know the final DOP when the buffer is created.
    // Repeating the final DOP is harmless; changing it after configuration is not.
    Status configure(int32_t dop);

    // A successfully enqueued chunk is shared with the buffer and must not be
    // mutated by the caller until it is dequeued.
    StatusOr<bool> try_put(int32_t driver_sequence, const ChunkPtr& chunk);
    StatusOr<bool> try_get(int32_t driver_sequence, ChunkPtr* output_chunk);
    StatusOr<bool> lane_has_chunk(int32_t driver_sequence) const;
    StatusOr<bool> lane_source_finished(int32_t driver_sequence) const;

    Status set_sink_eos(int32_t driver_sequence);
    StatusOr<bool> lane_finished(int32_t driver_sequence) const;

    // Drops only this lane. Once every source lane is finished, all subsequent
    // puts are rejected without retaining their chunks.
    Status set_source_finished(int32_t driver_sequence);
    bool all_sources_finished() const;

    // Attach calls are prepare-only, matching PipeObservable's contract.
    Status attach_source_observer(int32_t driver_sequence, RuntimeState* state, PipelineObserver* observer);
    Status attach_sink_observer(int32_t driver_sequence, RuntimeState* state, PipelineObserver* observer);

    size_t size() const;
    size_t retained_bytes() const;

    void close();

private:
    struct Entry {
        ChunkPtr chunk;
        size_t retained_bytes = 0;
    };

    using EntryQueue = std::deque<Entry>;
    static_assert(std::is_nothrow_swappable_v<EntryQueue>);

    struct Lane {
        EntryQueue chunks;
        bool sink_eos = false;
        bool source_finished = false;
        bool sink_waiting = false;
        bool wake_pending = false;
        size_t waiting_bytes = 0;
        Observable source_observable;
        Observable sink_observable;
    };

    using Lanes = std::vector<std::unique_ptr<Lane>>;
    using RetirementQueues = std::vector<EntryQueue>;
    using NotificationList = std::vector<Observable*>;
    static_assert(std::is_nothrow_swappable_v<Lanes>);
    static_assert(std::is_nothrow_swappable_v<RetirementQueues>);
    static_assert(std::is_nothrow_copy_constructible_v<NotificationList::value_type>);

    AIChunkBuffer(size_t capacity, size_t max_retained_bytes);

    Status _validate_lane_locked(int32_t driver_sequence) const;
    bool _can_admit_locked(size_t chunk_bytes) const;
    bool _has_unreserved_waiter_locked() const noexcept;
    StatusOr<bool> _clear_waiter_locked(Lane* lane);
    void _reserve_waiters_locked(NotificationList* observables) noexcept;
    void _reset_waiters_locked();
    void _drop_lane_locked(Lane* lane, EntryQueue* dropped_entries) noexcept;
    void _clear_all_locked() noexcept;
    void _clear_retirement_queues() noexcept;

    static void _notify_source(Observable* observable);
    static void _notify_sinks(const NotificationList& observables);
    void _notify_all_sinks();
    void _notify_all_sources();

    const size_t _capacity;
    const size_t _max_retained_bytes;

    mutable std::mutex _mutex;
    Lanes _lanes;
    RetirementQueues _retirement_queues;
    size_t _size = 0;
    size_t _retained_bytes = 0;
    size_t _finished_sources = 0;
    size_t _reserved_wake_count = 0;
    size_t _reserved_wake_bytes = 0;
    size_t _next_waiter = 0;
    bool _closed = false;
};

} // namespace starrocks::pipeline
