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

#include "exec/pipeline/ai/ai_chunk_buffer.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

#include "glog/logging.h"

namespace starrocks::pipeline {
namespace {

constexpr size_t kMiB = 1024UL * 1024;
constexpr size_t kUnknownQueryMemoryLimit = 32 * kMiB;
constexpr size_t kMinimumBufferMemoryLimit = 4 * kMiB;
constexpr size_t kMaximumBufferMemoryLimit = 256 * kMiB;
constexpr size_t kMinimumChunkCapacity = 12;
constexpr size_t kChunksPerDriver = 6;

} // namespace

StatusOr<std::shared_ptr<AIChunkBuffer>> AIChunkBuffer::create(int64_t capacity, int64_t max_retained_bytes) {
    if (capacity <= 0) {
        return Status::InvalidArgument("AI chunk buffer capacity must be positive");
    }
    if (max_retained_bytes < 0) {
        return Status::InvalidArgument("AI chunk buffer memory limit cannot be negative");
    }
    if (static_cast<uint64_t>(capacity) > std::numeric_limits<size_t>::max() ||
        static_cast<uint64_t>(max_retained_bytes) > std::numeric_limits<size_t>::max()) {
        return Status::InvalidArgument("AI chunk buffer limit exceeds the addressable range");
    }
    try {
        return std::shared_ptr<AIChunkBuffer>(
                new AIChunkBuffer(static_cast<size_t>(capacity), static_cast<size_t>(max_retained_bytes)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI chunk buffer");
    }
}

StatusOr<size_t> AIChunkBuffer::capacity_for_dop(int64_t dop) {
    if (dop <= 0) {
        return Status::InvalidArgument("AI chunk buffer DOP must be positive");
    }
    if (static_cast<uint64_t>(dop) > std::numeric_limits<size_t>::max() / kChunksPerDriver) {
        return Status::InvalidArgument("AI chunk buffer capacity overflows");
    }
    return std::max(kMinimumChunkCapacity, static_cast<size_t>(dop) * kChunksPerDriver);
}

StatusOr<size_t> AIChunkBuffer::memory_limit_for_query(int64_t query_memory_limit) {
    // Current main normalizes an unlimited/default query_mem_limit to -1.
    if (query_memory_limit <= 0) {
        return kUnknownQueryMemoryLimit;
    }
    const uint64_t proportional_limit = static_cast<uint64_t>(query_memory_limit) / 50;
    return std::clamp(static_cast<size_t>(std::min<uint64_t>(proportional_limit, kMaximumBufferMemoryLimit)),
                      kMinimumBufferMemoryLimit, kMaximumBufferMemoryLimit);
}

AIChunkBuffer::AIChunkBuffer(size_t capacity, size_t max_retained_bytes)
        : _capacity(capacity), _max_retained_bytes(max_retained_bytes) {}

AIChunkBuffer::~AIChunkBuffer() noexcept {
    {
        std::lock_guard lock(_mutex);
        _reset_waiters_locked();
        _clear_all_locked();
    }
    _clear_retirement_queues();
}

Status AIChunkBuffer::configure(int32_t dop) {
    if (dop <= 0) {
        return Status::InvalidArgument("AI chunk buffer DOP must be positive");
    }

    {
        std::lock_guard lock(_mutex);
        if (_closed) {
            return Status::InternalError("Cannot configure a closed AI chunk buffer");
        }
        if (!_lanes.empty()) {
            DCHECK_EQ(_lanes.size(), _retirement_queues.size());
            if (_lanes.size() == static_cast<size_t>(dop)) {
                return Status::OK();
            }
            return Status::InternalError("AI chunk buffer is already configured with a different DOP");
        }
        DCHECK(_retirement_queues.empty());
    }

    // Construct both sides outside the mutex. If any allocation fails, the
    // buffer remains unconfigured; the noexcept swaps install them atomically.
    Lanes lanes;
    RetirementQueues retirement_queues;
    try {
        lanes.reserve(dop);
        for (int32_t i = 0; i < dop; ++i) {
            lanes.emplace_back(std::make_unique<Lane>());
        }
        retirement_queues.resize(static_cast<size_t>(dop));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI chunk buffer lanes");
    }

    std::lock_guard lock(_mutex);
    if (_closed) {
        return Status::InternalError("Cannot configure a closed AI chunk buffer");
    }
    if (!_lanes.empty()) {
        DCHECK_EQ(_lanes.size(), _retirement_queues.size());
        if (_lanes.size() == static_cast<size_t>(dop)) {
            return Status::OK();
        }
        return Status::InternalError("AI chunk buffer is already configured with a different DOP");
    }
    DCHECK(_retirement_queues.empty());
    _lanes.swap(lanes);
    _retirement_queues.swap(retirement_queues);
    return Status::OK();
}

StatusOr<bool> AIChunkBuffer::try_put(int32_t driver_sequence, const ChunkPtr& chunk) {
    if (chunk == nullptr) {
        return Status::InvalidArgument("Cannot put a null chunk into the AI chunk buffer");
    }
    const size_t chunk_bytes = chunk->memory_usage();
    Observable* source_observable = nullptr;
    NotificationList sink_observables;
    bool admitted = false;
    {
        std::lock_guard lock(_mutex);
        RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
        Lane* lane = _lanes[driver_sequence].get();

        if (_closed || lane->source_finished || _finished_sources == _lanes.size()) {
            return false;
        }
        if (lane->sink_eos) {
            return Status::InternalError("Cannot put a chunk after sink EOS");
        }

        if (lane->wake_pending) {
            sink_observables.reserve(_lanes.size());
        }
        auto cleared_waiter = _clear_waiter_locked(lane);
        if (!cleared_waiter.ok()) {
            return cleared_waiter.status();
        }

        if (!_can_admit_locked(chunk_bytes)) {
            lane->sink_waiting = true;
            lane->waiting_bytes = chunk_bytes;
            if (cleared_waiter.value()) {
                _reserve_waiters_locked(&sink_observables);
            }
        } else {
            if (chunk_bytes > std::numeric_limits<size_t>::max() - _retained_bytes) {
                return Status::InternalError("AI chunk buffer retained-byte accounting overflows");
            }

            lane->chunks.emplace_back(Entry{chunk, chunk_bytes});
            ++_size;
            _retained_bytes += chunk_bytes;
            DCHECK_LE(_size, _capacity);
            DCHECK_LE(_reserved_wake_count, _capacity - _size);
            source_observable = &lane->source_observable;
            admitted = true;

            // A retry may use fewer bytes than its reservation. Fill any
            // capacity released by that atomic reservation-to-physical move.
            if (cleared_waiter.value()) {
                _reserve_waiters_locked(&sink_observables);
            }
        }
    }
    if (source_observable != nullptr) {
        _notify_source(source_observable);
    }
    _notify_sinks(sink_observables);
    return admitted;
}

StatusOr<bool> AIChunkBuffer::try_get(int32_t driver_sequence, ChunkPtr* output_chunk) {
    if (output_chunk == nullptr) {
        return Status::InvalidArgument("AI chunk buffer output cannot be null");
    }
    output_chunk->reset();

    NotificationList sink_observables;
    {
        std::lock_guard lock(_mutex);
        RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
        Lane* lane = _lanes[driver_sequence].get();
        if (_closed || lane->source_finished || lane->chunks.empty()) {
            return false;
        }
        if (_size == 0 || _retained_bytes < lane->chunks.front().retained_bytes) {
            return Status::InternalError("AI chunk buffer dequeue accounting underflows");
        }

        const bool has_waiter = _has_unreserved_waiter_locked();
        if (has_waiter) {
            sink_observables.reserve(_lanes.size());
        }
        Entry entry = std::move(lane->chunks.front());
        lane->chunks.pop_front();
        --_size;
        _retained_bytes -= entry.retained_bytes;
        *output_chunk = std::move(entry.chunk);
        if (has_waiter) {
            _reserve_waiters_locked(&sink_observables);
        }
    }
    _notify_sinks(sink_observables);
    return true;
}

StatusOr<bool> AIChunkBuffer::lane_has_chunk(int32_t driver_sequence) const {
    std::lock_guard lock(_mutex);
    RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
    return !_closed && !_lanes[driver_sequence]->chunks.empty();
}

StatusOr<bool> AIChunkBuffer::lane_source_finished(int32_t driver_sequence) const {
    std::lock_guard lock(_mutex);
    RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
    return _closed || _lanes[driver_sequence]->source_finished;
}

Status AIChunkBuffer::set_sink_eos(int32_t driver_sequence) {
    Observable* source_observable = nullptr;
    NotificationList sink_observables;
    {
        std::lock_guard lock(_mutex);
        RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
        Lane* lane = _lanes[driver_sequence].get();
        if (_closed || lane->sink_eos) {
            return Status::OK();
        }

        if (lane->wake_pending) {
            sink_observables.reserve(_lanes.size());
        }
        auto cleared_waiter = _clear_waiter_locked(lane);
        if (!cleared_waiter.ok()) {
            return cleared_waiter.status();
        }
        lane->sink_eos = true;
        if (!lane->source_finished) {
            source_observable = &lane->source_observable;
        }
        if (cleared_waiter.value()) {
            _reserve_waiters_locked(&sink_observables);
        }
    }
    if (source_observable != nullptr) {
        _notify_source(source_observable);
    }
    _notify_sinks(sink_observables);
    return Status::OK();
}

StatusOr<bool> AIChunkBuffer::lane_finished(int32_t driver_sequence) const {
    std::lock_guard lock(_mutex);
    RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
    const Lane* lane = _lanes[driver_sequence].get();
    return _closed || lane->source_finished || (lane->sink_eos && lane->chunks.empty());
}

Status AIChunkBuffer::set_source_finished(int32_t driver_sequence) {
    std::deque<Entry> dropped_entries;
    NotificationList sink_observables;
    {
        std::lock_guard lock(_mutex);
        RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
        Lane* lane = _lanes[driver_sequence].get();
        if (_closed || lane->source_finished) {
            return Status::OK();
        }

        sink_observables.reserve(_lanes.size());
        if (!lane->sink_eos) {
            sink_observables.emplace_back(&lane->sink_observable);
        }
        auto cleared_waiter = _clear_waiter_locked(lane);
        if (!cleared_waiter.ok()) {
            return cleared_waiter.status();
        }
        const bool had_chunks = !lane->chunks.empty();
        lane->source_finished = true;
        ++_finished_sources;
        _drop_lane_locked(lane, &dropped_entries);
        if (cleared_waiter.value() || had_chunks) {
            _reserve_waiters_locked(&sink_observables);
        }
    }
    dropped_entries.clear();
    _notify_sinks(sink_observables);
    return Status::OK();
}

bool AIChunkBuffer::all_sources_finished() const {
    std::lock_guard lock(_mutex);
    return _closed || (!_lanes.empty() && _finished_sources == _lanes.size());
}

Status AIChunkBuffer::attach_source_observer(int32_t driver_sequence, RuntimeState* state, PipelineObserver* observer) {
    if (state == nullptr || observer == nullptr) {
        return Status::InvalidArgument("AI chunk buffer source observer and runtime state cannot be null");
    }
    Observable* observable = nullptr;
    {
        std::lock_guard lock(_mutex);
        RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
        observable = &_lanes[driver_sequence]->source_observable;
    }
    observable->add_observer(state, observer);
    return Status::OK();
}

Status AIChunkBuffer::attach_sink_observer(int32_t driver_sequence, RuntimeState* state, PipelineObserver* observer) {
    if (state == nullptr || observer == nullptr) {
        return Status::InvalidArgument("AI chunk buffer sink observer and runtime state cannot be null");
    }
    Observable* observable = nullptr;
    {
        std::lock_guard lock(_mutex);
        RETURN_IF_ERROR(_validate_lane_locked(driver_sequence));
        observable = &_lanes[driver_sequence]->sink_observable;
    }
    observable->add_observer(state, observer);
    return Status::OK();
}

size_t AIChunkBuffer::size() const {
    std::lock_guard lock(_mutex);
    return _size;
}

size_t AIChunkBuffer::retained_bytes() const {
    std::lock_guard lock(_mutex);
    return _retained_bytes;
}

void AIChunkBuffer::close() {
    {
        std::lock_guard lock(_mutex);
        if (_closed) {
            return;
        }
        _closed = true;
        _reset_waiters_locked();
        _clear_all_locked();
        _finished_sources = _lanes.size();

        for (const auto& lane : _lanes) {
            lane->source_finished = true;
        }
    }
    _clear_retirement_queues();
    _notify_all_sources();
    _notify_all_sinks();
}

Status AIChunkBuffer::_validate_lane_locked(int32_t driver_sequence) const {
    if (_lanes.empty()) {
        return Status::InternalError("AI chunk buffer is not configured");
    }
    if (driver_sequence < 0 || static_cast<size_t>(driver_sequence) >= _lanes.size()) {
        return Status::InvalidArgument("AI chunk buffer driver sequence is out of range");
    }
    return Status::OK();
}

bool AIChunkBuffer::_can_admit_locked(size_t chunk_bytes) const {
    if (_size > _capacity || _reserved_wake_count > _capacity - _size) {
        return false;
    }
    const size_t occupied_slots = _size + _reserved_wake_count;
    if (occupied_slots >= _capacity) {
        return false;
    }
    if (_reserved_wake_bytes > std::numeric_limits<size_t>::max() - _retained_bytes) {
        return false;
    }
    const size_t effective_retained_bytes = _retained_bytes + _reserved_wake_bytes;

    // The byte limit is soft: one oversized chunk is admitted or reserved
    // only when the fragment buffer is globally empty.
    if (occupied_slots == 0) {
        return effective_retained_bytes == 0;
    }
    if (effective_retained_bytes > _max_retained_bytes) {
        return false;
    }
    return chunk_bytes <= _max_retained_bytes - effective_retained_bytes;
}

bool AIChunkBuffer::_has_unreserved_waiter_locked() const noexcept {
    return std::any_of(_lanes.begin(), _lanes.end(), [](const auto& lane) {
        return !lane->source_finished && !lane->sink_eos && lane->sink_waiting && !lane->wake_pending;
    });
}

StatusOr<bool> AIChunkBuffer::_clear_waiter_locked(Lane* lane) {
    if (!lane->sink_waiting) {
        if (lane->wake_pending) {
            return Status::InternalError("AI chunk buffer has a wake reservation without a waiting sink");
        }
        lane->waiting_bytes = 0;
        return false;
    }

    const bool released_reservation = lane->wake_pending;
    if (released_reservation) {
        if (_reserved_wake_count == 0 || _reserved_wake_bytes < lane->waiting_bytes) {
            return Status::InternalError("AI chunk buffer wake reservation accounting underflows");
        }
        --_reserved_wake_count;
        _reserved_wake_bytes -= lane->waiting_bytes;
        DCHECK_LE(_reserved_wake_count, _capacity - _size);
    }
    lane->sink_waiting = false;
    lane->wake_pending = false;
    lane->waiting_bytes = 0;
    return released_reservation;
}

void AIChunkBuffer::_reserve_waiters_locked(NotificationList* observables) noexcept {
    DCHECK(observables != nullptr);
    DCHECK_GE(observables->capacity(), _lanes.size());
    if (_closed || _lanes.empty()) {
        return;
    }

    const size_t lane_count = _lanes.size();
    size_t lane_index = _next_waiter;
    for (size_t examined = 0; examined < lane_count; ++examined) {
        Lane* lane = _lanes[lane_index].get();
        if (!lane->source_finished && !lane->sink_eos && lane->sink_waiting && !lane->wake_pending &&
            _can_admit_locked(lane->waiting_bytes)) {
            if (_reserved_wake_count == std::numeric_limits<size_t>::max() ||
                lane->waiting_bytes > std::numeric_limits<size_t>::max() - _reserved_wake_bytes) {
                break;
            }
            DCHECK_LT(observables->size(), observables->capacity());
            observables->emplace_back(&lane->sink_observable);
            lane->wake_pending = true;
            ++_reserved_wake_count;
            _reserved_wake_bytes += lane->waiting_bytes;
            DCHECK_LE(_reserved_wake_count, _capacity - _size);
            _next_waiter = lane_index + 1 == lane_count ? 0 : lane_index + 1;
        }
        lane_index = lane_index + 1 == lane_count ? 0 : lane_index + 1;
    }
}

void AIChunkBuffer::_reset_waiters_locked() {
    for (const auto& lane : _lanes) {
        lane->sink_waiting = false;
        lane->wake_pending = false;
        lane->waiting_bytes = 0;
    }
    _reserved_wake_count = 0;
    _reserved_wake_bytes = 0;
    _next_waiter = 0;
}

void AIChunkBuffer::_drop_lane_locked(Lane* lane, EntryQueue* dropped_entries) noexcept {
    DCHECK(dropped_entries != nullptr);
    DCHECK(dropped_entries->empty());
    for (const Entry& entry : lane->chunks) {
        DCHECK_GE(_retained_bytes, entry.retained_bytes);
        DCHECK_GT(_size, 0);
        _retained_bytes -= entry.retained_bytes;
        --_size;
    }
    dropped_entries->swap(lane->chunks);
}

void AIChunkBuffer::_clear_all_locked() noexcept {
    DCHECK_EQ(_lanes.size(), _retirement_queues.size());
    for (size_t i = 0; i < _lanes.size(); ++i) {
        DCHECK(_retirement_queues[i].empty());
        _retirement_queues[i].swap(_lanes[i]->chunks);
    }
    _size = 0;
    _retained_bytes = 0;
}

void AIChunkBuffer::_clear_retirement_queues() noexcept {
    for (EntryQueue& queue : _retirement_queues) {
        queue.clear();
    }
}

void AIChunkBuffer::_notify_source(Observable* observable) {
    observable->notify_source_observers();
}

void AIChunkBuffer::_notify_sinks(const NotificationList& observables) {
    for (Observable* observable : observables) {
        observable->notify_sink_observers();
    }
}

void AIChunkBuffer::_notify_all_sinks() {
    for (const auto& lane : _lanes) {
        lane->sink_observable.notify_sink_observers();
    }
}

void AIChunkBuffer::_notify_all_sources() {
    for (const auto& lane : _lanes) {
        lane->source_observable.notify_source_observers();
    }
}

} // namespace starrocks::pipeline
