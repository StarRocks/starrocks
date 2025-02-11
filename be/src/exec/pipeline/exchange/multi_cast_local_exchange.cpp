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

#include "exec/pipeline/exchange/multi_cast_local_exchange.h"

#include <glog/logging.h>

#include "exec/pipeline/exchange/multi_cast_local_exchange_sink_operator.h"
#include "util/logging.h"

namespace starrocks::pipeline {

#ifndef BE_TEST
static const size_t kBufferedRowSizeScaleFactor = 16;
#else
static const size_t kBufferedRowSizeScaleFactor = 2;
#endif

InMemoryMultiCastLocalExchanger::InMemoryMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number)
        : _runtime_state(runtime_state),
          _mutex(),
          _consumer_number(consumer_number),
          _progress(consumer_number),
          _opened_source_opcount(consumer_number) {
    Cell* dummy = new Cell();
    dummy->used_count = consumer_number;
    _head = dummy;
    _tail = dummy;
    for (size_t i = 0; i < consumer_number; i++) {
        _progress[i] = _tail;
        _opened_source_opcount[i] = 0;
    }
}

InMemoryMultiCastLocalExchanger::~InMemoryMultiCastLocalExchanger() {
    while (_head != nullptr) {
        auto* t = _head->next;
        _current_memory_usage -= _head->memory_usage;
        delete _head;
        _head = t;
    }
}

bool InMemoryMultiCastLocalExchanger::can_push_chunk() const {
    std::unique_lock l(_mutex);
    // if for the fastest consumer, the exchanger still has enough chunk to be consumed.
    // the exchanger does not need any input.

    if ((_current_accumulated_row_size - _fast_accumulated_row_size) >
        _runtime_state->chunk_size() * kBufferedRowSizeScaleFactor) {
        return false;
    }

    return true;
}

Status InMemoryMultiCastLocalExchanger::push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence) {
    if (chunk->num_rows() == 0) return Status::OK();

    auto* cell = new Cell();
    cell->chunk = chunk;
    cell->memory_usage = chunk->memory_usage();

    {
        std::unique_lock l(_mutex);

        int32_t closed_source_number = (_consumer_number - _opened_source_number);

        cell->used_count = closed_source_number;
        cell->next = nullptr;

        _tail->next = cell;
        _tail = cell;
        _current_accumulated_row_size += chunk->num_rows();
        cell->accumulated_row_size = _current_accumulated_row_size;
        _current_memory_usage += cell->memory_usage;
        _current_row_size = _current_accumulated_row_size - _head->accumulated_row_size;
#ifndef BE_TEST
        _peak_memory_usage_counter->set(_current_memory_usage);
        _peak_buffer_row_size_counter->set(_current_row_size);
#endif
    }

    return Status::OK();
}

Status InMemoryMultiCastLocalExchanger::init_metrics(RuntimeProfile* profile, bool is_first_sink_driver) {
    profile->add_info_string("IsSpill", "false");
    if (is_first_sink_driver) {
        _peak_memory_usage_counter = profile->AddHighWaterMarkCounter(
                "ExchangerPeakMemoryUsage", TUnit::BYTES,
                RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
        _peak_buffer_row_size_counter = profile->AddHighWaterMarkCounter(
                "ExchangerPeakBufferRowSize", TUnit::UNIT,
                RuntimeProfile::Counter::create_strategy(TUnit::UNIT, TCounterMergeType::SKIP_FIRST_MERGE));
    }
    return Status::OK();
}

bool InMemoryMultiCastLocalExchanger::can_pull_chunk(int32_t mcast_consumer_index) const {
    DCHECK(mcast_consumer_index < _consumer_number);

    std::unique_lock l(_mutex);
    DCHECK(_progress[mcast_consumer_index] != nullptr);
    if (_opened_sink_number == 0) return true;
    auto* cell = _progress[mcast_consumer_index];
    if (cell->next != nullptr) {
        return true;
    }
    return false;
}

StatusOr<ChunkPtr> InMemoryMultiCastLocalExchanger::pull_chunk(RuntimeState* state, int32_t mcast_consumer_index) {
    DCHECK(mcast_consumer_index < _consumer_number);

    std::unique_lock l(_mutex);
    DCHECK(_progress[mcast_consumer_index] != nullptr);
    Cell* cell = _progress[mcast_consumer_index];
    if (cell->next == nullptr) {
        if (_opened_sink_number == 0) return Status::EndOfFile("mcast_local_exchanger eof");
        return Status::InternalError("unreachable in multicast local exchanger");
    }
    cell = cell->next;
    VLOG_FILE << "MultiCastLocalExchanger: return chunk to " << mcast_consumer_index
              << ", row = " << cell->chunk->debug_row(0) << ", size = " << cell->chunk->num_rows();
    _progress[mcast_consumer_index] = cell;
    cell->used_count += 1;

    _update_progress(cell);
    return cell->chunk;
}

void InMemoryMultiCastLocalExchanger::open_source_operator(int32_t mcast_consumer_index) {
    std::unique_lock l(_mutex);
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number += 1;
    }
    _opened_source_opcount[mcast_consumer_index] += 1;
}

void InMemoryMultiCastLocalExchanger::close_source_operator(int32_t mcast_consumer_index) {
    std::unique_lock l(_mutex);
    _opened_source_opcount[mcast_consumer_index] -= 1;
    if (_opened_source_opcount[mcast_consumer_index] == 0) {
        _opened_source_number--;
        _closer_consumer(mcast_consumer_index);
    }
}

void InMemoryMultiCastLocalExchanger::open_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number++;
}
void InMemoryMultiCastLocalExchanger::close_sink_operator() {
    std::unique_lock l(_mutex);
    _opened_sink_number--;
}

void InMemoryMultiCastLocalExchanger::_closer_consumer(int32_t mcast_consumer_index) {
    Cell* now = _progress[mcast_consumer_index];
    now = now->next;
    while (now) {
        now->used_count += 1;
        now = now->next;
    }
    _progress[mcast_consumer_index] = nullptr;
    _update_progress();
}

void InMemoryMultiCastLocalExchanger::_update_progress(Cell* fast) {
    if (fast != nullptr) {
        _fast_accumulated_row_size = std::max(_fast_accumulated_row_size, fast->accumulated_row_size);
    } else {
        // update the fastest consumer.
        _fast_accumulated_row_size = 0;
        for (size_t i = 0; i < _consumer_number; i++) {
            Cell* c = _progress[i];
            if (c == nullptr) continue;
            _fast_accumulated_row_size = std::max(_fast_accumulated_row_size, c->accumulated_row_size);
        }
    }

    // release chunk if no one needs it.
    while (_head && _head->used_count == _consumer_number) {
        Cell* t = _head->next;
        if (t == nullptr) break;
        // in this case, _head may still be referenced by _progress and it is not safe to delete it.
        if (t->used_count < _consumer_number) break;
        _current_memory_usage -= _head->memory_usage;
        delete _head;
        _head = t;
    }
    if (_head != nullptr) {
        _current_row_size = _current_accumulated_row_size - _head->accumulated_row_size;
    }
}

} // namespace starrocks::pipeline
