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

#include <glog/logging.h>
#include <memory>
#include "common/compiler_util.h"
#include "common/status.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/mem_limited_chunk_queue.h"

#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "fs/fs.h"
#include "serde/column_array_serde.h"
#include "serde/protobuf_serde.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"
#include "fmt/format.h"

namespace starrocks::pipeline {

SpillableMultiCastLocalExchanger::SpillableMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number, int64_t memory_limit)
    : _runtime_state(runtime_state), _consumer_number(consumer_number), _progress(consumer_number), _opened_source_opcount(consumer_number), _has_load_io_task(consumer_number) {
    Cell* dummy = new Cell();
    _head = dummy;
    _tail = dummy;
    for (size_t i = 0; i < consumer_number; i++) {
        // _progress[i] = _cells.begin();
        _progress[i] = _tail;
        _opened_source_opcount[i] = 0;
        _has_load_io_task[i] = false;
    }

    MemLimitedChunkQueue::Options opts;
    // @TODO init opts
    opts.block_manager = runtime_state->query_ctx()->spill_manager()->block_manager();

    // @TODO link rf
    _runtime_profile = std::make_unique<RuntimeProfile>("SpillableMultiCastLocalExchanger");
    _queue = std::make_shared<MemLimitedChunkQueue>(runtime_state, _runtime_profile.get(), consumer_number, opts);
    _peak_memory_usage_counter = _runtime_profile->AddHighWaterMarkCounter(
            "PeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    _peak_buffer_row_size_counter = _runtime_profile->AddHighWaterMarkCounter(
            "PeakBufferRowSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT));

}
SpillableMultiCastLocalExchanger::~SpillableMultiCastLocalExchanger() {

    // while(_head) {
    //     auto* t = _head->next;
    //     _current_memory_usage -= _head->memory_usage;
    //     delete _head;
    //     _head = t;
    // }
}

bool SpillableMultiCastLocalExchanger::can_pull_chunk(int32_t mcast_consumer_index) const {
    return _queue->can_pop(mcast_consumer_index);
}

bool SpillableMultiCastLocalExchanger::can_push_chunk() const {
    return _queue->can_push();
}

Status SpillableMultiCastLocalExchanger::push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence, MultiCastLocalExchangeSinkOperator* sink_operator) {
    return _queue->push(chunk, sink_operator);
}

StatusOr<ChunkPtr> SpillableMultiCastLocalExchanger::pull_chunk(RuntimeState* state, int32_t mcast_consumer_index) {
    return _queue->pop(mcast_consumer_index);
}

void SpillableMultiCastLocalExchanger::open_source_operator(int32_t mcast_consumer_index) {
    _queue->open_consumer(mcast_consumer_index);
}

void SpillableMultiCastLocalExchanger::close_source_operator(int32_t mcast_consumer_index) {
    _queue->close_consumer(mcast_consumer_index);
}

void SpillableMultiCastLocalExchanger::open_sink_operator() {
    _queue->open_producer();
}

void SpillableMultiCastLocalExchanger::close_sink_operator() {
    _queue->close_producer();
}

void SpillableMultiCastLocalExchanger::_close_consumer(int32_t mcast_consumer_index) {
    LOG(INFO) << "_close consumer: " << mcast_consumer_index;
    Cell* now = _progress[mcast_consumer_index];
    now = now->next;
    while (now) {
        now->used_count += 1;
        // LOG(INFO) << "used_count: " << now->used_count;
        now = now->next;
    }
    _progress[mcast_consumer_index] = nullptr;
    _update_progress();
}

void SpillableMultiCastLocalExchanger::_update_progress(Cell* fast) {
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
    while (_head) {

    }
    while (_head && _head->used_count == _consumer_number) {
        Cell* t = _head->next;
        _current_memory_usage -= _head->memory_usage;
        LOG(INFO) << "release chunk, mem: " << _head->memory_usage << ", new mem: " << _current_memory_usage - _head->memory_usage;
        // @TODO free here
        delete _head;
        if (t == nullptr) break;
        _head = t;
    }
    if (_head != nullptr) {
        _current_row_size = _current_accumulated_row_size - _head->accumulated_row_size;
    }

}
}