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
#include "exec/pipeline/exchange/mem_limited_chunk_queue.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange.h"
#include "exec/pipeline/exchange/multi_cast_local_exchange_sink_operator.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "serde/column_array_serde.h"
#include "serde/protobuf_serde.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

SpillableMultiCastLocalExchanger::SpillableMultiCastLocalExchanger(RuntimeState* runtime_state, size_t consumer_number,
                                                                   int32_t plan_node_id) {
    DCHECK(runtime_state->enable_spill() && runtime_state->enable_multi_cast_local_exchange_spill());
    MemLimitedChunkQueue::Options opts;
    opts.block_size = config::mem_limited_chunk_queue_block_size;
    if (runtime_state->spill_mode() == TSpillMode::FORCE) {
        opts.memory_limit = 16L * 1024 * 1024;
    } else {
        opts.memory_limit = std::numeric_limits<size_t>::max();
    }
    opts.plan_node_id = plan_node_id;
    opts.block_manager = runtime_state->query_ctx()->spill_manager()->block_manager();
    opts.encode_level = runtime_state->spill_encode_level();

    _queue = std::make_shared<MemLimitedChunkQueue>(runtime_state, consumer_number, opts);
}

Status SpillableMultiCastLocalExchanger::init_metrics(RuntimeProfile* profile, bool is_first_sink_driver) {
    profile->add_info_string("IsSpill", "true");
    if (is_first_sink_driver) {
        return _queue->init_metrics(profile);
    }
    return Status::OK();
}

bool SpillableMultiCastLocalExchanger::can_pull_chunk(int32_t mcast_consumer_index) const {
    return _queue->can_pop(mcast_consumer_index);
}

bool SpillableMultiCastLocalExchanger::can_push_chunk() const {
    return _queue->can_push();
}

Status SpillableMultiCastLocalExchanger::push_chunk(const ChunkPtr& chunk, int32_t sink_driver_sequence) {
    return _queue->push(chunk);
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

void SpillableMultiCastLocalExchanger::enter_release_memory_mode() {
    _queue->enter_release_memory_mode();
}
} // namespace starrocks::pipeline