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

#include "exec/pipeline/spill_process_operator.h"

#include <memory>

#include "exec/pipeline/spill_process_channel.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller.hpp"

namespace starrocks::pipeline {

bool SpillProcessOperator::has_output() const {
    return _channel->has_output();
}

bool SpillProcessOperator::is_finished() const {
    return _channel->is_finished();
}

void SpillProcessOperator::close(RuntimeState* state) {
    SourceOperator::close(state);
}

StatusOr<ChunkPtr> SpillProcessOperator::pull_chunk(RuntimeState* state) {
    if (!_channel->current_task()) {
        bool res = _channel->acquire_spill_task();
        if (!res) {
            return Status::InternalError("couldn't get task from spill channel");
        }
    }
    DCHECK(_channel->current_task());

    auto chunk_st = _channel->current_task()();
    if (chunk_st.status().ok() && !state->is_cancelled()) {
        auto chunk = chunk_st.value();
        if (chunk != nullptr && !chunk->is_empty()) {
            auto& spiller = _channel->spiller();
            RETURN_IF_ERROR(spiller->spill(state, std::move(chunk_st.value()),
                                           TRACKER_WITH_SPILLER_GUARD(state, spiller)));
        }
    } else if (chunk_st.status().is_end_of_file()) {
        _channel->current_task().reset();
    } else {
        return chunk_st.status();
    }

    return nullptr;
}

OperatorPtr SpillProcessOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto channel = _process_ctx->get_or_create(driver_sequence);
    auto processor = std::make_shared<SpillProcessOperator>(this, id(), get_raw_name(), plan_node_id(), driver_sequence,
                                                            std::move(channel));
    return processor;
}

} // namespace starrocks::pipeline