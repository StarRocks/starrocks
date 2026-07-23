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

<<<<<<< HEAD
=======
#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/spiller.hpp"
#include "exec/pipeline/context_with_dependency.h"
>>>>>>> 5685652682 ([BugFix] Guard spillable join build set_finishing against cancel-time UAF (#76633))
#include "exec/pipeline/spill_process_channel.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller.hpp"

namespace starrocks::pipeline {

<<<<<<< HEAD
=======
Status SpillProcessOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    // The pump sleeps INPUT_EMPTY on has_output() (has_task && !is_full), so it belongs on the spiller's
    // source list: flush completion notifies both lists, and the channel handshake (enqueue / set_finishing)
    // wakes the source side. The spiller is set on the channel before prepare (create() wired it). The gate
    // for poller mode is inside subscribe_source.
    if (_channel->spiller() != nullptr) {
        _channel->spiller()->observable().subscribe_source(state, observer());
    }
    // Lifetime anchor: hold a ref on the spilling context (hash joiner / aggregator) for the whole
    // spill-processing lifetime, so async spill tasks (which reference context-owned state such as the
    // build chunks / hash map) never dereference it after the owning operators free it on cancel/close.
    if (auto* context = _channel->guarded_context(); context != nullptr) {
        context->ref();
    }
    return Status::OK();
}

>>>>>>> 5685652682 ([BugFix] Guard spillable join build set_finishing against cancel-time UAF (#76633))
bool SpillProcessOperator::has_output() const {
    return !_is_finished && _channel->has_output();
}

bool SpillProcessOperator::is_finished() const {
    return _is_finished || _channel->is_finished();
}

Status SpillProcessOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

void SpillProcessOperator::close(RuntimeState* state) {
    _channel->close();
    // Release the lifetime-anchor ref taken in prepare(); the channel is drained by now, so no spill
    // task will dereference the context after this (which may be the last unref -> context close()).
    if (auto* context = _channel->guarded_context(); context != nullptr) {
        context->unref(state);
    }
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
            RETURN_IF_ERROR(
                    spiller->spill(state, std::move(chunk_st.value()), TRACKER_WITH_SPILLER_GUARD(state, spiller)));
        }
    } else if (chunk_st.status().is_end_of_file()) {
        _channel->current_task().reset();
    } else if (!chunk_st.status().ok()) {
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