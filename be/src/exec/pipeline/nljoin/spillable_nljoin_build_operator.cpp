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

#include "exec/pipeline/nljoin/spillable_nljoin_build_operator.h"

#include "exec/pipeline/nljoin/nljoin_build_operator.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/options.h"
#include "exec/spill/spiller.hpp"
#include "gen_cpp/InternalService_types.h"

namespace starrocks::pipeline {
Status SpillableNLJoinBuildOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(NLJoinBuildOperator::prepare(state));
    _spill_channel->spiller()->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));
    RETURN_IF_ERROR(_spill_channel->spiller()->prepare(state));
    _cross_join_context->input_channel(_driver_sequence).set_spiller(_spill_channel->spiller());
    if (state->spill_mode() == TSpillMode::FORCE) {
        _strategy = spill::SpillStrategy::SPILL_ALL;
    }
    return Status::OK();
}

void SpillableNLJoinBuildOperator::close(RuntimeState* state) {
    NLJoinBuildOperator::close(state);
}

bool SpillableNLJoinBuildOperator::need_input() const {
    if (_strategy == spill::SpillStrategy::NO_SPILL) {
        return NLJoinBuildOperator::need_input();
    }
    return !_is_finished && !_spill_channel->spiller()->is_full();
}

bool SpillableNLJoinBuildOperator::is_finished() const {
    if (!_spill_channel->spiller()->spilled()) {
        return NLJoinBuildOperator::is_finished();
    }
    return _is_finished;
}

Status SpillableNLJoinBuildOperator::set_finishing(RuntimeState* state) {
    auto& executor = *_spill_channel->io_executor();
    auto spiller = _spill_channel->spiller();

    if (!spiller->spilled()) {
        _spill_channel->set_finishing();
        RETURN_IF_ERROR(NLJoinBuildOperator::set_finishing(state));
        return Status::OK();
    }

    RETURN_IF_ERROR(spiller->flush(state, executor, TRACKER_WITH_SPILLER_GUARD(state, spiller)));
    RETURN_IF_ERROR(spiller->set_flush_all_call_back(
            [&, state]() {
                RETURN_IF_ERROR(_cross_join_context->finish_one_right_sinker(_driver_sequence, state));
                _is_finished = true;
                _spill_channel->set_finishing();
                return Status::OK();
            },
            state, executor, TRACKER_WITH_SPILLER_GUARD(state, spiller)));

    return Status::OK();
}

Status SpillableNLJoinBuildOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (_strategy == spill::SpillStrategy::NO_SPILL) {
        RETURN_IF_ERROR(NLJoinBuildOperator::push_chunk(state, chunk));
    } else {
        // TODO: process auto spill mode
        RETURN_IF_ERROR(_cross_join_context->input_channel(_driver_sequence)
                                .add_chunk_to_spill_buffer(state, chunk, *_spill_channel->io_executor()));
    }
    return Status::OK();
}

Status SpillableNLJoinBuildOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    _spill_options = std::make_shared<spill::SpilledOptions>();
    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->min_spilled_size = state->spill_operator_min_bytes();
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "spillable-nestloop-join-build";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->read_shared = true;
    _spill_options->encode_level = state->spill_encode_level();
    _spill_options->wg = state->fragment_ctx()->workgroup();

    return Status::OK();
}

OperatorPtr SpillableNLJoinBuildOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto spiller = _spill_factory->create(*_spill_options);
    auto spill_channel = _cross_join_context->spill_channel_factory()->get_or_create(driver_sequence);
    spill_channel->set_spiller(spiller);

    auto build_operator = std::make_shared<SpillableNLJoinBuildOperator>(
            this, _id, _plan_node_id, driver_sequence, _cross_join_context, "spillable_nestloop_join_build");
    build_operator->set_channel(spill_channel);
    return build_operator;
}
} // namespace starrocks::pipeline