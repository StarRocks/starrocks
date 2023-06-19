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

#include "exec/pipeline/sort/spillable_partition_sort_sink_operator.h"

#include "exec/chunks_sorter_heap_sort.h"
#include "exec/chunks_sorter_topn.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/common.h"
#include "exec/spill/executor.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "exec/spillable_chunks_sorter_sort.h"
#include "gen_cpp/InternalService_types.h"
#include "storage/chunk_helper.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {
Status SpillablePartitionSortSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(PartitionSortSinkOperator::prepare(state));
    RETURN_IF_ERROR(_chunks_sorter->spiller()->prepare(state));
    if (state->spill_mode() == TSpillMode::FORCE) {
        _chunks_sorter->set_spill_stragety(spill::SpillStrategy::SPILL_ALL);
    }
    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    return Status::OK();
}

void SpillablePartitionSortSinkOperator::close(RuntimeState* state) {
    PartitionSortSinkOperator::close(state);
}

size_t SpillablePartitionSortSinkOperator::estimated_memory_reserved(const ChunkPtr& chunk) {
    return _chunks_sorter->reserved_bytes(chunk);
}

size_t SpillablePartitionSortSinkOperator::estimated_memory_reserved() {
    return _chunks_sorter->reserved_bytes(nullptr);
}

Status SpillablePartitionSortSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(PartitionSortSinkOperator::push_chunk(state, chunk));
    set_revocable_mem_bytes(_chunks_sorter->revocable_mem_bytes());
    return Status::OK();
}

Status SpillablePartitionSortSinkOperator::set_finishing(RuntimeState* state) {
    auto defer_set_finishing = DeferOp([this]() { _chunks_sorter->spill_channel()->set_finishing(); });
    if (state->is_cancelled()) {
        _is_finished = true;
        _chunks_sorter->cancel();
        return Status::Cancelled("runtime state is cancelled");
    }

    // channnel:
    //
    // if has spill task. we should wait all spill task finished then to call finished
    // TODO: test cancel case
    auto io_executor = _chunks_sorter->spill_channel()->io_executor();
    auto set_call_back_function = [this](RuntimeState* state, auto io_executor) {
        return _chunks_sorter->spiller()->set_flush_all_call_back(
                [this]() {
                    // Current partition sort is ended, and
                    // the last call will drive LocalMergeSortSourceOperator to work.
                    TRACE_SPILL_LOG << "finish partition rows:" << _chunks_sorter->get_output_rows();
                    _sort_context->finish_partition(_chunks_sorter->get_output_rows());
                    _is_finished = true;
                    return Status::OK();
                },
                state, *io_executor, RESOURCE_TLS_MEMTRACER_GUARD(state));
    };

    Status ret_status;
    auto defer = DeferOp([&]() {
        Status st = [&]() {
            if (_chunks_sorter->spill_channel()->is_working()) {
                std::function<StatusOr<ChunkPtr>()> task = [state, io_executor,
                                                            set_call_back_function]() -> StatusOr<ChunkPtr> {
                    RETURN_IF_ERROR(set_call_back_function(state, io_executor));
                    return Status::EndOfFile("eos");
                };
                _chunks_sorter->spill_channel()->add_spill_task({task});
            } else {
                RETURN_IF_ERROR(set_call_back_function(state, io_executor));
            }
            return Status::OK();
        }();
        ret_status = ret_status.ok() ? st : ret_status;
    });

    ret_status = _chunks_sorter->done(state);
    return ret_status;
}

Status SpillablePartitionSortSinkOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    _chunks_sorter->cancel();
    return Status::OK();
}

OperatorPtr SpillablePartitionSortSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    std::shared_ptr<ChunksSorter> chunks_sorter;

    chunks_sorter = std::make_unique<SpillableChunksSorterFullSort>(
            runtime_state(), &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first, _sort_keys,
            _max_buffered_rows, _max_buffered_bytes, _early_materialized_slots);

    auto spiller = _spill_factory->create(*_spill_options);
    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);
    spill_channel->set_spiller(spiller);

    chunks_sorter->set_spiller(spiller);
    chunks_sorter->set_spill_channel(spill_channel);

    auto sort_context = _sort_context_factory->create(driver_sequence);
    sort_context->add_partition_chunks_sorter(chunks_sorter);
    auto ope = std::make_shared<SpillablePartitionSortSinkOperator>(
            this, _id, _plan_node_id, driver_sequence, chunks_sorter, _sort_exec_exprs, _order_by_types,
            _materialized_tuple_desc, sort_context.get(), _runtime_filter_hub);

    return ope;
}

Status SpillablePartitionSortSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(PartitionSortSinkOperatorFactory::prepare(state));

    auto* sort_desc = state->obj_pool()->add(new SortDescs(_is_asc_order, _is_null_first));

    // init spill parameters
    _spill_options = std::make_shared<spill::SpilledOptions>(&_sort_exec_exprs, sort_desc);
    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "local-sort-spill";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    return Status::OK();
}

void SpillablePartitionSortSinkOperatorFactory::close(RuntimeState* state) {
    PartitionSortSinkOperatorFactory::close(state);
}

} // namespace starrocks::pipeline