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

#include "exec/pipeline/hashjoin/spillable_hash_join_build_operator.h"

#include <atomic>
#include <memory>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exec/hash_join_node.h"
#include "exec/join_hash_map.h"
#include "exec/pipeline/hashjoin/hash_join_build_operator.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/options.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "util/bit_util.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

Status SpillableHashJoinBuildOperator::prepare(RuntimeState* state) {
    HashJoinBuildOperator::prepare(state);
    _join_builder->spiller()->set_metrics(
            spill::SpillProcessMetrics(_unique_metrics.get(), state->mutable_total_spill_bytes()));
    RETURN_IF_ERROR(_join_builder->spiller()->prepare(state));
    if (state->spill_mode() == TSpillMode::FORCE) {
        set_spill_strategy(spill::SpillStrategy::SPILL_ALL);
    }
    _peak_revocable_mem_bytes = _unique_metrics->AddHighWaterMarkCounter(
            "PeakRevocableMemoryBytes", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    return Status::OK();
}

void SpillableHashJoinBuildOperator::close(RuntimeState* state) {
    HashJoinBuildOperator::close(state);
}

size_t SpillableHashJoinBuildOperator::estimated_memory_reserved(const ChunkPtr& chunk) {
    if (chunk && !chunk->is_empty()) {
        return chunk->memory_usage() + _join_builder->hash_join_builder()->hash_table().mem_usage();
    }
    return 0;
}

size_t SpillableHashJoinBuildOperator::estimated_memory_reserved() {
    return _join_builder->hash_join_builder()->hash_table().mem_usage() * 2;
}

bool SpillableHashJoinBuildOperator::need_input() const {
    return !is_finished() && !(_join_builder->spiller()->is_full() || _join_builder->spill_channel()->has_task());
}

Status SpillableHashJoinBuildOperator::set_finishing(RuntimeState* state) {
    auto defer_set_finishing = DeferOp([this]() { _join_builder->spill_channel()->set_finishing(); });

    if (spill_strategy() == spill::SpillStrategy::NO_SPILL ||
        (!_join_builder->spiller()->spilled() &&
         _join_builder->hash_join_builder()->hash_table().get_row_count() == 0)) {
        return HashJoinBuildOperator::set_finishing(state);
    }

    DCHECK(spill_strategy() == spill::SpillStrategy::SPILL_ALL);
    // if this operator is changed to spill mode just before set_finishing,
    // we should create spill task
    if (!_join_builder->spiller()->spilled()) {
        DCHECK(_is_first_time_spill);
        _is_first_time_spill = false;
        auto& ht = _join_builder->hash_join_builder()->hash_table();
        RETURN_IF_ERROR(init_spiller_partitions(state, ht));

        _hash_table_slice_iterator = _convert_hash_map_to_chunk();
        RETURN_IF_ERROR(_join_builder->append_spill_task(state, _hash_table_slice_iterator));
    }

    if (state->is_cancelled()) {
        _join_builder->spiller()->cancel();
    }

    auto flush_function = [this](RuntimeState* state, auto io_executor) {
        return _join_builder->spiller()->flush(state, *io_executor, RESOURCE_TLS_MEMTRACER_GUARD(state));
    };

    auto io_executor = _join_builder->spill_channel()->io_executor();
    auto set_call_back_function = [this](RuntimeState* state, auto io_executor) {
        return _join_builder->spiller()->set_flush_all_call_back(
                [this]() {
                    _is_finished = true;
                    _join_builder->enter_probe_phase();
                    return Status::OK();
                },
                state, *io_executor, RESOURCE_TLS_MEMTRACER_GUARD(state));
    };

    publish_runtime_filters(state);
    SpillProcessTasksBuilder task_builder(state, io_executor);
    task_builder.then(flush_function).finally(set_call_back_function);

    RETURN_IF_ERROR(_join_builder->spill_channel()->execute(task_builder));

    return Status::OK();
}

Status SpillableHashJoinBuildOperator::publish_runtime_filters(RuntimeState* state) {
    // publish empty runtime filters

    // Building RuntimeBloomFilter need to know the initial hash table size and all join keys datas.
    // It usually involves re-reading all the data that has been spilled
    // which cannot be streamed process in the spill scenario when build phase is finished
    // (unless FE can give an estimate of the hash table size), so we currently empty all the hash tables first
    // we could build global runtime filter for this case later.
    auto merged = _partial_rf_merger->set_always_true();

    if (merged) {
        RuntimeInFilterList in_filters;
        RuntimeBloomFilterList bloom_filters;
        // publish empty runtime bloom-filters
        state->runtime_filter_port()->publish_runtime_filters(bloom_filters);
        // move runtime filters into RuntimeFilterHub.
        runtime_filter_hub()->set_collector(_plan_node_id, std::make_unique<RuntimeFilterCollector>(
                                                                   std::move(in_filters), std::move(bloom_filters)));
    }
    return Status::OK();
}

Status SpillableHashJoinBuildOperator::append_hash_columns(const ChunkPtr& chunk) {
    auto factory = down_cast<SpillableHashJoinBuildOperatorFactory*>(_factory);
    const auto& build_partition = factory->build_side_partition();

    size_t num_rows = chunk->num_rows();
    auto hash_column = spill::SpillHashColumn::create(num_rows);
    auto& hash_values = hash_column->get_data();

    // TODO: use different hash method
    for (auto& expr_ctx : build_partition) {
        ASSIGN_OR_RETURN(auto res, expr_ctx->evaluate(chunk.get()));
        res->fnv_hash(hash_values.data(), 0, num_rows);
    }
    chunk->append_column(std::move(hash_column), -1);
    return Status::OK();
}

Status SpillableHashJoinBuildOperator::init_spiller_partitions(RuntimeState* state, JoinHashTable& ht) {
    if (ht.get_row_count() > 0) {
        // We estimate the size of the hash table to be twice the size of the already input hash table
        auto num_partitions = ht.mem_usage() * 2 / _join_builder->spiller()->options().spill_mem_table_bytes_size;
        RETURN_IF_ERROR(_join_builder->spiller()->set_partition(state, num_partitions));
    }
    return Status::OK();
}

bool SpillableHashJoinBuildOperator::is_finished() const {
    return _is_finished || _join_builder->is_finished();
}

Status SpillableHashJoinBuildOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DeferOp update_revocable_bytes{
            [this]() { set_revocable_mem_bytes(_join_builder->hash_join_builder()->hash_table().mem_usage()); }};

    if (spill_strategy() == spill::SpillStrategy::NO_SPILL) {
        return HashJoinBuildOperator::push_chunk(state, chunk);
    }

    if (!chunk || chunk->is_empty()) {
        return Status::OK();
    }

    auto& ht = _join_builder->hash_join_builder()->hash_table();
    // Estimate the appropriate number of partitions
    if (_is_first_time_spill) {
        RETURN_IF_ERROR(init_spiller_partitions(state, ht));
    }

    ASSIGN_OR_RETURN(auto spill_chunk, ht.convert_to_spill_schema(chunk));
    RETURN_IF_ERROR(append_hash_columns(spill_chunk));

    RETURN_IF_ERROR(_join_builder->append_chunk_to_spill_buffer(state, spill_chunk));

    if (_is_first_time_spill) {
        _is_first_time_spill = false;
        _hash_table_slice_iterator = _convert_hash_map_to_chunk();
        RETURN_IF_ERROR(_join_builder->append_spill_task(state, _hash_table_slice_iterator));
    }

    return Status::OK();
}

void SpillableHashJoinBuildOperator::set_execute_mode(int performance_level) {
    if (!_is_finished) {
        _join_builder->set_spill_strategy(spill::SpillStrategy::SPILL_ALL);
    }
}

std::function<StatusOr<ChunkPtr>()> SpillableHashJoinBuildOperator::_convert_hash_map_to_chunk() {
    auto build_chunk = _join_builder->hash_join_builder()->hash_table().get_build_chunk();
    DCHECK_GT(build_chunk->num_rows(), 0);

    _hash_table_build_chunk_slice.reset(build_chunk);
    _hash_table_build_chunk_slice.skip(kHashJoinKeyColumnOffset);

    return [this]() -> StatusOr<ChunkPtr> {
        if (_hash_table_build_chunk_slice.empty()) {
            _join_builder->hash_join_builder()->reset(_join_builder->hash_table_param());
            return Status::EndOfFile("eos");
        }
        auto chunk = _hash_table_build_chunk_slice.cutoff(runtime_state()->chunk_size());
        RETURN_IF_ERROR(append_hash_columns(chunk));
        _join_builder->update_build_rows(chunk->num_rows());
        return chunk;
    };
}

Status SpillableHashJoinBuildOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinBuildOperatorFactory::prepare(state));

    // no order by, init with 4 partitions
    _spill_options = std::make_shared<spill::SpilledOptions>(config::spill_init_partition);
    _spill_options->spill_mem_table_bytes_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    _spill_options->min_spilled_size = state->spill_operator_min_bytes();
    _spill_options->block_manager = state->query_ctx()->spill_manager()->block_manager();
    _spill_options->name = "hash-join-build";
    _spill_options->plan_node_id = _plan_node_id;
    _spill_options->encode_level = state->spill_encode_level();
    // TODO: Our current adaptive dop for non-broadcast functions will also result in a build hash_joiner corresponding to multiple prob hash_join prober.
    //
    _spill_options->read_shared =
            _hash_joiner_factory->hash_join_param()._distribution_mode == TJoinDistributionMode::BROADCAST ||
            state->fragment_ctx()->enable_adaptive_dop();

    const auto& param = _hash_joiner_factory->hash_join_param();

    _build_side_partition = param._build_expr_ctxs;

    return Status::OK();
}

void SpillableHashJoinBuildOperatorFactory::close(RuntimeState* state) {
    HashJoinBuildOperatorFactory::close(state);
}

OperatorPtr SpillableHashJoinBuildOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    if (_string_key_columns.empty()) {
        _string_key_columns.resize(degree_of_parallelism);
    }

    auto spiller = _spill_factory->create(*_spill_options);
    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);
    spill_channel->set_spiller(spiller);

    auto joiner = _hash_joiner_factory->create_builder(degree_of_parallelism, driver_sequence);

    joiner->set_spill_channel(spill_channel);
    joiner->set_spiller(spiller);

    return std::make_shared<SpillableHashJoinBuildOperator>(this, _id, "spillable_hash_join_build", _plan_node_id,
                                                            driver_sequence, joiner, _partial_rf_merger.get(),
                                                            _distribution_mode);
}

} // namespace starrocks::pipeline
