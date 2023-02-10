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

#include "exec/aggregate/distinct_blocking_node.h"

#include <variant>

#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_blocking_source_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_distinct_streaming_source_operator.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "runtime/current_thread.h"

namespace starrocks {

Status DistinctBlockingNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBaseNode::prepare(state));
    _aggregator->set_aggr_phase(AggrPhase2);
    return Status::OK();
}

Status DistinctBlockingNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_aggregator->open(state));
    RETURN_IF_ERROR(_children[0]->open(state));

    ChunkPtr chunk;
    bool limit_with_no_agg = limit() != -1;
    VLOG_ROW << "group_by_expr_ctxs size " << _aggregator->group_by_expr_ctxs().size() << " _needs_finalize "
             << _aggregator->needs_finalize();

    while (true) {
        RETURN_IF_ERROR(state->check_mem_limit("AggrNode"));

        bool eos = false;
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_children[0]->get_next(state, &chunk, &eos));

        if (eos) {
            break;
        }
        if (chunk->is_empty()) {
            continue;
        }
        DCHECK_LE(chunk->num_rows(), runtime_state()->chunk_size());

        RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));

        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set(chunk->num_rows()));
            _mem_tracker->set(_aggregator->hash_set_variant().reserved_memory_usage(_aggregator->mem_pool()));
            TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());

            _aggregator->update_num_input_rows(chunk->num_rows());
            if (limit_with_no_agg) {
                auto size = _aggregator->hash_set_variant().size();
                if (size >= limit()) {
                    break;
                }
            }
        }
    }

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

    // If hash set is empty, we don't need to return value
    if (_aggregator->hash_set_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }
    _aggregator->hash_set_variant().visit(
            [&](auto& hash_set_with_key) { _aggregator->it_hash() = hash_set_with_key->hash_set.begin(); });

    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _mem_tracker->set(_aggregator->hash_set_variant().reserved_memory_usage(_aggregator->mem_pool()));

    return Status::OK();
}

Status DistinctBlockingNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    *eos = false;

    if (_aggregator->is_ht_eos()) {
        COUNTER_SET(_aggregator->rows_returned_counter(), _aggregator->num_rows_returned());
        *eos = true;
        return Status::OK();
    }
    const auto chunk_size = runtime_state()->chunk_size();

    _aggregator->convert_hash_set_to_chunk(chunk_size, chunk);

    const int64_t old_size = (*chunk)->num_rows();
    eval_join_runtime_filters(chunk->get());

    // For having
    RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get()));
    _aggregator->update_num_rows_returned(-(old_size - static_cast<int64_t>((*chunk)->num_rows())));

    _aggregator->process_limit(chunk);

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

pipeline::OpFactories DistinctBlockingNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);

    // shared by sink operator and source operator
    auto should_cache = context->should_interpolate_cache_operator(ops_with_sink[0], id());
    auto* upstream_source_op = context->source_operator(ops_with_sink);
    auto operators_generator = [this, should_cache, &upstream_source_op, context](bool post_cache) {
        AggregatorFactoryPtr aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);
        AggrMode aggr_mode = should_cache ? (post_cache ? AM_BLOCKING_POST_CACHE : AM_BLOCKING_PRE_CACHE) : AM_DEFAULT;
        aggregator_factory->set_aggr_mode(aggr_mode);
        std::vector<ExprContext*> partition_expr_ctxs;
        Expr::create_expr_trees(_pool, _tnode.agg_node.grouping_exprs, &partition_expr_ctxs, runtime_state());
        Expr::prepare(partition_expr_ctxs, runtime_state());
        Expr::open(partition_expr_ctxs, runtime_state());
        auto sink_operator = std::make_shared<AggregateDistinctBlockingSinkOperatorFactory>(
                context->next_operator_id(), id(), aggregator_factory, std::move(partition_expr_ctxs));
        auto source_operator = std::make_shared<AggregateDistinctBlockingSourceOperatorFactory>(
                context->next_operator_id(), id(), aggregator_factory);
        context->inherit_upstream_source_properties(source_operator.get(), upstream_source_op);
        return std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>{sink_operator, source_operator};
    };

    auto [agg_sink_op, agg_source_op] = operators_generator(false);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(agg_sink_op.get(), context, rc_rf_probe_collector);

    OpFactories ops_with_source;
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(agg_source_op.get(), context, rc_rf_probe_collector);

    const auto& agg_partition_exprs =
            down_cast<AggregateDistinctBlockingSinkOperatorFactory*>(agg_sink_op.get())->partition_by_exprs();
    ops_with_sink =
            context->maybe_interpolate_local_shuffle_exchange(runtime_state(), ops_with_sink, agg_partition_exprs);
    ops_with_sink.push_back(std::move(agg_sink_op));

    // The upstream pipeline may be changed by *maybe_interpolate_local_shuffle_exchange*.
    upstream_source_op = context->source_operator(ops_with_sink);
    context->inherit_upstream_source_properties(agg_source_op.get(), upstream_source_op);
    ops_with_source.push_back(std::move(agg_source_op));

    if (should_cache) {
        ops_with_source = context->interpolate_cache_operator(ops_with_sink, ops_with_source, operators_generator);
    }
    context->add_pipeline(ops_with_sink);

    if (!_tnode.conjuncts.empty() || ops_with_source.back()->has_runtime_filters()) {
        may_add_chunk_accumulate_operator(ops_with_source, context, id());
    }

    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return ops_with_source;
}

} // namespace starrocks
