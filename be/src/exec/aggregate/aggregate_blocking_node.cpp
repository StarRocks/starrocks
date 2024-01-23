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

#include "exec/aggregate/aggregate_blocking_node.h"

#include <memory>
#include <type_traits>
#include <variant>

#include "exec/aggregator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/pipeline/aggregate/aggregate_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_streaming_source_operator.h"
#include "exec/pipeline/aggregate/sorted_aggregate_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/sorted_aggregate_streaming_source_operator.h"
#include "exec/pipeline/aggregate/spillable_aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/spillable_aggregate_blocking_source_operator.h"
#include "exec/pipeline/bucket_process_operator.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/noop_sink_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/spill_process_operator.h"
#include "exec/sorted_streaming_aggregator.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks {

Status AggregateBlockingNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBaseNode::prepare(state));
    _aggregator->set_aggr_phase(AggrPhase2);
    return Status::OK();
}

Status AggregateBlockingNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_aggregator->open(state));
    RETURN_IF_ERROR(_children[0]->open(state));

    ChunkPtr chunk;

    VLOG_ROW << "group_by_expr_ctxs size " << _aggregator->group_by_expr_ctxs().size() << " _needs_finalize "
             << _aggregator->needs_finalize();
    bool agg_group_by_with_limit =
            (!_aggregator->is_none_group_by_exprs() &&     // has group by
             _limit != -1 &&                               // has limit
             _conjunct_ctxs.empty() &&                     // no 'having' clause
             _aggregator->get_aggr_phase() == AggrPhase2); // phase 2, keep it to make things safe
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

        size_t chunk_size = chunk->num_rows();
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            if (!_aggregator->is_none_group_by_exprs()) {
                TRY_CATCH_ALLOC_SCOPE_START()
                _aggregator->build_hash_map(chunk_size, agg_group_by_with_limit);

                _aggregator->try_convert_to_two_level_map();
                TRY_CATCH_ALLOC_SCOPE_END()
            }
            if (_aggregator->is_none_group_by_exprs()) {
                RETURN_IF_ERROR(_aggregator->compute_single_agg_state(chunk.get(), chunk_size));
            } else {
                if (agg_group_by_with_limit) {
                    // use `_aggregator->streaming_selection()` here to mark whether needs to filter key when compute agg states,
                    // it's generated in `build_hash_map`
                    size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection().data(), chunk_size);
                    if (zero_count == chunk_size) {
                        RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
                    } else {
                        RETURN_IF_ERROR(_aggregator->compute_batch_agg_states_with_selection(chunk.get(), chunk_size));
                    }
                } else {
                    RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
                }
            }

            _aggregator->update_num_input_rows(chunk_size);
        }
        RETURN_IF_ERROR(_aggregator->check_has_error());
    }

    if (!_aggregator->is_none_group_by_exprs()) {
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
        // If hash map is empty, we don't need to return value
        if (_aggregator->hash_map_variant().size() == 0) {
            _aggregator->set_ht_eos();
        }
        _aggregator->hash_map_variant().visit(
                [&](auto& hash_map_with_key) { _aggregator->it_hash() = _aggregator->_state_allocator.begin(); });
    } else if (_aggregator->is_none_group_by_exprs()) {
        // for aggregate no group by, if _num_input_rows is 0,
        // In update phase, we directly return empty chunk.
        // In merge phase, we will handle it.
        if (_aggregator->num_input_rows() == 0 && !_aggregator->needs_finalize()) {
            _aggregator->set_ht_eos();
        }
    }

    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));

    return Status::OK();
}

Status AggregateBlockingNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
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

    if (_aggregator->is_none_group_by_exprs()) {
        RETURN_IF_ERROR(_aggregator->convert_to_chunk_no_groupby(chunk));
    } else {
        RETURN_IF_ERROR(_aggregator->convert_hash_map_to_chunk(chunk_size, chunk));
    }

    const int64_t old_size = (*chunk)->num_rows();
    eval_join_runtime_filters(chunk->get());

    // For having
    RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get()));
    _aggregator->update_num_rows_returned(-(old_size - static_cast<int64_t>((*chunk)->num_rows())));

    _aggregator->process_limit(chunk);

    DCHECK_CHUNK(*chunk);

    RETURN_IF_ERROR(_aggregator->check_has_error());

    return Status::OK();
}

template <class AggFactory, class SourceFactory, class SinkFactory>
pipeline::OpFactories AggregateBlockingNode::_decompose_to_pipeline(pipeline::OpFactories& ops_with_sink,
                                                                    pipeline::PipelineBuilderContext* context,
                                                                    bool per_bucket_optimize) {
    using namespace pipeline;

    auto workgroup = context->fragment_context()->workgroup();
    auto executor = std::make_shared<spill::AsyncIOTaskExecutor>(ExecEnv::GetInstance()->scan_executor(), workgroup);
    auto degree_of_parallelism = context->source_operator(ops_with_sink)->degree_of_parallelism();
    auto spill_channel_factory =
            std::make_shared<SpillProcessChannelFactory>(degree_of_parallelism, std::move(executor));
    if (std::is_same_v<SinkFactory, SpillableAggregateBlockingSinkOperatorFactory>) {
        context->interpolate_spill_process(id(), spill_channel_factory, degree_of_parallelism);
    }

    auto should_cache = context->should_interpolate_cache_operator(id(), ops_with_sink[0]);
    auto* upstream_source_op = context->source_operator(ops_with_sink);
    auto operators_generator = [this, should_cache, upstream_source_op, context,
                                spill_channel_factory](bool post_cache) {
        // create aggregator factory
        // shared by sink operator and source operator
        auto aggregator_factory = std::make_shared<AggFactory>(_tnode);
        AggrMode aggr_mode = should_cache ? (post_cache ? AM_BLOCKING_POST_CACHE : AM_BLOCKING_PRE_CACHE) : AM_DEFAULT;
        aggregator_factory->set_aggr_mode(aggr_mode);
        auto sink_operator = std::make_shared<SinkFactory>(context->next_operator_id(), id(), aggregator_factory,
                                                           spill_channel_factory);
        auto source_operator = std::make_shared<SourceFactory>(context->next_operator_id(), id(), aggregator_factory);

        context->inherit_upstream_source_properties(source_operator.get(), upstream_source_op);
        return std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>(sink_operator, source_operator);
    };

    auto [agg_sink_op, agg_source_op] = operators_generator(false);
    // Create a shared RefCountedRuntimeFilterCollector
    // Initialize OperatorFactory's fields involving runtime filters.
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    this->init_runtime_filter_for_operator(agg_sink_op.get(), context, rc_rf_probe_collector);
    auto bucket_process_context_factory = std::make_shared<BucketProcessContextFactory>();
    if (per_bucket_optimize) {
        agg_sink_op = std::make_shared<BucketProcessSinkOperatorFactory>(
                context->next_operator_id(), id(), bucket_process_context_factory, std::move(agg_sink_op));
    }

    ops_with_sink.push_back(std::move(agg_sink_op));

    OpFactories ops_with_source;
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(agg_source_op.get(), context, rc_rf_probe_collector);

    if (per_bucket_optimize) {
        auto bucket_source_operator = std::make_shared<BucketProcessSourceOperatorFactory>(
                context->next_operator_id(), id(), bucket_process_context_factory, std::move(agg_source_op));
        context->inherit_upstream_source_properties(bucket_source_operator.get(), upstream_source_op);
        agg_source_op = std::move(bucket_source_operator);
    }
    ops_with_source.push_back(std::move(agg_source_op));

    if (should_cache) {
        ops_with_source =
                context->interpolate_cache_operator(id(), ops_with_sink, ops_with_source, operators_generator);
    }
    context->add_pipeline(ops_with_sink);

    return ops_with_source;
}

pipeline::OpFactories AggregateBlockingNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    context->has_aggregation = true;
    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);
    auto& agg_node = _tnode.agg_node;

    bool sorted_streaming_aggregate = _tnode.agg_node.__isset.use_sort_agg && _tnode.agg_node.use_sort_agg;
    bool use_per_bucket_optimize =
            _tnode.agg_node.__isset.use_per_bucket_optimize && _tnode.agg_node.use_per_bucket_optimize;
    bool has_group_by_keys = agg_node.__isset.grouping_exprs && !_tnode.agg_node.grouping_exprs.empty();
    bool could_local_shuffle = context->could_local_shuffle(ops_with_sink);

    auto try_interpolate_local_shuffle = [this, context](auto& ops) {
        return context->maybe_interpolate_local_shuffle_exchange(runtime_state(), id(), ops, [this]() {
            std::vector<ExprContext*> group_by_expr_ctxs;
            WARN_IF_ERROR(Expr::create_expr_trees(_pool, _tnode.agg_node.grouping_exprs, &group_by_expr_ctxs,
                                                  runtime_state()),
                          "create grouping expr failed");
            return group_by_expr_ctxs;
        });
    };

    if (!sorted_streaming_aggregate) {
        // 1. Finalize aggregation:
        //   - Without group by clause, it cannot be parallelized and need local passthough.
        //   - With group by clause, it can be parallelized and need local shuffle when could_local_shuffle is true.
        // 2. Non-finalize aggregation:
        //   - Without group by clause, it can be parallelized and needn't local shuffle.
        //   - With group by clause, it can be parallelized and need local shuffle when could_local_shuffle is true.
        if (agg_node.need_finalize) {
            if (!has_group_by_keys) {
                ops_with_sink =
                        context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), ops_with_sink);
            } else if (could_local_shuffle) {
                ops_with_sink = try_interpolate_local_shuffle(ops_with_sink);
            }
        } else {
            if (!has_group_by_keys) {
                // Do nothing.
            } else if (could_local_shuffle) {
                ops_with_sink = try_interpolate_local_shuffle(ops_with_sink);
            }
        }
    }

    use_per_bucket_optimize &= dynamic_cast<LocalExchangeSourceOperatorFactory*>(ops_with_sink.back().get()) == nullptr;

    OpFactories ops_with_source;
    if (sorted_streaming_aggregate) {
        ops_with_source =
                _decompose_to_pipeline<StreamingAggregatorFactory, SortedAggregateStreamingSourceOperatorFactory,
                                       SortedAggregateStreamingSinkOperatorFactory>(ops_with_sink, context, false);
    } else {
        if (runtime_state()->enable_spill() && runtime_state()->enable_agg_spill() && has_group_by_keys) {
            ops_with_source = _decompose_to_pipeline<AggregatorFactory, SpillableAggregateBlockingSourceOperatorFactory,
                                                     SpillableAggregateBlockingSinkOperatorFactory>(
                    ops_with_sink, context, use_per_bucket_optimize && has_group_by_keys);
        } else {
            ops_with_source = _decompose_to_pipeline<AggregatorFactory, AggregateBlockingSourceOperatorFactory,
                                                     AggregateBlockingSinkOperatorFactory>(
                    ops_with_sink, context, use_per_bucket_optimize && has_group_by_keys);
        }
    }

    // insert local shuffle after sorted streaming aggregate
    if (_tnode.agg_node.need_finalize && sorted_streaming_aggregate && could_local_shuffle && has_group_by_keys) {
        ops_with_source = try_interpolate_local_shuffle(ops_with_source);
    }

    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    if (!_tnode.conjuncts.empty() || ops_with_source.back()->has_runtime_filters()) {
        may_add_chunk_accumulate_operator(ops_with_source, context, id());
    }

    return ops_with_source;
}

} // namespace starrocks
