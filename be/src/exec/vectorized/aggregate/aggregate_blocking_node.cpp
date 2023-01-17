// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/aggregate/aggregate_blocking_node.h"

#include <type_traits>
#include <variant>

#include "exec/pipeline/aggregate/aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/pipeline/aggregate/aggregate_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_streaming_source_operator.h"
#include "exec/pipeline/aggregate/sorted_aggregate_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/sorted_aggregate_streaming_source_operator.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/vectorized/aggregator.h"
#include "exec/vectorized/sorted_streaming_aggregator.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks::vectorized {

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

        RETURN_IF_ERROR(_aggregator->evaluate_exprs(chunk.get()));

        size_t chunk_size = chunk->num_rows();
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            if (!_aggregator->is_none_group_by_exprs()) {
                TRY_CATCH_ALLOC_SCOPE_START()
                _aggregator->build_hash_map(chunk_size, agg_group_by_with_limit);
                _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));

                _aggregator->try_convert_to_two_level_map();
                TRY_CATCH_ALLOC_SCOPE_END()
            }
            if (_aggregator->is_none_group_by_exprs()) {
                _aggregator->compute_single_agg_state(chunk_size);
            } else {
                if (agg_group_by_with_limit) {
                    // use `_aggregator->streaming_selection()` here to mark whether needs to filter key when compute agg states,
                    // it's generated in `build_hash_map`
                    size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection().data(), chunk_size);
                    if (zero_count == chunk_size) {
                        _aggregator->compute_batch_agg_states(chunk_size);
                    } else {
                        _aggregator->compute_batch_agg_states_with_selection(chunk_size);
                    }
                } else {
                    _aggregator->compute_batch_agg_states(chunk_size);
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
    int32_t chunk_size = runtime_state()->chunk_size();

    if (_aggregator->is_none_group_by_exprs()) {
        RETURN_IF_ERROR(_aggregator->convert_to_chunk_no_groupby(chunk));
    } else {
        RETURN_IF_ERROR(_aggregator->convert_hash_map_to_chunk(chunk_size, chunk));
    }

    size_t old_size = (*chunk)->num_rows();
    eval_join_runtime_filters(chunk->get());

    // For having
    RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get()));
    _aggregator->update_num_rows_returned(-(old_size - (*chunk)->num_rows()));

    _aggregator->process_limit(chunk);

    DCHECK_CHUNK(*chunk);

    RETURN_IF_ERROR(_aggregator->check_has_error());

    return Status::OK();
}

template <class AggFactory, class SourceFactory, class SinkFactory>
std::vector<std::shared_ptr<pipeline::OperatorFactory>> AggregateBlockingNode::_decompose_to_pipeline(
        std::vector<std::shared_ptr<pipeline::OperatorFactory>>& ops_with_sink,
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    // create aggregator factory
    // shared by sink operator and source operator
    auto aggregator_factory = std::make_shared<AggFactory>(_tnode);

    // We cannot get degree of parallelism from PipelineBuilderContext, of which is only a suggest value
    // and we may set other parallelism for source operator in many special cases
    size_t degree_of_parallelism = down_cast<SourceOperatorFactory*>(ops_with_sink[0].get())->degree_of_parallelism();

    auto should_cache = context->should_interpolate_cache_operator(ops_with_sink[0], id());
    bool could_local_shuffle = !should_cache && context->could_local_shuffle(ops_with_sink);
    auto operators_generator = [this, should_cache, could_local_shuffle, &context](bool post_cache) {
        // shared by sink operator and source operator
        auto aggregator_factory = std::make_shared<AggFactory>(_tnode);
        AggrMode aggr_mode = should_cache ? (post_cache ? AM_BLOCKING_POST_CACHE : AM_BLOCKING_PRE_CACHE) : AM_DEFAULT;
        aggregator_factory->set_aggr_mode(aggr_mode);
        auto sink_operator = std::make_shared<SinkFactory>(context->next_operator_id(), id(), aggregator_factory);
        auto source_operator = std::make_shared<SourceFactory>(context->next_operator_id(), id(), aggregator_factory);
        source_operator->set_could_local_shuffle(could_local_shuffle);
        return std::tuple<OpFactoryPtr, SourceOperatorFactoryPtr>(sink_operator, source_operator);
    };

    auto operators = operators_generator(false);
    auto sink_operator = std::move(std::get<0>(operators));
    auto source_operator = std::move(std::get<1>(operators));
    // Create a shared RefCountedRuntimeFilterCollector
    // Initialize OperatorFactory's fields involving runtime filters.
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    this->init_runtime_filter_for_operator(sink_operator.get(), context, rc_rf_probe_collector);
    ops_with_sink.push_back(std::move(sink_operator));

    OpFactories ops_with_source;
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(source_operator.get(), context, rc_rf_probe_collector);
    // Aggregator must be used by a pair of sink and source operators,
    // so ops_with_source's degree of parallelism must be equal with operators_with_sink's
    source_operator->set_degree_of_parallelism(degree_of_parallelism);

    source_operator->set_could_local_shuffle(
            down_cast<pipeline::SourceOperatorFactory*>(ops_with_sink[0].get())->could_local_shuffle());

    ops_with_source.push_back(std::move(source_operator));
    if (should_cache) {
        ops_with_source = context->interpolate_cache_operator(ops_with_sink, ops_with_source, operators_generator);
    }
    context->add_pipeline(ops_with_sink);

    return ops_with_source;
}

std::vector<std::shared_ptr<pipeline::OperatorFactory>> AggregateBlockingNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    context->has_aggregation = true;
    OpFactories ops_with_sink = _children[0]->decompose_to_pipeline(context);
    auto& agg_node = _tnode.agg_node;

    bool sorted_streaming_aggregate = _tnode.agg_node.__isset.use_sort_agg && _tnode.agg_node.use_sort_agg;
    bool has_group_by_keys = agg_node.__isset.grouping_exprs && !_tnode.agg_node.grouping_exprs.empty();
    bool could_local_shuffle = context->could_local_shuffle(ops_with_sink);

    auto try_interpolate_local_shuffle = [this, context](auto& ops) {
        std::vector<ExprContext*> group_by_expr_ctxs;
        Expr::create_expr_trees(_pool, _tnode.agg_node.grouping_exprs, &group_by_expr_ctxs);
        Expr::prepare(group_by_expr_ctxs, runtime_state());
        Expr::open(group_by_expr_ctxs, runtime_state());
        return context->maybe_interpolate_local_shuffle_exchange(runtime_state(), ops, group_by_expr_ctxs);
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
                ops_with_sink = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), ops_with_sink);
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

    OpFactories ops_with_source;
    if (sorted_streaming_aggregate) {
        ops_with_source =
                _decompose_to_pipeline<StreamingAggregatorFactory, SortedAggregateStreamingSourceOperatorFactory,
                                       SortedAggregateStreamingSinkOperatorFactory>(ops_with_sink, context);
    } else {
        ops_with_source = _decompose_to_pipeline<AggregatorFactory, AggregateBlockingSourceOperatorFactory,
                                                 AggregateBlockingSinkOperatorFactory>(ops_with_sink, context);
    }

    // insert local shuffle after sorted streaming aggregate
    if (_tnode.agg_node.need_finalize && sorted_streaming_aggregate && could_local_shuffle && has_group_by_keys) {
        ops_with_source = try_interpolate_local_shuffle(ops_with_source);
    }

    if (!_tnode.conjuncts.empty() || ops_with_source.back()->has_runtime_filters()) {
        ops_with_source.emplace_back(
                std::make_shared<ChunkAccumulateOperatorFactory>(context->next_operator_id(), id()));
    }

    if (limit() != -1) {
        ops_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return ops_with_source;
}

} // namespace starrocks::vectorized
