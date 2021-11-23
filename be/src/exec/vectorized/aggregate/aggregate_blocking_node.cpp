// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/aggregate/aggregate_blocking_node.h"

#include "exec/pipeline/aggregate/aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/vectorized/aggregator.h"
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

        DCHECK_LE(chunk->num_rows(), config::vector_chunk_size);

        _aggregator->evaluate_exprs(chunk.get());

        size_t chunk_size = chunk->num_rows();
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            if (!_aggregator->is_none_group_by_exprs()) {
                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                                          \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME)                       \
            _aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, chunk_size, agg_group_by_with_limit);
                APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD

                _mem_tracker->set(_aggregator->hash_map_variant().memory_usage() +
                                  _aggregator->mem_pool()->total_reserved_bytes());
                _aggregator->try_convert_to_two_level_map();
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
    }

    if (!_aggregator->is_none_group_by_exprs()) {
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
        // If hash map is empty, we don't need to return value
        if (_aggregator->hash_map_variant().size() == 0) {
            _aggregator->set_ht_eos();
        }

        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                             \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME) _aggregator->it_hash() = \
            _aggregator->hash_map_variant().NAME->hash_map.begin();
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    } else if (_aggregator->is_none_group_by_exprs()) {
        // for aggregate no group by, if _num_input_rows is 0,
        // In update phase, we directly return empty chunk.
        // In merge phase, we will handle it.
        if (_aggregator->num_input_rows() == 0 && !_aggregator->needs_finalize()) {
            _aggregator->set_ht_eos();
        }
    }
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    _mem_tracker->set(_aggregator->hash_map_variant().memory_usage() + _aggregator->mem_pool()->total_reserved_bytes());

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
    int32_t chunk_size = config::vector_chunk_size;

    if (_aggregator->is_none_group_by_exprs()) {
        SCOPED_TIMER(_aggregator->get_results_timer());
        _aggregator->convert_to_chunk_no_groupby(chunk);
    } else {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                     \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME)                                  \
            _aggregator->convert_hash_map_to_chunk<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, chunk_size, chunk);
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    }

    eval_join_runtime_filters(chunk->get());

    // For having
    size_t old_size = (*chunk)->num_rows();
    ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get());
    _aggregator->update_num_rows_returned(-(old_size - (*chunk)->num_rows()));

    _aggregator->process_limit(chunk);

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory> > AggregateBlockingNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators_with_sink = _children[0]->decompose_to_pipeline(context);
    auto& agg_node = _tnode.agg_node;
    if (agg_node.need_finalize) {
        // If finalize aggregate with group by clause, then it can be paralized
        if (agg_node.__isset.grouping_exprs && !_tnode.agg_node.grouping_exprs.empty()) {
            std::vector<ExprContext*> group_by_expr_ctxs;
            Expr::create_expr_trees(_pool, _tnode.agg_node.grouping_exprs, &group_by_expr_ctxs);
            operators_with_sink =
                    context->maybe_interpolate_local_shuffle_exchange(operators_with_sink, group_by_expr_ctxs);
        } else {
            operators_with_sink = context->maybe_interpolate_local_passthrough_exchange(operators_with_sink);
        }
    }
    // We cannot get degree of parallelism from PipelineBuilderContext, of which is only a suggest value
    // and we may set other parallelism for source operator in many special cases
    size_t degree_of_parallelism =
            down_cast<SourceOperatorFactory*>(operators_with_sink[0].get())->degree_of_parallelism();

    // shared by sink operator and source operator
    AggregatorFactoryPtr aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);

    auto sink_operator = std::make_shared<AggregateBlockingSinkOperatorFactory>(context->next_operator_id(), id(),
                                                                                aggregator_factory);
    operators_with_sink.push_back(std::move(sink_operator));
    context->add_pipeline(operators_with_sink);

    OpFactories operators_with_source;
    auto source_operator = std::make_shared<AggregateBlockingSourceOperatorFactory>(context->next_operator_id(), id(),
                                                                                    aggregator_factory);

    // Aggregator must be used by a pair of sink and source operators,
    // so operators_with_source's degree of parallelism must be equal with operators_with_sink's
    source_operator->set_degree_of_parallelism(degree_of_parallelism);
    operators_with_source.push_back(std::move(source_operator));
    return operators_with_source;
}

} // namespace starrocks::vectorized
