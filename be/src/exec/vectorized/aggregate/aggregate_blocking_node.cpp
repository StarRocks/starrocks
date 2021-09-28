// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/aggregate/aggregate_blocking_node.h"

#include "exec/pipeline/aggregate/aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/vectorized/aggregator.h"

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
    while (true) {
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

        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            if (!_aggregator->is_none_group_by_exprs()) {
                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                                          \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME)                       \
            _aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, chunk->num_rows());
                APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD

                RETURN_IF_ERROR(_aggregator->check_hash_map_memory_usage(state));
                _aggregator->try_convert_to_two_level_map();
            }
            if (_aggregator->is_none_group_by_exprs()) {
                _aggregator->compute_single_agg_state(chunk->num_rows());
            } else {
                _aggregator->compute_batch_agg_states(chunk->num_rows());
            }

            _aggregator->update_num_input_rows(chunk->num_rows());
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
    operators_with_sink = context->maybe_interpolate_local_exchange(operators_with_sink);

    // shared by sink operator and source operator
    AggregatorPtr aggregator = std::make_shared<Aggregator>(_tnode, child(0)->row_desc());

    operators_with_sink.emplace_back(
            std::make_shared<AggregateBlockingSinkOperatorFactory>(context->next_operator_id(), id(), aggregator));
    context->add_pipeline(operators_with_sink);

    OpFactories operators_with_source;
    auto source_operator =
            std::make_shared<AggregateBlockingSourceOperatorFactory>(context->next_operator_id(), id(), aggregator);

    // TODO(hcf) Currently, the shared data structure aggregator does not support concurrency.
    // So the degree of parallism must set to 1, we'll fix it later
    source_operator->set_degree_of_parallelism(1);
    operators_with_source.push_back(std::move(source_operator));
    return operators_with_source;
}

} // namespace starrocks::vectorized
