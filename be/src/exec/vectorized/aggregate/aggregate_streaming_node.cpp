// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/aggregate/aggregate_streaming_node.h"

#include "exec/pipeline/aggregate/aggregate_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_streaming_source_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "simd/simd.h"

namespace starrocks::vectorized {

Status AggregateStreamingNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBaseNode::prepare(state));
    _aggregator->set_aggr_phase(AggrPhase1);
    return Status::OK();
}

Status AggregateStreamingNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_aggregator->open(state));
    return _children[0]->open(state);
}

Status AggregateStreamingNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    *eos = false;

    if (_aggregator->is_ht_eos()) {
        COUNTER_SET(_aggregator->rows_returned_counter(), _aggregator->num_rows_returned());
        COUNTER_SET(_aggregator->pass_through_row_count(), _aggregator->num_pass_through_rows());
        *eos = true;
        return Status::OK();
    }

    if (*chunk != nullptr) {
        (*chunk)->reset();
    }

    // TODO: merge small chunks to large chunk for optimization
    while (!_child_eos) {
        ChunkPtr input_chunk;
        RETURN_IF_ERROR(_children[0]->get_next(state, &input_chunk, &_child_eos));
        if (!_child_eos) {
            if (input_chunk->is_empty()) {
                continue;
            }
            size_t input_chunk_size = input_chunk->num_rows();
            _aggregator->update_num_input_rows(input_chunk_size);
            COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());
            RETURN_IF_ERROR(_aggregator->check_hash_map_memory_usage(state));
            _aggregator->evaluate_exprs(input_chunk.get());

            if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
                // force execute streaming
                SCOPED_TIMER(_aggregator->streaming_timer());
                _aggregator->output_chunk_by_streaming(chunk);
                break;
            } else if (_aggregator->streaming_preaggregation_mode() ==
                       TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
                SCOPED_TIMER(_aggregator->agg_compute_timer());
                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                                          \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME)                       \
            _aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, input_chunk_size);
                APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                else {
                    DCHECK(false);
                }

                if (_aggregator->is_none_group_by_exprs()) {
                    _aggregator->compute_single_agg_state(input_chunk_size);
                } else {
                    _aggregator->compute_batch_agg_states(input_chunk_size);
                }

                _aggregator->try_convert_to_two_level_map();
                COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());

                continue;
            } else {
                // TODO: calc the real capacity of hashtable, will add one interface in the class of habletable
                size_t real_capacity =
                        _aggregator->hash_map_variant().capacity() - _aggregator->hash_map_variant().capacity() / 8;
                size_t remain_size = real_capacity - _aggregator->hash_map_variant().size();
                bool ht_needs_expansion = remain_size < input_chunk_size;
                if (!ht_needs_expansion ||
                    _aggregator->should_expand_preagg_hash_tables(_children[0]->rows_returned(), input_chunk_size,
                                                                  _aggregator->mem_pool()->total_allocated_bytes(),
                                                                  _aggregator->hash_map_variant().size())) {
                    // hash table is not full or allow expand the hash table according reduction rate
                    SCOPED_TIMER(_aggregator->agg_compute_timer());
                    if (false) {
                    }
#define HASH_MAP_METHOD(NAME)                                                                          \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME)                       \
            _aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, input_chunk_size);
                    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                    else {
                        DCHECK(false);
                    }

                    if (_aggregator->is_none_group_by_exprs()) {
                        _aggregator->compute_single_agg_state(input_chunk_size);
                    } else {
                        _aggregator->compute_batch_agg_states(input_chunk_size);
                    }

                    _aggregator->try_convert_to_two_level_map();
                    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());

                    continue;
                } else {
                    // TODO: direct call the function may affect the performance of some aggregated cases
                    {
                        SCOPED_TIMER(_aggregator->agg_compute_timer());
                        if (false) {
                        }
#define HASH_MAP_METHOD(NAME)                                                                                       \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME) _aggregator                        \
            ->build_hash_map_with_selection<typename decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, input_chunk_size);
                        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                        else {
                            DCHECK(false);
                        }
                    }

                    size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection());
                    if (zero_count == 0) {
                        SCOPED_TIMER(_aggregator->streaming_timer());
                        _aggregator->output_chunk_by_streaming(chunk);
                    } else if (zero_count == _aggregator->streaming_selection().size()) {
                        SCOPED_TIMER(_aggregator->agg_compute_timer());
                        _aggregator->compute_batch_agg_states(input_chunk_size);
                    } else {
                        {
                            SCOPED_TIMER(_aggregator->agg_compute_timer());
                            _aggregator->compute_batch_agg_states_with_selection(input_chunk_size);
                        }
                        {
                            SCOPED_TIMER(_aggregator->streaming_timer());
                            _aggregator->output_chunk_by_streaming_with_selection(chunk);
                        }
                    }

                    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
                    if ((*chunk)->num_rows() > 0) {
                        break;
                    } else {
                        continue;
                    }
                }
            }
        }
    }

    eval_join_runtime_filters(chunk->get());

    if (_child_eos) {
        if (_aggregator->is_ht_eos()) {
            COUNTER_SET(_aggregator->rows_returned_counter(), _aggregator->num_rows_returned());
            *eos = true;
            return Status::OK();
        }

        if (_aggregator->hash_map_variant().size() > 0) {
            // child has iterator over, and the hashtable has data
            _output_chunk_from_hash_map(chunk);
            *eos = false;
            _aggregator->process_limit(chunk);
            DCHECK_CHUNK(*chunk);
            return Status::OK();
        }

        COUNTER_SET(_aggregator->rows_returned_counter(), _aggregator->num_rows_returned());
        *eos = true;
        return Status::OK();
    }

    _aggregator->process_limit(chunk);

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void AggregateStreamingNode::_output_chunk_from_hash_map(ChunkPtr* chunk) {
    if (!_aggregator->it_hash().has_value()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                             \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME) _aggregator->it_hash() = \
            _aggregator->hash_map_variant().NAME->hash_map.begin();
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    }

    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                                     \
    else if (_aggregator->hash_map_variant().type == HashMapVariant::Type::NAME)                                  \
            _aggregator->convert_hash_map_to_chunk<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, config::vector_chunk_size, chunk);
    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }
}

std::vector<std::shared_ptr<pipeline::OperatorFactory> > AggregateStreamingNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators_with_sink = _children[0]->decompose_to_pipeline(context);
    context->maybe_interpolate_local_exchange(operators_with_sink);

    // shared by sink operator and source operator
    AggregatorPtr aggregator = std::make_shared<Aggregator>(_tnode, child(0)->row_desc());

    operators_with_sink.emplace_back(
            std::make_shared<AggregateStreamingSinkOperatorFactory>(context->next_operator_id(), id(), aggregator));
    context->add_pipeline(operators_with_sink);

    OpFactories operators_with_source;
    auto source_operator =
            std::make_shared<AggregateStreamingSourceOperatorFactory>(context->next_operator_id(), id(), aggregator);

    // TODO(hcf) Currently, the shared data structure aggregator does not support concurrency.
    // So the degree of parallism must set to 1, we'll fix it later
    source_operator->set_degree_of_parallelism(1);
    operators_with_source.push_back(std::move(source_operator));
    return operators_with_source;
}

} // namespace starrocks::vectorized
