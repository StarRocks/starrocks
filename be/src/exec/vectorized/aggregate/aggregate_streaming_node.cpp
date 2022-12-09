// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/aggregate/aggregate_streaming_node.h"

#include "exec/pipeline/aggregate/aggregate_streaming_sink_operator.h"
#include "exec/pipeline/aggregate/aggregate_streaming_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "runtime/current_thread.h"
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

#ifdef DEBUG
    static int loop = 0;
#endif

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
            RETURN_IF_ERROR(_aggregator->evaluate_exprs(input_chunk.get()));

            if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
                // force execute streaming
                SCOPED_TIMER(_aggregator->streaming_timer());
                _aggregator->output_chunk_by_streaming(chunk);
                break;
            } else if (_aggregator->streaming_preaggregation_mode() ==
                       TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
                RETURN_IF_ERROR(state->check_mem_limit("AggrNode"));
                SCOPED_TIMER(_aggregator->agg_compute_timer());
                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                                                          \
    else if (_aggregator->hash_map_variant().type == AggHashMapVariant::Type::NAME) {                                  \
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                *_aggregator->hash_map_variant().NAME, input_chunk_size));                                             \
    }
                APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                else {
                    DCHECK(false);
                }

                if (_aggregator->is_none_group_by_exprs()) {
                    _aggregator->compute_single_agg_state(input_chunk_size);
                } else {
                    _aggregator->compute_batch_agg_states(input_chunk_size);
                }

                _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
                TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());

                COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());

                continue;
            } else {
                // TODO: calc the real capacity of hashtable, will add one interface in the class of habletable
                size_t real_capacity =
                        _aggregator->hash_map_variant().capacity() - _aggregator->hash_map_variant().capacity() / 8;
                size_t remain_size = real_capacity - _aggregator->hash_map_variant().size();
                [[maybe_unused]] bool ht_needs_expansion = remain_size < input_chunk_size;

#ifdef DEBUG
                // chaos test for streaming or agg, The results have to be consistent
                // when group by type of double, it maybe cause dissonant result because of precision loss for double
                // thus, so check case will fail, so it only work under DEBUG mode
                loop++;
                if (loop % 2 == 0) {
#else
                if (!ht_needs_expansion ||
                    _aggregator->should_expand_preagg_hash_tables(_children[0]->rows_returned(), input_chunk_size,
                                                                  _aggregator->mem_pool()->total_allocated_bytes(),
                                                                  _aggregator->hash_map_variant().size())) {
#endif
                    RETURN_IF_ERROR(state->check_mem_limit("AggrNode"));
                    // hash table is not full or allow expand the hash table according reduction rate
                    SCOPED_TIMER(_aggregator->agg_compute_timer());
                    if (false) {
                    }
#define HASH_MAP_METHOD(NAME)                                                                                          \
    else if (_aggregator->hash_map_variant().type == AggHashMapVariant::Type::NAME) {                                  \
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                *_aggregator->hash_map_variant().NAME, input_chunk_size));                                             \
    }
                    APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                    else {
                        DCHECK(false);
                    }

                    if (_aggregator->is_none_group_by_exprs()) {
                        _aggregator->compute_single_agg_state(input_chunk_size);
                    } else {
                        _aggregator->compute_batch_agg_states(input_chunk_size);
                    }

                    _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
                    TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());
                    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
                    continue;
                } else {
                    // TODO: direct call the function may affect the performance of some aggregated cases
                    {
                        SCOPED_TIMER(_aggregator->agg_compute_timer());
                        if (false) {
                        }
#define HASH_MAP_METHOD(NAME)                                                             \
    else if (_aggregator->hash_map_variant().type == AggHashMapVariant::Type::NAME) {     \
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map_with_selection<typename decltype( \
                                    _aggregator->hash_map_variant().NAME)::element_type>( \
                *_aggregator->hash_map_variant().NAME, input_chunk_size));                \
    }
                        APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
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
                    if (*chunk != nullptr && (*chunk)->num_rows() > 0) {
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
            RETURN_IF_ERROR(_output_chunk_from_hash_map(chunk));
            *eos = false;
            _aggregator->process_limit(chunk);
            DCHECK_CHUNK(*chunk);
            return Status::OK();
        }

        COUNTER_SET(_aggregator->rows_returned_counter(), _aggregator->num_rows_returned());
        *eos = true;
        RETURN_IF_ERROR(_aggregator->check_has_error());
        return Status::OK();
    }

    _aggregator->process_limit(chunk);
    RETURN_IF_ERROR(_aggregator->check_has_error());

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

Status AggregateStreamingNode::_output_chunk_from_hash_map(ChunkPtr* chunk) {
    if (!_aggregator->it_hash().has_value()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                                \
    else if (_aggregator->hash_map_variant().type == AggHashMapVariant::Type::NAME) _aggregator->it_hash() = \
            _aggregator->_state_allocator.begin();
        APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    }

    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                                     \
    else if (_aggregator->hash_map_variant().type == AggHashMapVariant::Type::NAME) RETURN_IF_ERROR(              \
            _aggregator->convert_hash_map_to_chunk<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, runtime_state()->chunk_size(), chunk));
    APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }
    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory> > AggregateStreamingNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators_with_sink = _children[0]->decompose_to_pipeline(context);
    // We cannot get degree of parallelism from PipelineBuilderContext, of which is only a suggest value
    // and we may set other parallelism for source operator in many special cases
    size_t degree_of_parallelism =
            down_cast<SourceOperatorFactory*>(operators_with_sink[0].get())->degree_of_parallelism();

    // shared by sink operator factory and source operator factory
    AggregatorFactoryPtr aggregator_factory = std::make_shared<AggregatorFactory>(_tnode);

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(2, std::move(this->runtime_filter_collector()));
    auto sink_operator = std::make_shared<AggregateStreamingSinkOperatorFactory>(context->next_operator_id(), id(),
                                                                                 aggregator_factory);
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(sink_operator.get(), context, rc_rf_probe_collector);
    operators_with_sink.emplace_back(sink_operator);
    context->add_pipeline(operators_with_sink);

    OpFactories operators_with_source;
    auto source_operator = std::make_shared<AggregateStreamingSourceOperatorFactory>(context->next_operator_id(), id(),
                                                                                     aggregator_factory);
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(source_operator.get(), context, rc_rf_probe_collector);

    // Aggregator must be used by a pair of sink and source operators,
    // so operators_with_source's degree of parallelism must be equal with operators_with_sink's
    source_operator->set_degree_of_parallelism(degree_of_parallelism);
    operators_with_source.push_back(std::move(source_operator));
    if (limit() != -1) {
        operators_with_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operators_with_source;
}

} // namespace starrocks::vectorized
