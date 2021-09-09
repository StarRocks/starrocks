// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/aggregate/distinct_streaming_node.h"

#include "simd/simd.h"

namespace starrocks::vectorized {

Status DistinctStreamingNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_group_by_expr_ctxs, state));
    RETURN_IF_ERROR(_children[0]->open(state));
    return Status::OK();
}

Status DistinctStreamingNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    *eos = false;

    if (_is_finished) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        COUNTER_SET(_pass_through_row_count, _num_pass_through_rows);
        *eos = true;
        return Status::OK();
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
            _num_input_rows += input_chunk_size;
            COUNTER_SET(_input_row_count, _num_input_rows);
            RETURN_IF_ERROR(_check_hash_set_memory_usage(state));
            _evaluate_exprs(input_chunk.get());

            if (_streaming_preaggregation_mode == TStreamingPreaggregationMode::FORCE_STREAMING) {
                // force execute streaming
                SCOPED_TIMER(_streaming_timer);
                _output_chunk_by_streaming(chunk);
                break;
            } else if (_streaming_preaggregation_mode == TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
                SCOPED_TIMER(_agg_compute_timer);

                if (false) {
                }
#define HASH_MAP_METHOD(NAME)                                                                        \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME)                                   \
            _build_hash_set<decltype(_hash_set_variant.NAME)::element_type>(*_hash_set_variant.NAME, \
                                                                            input_chunk_size);
                APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                else {
                    DCHECK(false);
                }

                (this->*_compute_agg_states)(input_chunk_size);
                COUNTER_SET(_hash_table_size, (int64_t)_hash_set_variant.size());
                continue;
            } else {
                // TODO: calc the real capacity of hashtable, will add one interface in the class of habletable
                size_t real_capacity = _hash_set_variant.capacity() - _hash_set_variant.capacity() / 8;
                size_t remain_size = real_capacity - _hash_set_variant.size();
                bool ht_needs_expansion = remain_size < input_chunk_size;
                if (!ht_needs_expansion ||
                    _should_expand_preagg_hash_tables(input_chunk_size, _mem_pool->total_allocated_bytes(),
                                                      _hash_set_variant.size())) {
                    // hash table is not full or allow expand the hash table according reduction rate
                    SCOPED_TIMER(_agg_compute_timer);

                    if (false) {
                    }
#define HASH_MAP_METHOD(NAME)                                                                        \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME)                                   \
            _build_hash_set<decltype(_hash_set_variant.NAME)::element_type>(*_hash_set_variant.NAME, \
                                                                            input_chunk_size);
                    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                    else {
                        DCHECK(false);
                    }

                    (this->*_compute_agg_states)(input_chunk_size);
                    COUNTER_SET(_hash_table_size, (int64_t)_hash_set_variant.size());
                    continue;
                } else {
                    {
                        SCOPED_TIMER(_agg_compute_timer);
                        if (false) {
                        }
#define HASH_MAP_METHOD(NAME)                                                         \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME)                    \
            _build_hash_set<typename decltype(_hash_set_variant.NAME)::element_type>( \
                    *_hash_set_variant.NAME, input_chunk_size, &_streaming_selection);
                        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
                        else {
                            DCHECK(false);
                        }
                    }

                    {
                        SCOPED_TIMER(_streaming_timer);
                        size_t zero_count = SIMD::count_zero(_streaming_selection);
                        if (zero_count == 0) {
                            _output_chunk_by_streaming(chunk);
                        } else if (zero_count != _streaming_selection.size()) {
                            _output_chunk_by_streaming(chunk, _streaming_selection);
                        } else {
                            // do nothing
                        }
                    }

                    COUNTER_SET(_hash_table_size, (int64_t)_hash_set_variant.size());
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
        if (!_hash_table_eos && _hash_set_variant.size() > 0) {
            _output_chunk_from_hash_set(chunk);
            *eos = false;
            _process_limit(chunk);

            DCHECK_CHUNK(*chunk);
            return Status::OK();
        } else if (_hash_set_variant.size() == 0) {
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            COUNTER_SET(_pass_through_row_count, _num_pass_through_rows);
            *eos = true;
            return Status::OK();
        }
    }

    _process_limit(chunk);
    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void DistinctStreamingNode::_output_chunk_from_hash_set(ChunkPtr* chunk) {
    if (!_it_hash.has_value()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME) \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME) _it_hash = _hash_set_variant.NAME->hash_set.begin();
        APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
        else {
            DCHECK(false);
        }
        COUNTER_SET(_hash_table_size, (int64_t)_hash_set_variant.size());
    }

    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                           \
    else if (_hash_set_variant.type == HashSetVariant::Type::NAME)                      \
            _convert_hash_set_to_chunk<decltype(_hash_set_variant.NAME)::element_type>( \
                    *_hash_set_variant.NAME, config::vector_chunk_size, chunk);
    APPLY_FOR_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }
}

} // namespace starrocks::vectorized