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

#include "aggregate_streaming_sink_operator.h"

#include <variant>

#include "column/vectorized_fwd.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"
namespace starrocks::pipeline {

Status AggregateStreamingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    return _aggregator->open(state);
}

void AggregateStreamingSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateStreamingSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_aggregator->hash_map_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<ChunkPtr> AggregateStreamingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status AggregateStreamingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();
    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));

    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
        RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk, chunk->num_rows()));
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::AUTO_NEW) {
        RETURN_IF_ERROR(_push_chunk_by_auto_new(chunk, chunk->num_rows()));
    } else {
        RETURN_IF_ERROR(_push_chunk_by_auto(chunk, chunk->num_rows()));
    }
    RETURN_IF_ERROR(_aggregator->check_has_error());
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_force_streaming(ChunkPtr chunk) {
    SCOPED_TIMER(_aggregator->streaming_timer());
    ChunkPtr res = std::make_shared<Chunk>();
    RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
    _aggregator->offer_chunk_to_buffer(std::move(res));
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_force_preaggregation(ChunkPtr chunk, const size_t chunk_size) {
    SCOPED_TIMER(_aggregator->agg_compute_timer());
    TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map(chunk_size));
    if (_aggregator->is_none_group_by_exprs()) {
        RETURN_IF_ERROR(_aggregator->compute_single_agg_state(chunk.get(), chunk_size));
    } else {
        RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
    }

    _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
    TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_auto(const size_t chunk_size) {
    // TODO: calc the real capacity of hashtable, will add one interface in the class of habletable
    size_t real_capacity = _aggregator->hash_map_variant().capacity() - _aggregator->hash_map_variant().capacity() / 8;
    size_t remain_size = real_capacity - _aggregator->hash_map_variant().size();
    bool ht_needs_expansion = remain_size < chunk_size;
    size_t allocated_bytes = _aggregator->hash_map_variant().allocated_memory_usage(_aggregator->mem_pool());
    if (!ht_needs_expansion ||
        _aggregator->should_expand_preagg_hash_tables(_aggregator->num_input_rows(), chunk_size, allocated_bytes,
                                                      _aggregator->hash_map_variant().size())) {
        // hash table is not full or allow expand the hash table according reduction rate
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map(chunk_size));
        if (_aggregator->is_none_group_by_exprs()) {
            _aggregator->compute_single_agg_state(chunk_size);
        } else {
            _aggregator->compute_batch_agg_states(chunk_size);
        }

        _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
        TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());

        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    } else {
        RETURN_IF_ERROR(_push_chunk_by_selective_preaggregation(chunk_size, true));
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    }
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_selective_preaggregation(const size_t chunk_size,
                                                                               bool need_build) {
    if (need_build) {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map_with_selection(chunk_size));
    }

    size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection());
    // very poor aggregation
    if (zero_count == 0) {
        SCOPED_TIMER(_aggregator->streaming_timer());
        ChunkPtr chunk = std::make_shared<Chunk>();
        _aggregator->output_chunk_by_streaming(&chunk);
        _aggregator->offer_chunk_to_buffer(chunk);
    }
    // very high aggregation
    else if (zero_count == _aggregator->streaming_selection().size()) {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        _aggregator->compute_batch_agg_states(chunk_size);
    } else {
        // middle cases, first aggregate locally and output by stream
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            _aggregator->compute_batch_agg_states_with_selection(chunk_size);
        }
        {
            SCOPED_TIMER(_aggregator->streaming_timer());
            ChunkPtr chunk = std::make_shared<Chunk>();
            _aggregator->output_chunk_by_streaming_with_selection(&chunk);
            _aggregator->offer_chunk_to_buffer(chunk);
        }
    }
    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_auto_new(ChunkPtr chunk, const size_t chunk_size) {
    size_t allocated_bytes = _aggregator->hash_map_variant().allocated_memory_usage(_aggregator->mem_pool());
    const size_t continuous_limit = _auto_context.get_continuous_limit();
    switch (_auto_state) {
    case AggrAutoState::INIT_BUILD: {
        size_t real_capacity =
                _aggregator->hash_map_variant().capacity() - _aggregator->hash_map_variant().capacity() / 8;
        size_t remain_size = real_capacity - _aggregator->hash_map_variant().size();
        bool ht_needs_expansion = remain_size < chunk_size;
        _auto_context.init_build_count++;
        if (!ht_needs_expansion ||
            _aggregator->should_expand_preagg_hash_tables(_aggregator->num_input_rows(), chunk_size, allocated_bytes,
                                                          _aggregator->hash_map_variant().size())) {
            // hash table is not full or allow expand the hash table according reduction rate
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map(chunk_size));
            if (_aggregator->is_none_group_by_exprs()) {
                _aggregator->compute_single_agg_state(chunk.get(), chunk_size);
            } else {
                _aggregator->compute_batch_agg_states(chunk.get(), chunk_size);
            }

            _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
            TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());
            COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
            break;
        } else {
            _auto_state = AggrAutoState::ADJUST;
            _auto_context.adjust_count = 0;
            LOG(INFO) << "auto agg: " << _auto_context.get_auto_state_string(AggrAutoState::INIT_BUILD) << " "
                      << _auto_context.init_build_count << " -> " << _auto_context.get_auto_state_string(_auto_state);
        }
    }
    case AggrAutoState::ADJUST: {
        _auto_context.adjust_count++;
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map_with_selection(chunk_size));
        }

        size_t agg_count = SIMD::count_zero(_aggregator->streaming_selection());
        if (_auto_context.adjust_count < continuous_limit && _auto_context.low_reduction(agg_count, chunk_size)) {
            RETURN_IF_ERROR(_push_chunk_by_force_streaming());
            _auto_context.pass_through_count++;
            _auto_context.build_count = 0;
            _auto_context.selective_build_count = 0;
            if (_auto_context.pass_through_count == AggrAutoContext::AdjustLimit) {
                _auto_state = AggrAutoState::PASS_THROUGH;
                LOG(INFO) << "auto agg: continuous " << AggrAutoContext::AdjustLimit << " low reduction "
                          << agg_count * 1.0 / chunk_size << " "
                          << _auto_context.get_auto_state_string(AggrAutoState::ADJUST) << " -> "
                          << _auto_context.get_auto_state_string(_auto_state);
            }

        } else if (_auto_context.adjust_count < continuous_limit &&
                   _auto_context.high_reduction(agg_count, chunk_size) &&
                   allocated_bytes < AggrAutoContext::MaxHtSize) {
            //TODO rebuilding is OK?
            RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk_size));

            _auto_context.build_count++;
            _auto_context.pass_through_count = 0;
            _auto_context.selective_build_count = 0;
            if (_auto_context.build_count == AggrAutoContext::AdjustLimit) {
                _auto_state = AggrAutoState::BUILD;
                LOG(INFO) << "auto agg: continuous " << AggrAutoContext::AdjustLimit << " high reduction "
                          << agg_count * 1.0 / chunk_size << " "
                          << _auto_context.get_auto_state_string(AggrAutoState::ADJUST) << " -> "
                          << _auto_context.get_auto_state_string(_auto_state);
            }
        } else {
            RETURN_IF_ERROR(_push_chunk_by_selective_preaggregation(chunk_size, false));
            _auto_context.selective_build_count++;
            _auto_context.pass_through_count = 0;
            _auto_context.build_count = 0;
            if (_auto_context.selective_build_count == AggrAutoContext::AdjustLimit) {
                _auto_state = AggrAutoState::SELECTIVE_BUILD;
                LOG(INFO) << "auto agg: continuous " << AggrAutoContext::AdjustLimit << " "
                          << _auto_context.get_auto_state_string(AggrAutoState::ADJUST)
                          << _auto_context.get_auto_state_string(_auto_state);
            }
        }
        break;
    }
    case AggrAutoState::PASS_THROUGH: {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming());
        _auto_context.pass_through_count++;
        if (_auto_context.pass_through_count > continuous_limit) {
            _auto_state =
                    allocated_bytes < AggrAutoContext::MaxHtSize ? AggrAutoState::FORCE_BUILD : AggrAutoState::ADJUST;
            _auto_context.pass_through_count = 0;
            _auto_context.build_count = 0;
            _auto_context.adjust_count = 0;

            LOG(INFO) << "auto agg: continuous " << continuous_limit << " "
                      << _auto_context.get_auto_state_string(AggrAutoState::PASS_THROUGH) << " -> "
                      << _auto_context.get_auto_state_string(_auto_state);
            _auto_context.update_continuous_limit();
        }
        break;
    }
    case AggrAutoState::FORCE_BUILD:
    case AggrAutoState::BUILD: {
        RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk_size));
        _auto_context.build_count++;
        if (_auto_state == AggrAutoState::FORCE_BUILD) {
            if (_auto_context.build_count > AggrAutoContext::BuildLimit) {
                _auto_state = AggrAutoState::ADJUST;
                _auto_context.build_count = 0;
                _auto_context.adjust_count = 0;
                LOG(INFO) << "auto agg: continuous " << AggrAutoContext::BuildLimit << " "
                          << _auto_context.get_auto_state_string(AggrAutoState::FORCE_BUILD) << " -> "
                          << _auto_context.get_auto_state_string(_auto_state);
            }
        } else {
            if (_auto_context.build_count > AggrAutoContext::AdjustLimit + AggrAutoContext::BuildLimit) {
                _auto_state = AggrAutoState::ADJUST;
                _auto_context.build_count = 0;
                _auto_context.adjust_count = 0;
                LOG(INFO) << "auto agg: continuous " << AggrAutoContext::AdjustLimit + AggrAutoContext::BuildLimit
                          << " " << _auto_context.get_auto_state_string(AggrAutoState::BUILD) << " -> "
                          << _auto_context.get_auto_state_string(_auto_state);
            }
        }
        break;
    }
    case AggrAutoState::SELECTIVE_BUILD: {
        RETURN_IF_ERROR(_push_chunk_by_selective_preaggregation(chunk_size, true));
        _auto_context.selective_build_count++;
        if (_auto_context.selective_build_count > continuous_limit) {
            _auto_state = AggrAutoState::ADJUST;
            _auto_context.selective_build_count = 0;
            _auto_context.adjust_count = 0;
            LOG(INFO) << "auto agg: continuous " << continuous_limit << " "
                      << _auto_context.get_auto_state_string(AggrAutoState::SELECTIVE_BUILD) << " -> "
                      << _auto_context.get_auto_state_string(_auto_state);
            _auto_context.update_continuous_limit();
        }
        break;
    }
    }
    return Status::OK();
}

Status AggregateStreamingSinkOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    return _aggregator->reset_state(state, refill_chunks, this);
}
} // namespace starrocks::pipeline
