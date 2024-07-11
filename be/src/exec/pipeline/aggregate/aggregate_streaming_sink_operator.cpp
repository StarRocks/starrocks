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
#include "common/config.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"
namespace starrocks::pipeline {

Status AggregateStreamingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::LIMITED_MEM) {
        _limited_mem_state.limited_memory_size = config::streaming_agg_limited_memory_size;
    }
    return _aggregator->open(state);
}

void AggregateStreamingSinkOperator::close(RuntimeState* state) {
    auto* counter = ADD_COUNTER(_unique_metrics, "HashTableMemoryUsage", TUnit::BYTES);
    counter->set(_aggregator->hash_map_memory_usage());
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateStreamingSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    // skip processing if cancelled
    if (state->is_cancelled()) {
        return Status::OK();
    }

    if (_aggregator->hash_map_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<ChunkPtr> AggregateStreamingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

void AggregateStreamingSinkOperator::set_execute_mode(int performance_level) {
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::AUTO) {
        _aggregator->streaming_preaggregation_mode() = TStreamingPreaggregationMode::LIMITED_MEM;
    }
    _limited_mem_state.limited_memory_size = _aggregator->hash_map_memory_usage();
}

Status AggregateStreamingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DeferOp update_revocable_bytes{[this]() {
        set_revocable_mem_bytes(_aggregator->hash_map_variant().allocated_memory_usage(_aggregator->mem_pool()));
    }};
    size_t chunk_size = chunk->num_rows();
    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());

    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
        RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk, chunk->num_rows()));
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::LIMITED_MEM) {
        RETURN_IF_ERROR(_push_chunk_by_limited_memory(chunk, chunk_size));
    } else {
        RETURN_IF_ERROR(_push_chunk_by_auto(chunk, chunk->num_rows()));
    }
    RETURN_IF_ERROR(_aggregator->check_has_error());
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_force_streaming(const ChunkPtr& chunk) {
    SCOPED_TIMER(_aggregator->streaming_timer());
    ChunkPtr res = std::make_shared<Chunk>();
    RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
    _aggregator->offer_chunk_to_buffer(std::move(res));
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_force_preaggregation(const ChunkPtr& chunk,
                                                                           const size_t chunk_size) {
    SCOPED_TIMER(_aggregator->agg_compute_timer());
    TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map(chunk_size));
    if (_aggregator->is_none_group_by_exprs()) {
        RETURN_IF_ERROR(_aggregator->compute_single_agg_state(chunk.get(), chunk_size));
    } else {
        RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
    }

    TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_selective_preaggregation(const ChunkPtr& chunk,
                                                                               const size_t chunk_size,
                                                                               bool need_build) {
    if (need_build) {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map_with_selection(chunk_size));
    }

    size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection());
    // very poor aggregation
    if (zero_count == 0) {
        SCOPED_TIMER(_aggregator->streaming_timer());
        ChunkPtr res = std::make_shared<Chunk>();
        RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
        _aggregator->offer_chunk_to_buffer(res);
    }
    // very high aggregation
    else if (zero_count == _aggregator->streaming_selection().size()) {
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
    } else {
        // middle cases, first aggregate locally and output by stream
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            RETURN_IF_ERROR(_aggregator->compute_batch_agg_states_with_selection(chunk.get(), chunk_size));
        }
        {
            SCOPED_TIMER(_aggregator->streaming_timer());
            ChunkPtr res = std::make_shared<Chunk>();
            RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming_with_selection(chunk.get(), &res));
            _aggregator->offer_chunk_to_buffer(res);
        }
    }
    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    return Status::OK();
}

/* A state machine autoly chooses different preaggregation modes. If the initial preaggregation cannot insert
 * more data into hash table, the state shifts from INIT_PREAGG to ADJUST. The ADJUST state has 3 branches:
 * (1) If continuous AggrAutoContext::StableLimit chunks are lowly aggregated, shifting to PASS_THROUGH state;
 * (2) Else if continuous AggrAutoContext::StableLimit chunks are highly aggregated, shifting to PREAGG state;
 * (3) otherwise or the ADJUST state sustains continuous_limit times, shifting to SELECTIVE_PREAGG state.
 *
 * PASS_THROUGH state sustains continuous_limit times, it will force doing preaggregation if the hash table's size <
 * MaxHtSize, otherwise it will go to ADJUST state. Doing FORCE_PREAGG aims freshening the hash table with new coming
 * rows, helping to aggregating new coming chunks with limiting the size of hash table.
 *
 * FORCE_PREAGG/PREAGG state aggregates AggrAutoContext::PreaggLimit chunks, then going to ADJUST state. PreaggLimit
 * should be small enough to limit the size of hash table.
 *
 * SELECTIVE_PREAGG state aggregates continuous_limit chunks, then shifting to ADJUST state.
 */
Status AggregateStreamingSinkOperator::_push_chunk_by_auto(const ChunkPtr& chunk, const size_t chunk_size) {
    size_t allocated_bytes = _aggregator->hash_map_variant().allocated_memory_usage(_aggregator->mem_pool());
    const size_t continuous_limit = _auto_context.get_continuous_limit();
    switch (_auto_state) {
    case AggrAutoState::INIT_PREAGG: {
        bool ht_needs_expansion = _aggregator->hash_map_variant().need_expand(chunk_size);
        _auto_context.init_preagg_count++;
        if (!ht_needs_expansion ||
            _aggregator->should_expand_preagg_hash_tables(_aggregator->num_input_rows(), chunk_size, allocated_bytes,
                                                          _aggregator->hash_map_variant().size())) {
            // hash table is not full or allow to expand the hash table according reduction rate
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map(chunk_size));
            if (_aggregator->is_none_group_by_exprs()) {
                RETURN_IF_ERROR(_aggregator->compute_single_agg_state(chunk.get(), chunk_size));
            } else {
                RETURN_IF_ERROR(_aggregator->compute_batch_agg_states(chunk.get(), chunk_size));
            }

            TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());

            COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
            break;
        } else {
            _auto_state = AggrAutoState::ADJUST;
            _auto_context.adjust_count = 0;
            VLOG_ROW << "auto agg: " << _auto_context.get_auto_state_string(AggrAutoState::INIT_PREAGG) << " "
                     << _auto_context.init_preagg_count << " -> " << _auto_context.get_auto_state_string(_auto_state);
        }
    }
    case AggrAutoState::ADJUST: {
        _auto_context.adjust_count++;
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map_with_selection(chunk_size));
        }

        size_t hit_count = SIMD::count_zero(_aggregator->streaming_selection());
        if (_auto_context.adjust_count < continuous_limit && _auto_context.is_low_reduction(hit_count, chunk_size)) {
            RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
            _auto_context.pass_through_count++;
            _auto_context.preagg_count = 0;
            _auto_context.selective_preagg_count = 0;
            if (_auto_context.pass_through_count == AggrAutoContext::StableLimit) {
                _auto_state = AggrAutoState::PASS_THROUGH;
                VLOG_ROW << "auto agg: continuous " << AggrAutoContext::StableLimit << " low reduction "
                         << hit_count * 1.0 / chunk_size << " "
                         << _auto_context.get_auto_state_string(AggrAutoState::ADJUST) << " -> "
                         << _auto_context.get_auto_state_string(_auto_state);
            }

        } else if (_auto_context.adjust_count < continuous_limit &&
                   _auto_context.is_high_reduction(hit_count, chunk_size) &&
                   allocated_bytes < AggrAutoContext::MaxHtSize) {
            RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk, chunk_size));

            _auto_context.preagg_count++;
            _auto_context.pass_through_count = 0;
            _auto_context.selective_preagg_count = 0;
            if (_auto_context.preagg_count == AggrAutoContext::StableLimit) {
                _auto_state = AggrAutoState::PREAGG;
                _auto_context.preagg_count = 0;
                VLOG_ROW << "auto agg: continuous " << AggrAutoContext::StableLimit << " high reduction "
                         << hit_count * 1.0 / chunk_size << " "
                         << _auto_context.get_auto_state_string(AggrAutoState::ADJUST) << " -> "
                         << _auto_context.get_auto_state_string(_auto_state);
            }
        } else {
            RETURN_IF_ERROR(_push_chunk_by_selective_preaggregation(chunk, chunk_size, false));
            _auto_context.selective_preagg_count++;
            _auto_context.pass_through_count = 0;
            _auto_context.preagg_count = 0;
            if (_auto_context.selective_preagg_count == AggrAutoContext::StableLimit) {
                _auto_state = AggrAutoState::SELECTIVE_PREAGG;
                VLOG_ROW << "auto agg: continuous " << AggrAutoContext::StableLimit << " "
                         << _auto_context.get_auto_state_string(AggrAutoState::ADJUST)
                         << _auto_context.get_auto_state_string(_auto_state);
            }
        }
        break;
    }
    case AggrAutoState::PASS_THROUGH: {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
        _auto_context.pass_through_count++;
        if (_auto_context.pass_through_count > continuous_limit) {
            _auto_state =
                    allocated_bytes < AggrAutoContext::MaxHtSize ? AggrAutoState::FORCE_PREAGG : AggrAutoState::ADJUST;
            _auto_context.pass_through_count = 0;
            _auto_context.preagg_count = 0;
            _auto_context.adjust_count = 0;

            VLOG_ROW << "auto agg: continuous " << continuous_limit << " "
                     << _auto_context.get_auto_state_string(AggrAutoState::PASS_THROUGH) << " -> "
                     << _auto_context.get_auto_state_string(_auto_state);
            _auto_context.update_continuous_limit();
        }
        break;
    }
    case AggrAutoState::FORCE_PREAGG:
    case AggrAutoState::PREAGG: {
        RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk, chunk_size));
        _auto_context.preagg_count++;
        auto limit = _auto_state == AggrAutoState::FORCE_PREAGG ? AggrAutoContext::ForcePreaggLimit
                                                                : AggrAutoContext::PreaggLimit;
        if (_auto_context.preagg_count > limit) {
            auto current_state = _auto_context.get_auto_state_string(_auto_state);
            _auto_state = AggrAutoState::ADJUST;
            _auto_context.preagg_count = 0;
            _auto_context.adjust_count = 0;
            VLOG_ROW << "auto agg: continuous " << AggrAutoContext::PreaggLimit << " " << current_state << " -> "
                     << _auto_context.get_auto_state_string(_auto_state);
        }
        break;
    }
    case AggrAutoState::SELECTIVE_PREAGG: {
        RETURN_IF_ERROR(_push_chunk_by_selective_preaggregation(chunk, chunk_size, true));
        _auto_context.selective_preagg_count++;
        if (_auto_context.selective_preagg_count > continuous_limit) {
            _auto_state = AggrAutoState::ADJUST;
            _auto_context.selective_preagg_count = 0;
            _auto_context.adjust_count = 0;
            VLOG_ROW << "auto agg: continuous " << continuous_limit << " "
                     << _auto_context.get_auto_state_string(AggrAutoState::SELECTIVE_PREAGG) << " -> "
                     << _auto_context.get_auto_state_string(_auto_state);
            _auto_context.update_continuous_limit();
        }
        break;
    }
    }
    return Status::OK();
}

Status AggregateStreamingSinkOperator::_push_chunk_by_limited_memory(const ChunkPtr& chunk, const size_t chunk_size) {
    if (_limited_mem_state.has_limited(*_aggregator)) {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
        _aggregator->set_streaming_all_states(true);
    } else {
        RETURN_IF_ERROR(_push_chunk_by_auto(chunk, chunk_size));
    }
    return Status::OK();
}

Status AggregateStreamingSinkOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    return _aggregator->reset_state(state, refill_chunks, this);
}
} // namespace starrocks::pipeline
