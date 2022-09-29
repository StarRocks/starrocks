// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "streaming_aggregate_sink_operator.h"

#include "runtime/current_thread.h"
#include "simd/simd.h"

namespace starrocks::pipeline {

Status StreamingAggregateSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get(), _mem_tracker.get()));
    if (_imt_detail) {
        RETURN_IF_ERROR(_imt_detail->prepare(state));
        RETURN_IF_ERROR(_imt_detail->open(state));
    }
//    if (_imt_agg_result) {
//        RETURN_IF_ERROR(_imt_agg_result->prepare(state));
//        RETURN_IF_ERROR(_imt_agg_result->open(state));
//    }
    return _aggregator->open(state);
}

void StreamingAggregateSinkOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    if (_imt_detail) {
        _imt_detail->close(state);
    }
//    if (_imt_agg_result) {
//        _imt_agg_result->close(state);
//    }
    Operator::close(state);
}

Status StreamingAggregateSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    if (_aggregator->hash_map_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    _aggregator->sink_complete();
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> StreamingAggregateSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

Status StreamingAggregateSinkOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();

    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());
    RETURN_IF_ERROR(_aggregator->evaluate_exprs(chunk.get()));
    if (_imt_detail) {
        VLOG(1) << "write chunk.";
        _imt_detail->send_chunk(state, chunk.get());
    }
    // step1: Load state from IMT
    // step2: Refresh IMT
    // step3: Update new input datas
    RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk->num_rows()));
    return Status::OK();
}

Status StreamingAggregateSinkOperator::_push_chunk_by_force_preaggregation(const size_t chunk_size) {
    SCOPED_TIMER(_aggregator->agg_compute_timer());
    if (false) {
    }
#define HASH_MAP_METHOD(NAME)                                                                                          \
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME) {                      \
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_map<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                *_aggregator->hash_map_variant().NAME, chunk_size));                                                   \
    }
    APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }

    if (_aggregator->is_none_group_by_exprs()) {
        _aggregator->compute_single_agg_state(chunk_size);
    } else {
        _aggregator->compute_batch_agg_states(chunk_size);
    }

    _mem_tracker->set(_aggregator->hash_map_variant().reserved_memory_usage(_aggregator->mem_pool()));
    TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_map());

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_map_variant().size());
    RETURN_IF_ERROR(_aggregator->check_has_error());
    return Status::OK();
}

} // namespace starrocks::pipeline
