// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "streaming_aggregate_source_operator.h"

namespace starrocks::pipeline {

Status StreamingAggregateSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    if (_imt_agg_result) {
//        RETURN_IF_ERROR(_imt_agg_result->prepare(state));
        VLOG(1) << "open imt_agg_result";
//        RETURN_IF_ERROR(_imt_agg_result_sink->try_open(state));
        RETURN_IF_ERROR(_imt_agg_result->open(state));
        VLOG(1) << "is_open_done:" << _imt_agg_result_sink->is_open_done();
    }
    return Status::OK();
}

bool StreamingAggregateSourceOperator::has_output() const {
    if (!_aggregator->is_chunk_buffer_empty()) {
        // There are two cases where chunk buffer is not empty
        // case1: streaming mode is 'FORCE_STREAMING'
        // case2: streaming mode is 'AUTO'
        //     case 2.1: very poor aggregation
        //     case 2.2: middle cases, first aggregate locally and output by stream
        return true;
    }

    // There are four cases where chunk buffer is empty
    // case1: streaming mode is 'FORCE_STREAMING'
    // case2: streaming mode is 'AUTO'
    //     case 2.1: very poor aggregation
    //     case 2.2: middle cases, first aggregate locally and output by stream
    // case3: streaming mode is 'FORCE_PREAGGREGATION'
    // case4: streaming mode is 'AUTO'
    //     case 4.1: very high aggregation
    //
    // case1 and case2 means that it will wait for the next chunk from the buffer
    // case3 and case4 means that it will apply local aggregate, so need to wait sink operator finish
    return _aggregator->is_sink_complete() && !_aggregator->is_ht_eos();
}

bool StreamingAggregateSourceOperator::is_finished() const {
    // source operator may finish early
    if (_is_finished) {
        return true;
    }

    // since there are two behavior of streaming operator
    // case 1: chunk-at-a-time, so we check whether the chunk buffer is empty
    // case 2: local aggregate, so we check whether hash table is eos
    if (_aggregator->is_sink_complete() && _aggregator->is_chunk_buffer_empty() && _aggregator->is_ht_eos()) {
        _is_finished = true;
    }
    return _is_finished;
}

Status StreamingAggregateSourceOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status StreamingAggregateSourceOperator::set_finished(RuntimeState* state) {
    return _aggregator->set_finished();
}

void StreamingAggregateSourceOperator::close(RuntimeState* state) {
    _aggregator->unref(state);
    if (_imt_agg_result) {
        _imt_agg_result->close(state);
        VLOG(1) << "is_close_done:" << _imt_agg_result_sink->is_close_done();
    }
    SourceOperator::close(state);
}

//bool StreamingAggregateSourceOperator::pending_finish() const {
//    if (!_imt_agg_result_sink) {
//        return false;
//    }
//
//    // sink's open not finish, we need check util finish
//    if (!_is_open_done) {
//        if (!_imt_agg_result_sink->is_open_done()) {
//            return true;
//        }
//        _is_open_done = true;
//        // since is_open_done(), open_wait will not block
//        auto st = _imt_agg_result_sink->open_wait();
//        if (!st.ok()) {
//            _fragment_ctx->cancel(st);
//            return false;
//        }
//    }
//
//    if (!_imt_agg_result_sink->is_close_done()) {
//        auto st = _imt_agg_result_sink->try_close(_fragment_ctx->runtime_state());
//        if (!st.ok()) {
//            return false;
//        }
//        return true;
//    }
//
//    auto st = _imt_agg_result_sink->close(_fragment_ctx->runtime_state(), Status::OK());
//    if (!st.ok()) {
//        _fragment_ctx->cancel(st);
//    }
//
//    return false;
//}

StatusOr<vectorized::ChunkPtr> StreamingAggregateSourceOperator::pull_chunk(RuntimeState* state) {
//    if (!_is_open_done && _imt_agg_result) {
//        _is_open_done = true;
//        // we can be here cause _sink->is_open_done() return true
//        // so that open_wait() will not block
//        RETURN_IF_ERROR(_imt_agg_result_sink->open_wait());
//    }
    // step1: Update result IMT

    // step2: Output data
    // It is no need to distinguish whether streaming or aggregation mode
    // We just first read chunk from buffer and finally read chunk from hash table
    if (!_aggregator->is_chunk_buffer_empty()) {
        return std::move(_aggregator->poll_chunk_buffer());
    }

    // Even if it is streaming mode, the purpose of reading from hash table is to
    // correctly process the state of hash table(_is_ht_eos)
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    _output_chunk_from_hash_map(&chunk, state);
    DCHECK_CHUNK(chunk);
    if (_imt_agg_result) {
        VLOG(1) << "write imt agg result.";
        for (size_t i = 0; i < chunk->num_rows(); i++) {
            VLOG(2) << "output_chunk output: " << chunk->debug_row(i);
        }
        for (auto [k, v] : chunk->get_slot_id_to_index_map()) {
           VLOG(1) << "slot_id:" << k << ", index:" << v;
        }
        RETURN_IF_ERROR(_imt_agg_result->send_chunk(state, chunk.get()));
    }
    return std::move(chunk);
}

void StreamingAggregateSourceOperator::_output_chunk_from_hash_map(vectorized::ChunkPtr* chunk, RuntimeState* state) {
    if (!_aggregator->it_hash().has_value()) {
        if (false) {
        }
#define HASH_MAP_METHOD(NAME)                                                                   \
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME) \
            _aggregator->it_hash() = _aggregator->_state_allocator.begin();
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
    else if (_aggregator->hash_map_variant().type == vectorized::AggHashMapVariant::Type::NAME)                   \
            _aggregator->convert_hash_map_to_chunk<decltype(_aggregator->hash_map_variant().NAME)::element_type>( \
                    *_aggregator->hash_map_variant().NAME, state->chunk_size(), chunk);
    APPLY_FOR_AGG_VARIANT_ALL(HASH_MAP_METHOD)
#undef HASH_MAP_METHOD
    else {
        DCHECK(false);
    }
}
} // namespace starrocks::pipeline
