// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/exchange/exchange_merge_sort_source_operator.h"

#include "exec/sort_exec_exprs.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/sorted_chunks_merger.h"

namespace starrocks::pipeline {
Status ExchangeMergeSortSourceOperator::prepare(RuntimeState* state) {
    SourceOperator::prepare(state);
    auto query_statistic_recv = state->query_recv();
    _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
            state, _row_desc, state->fragment_instance_id(), _plan_node_id, _num_sender,
            config::exchg_node_buffer_size_bytes, true, query_statistic_recv, true, 1, true);
    _stream_recvr->bind_profile(_driver_sequence, _unique_metrics);
    return _stream_recvr->create_merger_for_pipeline(state, _sort_exec_exprs, &_is_asc_order, &_nulls_first);
}

void ExchangeMergeSortSourceOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool ExchangeMergeSortSourceOperator::has_output() const {
    return _stream_recvr->is_data_ready();
}

bool ExchangeMergeSortSourceOperator::is_finished() const {
    if (_limit < 0) {
        return _is_finished;
    } else {
        return _num_rows_returned >= _limit || _is_finished;
    }
}

Status ExchangeMergeSortSourceOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _stream_recvr->close();
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ExchangeMergeSortSourceOperator::pull_chunk(RuntimeState* state) {
    auto chunk = std::make_shared<vectorized::Chunk>();
    RETURN_IF_ERROR(get_next_merging(state, &chunk));
    eval_runtime_bloom_filters(chunk.get());
    return std::move(chunk);
}

Status ExchangeMergeSortSourceOperator::get_next_merging(RuntimeState* state, ChunkPtr* chunk) {
    RETURN_IF_ERROR(state->check_query_state("Exchange, while merging next."));
    if (is_finished()) {
        return Status::OK();
    }

    /* The following code first filters out _offset rows of data, 
     * and then get _limit rows from the subsequent data. 
     * Because of the streaming implementation, 
     * _num_rows_input ensures that this filtering process is not repeated, 
     * then we try to get _limit rows.
     * should_exit is used to cooperate with pipeline, 
     * When there is no data, we should not continue.
     */
    bool should_exit = false;
    if (_num_rows_input < _offset) {
        ChunkPtr tmp_chunk;
        while (!_is_finished && !should_exit && _num_rows_input < _offset) {
            ChunkPtr empty_chunk;
            RETURN_IF_ERROR(_stream_recvr->get_next_for_pipeline(&empty_chunk, &_is_finished, &should_exit));

            if (empty_chunk) {
                _num_rows_input += empty_chunk->num_rows();
                std::swap(empty_chunk, tmp_chunk);
            } else {
                break;
            }
        }

        // tmp_chunk is the last chunk, no extra chunks needs to be read
        if (_num_rows_input > _offset) {
            int64_t rewind_size = _num_rows_input - _offset;
            int64_t offset_in_chunk = tmp_chunk->num_rows() - rewind_size;
            if (_limit > 0 && rewind_size > _limit) {
                rewind_size = _limit;
            }
            *chunk = tmp_chunk->clone_empty_with_slot(rewind_size);
            for (size_t c = 0; c < tmp_chunk->num_columns(); ++c) {
                const ColumnPtr& src = tmp_chunk->get_column_by_index(c);
                ColumnPtr& dest = (*chunk)->get_column_by_index(c);
                dest->append(*src, offset_in_chunk, rewind_size);
                // resize constant column as same as other non-constant columns, so Chunk::num_rows()
                // can return a right number if this ConstColumn is the first column of the chunk.
                if (dest->is_constant()) {
                    dest->resize(rewind_size);
                }
            }
            _num_rows_input = _offset;
            _num_rows_returned += rewind_size;

            // the first Chunk will have a size less than state->chunk_size().
            return Status::OK();
        }

        if (!tmp_chunk) {
            // check EOS after (_num_rows_input < _offset), so the only one chunk can be returned.
            return Status::OK();
        }
    }

    if (!should_exit) {
        RETURN_IF_ERROR(_stream_recvr->get_next_for_pipeline(chunk, &_is_finished, &should_exit));
    }

    if ((*chunk) != nullptr) {
        size_t size_in_chunk = (*chunk)->num_rows();
        if (_limit > 0 && size_in_chunk + _num_rows_returned > _limit) {
            size_in_chunk -= (size_in_chunk + _num_rows_returned - _limit);
            (*chunk)->set_num_rows(size_in_chunk);
        }
        _num_rows_returned += size_in_chunk;
    }

    return Status::OK();
}

Status ExchangeMergeSortSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_sort_exec_exprs->prepare(state, _row_desc, _row_desc));
    RETURN_IF_ERROR(_sort_exec_exprs->open(state));
    return Status::OK();
}

void ExchangeMergeSortSourceOperatorFactory::close(RuntimeState* state) {
    _sort_exec_exprs->close(state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
