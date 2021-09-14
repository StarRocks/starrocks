// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/exchange/exchange_merge_sort_source_operator.h"

#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/vectorized/sorted_chunks_merger.h"

namespace starrocks::pipeline {
Status ExchangeMergeSortSourceOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
            state, _row_desc, state->fragment_instance_id(), _plan_node_id, _num_sender,
            config::exchg_node_buffer_size_bytes, _runtime_profile, _is_merging, nullptr, true);
    _stream_recvr->create_merger_for_pipeline(_sort_exec_exprs, &_is_asc_order, &_nulls_first);
    return Status::OK();
}

Status ExchangeMergeSortSourceOperator::close(RuntimeState* state) {
    Operator::close(state);
    return Status::OK();
}

bool ExchangeMergeSortSourceOperator::has_output() const {
    return _stream_recvr->is_data_ready();
}

bool ExchangeMergeSortSourceOperator::is_finished() const {
    return _num_rows_returned >= _limit || _is_finishing;
}

void ExchangeMergeSortSourceOperator::finish(RuntimeState* state) {
    if (_is_finishing) {
        return;
    }
    _is_finishing = true;
    return _stream_recvr->close();
}

StatusOr<vectorized::ChunkPtr> ExchangeMergeSortSourceOperator::pull_chunk(RuntimeState* state) {
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();
    get_chunk(state, &chunk);
    return std::move(chunk);
}

Status ExchangeMergeSortSourceOperator::get_chunk(RuntimeState* state, ChunkPtr* chunk) {
    if (is_finished()) {
        *chunk = nullptr;
        return Status::OK();
    }

    return get_next_merging(state, chunk);
}

Status ExchangeMergeSortSourceOperator::get_next_merging(RuntimeState* state, ChunkPtr* chunk) {
    RETURN_IF_ERROR(state->check_query_state("Exchange, while merging next."));
    *chunk = nullptr;
    if (is_finished()) {
        return Status::OK();
    }

    bool should_exit = false;
    if (_num_rows_skipped < _offset) {
        ChunkPtr tmp_chunk;
        do {
            if (!should_exit) {
                RETURN_IF_ERROR(_stream_recvr->get_next_for_pipeline(&tmp_chunk, &_is_finishing, &should_exit));
            }

            if (tmp_chunk) {
                _num_rows_skipped += tmp_chunk->num_rows();
            } else {
                break;
            }
        } while (!should_exit && _num_rows_skipped < _offset);

        if (_num_rows_skipped > _offset) {
            int64_t size = _num_rows_skipped - _offset;
            int64_t offset_in_chunk = tmp_chunk->num_rows() - size;
            if (_limit > 0 && size > _limit) {
                size = _limit;
            }
            *chunk = tmp_chunk->clone_empty_with_slot(size);
            for (size_t c = 0; c < tmp_chunk->num_columns(); ++c) {
                const ColumnPtr& src = tmp_chunk->get_column_by_index(c);
                ColumnPtr& dest = (*chunk)->get_column_by_index(c);
                dest->append(*src, offset_in_chunk, size);
                // resize constant column as same as other non-constant columns, so Chunk::num_rows()
                // can return a right number if this ConstColumn is the first column of the chunk.
                if (dest->is_constant()) {
                    dest->resize(size);
                }
            }
            _num_rows_skipped = _offset;
            _num_rows_returned += size;
            
            // the first Chunk will have a size less than config::vector_chunk_size.
            return Status::OK();
        }

        if (!tmp_chunk) {
            // check EOS after (_num_rows_skipped < _offset), so the only one chunk can be returned.
            return Status::OK();
        }
    }

    if (!should_exit) {
        RETURN_IF_ERROR(_stream_recvr->get_next_for_pipeline(chunk, &_is_finishing, &should_exit));
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

} // namespace starrocks::pipeline
