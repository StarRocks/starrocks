// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/chunk_accumulate_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace pipeline {

Status ChunkAccumulateOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    DCHECK(_out_chunk == nullptr);

    if (_in_chunk == nullptr) {
        _in_chunk = chunk;
    } else if (_in_chunk->num_rows() + chunk->num_rows() > state->chunk_size()) {
        _out_chunk = std::move(_in_chunk);
        _in_chunk = chunk;
    } else {
        _in_chunk->append(*chunk);
    }

    if (_out_chunk == nullptr && (_in_chunk->num_rows() >= state->chunk_size() * LOW_WATERMARK_ROWS_RATE ||
                                  _in_chunk->memory_usage() >= LOW_WATERMARK_BYTES)) {
        _out_chunk = std::move(_in_chunk);
    }

    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> ChunkAccumulateOperator::pull_chunk(RuntimeState*) {
    // If there isn't more input chunk and _out_chunk has been outputted, output _in_chunk this time.
    if (_is_finished && _out_chunk == nullptr) {
        return std::move(_in_chunk);
    }

    return std::move(_out_chunk);
}

Status ChunkAccumulateOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;

    return Status::OK();
}

Status ChunkAccumulateOperator::set_finished(RuntimeState*) {
    _is_finished = true;
    _in_chunk.reset();
    _out_chunk.reset();

    return Status::OK();
}

Status ChunkAccumulateOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    _in_chunk = nullptr;
    _out_chunk = nullptr;

    return Status::OK();
}

} // namespace pipeline
} // namespace starrocks
