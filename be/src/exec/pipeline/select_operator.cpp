// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/select_operator.h"

#include "column/chunk.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status SelectOperator::prepare(RuntimeState* state) {
    return Operator::prepare(state);
}

void SelectOperator::close(RuntimeState* state) {
    _curr_chunk.reset();
    _pre_output_chunk.reset();
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> SelectOperator::pull_chunk(RuntimeState* state) {
    auto chunk_size = state->chunk_size();

    /*
     *  case pre chunk is empty:
     *  if input chunk is big enough( > chunk_size/2)
     *      return it;
     *  else
     *      merge it into _pre_output_chunk.
     */
    if (!_pre_output_chunk) {
        auto cur_size = _curr_chunk->num_rows();
        if (cur_size >= chunk_size / 2) {
            return std::move(_curr_chunk);
        } else {
            _pre_output_chunk = std::move(_curr_chunk);
        }
    } else {
        // if input is done.
        if (!_curr_chunk) {
            return std::move(_pre_output_chunk);
        } else {
            /*
             *  case pre chunk is not-empty:
             *  if _pre_output_chunk is big enough(+input_chunk > chunk_size)
             *      return it and merge input chunk into _pre_output_chunk;
             *  else
             *      merge input chunk into _pre_output_chunk.
             */
            auto cur_size = _curr_chunk->num_rows();
            if (cur_size + _pre_output_chunk->num_rows() > chunk_size) {
                auto output_chunk = _pre_output_chunk;
                _pre_output_chunk = std::move(_curr_chunk);
                return output_chunk;
            } else {
                vectorized::Columns& dest_columns = _pre_output_chunk->columns();
                vectorized::Columns& src_columns = _curr_chunk->columns();
                size_t num_rows = cur_size;
                // copy the new read chunk to the reserved
                for (size_t i = 0; i < dest_columns.size(); i++) {
                    dest_columns[i]->append(*src_columns[i], 0, num_rows);
                }
                _curr_chunk = nullptr;
            }
        }
    }

    // when output chunk is small, we just return empty chunk.
    return std::make_shared<vectorized::Chunk>();
}

bool SelectOperator::need_input() const {
    return !_curr_chunk;
}

Status SelectOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_conjunct_ctxs, chunk.get()));
    _curr_chunk = chunk;
    return Status::OK();
}

Status SelectOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

void SelectOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
