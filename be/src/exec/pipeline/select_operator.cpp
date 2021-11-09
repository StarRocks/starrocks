// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/select_operator.h"

#include "column/chunk.h"
#include "exec/exec_node.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status SelectOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status SelectOperator::close(RuntimeState* state) {
    Operator::close(state);
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> SelectOperator::pull_chunk(RuntimeState* state) {
    auto batch_size = state->batch_size();

    /*
     *  case pre chunk is empty:
     *  if input chunk is big enough( > batch_size/2)
     *      return it;
     *  else 
     *      merge it into _pre_output_chunk.
     */
    if (!_pre_output_chunk) {
        auto cur_size = _curr_chunk->num_rows();
        if (cur_size >= batch_size / 2) {
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
             *  if _pre_output_chunk is big enough(+input_chunk > batch_size)
             *      return it and merge input chunk into _pre_output_chunk;
             *  else 
             *      merge input chunk into _pre_output_chunk.
             */
            auto cur_size = _curr_chunk->num_rows();
            if (cur_size + _pre_output_chunk->num_rows() > batch_size) {
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
    ExecNode::eval_conjuncts(_conjunct_ctxs, chunk.get());
    _curr_chunk = chunk;
    return Status::OK();
}

Status SelectOperatorFactory::prepare(RuntimeState* state) {
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

void SelectOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
}

} // namespace starrocks::pipeline
