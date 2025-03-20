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

StatusOr<ChunkPtr> SelectOperator::pull_chunk(RuntimeState* state) {
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
                auto& dest_columns = _pre_output_chunk->columns();
                auto& src_columns = _curr_chunk->columns();
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
    return std::make_shared<Chunk>();
}

bool SelectOperator::need_input() const {
    return !_curr_chunk;
}

Status SelectOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _curr_chunk = chunk;
    if (!_common_exprs.empty()) {
        SCOPED_TIMER(_conjuncts_timer);
        for (const auto& [slot_id, expr] : _common_exprs) {
            ASSIGN_OR_RETURN(auto col, expr->evaluate(_curr_chunk.get()));
            _curr_chunk->append_column(col, slot_id);
        }
    }
    RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_conjunct_ctxs, _curr_chunk.get()));
    {
        // common_exprs' slots are only needed inside this operator, no need to pass it downsteam
        for (const auto& [slot_id, _] : _common_exprs) {
            _curr_chunk->remove_column_by_slot_id(slot_id);
        }
    }
    return Status::OK();
}

Status SelectOperator::reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _curr_chunk.reset();
    _pre_output_chunk.reset();
    _is_finished = false;
    return Status::OK();
}

Status SelectOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));
    std::vector<ExprContext*> common_expr_ctxs;
    for (const auto& [_, ctx] : _common_exprs) {
        common_expr_ctxs.emplace_back(ctx);
    }
    RETURN_IF_ERROR(Expr::prepare(common_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(common_expr_ctxs, state));
    return Status::OK();
}

void SelectOperatorFactory::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
