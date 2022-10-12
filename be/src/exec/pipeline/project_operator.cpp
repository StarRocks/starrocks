// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/project_operator.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/expr.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status ProjectOperator::prepare(RuntimeState* state) {
    return Operator::prepare(state);
}

void ProjectOperator::close(RuntimeState* state) {
    _cur_chunk.reset();
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> ProjectOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

Status ProjectOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    TRY_CATCH_ALLOC_SCOPE_START()
    for (size_t i = 0; i < _common_sub_column_ids.size(); ++i) {
        ASSIGN_OR_RETURN(auto col, _common_sub_expr_ctxs[i]->evaluate(chunk.get()));
        chunk->append_column(std::move(col), _common_sub_column_ids[i]);
        RETURN_IF_HAS_ERROR(_common_sub_expr_ctxs);
    }

    using namespace vectorized;
    vectorized::Columns result_columns(_column_ids.size());
    {
        for (size_t i = 0; i < _column_ids.size(); ++i) {
            ASSIGN_OR_RETURN(result_columns[i], _expr_ctxs[i]->evaluate(chunk.get()));

            if (result_columns[i]->only_null()) {
                result_columns[i] = ColumnHelper::create_column(_expr_ctxs[i]->root()->type(), true);
                result_columns[i]->append_nulls(chunk->num_rows());
            } else if (result_columns[i]->is_constant()) {
                // Note: we must create a new column every time here,
                // because result_columns[i] is shared_ptr
                ColumnPtr new_column = ColumnHelper::create_column(_expr_ctxs[i]->root()->type(), false);
                ConstColumn* const_column = down_cast<ConstColumn*>(result_columns[i].get());
                new_column->append(*const_column->data_column(), 0, 1);
                new_column->assign(chunk->num_rows(), 0);
                result_columns[i] = std::move(new_column);
            }

            // follow SlotDescriptor is_null flag
            if (_type_is_nullable[i] && !result_columns[i]->is_nullable()) {
                result_columns[i] =
                        NullableColumn::create(result_columns[i], NullColumn::create(result_columns[i]->size(), 0));
            }
        }
        RETURN_IF_HAS_ERROR(_expr_ctxs);
    }

    _cur_chunk = std::make_shared<vectorized::Chunk>();
    for (size_t i = 0; i < result_columns.size(); ++i) {
        _cur_chunk->append_column(result_columns[i], _column_ids[i]);
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

Status ProjectOperator::reset_state(std::vector<ChunkPtr>&& chunks) {
    _is_finished = false;
    _cur_chunk = nullptr;

    return Status::OK();
}

Status ProjectOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_common_sub_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::open(_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_common_sub_expr_ctxs, state));

    _dict_optimize_parser.set_mutable_dict_maps(state, state->mutable_query_global_dict_map());

    auto init_dict_optimize = [&](std::vector<ExprContext*>& expr_ctxs, std::vector<SlotId>& target_slots) {
        return _dict_optimize_parser.rewrite_exprs(&expr_ctxs, state, target_slots);
    };

    RETURN_IF_ERROR(init_dict_optimize(_common_sub_expr_ctxs, _common_sub_column_ids));
    RETURN_IF_ERROR(init_dict_optimize(_expr_ctxs, _column_ids));

    return Status::OK();
}

void ProjectOperatorFactory::close(RuntimeState* state) {
    Expr::close(_expr_ctxs, state);
    Expr::close(_common_sub_expr_ctxs, state);
    _dict_optimize_parser.close(state);
    OperatorFactory::close(state);
}
} // namespace starrocks::pipeline
