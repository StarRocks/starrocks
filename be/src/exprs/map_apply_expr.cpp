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

#include "exprs/map_apply_expr.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "exprs/function_helper.h"
#include "exprs/lambda_function.h"
#include "exprs/map_expr.h"
#include "glog/logging.h"
#include "runtime/user_function_cache.h"
#include "storage/chunk_helper.h"

namespace starrocks {

MapApplyExpr::MapApplyExpr(const TExprNode& node) : Expr(node, false) {}

// for tests
MapApplyExpr::MapApplyExpr(TypeDescriptor type) : Expr(std::move(type), false), _maybe_duplicated_keys(true) {}

Status MapApplyExpr::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    if (_is_prepared) {
        return Status::OK();
    }
    _is_prepared = true;
    if (_children.size() < 2) {
        return Status::InternalError("map expression's children size should not less than 2");
    }
    auto lambda_func = down_cast<LambdaFunction*>(_children[0]);
    auto map_expr = down_cast<MapExpr*>(lambda_func->get_lambda_expr());
    _maybe_duplicated_keys = map_expr->maybe_duplicated_keys();
    lambda_func->get_lambda_arguments_ids(&_arguments_ids);
    return Status::OK();
}

StatusOr<ColumnPtr> MapApplyExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    std::vector<ColumnPtr> input_columns;
    NullColumnPtr input_null_map = nullptr;
    MapColumn* input_map = nullptr;
    ColumnPtr input_map_ptr_ref = nullptr; // hold shared_ptr to avoid early deleted.
    // step 1: get input columns from map(key_col, value_col)
    for (int i = 1; i < _children.size(); ++i) { // currently only 2 children, may be more in the future
        ASSIGN_OR_RETURN(auto child_col, context->evaluate(_children[i], chunk));
        // the column is a null literal.
        if (child_col->only_null()) {
            return ColumnHelper::align_return_type(child_col, type(), chunk->num_rows(), true);
        }
        // no optimization for const columns.
        child_col = ColumnHelper::unpack_and_duplicate_const_column(child_col->size(), child_col);
        auto data_column = child_col;
        if (child_col->is_nullable()) {
            auto nullable = down_cast<const NullableColumn*>(child_col.get());
            DCHECK(nullable != nullptr);
            data_column = nullable->data_column();
            // empty null map with non-empty elements
            data_column->empty_null_in_complex_column(
                    nullable->null_column()->get_data(),
                    down_cast<MapColumn*>(data_column.get())->offsets_column()->get_data());
            if (input_null_map) {
                input_null_map =
                        FunctionHelper::union_null_column(nullable->null_column(), input_null_map); // merge null
            } else {
                input_null_map = nullable->null_column();
            }
        }
        DCHECK(data_column->is_map());
        auto cur_map = down_cast<MapColumn*>(data_column.get());

        if (input_map == nullptr) {
            input_map = cur_map;
            input_map_ptr_ref = data_column;
        } else {
            if (UNLIKELY(!ColumnHelper::offsets_equal(cur_map->offsets_column(), input_map->offsets_column()))) {
                return Status::InternalError("Input map element's size are not equal in map_apply().");
            }
        }
        input_columns.push_back(cur_map->keys_column());
        input_columns.push_back(cur_map->values_column());
    }
    // step 2: construct a new chunk to evaluate the lambda expression, output a map column without warping null info.
    ColumnPtr column = nullptr;
    if (input_map->keys_column()->empty()) { // map is empty
        column = ColumnHelper::create_column(type(), false);
    } else {
        auto cur_chunk = std::make_shared<Chunk>();
        // put all arguments into the new chunk
        int argument_num = _arguments_ids.size();
        DCHECK(argument_num == input_columns.size())
                << "arg num << " << argument_num << " != input size " << input_columns.size();
        for (int i = 0; i < argument_num; ++i) {
            cur_chunk->append_column(input_columns[i], _arguments_ids[i]); // column ref
        }
        // put captured columns into the new chunk aligning with the first map's offsets
        std::vector<SlotId> slot_ids;
        _children[0]->get_slot_ids(&slot_ids);
        for (auto id : slot_ids) {
            DCHECK(id > 0);
            auto captured = chunk->get_column_by_slot_id(id);
            if (UNLIKELY(captured->size() < input_map->size())) {
                return Status::InternalError(fmt::format("The size of the captured column {} is less than map's size.",
                                                         captured->get_name()));
            }
            cur_chunk->append_column(captured->replicate(input_map->offsets_column()->get_data()), id);
        }
        // evaluate the lambda expression
        if (cur_chunk->num_rows() <= chunk->num_rows() * 8) {
            ASSIGN_OR_RETURN(column, context->evaluate(_children[0], cur_chunk.get()));
            column = ColumnHelper::align_return_type(column, type(), cur_chunk->num_rows(), false);
        } else { // split large chunks into small ones to avoid too large or various batch_size
            ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
            accumulator.push(std::move(cur_chunk));
            accumulator.finalize();
            while (auto tmp_chunk = accumulator.pull()) {
                ASSIGN_OR_RETURN(auto tmp_col, context->evaluate(_children[0], tmp_chunk.get()));
                tmp_col = ColumnHelper::align_return_type(tmp_col, type(), tmp_chunk->num_rows(), false);
                if (column == nullptr) {
                    column = tmp_col;
                } else {
                    column->append(*tmp_col);
                }
            }
        }
    }
    // attach offsets
    auto map_col = down_cast<MapColumn*>(column.get());
    if (UNLIKELY(input_map->offsets_column()->get_data().back() < map_col->keys_column()->size())) {
        return Status::InternalError(fmt::format("The max index of offsets {} < map->key column's size {}",
                                                 input_map->offsets_column()->get_data().back(),
                                                 map_col->keys_column()->size()));
    }

    auto res_map =
            std::make_shared<MapColumn>(map_col->keys_column(), map_col->values_column(), input_map->offsets_column());

    if (_maybe_duplicated_keys && res_map->size() > 0) {
        res_map->remove_duplicated_keys();
    }
    // attach null info
    if (input_null_map != nullptr) {
        return NullableColumn::create(std::move(res_map), input_null_map);
    }
    return res_map;
}

} // namespace starrocks
