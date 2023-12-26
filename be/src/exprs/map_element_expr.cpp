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

#include "exprs/map_element_expr.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/object_pool.h"
#include "common/statusor.h"
#include "types/logical_type.h"
#include "util/raw_container.h"

namespace starrocks {

class MapElementExpr final : public Expr {
public:
    explicit MapElementExpr(const TExprNode& node) : Expr(node) {}

    MapElementExpr(const MapElementExpr& m) : Expr(m) { _const_input = m._const_input; }
    MapElementExpr(MapElementExpr&& m) noexcept : Expr(m) { _const_input = m._const_input; }

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override {
        RETURN_IF_ERROR(Expr::open(state, context, scope));
        DCHECK_EQ(2, _children.size());
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            _const_input.resize(_children.size());
            for (auto i = 0; i < _children.size(); ++i) {
                if (_children[i]->is_constant()) {
                    // _const_input[i] maybe not be of ConstColumn
                    ASSIGN_OR_RETURN(_const_input[i], _children[i]->evaluate_checked(context, nullptr));
                } else {
                    _const_input[i] = nullptr;
                }
            }
        } else {
            DCHECK_EQ(_const_input.size(), _children.size());
        }
        return Status::OK();
    }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
#ifndef BE_TEST
        DCHECK_EQ(_type, _children[0]->type().children[1]);
#endif
        ColumnPtr map_col;
        ColumnPtr key_col;
        if (_const_input[0] == nullptr) {
            ASSIGN_OR_RETURN(map_col, _children[0]->evaluate_checked(context, chunk));
        } else {
            map_col = _const_input[0];
        }

        if (_const_input[1] == nullptr) {
            ASSIGN_OR_RETURN(key_col, _children[1]->evaluate_checked(context, chunk));
        } else {
            key_col = _const_input[1];
        }

        size_t num_rows = 0;
        if (UNLIKELY(chunk == nullptr)) {
            // in test case
            num_rows = std::max(map_col->size(), key_col->size());
        } else {
            num_rows = chunk->num_rows();
        }

        if (map_col->only_null()) {
            return ColumnHelper::create_const_null_column(num_rows);
        }

        bool map_is_const = map_col->is_constant();
        bool key_is_const = key_col->is_constant();

        map_col = ColumnHelper::unpack_and_duplicate_const_column(1, map_col);
        key_col = ColumnHelper::unfold_const_column(_children[1]->type(), 1, key_col); // may only null
        auto [map_column, map_nulls] = ColumnHelper::unpack_nullable_column(map_col);
        auto [key_column, key_nulls] = ColumnHelper::unpack_nullable_column(key_col);

        auto& map_keys = down_cast<MapColumn*>(map_column)->keys_column();
        auto& map_values = down_cast<MapColumn*>(map_column)->values_column();
        const auto& offsets = down_cast<MapColumn*>(map_column)->offsets().get_data();

        size_t actual_rows = map_is_const && key_is_const ? 1 : num_rows;
        auto res = map_values->clone_empty(); // must be nullable
        res->reserve(actual_rows);

        for (size_t i = 0; i < actual_rows; i++) {
            auto map_idx = map_is_const ? 0 : i;
            auto key_idx = key_is_const ? 0 : i;
            bool has_equal = false;

            // map is not null and not empty
            if ((map_nulls == nullptr || !map_nulls->get_data()[map_idx]) && offsets[map_idx + 1] > offsets[map_idx]) {
                if (key_nulls == nullptr || !key_nulls->get_data()[key_idx]) {
                    // target not null
                    for (ssize_t j = offsets[map_idx + 1] - 1; j >= offsets[map_idx]; j--) { // last win
                        if (map_keys->equals(j, *key_column, key_idx)) {
                            res->append(*map_values, j, 1);
                            has_equal = true;
                            break;
                        }
                    }
                } else { // target is null
                    for (ssize_t j = offsets[map_idx + 1] - 1; j >= offsets[map_idx]; j--) {
                        if (map_keys->is_null(j)) {
                            res->append(*map_values, j, 1);
                            has_equal = true;
                            break;
                        }
                    }
                }
            }
            if (!has_equal) {
                res->append_nulls(1);
            }
        }

        if (map_is_const && key_is_const) {
            if (!res->is_null(0)) {
                // map_value is nullable, remove it.
                auto col = down_cast<NullableColumn*>(res.get())->data_column();
                return ConstColumn::create(std::move(col), num_rows);
            }
            return ConstColumn::create(std::move(res), num_rows);
        } else {
            return res;
        }
    }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MapElementExpr(*this)); }

private:
    Columns _const_input;
};

Expr* MapElementExprFactory::from_thrift(const TExprNode& node) {
    DCHECK_EQ(TExprNodeType::MAP_ELEMENT_EXPR, node.node_type);
    return new MapElementExpr(node);
}

} // namespace starrocks
