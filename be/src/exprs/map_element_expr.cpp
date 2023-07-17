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
#include "common/object_pool.h"
#include "util/raw_container.h"

namespace starrocks {

class MapElementExpr final : public Expr {
public:
    explicit MapElementExpr(const TExprNode& node) : Expr(node) {}

    MapElementExpr(const MapElementExpr& m) : Expr(m) { _const_input = m._const_input; }
    MapElementExpr(MapElementExpr&& m) : Expr(m) { _const_input = m._const_input; }

    Status open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) override {
        RETURN_IF_ERROR(Expr::open(state, context, scope));
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
        DCHECK_EQ(2, _children.size());
        // check the map's value type is the same as the expr's type
#ifndef BE_TEST
        DCHECK_EQ(_type, _children[0]->type().children[1]);
#endif
        auto child_size = _children.size();
        Columns inputs(child_size);
        Columns input_data(child_size);
        std::vector<NullColumnPtr> input_null(child_size);
        std::vector<bool> is_const(child_size, true);
        bool all_const = true;
        for (auto i = 0; i < child_size; ++i) {
            inputs[i] = _const_input[i];
            if (inputs[i] == nullptr) {
                ASSIGN_OR_RETURN(inputs[i], _children[i]->evaluate_checked(context, chunk));
                is_const[i] = inputs[i]->is_constant();
                all_const &= is_const[i];
            }
            if (i == 0 && inputs[0]->only_null()) {
                DCHECK(chunk == nullptr || chunk->num_rows() == inputs[0]->size()); // chunk is null in tests.
                return ColumnHelper::create_const_null_column(inputs[0]->size());
            }
            auto value = inputs[i];
            if (value->is_constant()) {
                value = down_cast<ConstColumn*>(value.get())->data_column();
            }
            if (value->is_nullable()) {
                auto nullable = down_cast<const NullableColumn*>(value.get());
                input_null[i] = nullable->null_column();
                input_data[i] = nullable->data_column();
            } else {
                input_null[i] = nullptr;
                input_data[i] = value;
            }
        }
        auto size = inputs[0]->size();
        DCHECK(chunk == nullptr || chunk->num_rows() == size); // chunk is null in tests.

        auto dest_size = size;
        if (all_const) {
            dest_size = 1;
        }

        DCHECK(input_data[0]->is_map());
        auto* map_column = down_cast<MapColumn*>(input_data[0].get());
        auto& map_keys = map_column->keys_column();
        auto& map_values = map_column->values_column();
        const auto& offsets = map_column->offsets().get_data();

        auto res = map_values->clone_empty(); // must be nullable
        res->reserve(dest_size);
        for (size_t i = 0; i < dest_size; i++) {
            auto id_0 = is_const[0] ? 0 : i;
            auto id_1 = is_const[1] ? 0 : i;
            bool has_equal = false;
            if ((input_null[0] == nullptr || !input_null[0]->get_data()[id_0]) &&
                offsets[id_0 + 1] > offsets[id_0]) {                                   // map is not null and not empty
                if (input_null[1] == nullptr || !input_null[1]->get_data()[id_1]) {    // target not null
                    for (ssize_t j = offsets[id_0 + 1] - 1; j >= offsets[id_0]; j--) { // last win
                        if (map_keys->equals(j, *input_data[1], id_1)) {
                            res->append(*map_values, j, 1);
                            has_equal = true;
                            break;
                        }
                    }
                } else { // target is null
                    for (ssize_t j = offsets[id_0 + 1] - 1; j >= offsets[id_0]; j--) {
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

        if (all_const) {
            return ConstColumn::create(std::move(res), size);
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
