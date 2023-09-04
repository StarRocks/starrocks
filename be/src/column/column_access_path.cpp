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

#include "column/column_access_path.h"

#include "column/column_helper.h"
#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

Status ColumnAccessPath::init(const TColumnAccessPath& column_path, RuntimeState* state, ObjectPool* pool) {
    _type = column_path.type;
    _from_predicate = column_path.from_predicate;

    ExprContext* expr_ctx = nullptr;
    // Todo: may support late materialization? to compute path by other column predicate
    RETURN_IF_ERROR(Expr::create_expr_tree(pool, column_path.path, &expr_ctx, state));
    if (!expr_ctx->root()->is_constant()) {
        return Status::InternalError("error column access constant path.");
    }

    RETURN_IF_ERROR(expr_ctx->prepare(state));
    RETURN_IF_ERROR(expr_ctx->open(state));
    ASSIGN_OR_RETURN(ColumnPtr column, expr_ctx->evaluate(nullptr));

    if (column->is_null(0)) {
        return Status::InternalError("error column access null path.");
    }

    Column* data = ColumnHelper::get_data_column(column.get());
    if (!data->is_binary()) {
        return Status::InternalError("error column access string path.");
    }

    Slice slice = ColumnHelper::as_raw_column<BinaryColumn>(data)->get_slice(0);
    _path = slice.to_string();

    for (const auto& child : column_path.children) {
        ColumnAccessPathPtr child_path = std::make_unique<ColumnAccessPath>();
        RETURN_IF_ERROR(child_path->init(child, state, pool));
        _children.emplace_back(std::move(child_path));
    }

    return Status::OK();
}

Status ColumnAccessPath::init(const TAccessPathType::type& type, const std::string& path, uint32_t index) {
    _type = type;
    _path = path;
    _column_index = index;

    return Status::OK();
}

StatusOr<ColumnAccessPathPtr> ColumnAccessPath::convert_by_index(const Field* field, uint32_t index) {
    ColumnAccessPathPtr path = std::make_unique<ColumnAccessPath>();
    path->_type = this->_type;
    path->_path = this->_path;
    path->_from_predicate = this->_from_predicate;
    path->_column_index = index;

    if (!field->has_sub_fields()) {
        if (!this->_children.empty()) {
            return Status::InternalError(fmt::format(
                    "impossable bad storage schema for access path, field: {}, path: {}", field->name(), this->_path));
        }
        return path;
    }

    auto all_field = field->sub_fields();

    if (field->type()->type() == LogicalType::TYPE_ARRAY) {
        // _type must be ALL/INDEX/OFFSET
        for (const auto& child : this->_children) {
            ASSIGN_OR_RETURN(auto copy, child->convert_by_index(&all_field[0], 0));
            path->_children.emplace_back(std::move(copy));
        }
    } else if (field->type()->type() == LogicalType::TYPE_MAP) {
        // _type must be ALL/INDEX/OFFSET/KEY
        for (const auto& child : this->_children) {
            if (child->_type == TAccessPathType::type::KEY || child->_type == TAccessPathType::type::OFFSET) {
                // KEY/OFFSET never has children
                ASSIGN_OR_RETURN(auto copy, child->convert_by_index(&all_field[0], 0));
                path->_children.emplace_back(std::move(copy));
            } else if (child->_type == TAccessPathType::type::INDEX || child->_type == TAccessPathType::type::ALL) {
                ASSIGN_OR_RETURN(auto copy, child->convert_by_index(&all_field[1], 1));
                path->_children.emplace_back(std::move(copy));
            } else {
                return Status::InternalError(fmt::format("impossable child access path, field: {}, path: {}",
                                                         field->name(), child->to_string()));
            }
        }
    } else if (field->type()->type() == LogicalType::TYPE_STRUCT) {
        // _type must be FIELD
        std::unordered_map<std::string_view, uint32_t> name_index;

        for (uint32_t i = 0; i < all_field.size(); i++) {
            name_index[all_field[i].name()] = i;
        }

        for (const auto& child : this->_children) {
            if (child->_type != TAccessPathType::type::FIELD) {
                return Status::InternalError(fmt::format("impossable child access path, field: {}, path: {}",
                                                         field->name(), child->to_string()));
            }
            uint32_t i = name_index[child->_path];

            ASSIGN_OR_RETURN(auto copy, child->convert_by_index(&all_field[i], i));
            path->_children.emplace_back(std::move(copy));
        }
    }

    return path;
}

const std::string ColumnAccessPath::to_string() const {
    std::stringstream ss;
    ss << _path << "(" << _type << ")";
    return ss.str();
}

} // namespace starrocks
