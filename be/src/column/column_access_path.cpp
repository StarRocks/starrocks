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

#include <cstddef>
#include <utility>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "types/logical_type.h"

namespace starrocks {

Status ColumnAccessPath::init(const std::string& parent_path, const TColumnAccessPath& column_path, RuntimeState* state,
                              ObjectPool* pool) {
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
    _absolute_path = parent_path + _path;

    if (column_path.__isset.type_desc) {
        _value_type = TypeDescriptor::from_thrift(column_path.type_desc);
    }

    for (const auto& child : column_path.children) {
        ColumnAccessPathPtr child_path = std::make_unique<ColumnAccessPath>();
        RETURN_IF_ERROR(child_path->init(_absolute_path + ".", child, state, pool));
        _children.emplace_back(std::move(child_path));
    }

    return Status::OK();
}

ColumnAccessPath* ColumnAccessPath::get_child(const std::string& path) {
    for (const auto& child : _children) {
        if (child->_path == path) {
            return child.get();
        }
    }
    return nullptr;
}

StatusOr<ColumnAccessPathPtr> ColumnAccessPath::convert_by_index(const Field* field, uint32_t index) {
    ColumnAccessPathPtr path = std::make_unique<ColumnAccessPath>();
    path->_type = this->_type;
    path->_path = this->_path;
    path->_from_predicate = this->_from_predicate;
    path->_absolute_path = this->_absolute_path;
    path->_column_index = index;
    path->_value_type = this->_value_type;

    // json field has none sub-fields, and we only convert the root path, child path find reader by name
    if (field->type()->type() == LogicalType::TYPE_JSON) {
        // _type must be INDEX
        for (const auto& child : this->_children) {
            ASSIGN_OR_RETURN(auto copy, child->convert_by_index(field, -1));
            path->_children.emplace_back(std::move(copy));
        }
        return path;
    }

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
        // _type must be FIELD,
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

size_t ColumnAccessPath::leaf_size() const {
    if (_children.empty()) {
        return 1;
    }
    size_t size = 0;
    for (const auto& child : _children) {
        size += child->leaf_size();
    }
    return size;
}

void ColumnAccessPath::get_all_leafs(std::vector<ColumnAccessPath*>* result) {
    if (_children.empty()) {
        result->emplace_back(this);
        return;
    }
    for (const auto& child : _children) {
        child->get_all_leafs(result);
    }
}

const std::string ColumnAccessPath::to_string() const {
    std::stringstream ss;
    ss << _path << "(" << _type << ")";
    return ss.str();
}

StatusOr<std::unique_ptr<ColumnAccessPath>> ColumnAccessPath::create(const TColumnAccessPath& column_path,
                                                                     RuntimeState* state, ObjectPool* pool) {
    auto path = std::make_unique<ColumnAccessPath>();
    RETURN_IF_ERROR(path->init("", column_path, state, pool));
    return path;
}

StatusOr<std::unique_ptr<ColumnAccessPath>> ColumnAccessPath::create(const TAccessPathType::type& type,
                                                                     const std::string& path, uint32_t index,
                                                                     const std::string& prefix) {
    auto p = std::make_unique<ColumnAccessPath>();
    p->_type = type;
    p->_path = path;
    p->_column_index = index;
    if (prefix != "") {
        p->_absolute_path = prefix + "." + path;
    } else {
        p->_absolute_path = path;
    }
    p->_value_type = TypeDescriptor(LogicalType::TYPE_JSON);
    p->_children.clear();
    return std::move(p);
}

std::pair<std::string, std::string> _split_path(const std::string& path) {
    size_t pos = 0;
    if (path.starts_with("\"")) {
        pos = path.find('\"', 1);
        DCHECK(pos != std::string::npos);
    }
    pos = path.find('.', pos);
    std::string key;
    std::string next;
    if (pos == std::string::npos) {
        key = path;
    } else {
        key = path.substr(0, pos);
        next = path.substr(pos + 1);
    }

    return {key, next};
}

ColumnAccessPath* insert_json_path_impl(const std::string& path, ColumnAccessPath* root) {
    if (path.empty()) {
        return root;
    }

    auto [key, next] = _split_path(path);
    auto child = root->get_child(key);
    if (child == nullptr) {
        auto n = ColumnAccessPath::create(TAccessPathType::FIELD, key, 0, root->absolute_path());
        DCHECK(n.ok());
        root->children().emplace_back(std::move(n.value()));
        child = root->children().back().get();
    }
    return insert_json_path_impl(next, child);
}

void ColumnAccessPath::insert_json_path(ColumnAccessPath* root, LogicalType type, const std::string& path) {
    auto leaf = insert_json_path_impl(path, root);
    leaf->_type = TAccessPathType::type::FIELD;
    leaf->_column_index = 0;
    leaf->_absolute_path = root->absolute_path() + "." + path;
    leaf->_value_type = TypeDescriptor(type);
}

} // namespace starrocks
