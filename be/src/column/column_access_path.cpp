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
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/field.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
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

    for (const auto& child : column_path.children) {
        ColumnAccessPathPtr child_path = std::make_unique<ColumnAccessPath>();
        RETURN_IF_ERROR(child_path->init(_absolute_path + "/", child, state, pool));
        _children.emplace_back(std::move(child_path));
    }

    return Status::OK();
}

Status ColumnAccessPath::init(TAccessPathType::type type, const std::string& path, uint32_t index) {
    _type = type;
    _path = path;
    _column_index = index;
    _absolute_path = path;

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

const std::string ColumnAccessPath::to_string() const {
    std::stringstream ss;
    ss << _path << "(" << _type << ")";
    return ss.str();
}

StatusOr<std::unique_ptr<ColumnAccessPath>> ColumnAccessPath::create(const TColumnAccessPath& column_path,
                                                                     RuntimeState* state, ObjectPool* pool) {
    auto path = std::make_unique<ColumnAccessPath>();
    RETURN_IF_ERROR(path->init("/", column_path, state, pool));
    return path;
}

StatusOr<std::unique_ptr<ColumnAccessPath>> ColumnAccessPath::create(const TAccessPathType::type& type,
                                                                     const std::string& path, uint32_t index) {
    auto p = std::make_unique<ColumnAccessPath>();
    RETURN_IF_ERROR(p->init(type, path, index));
    return p;
}

void ColumnAccessPathUtil::rewrite_struct_type_descriptor(TypeDescriptor& original_type,
                                                          const ColumnAccessPathPtr& access_path) {
    DCHECK(original_type.is_struct_type());

    if (is_select_all_subfields(access_path)) {
        return;
    }

    // build child access path mapping
    std::unordered_map<std::string, const ColumnAccessPathPtr&> subfield_mapping{};
    for (const auto& child : access_path->children()) {
        subfield_mapping.emplace(child->path(), child);
    }

    std::vector<std::string> new_subfield_names{};
    std::vector<TypeDescriptor> new_subfield_types{};

    for (size_t i = 0; i < original_type.children.size(); i++) {
        TypeDescriptor& subfield_type = original_type.children[i];
        const auto& subfield_name = original_type.field_names[i];

        const auto& it = subfield_mapping.find(subfield_name);
        if (it == subfield_mapping.end()) {
            continue;
        }

        if (subfield_type.is_complex_type()) {
            const ColumnAccessPathPtr& child_path = it->second;
            DCHECK(child_path->is_field());
            rewrite_complex_type_descriptor(subfield_type, child_path);
        }

        new_subfield_names.emplace_back(subfield_name);
        new_subfield_types.emplace_back(subfield_type);
    }

    original_type.children = new_subfield_types;
    original_type.field_names = new_subfield_names;
}

void ColumnAccessPathUtil::rewrite_map_type_descriptor(TypeDescriptor& original_type,
                                                       const ColumnAccessPathPtr& access_path) {
    DCHECK(original_type.is_map_type());
    if (is_select_all_subfields(access_path)) {
        return;
    }

    DCHECK_EQ(1, access_path->children().size());
    ColumnAccessPathPtr& value_path = access_path->children()[0];

    bool access_key = false;
    bool access_value = false;

    // TODO(SmithCruise) Not support to read offset column only
    if (value_path->is_key() || value_path->is_offset()) {
        access_key = true;
    } else if (value_path->is_value()) {
        access_value = true;
    } else if (value_path->is_index() || value_path->is_all()) {
        access_key = true;
        access_value = true;
    } else {
        DCHECK(false) << "Error ColumnAccessPaths for MapType";
        // Defense code, just select all
        access_key = true;
        access_value = true;
    }

    TypeDescriptor& key_type = original_type.children[0];
    TypeDescriptor& value_type = original_type.children[1];

    if (!access_key) {
        key_type.type = TYPE_UNKNOWN;
    }
    if (!access_value) {
        value_type.type = TYPE_UNKNOWN;
        value_type.children.resize(0);
        value_type.field_names.resize(0);
    } else {
        // Map value column may contains complex type, rewrite it either
        if (value_type.is_complex_type()) {
            // Consider for [/col2/VALUE/INDEX/a], we need to advance one level if it has child
            // If is_value()=true, means it's map_values() function,
            if (!value_path->children().empty()) {
                DCHECK_EQ(1, value_path->children().size());
                const ColumnAccessPathPtr& value_child_path = value_path->children()[0];
                DCHECK(value_child_path->is_index());
                rewrite_complex_type_descriptor(value_type, value_child_path);
            }
        }
    }
}

void ColumnAccessPathUtil::rewrite_array_type_descriptor(TypeDescriptor& original_type,
                                                         const ColumnAccessPathPtr& access_path) {
    DCHECK(original_type.is_array_type());

    TypeDescriptor& element_type = original_type.children[0];

    if (!element_type.is_complex_type()) {
        return;
    }

    if (is_select_all_subfields(access_path)) {
        return;
    }

    DCHECK_EQ(1, access_path->children().size());
    const ColumnAccessPathPtr& element_path = access_path->children()[0];
    DCHECK(element_path->is_index());

    rewrite_complex_type_descriptor(element_type, element_path);
}

void ColumnAccessPathUtil::rewrite_complex_type_descriptor(TypeDescriptor& original_type,
                                                           const ColumnAccessPathPtr& access_path) {
    if (original_type.is_struct_type()) {
        rewrite_struct_type_descriptor(original_type, access_path);
    } else if (original_type.is_map_type()) {
        rewrite_map_type_descriptor(original_type, access_path);
    } else if (original_type.is_array_type()) {
        rewrite_array_type_descriptor(original_type, access_path);
    } else {
        DCHECK(false);
    }
}

bool ColumnAccessPathUtil::is_select_all_subfields(const ColumnAccessPathPtr& path) {
    if (path == nullptr || path->is_all() || path->children().empty()) {
        return true;
    } else {
        return false;
    }
}

} // namespace starrocks
