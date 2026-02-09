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

#include <utility>
#include <vector>

#include "storage/tablet_schema.h"
#include "storage/types.h"

namespace starrocks {

TypeInfoPtr get_type_info(const ColumnMetaPB& column_meta_pb) {
    auto type = static_cast<LogicalType>(column_meta_pb.type());
    if (type == TYPE_ARRAY) {
        const ColumnMetaPB& child = column_meta_pb.children_columns(0);
        TypeInfoPtr child_type_info = get_type_info(child);
        return get_array_type_info(child_type_info);
    }
    if (type == TYPE_MAP) {
        const ColumnMetaPB& key_meta = column_meta_pb.children_columns(0);
        const ColumnMetaPB& value_meta = column_meta_pb.children_columns(1);
        TypeInfoPtr key_type_info = get_type_info(key_meta);
        TypeInfoPtr value_type_info = get_type_info(value_meta);
        return get_map_type_info(std::move(key_type_info), std::move(value_type_info));
    }
    if (type == TYPE_STRUCT) {
        std::vector<TypeInfoPtr> field_types;
        field_types.reserve(column_meta_pb.children_columns_size());
        for (int i = 0; i < column_meta_pb.children_columns_size(); ++i) {
            field_types.emplace_back(get_type_info(column_meta_pb.children_columns(i)));
        }
        return get_struct_type_info(std::move(field_types));
    }
    return get_type_info(delegate_type(type));
}

TypeInfoPtr get_type_info(const TabletColumn& col) {
    if (col.type() == TYPE_ARRAY) {
        const TabletColumn& child = col.subcolumn(0);
        TypeInfoPtr child_type_info = get_type_info(child);
        return get_array_type_info(child_type_info);
    }
    if (col.type() == TYPE_MAP) {
        const TabletColumn& key_meta = col.subcolumn(0);
        const TabletColumn& value_meta = col.subcolumn(1);
        TypeInfoPtr key_type_info = get_type_info(key_meta);
        TypeInfoPtr value_type_info = get_type_info(value_meta);
        return get_map_type_info(std::move(key_type_info), std::move(value_type_info));
    }
    if (col.type() == TYPE_STRUCT) {
        std::vector<TypeInfoPtr> field_types;
        field_types.reserve(col.subcolumn_count());
        for (int i = 0; i < col.subcolumn_count(); ++i) {
            field_types.emplace_back(get_type_info(col.subcolumn(i)));
        }
        return get_struct_type_info(std::move(field_types));
    }
    return get_type_info(col.type(), col.precision(), col.scale());
}

} // namespace starrocks
