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

#include "storage/primitive/schema_helper.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "column/schema.h"
#include "storage/primitive/tablet_column.h"
#include "types/type_info.h"

namespace starrocks {

namespace {

TypeInfoPtr get_tablet_column_type_info(const TabletColumn& col) {
    if (col.type() == TYPE_ARRAY) {
        return get_array_type_info(get_tablet_column_type_info(col.subcolumn(0)));
    }
    if (col.type() == TYPE_MAP) {
        return get_map_type_info(get_tablet_column_type_info(col.subcolumn(0)),
                                 get_tablet_column_type_info(col.subcolumn(1)));
    }
    if (col.type() == TYPE_STRUCT) {
        std::vector<TypeInfoPtr> field_types;
        field_types.reserve(col.subcolumn_count());
        for (int i = 0; i < col.subcolumn_count(); ++i) {
            field_types.emplace_back(get_tablet_column_type_info(col.subcolumn(i)));
        }
        return get_struct_type_info(std::move(field_types));
    }
    return get_type_info(col.type(), col.precision(), col.scale());
}

} // namespace

Field StorageSchemaHelper::convert_field(ColumnId id, const TabletColumn& c) {
    starrocks::Field f(id, std::string(c.name()), get_tablet_column_type_info(c), c.is_nullable());
    f.set_is_key(c.is_key());
    f.set_length(c.length());
    f.set_uid(c.unique_id());
    f.set_is_virtual(c.is_virtual_column());

    if (c.type() == TYPE_ARRAY) {
        f.add_sub_field(convert_field(id, c.subcolumn(0)));
    } else if (c.type() == TYPE_MAP) {
        for (int i = 0; i < 2; ++i) {
            f.add_sub_field(convert_field(id, c.subcolumn(i)));
        }
    } else if (c.type() == TYPE_STRUCT) {
        for (int i = 0; i < c.subcolumn_count(); ++i) {
            f.add_sub_field(convert_field(id, c.subcolumn(i)));
        }
    }

    f.set_short_key_length(c.index_length());
    f.set_aggregate_method(c.aggregation());
    f.set_agg_state_desc(c.get_agg_state_desc());
    return f;
}

SchemaPtr StorageSchemaHelper::convert_schema(const std::vector<TabletColumn*>& columns,
                                              const std::vector<std::string_view>& col_names) {
    SchemaPtr schema = std::make_shared<Schema>();
    int new_column_idx = 0;
    for (auto s : col_names) {
        for (int32_t idx = 0; idx < columns.size(); ++idx) {
            if (!s.compare(columns[idx]->name())) {
                auto f = std::make_shared<Field>(convert_field(new_column_idx++, *columns[idx]));
                schema->append(f);
            }
        }
    }
    return schema->fields().size() != 0 ? schema : nullptr;
}

} // namespace starrocks
