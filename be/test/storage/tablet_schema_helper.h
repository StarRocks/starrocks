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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/tablet_schema_helper.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>

#include "runtime/mem_pool.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class TabletSchemaHelper {
public:
    static std::unique_ptr<TabletSchema> create_tablet_schema(const std::vector<ColumnPB>& columns,
                                                              int num_short_key_columns = -1);

    // (k1 int, k2 varchar(20), k3 int) duplicated key (k1, k2)
    static std::shared_ptr<TabletSchema> create_tablet_schema();
};

inline ColumnPB create_int_key_pb(int32_t id, bool is_nullable = true, bool is_bf_column = false,
                                  bool has_bitmap_index = false) {
    ColumnPB col;
    col.set_unique_id(id);
    col.set_name(std::to_string(id));
    col.set_type("INT");
    col.set_is_key(true);
    col.set_is_nullable(is_nullable);
    col.set_length(4);
    col.set_index_length(4);
    col.set_is_bf_column(is_bf_column);
    col.set_has_bitmap_index(has_bitmap_index);
    return col;
}

inline TabletColumn create_int_key(int32_t id, bool is_nullable = true, bool is_bf_column = false,
                                   bool has_bitmap_index = false) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(TYPE_INT);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(4);
    column.set_index_length(4);
    column.set_is_bf_column(is_bf_column);
    column.set_has_bitmap_index(has_bitmap_index);
    return column;
}

inline ColumnPB create_int_value_pb(int32_t id, const std::string& agg_method = "SUM", bool is_nullable = true,
                                    const std::string& default_value = "", bool is_bf_column = false,
                                    bool has_bitmap_index = false) {
    ColumnPB col;
    col.set_unique_id(id);
    col.set_name(std::to_string(id));
    col.set_type("INT");
    col.set_is_key(false);
    col.set_aggregation(agg_method);
    col.set_is_nullable(is_nullable);
    col.set_length(4);
    col.set_index_length(4);
    if (!default_value.empty()) {
        col.set_default_value(default_value);
    }
    col.set_is_bf_column(is_bf_column);
    col.set_has_bitmap_index(has_bitmap_index);
    return col;
}

inline TabletColumn create_int_value(int32_t id, StorageAggregateType agg_method = STORAGE_AGGREGATE_SUM,
                                     bool is_nullable = true, const std::string default_value = "",
                                     bool is_bf_column = false, bool has_bitmap_index = false) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(TYPE_INT);
    column.set_is_key(false);
    column.set_aggregation(agg_method);
    column.set_is_nullable(is_nullable);
    column.set_length(4);
    column.set_index_length(4);
    if (default_value != "") {
        column.set_default_value(default_value);
    }
    column.set_is_bf_column(is_bf_column);
    column.set_has_bitmap_index(has_bitmap_index);
    return column;
}

inline TabletColumn create_char_key(int32_t id, bool is_nullable = true, int length = 8) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(TYPE_CHAR);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(length);
    column.set_index_length(1);
    return column;
}

inline TabletColumn create_varchar_key(int32_t id, bool is_nullable = true, int length = 8) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(TYPE_VARCHAR);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(length);
    column.set_index_length(4);
    return column;
}

inline TabletColumn create_array(int32_t id, bool is_nullable = true, int length = 24) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(TYPE_ARRAY);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(length);
    column.set_index_length(length);
    return column;
}

inline TabletColumn create_map(int32_t id, bool is_nullable = true) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(TYPE_MAP);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(16);
    column.set_index_length(16);
    return column;
}

inline TabletColumn create_struct(int32_t id, bool is_nullable = true) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(TYPE_STRUCT);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(16);
    column.set_index_length(16);
    return column;
}

inline ColumnPB create_with_default_value_pb(const std::string& col_type, std::string default_value) {
    ColumnPB column;
    column.set_type(col_type);
    column.set_is_nullable(true);
    column.set_aggregation("NONE");
    column.set_default_value(default_value);
    column.set_length(4);
    return column;
}

template <LogicalType type>
inline TabletColumn create_with_default_value(std::string default_value) {
    TabletColumn column;
    column.set_type(type);
    column.set_is_nullable(true);
    column.set_aggregation(STORAGE_AGGREGATE_NONE);
    column.set_default_value(default_value);
    column.set_length(4);
    return column;
}

} // namespace starrocks
