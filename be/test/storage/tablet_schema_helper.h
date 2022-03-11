// This file is made available under Elastic License 2.0.
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

// (k1 int, k2 varchar(20), k3 int) duplicated key (k1, k2)
void inline create_tablet_schema(TabletSchema* tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(DUP_KEYS);
    tablet_schema_pb.set_num_short_key_columns(2);
    tablet_schema_pb.set_num_rows_per_row_block(1024);
    tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    tablet_schema_pb.set_next_column_unique_id(4);

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("k1");
    column_1->set_type("INT");
    column_1->set_is_key(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_is_nullable(true);
    column_1->set_is_bf_column(false);

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("k2");
    column_2->set_type("INT"); // TODO change to varchar(20) when dict encoding for string is supported
    column_2->set_length(4);
    column_2->set_index_length(4);
    column_2->set_is_nullable(true);
    column_2->set_is_key(true);
    column_2->set_is_nullable(true);
    column_2->set_is_bf_column(false);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("v1");
    column_3->set_type("INT");
    column_3->set_length(4);
    column_3->set_is_key(false);
    column_3->set_is_nullable(false);
    column_3->set_is_bf_column(false);
    column_3->set_aggregation("SUM");

    tablet_schema->init_from_pb(tablet_schema_pb);
}

inline TabletColumn create_int_key(int32_t id, bool is_nullable = true, bool is_bf_column = false,
                                   bool has_bitmap_index = false) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(OLAP_FIELD_TYPE_INT);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(4);
    column.set_index_length(4);
    column.set_is_bf_column(is_bf_column);
    column.set_has_bitmap_index(has_bitmap_index);
    return column;
}

inline TabletColumn create_int_value(int32_t id, FieldAggregationMethod agg_method = OLAP_FIELD_AGGREGATION_SUM,
                                     bool is_nullable = true, const std::string default_value = "",
                                     bool is_bf_column = false, bool has_bitmap_index = false) {
    TabletColumn column;
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_type(OLAP_FIELD_TYPE_INT);
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
    column.set_type(OLAP_FIELD_TYPE_CHAR);
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
    column.set_type(OLAP_FIELD_TYPE_VARCHAR);
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
    column.set_type(OLAP_FIELD_TYPE_ARRAY);
    column.set_is_key(true);
    column.set_is_nullable(is_nullable);
    column.set_length(length);
    column.set_index_length(length);
    return column;
}

template <FieldType type>
inline TabletColumn create_with_default_value(std::string default_value) {
    TabletColumn column;
    column.set_type(type);
    column.set_is_nullable(true);
    column.set_aggregation(OLAP_FIELD_AGGREGATION_NONE);
    column.set_default_value(default_value);
    column.set_length(4);
    return column;
}

inline void set_column_value_by_type(FieldType fieldType, int src, char* target, MemPool* pool, size_t _length = 0) {
    if (fieldType == OLAP_FIELD_TYPE_CHAR) {
        std::string s = std::to_string(src);
        char* src_value = &s[0];
        int src_len = s.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = _length;
        dest_slice->data = (char*)pool->allocate(dest_slice->size);
        memcpy(dest_slice->data, src_value, src_len);
        memset(dest_slice->data + src_len, 0, dest_slice->size - src_len);
    } else if (fieldType == OLAP_FIELD_TYPE_VARCHAR) {
        std::string s = std::to_string(src);
        char* src_value = &s[0];
        int src_len = s.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = src_len;
        dest_slice->data = (char*)pool->allocate(src_len);
        memcpy(dest_slice->data, src_value, src_len);
    } else {
        *(int*)target = src;
    }
}

} // namespace starrocks
