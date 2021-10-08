// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_schema.h

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

#ifndef STARROCKS_BE_SRC_OLAP_TABLET_SCHEMA_H
#define STARROCKS_BE_SRC_OLAP_TABLET_SCHEMA_H

#include <gtest/gtest_prod.h>

#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "storage/olap_define.h"
#include "storage/types.h"
#include "storage/vectorized/type_utils.h"

namespace starrocks {

namespace segment_v2 {
class SegmentReaderWriterTest;
class SegmentReaderWriterTest_estimate_segment_size_Test;
class SegmentReaderWriterTest_TestStringDict_Test;
} // namespace segment_v2

class TabletColumn {
public:
    TabletColumn();
    TabletColumn(FieldAggregationMethod agg, FieldType type);
    TabletColumn(FieldAggregationMethod agg, FieldType field_type, bool is_nullable);
    TabletColumn(FieldAggregationMethod agg, FieldType field_type, bool is_nullable, int32_t unique_id, size_t length);
    void init_from_pb(const ColumnPB& column);
    void to_schema_pb(ColumnPB* column) const;

    inline int32_t unique_id() const { return _unique_id; }
    inline std::string name() const { return _col_name; }
    inline FieldType type() const { return _type; }
    inline bool is_key() const { return _is_key; }
    inline bool is_nullable() const { return _is_nullable; }
    inline bool is_bf_column() const { return _is_bf_column; }
    inline bool has_bitmap_index() const { return _has_bitmap_index; }
    bool has_default_value() const { return _has_default_value; }
    std::string default_value() const { return _default_value; }
    bool has_reference_column() const { return _has_referenced_column; }
    int32_t referenced_column_id() const { return _referenced_column_id; }
    std::string referenced_column() const { return _referenced_column; }
    size_t length() const { return _length; }
    size_t index_length() const { return _index_length; }
    FieldAggregationMethod aggregation() const { return _aggregation; }
    int precision() const { return _precision; }
    int scale() const { return _scale; }
    bool visible() const { return _visible; }
    void add_sub_column(TabletColumn& sub_column);
    uint32_t get_subtype_count() const { return _sub_columns.size(); }
    const TabletColumn& get_sub_column(uint32_t i) const { return _sub_columns[i]; }

    friend bool operator==(const TabletColumn& a, const TabletColumn& b);
    friend bool operator!=(const TabletColumn& a, const TabletColumn& b);

    static std::string get_string_by_field_type(FieldType type);
    static std::string get_string_by_aggregation_type(FieldAggregationMethod aggregation_type);
    static FieldType get_field_type_by_string(const std::string& str);
    static FieldAggregationMethod get_aggregation_type_by_string(const std::string& str);
    static uint32_t get_field_length_by_type(FieldType type, uint32_t string_length);

    std::string debug_string() const;

    bool is_format_v1_column() const;
    bool is_format_v2_column() const;

    int64_t mem_usage() const {
        int64_t mem_usage =
                sizeof(TabletColumn) + _col_name.length() + _default_value.length() + _referenced_column.length();
        for (const auto& col : _sub_columns) {
            mem_usage += col.mem_usage();
        }
        return mem_usage;
    }

private:
    int32_t _unique_id;
    std::string _col_name;
    FieldType _type;
    bool _is_key = false;
    FieldAggregationMethod _aggregation{OLAP_FIELD_AGGREGATION_NONE};
    bool _is_nullable = false;

    bool _has_default_value = false;
    std::string _default_value;

    bool _is_decimal = false;
    int32_t _precision;
    int32_t _scale;

    int32_t _length;
    int32_t _index_length;

    bool _is_bf_column = false;

    bool _has_referenced_column = false;
    int32_t _referenced_column_id;
    std::string _referenced_column;

    bool _has_bitmap_index = false;

    // for hidded column, which is transparent to user
    bool _visible = true;

    TabletColumn* _parent = nullptr;
    std::vector<TabletColumn> _sub_columns;
};

bool operator==(const TabletColumn& a, const TabletColumn& b);
bool operator!=(const TabletColumn& a, const TabletColumn& b);

class TabletSchema {
public:
    TabletSchema() = default;
    void init_from_pb(const TabletSchemaPB& schema);
    void to_schema_pb(TabletSchemaPB* tablet_meta_pb) const;
    size_t row_size() const;
    size_t field_index(const std::string& field_name) const;
    const TabletColumn& column(size_t ordinal) const;
    const std::vector<TabletColumn>& columns() const;
    inline size_t num_columns() const { return _num_columns; }
    inline size_t num_key_columns() const { return _num_key_columns; }
    inline size_t num_short_key_columns() const { return _num_short_key_columns; }
    inline size_t num_rows_per_row_block() const { return _num_rows_per_row_block; }
    inline KeysType keys_type() const { return _keys_type; }
    inline CompressKind compress_kind() const { return _compress_kind; }
    inline size_t next_column_unique_id() const { return _next_column_unique_id; }
    inline bool is_in_memory() const { return _is_in_memory; }
    inline void set_is_in_memory(bool is_in_memory) { _is_in_memory = is_in_memory; }

    bool contains_format_v1_column() const;
    bool contains_format_v2_column() const;

    std::unique_ptr<TabletSchema> convert_to_format(DataFormatVersion format) const;

    std::string debug_string() const;

    int64_t mem_usage() const {
        int64_t mem_usage = sizeof(TabletSchema);
        for (const auto& col : _cols) {
            mem_usage += col.mem_usage();
        }
        return mem_usage;
    }

private:
    friend class segment_v2::SegmentReaderWriterTest;
    FRIEND_TEST(segment_v2::SegmentReaderWriterTest, estimate_segment_size);
    FRIEND_TEST(segment_v2::SegmentReaderWriterTest, TestStringDict);

    friend bool operator==(const TabletSchema& a, const TabletSchema& b);
    friend bool operator!=(const TabletSchema& a, const TabletSchema& b);

    KeysType _keys_type = DUP_KEYS;
    std::vector<TabletColumn> _cols;
    size_t _num_columns = 0;
    size_t _num_key_columns = 0;
    size_t _num_null_columns = 0;
    size_t _num_short_key_columns = 0;
    size_t _num_rows_per_row_block = 0;
    CompressKind _compress_kind = COMPRESS_NONE;
    size_t _next_column_unique_id = 0;

    bool _has_bf_fpp = false;
    double _bf_fpp = 0;
    bool _is_in_memory = false;
};

bool operator==(const TabletSchema& a, const TabletSchema& b);
bool operator!=(const TabletSchema& a, const TabletSchema& b);

} // namespace starrocks

#endif // STARROCKS_BE_SRC_OLAP_TABLET_SCHEMA_H
