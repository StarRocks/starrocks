// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet_schema.cpp

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

#include "storage/tablet_schema.h"

#include <algorithm>
#include <cctype>
#include <vector>

#include "storage/vectorized/type_utils.h"

namespace starrocks {

FieldType TabletColumn::get_field_type_by_string(const std::string& type_str) {
    std::string upper_type_str = type_str;
    std::transform(type_str.begin(), type_str.end(), upper_type_str.begin(), ::toupper);
    if (upper_type_str == "TINYINT") return OLAP_FIELD_TYPE_TINYINT;
    if (upper_type_str == "SMALLINT") return OLAP_FIELD_TYPE_SMALLINT;
    if (upper_type_str == "INT") return OLAP_FIELD_TYPE_INT;
    if (upper_type_str == "BIGINT") return OLAP_FIELD_TYPE_BIGINT;
    if (upper_type_str == "LARGEINT") return OLAP_FIELD_TYPE_LARGEINT;
    if (upper_type_str == "UNSIGNED_TINYINT") return OLAP_FIELD_TYPE_UNSIGNED_TINYINT;
    if (upper_type_str == "UNSIGNED_SMALLINT") return OLAP_FIELD_TYPE_UNSIGNED_SMALLINT;
    if (upper_type_str == "UNSIGNED_INT") return OLAP_FIELD_TYPE_UNSIGNED_INT;
    if (upper_type_str == "UNSIGNED_BIGINT") return OLAP_FIELD_TYPE_UNSIGNED_BIGINT;
    if (upper_type_str == "FLOAT") return OLAP_FIELD_TYPE_FLOAT;
    if (upper_type_str == "DISCRETE_DOUBLE") return OLAP_FIELD_TYPE_DISCRETE_DOUBLE;
    if (upper_type_str == "DOUBLE") return OLAP_FIELD_TYPE_DOUBLE;
    if (upper_type_str == "CHAR") return OLAP_FIELD_TYPE_CHAR;
    if (upper_type_str == "DATE_V2") return OLAP_FIELD_TYPE_DATE_V2;
    if (upper_type_str == "DATE") return OLAP_FIELD_TYPE_DATE;
    if (upper_type_str == "DATETIME") return OLAP_FIELD_TYPE_DATETIME;
    if (upper_type_str == "TIMESTAMP") return OLAP_FIELD_TYPE_TIMESTAMP;
    if (upper_type_str == "DECIMAL_V2") return OLAP_FIELD_TYPE_DECIMAL_V2;
    if (upper_type_str == "DECIMAL") return OLAP_FIELD_TYPE_DECIMAL;
    if (upper_type_str == "VARCHAR") return OLAP_FIELD_TYPE_VARCHAR;
    if (upper_type_str == "BOOLEAN") return OLAP_FIELD_TYPE_BOOL;
    if (upper_type_str == "HLL") return OLAP_FIELD_TYPE_HLL;
    if (upper_type_str == "STRUCT") return OLAP_FIELD_TYPE_STRUCT;
    if (upper_type_str == "ARRAY") return OLAP_FIELD_TYPE_ARRAY;
    if (upper_type_str == "MAP") return OLAP_FIELD_TYPE_MAP;
    if (upper_type_str == "OBJECT") return OLAP_FIELD_TYPE_OBJECT;
    if (upper_type_str == "PERCENTILE") return OLAP_FIELD_TYPE_PERCENTILE;
    if (upper_type_str == "DECIMAL32") return OLAP_FIELD_TYPE_DECIMAL32;
    if (upper_type_str == "DECIMAL64") return OLAP_FIELD_TYPE_DECIMAL64;
    if (upper_type_str == "DECIMAL128") return OLAP_FIELD_TYPE_DECIMAL128;
    LOG(WARNING) << "invalid type string. [type='" << type_str << "']";
    return OLAP_FIELD_TYPE_UNKNOWN;
}

FieldAggregationMethod TabletColumn::get_aggregation_type_by_string(const std::string& str) {
    std::string upper_str = str;
    std::transform(str.begin(), str.end(), upper_str.begin(), ::toupper);

    if (upper_str == "NONE") return OLAP_FIELD_AGGREGATION_NONE;
    if (upper_str == "SUM") return OLAP_FIELD_AGGREGATION_SUM;
    if (upper_str == "MIN") return OLAP_FIELD_AGGREGATION_MIN;
    if (upper_str == "MAX") return OLAP_FIELD_AGGREGATION_MAX;
    if (upper_str == "REPLACE") return OLAP_FIELD_AGGREGATION_REPLACE;
    if (upper_str == "REPLACE_IF_NOT_NULL") return OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL;
    if (upper_str == "HLL_UNION") return OLAP_FIELD_AGGREGATION_HLL_UNION;
    if (upper_str == "BITMAP_UNION") return OLAP_FIELD_AGGREGATION_BITMAP_UNION;
    if (upper_str == "PERCENTILE_UNION") return OLAP_FIELD_AGGREGATION_PERCENTILE_UNION;
    LOG(WARNING) << "invalid aggregation type string. [aggregation='" << str << "']";
    return OLAP_FIELD_AGGREGATION_UNKNOWN;
}

std::string TabletColumn::get_string_by_field_type(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return "TINYINT";
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return "UNSIGNED_TINYINT";
    case OLAP_FIELD_TYPE_SMALLINT:
        return "SMALLINT";
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return "UNSIGNED_SMALLINT";
    case OLAP_FIELD_TYPE_INT:
        return "INT";
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return "UNSIGNED_INT";
    case OLAP_FIELD_TYPE_BIGINT:
        return "BIGINT";
    case OLAP_FIELD_TYPE_LARGEINT:
        return "LARGEINT";
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return "UNSIGNED_BIGINT";
    case OLAP_FIELD_TYPE_FLOAT:
        return "FLOAT";
    case OLAP_FIELD_TYPE_DOUBLE:
        return "DOUBLE";
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        return "DISCRETE_DOUBLE";
    case OLAP_FIELD_TYPE_CHAR:
        return "CHAR";
    case OLAP_FIELD_TYPE_DATE:
        return "DATE";
    case OLAP_FIELD_TYPE_DATE_V2:
        return "DATE_V2";
    case OLAP_FIELD_TYPE_DATETIME:
        return "DATETIME";
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return "TIMESTAMP";
    case OLAP_FIELD_TYPE_DECIMAL:
        return "DECIMAL";
    case OLAP_FIELD_TYPE_DECIMAL_V2:
        return "DECIMAL_V2";
    case OLAP_FIELD_TYPE_DECIMAL32:
        return "DECIMAL32";
    case OLAP_FIELD_TYPE_DECIMAL64:
        return "DECIMAL64";
    case OLAP_FIELD_TYPE_DECIMAL128:
        return "DECIMAL128";
    case OLAP_FIELD_TYPE_VARCHAR:
        return "VARCHAR";
    case OLAP_FIELD_TYPE_BOOL:
        return "BOOLEAN";
    case OLAP_FIELD_TYPE_HLL:
        return "HLL";
    case OLAP_FIELD_TYPE_STRUCT:
        return "STRUCT";
    case OLAP_FIELD_TYPE_ARRAY:
        return "ARRAY";
    case OLAP_FIELD_TYPE_MAP:
        return "MAP";
    case OLAP_FIELD_TYPE_OBJECT:
        return "OBJECT";
    case OLAP_FIELD_TYPE_PERCENTILE:
        return "PERCENTILE";
    case OLAP_FIELD_TYPE_UNKNOWN:
        return "UNKNOWN";
    case OLAP_FIELD_TYPE_NONE:
        return "NONE";
    case OLAP_FIELD_TYPE_MAX_VALUE:
        return "MAX_VALUE";
    }
    return "";
}

std::string TabletColumn::get_string_by_aggregation_type(FieldAggregationMethod type) {
    switch (type) {
    case OLAP_FIELD_AGGREGATION_NONE:
        return "NONE";
    case OLAP_FIELD_AGGREGATION_SUM:
        return "SUM";
    case OLAP_FIELD_AGGREGATION_MIN:
        return "MIN";
    case OLAP_FIELD_AGGREGATION_MAX:
        return "MAX";
    case OLAP_FIELD_AGGREGATION_REPLACE:
        return "REPLACE";
    case OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL:
        return "REPLACE_IF_NOT_NULL";
    case OLAP_FIELD_AGGREGATION_HLL_UNION:
        return "HLL_UNION";
    case OLAP_FIELD_AGGREGATION_BITMAP_UNION:
        return "BITMAP_UNION";
    case OLAP_FIELD_AGGREGATION_PERCENTILE_UNION:
        return "PERCENTILE_UNION";
    case OLAP_FIELD_AGGREGATION_UNKNOWN:
        return "UNKNOWN";
    }
    return "";
}

uint32_t TabletColumn::get_field_length_by_type(FieldType type, uint32_t string_length) {
    switch (type) {
    case OLAP_FIELD_TYPE_UNKNOWN:
    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_MAP:
    case OLAP_FIELD_TYPE_NONE:
    case OLAP_FIELD_TYPE_MAX_VALUE:
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_TINYINT:
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return 1;
    case OLAP_FIELD_TYPE_SMALLINT:
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return 2;
    case OLAP_FIELD_TYPE_DATE:
        return 3;
    case OLAP_FIELD_TYPE_INT:
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
    case OLAP_FIELD_TYPE_FLOAT:
    case OLAP_FIELD_TYPE_DATE_V2:
    case OLAP_FIELD_TYPE_DECIMAL32:
        return 4;
    case OLAP_FIELD_TYPE_BIGINT:
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
    case OLAP_FIELD_TYPE_DOUBLE:
    case OLAP_FIELD_TYPE_DATETIME:
    case OLAP_FIELD_TYPE_TIMESTAMP:
    case OLAP_FIELD_TYPE_DECIMAL64:
        return 8;
    case OLAP_FIELD_TYPE_DECIMAL:
        return 12;
    case OLAP_FIELD_TYPE_LARGEINT:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_DECIMAL_V2:
    case OLAP_FIELD_TYPE_DECIMAL128:
        return 16;
    case OLAP_FIELD_TYPE_CHAR:
        return string_length;
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_PERCENTILE:
        return string_length + sizeof(OLAP_STRING_MAX_LENGTH);
    case OLAP_FIELD_TYPE_ARRAY:
        return string_length;
    }
    return 0;
}

TabletColumn::TabletColumn() {}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType type) {
    _aggregation = agg;
    _type = type;
}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType field_type, bool is_nullable) {
    _aggregation = agg;
    _type = field_type;
    _length = get_type_info(field_type)->size();
    _is_nullable = is_nullable;
}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType field_type, bool is_nullable, int32_t unique_id,
                           size_t length) {
    _aggregation = agg;
    _type = field_type;
    _is_nullable = is_nullable;
    _unique_id = unique_id;
    _length = length;
}

void TabletColumn::init_from_pb(const ColumnPB& column) {
    _unique_id = column.unique_id();
    _col_name = column.name();
    _type = TabletColumn::get_field_type_by_string(column.type());
    _is_key = column.is_key();
    _is_nullable = column.is_nullable();

    _has_default_value = column.has_default_value();
    if (_has_default_value) {
        _default_value = column.default_value();
    }

    if (column.has_precision()) {
        _is_decimal = true;
        _precision = column.precision();
    } else {
        _is_decimal = false;
    }
    if (column.has_frac()) {
        _scale = column.frac();
    }
    _length = column.length();
    _index_length = column.index_length();
    if (column.has_is_bf_column()) {
        _is_bf_column = column.is_bf_column();
    } else {
        _is_bf_column = false;
    }
    if (column.has_has_bitmap_index()) {
        _has_bitmap_index = column.has_bitmap_index();
    } else {
        _has_bitmap_index = false;
    }
    _has_referenced_column = column.has_referenced_column_id();
    if (_has_referenced_column) {
        _referenced_column_id = column.referenced_column_id();
    }
    if (column.has_aggregation()) {
        _aggregation = get_aggregation_type_by_string(column.aggregation());
    }
    if (column.has_visible()) {
        _visible = column.visible();
    }
    for (size_t i = 0; i < column.children_columns_size(); ++i) {
        TabletColumn sub_column;
        sub_column.init_from_pb(column.children_columns(i));
        add_sub_column(sub_column);
    }
}

void TabletColumn::to_schema_pb(ColumnPB* column) const {
    column->set_unique_id(_unique_id);
    column->set_name(_col_name);
    column->set_type(get_string_by_field_type(_type));
    column->set_is_key(_is_key);
    column->set_is_nullable(_is_nullable);
    if (_has_default_value) {
        column->set_default_value(_default_value);
    }
    if (_is_decimal) {
        column->set_precision(_precision);
        column->set_frac(_scale);
    }
    column->set_length(_length);
    column->set_index_length(_index_length);
    if (_is_bf_column) {
        column->set_is_bf_column(_is_bf_column);
    }
    column->set_aggregation(get_string_by_aggregation_type(_aggregation));
    if (_has_referenced_column) {
        column->set_referenced_column_id(_referenced_column_id);
    }
    if (_has_bitmap_index) {
        column->set_has_bitmap_index(_has_bitmap_index);
    }
    for (const auto& sub_column : _sub_columns) {
        sub_column.to_schema_pb(column->add_children_columns());
    }
}

void TabletColumn::add_sub_column(TabletColumn& sub_column) {
    _sub_columns.push_back(sub_column);
    sub_column._parent = this;
}

bool TabletColumn::is_format_v1_column() const {
    return TypeUtils::specific_type_of_format_v1(_type);
}

bool TabletColumn::is_format_v2_column() const {
    return TypeUtils::specific_type_of_format_v2(_type);
}

void TabletSchema::init_from_pb(const TabletSchemaPB& schema) {
    _keys_type = schema.keys_type();
    _num_columns = 0;
    _num_key_columns = 0;
    _num_null_columns = 0;
    _cols.clear();
    for (auto& column_pb : schema.column()) {
        TabletColumn column;
        column.init_from_pb(column_pb);
        _cols.push_back(column);
        _num_columns++;
        if (column.is_key()) {
            _num_key_columns++;
        }
        if (column.is_nullable()) {
            _num_null_columns++;
        }
    }
    _num_short_key_columns = schema.num_short_key_columns();
    _num_rows_per_row_block = schema.num_rows_per_row_block();
    _compress_kind = schema.compress_kind();
    _next_column_unique_id = schema.next_column_unique_id();
    if (schema.has_bf_fpp()) {
        _has_bf_fpp = true;
        _bf_fpp = schema.bf_fpp();
    } else {
        _has_bf_fpp = false;
        _bf_fpp = BLOOM_FILTER_DEFAULT_FPP;
    }
    _is_in_memory = schema.is_in_memory();
}

void TabletSchema::to_schema_pb(TabletSchemaPB* tablet_meta_pb) const {
    tablet_meta_pb->set_keys_type(_keys_type);
    for (auto& col : _cols) {
        ColumnPB* column = tablet_meta_pb->add_column();
        col.to_schema_pb(column);
    }
    tablet_meta_pb->set_num_short_key_columns(_num_short_key_columns);
    tablet_meta_pb->set_num_rows_per_row_block(_num_rows_per_row_block);
    tablet_meta_pb->set_compress_kind(_compress_kind);
    if (_has_bf_fpp) {
        tablet_meta_pb->set_bf_fpp(_bf_fpp);
    }
    tablet_meta_pb->set_next_column_unique_id(_next_column_unique_id);
    tablet_meta_pb->set_is_in_memory(_is_in_memory);
}

bool TabletSchema::contains_format_v1_column() const {
    return std::any_of(_cols.begin(), _cols.end(), [](const TabletColumn& col) { return col.is_format_v1_column(); });
}

bool TabletSchema::contains_format_v2_column() const {
    return std::any_of(_cols.begin(), _cols.end(), [](const TabletColumn& col) { return col.is_format_v2_column(); });
}

std::unique_ptr<TabletSchema> TabletSchema::convert_to_format(DataFormatVersion format) const {
    TabletSchemaPB schema_pb;
    to_schema_pb(&schema_pb);
    int n = schema_pb.column_size();
    for (int i = 0; i < n; i++) {
        auto* col_pb = schema_pb.mutable_column(i);
        auto t1 = column(i).type();
        auto t2 = TypeUtils::convert_to_format(t1, format);
        if (UNLIKELY(t2 == OLAP_FIELD_TYPE_UNKNOWN)) {
            return nullptr;
        }
        if (t1 != t2) {
            auto* type_info = get_scalar_type_info(t2);
            col_pb->set_type(TabletColumn::get_string_by_field_type(t2));
            col_pb->set_length(type_info->size());
            col_pb->set_index_length(type_info->size());
        }
    }
    auto schema = std::make_unique<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

size_t TabletSchema::row_size() const {
    size_t size = 0;
    for (auto& column : _cols) {
        size += column.length();
    }
    size += (_num_columns + 7) / 8;

    return size;
}

size_t TabletSchema::field_index(const std::string& field_name) const {
    bool field_exist = false;
    int ordinal = -1;
    for (auto& column : _cols) {
        ordinal++;
        if (column.name() == field_name) {
            field_exist = true;
            break;
        }
    }
    return field_exist ? ordinal : -1;
}

const std::vector<TabletColumn>& TabletSchema::columns() const {
    return _cols;
}

const TabletColumn& TabletSchema::column(size_t ordinal) const {
    DCHECK(ordinal < _num_columns) << "ordinal:" << ordinal << ", _num_columns:" << _num_columns;
    return _cols[ordinal];
}

bool operator==(const TabletColumn& a, const TabletColumn& b) {
    if (a._unique_id != b._unique_id) return false;
    if (a._col_name != b._col_name) return false;
    if (a._type != b._type) return false;
    if (a._is_key != b._is_key) return false;
    if (a._aggregation != b._aggregation) return false;
    if (a._is_nullable != b._is_nullable) return false;
    if (a._has_default_value != b._has_default_value) return false;
    if (a._has_default_value) {
        if (a._default_value != b._default_value) return false;
    }
    if (a._is_decimal != b._is_decimal) return false;
    if (a._is_decimal) {
        if (a._precision != b._precision) return false;
        if (a._scale != b._scale) return false;
    }
    if (a._length != b._length) return false;
    if (a._index_length != b._index_length) return false;
    if (a._is_bf_column != b._is_bf_column) return false;
    if (a._has_referenced_column != b._has_referenced_column) return false;
    if (a._has_referenced_column) {
        if (a._referenced_column_id != b._referenced_column_id) return false;
        if (a._referenced_column != b._referenced_column) return false;
    }
    if (a._has_bitmap_index != b._has_bitmap_index) return false;
    return true;
}

bool operator!=(const TabletColumn& a, const TabletColumn& b) {
    return !(a == b);
}

bool operator==(const TabletSchema& a, const TabletSchema& b) {
    if (a._keys_type != b._keys_type) return false;
    if (a._cols.size() != b._cols.size()) return false;
    for (int i = 0; i < a._cols.size(); ++i) {
        if (a._cols[i] != b._cols[i]) return false;
    }
    if (a._num_columns != b._num_columns) return false;
    if (a._num_key_columns != b._num_key_columns) return false;
    if (a._num_null_columns != b._num_null_columns) return false;
    if (a._num_short_key_columns != b._num_short_key_columns) return false;
    if (a._num_rows_per_row_block != b._num_rows_per_row_block) return false;
    if (a._compress_kind != b._compress_kind) return false;
    if (a._next_column_unique_id != b._next_column_unique_id) return false;
    if (a._has_bf_fpp != b._has_bf_fpp) return false;
    if (a._has_bf_fpp) {
        if (std::abs(a._bf_fpp - b._bf_fpp) > 1e-6) return false;
    }
    if (a._is_in_memory != b._is_in_memory) return false;
    return true;
}

std::string TabletColumn::debug_string() const {
    std::stringstream ss;
    ss << "(unique_id=" << _unique_id << ",name=" << _col_name << ",type=" << _type << ",is_key=" << _is_key
       << ",aggregation=" << _aggregation << ",is_nullable=" << _is_nullable
       << ",has_default_value=" << _has_default_value << ",default_value=" << _default_value
       << ",is_decimal=" << _is_decimal << ",precision=" << _precision << ",frac=" << _scale << ",length=" << _length
       << ",index_length=" << _index_length << ",is_bf_column=" << _is_bf_column
       << ",has_reference_column=" << _has_referenced_column << ",referenced_column_id=" << _referenced_column_id
       << ",referenced_column=" << _referenced_column << ",has_bitmap_index=" << _has_bitmap_index << ")";
    return ss.str();
}

bool operator!=(const TabletSchema& a, const TabletSchema& b) {
    return !(a == b);
}

std::string TabletSchema::debug_string() const {
    std::stringstream ss;
    ss << "column=[";
    for (int i = 0; i < _cols.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << _cols[i].debug_string();
    }
    ss << "],keys_type=" << _keys_type << ",num_columns=" << _num_columns << ",num_key_columns=" << _num_key_columns
       << ",num_null_columns=" << _num_null_columns << ",num_short_key_columns=" << _num_short_key_columns
       << ",num_rows_per_row_block=" << _num_rows_per_row_block << ",compress_kind=" << _compress_kind
       << ",next_column_unique_id=" << _next_column_unique_id << ",has_bf_fpp=" << _has_bf_fpp << ",bf_fpp=" << _bf_fpp
       << ",is_in_memory=" << _is_in_memory;
    return ss.str();
}

} // namespace starrocks
