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

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/tablet_schema_map.h"
#include "storage/type_utils.h"

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
    if (upper_type_str == "JSON") return OLAP_FIELD_TYPE_JSON;
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
    case OLAP_FIELD_TYPE_JSON:
        return "JSON";
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

size_t TabletColumn::estimate_field_size(size_t variable_length) const {
    return TypeUtils::estimate_field_size(_type, variable_length);
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
    case OLAP_FIELD_TYPE_JSON:
        return string_length + sizeof(OLAP_STRING_MAX_LENGTH);
    case OLAP_FIELD_TYPE_ARRAY:
        return string_length;
    }
    return 0;
}

TabletColumn::TabletColumn() = default;

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType type) : _aggregation(agg), _type(type) {}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType type, bool is_nullable)
        : _aggregation(agg), _type(type) {
    _length = get_type_info(type)->size();
    _set_flag(kIsNullableShift, is_nullable);
}

TabletColumn::TabletColumn(FieldAggregationMethod agg, FieldType type, bool is_nullable, int32_t unique_id,
                           size_t length)
        : _unique_id(unique_id), _length(length), _aggregation(agg), _type(type) {
    _set_flag(kIsNullableShift, is_nullable);
}

TabletColumn::TabletColumn(const TabletColumn& rhs)
        : _col_name(rhs._col_name),
          _unique_id(rhs._unique_id),
          _length(rhs._length),
          _aggregation(rhs._aggregation),
          _type(rhs._type),
          _index_length(rhs._index_length),
          _precision(rhs._precision),
          _scale(rhs._scale),
          _flags(rhs._flags) {
    if (rhs._extra_fields != nullptr) {
        _extra_fields = new ExtraFields(*rhs._extra_fields);
    }
}

TabletColumn::TabletColumn(TabletColumn&& rhs) noexcept
        : _col_name(std::move(rhs._col_name)),
          _unique_id(rhs._unique_id),
          _length(rhs._length),
          _aggregation(rhs._aggregation),
          _type(rhs._type),
          _index_length(rhs._index_length),
          _precision(rhs._precision),
          _scale(rhs._scale),
          _flags(rhs._flags),
          _extra_fields(rhs._extra_fields) {
    rhs._extra_fields = nullptr;
}

TabletColumn::~TabletColumn() {
    delete _extra_fields;
}

void TabletColumn::swap(TabletColumn* rhs) {
    using std::swap;
    swap(_col_name, rhs->_col_name);
    swap(_unique_id, rhs->_unique_id);
    swap(_length, rhs->_length);
    swap(_aggregation, rhs->_aggregation);
    swap(_type, rhs->_type);
    swap(_index_length, rhs->_index_length);
    swap(_precision, rhs->_precision);
    swap(_scale, rhs->_scale);
    swap(_flags, rhs->_flags);
    swap(_extra_fields, rhs->_extra_fields);
}

TabletColumn& TabletColumn::operator=(const TabletColumn& rhs) {
    TabletColumn tmp(rhs);
    swap(&tmp);
    return *this;
}

TabletColumn& TabletColumn::operator=(TabletColumn&& rhs) noexcept {
    TabletColumn tmp(std::move(rhs));
    swap(&tmp);
    return *this;
}

void TabletColumn::init_from_pb(const ColumnPB& column) {
    _unique_id = column.unique_id();
    _col_name.assign(column.name());
    _type = TabletColumn::get_field_type_by_string(column.type());

    _set_flag(kIsKeyShift, column.is_key());
    _set_flag(kIsNullableShift, column.is_nullable());
    _set_flag(kIsBfColumnShift, column.is_bf_column());
    _set_flag(kHasBitmapIndexShift, column.has_bitmap_index());
    _set_flag(kHasPrecisionShift, column.has_precision());
    _set_flag(kHasScaleShift, column.has_frac());

    _length = column.length();

    if (column.has_precision()) {
        DCHECK_LE(column.precision(), UINT8_MAX);
        _precision = column.precision();
    }
    if (column.has_frac()) {
        DCHECK_LE(column.frac(), UINT8_MAX);
        _scale = column.frac();
    }
    if (column.has_index_length()) {
        // https://github.com/StarRocks/starrocks/issues/677
        // DCHECK_LE(column.index_length(), UINT8_MAX);
        _index_length = column.index_length();
    }
    if (column.has_aggregation()) {
        _aggregation = get_aggregation_type_by_string(column.aggregation());
    }
    if (column.has_default_value()) {
        ExtraFields* extra = _get_or_alloc_extra_fields();
        extra->has_default_value = true;
        extra->default_value = column.default_value();
    }
    for (size_t i = 0; i < column.children_columns_size(); ++i) {
        TabletColumn sub_column;
        sub_column.init_from_pb(column.children_columns(i));
        add_sub_column(std::move(sub_column));
    }
}

void TabletColumn::to_schema_pb(ColumnPB* column) const {
    column->mutable_name()->assign(_col_name.data(), _col_name.size());
    column->set_unique_id(_unique_id);
    column->set_type(get_string_by_field_type(_type));
    column->set_is_key(is_key());
    column->set_is_nullable(is_nullable());
    if (has_default_value()) {
        column->set_default_value(default_value());
    }
    if (has_precision()) {
        column->set_precision(_precision);
    }
    if (has_scale()) {
        column->set_frac(_scale);
    }
    column->set_length(_length);
    column->set_index_length(_index_length);
    column->set_is_bf_column(is_bf_column());
    column->set_aggregation(get_string_by_aggregation_type(_aggregation));
    column->set_has_bitmap_index(has_bitmap_index());
    for (int i = 0; i < subcolumn_count(); i++) {
        subcolumn(i).to_schema_pb(column->add_children_columns());
    }
}

void TabletColumn::add_sub_column(const TabletColumn& sub_column) {
    _get_or_alloc_extra_fields()->sub_columns.push_back(sub_column);
}

void TabletColumn::add_sub_column(TabletColumn&& sub_column) {
    _get_or_alloc_extra_fields()->sub_columns.emplace_back(std::move(sub_column));
}

bool TabletColumn::is_format_v1_column() const {
    return TypeUtils::specific_type_of_format_v1(_type);
}

bool TabletColumn::is_format_v2_column() const {
    return TypeUtils::specific_type_of_format_v2(_type);
}

/******************************************************************
 * TabletSchema
 ******************************************************************/

std::shared_ptr<TabletSchema> TabletSchema::create(const TabletSchemaPB& schema_pb) {
    return std::make_shared<TabletSchema>(schema_pb);
}

std::shared_ptr<TabletSchema> TabletSchema::create(const TabletSchemaPB& schema_pb, TabletSchemaMap* schema_map) {
    return std::make_shared<TabletSchema>(schema_pb, schema_map);
}

std::shared_ptr<TabletSchema> TabletSchema::create(const TabletSchema& src_tablet_schema,
                                                   const std::vector<int32_t>& referenced_column_ids) {
    TabletSchemaPB partial_tablet_schema_pb;
    partial_tablet_schema_pb.set_id(src_tablet_schema.id());
    partial_tablet_schema_pb.set_next_column_unique_id(src_tablet_schema.next_column_unique_id());
    partial_tablet_schema_pb.set_num_rows_per_row_block(src_tablet_schema.num_rows_per_row_block());
    partial_tablet_schema_pb.set_num_short_key_columns(src_tablet_schema.num_short_key_columns());
    partial_tablet_schema_pb.set_keys_type(src_tablet_schema.keys_type());
    if (src_tablet_schema.has_bf_fpp()) {
        partial_tablet_schema_pb.set_bf_fpp(src_tablet_schema.bf_fpp());
    }
    for (const auto referenced_column_id : referenced_column_ids) {
        auto* tablet_column = partial_tablet_schema_pb.add_column();
        src_tablet_schema.column(referenced_column_id).to_schema_pb(tablet_column);
    }
    return std::make_shared<TabletSchema>(partial_tablet_schema_pb);
}

void TabletSchema::_init_schema() const {
    starrocks::vectorized::Fields fields;
    for (ColumnId cid = 0; cid < num_columns(); ++cid) {
        auto f = ChunkHelper::convert_field_to_format_v2(cid, column(cid));
        fields.emplace_back(std::make_shared<starrocks::vectorized::Field>(std::move(f)));
    }
    _schema = std::make_unique<vectorized::Schema>(std::move(fields), keys_type(), _sort_key_idxes);
}

vectorized::Schema* TabletSchema::schema() const {
    std::call_once(_init_schema_once_flag, [this] { return _init_schema(); });
    return _schema.get();
}

TabletSchema::TabletSchema(const TabletSchemaPB& schema_pb) {
    _init_from_pb(schema_pb);
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage())
}

TabletSchema::TabletSchema(const TabletSchemaPB& schema_pb, TabletSchemaMap* schema_map) : _schema_map(schema_map) {
    _init_from_pb(schema_pb);
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage())
}

TabletSchema::~TabletSchema() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage())
    if (_schema_map != nullptr) {
        _schema_map->erase(_id);
    }
}

void TabletSchema::_init_from_pb(const TabletSchemaPB& schema) {
    _id = schema.has_id() ? schema.id() : invalid_id();
    _keys_type = static_cast<uint8_t>(schema.keys_type());
    _num_key_columns = 0;
    _cols.clear();
    _compression_type = schema.compression_type();
    for (auto& column_pb : schema.column()) {
        TabletColumn column;
        column.init_from_pb(column_pb);
        _cols.push_back(column);
        if (column.is_key()) {
            _num_key_columns++;
        }
    }
    if (schema.sort_key_idxes().empty()) {
        _sort_key_idxes.reserve(_num_key_columns);
        for (auto i = 0; i < _num_key_columns; ++i) {
            _sort_key_idxes.push_back(i);
        }
    } else {
        _sort_key_idxes.reserve(schema.sort_key_idxes_size());
        for (auto i = 0; i < schema.sort_key_idxes_size(); ++i) {
            _sort_key_idxes.push_back(schema.sort_key_idxes(i));
        }
    }
    for (auto cid : _sort_key_idxes) {
        _cols[cid].set_is_sort_key(true);
    }
    _num_short_key_columns = schema.num_short_key_columns();
    _num_rows_per_row_block = schema.num_rows_per_row_block();
    _next_column_unique_id = schema.next_column_unique_id();
    if (schema.has_bf_fpp()) {
        _has_bf_fpp = true;
        _bf_fpp = schema.bf_fpp();
    } else {
        _has_bf_fpp = false;
        _bf_fpp = BLOOM_FILTER_DEFAULT_FPP;
    }
}

void TabletSchema::to_schema_pb(TabletSchemaPB* tablet_schema_pb) const {
    if (_id != invalid_id()) {
        tablet_schema_pb->set_id(_id);
    }
    tablet_schema_pb->set_keys_type(static_cast<KeysType>(_keys_type));
    for (auto& col : _cols) {
        col.to_schema_pb(tablet_schema_pb->add_column());
    }
    tablet_schema_pb->set_num_short_key_columns(_num_short_key_columns);
    tablet_schema_pb->set_num_rows_per_row_block(_num_rows_per_row_block);
    if (_has_bf_fpp) {
        tablet_schema_pb->set_bf_fpp(_bf_fpp);
    }
    tablet_schema_pb->set_next_column_unique_id(_next_column_unique_id);
    tablet_schema_pb->set_compression_type(_compression_type);
    tablet_schema_pb->mutable_sort_key_idxes()->Add(_sort_key_idxes.begin(), _sort_key_idxes.end());
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
    auto schema = std::make_unique<TabletSchema>(schema_pb);
    return schema;
}

size_t TabletSchema::estimate_row_size(size_t variable_len) const {
    size_t size = 0;
    for (const auto& col : _cols) {
        size += col.estimate_field_size(variable_len);
    }
    return size;
}

size_t TabletSchema::row_size() const {
    size_t size = 0;
    for (auto& column : _cols) {
        size += column.length();
    }
    size += (num_columns() + 7) / 8;

    return size;
}

size_t TabletSchema::field_index(std::string_view field_name) const {
    int ordinal = -1;
    for (auto& column : _cols) {
        ordinal++;
        if (column.name() == field_name) {
            return ordinal;
        }
    }
    return -1;
}

const std::vector<TabletColumn>& TabletSchema::columns() const {
    return _cols;
}

const TabletColumn& TabletSchema::column(size_t ordinal) const {
    DCHECK(ordinal < num_columns()) << "ordinal:" << ordinal << ", num_columns:" << num_columns();
    return _cols[ordinal];
}

bool operator==(const TabletColumn& a, const TabletColumn& b) {
    if (a._flags != b._flags) return false;
    if (a._unique_id != b._unique_id) return false;
    if (a._col_name != b._col_name) return false;
    if (a._type != b._type) return false;
    if (a._aggregation != b._aggregation) return false;
    if (a.has_default_value() != b.has_default_value()) return false;
    if (a.has_default_value()) {
        if (a.default_value() != b.default_value()) return false;
    }
    if (a.has_precision() != b.has_precision()) return false;
    if (a.has_precision()) {
        if (a._precision != b._precision) return false;
    }
    if (a.has_scale() != b.has_scale()) return false;
    if (a.has_scale()) {
        if (a._scale != b._scale) return false;
    }
    if (a._length != b._length) return false;
    if (a._index_length != b._index_length) return false;
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
    if (a._num_key_columns != b._num_key_columns) return false;
    if (a._num_short_key_columns != b._num_short_key_columns) return false;
    if (a._num_rows_per_row_block != b._num_rows_per_row_block) return false;
    if (a._next_column_unique_id != b._next_column_unique_id) return false;
    if (a._has_bf_fpp != b._has_bf_fpp) return false;
    if (a._has_bf_fpp) {
        if (std::abs(a._bf_fpp - b._bf_fpp) > 1e-6) return false;
    }
    return true;
}

std::string TabletColumn::debug_string() const {
    std::stringstream ss;
    ss << "(unique_id=" << _unique_id << ",name=" << _col_name << ",type=" << _type << ",is_key=" << is_key()
       << ",aggregation=" << _aggregation << ",is_nullable=" << is_nullable()
       << ",default_value=" << (has_default_value() ? default_value() : "N/A")
       << ",precision=" << (has_precision() ? std::to_string(_precision) : "N/A")
       << ",frac=" << (has_scale() ? std::to_string(_scale) : "N/A") << ",length=" << _length
       << ",index_length=" << _index_length << ",is_bf_column=" << is_bf_column()
       << ",has_bitmap_index=" << has_bitmap_index() << ")";
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
    ss << "],keys_type=" << _keys_type << ",num_columns=" << num_columns() << ",num_key_columns=" << _num_key_columns
       << ",num_short_key_columns=" << _num_short_key_columns << ",num_rows_per_row_block=" << _num_rows_per_row_block
       << ",next_column_unique_id=" << _next_column_unique_id << ",has_bf_fpp=" << _has_bf_fpp << ",bf_fpp=" << _bf_fpp;
    return ss.str();
}

} // namespace starrocks
