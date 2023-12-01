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

#include <gen_cpp/descriptors.pb.h>

#include <algorithm>
#include <vector>

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/metadata_util.h"
#include "storage/tablet_schema_map.h"
#include "storage/type_utils.h"
#include "tablet_meta.h"

namespace starrocks {

size_t TabletColumn::estimate_field_size(size_t variable_length) const {
    return TypeUtils::estimate_field_size(_type, variable_length);
}

uint32_t TabletColumn::get_field_length_by_type(LogicalType type, uint32_t string_length) {
    switch (type) {
    case TYPE_UNKNOWN:
    case TYPE_DISCRETE_DOUBLE:
    case TYPE_STRUCT:
    case TYPE_MAP:
    case TYPE_NONE:
    case TYPE_NULL:
    case TYPE_FUNCTION:
    case TYPE_TIME:
    case TYPE_BINARY:
    case TYPE_MAX_VALUE:
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_UNSIGNED_TINYINT:
        return 1;
    case TYPE_SMALLINT:
    case TYPE_UNSIGNED_SMALLINT:
        return 2;
    case TYPE_DATE_V1:
        return 3;
    case TYPE_INT:
    case TYPE_UNSIGNED_INT:
    case TYPE_FLOAT:
    case TYPE_DATE:
    case TYPE_DECIMAL32:
        return 4;
    case TYPE_BIGINT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DOUBLE:
    case TYPE_DATETIME_V1:
    case TYPE_DATETIME:
    case TYPE_DECIMAL64:
        return 8;
    case TYPE_DECIMAL:
        return 12;
    case TYPE_LARGEINT:
    case TYPE_OBJECT:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL128:
        return 16;
    case TYPE_CHAR:
        return string_length;
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_PERCENTILE:
    case TYPE_JSON:
    case TYPE_VARBINARY:
        return string_length + sizeof(OLAP_STRING_MAX_LENGTH);
    case TYPE_ARRAY:
        return string_length;
    }
    return 0;
}

TabletColumn::TabletColumn() = default;

TabletColumn::TabletColumn(StorageAggregateType agg, LogicalType type) : _aggregation(agg), _type(type) {}

TabletColumn::TabletColumn(StorageAggregateType agg, LogicalType type, bool is_nullable)
        : _aggregation(agg), _type(type) {
    _length = get_type_info(type)->size();
    _set_flag(kIsNullableShift, is_nullable);
}

TabletColumn::TabletColumn(StorageAggregateType agg, LogicalType type, bool is_nullable, int32_t unique_id,
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

TabletColumn::TabletColumn(const ColumnPB& column) {
    init_from_pb(column);
}

TabletColumn::TabletColumn(const TColumn& column) {
    init_from_thrift(column);
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
    _type = string_to_logical_type(column.type());

    // NOTE(alvin): Change _type to format v2 type to have TableColumn has only storage V2 format.
    bool is_format_v1 = TypeUtils::specific_type_of_format_v1(_type);
    if (is_format_v1) {
        _type = TypeUtils::to_storage_format_v2(_type);
        auto type_info = get_type_info(_type);
        _length = type_info->size();
        if (column.has_index_length()) {
            _index_length = type_info->size();
        }
    } else {
        _length = column.length();
        if (column.has_index_length()) {
            // https://github.com/StarRocks/starrocks/issues/677
            // DCHECK_LE(column.index_length(), UINT8_MAX);
            _index_length = column.index_length();
        }
    }

    _set_flag(kIsKeyShift, column.is_key());
    _set_flag(kIsNullableShift, column.is_nullable());
    _set_flag(kIsBfColumnShift, column.is_bf_column());
    _set_flag(kHasBitmapIndexShift, column.has_bitmap_index());
    _set_flag(kHasPrecisionShift, column.has_precision());
    _set_flag(kHasScaleShift, column.has_frac());
    _set_flag(kHasAutoIncrementShift, column.is_auto_increment());

    if (column.has_precision()) {
        DCHECK_LE(column.precision(), UINT8_MAX);
        _precision = column.precision();
    }
    if (column.has_frac()) {
        DCHECK_LE(column.frac(), UINT8_MAX);
        _scale = column.frac();
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

void TabletColumn::init_from_thrift(const TColumn& tcolumn) {
    _unique_id = tcolumn.col_unique_id;
    ColumnPB column_pb;
    auto shared_tcolumn_desc = std::make_shared<TColumn>(tcolumn);
    convert_to_new_version(shared_tcolumn_desc.get());

    t_column_to_pb_column(_unique_id, *shared_tcolumn_desc, &column_pb);
    init_from_pb(column_pb);
}

void TabletColumn::to_schema_pb(ColumnPB* column) const {
    column->mutable_name()->assign(_col_name.data(), _col_name.size());
    column->set_unique_id(_unique_id);
    column->set_type(logical_type_to_string(_type));
    column->set_is_key(is_key());
    column->set_is_nullable(is_nullable());
    column->set_is_auto_increment(is_auto_increment());
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

void TabletSchema::append_column(TabletColumn column) {
    if (column.is_key()) {
        _num_key_columns++;
    }
    _unique_id_to_index[column.unique_id()] = _num_columns;
    _cols.push_back(std::move(column));
    if (_sort_key_idxes_set.count(_num_columns) > 0) {
        _cols[_num_columns].set_is_sort_key(true);
    }
    _num_columns++;
}

void TabletSchema::clear_columns() {
    _unique_id_to_index.clear();
    _num_columns = 0;
    _num_key_columns = 0;
    _cols.clear();
}

void TabletSchema::copy_from(const std::shared_ptr<const TabletSchema>& tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema->to_schema_pb(&tablet_schema_pb);
    _init_from_pb(tablet_schema_pb);
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage())
}

void TabletColumn::add_sub_column(const TabletColumn& sub_column) {
    _get_or_alloc_extra_fields()->sub_columns.push_back(sub_column);
}

void TabletColumn::add_sub_column(TabletColumn&& sub_column) {
    _get_or_alloc_extra_fields()->sub_columns.emplace_back(std::move(sub_column));
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

// Be careful
// When you use this function to create a new partial tablet schema, please make sure `referenced_column_ids` include
// all sort key column index of `src_tablet_schema`. Otherwise you need to recalculate the short key columns of the
// partial tablet schema
std::shared_ptr<TabletSchema> TabletSchema::create(const TabletSchemaCSPtr& src_tablet_schema,
                                                   const std::vector<int32_t>& referenced_column_ids) {
    TabletSchemaPB partial_tablet_schema_pb;
    partial_tablet_schema_pb.set_id(src_tablet_schema->id());
    partial_tablet_schema_pb.set_next_column_unique_id(src_tablet_schema->next_column_unique_id());
    partial_tablet_schema_pb.set_num_rows_per_row_block(src_tablet_schema->num_rows_per_row_block());
    partial_tablet_schema_pb.set_num_short_key_columns(src_tablet_schema->num_short_key_columns());
    partial_tablet_schema_pb.set_keys_type(src_tablet_schema->keys_type());
    if (src_tablet_schema->has_bf_fpp()) {
        partial_tablet_schema_pb.set_bf_fpp(src_tablet_schema->bf_fpp());
    }
    std::vector<ColumnId> sort_key_idxes;
    uint32_t cid = 0;
    for (const auto referenced_column_id : referenced_column_ids) {
        auto* tablet_column = partial_tablet_schema_pb.add_column();
        src_tablet_schema->column(referenced_column_id).to_schema_pb(tablet_column);
        if (src_tablet_schema->column(referenced_column_id).is_sort_key()) {
            sort_key_idxes.emplace_back(cid);
        }
        cid++;
    }
    partial_tablet_schema_pb.mutable_sort_key_idxes()->Add(sort_key_idxes.begin(), sort_key_idxes.end());
    return std::make_shared<TabletSchema>(partial_tablet_schema_pb);
}

std::shared_ptr<TabletSchema> TabletSchema::create_with_uid(const TabletSchemaCSPtr& tablet_schema,
                                                            const std::vector<uint32_t>& unique_column_ids) {
    std::unordered_set<int32_t> unique_cid_filter(unique_column_ids.begin(), unique_column_ids.end());
    std::vector<int32_t> column_indexes;
    for (int cid = 0; cid < tablet_schema->columns().size(); cid++) {
        if (unique_cid_filter.count(tablet_schema->column(cid).unique_id()) > 0) {
            column_indexes.push_back(cid);
        }
    }
    return TabletSchema::create(tablet_schema, column_indexes);
}

std::unique_ptr<TabletSchema> TabletSchema::copy(const std::shared_ptr<const TabletSchema>& tablet_schema) {
    auto t_ptr = std::make_unique<TabletSchema>();
    t_ptr->copy_from(tablet_schema);
    return t_ptr;
}

void TabletSchema::_init_schema() const {
    starrocks::Fields fields;
    for (ColumnId cid = 0; cid < num_columns(); ++cid) {
        auto f = ChunkHelper::convert_field(cid, column(cid));
        fields.emplace_back(std::make_shared<starrocks::Field>(std::move(f)));
    }
    _schema = std::make_unique<Schema>(std::move(fields), keys_type(), _sort_key_idxes);
}

Schema* TabletSchema::schema() const {
    std::call_once(_init_schema_once_flag, [this] { return _init_schema(); });
    return _schema.get();
}

TabletSchema::TabletSchema(const TabletSchemaPB& schema_pb) {
    _init_from_pb(schema_pb);
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage())
}

TabletSchema::TabletSchema(const TabletSchemaPB& schema_pb, TabletSchemaMap* schema_map) : _schema_map(schema_map) {
    _init_from_pb(schema_pb);
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage())
}

TabletSchema::~TabletSchema() {
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage())
    if (_schema_map != nullptr) {
        _schema_map->erase(_id);
    }
}

void TabletSchema::_init_from_pb(const TabletSchemaPB& schema) {
    _id = schema.has_id() ? schema.id() : invalid_id();
    _keys_type = static_cast<uint8_t>(schema.keys_type());
    _num_key_columns = 0;
    _num_columns = 0;
    _cols.clear();
    _compression_type = schema.compression_type();
    for (auto& column_pb : schema.column()) {
        TabletColumn column;
        column.init_from_pb(column_pb);
        _cols.push_back(column);
        if (column.is_key()) {
            _num_key_columns++;
        }
        _unique_id_to_index[column.unique_id()] = _num_columns;
        _num_columns++;
    }

    // There are three conditions:
    // 1. sort_key_unique_ids is not empty, sort key column should be located by unique id
    // 2. sort_key_unique_ids is empty but sort_key_idxes is not empty. This table maybe create in
    //    old version and upgrade, sort key column shoud be located by sort key column index
    // 3. both of them are empty, sort key columns are equal to key columns
    if (!schema.sort_key_unique_ids().empty()) {
        for (auto uid : schema.sort_key_unique_ids()) {
            _sort_key_uids.emplace_back(uid);
            _sort_key_idxes.emplace_back(_unique_id_to_index.at(uid));
        }
    } else if (!schema.sort_key_idxes().empty()) {
        _sort_key_idxes.reserve(schema.sort_key_idxes_size());
        for (auto i = 0; i < schema.sort_key_idxes_size(); ++i) {
            _sort_key_idxes.push_back(schema.sort_key_idxes(i));
            _sort_key_idxes_set.emplace(schema.sort_key_idxes(i));
        }
    } else {
        _sort_key_idxes.reserve(_num_key_columns);
        for (auto i = 0; i < _num_key_columns; ++i) {
            _sort_key_idxes.push_back(i);
            _sort_key_idxes_set.emplace(i);
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
    _schema_version = schema.schema_version();
}

Status TabletSchema::build_current_tablet_schema(int64_t index_id, int32_t version, const POlapTableIndexSchema& index,
                                                 const TabletSchemaCSPtr& ori_tablet_schema) {
    // copy from ori_tablet_schema
    _keys_type = ori_tablet_schema->keys_type();
    _num_short_key_columns = index.column_param().short_key_column_count();
    _num_rows_per_row_block = ori_tablet_schema->num_rows_per_row_block();
    _compression_type = ori_tablet_schema->compression_type();

    // todo(yixiu): unique_id
    _next_column_unique_id = ori_tablet_schema->next_column_unique_id();
    // copy from table_schema_param
    _num_key_columns = 0;
    _num_columns = 0;
    bool has_bf_columns = false;

    _cols.clear();
    _unique_id_to_index.clear();
    _sort_key_uids.clear();

    _schema_version = version;
    if (index.id() == index_id) {
        for (auto& pcolumn : index.column_param().columns_desc()) {
            TabletColumn column;
            column.init_from_pb(pcolumn);
            if (column.is_key()) {
                _num_key_columns++;
            }
            if (column.is_bf_column()) {
                has_bf_columns = true;
            }
            _unique_id_to_index[column.unique_id()] = _num_columns;
            _cols.emplace_back(std::move(column));
            _num_columns++;
        }
    }

    if (!index.column_param().sort_key_uid().empty()) {
        _sort_key_idxes.clear();
        for (auto uid : index.column_param().sort_key_uid()) {
            auto it = _unique_id_to_index.find(uid);
            if (it == _unique_id_to_index.end()) {
                std::string msg = strings::Substitute("sort key column uid: $0 is not exist in columns", uid);
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
            }
            _sort_key_uids.emplace_back(uid);
            _sort_key_idxes.emplace_back(it->second);
            _cols[it->second].set_is_sort_key(true);
        }
    } else {
        for (auto cid : _sort_key_idxes) {
            _cols[cid].set_is_sort_key(true);
        }
    }
    if (has_bf_columns) {
        _has_bf_fpp = true;
        _bf_fpp = ori_tablet_schema->bf_fpp();
    } else {
        _has_bf_fpp = false;
        _bf_fpp = BLOOM_FILTER_DEFAULT_FPP;
    }
    return Status::OK();
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
    tablet_schema_pb->mutable_sort_key_unique_ids()->Add(_sort_key_uids.begin(), _sort_key_uids.end());
    tablet_schema_pb->set_schema_version(_schema_version);
}

size_t TabletSchema::estimate_row_size(size_t variable_len) const {
    size_t size = 0;
    for (const auto& col : _cols) {
        size += col.estimate_field_size(variable_len);
    }
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

int32_t TabletSchema::field_index(int32_t col_unique_id) const {
    const auto& found = _unique_id_to_index.find(col_unique_id);
    return (found == _unique_id_to_index.end()) ? -1 : found->second;
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
