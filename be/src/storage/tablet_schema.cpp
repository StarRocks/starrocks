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
#include <cmath>
#include <map>
#include <sstream>
#include <vector>

#include "column/schema.h"
#include "gutil/strings/substitute.h"
#include "storage/primitive/primary_key_encoder.h"
#include "storage/primitive/schema_helper.h"
#include "storage/tablet_schema_map.h"

namespace starrocks {

void TabletSchema::append_column(TabletColumn column) {
    if (column.is_key()) {
        _num_key_columns++;
    }
    _unique_id_to_index[column.unique_id()] = _num_columns;
    _cols.push_back(std::move(column));
    if (_sort_key_uids_set.count(column.unique_id()) > 0) {
        _cols[_num_columns].set_is_sort_key(true);
    }
    _num_columns++;
}

void TabletSchema::_clear_columns() {
    _unique_id_to_index.clear();
    _num_columns = 0;
    _num_key_columns = 0;
    _cols.clear();
    _sort_key_idxes.clear();
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
    partial_tablet_schema_pb.set_primary_key_encoding_type(
            PrimaryKeyEncoder::pb_from_encoding_type(src_tablet_schema->primary_key_encoding_type()));
    std::vector<ColumnId> sort_key_idxes;
    // from referenced column name to index, used for build sort key idxes later.
    std::map<std::string, uint32_t> col_name_to_idx;
    uint32_t cid = 0;
    for (const auto referenced_column_id : referenced_column_ids) {
        auto* tablet_column = partial_tablet_schema_pb.add_column();
        src_tablet_schema->column(referenced_column_id).to_schema_pb(tablet_column);
        col_name_to_idx[tablet_column->name()] = cid++;
    }
    // build sort key idxes
    for (const auto& sort_key_idx : src_tablet_schema->sort_key_idxes()) {
        std::string col_name = std::string(src_tablet_schema->column(sort_key_idx).name());
        if (col_name_to_idx.count(col_name) <= 0) {
            // sort key column is not in referenced column, skip it.
            continue;
        }
        sort_key_idxes.emplace_back(col_name_to_idx[col_name]);
    }
    const auto* indexes = src_tablet_schema->indexes();
    for (const auto& index : *indexes) {
        TabletIndexPB* index_pb = partial_tablet_schema_pb.add_table_indices();
        index.to_schema_pb(index_pb);
    }
    partial_tablet_schema_pb.mutable_sort_key_idxes()->Add(sort_key_idxes.begin(), sort_key_idxes.end());
    auto partial_schema = std::make_shared<TabletSchema>(partial_tablet_schema_pb);
    // _init_from_pb may fallback sort_key_idxes to key columns when the PB's sort_key_idxes is empty,
    // making it inconsistent with num_short_key_columns copied from the source schema. Fix up here.
    if (partial_schema->num_short_key_columns() > partial_schema->sort_key_idxes().size()) {
        partial_schema->set_num_short_key_columns(partial_schema->sort_key_idxes().size());
    }
    return partial_schema;
}

std::shared_ptr<TabletSchema> TabletSchema::create_with_uid(const TabletSchemaCSPtr& tablet_schema,
                                                            const std::vector<ColumnUID>& unique_column_ids) {
    std::unordered_set<int32_t> unique_cid_filter(unique_column_ids.begin(), unique_column_ids.end());
    std::vector<int32_t> column_indexes;
    for (int cid = 0; cid < tablet_schema->columns().size(); cid++) {
        if (unique_cid_filter.count(tablet_schema->column(cid).unique_id()) > 0) {
            column_indexes.push_back(cid);
        }
    }
    return TabletSchema::create(tablet_schema, column_indexes);
}

StatusOr<TabletSchemaSPtr> TabletSchema::create(const TabletSchema& ori_schema, int64_t schema_id, int32_t version,
                                                const POlapTableColumnParam& column_param) {
    TabletSchemaSPtr new_schema = std::make_shared<TabletSchema>(ori_schema);
    RETURN_IF_ERROR(new_schema->_build_current_tablet_schema(schema_id, version, column_param, ori_schema));
    return new_schema;
}

TabletSchema::TabletSchema(const TabletSchema& tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema.to_schema_pb(&tablet_schema_pb);
    _init_from_pb(tablet_schema_pb);
}

TabletSchemaSPtr TabletSchema::copy(const TabletSchema& tablet_schema) {
    return std::make_shared<TabletSchema>(tablet_schema);
}

TabletSchemaSPtr TabletSchema::copy(const TabletSchema& src_schema, const std::vector<TabletColumn>& cols) {
    auto dst_schema = std::make_unique<TabletSchema>(src_schema);
    dst_schema->_clear_columns();
    for (const auto& col : cols) {
        dst_schema->append_column(TabletColumn(col));
    }
    dst_schema->_generate_sort_key_idxes();
    return dst_schema;
}

TabletSchemaCSPtr TabletSchema::copy(const TabletSchema& src_schema, const std::vector<TColumn>& cols) {
    auto dst_schema = std::make_unique<TabletSchema>(src_schema);
    dst_schema->_clear_columns();
    for (const auto& col : cols) {
        dst_schema->append_column(TabletColumn(col));
    }
    dst_schema->_generate_sort_key_idxes();
    return dst_schema;
}

void TabletSchema::_fill_index_map(const TabletIndex& index) {
    const auto idx_type = index.index_type();
    if (_index_map_col_unique_id.count(idx_type) <= 0) {
        auto col_unique_id_set = std::make_shared<std::unordered_set<int32_t>>();
        _index_map_col_unique_id.emplace(idx_type, col_unique_id_set);
    }
    std::for_each(index.col_unique_ids().begin(), index.col_unique_ids().end(),
                  [&](int32_t uid) { _index_map_col_unique_id[idx_type]->insert(uid); });
}

void TabletSchema::_init_schema() const {
    starrocks::Fields fields;
    for (ColumnId cid = 0; cid < num_columns(); ++cid) {
        auto f = StorageSchemaHelper::convert_field(cid, column(cid));
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
}

TabletSchema::TabletSchema(const TabletSchemaPB& schema_pb, TabletSchemaMap* schema_map) : _schema_map(schema_map) {
    _init_from_pb(schema_pb);
}

TabletSchema::~TabletSchema() {
    if (_schema_map != nullptr) {
        _schema_map->erase(_id);
    }
}

void TabletSchema::_init_from_pb(const TabletSchemaPB& schema) {
    _id = schema.has_id() ? schema.id() : invalid_id();
    _keys_type = static_cast<uint8_t>(schema.keys_type());
    _num_key_columns = 0;
    _num_columns = 0;
    _indexes.clear();
    _index_map_col_unique_id.clear();
    _cols.clear();
    _compression_type = schema.compression_type();
    _compression_level = schema.compression_level();
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

    for (auto& index_pb : schema.table_indices()) {
        TabletIndex index;
        WARN_IF_ERROR(index.init_from_pb(index_pb), "Init from index pb error! ");
        _indexes.emplace_back(index);
        _fill_index_map(index);
    }

    // dropped_table_indices: tombstones for indexes whose TabletIndexPB was
    // removed from table_indices via the lake IDG fast path, but whose payload
    // may still exist in legacy segment footers. See proto comment.
    _dropped_index_map_col_unique_id.clear();
    for (auto& index_pb : schema.dropped_table_indices()) {
        auto idx_type = index_pb.index_type();
        auto& set_ptr = _dropped_index_map_col_unique_id[idx_type];
        if (!set_ptr) {
            set_ptr = std::make_shared<std::unordered_set<int32_t>>();
        }
        for (int32_t uid : index_pb.col_unique_id()) {
            set_ptr->insert(uid);
        }
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
            _sort_key_uids_set.emplace(uid);
        }
    } else if (!schema.sort_key_idxes().empty()) {
        _sort_key_idxes.reserve(schema.sort_key_idxes_size());
        for (auto i = 0; i < schema.sort_key_idxes_size(); ++i) {
            ColumnId cid = schema.sort_key_idxes(i);
            _sort_key_idxes.push_back(cid);
            _sort_key_uids_set.emplace(schema.column(cid).unique_id());
        }
    } else {
        _sort_key_idxes.reserve(_num_key_columns);
        for (auto i = 0; i < _num_key_columns; ++i) {
            _sort_key_idxes.push_back(i);
            _sort_key_uids_set.emplace(schema.column(i).unique_id());
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

    if (schema.has_primary_key_encoding_type()) {
        _primary_key_encoding_type = PrimaryKeyEncoder::encoding_type_from_pb(schema.primary_key_encoding_type());
    } else if (schema.keys_type() == KeysType::PRIMARY_KEYS) {
        // Compatibility fallback: schemas created before `primary_key_encoding_type` was introduced.
        // PRIMARY_KEYS tables used V1 encoding historically, so default to V1 when the field is absent.
        _primary_key_encoding_type = PrimaryKeyEncodingType::PK_ENCODING_TYPE_V1;
    } else {
        _primary_key_encoding_type = PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE;
    }
}

Status TabletSchema::_build_current_tablet_schema(int64_t schema_id, int32_t version,
                                                  const POlapTableColumnParam& column_param,
                                                  const TabletSchema& ori_tablet_schema) {
    // copy from ori_tablet_schema
    _keys_type = ori_tablet_schema.keys_type();
    _num_short_key_columns = column_param.short_key_column_count();
    _num_rows_per_row_block = ori_tablet_schema.num_rows_per_row_block();
    _compression_type = ori_tablet_schema.compression_type();
    _compression_level = ori_tablet_schema.compression_level();
    _primary_key_encoding_type = ori_tablet_schema._primary_key_encoding_type;

    // todo(yixiu): unique_id
    _next_column_unique_id = ori_tablet_schema.next_column_unique_id();
    // copy from table_schema_param
    _num_key_columns = 0;
    _num_columns = 0;
    bool has_bf_columns = false;

    _cols.clear();
    _unique_id_to_index.clear();
    _sort_key_uids.clear();

    _schema_version = version;
    _id = schema_id;
    for (auto& pcolumn : column_param.columns_desc()) {
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
    if (ori_tablet_schema.columns().back().name() == Schema::FULL_ROW_COLUMN) {
        _cols.emplace_back(ori_tablet_schema.columns().back());
    }

    if (!column_param.sort_key_uid().empty()) {
        _sort_key_idxes.clear();
        for (auto uid : column_param.sort_key_uid()) {
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
        _bf_fpp = ori_tablet_schema.bf_fpp();
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
    tablet_schema_pb->set_compression_level(_compression_level);
    tablet_schema_pb->mutable_sort_key_idxes()->Add(_sort_key_idxes.begin(), _sort_key_idxes.end());
    tablet_schema_pb->mutable_sort_key_unique_ids()->Add(_sort_key_uids.begin(), _sort_key_uids.end());
    tablet_schema_pb->set_schema_version(_schema_version);
    for (auto& index : _indexes) {
        auto* tablet_index_pb = tablet_schema_pb->add_table_indices();
        index.to_schema_pb(tablet_index_pb);
    }
    // for simplicity, we always persist the primary key encoding type even for non-cloud-native tables
    tablet_schema_pb->set_primary_key_encoding_type(
            PrimaryKeyEncoder::pb_from_encoding_type(_primary_key_encoding_type));
}

Status TabletSchema::get_indexes_for_column(int32_t col_unique_id,
                                            std::unordered_map<IndexType, TabletIndex>* res) const {
    RETURN_IF(res == nullptr, Status::InternalError("Index map should not be nullptr"));
    for (const auto& index : _indexes) {
        if (index.col_unique_ids().size() == 1) {
            if (index.col_unique_ids()[0] == col_unique_id) {
                res->emplace(index.index_type(), index);
            }
        } else if (index.col_unique_ids().size() > 1) {
            // TODO: implement multi-column index
            return Status::NotSupported("Multi-column index is not supported for now. ");
        }
    }
    return Status::OK();
}

Status TabletSchema::get_indexes_for_column(int32_t col_unique_id, IndexType index_type,
                                            std::shared_ptr<TabletIndex>& res) const {
    std::unordered_map<IndexType, TabletIndex> map_res;
    RETURN_IF_ERROR(get_indexes_for_column(col_unique_id, &map_res));
    if (!map_res.empty()) {
        const auto& it = map_res.find(index_type);
        if (it != map_res.end()) {
            res = std::make_shared<TabletIndex>(it->second);
        }
    }
    return Status::OK();
}

bool TabletSchema::has_index(int32_t col_unique_id, IndexType index_type) const {
    if (auto it = _index_map_col_unique_id.find(index_type); it != _index_map_col_unique_id.end()) {
        return it->second->count(col_unique_id) > 0;
    }
    return false;
}

bool TabletSchema::has_dropped_index(int32_t col_unique_id, IndexType index_type) const {
    if (auto it = _dropped_index_map_col_unique_id.find(index_type); it != _dropped_index_map_col_unique_id.end()) {
        return it->second->count(col_unique_id) > 0;
    }
    return false;
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

void TabletSchema::_generate_sort_key_idxes() {
    if (!_sort_key_idxes.empty()) {
        return;
    }
    if (!_sort_key_uids.empty()) {
        for (auto uid : _sort_key_uids) {
            _sort_key_idxes.emplace_back(_unique_id_to_index.at(uid));
        }
    } else {
        for (int32_t i = 0; i < _num_key_columns; i++) {
            _sort_key_idxes.emplace_back(i);
        }
    }
}

const std::vector<TabletColumn>& TabletSchema::columns() const {
    return _cols;
}

const TabletColumn& TabletSchema::column(size_t ordinal) const {
    DCHECK(ordinal < num_columns()) << "ordinal:" << ordinal << ", num_columns:" << num_columns();
    return _cols[ordinal];
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
    if (a._primary_key_encoding_type != b._primary_key_encoding_type) return false;
    return true;
}

bool operator!=(const TabletSchema& a, const TabletSchema& b) {
    return !(a == b);
}

bool TabletSchema::has_separate_sort_key() const {
    RETURN_IF(_sort_key_idxes.size() != _num_key_columns, true);
    for (size_t i = 0; i < _sort_key_idxes.size(); ++i) {
        if (_sort_key_idxes[i] != i) {
            return true;
        }
    }
    return false;
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
    ss << "],keys_type=" << static_cast<int32_t>(_keys_type) << ",num_columns=" << num_columns()
       << ",num_key_columns=" << _num_key_columns << ",num_short_key_columns=" << _num_short_key_columns
       << ",num_rows_per_row_block=" << _num_rows_per_row_block << ",next_column_unique_id=" << _next_column_unique_id
       << ",has_bf_fpp=" << _has_bf_fpp << ",bf_fpp=" << _bf_fpp;
    ss << ",primary_key_encoding_type=" << static_cast<int32_t>(_primary_key_encoding_type);
    return ss.str();
}

int64_t TabletSchema::mem_usage() const {
    int64_t mem_usage = sizeof(TabletSchema);
    for (const auto& col : _cols) {
        mem_usage += col.mem_usage();
    }
    for (const auto& index : _indexes) {
        mem_usage += index.mem_usage();
    }
    return mem_usage;
}

} // namespace starrocks
