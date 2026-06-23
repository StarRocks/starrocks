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

#pragma once

#include <gtest/gtest_prod.h>

#include <string_view>
#include <vector>

#include "base/concurrency/once.h"
#include "base/string/c_string.h"
#include "column/column_access_path.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/primitive/aggregate_type.h"
#include "storage/primitive/primary_key_encoding_types.h"
#include "storage/primitive/storage_define.h"
#include "storage/primitive/tablet_column.h"
#include "storage/tablet_index.h"
#include "storage/types.h"
#include "types/agg_state_desc.h"

namespace starrocks {

class TabletSchemaMap;
class MemTracker;
class SegmentReaderWriterTest;
class POlapTableIndexSchema;
class Schema;
class TColumn;

class TabletIndex;

class TabletSchema {
public:
    using SchemaId = int64_t;
    using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;
    using TabletSchemaCSPtr = std::shared_ptr<const TabletSchema>;

    static TabletSchemaSPtr create(const TabletSchemaPB& schema_pb);
    static TabletSchemaSPtr create(const TabletSchemaPB& schema_pb, TabletSchemaMap* schema_map);
    static TabletSchemaSPtr create(const TabletSchemaCSPtr& tablet_schema, const std::vector<int32_t>& column_indexes);
    static TabletSchemaSPtr create_with_uid(const TabletSchemaCSPtr& tablet_schema,
                                            const std::vector<ColumnUID>& unique_column_ids);
    static StatusOr<TabletSchemaSPtr> create(const TabletSchema& ori_schema, int64_t schema_id, int32_t version,
                                             const POlapTableColumnParam& column_param);
    static TabletSchemaSPtr copy(const TabletSchema& tablet_schema);
    static TabletSchemaSPtr copy(const TabletSchema& tablet_schema, const std::vector<TabletColumn>& columns);
    static TabletSchemaCSPtr copy(const TabletSchema& src_schema, const std::vector<TColumn>& cols);

    // Must be consistent with MaterializedIndexMeta.INVALID_SCHEMA_ID defined in
    // file ./fe/fe-core/src/main/java/com/starrocks/catalog/MaterializedIndexMeta.java
    constexpr static SchemaId invalid_id() { return 0; }

    TabletSchema() = default;
    explicit TabletSchema(const TabletSchemaPB& schema_pb);
    // Does NOT take ownership of |schema_map| and |schema_map| must outlive TabletSchema.
    TabletSchema(const TabletSchemaPB& schema_pb, TabletSchemaMap* schema_map);
    TabletSchema(const TabletSchema& tablet_schema);

    ~TabletSchema();

    void to_schema_pb(TabletSchemaPB* tablet_meta_pb) const;

    // Caller should always check the returned value with `invalid_id()`.
    SchemaId id() const { return _id; }
    void set_id(SchemaId id) { _id = id; }
    size_t estimate_row_size(size_t variable_len) const;
    int32_t field_index(int32_t col_unique_id) const;
    size_t field_index(std::string_view field_name) const;
    const TabletColumn& column(size_t ordinal) const;
    const std::vector<TabletColumn>& columns() const;
    const std::vector<ColumnId> sort_key_idxes() const { return _sort_key_idxes; }

    size_t num_columns() const { return _cols.size(); }
    size_t num_key_columns() const { return _num_key_columns; }
    size_t num_short_key_columns() const { return _num_short_key_columns; }

    size_t num_rows_per_row_block() const { return _num_rows_per_row_block; }
    KeysType keys_type() const { return static_cast<KeysType>(_keys_type); }
    size_t next_column_unique_id() const { return _next_column_unique_id; }
    bool has_bf_fpp() const { return _has_bf_fpp; }
    double bf_fpp() const { return _bf_fpp; }
    CompressionTypePB compression_type() const { return _compression_type; }
    int compression_level() const { return _compression_level; }

    bool has_valid_primary_key_encoding_type() const {
        return _primary_key_encoding_type != PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE;
    }
    PrimaryKeyEncodingType primary_key_encoding_type() const { return _primary_key_encoding_type; }
    StatusOr<PrimaryKeyEncodingType> primary_key_encoding_type_or_error() const {
        if (!has_valid_primary_key_encoding_type()) {
            return Status::InternalError("tablet schema has no available primary key encoding type");
        }
        return _primary_key_encoding_type;
    }
    void append_column(TabletColumn column);

    int32_t schema_version() const { return _schema_version; }
    void set_schema_version(int32_t version) { _schema_version = version; }

    // Please call the following function with caution. Most of the time,
    // the following two functions should not be called explicitly.
    // When we do column partial update for primary key table which seperate primary keys
    // and sort keys, we will create a partial tablet schema for rowset writer. However,
    // the sort key columns maybe not exist in the partial tablet schema and the partial tablet
    // schema will keep a wrong sort key idxes and short key column num. So BE will crash in ASAN
    // mode. However, the sort_key_idxes and short_key_column_num in partial tablet schema is not
    // important actually, because the update segment file does not depend on it and the update
    // segment file will be rewrite to col file after apply. So these function are used to modify
    // the sort_key_idxes and short_key_column_num in partial tablet schema to avoid BE crash so far.
    void set_sort_key_idxes(std::vector<ColumnId> sort_key_idxes) {
        for (auto idx : _sort_key_idxes) {
            _cols[idx].set_is_sort_key(false);
        }
        _sort_key_idxes.clear();
        _sort_key_idxes.assign(sort_key_idxes.begin(), sort_key_idxes.end());
        for (auto idx : _sort_key_idxes) {
            _cols[idx].set_is_sort_key(true);
        }
    }
    void set_num_short_key_columns(uint16_t num_short_key_columns) { _num_short_key_columns = num_short_key_columns; }

    bool has_separate_sort_key() const;

    std::string debug_string() const;

    int64_t mem_usage() const;

    bool shared() const { return _schema_map != nullptr; }

    Schema* schema() const;

    const std::vector<TabletIndex>* indexes() const { return &_indexes; }
    Status get_indexes_for_column(int32_t col_unique_id, std::unordered_map<IndexType, TabletIndex>* res) const;
    Status get_indexes_for_column(int32_t col_unique_id, IndexType index_type, std::shared_ptr<TabletIndex>& res) const;
    bool has_index(int32_t col_unique_id, IndexType index_type) const;
    // Returns true if (col_unique_id, index_type) was recorded in
    // TabletSchemaPB.dropped_table_indices — i.e. the index was removed from
    // table_indices by a lake metadata-only DROP, but stale payload may
    // still live in existing segment footers. Readers use this to avoid
    // misinterpreting that payload (e.g. NGRAMBF bloom read as regular
    // bloom) until compaction rewrites the segment.
    bool has_dropped_index(int32_t col_unique_id, IndexType index_type) const;

private:
    friend class SegmentReaderWriterTest;
    FRIEND_TEST(SegmentReaderWriterTest, estimate_segment_size);
    FRIEND_TEST(SegmentReaderWriterTest, TestStringDict);

    friend bool operator==(const TabletSchema& a, const TabletSchema& b);
    friend bool operator!=(const TabletSchema& a, const TabletSchema& b);

    void _generate_sort_key_idxes();
    void _clear_columns();
    Status _build_current_tablet_schema(int64_t schema_id, int32_t version, const POlapTableColumnParam& column_param,
                                        const TabletSchema& ori_tablet_schema);

    void _init_from_pb(const TabletSchemaPB& schema);

    void _init_schema() const;
    void _fill_index_map(const TabletIndex& index);

    SchemaId _id = invalid_id();
    TabletSchemaMap* _schema_map = nullptr;

    double _bf_fpp = 0;

    std::vector<TabletIndex> _indexes;
    std::unordered_map<IndexType, std::shared_ptr<std::unordered_set<int32_t>>> _index_map_col_unique_id;
    std::unordered_map<IndexType, std::shared_ptr<std::unordered_set<int32_t>>> _dropped_index_map_col_unique_id;

    std::vector<TabletColumn> _cols;
    size_t _num_rows_per_row_block = 0;
    size_t _next_column_unique_id = 0;

    mutable uint32_t _num_columns = 0;
    mutable uint16_t _num_key_columns = 0;
    uint16_t _num_short_key_columns = 0;
    std::vector<ColumnId> _sort_key_idxes;
    std::vector<ColumnUID> _sort_key_uids;
    std::unordered_set<ColumnUID> _sort_key_uids_set;

    uint8_t _keys_type = static_cast<uint8_t>(DUP_KEYS);
    CompressionTypePB _compression_type = CompressionTypePB::LZ4_FRAME;
    // only use for zstd compression type
    int _compression_level = -1;

    std::unordered_map<int32_t, int32_t> _unique_id_to_index;

    bool _has_bf_fpp = false;

    mutable std::unique_ptr<starrocks::Schema> _schema;
    mutable std::once_flag _init_schema_once_flag;
    int32_t _schema_version = -1;

    PrimaryKeyEncodingType _primary_key_encoding_type = PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE;
};

bool operator==(const TabletSchema& a, const TabletSchema& b);
bool operator!=(const TabletSchema& a, const TabletSchema& b);

using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;
using TabletSchemaCSPtr = std::shared_ptr<const TabletSchema>;

} // namespace starrocks
