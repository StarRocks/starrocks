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

#pragma once

#include <string>
#include <unordered_map>

#include "storage/olap_common.h"
#include "storage/primary_index.h"
#include "storage/tablet_updates.h"

namespace starrocks {

class Tablet;

struct PartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    std::vector<std::unique_ptr<Column>> write_columns;
    std::vector<uint32_t> write_columns_uid;
    ChunkPtr partial_update_value_columns; // only used for column_with_row store
    bool inited = false;
    EditVersion read_version;
    int64_t byte_size = 0;
    int32_t schema_version = -1;

    void update_byte_size() {
        for (size_t i = 0; i < write_columns.size(); i++) {
            if (write_columns[i] != nullptr) {
                byte_size += write_columns[i]->byte_size();
            }
        }
    }

    void release() {
        src_rss_rowids.clear();
        for (size_t i = 0; i < write_columns.size(); i++) {
            if (write_columns[i] != nullptr) {
                write_columns[i].reset();
            }
        }
        write_columns.clear();
        write_columns_uid.clear();
        schema_version = -1;
        byte_size = 0;
        if (partial_update_value_columns != nullptr) {
            partial_update_value_columns->reset();
        }
        inited = false;
    }
};

struct AutoIncrementPartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    std::unique_ptr<Column> write_column;
    Rowset* rowset;
    TabletSchemaCSPtr schema;
    // auto increment column id in partial segment file
    // but not in full tablet schema
    uint32_t id;
    uint32_t segment_id;
    std::vector<uint32_t> rowids;
    std::unique_ptr<Column> delete_pks;
    bool skip_rewrite;
    AutoIncrementPartialUpdateState() : rowset(nullptr), schema(nullptr), id(0), segment_id(0), skip_rewrite(false) {}

    void init(Rowset* rowset, TabletSchemaCSPtr schema, uint32_t id, uint32_t segment_id) {
        this->rowset = rowset;
        this->schema = schema;
        this->id = id;
        this->segment_id = segment_id;
    }

    void release() {
        src_rss_rowids.clear();
        rowids.clear();
        write_column.reset();
        delete_pks.reset();

        rowset = nullptr;
        schema = nullptr;
        id = 0;
        segment_id = 0;
        skip_rewrite = false;
    }
};

class RowsetUpdateState {
public:
    using ColumnUniquePtr = std::unique_ptr<Column>;

    RowsetUpdateState();
    ~RowsetUpdateState();

    Status load(Tablet* tablet, Rowset* rowset);

    Status apply(Tablet* tablet, const TabletSchemaCSPtr& tablet_schema, Rowset* rowset, uint32_t rowset_id,
                 uint32_t segment_id, EditVersion latest_applied_version, const PrimaryIndex& index,
                 std::unique_ptr<Column>& delete_pks, int64_t* append_column_size);

    const std::vector<ColumnUniquePtr>& upserts() const { return _upserts; }
    const std::vector<ColumnUniquePtr>& deletes() const { return _deletes; }

    std::size_t memory_usage() const { return _memory_usage; }

    std::string to_string() const;

    const std::vector<PartialUpdateState>& parital_update_states() { return _partial_update_states; }

    // call check conflict directly
    // only use for ut of partial update
    Status test_check_conflict(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, uint32_t segment_id,
                               EditVersion latest_applied_version, std::vector<uint32_t>& read_column_ids,
                               const PrimaryIndex& index) {
        return _check_and_resolve_conflict(tablet, rowset, rowset_id, segment_id, latest_applied_version,
                                           read_column_ids, index, tablet->tablet_schema());
    }

    static void plan_read_by_rssid(const vector<uint64_t>& rowids, size_t* num_default,
                                   std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid, vector<uint32_t>* idxes);

    Status load_deletes(Rowset* rowset, uint32_t delete_id);
    Status load_upserts(Rowset* rowset, uint32_t upsert_id);
    void release_upserts(uint32_t idx);
    void release_deletes(uint32_t idx);

private:
    Status _load_deletes(Rowset* rowset, uint32_t delete_id, Column* pk_column);
    Status _load_upserts(Rowset* rowset, uint32_t upsert_id, Column* pk_column);

    Status _do_load(Tablet* tablet, Rowset* rowset);

    Status _prepare_partial_update_value_columns(Tablet* tablet, Rowset* rowset, uint32_t idx,
                                                 const std::vector<uint32_t>& update_column_ids,
                                                 const TabletSchemaCSPtr& tablet_schema);

    // `need_lock` means whether the `_index_lock` in TabletUpdates needs to held.
    // `index_lock` is used to avoid access the PrimaryIndex at the same time as the apply thread.
    // This function will be called in two places, one is the commit phase and the other is the apply phase.
    // In rowset commit phase, `need_lock` should be set as true to prevent concurrent access.
    // In rowset apply phase, `_index_lock` is already held by apply thread, `need_lock` should be set as false
    // to avoid dead lock.
    Status _prepare_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t idx, bool need_lock,
                                          const TabletSchemaCSPtr& tablet_schema);

    Status _prepare_auto_increment_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t idx,
                                                         EditVersion latest_applied_version,
                                                         const std::vector<uint32_t>& column_id,
                                                         const TabletSchemaCSPtr& tablet_schema);

    Status _check_and_resolve_conflict(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, uint32_t segment_id,
                                       EditVersion latest_applied_version, std::vector<uint32_t>& read_column_ids,
                                       const PrimaryIndex& index, const TabletSchemaCSPtr& tablet_schema);

    Status _rebuild_partial_update_states(Tablet* tablet, Rowset* rowset, uint32_t rowset_id, uint32_t segment_id,
                                          const TabletSchemaCSPtr& tablet_schema);

    bool _check_partial_update(Rowset* rowset);

    std::once_flag _load_once_flag;
    Status _status;
    // one for each segment file
    std::vector<ColumnUniquePtr> _upserts;
    // one for each delete file
    std::vector<ColumnUniquePtr> _deletes;
    size_t _memory_usage = 0;
    int64_t _tablet_id = 0;
    TabletSchemaCSPtr _tablet_schema = nullptr;

    // column_with_row partial update states
    std::vector<uint32_t> _partial_update_value_column_ids;
    // only column added by reading rowset
    Schema _partial_update_value_columns_schema;
    std::vector<ChunkIteratorPtr> _partial_update_value_column_iterators;
    OlapReaderStatistics _partial_update_value_column_read_stats;

    // TODO: dump to disk if memory usage is too large
    std::vector<PartialUpdateState> _partial_update_states;

    std::vector<AutoIncrementPartialUpdateState> _auto_increment_partial_update_states;

    RowsetUpdateState(const RowsetUpdateState&) = delete;
    const RowsetUpdateState& operator=(const RowsetUpdateState&) = delete;
};

inline std::ostream& operator<<(std::ostream& os, const RowsetUpdateState& o) {
    os << o.to_string();
    return os;
}

} // namespace starrocks
