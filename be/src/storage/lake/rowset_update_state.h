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

#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class MetaFileBuilder;

struct PartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    std::vector<std::unique_ptr<Column>> write_columns;
};

struct AutoIncrementPartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    std::unique_ptr<Column> write_column;
    std::shared_ptr<TabletSchema> schema;
    // auto increment column id in partial segment file
    // but not in full tablet schema
    uint32_t id;
    uint32_t segment_id;
    std::vector<uint32_t> rowids;
    bool skip_rewrite;

    AutoIncrementPartialUpdateState() : schema(nullptr), id(0), segment_id(0), skip_rewrite(false) {}

    void init(std::shared_ptr<TabletSchema>& schema, uint32_t id, uint32_t segment_id) {
        this->schema = schema;
        this->id = id;
        this->segment_id = segment_id;
    }
};

class RowsetUpdateState {
public:
    using ColumnUniquePtr = std::unique_ptr<Column>;

    RowsetUpdateState();
    ~RowsetUpdateState();

    Status load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, int64_t base_version, Tablet* tablet,
                const MetaFileBuilder* builder, bool need_check_conflict, bool need_lock);

    Status rewrite_segment(const TxnLogPB_OpWrite& op_write, int64_t txn_id, const TabletMetadata& metadata,
                           Tablet* tablet, std::map<int, FileInfo>* replace_segments,
                           std::vector<std::string>* orphan_files);

    const std::vector<ColumnUniquePtr>& upserts() const { return _upserts; }
    const std::vector<ColumnUniquePtr>& deletes() const { return _deletes; }

    std::size_t memory_usage() const { return _memory_usage; }

    std::string to_string() const;

    const std::vector<PartialUpdateState>& parital_update_states() { return _partial_update_states; }

    static void plan_read_by_rssid(const std::vector<uint64_t>& rowids, size_t* num_default,
                                   std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid,
                                   std::vector<uint32_t>* idxes);

    const std::vector<std::unique_ptr<Column>>& auto_increment_deletes() const;

private:
    Status _do_load(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, Tablet* tablet, bool need_lock);

    Status _do_load_upserts_deletes(const TxnLogPB_OpWrite& op_write, const TabletSchemaCSPtr& tablet_schema,
                                    Tablet* tablet, Rowset* rowset_ptr);

    Status _prepare_partial_update_states(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                          Tablet* tablet, const TabletSchemaCSPtr& tablet_schema, bool need_lock);

    Status _resolve_conflict(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata, int64_t base_version,
                             Tablet* tablet, const MetaFileBuilder* builder);

    Status _resolve_conflict_partial_update(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                            Tablet* tablet, const std::vector<uint64_t>& new_rss_rowids,
                                            std::vector<uint32_t>& read_column_ids, uint32_t segment_id,
                                            size_t& total_conflicts, const TabletSchemaCSPtr& tablet_schema);

    Status _resolve_conflict_auto_increment(const TxnLogPB_OpWrite& op_write, const TabletMetadata& metadata,
                                            Tablet* tablet, const std::vector<uint64_t>& new_rss_rowids,
                                            uint32_t segment_id, size_t& total_conflicts,
                                            const TabletSchemaCSPtr& tablet_schema);

    Status _prepare_auto_increment_partial_update_states(const TxnLogPB_OpWrite& op_write,
                                                         const TabletMetadata& metadata, Tablet* tablet,
                                                         const TabletSchemaCSPtr& tablet_schema, bool need_lock);

    std::once_flag _load_once_flag;
    Status _status;
    // one for each segment file
    std::vector<ColumnUniquePtr> _upserts;
    // one for each delete file
    std::vector<ColumnUniquePtr> _deletes;
    size_t _memory_usage = 0;
    int64_t _tablet_id = 0;

    // TODO: dump to disk if memory usage is too large
    std::vector<PartialUpdateState> _partial_update_states;

    std::vector<AutoIncrementPartialUpdateState> _auto_increment_partial_update_states;

    std::vector<std::unique_ptr<Column>> _auto_increment_delete_pks;

    int64_t _base_version;
    const MetaFileBuilder* _builder;

    RowsetUpdateState(const RowsetUpdateState&) = delete;
    const RowsetUpdateState& operator=(const RowsetUpdateState&) = delete;
};

inline std::ostream& operator<<(std::ostream& os, const RowsetUpdateState& o) {
    os << o.to_string();
    return os;
}

} // namespace starrocks::lake
