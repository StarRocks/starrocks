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

#include "gutil/macros.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

class RssidFileInfoContainer;

struct PartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    std::vector<std::unique_ptr<Column>> write_columns;
    void reset() {
        src_rss_rowids.clear();
        write_columns.clear();
    }
    size_t memory_usage() const {
        size_t memory_usage = 0;
        for (const auto& col : write_columns) {
            if (col != nullptr) {
                memory_usage += col->memory_usage();
            }
        }
        return memory_usage;
    }
};

struct AutoIncrementPartialUpdateState {
    std::vector<uint64_t> src_rss_rowids;
    // Container used to store the values of auto increment columns
    std::unique_ptr<Column> write_column;
    // Schema of modified columns
    std::shared_ptr<TabletSchema> schema;
    // auto increment column id in partial segment file
    // but not in full tablet schema
    uint32_t id;
    uint32_t segment_id;
    std::vector<uint32_t> rowids;
    bool skip_rewrite;

    AutoIncrementPartialUpdateState() : schema(nullptr), id(0), segment_id(0), skip_rewrite(false) {}

    void init(std::shared_ptr<TabletSchema> modified_schema, uint32_t id, uint32_t segment_id) {
        this->schema = std::move(modified_schema);
        this->id = id;
        this->segment_id = segment_id;
    }
    void reset() {
        src_rss_rowids.clear();
        write_column.reset();
        schema.reset();
        rowids.clear();
    }
    size_t memory_usage() const { return write_column ? write_column->memory_usage() : 0; }
};

struct RowsetUpdateStateParams {
    const TxnLogPB_OpWrite& op_write;
    const TabletSchemaPtr& tablet_schema;
    const TabletMetadataPtr& metadata;
    const Tablet* tablet;
    const RssidFileInfoContainer& container;
};

class RowsetUpdateState {
public:
    using ColumnUniquePtr = std::unique_ptr<Column>;

    RowsetUpdateState();
    ~RowsetUpdateState();

    DISALLOW_COPY_AND_MOVE(RowsetUpdateState);

    // How to use `RowsetUpdateState` when publish:
    //
    // init()
    //
    // for each segment:
    //      load_segment()
    //      rewrite_segment()
    //      ...
    //      release_segment()
    //
    // for each del file:
    //      load_delete()
    //      ...
    //      release_delete()

    // init params in RowsetUpdateState.
    void init(const RowsetUpdateStateParams& params);

    // Load `segment_id`-th segment file's state.
    Status load_segment(uint32_t segment_id, const RowsetUpdateStateParams& params, int64_t base_version,
                        bool need_resolve_conflict, bool need_lock);

    // Handle `segment_id`-th segment file's partial update request.
    Status rewrite_segment(uint32_t segment_id, const RowsetUpdateStateParams& params,
                           std::map<int, FileInfo>* replace_segments, std::vector<std::string>* orphan_files);

    // Release `segment_id`-th segment file's state.
    void release_segment(uint32_t segment_id);

    // Load `del_id`-th delete file's state.
    Status load_delete(uint32_t del_id, const RowsetUpdateStateParams& params);

    // Release `del_id`-th delete file's state.
    void release_delete(uint32_t del_id);

    const ColumnUniquePtr& upserts(uint32_t segment_id) const { return _upserts[segment_id]; }
    const ColumnUniquePtr& deletes(uint32_t segment_id) const { return _deletes[segment_id]; }

    std::size_t memory_usage() const { return _memory_usage; }

    std::string to_string() const;

    const PartialUpdateState& parital_update_states(uint32_t segment_id) { return _partial_update_states[segment_id]; }

    static void plan_read_by_rssid(const std::vector<uint64_t>& rowids, size_t* num_default,
                                   std::map<uint32_t, std::vector<uint32_t>>* rowids_by_rssid,
                                   std::vector<uint32_t>* idxes);

    const ColumnUniquePtr& auto_increment_deletes(uint32_t segment_id) const;

    static StatusOr<bool> file_exist(const std::string& full_path);

private:
    // Load segment state
    Status _do_load_upserts(uint32_t segment_id, const RowsetUpdateStateParams& params);

    Status _prepare_partial_update_states(uint32_t segment_id, const RowsetUpdateStateParams& params, bool need_lock);

    Status _prepare_auto_increment_partial_update_states(uint32_t segment_id, const RowsetUpdateStateParams& params,
                                                         bool need_lock);

    // resolve conflict when publish transaction
    Status _resolve_conflict(uint32_t segment_id, const RowsetUpdateStateParams& params, int64_t base_version);

    Status _resolve_conflict_partial_update(const RowsetUpdateStateParams& params,
                                            const std::vector<uint64_t>& new_rss_rowids,
                                            std::vector<uint32_t>& read_column_ids, uint32_t segment_id,
                                            size_t& total_conflicts);

    Status _resolve_conflict_auto_increment(const RowsetUpdateStateParams& params,
                                            const std::vector<uint64_t>& new_rss_rowids, uint32_t segment_id,
                                            size_t& total_conflicts);

    void _reset();

    // one for each segment file
    std::vector<ColumnUniquePtr> _upserts;
    // one for each delete file
    std::vector<ColumnUniquePtr> _deletes;
    size_t _memory_usage = 0;
    int64_t _tablet_id = 0;
    // Because we can load partial segments when preload, so need vector to track their version.
    std::vector<int64_t> _base_versions;
    int64_t _schema_version = 0;

    // TODO: dump to disk if memory usage is too large
    std::vector<PartialUpdateState> _partial_update_states;

    std::vector<AutoIncrementPartialUpdateState> _auto_increment_partial_update_states;

    std::vector<ColumnUniquePtr> _auto_increment_delete_pks;

    // `_rowset_meta_ptr` contains full life cycle rowset meta in `_rowset_ptr`.
    RowsetMetadataUniquePtr _rowset_meta_ptr;
    std::unique_ptr<Rowset> _rowset_ptr;

    // to be destructed after segment iters
    OlapReaderStatistics _stats;
    std::vector<ChunkIteratorPtr> _segment_iters;
    std::map<string, string> _column_to_expr_value;
};

inline std::ostream& operator<<(std::ostream& os, const RowsetUpdateState& o) {
    os << o.to_string();
    return os;
}

} // namespace starrocks::lake
