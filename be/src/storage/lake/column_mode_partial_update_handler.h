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

#include <functional>

#include "storage/lake/cross_publish_context.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/rowset_column_update_state.h"

namespace starrocks::lake {

class LakeDeltaColumnGroupLoader : public DeltaColumnGroupLoader {
public:
    LakeDeltaColumnGroupLoader(TabletMetadataPtr tablet_metadata);
    Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) override;
    Status load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                DeltaColumnGroupList* pdcgs) override;

private:
    TabletMetadataPtr _tablet_metadata;
};

// What the inserted rows of a FLEXIBLE partial update declared, per row.
//
// The update file of a flexible load holds every column any of its rows declared, with a NULL
// placeholder in each cell a row did not declare. An update masks those cells out; an insert creates
// the row, so each of those cells must become the column's default instead, as for any column a plain
// partial update does not write. Filled by ColumnModePartialUpdateHandler::execute and consumed by
// UpdateManager::_handle_column_upsert_mode.
struct FlexibleInsertMask {
    // Per update segment, the per-row column-set id read from the hidden "__cset__" column, indexed by
    // rowid in that segment (the space insert_rowids uses; flexible loads never see a shared segment,
    // whose rowids would be offset by the tablet range).
    std::vector<std::vector<int16_t>> set_ids_by_segment;
    // set id -> the unique ids of the columns that set covers.
    std::vector<std::vector<ColumnUID>> distinct_column_sets;

    bool valid() const { return !distinct_column_sets.empty(); }
};

// Used in column mode partial update
class ColumnModePartialUpdateHandler {
public:
    ColumnModePartialUpdateHandler(int64_t base_version, int64_t txn_id, MemTracker* tracker);
    ~ColumnModePartialUpdateHandler();

    // |flexible_insert_mask|, when not null, receives what the inserted rows of a flexible load declared.
    Status execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder,
                   std::vector<std::vector<uint32_t>>* insert_rowids_by_segment = nullptr,
                   FlexibleInsertMask* flexible_insert_mask = nullptr);

private:
    Status _load_update_state(const RowsetUpdateStateParams& params);
    StatusOr<int64_t> _calc_upt_memory_usage_per_row(const TabletSchema& tablet_schema);
    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_delta_column_group_writer(
            const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& tschema);
    // Merges the update files into one streamed range of a source segment. The only entry point of the
    // merge: it sends a flexible load through _update_source_chunk_by_upt_flexible and every other load
    // through _update_source_chunk_by_upt.
    Status _update_source_chunk(const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& partial_schema,
                                const std::vector<ColumnUID>& selective_unique_update_column_ids,
                                StreamChunkContainer container, int32_t condition_idx_in_partial_schema);
    // Overlays every update-file column onto every matched source row. Refuses a flexible load, whose
    // update files hold NULL placeholders in the cells a row did not declare.
    Status _update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& partial_schema,
                                       StreamChunkContainer container, int32_t condition_idx_in_partial_schema);
    // Flexible partial update: overlays an update-file column onto a source row only when the column-set
    // of that update row covers the column; every other cell keeps the value already in the source
    // range (read through the existing delta column groups). The result is written as an ordinary
    // row-complete `.cols` file.
    Status _update_source_chunk_by_upt_flexible(const UptidToRowidPairs& upt_id_to_rowid_pairs,
                                                const Schema& partial_schema,
                                                const std::vector<ColumnUID>& selective_unique_update_column_ids,
                                                StreamChunkContainer container);
    bool _is_flexible() const { return !_flexible_column_sets.empty(); }
    Status _read_from_source_segment_and_update(const RowsetUpdateStateParams& params, const Schema& schema,
                                                uint32_t rssid,
                                                const std::function<Status(StreamChunkContainer)>& update_func);
    // Resolve txn_meta.merge_condition() to a column id in `tschema`.
    // Returns -1 when no condition is set, or an error when the named column is missing from the schema.
    static StatusOr<int32_t> _resolve_condition_cid(const RowsetTxnMetaPB& txn_meta, const TabletSchema& tschema);
    // Locate `condition_cid` inside a per-batch partial column id list. Returns an error when the
    // condition column is not present in the batch — with the single-batch invariant this indicates
    // a logic bug (delta_writer should have rejected it), so propagate it loudly rather than silently
    // falling back to the no-condition path.
    static StatusOr<int32_t> _locate_condition_idx_in_partial_schema(
            const std::vector<ColumnId>& selective_update_column_ids, int32_t condition_cid);

private:
    // params
    int64_t _base_version = 0;
    int64_t _txn_id = 0;
    MemTracker* _tracker = nullptr;
    // Used for release memory to tracker when meet failure.
    int64_t _memory_usage = 0;

    // maintain the reference from rowids in segment files been updated to rowids in update files.
    std::vector<ColumnPartialUpdateState> _partial_update_states;

    // `_rowset_meta_ptr` contains full life cycle rowset meta in `_rowset_ptr`.
    RowsetMetadataUniquePtr _rowset_meta_ptr;
    std::unique_ptr<Rowset> _rowset_ptr;
    // Only a SPLIT child's cross publish builds one. Outlives the SegmentPKIterators that reference it.
    CrossPublishRowSelectorPtr _row_selector;
    int64_t _upt_memory_usage_per_row = 0;

    // Flexible partial update, empty otherwise. Set once in execute() before any merge task starts and
    // read-only afterwards.
    // set id -> the unique ids of the columns that set covers (RowsetTxnMetaPB.distinct_column_sets).
    std::vector<std::vector<ColumnUID>> _flexible_column_sets;
    // Per update segment (index == upt_id), the column-set id of each update row.
    std::vector<std::vector<int16_t>> _flexible_set_ids;
};

class CompactionUpdateConflictChecker {
public:
    static bool conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                               const TabletMetadata& metadata, MetaFileBuilder* builder);
};

} // namespace starrocks::lake
