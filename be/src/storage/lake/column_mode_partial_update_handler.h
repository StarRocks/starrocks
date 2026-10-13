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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "storage/lake/cross_publish_context.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/rowset_column_update_state.h"

namespace starrocks {
class WritableFile;
} // namespace starrocks

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

// Used in column mode partial update
class ColumnModePartialUpdateHandler {
public:
    ColumnModePartialUpdateHandler(int64_t base_version, int64_t txn_id, MemTracker* tracker);
    ~ColumnModePartialUpdateHandler();

    Status execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder,
                   std::vector<std::vector<uint32_t>>* insert_rowids_by_segment = nullptr);

private:
    Status _load_update_state(const RowsetUpdateStateParams& params);
    StatusOr<int64_t> _calc_upt_memory_usage_per_row(const TabletSchema& tablet_schema);
    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_delta_column_group_writer(
            const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& tschema);
    Status _update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& partial_schema,
                                       StreamChunkContainer container, int32_t condition_idx_in_partial_schema);

    // Partition the update rows of a column batch by source segment into a local spill file.
    //
    // A source segment needs only the rows of each update file that land in it, but the update files
    // are ordered by key, not by source segment, so the per-segment apply used to read every update
    // file it touched in full: keys are hash distributed, every update file carries rows of nearly
    // every source segment, and a full-table column update read every update file once per source
    // segment, O(source segments x update files) reads of the same data. The partition pass reads each
    // update file once, in update-file order, and appends each row (its columns of this batch plus
    // its source rowid) to its segment's slice of the spill file. The segment tasks then read their own
    // slices, a few KB each, from local disk. Memory is bounded by lake_pk_column_mode_spill_buffer_bytes
    // and does not depend on the number of source segments or update files.
    Status _spill_update_rows(const std::map<uint32_t, UptidToRowidPairs>& rss_upt_id_to_rowid_pairs,
                              const Schema& partial_schema);
    // Apply the spilled rows of source segment `rssid` that fall into `container`. Same semantics as
    // _update_source_chunk_by_upt: update files in order, rows of one file sorted by source rowid,
    // the condition column compared inline when condition update is on.
    Status _update_source_chunk_from_spill(uint32_t rssid, const Schema& partial_schema, StreamChunkContainer container,
                                           int32_t condition_idx_in_partial_schema);
    void _release_spill();
    static std::string _spill_dir();
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

    struct SpillRecord {
        uint32_t upt_id;
        uint32_t num_rows;
        int64_t offset;
        int64_t length;
    };
    struct UptSpill {
        std::string path;
        std::unique_ptr<WritableFile> writer;
        int64_t size = 0;
        // Source segment -> its records, in apply order (update-file order, then write order).
        std::map<uint32_t, std::vector<SpillRecord>> records;
        bool ready = false;
    };
    UptSpill _spill;
    int64_t _spill_seq = 0;
};

class CompactionUpdateConflictChecker {
public:
    static bool conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                               const TabletMetadata& metadata, MetaFileBuilder* builder);
};

} // namespace starrocks::lake
