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

// Used in column mode partial update
class ColumnModePartialUpdateHandler {
public:
    ColumnModePartialUpdateHandler(int64_t base_version, int64_t txn_id, MemTracker* tracker);
    ~ColumnModePartialUpdateHandler();

    Status execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder,
                   std::vector<std::vector<uint32_t>>* insert_rowids_by_segment = nullptr);

private:
    Status _load_update_state(const RowsetUpdateStateParams& params);
    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_delta_column_group_writer(
            const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& tschema);
    Status _update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& partial_schema,
                                       ChunkPtr* source_chunk, int32_t condition_idx_in_partial_schema);
    StatusOr<ChunkPtr> _read_from_source_segment(const RowsetUpdateStateParams& params, const Schema& schema,
                                                 uint32_t rssid);

    // SDCG sparse write path (lake only, gated by config::enable_sparse_dcg).
    //
    // Resolve the base segment row count M for |rssid| WITHOUT a footer GET when possible: scan
    // params.metadata rowsets' segment_metas (rssid -> rowset+segment via get_rssid) and read
    // SegmentMetadataPB.num_rows; fall back to load_segment(...)->num_rows() for legacy segments that
    // don't carry num_rows. Returns 0 when M cannot be determined (callers then skip the sparse path).
    StatusOr<int64_t> _resolve_source_segment_num_rows(const RowsetUpdateStateParams& params, uint32_t rssid);

    // Build a K-row sparse overlay chunk for one (column-batch, rssid): column 0 is the synthetic
    // source_rowid column (sorted ascending, uid=kSDCGSourceRowidUid), followed by the update value
    // columns gathered from the `.upt` payload by upt_rowid. K = number of distinct source_rowids across
    // all upt_ids for this rssid; later upt_ids win per source_rowid (last-write-wins), matching the
    // dense path's ascending-upt_id overwrite. |out_num_rows| receives K. |out_min_source_rowid| /
    // |out_max_source_rowid| receive the smallest / largest base-segment ordinal touched (the closed
    // presence range emitted into SparsePresencePB); both are kSDCGPresenceUnknown when K == 0.
    // |value_schema| is the partial schema of the update columns (NOT including source_rowid);
    // |sparse_schema| is value_schema with the source_rowid column prepended. No source-segment read.
    StatusOr<ChunkPtr> _build_sparse_chunk_from_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs,
                                                    const Schema& value_schema, const Schema& sparse_schema,
                                                    int64_t source_segment_num_rows, int64_t* out_num_rows,
                                                    int64_t* out_min_source_rowid, int64_t* out_max_source_rowid);

    // Prepare a SegmentWriter for a sparse `.spcols` file. Identical construction to the dense `.cols`
    // writer (options/encryption/init(false)) except the filename is a `.spcols` name and the schema is
    // |sparse_tschema| (source_rowid + value columns).
    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_sparse_delta_column_group_writer(
            const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& sparse_tschema);

    // Build an InlineSparsePatchPB from the K-row sparse chunk produced by _build_sparse_chunk_from_upt
    // (column 0 = source_rowid, columns 1..N = value columns). Serializes each value column via
    // serde::ColumnArraySerde (one blob per uid, in |unique_update_column_ids| order) and the
    // source_rowid column to K x uint32 little-endian bytes. The patch is the in-meta equivalent of a
    // `.spcols` file: no file IO, no source-segment read. Sets |out_patch_bytes| to the serialized
    // ByteSizeLong of the produced patch so the writer can apply the inline byte gates BEFORE committing
    // to the inline path. |min_source_rowid| / |max_source_rowid| / |row_count| are the presence range
    // and K already computed by _build_sparse_chunk_from_upt. NOTE: the patch's `version` is left UNSET;
    // MetaFileBuilder::append_dcg stamps it with the publish version (_tablet_meta->version()) so a new
    // inline patch interleaves with the file entries' versions[] at the same publish version.
    static StatusOr<InlineSparsePatchPB> _build_inline_patch_from_sparse_chunk(
            const Chunk& sparse_chunk, const std::vector<ColumnUID>& unique_update_column_ids, int64_t row_count,
            int64_t min_source_rowid, int64_t max_source_rowid, size_t* out_patch_bytes);

    // Build the synthetic [source_rowid] + value-columns TabletSchema for a sparse `.spcols` file.
    static std::shared_ptr<TabletSchema> _build_sparse_tablet_schema(
            const TabletSchemaCSPtr& base_tablet_schema, const std::shared_ptr<TabletSchema>& value_tschema);
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
};

class CompactionUpdateConflictChecker {
public:
    static bool conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                               const TabletMetadata& metadata, MetaFileBuilder* builder);
};

} // namespace starrocks::lake