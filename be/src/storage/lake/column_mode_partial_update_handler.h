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

    // Per-column presence of one column inside a PACKED `.spcols` file, returned alongside the packed
    // chunk by _build_packed_sparse_chunk_from_upt. `roaring` is the serialized 32-bit CRoaring portable
    // bitmap of the EXACT base-segment rowids this column covers (the authoritative apply gate);
    // min/max/count are the cheap zero-IO pre-filter. column_uid is the UPDATE column uid (NOT the
    // reserved source_rowid uid).
    struct PackedColumnPresence {
        ColumnUID column_uid = 0;
        int64_t min_source_rowid = 0;
        int64_t max_source_rowid = 0;
        int64_t count = 0;
        std::string roaring;
    };

    // FLEXIBLE packing builder. Replaces _build_sparse_chunk_from_upt for the per-row heterogeneous case.
    // Reads the hidden "__cset__" set-id column from the `.upt` (by upt_rowid), decodes each upt row's
    // set-id into a column-uid mask via |distinct_column_sets| (the RowsetTxnMetaPB dictionary), builds a
    // PER-COLUMN (source_rowid, upt_rowid) list (a pair enters column c only if its row's mask covers c),
    // then packs ALL the batch's value columns into ONE union `.spcols`: column 0 = ascending union
    // source_rowids (K_union rows), each value column default-filled to K_union and update_rows ONLY at its
    // covered union ordinals (placeholders elsewhere). |value_schema| / |sparse_schema| are as in
    // _build_sparse_chunk_from_upt. |selective_unique_update_column_ids| is the batch's UPDATE column uids,
    // 1:1 by position with value_schema's columns. On success returns the K_union-row packed chunk and fills
    // |out_num_rows| (K_union), |out_min_source_rowid| / |out_max_source_rowid| (file-level union range),
    // and |out_column_presences| (one entry per UPDATE column that actually covers >=1 row; columns covering
    // NO row are omitted from the file's column id list by the caller). No source-segment read.
    StatusOr<ChunkPtr> _build_packed_sparse_chunk_from_upt(
            const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& value_schema, const Schema& sparse_schema,
            const std::vector<ColumnUID>& selective_unique_update_column_ids,
            const std::vector<std::vector<ColumnUID>>& distinct_column_sets, int64_t source_segment_num_rows,
            int64_t* out_num_rows, int64_t* out_min_source_rowid, int64_t* out_max_source_rowid,
            std::vector<PackedColumnPresence>* out_column_presences);

    // Read the hidden "__cset__" set-id column from the `.upt` for one upt_id. Returns a per-upt-row
    // vector of set-ids (size == upt segment row count). The column is read by NAME (kSDCGCsetColumnName)
    // via a synthetic single-column schema; if the upt segment does not carry it (non-flexible payload) the
    // caller must not invoke this. SMALLINT/INT/BIGINT storage are all accepted (value is a small set-id).
    StatusOr<std::vector<int32_t>> _read_cset_column_from_upt(uint32_t upt_id);

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

// Classification of a PK-compaction-vs-concurrent-update conflict, used to decide how the compaction
// result is reconciled at publish:
//   NONE           - no concurrent update raced this compaction; apply the output normally.
//   REPLAYABLE_DCG - the only race is column-mode partial updates that appended plain SPARSE_PERCOL
//                    `.spcols` overlays (no IDG, no inline patches, no flexible/packed) on an input
//                    segment. Such overlays CAN be remapped onto the compaction output via the rows
//                    mapper and replayed instead of discarding the whole compaction (see
//                    DESIGN_CONFLICT_REPLAY). NOTE: the replay itself is not implemented yet --
//                    groundwork only -- so this currently still falls back to discard.
//   MUST_DISCARD   - an IDG (index) race, or a DCG race that the simple rowid-remap replay cannot
//                    safely handle (inline patches / flexible-packed / dense conflicting layer).
//                    The compaction output is discarded (orphaned) so the newer delta is preserved.
enum class CompactionConflictKind { NONE, REPLAYABLE_DCG, MUST_DISCARD };

class CompactionUpdateConflictChecker {
public:
    // Returns true if the compaction conflicts with a concurrent update and its output was discarded
    // (orphaned) so the newer delta survives. Behaviorally unchanged: every conflict is still
    // discarded; classify_conflict() only tags WHY (for observability + the future replay hook).
    static bool conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                               const TabletMetadata& metadata, MetaFileBuilder* builder);

    // Pure (no side effects) classification of the conflict between |op_compaction| and the concurrent
    // updates recorded in |metadata|. Detection is identical to conflict_check's (a DCG/IDG entry on an
    // input segment with version > op_compaction.compact_version()); the kind tells the caller whether
    // the conflict is a candidate for overlay replay or must be discarded.
    static CompactionConflictKind classify_conflict(const TxnLogPB_OpCompaction& op_compaction,
                                                    const TabletMetadata& metadata);
};

} // namespace starrocks::lake