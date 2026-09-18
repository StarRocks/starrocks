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
#include <mutex>
#include <optional>

#include "storage/lake/cross_publish_context.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/rowset_column_update_state.h"

namespace starrocks {
class PrimaryIndex;
}

namespace starrocks::lake {

class Tablet;
class Rowset;

class LakeDeltaColumnGroupLoader : public DeltaColumnGroupLoader {
public:
    LakeDeltaColumnGroupLoader(TabletMetadataPtr tablet_metadata);
    Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) override;
    Status load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                DeltaColumnGroupList* pdcgs) override;

private:
    TabletMetadataPtr _tablet_metadata;
};

// What a FLEXIBLE load's inserted rows declared, per row.
//
// A flexible `.upt` is a DENSE UNION of every column any row touched, with NULL placeholders in
// the cells a given row did NOT declare. For an UPDATE that is handled by masking the overlay, but
// an INSERT builds a brand-new row: writing the union verbatim would store NULL where the row
// simply said nothing, instead of the column's DEFAULT. That is a silent difference from every
// other partial-update path, so the insert needs the same mask the update already uses.
struct FlexibleInsertMask {
    // Per update-segment, the per-row set id read from the hidden `__cset__` column. Indexed by the
    // row's PHYSICAL rowid in that segment -- the same space insert_rowids uses.
    std::vector<std::vector<int32_t>> set_ids_by_segment;
    // set id -> the column unique-ids that set covers.
    std::vector<std::vector<ColumnUID>> distinct_column_sets;

    bool valid() const { return !distinct_column_sets.empty(); }
};

// Used in column mode partial update
class ColumnModePartialUpdateHandler {
public:
    ColumnModePartialUpdateHandler(int64_t base_version, int64_t txn_id, MemTracker* tracker);
    ~ColumnModePartialUpdateHandler();

    Status execute(const RowsetUpdateStateParams& params, MetaFileBuilder* builder,
                   std::vector<std::vector<uint32_t>>* insert_rowids_by_segment = nullptr,
                   FlexibleInsertMask* flexible_insert_mask = nullptr);

private:
    Status _load_update_state(const RowsetUpdateStateParams& params);
    StatusOr<int64_t> _calc_upt_memory_usage_per_row(const TabletSchema& tablet_schema);
    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_delta_column_group_writer(
            const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& tschema);
    Status _update_source_chunk_by_upt(const UptidToRowidPairs& upt_id_to_rowid_pairs, const Schema& partial_schema,
                                       StreamChunkContainer container, int32_t condition_idx_in_partial_schema);
    // FLEX_COLUMN: heterogeneous (per-row column subset) apply via the DENSE path. Like
    // _update_source_chunk_by_upt but per-COLUMN masked: a given upt row overlays a value column c ONLY
    // when that row's `__cset__` set-id covers c (else the column keeps the base/current value, NOT the
    // NULL placeholder the upt carries for declared-absent columns). The result is a row-complete dense
    // `.cols` for the union columns (read-neutral, no source_rowid / per-column roaring). Selected when
    // flexible_partial_update && !config::enable_sparse_dcg (the dense alternative to the sparse packed path).
    // Takes the same StreamChunkContainer as _update_source_chunk_by_upt: the source segment is streamed
    // in bounded row ranges, so a pair whose source rowid falls outside [start_rowid, end_rowid) belongs
    // to another round and is skipped, and the rowids that remain are rebased onto the container.
    Status _update_source_chunk_by_upt_flexible(const UptidToRowidPairs& upt_id_to_rowid_pairs,
                                                const Schema& partial_schema,
                                                const std::vector<ColumnUID>& selective_unique_update_column_ids,
                                                const std::vector<std::vector<ColumnUID>>& distinct_column_sets,
                                                StreamChunkContainer container);
    Status _read_from_source_segment_and_update(const RowsetUpdateStateParams& params, const Schema& schema,
                                                uint32_t rssid,
                                                const std::function<Status(StreamChunkContainer)>& update_func);

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

    // Read the hidden "__cset__" set-id column of EVERY `.upt` segment in ONE cache-bypassing pass. The
    // result is POSITIONAL (index == upt_id, size == the rowset's segment count) with one set-id per upt
    // row; a segment that holds no rows for this tablet (a range-distributed hole) leaves an EMPTY entry.
    // The column is resolved by its reserved uid (kCsetReservedColumnUid) through a synthetic one-column
    // schema; if the payload does not carry it (non-flexible load) the caller must not invoke this.
    // SMALLINT/INT/BIGINT storage are all accepted (the value is a small set-id).
    //
    // One pass, not one call per upt_id: opening the iterators re-opens every segment of the rowset with
    // a fresh footer read (the metacache holds the base-schema Segment, which has no reader for the
    // reserved-uid column), so a per-segment helper costs O(segments^2) footer GETs against object storage.
    StatusOr<std::vector<std::vector<int32_t>>> _read_all_cset_columns_from_upt();

    // The result of _read_all_cset_columns_from_upt, read once per publish and shared by every consumer:
    // the insert-mask fill, the packed builder of each (batch, rssid) task, and the masked-dense path that
    // runs once per streamed source range. The tasks run in parallel, so the fill is serialised by
    // _cset_cache_mutex; after it the vector is immutable and read without the lock. Owned by the handler,
    // which outlives the ParallelTaskRunner join.
    StatusOr<const std::vector<std::vector<int32_t>>*> _cached_cset_columns();

    // Prepare a SegmentWriter for a sparse `.spcols` file. Identical construction to the dense `.cols`
    // writer (options/encryption/init(false)) except the filename is a `.spcols` name and the schema is
    // |sparse_tschema| (source_rowid + value columns).
    StatusOr<std::unique_ptr<SegmentWriter>> _prepare_sparse_delta_column_group_writer(
            const RowsetUpdateStateParams& params, const std::shared_ptr<TabletSchema>& sparse_tschema);

public:
    // Build the synthetic [source_rowid] + value-columns TabletSchema for a sparse `.spcols` file.
    // Public so the compaction-conflict replay path (CompactionUpdateConflictChecker) can reuse it.
    static std::shared_ptr<TabletSchema> build_sparse_tablet_schema(const TabletSchemaCSPtr& base_tablet_schema,
                                                                    const std::shared_ptr<TabletSchema>& value_tschema);

private:
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
    // See _cached_cset_columns().
    std::mutex _cset_cache_mutex;
    std::optional<std::vector<std::vector<int32_t>>> _cset_cache;
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
    // (orphaned) so the newer delta survives. Behaviorally: every conflict is discarded here;
    // classify_conflict() only tags WHY. Retained as a thin bool wrapper used by the meta-file tests
    // (production uses check_and_maybe_discard()).
    static bool conflict_check(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                               const TabletMetadata& metadata, MetaFileBuilder* builder);

    // Pure (no side effects) classification of the conflict between |op_compaction| and the concurrent
    // updates recorded in |metadata|. Detection is identical to conflict_check's (a DCG/IDG entry on an
    // input segment with version > op_compaction.compact_version()); the kind tells the caller whether
    // the conflict is a candidate for overlay replay or must be discarded.
    static CompactionConflictKind classify_conflict(const TxnLogPB_OpCompaction& op_compaction,
                                                    const TabletMetadata& metadata);

    // Conflict decision used by the compaction publish path. Returns the classified kind and sets
    // |*out_discarded|:
    //   - NONE                 -> *out_discarded=false; caller publishes normally.
    //   - REPLAYABLE_DCG, and replay is enabled AND the strict replay preconditions hold (pure
    //     homogeneous SPARSE_PERCOL race, uniform updated-column set, NO racing delvec) -> the output is
    //     NOT orphaned (*out_discarded=false); the caller proceeds to publish and MUST then call
    //     replay_sparse_overlays_onto_output() before apply_opcompaction().
    //   - otherwise (MUST_DISCARD, or REPLAYABLE_DCG with replay off / preconditions unmet) -> the output
    //     is orphaned here via apply_opcompaction_with_conflict (*out_discarded=true); caller returns OK.
    static CompactionConflictKind check_and_maybe_discard(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                                          const TabletMetadata& metadata, MetaFileBuilder* builder,
                                                          bool* out_discarded);

    // A-family PK-keyed replay. Re-applies the racing SPARSE_PERCOL `.spcols` overlays (version >
    // compact_version) onto the KEPT compaction output, keyed by primary key: read the base PK at each
    // racing source_rowid, look the PK up in |index| (which already points at the output segments at this
    // point) to get its new (output_rssid, output_rowid), pre-merge per (output_rowid, column) keeping the
    // highest racing version, and emit one equivalent `.spcols` overlay per output segment via append_dcg
    // into |builder| (so it publishes atomically with apply_opcompaction). Must be called AFTER the index
    // is updated to the output and BEFORE builder->apply_opcompaction() (the input segments must still
    // exist). Only valid when check_and_maybe_discard returned REPLAYABLE_DCG with *out_discarded==false.
    static Status replay_sparse_overlays_onto_output(const TxnLogPB_OpCompaction& op_compaction, int64_t txn_id,
                                                     const TabletMetadataPtr& metadata, const Tablet& tablet,
                                                     const Rowset& output_rowset, MetaFileBuilder* builder);
};

} // namespace starrocks::lake
