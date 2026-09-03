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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "compute_env/query/scan_conjuncts_manager.h"
#include "connector_primitive/connector.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "storage/del_vector.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/predicate_parser.h"
#include "storage/seek_range.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/chunk_iterator.h"
#include "storage_primitive/predicate_tree/predicate_tree.hpp"
#include "storage_primitive/range.h"

namespace starrocks {
class TabletMetadataPB;
class RowsetMetadataPB;
namespace lake {
class TabletManager;
} // namespace lake
} // namespace starrocks

// CHANGES scan — theory of operation
//
// CHANGES reads the row-level changes a cloud-native table accumulated over a
// version range, feeding incremental materialized-view maintenance. This
// connector serves one tablet's slice: it reads that tablet's changes over the
// tablet version range (base, head] (left-open, right-closed).
//
// Contract. Each row this scan emits is the table's data columns plus two
// metadata columns:
//   __CHANGE_TYPE__  0 = INSERT (the row's after value), 1 = DELETE (its before
//                    value). An inserted key gives one INSERT; a deleted key one
//                    DELETE; an updated key a DELETE (before) + INSERT (after).
//                    Every row carries its full column values, whatever the
//                    write touched.
//   __ROW_VERSION__  the tablet version that produced the change.
// Granularity is per publish (see below), not per write: a key changed several
// times within one publish surfaces only its two endpoints. The result is a set
// with no inherent order.
//
// Mechanism. A write becomes readable through a "publish", which appends a new
// immutable tablet metadata version pointing back to the parent it was built
// from; the versions form a chain. before_meta / after_meta name
// the two ends of one publish's edge:
//
//     v_base <--p1-- v1 <--p2-- v2 <--p3-- ... <--pN-- v_head
//
//     (each "<--pi--" is one publish; an arrow points from a child version to
//      its parent.) The scan traverses the edges backwards — pN, p(N-1), ..., p1,
//      i.e. head down to base — and at each edge diffs before_meta (the parent)
//      against after_meta (the child) to recover that publish's changed rows.
//      One publish per step keeps a change locatable: within a single publish a
//      row's before value and after value are unambiguous.
//
// Per-publish capture. A publish's change is a diff of before_meta vs after_meta:
// live rows in an added rowset are after values (INSERT), new delete bits on a
// surviving segment are before values (DELETE), compaction outputs skipped.
//   update k1->k1' + insert k2:  before S0[k1]  ->  after S0[k1] del{k1} + S1[k1' k2]
//   yields DELETE k1 (before), INSERT k1' & k2 (after)
// This covers whole-row writes and DML-only batches, but cannot locate a change
// that adds no rowset and no delete bit — a column partial update, or delete bits
// compaction merged away or blurred with conflict resolution. For those, each
// publish records the locating info the diff lacks into after_meta's CdcMetadataPB.
// Primary-key tables only (others just append, INSERTs only); an unreconstructable
// publish records a degradation status instead — an error, not silent row loss.
//
// Two layers:
//   - Planning (ChangesReadPlanner): a near-pure function over one edge. From
//     (before_meta, after_meta) it locates which rows changed and where to read
//     them (a VersionChangeReadPlan), inspecting only small bitmaps, no columns.
//   - Connector (ChangesConnector / ChangesDataSourceProvider / ChangesDataSource):
//     drives the backward traversal, runs the planner per edge, reads the located rows
//     through segment iterators, and appends the two metadata columns.

namespace starrocks::connector {

Status make_cdc_error(TCdcErrorCode::type code, std::string_view message);

// =============================================================================
// Planning layer: read-plan types and ChangesReadPlanner
// =============================================================================

// One segment's located rows plus how to read them. The change type (INSERT vs
// DELETE) is NOT stored here — it is implied by which VersionChangeReadPlan vector
// (insert_changes vs delete_changes) this plan lives in. The read-from axis
// (from_before_meta) and the change axis are orthogonal.
struct SegmentChangeReadPlan {
    int rowset_pos;                // index into the meta chosen by from_before_meta
    int segment_pos;               // index into that rowset's segment_metas
    bool from_before_meta;         // true: read before_meta; false: read after_meta
    std::optional<Roaring> rowids; // rows to read; nullopt = read the whole segment
    bool read_with_dcg;            // apply the dcg overlay (true) or read raw (false); neither applies a delvec
};

// One publish = the edge before_meta -> after_meta. Every surfaced row's ROW_VERSION is
// after_meta->version(), so it is not stored per read.
struct VersionChangeReadPlan {
    TabletMetadataPtr before_meta; // the parent version in a VERSION_CHAIN_DIFF read; null in a FULL_SCAN read
    TabletMetadataPtr after_meta;
    std::vector<SegmentChangeReadPlan> insert_changes; // -> INSERT (after value)
    std::vector<SegmentChangeReadPlan> delete_changes; // -> DELETE (before value; some are read raw from after_meta)
};

// Plans one publish's reads from (before_meta, after_meta): a near-pure function that inspects
// only metadata bitmaps, no columns.
class ChangesReadPlanner {
public:
    // |lake_io_opts| governs the delete-vector reads this planner makes: whether their file reads
    // populate the local data cache, and -- through the same fill_data_cache flag, matching what an
    // ordinary lake scan does -- whether the parsed vectors stay in the metadata cache.
    ChangesReadPlanner(lake::TabletManager* tablet_mgr, bool is_primary_keys, LakeIOOptions lake_io_opts)
            : _tablet_mgr(tablet_mgr), _is_primary_keys(is_primary_keys), _lake_io_opts(std::move(lake_io_opts)) {}

    // Locates this edge's changed rows: after-value reads (-> INSERT) for every table, plus
    // before-value reads (-> DELETE) for primary-key tables. Duplicate- and aggregate-key tables
    // have only after values.
    StatusOr<VersionChangeReadPlan> plan_version_diff(TabletMetadataPtr before_meta, TabletMetadataPtr after_meta);

    // There is no data at the base, so every row visible at head_meta is an insert;
    // no before_meta is read.
    StatusOr<VersionChangeReadPlan> plan_full_scan(TabletMetadataPtr head_meta);

private:
    // A segment present in after_meta but not before_meta — introduced by this publish.
    struct AddedSegment {
        int rowset_pos; // into after_meta.rowsets()
        int segment_pos;
        bool is_compaction_output; // produced by compaction (RowsetMetadataPB.has_max_compact_input_rowset_id)
    };
    // A segment present in before_meta that this publish changed.
    struct CarriedSegment {
        int rowset_pos; // into before_meta.rowsets()
        int segment_pos;
        bool is_compaction_input; // merged away by this publish's compaction (gone from after_meta.rowsets())
        bool delvec_changed;      // delete vector grew this publish
        bool column_overlaid;     // a column partial update overlaid value column(s) this publish
    };
    using AfterPosition =
            std::unordered_map<uint32_t, std::pair<int, int>>; // rssid -> (rowset_pos, segment_pos) in after_meta

    // Step 1 — classify. Splits the segments this edge touched into AddedSegment
    // (in after_meta, not before_meta) and CarriedSegment (in before_meta and
    // changed this publish), and records every after_meta segment's position
    // (after_position: rssid -> position) so a carried segment's after side can
    // be found by rssid.
    Status _locate_changed_segments(const TabletMetadataPB& before_meta, const TabletMetadataPB& after_meta,
                                    std::vector<AddedSegment>* added, std::vector<CarriedSegment>* carried,
                                    AfterPosition* after_position);
    // Step 2 — after-value reads (-> INSERT). Two sources:
    //   - an added segment's live rows: the whole segment minus its delete vector
    //     (a compaction output contributes only the rows it column-updated this
    //     publish, since its bulk rows were merged in unchanged, not inserted).
    //   - a surviving carried segment's column-updated rows, read at its after
    //     position with the dcg overlay applied.
    Status _plan_insert_change_read(const VersionChangeReadPlan& p, const std::vector<AddedSegment>& added,
                                    const std::vector<CarriedSegment>& carried, const AfterPosition& after_position,
                                    std::vector<SegmentChangeReadPlan>* insert_changes);
    // Step 3 — before-value reads (-> DELETE), primary-key tables only. Two sources:
    //   - a compaction output's rows deleted or updated this publish. Their before
    //     value has no separate before_meta segment, so it is read raw from the
    //     output segment in after_meta — the one case a before value is read from
    //     after_meta rather than before_meta.
    //   - a carried segment's deleted rows (its delete vector grew) and column-
    //     updated rows' before values, read from before_meta as one before-side read.
    Status _plan_delete_change_read(const VersionChangeReadPlan& p, const std::vector<AddedSegment>& added,
                                    const std::vector<CarriedSegment>& carried,
                                    std::vector<SegmentChangeReadPlan>* delete_changes);

    // Delete-vector reads, honoring this planner's cache settings.
    StatusOr<Roaring> _load_delvec(const TabletMetadata& metadata, uint32_t rssid) const;
    StatusOr<Roaring> _load_delvec_page(const TabletMetadata& metadata, const DelvecPagePB& page) const;

    lake::TabletManager* _tablet_mgr;
    bool _is_primary_keys;
    LakeIOOptions _lake_io_opts;
};

// =============================================================================
// Connector layer: ChangesConnector / ChangesDataSourceProvider / ChangesDataSource
// =============================================================================

class ChangesConnector final : public Connector {
public:
    ~ChangesConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::CHANGES; }
};

class ChangesDataSource;

class ChangesDataSourceProvider final : public DataSourceProvider {
public:
    ~ChangesDataSourceProvider() override = default;
    friend class ChangesDataSource;
    explicit ChangesDataSourceProvider(const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    const TChangesScanNode _changes_scan_node;
};

// Drives one tablet's CHANGES scan: backward traversal over the version chain, planner per
// publish, segment reads, and the metadata-column append.
class ChangesDataSource final : public DataSource {
public:
    ~ChangesDataSource() override = default;

    ChangesDataSource(const ChangesDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override { return "ChangesDataSource"; }
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status parse_runtime_filters(RuntimeState* state) override { return Status::OK(); }
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override { return _rows_read; }
    int64_t num_rows_read() const override { return _rows_read; }
    int64_t num_bytes_read() const override { return _bytes_read; }
    int64_t cpu_time_spent() const override { return _cpu_time_ns; }

    const OlapReaderStatistics& insert_read_stats() const { return _insert_read_stats; }
    const OlapReaderStatistics& delete_read_stats() const { return _delete_read_stats; }

private:
    // Numbered to match TOpType (UPSERT/DELETE) — the constants FE's ChangesScanBuilder compares
    // __CHANGE_TYPE__ against — so both sides agree on the value.
    enum class ChangeType : int8_t {
        INSERT = TOpType::UPSERT,
        DELETE = TOpType::DELETE,
    };
    // Pairs a tuple slot with the CHANGES metadata kind that fills it.
    struct ChangesMetaSlot {
        TChangesMetaKind::type kind;
        const SlotDescriptor* slot;
    };

    struct ScanCounters {
        RuntimeProfile::Counter* raw_rows = nullptr;
        RuntimeProfile::Counter* zonemap_filtered = nullptr;
        RuntimeProfile::Counter* bloom_filter_filtered = nullptr;
        RuntimeProfile::Counter* short_key_filtered = nullptr;
        RuntimeProfile::Counter* predicate_filtered = nullptr;
        // Rows entering / leaving the storage-layer runtime-filter predicate (join / TopN / aggregation
        // RF), mirroring the internal scan's RuntimeFilterInputRows/RuntimeFilterOutputRows.
        RuntimeProfile::Counter* runtime_filter_input = nullptr;
        RuntimeProfile::Counter* runtime_filter_output = nullptr;
    };

    void _init_counter();
    void _resolve_cache_mode(const TChangesScanRange& range);
    Status _init_tablet_schema();
    Status _init_pushdown_predicates();
    Status _init_storage_read_schema();
    Status _init_output_columns();

    bool _is_primary_key_table() const { return _tablet_schema->keys_type() == KeysType::PRIMARY_KEYS; }
    Status _read_next_chunk(ChunkPtr* chunk);
    // Steps the traversal to the parent version and plans that publish edge; returns false once it
    // reaches base. Errors (rather than short-reading) if the version chain can't reach base — i.e.
    // the requested range isn't on it.
    StatusOr<bool> _advance_to_next_version();
    // Rejects a window a CHANGES read cannot reconstruct: a primary-key version where change data
    // capture was not enabled, or a publish whose recorded capture status is not OK. Surfacing the
    // error keeps an unreconstructable publish from silently dropping rows.
    Status _check_degradation(const TabletMetadataPtr& meta) const;
    // Opens a chunk iterator for one located read (segment + rowids). change_type selects the
    // read-stats counter; returns null for an empty read.
    StatusOr<ChunkIteratorPtr> _build_segment_iterator(const VersionChangeReadPlan& plan,
                                                       const SegmentChangeReadPlan& seg, ChangeType change_type);
    // Turns a data-only chunk into the row shape the scan emits: maps the data columns to their slot ids and
    // appends the CHANGES metadata columns (__CHANGE_TYPE__ / __ROW_VERSION__), whose change_type and
    // row_version are constant for the whole read.
    Status _append_meta_columns(Chunk* chunk, ChangeType change_type, int64_t row_version);

    void _update_counter();

    // --- Inputs (immutable after open) ---
    const ChangesDataSourceProvider* _provider;
    int64_t _tablet_id = 0;
    TChangeDerivationMode::type _derivation_mode = TChangeDerivationMode::VERSION_CHAIN_DIFF;
    int64_t _base_version;     // left-open; set only for VERSION_CHAIN_DIFF
    int64_t _head_version = 0; // right-closed

    // --- Cache filling (resolved from the scan range in the constructor) ---
    // Three settings rather than one flag because they reach caches that do not overlap:
    //   _lake_io_opts           segment and delete-vector file reads, and their metacache entries
    //   _use_page_cache         decoded data and index pages
    //   _cache_tablet_metadata  tablet metadata, which TabletManager reads outside _lake_io_opts
    // See _resolve_cache_mode for how the scan range's four fields fold into them.
    LakeIOOptions _lake_io_opts;
    bool _use_page_cache = false;
    bool _cache_tablet_metadata = true;

    // --- Runtime context ---
    RuntimeState* _runtime_state = nullptr;
    ObjectPool _obj_pool;
    MemPool _mem_pool;

    // --- Slots and schema ---
    const std::vector<SlotDescriptor*>* _all_slots = nullptr;
    std::vector<SlotDescriptor*> _data_slots;
    std::vector<ChangesMetaSlot> _changes_meta_slots;
    std::shared_ptr<const TabletMetadataPB> _head_metadata = nullptr;
    TabletSchemaCSPtr _tablet_schema;

    // --- Predicate pushdown ---
    std::vector<ExprContext*> _data_slot_conjunct_ctxs;
    std::vector<std::string> _sort_key_column_names;
    std::unique_ptr<ScanConjunctsManager> _conjuncts_manager;
    ColumnPredicatePtrs _parsed_column_predicates;

    // A CHANGES query's predicates play one of two roles:
    //
    //   Pushdown predicates — enforced by the storage read itself, so a row that fails them is never
    //   read back. Held as a row-level ColumnPredicate tree, its zonemap-index form, and short-key
    //   ranges over the sort-key prefix.
    //
    //   Residual predicates — the ones the storage read cannot enforce, left for the connector to
    //   apply to the rows it returns. Two forms: a ColumnPredicate tree over data columns (predicates
    //   that could not be pushed down), and expressions over the CHANGES metadata columns
    //   (__CHANGE_TYPE__ / __ROW_VERSION__) or predicates that do not reduce to a ColumnPredicate.
    PredicateTree _pushdown_pred_tree;
    PredicateTree _pushdown_pred_tree_for_zone_map;
    std::vector<SeekRange> _pushdown_key_ranges;
    PredicateTree _residual_pred_tree;
    std::vector<ExprContext*> _residual_conjunct_ctxs;
    Filter _reused_selection;

    // Runtime filter pushdown
    // A metadata-column-filtered copy of the framework collector (_runtime_filters, from
    // DataSource::set_runtime_filters), passed to ScanConjunctsManager instead of the framework
    // collector directly: a probe on a CHANGES metadata slot (__CHANGE_TYPE__ / __ROW_VERSION__) has
    // no tablet-schema column, and OlapPredicateParser::can_pushdown CHECK-fails if asked to resolve
    // one. Holds re-indexed pointers into _runtime_filters's descriptors, which outlive the scan.
    RuntimeFilterProbeCollector _data_column_runtime_filters;
    RuntimeFilterPredicates _runtime_filter_preds{0};
    RuntimeScanRangePruner _runtime_range_pruner;

    // The tablet columns the segment read materializes: every data slot's column, or — when the
    // projection is metadata-only (e.g. SELECT __ROW_VERSION__) — the first tablet column, forced in so
    // the iterator still drives a real row count. _read_schema_has_forced_column marks that filler case;
    // no slot references the filler, so the surface step drops it before the tuple sees it.
    Schema _storage_read_schema;
    bool _read_schema_has_forced_column = false;

    // Output-column narrowing, applied at two points:
    //   _unused_cids_after_pushdown — read columns a pushed-down predicate filtered on but nothing reads
    //     back (no output slot, no residual). The segment read drops them (init_output_schema), skipping
    //     their dict-decode and materialization.
    //   _unused_slot_ids — non-output data and metadata slots. Read/appended for the residual eval, then
    //     stripped from the surfaced chunk so it carries strictly the isOutputColumn slots.
    // _has_output_column is false only when every slot is predicate-only; stripping all columns would
    // then zero the row count, so the surface step leaves the chunk as-is instead.
    std::unordered_set<uint32_t> _unused_cids_after_pushdown;
    std::unordered_set<uint32_t> _unused_slot_ids;
    bool _has_output_column = false;

    // --- Current read position (head → base) ---
    std::optional<ChangesReadPlanner> _changes_read_planner;
    TabletMetadataPtr _current_meta = nullptr;            // tablet metadata version the scan currently sits at
    std::optional<VersionChangeReadPlan> _current_plan;   // read plan of the publish currently being drained
    bool _full_scan_planned = false;                      // FULL_SCAN only; set after the full scan has been planned
    ChangeType _current_change_type = ChangeType::INSERT; // side being emitted; INSERT drained before DELETE
    size_t _current_segment_index = 0;                    // index of the current segment read in the change list
    ChunkIteratorPtr _current_segment_iterator = nullptr; // iterator over the current segment read; null = none

    OlapReaderStatistics _insert_read_stats;
    OlapReaderStatistics _delete_read_stats;

    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;

    std::unique_ptr<ScanCounters> _scan_counters;
};

} // namespace starrocks::connector
