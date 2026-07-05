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
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "column/column.h"
#include "connector_primitive/connector.h"
#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/Descriptors_types.h"
#include "storage/del_vector.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/olap_common.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/chunk_iterator.h"
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
// from; the versions form a chain (the lineage). before_meta / after_meta name
// the two ends of one publish's edge:
//
//     v_base <--p1-- v1 <--p2-- v2 <--p3-- ... <--pN-- v_head
//
//     (each "<--pi--" is one publish; an arrow points from a child version to
//      its parent.) The scan walks the edges backwards — pN, p(N-1), ..., p1,
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
//     drives the backward walk, runs the planner per edge, reads the located rows
//     through segment iterators, and appends the two metadata columns.

namespace starrocks::connector {

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

// One publish = the edge before_meta -> after_meta. Holds before_meta, after_meta,
// and the located reads grouped by change type. Every surfaced row's ROW_VERSION is
// after_meta->version(), so it is not stored per read.
struct VersionChangeReadPlan {
    TabletMetadataPtr before_meta;
    TabletMetadataPtr after_meta;
    std::vector<SegmentChangeReadPlan> insert_changes; // -> INSERT (after value)
    std::vector<SegmentChangeReadPlan> delete_changes; // -> DELETE (before value; some are read raw from after_meta)
};

// Plans one publish's reads: a near-pure function over (before_meta, after_meta).
// plan() runs three steps — classify the changed segments, then locate the
// after-value reads and the before-value reads (primary-key tables only).
class ChangesReadPlanner {
public:
    ChangesReadPlanner(lake::TabletManager* tablet_mgr, bool is_primary_keys)
            : _tablet_mgr(tablet_mgr), _is_primary_keys(is_primary_keys) {}

    // Locates this edge's changed rows and returns the reads for them: emit the
    // after-value reads (-> INSERT); for primary-key tables also emit the
    // before-value reads (-> DELETE). Duplicate- and aggregate-key tables surface
    // only after values.
    StatusOr<VersionChangeReadPlan> plan(TabletMetadataPtr before_meta, TabletMetadataPtr after_meta);

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

    lake::TabletManager* _tablet_mgr;
    bool _is_primary_keys;
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
    ChangesDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return false; }
    // CHANGES emits one scan range per tablet; an empty delta yields zero
    // ranges and 0 rows. Returning false here would let the pipeline layer
    // inject a default-constructed TScanRangeParams() placeholder, which
    // drives a get_tablet_metadata(tablet_id=0, ...) call that fails with
    // "starlet err grpc.GetShard(shardId=0)". Matches OlapScanNode/Lake.
    // TODO: fold empty-delta CHANGES into a ValuesScan in the FE optimizer
    // so no scan operator is scheduled at all.
    bool accept_empty_scan_ranges() const override { return true; }

    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TChangesScanNode _changes_scan_node;
};

// Pairs a tuple slot with the CHANGES metadata kind that fills it.
struct ChangesMetaSlot {
    TChangesMetaKind::type kind;
    const SlotDescriptor* slot;
};

// Drives one tablet's CHANGES scan: walks the metadata lineage backwards from
// head to base, runs the planner on each publish, and reads the located rows
// through segment iterators, appending the change-type and row-version columns
// named in the plan node.
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

private:
    // Builds the projected read schema, read from the live schema service rather than
    // head metadata, whose schema can lag one just published.
    Status _init_read_schema();
    Status _read_next_chunk(ChunkPtr* chunk);
    // Steps the walk to the parent publish and plans that edge. Errors if the lineage
    // can't reach base (the range isn't on it) rather than short-reading; returns false
    // once the walk reaches base.
    StatusOr<bool> _advance_to_next_publish();
    // Returns the publish's recorded degradation status (OK if reconstructable), so an
    // unreconstructable publish surfaces as an error instead of silently dropping rows.
    static Status _check_degradation(const TabletMetadataPtr& meta);
    // Opens a chunk iterator for one located read (segment + rowids), wrapped to append
    // the change-type and row-version columns. change_type tags the rows and selects the
    // read-stats counter; returns null for an empty read.
    StatusOr<ChunkIteratorPtr> _build_segment_iterator(const VersionChangeReadPlan& plan,
                                                       const SegmentChangeReadPlan& seg, int8_t change_type);

    // --- Inputs (immutable after open) ---
    const ChangesDataSourceProvider* _provider;
    int64_t _tablet_id = 0;
    int64_t _base_version = 0; // left-open
    int64_t _head_version = 0; // right-closed

    // --- Runtime context ---
    RuntimeState* _runtime_state = nullptr;
    std::vector<SlotDescriptor*> _data_slots;
    std::vector<ChangesMetaSlot> _changes_meta_slots;
    std::vector<std::pair<SlotId, size_t>> _data_slot_chunk_indices;

    std::shared_ptr<const TabletMetadataPB> _head_metadata;
    TabletSchemaCSPtr _tablet_schema;
    bool _is_primary_keys = false;
    std::optional<ChangesReadPlanner> _planner;

    // --- Lazy ancestor-walk state ---
    TabletMetadataPtr _walk_meta; // node the walk currently sits at (the after_meta of the next edge)
    int64_t _walk_version = 0;
    std::optional<VersionChangeReadPlan> _current_plan; // the publish being drained
    bool _draining_delete_changes = false;              // insert_changes drained first, then delete_changes
    size_t _segment_read_index = 0;                     // cursor into the change list being drained
    ChunkIteratorPtr _active_iterator;                  // iterator of the current segment read; null = none open

    Schema _read_schema;
    OlapReaderStatistics _insert_read_stats;
    OlapReaderStatistics _delete_read_stats;

    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;
};

} // namespace starrocks::connector
