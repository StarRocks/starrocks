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

#include <cctz/time_zone.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/scan/shared_buffered_input_stream.h"
#include "column/column.h"
#include "column/column_access_path.h"
#include "column/variant_path_parser.h"
#include "common/global_types.h"
#include "common/statusor.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/utils.h"
#include "storage/primitive/range.h"
#include "types/type_descriptor.h"

namespace starrocks {
class Chunk;
class ExprContext;
struct HdfsScannerStats;
} // namespace starrocks

namespace starrocks::parquet {

class ColumnReader;
class VariantColumnReader;
class GroupReader;
struct GroupReaderParam;

// ── Free functions: variant projection utilities ────────────────────────────

StatusOr<ColumnPtr> project_variant_leaf_column(const ColumnPtr& variant_src, const VariantPath& path,
                                                const TypeDescriptor& target_type, const cctz::time_zone& zone);

bool collect_variant_leaf_paths(const ColumnAccessPath* node, std::vector<VariantSegment>* segments,
                                VariantShreddedReadHints* hints);

VariantShreddedReadHints build_variant_shredded_hints(const std::vector<ColumnAccessPathPtr>* column_access_paths,
                                                      std::string_view column_name);

// ── VariantProjectionHandler: encapsulates variant-specific pipeline phases ──
//
// GroupReader owns exactly one optional VariantProjectionHandler.  When no variant
// columns are present the pointer is null and all variant phases in get_next()
// become no-ops.  This keeps GroupReader's member list minimal and its pipeline
// composed of named conceptual phases rather than variant-instrumented loops.
class VariantProjectionHandler {
public:
    VariantProjectionHandler(GroupReader* group_reader, const GroupReaderParam& param,
                             const tparquet::RowGroup* row_group_metadata);
    ~VariantProjectionHandler();

    bool empty() const { return _projections.empty() && _hidden_sources.empty(); }

    // ── Setup (called once during GroupReader::_create_column_readers) ──────
    Status setup_readers();

    // Registers VariantVirtualZoneMapReader wrappers in column_readers for
    // all variant virtual projections.  Must be called AFTER all physical
    // column readers have been added to column_readers so source lookups work.
    void register_zone_map_readers();

    // ── Classification (during _process_columns_and_conjunct_ctxs) ──────────
    std::unordered_set<SlotId> deferred_conjunct_physical_source_slots() const;
    void collect_deferred_conjuncts();
    void classify_hidden_sources();

    // Accessors used by GroupReader to build active/lazy slot ID lists.
    const std::vector<SlotId>& active_hidden_slot_ids() const { return _active_hidden_slot_ids; }
    const std::vector<SlotId>& lazy_hidden_slot_ids() const { return _lazy_hidden_slot_ids; }

    // ── Prepare (during GroupReader::prepare / _prepare_column_readers) ──────
    Status prepare_hidden_readers();
    void select_hidden_source_offset_index();

    // ── Promotion (after _prepare_column_readers, before collect_io_ranges) ─
    bool try_promote();

    // ── Lazy→active promotion when active_chunk would be empty ───────────────
    void promote_lazy_to_active();

    // ── IO range collection ──────────────────────────────────────────────────
    void collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                           ColumnIOTypeFlags types);

    // ── Read chunk initialisation ────────────────────────────────────────────
    void init_read_chunk_slots();

    // ── get_next() phases ────────────────────────────────────────────────────
    //
    // The handler acts as a stateful pipeline stage: intermediate state
    // (projected columns, filters) is tracked internally and consumed
    // by later phases within the same get_next() iteration.
    //
    // Phase ordering: fetch → filter → align → backfill → emit
    // A Status::InternalError is returned when phases are called out of order.

    // 3. Fetch variant source columns that back subfield conjuncts.
    Status fetch_sources(const Range<uint64_t>& range, ChunkPtr& active_chunk);

    // 4. Evaluate deferred variant subfield conjuncts, projecting virtual
    //    columns from sources already present in active_chunk.
    //    Returns an empty Filter when there are no conjuncts.
    //    Projected columns are stored internally for later use by
    //    align_after_combined_filter and emit_projections.
    StatusOr<Filter> filter_subfields(ChunkPtr& active_chunk, size_t raw_count, HdfsScannerStats* stats,
                                      const cctz::time_zone& zone);
    bool has_deferred_conjuncts() const { return !_deferred_variant_virtual_conjunct_ctxs.empty(); }

    //    Align internally stored projected chunk with the post-filter active_chunk.
    Status align_after_combined_filter(const ChunkPtr& active_chunk, const Filter& filter, size_t pre_filter_rows);

    // 6. Backfill projection-only variant source columns.
    // Slots already triggered via LazyMaterializationContext (already present in
    // active_chunk) are skipped to avoid double-reading.
    Status backfill_sources(const Range<uint64_t>& full_range, const Range<uint64_t>* post_filter_range,
                            const Filter* post_filter, bool has_filter, ChunkPtr& active_chunk);

    // On-demand materialization of a single lazy hidden source slot into active_chunk.
    // Called by LazyMaterializationContext when an expression triggers the slot.
    // No-op if slot_id is already in active_chunk or is not a lazy hidden source.
    Status materialize_hidden_source(SlotId slot_id, const Range<uint64_t>& range, const Filter* filter,
                                     ChunkPtr& active_chunk);

    // 7. Emit variant virtual-column projections into the destination chunk.
    //    Consumes internally stored projected chunk (set by filter_subfields).
    //    Must be called BEFORE physical columns are swapped out.
    Status emit_projections(ChunkPtr& active_chunk, ChunkPtr* dst, const cctz::time_zone& zone);
    bool has_projections() const { return !_projections.empty(); }
    bool is_virtual_slot(SlotId slot_id) const { return _projections.count(slot_id) > 0; }
    std::unordered_set<SlotId> projection_slot_ids() const;

    // Returns the subset of variant virtual slot ids that are referenced by
    // the given conjuncts.  Used to decide which projections must be
    // materialised before compound-conjunct evaluation (Phase 2b).
    std::unordered_set<SlotId> referenced_variant_virtual_slot_ids(
            const std::vector<ExprContext*>& conjunct_ctxs) const;

    // Project only the requested variant virtual slots into active_chunk.
    // Fetches only the hidden source columns that are needed for the given
    // virtual slots (skipping ones already present in active_chunk).
    // Returns the set of virtual slots that were successfully projected.
    Status fetch_and_project_virtual_slots(const std::unordered_set<SlotId>& virtual_slot_ids,
                                           const Range<uint64_t>& range, ChunkPtr& active_chunk,
                                           const cctz::time_zone& zone);

    //    Reset per-iteration state (call at start of each get_next() call).
    void reset_iteration_state();

    // ── Timezone for variant projection ──────────────────────────────────────
    const cctz::time_zone& projection_timezone();

private:
    struct Projection {
        VariantPath parsed_path;
        TypeDescriptor target_type;
        SlotId source_slot_id;
    };

    struct HiddenSource {
        SlotId slot_id;
        std::unique_ptr<ColumnReader> reader;
        bool is_active = false;
        bool fully_promoted = false;
    };

    // These are called from setup_readers / try_promote; factored out from
    // GroupReader's original _create_column_readers / _promote_variant_virtual_columns.
    VariantShreddedReadHints _build_shredded_hints(std::string_view column_name) const;

    const GroupReaderParam& _param;
    const tparquet::RowGroup* _row_group_metadata;
    GroupReader* _group_reader;

    std::unordered_map<SlotId, Projection> _projections;
    std::unordered_map<std::string, HiddenSource> _hidden_sources;
    std::unordered_map<SlotId, HiddenSource*> _hidden_slot_index;
    SlotId _next_hidden_slot_id = -1;

    std::vector<SlotId> _active_hidden_slot_ids;
    std::vector<SlotId> _lazy_hidden_slot_ids;

    std::vector<ExprContext*> _deferred_variant_virtual_conjunct_ctxs;
    std::unordered_set<SlotId> _deferred_conjunct_slot_ids;
    std::unordered_set<SlotId> _promoted_virtual_slots;

    cctz::time_zone _timezone_obj = cctz::utc_time_zone();
    bool _timezone_resolved = false;

    // Tracks hidden source slots that have already been populated with data
    // during the current iteration (either by the normal fetch_sources() path
    // or by an early fetch for compound-conjunct variant virtual slots).
    // Reset by reset_iteration_state().
    std::unordered_set<SlotId> _fetched_hidden_slots;

    // Per-iteration state — set by filter_subfields, consumed by
    // align_after_combined_filter and emit_projections.
    ChunkPtr _deferred_projected_chunk;
};

} // namespace starrocks::parquet
