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

#include "connector/changes/changes_connector.h"

#include <fmt/format.h>

#include <algorithm>
#include <unordered_map>

#include "column/chunk_factory.h"
#include "column/nullable_column.h"
#include "common/config_scan_io_fwd.h"
#include "common/config_storage_fwd.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/runtime_range_pruner.hpp"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate_rewriter.h"
#include "storage/del_vector.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/rowset/rowset_options.h"
#include "storage/storage_env.h"
#include "storage/tablet_reader_params.h"
#include "storage/virtual_column_utils.h"
#include "storage_primitive/olap_scan_range.h"
#include "storage_primitive/range.h"
#include "storage_primitive/roaring2range.h"

namespace starrocks::connector {

Status make_cdc_error(TCdcErrorCode::type code, std::string_view message) {
    const auto it = _TCdcErrorCode_VALUES_TO_NAMES.find(code);
    const std::string_view symbol =
            it == _TCdcErrorCode_VALUES_TO_NAMES.end() ? std::string_view() : std::string_view(it->second);
    return Status::InternalError(fmt::format("CDC-ERROR-{} ({}): {}", static_cast<int>(code), symbol, message));
}

namespace {

lake::TabletManager* lake_tablet_manager() {
    return StorageEnv::GetInstance()->lake_tablet_manager();
}

} // namespace

// Load a segment's delete vector by rssid from |metadata|'s live delvecs map; empty Roaring if absent.
StatusOr<Roaring> ChangesReadPlanner::_load_delvec(const TabletMetadata& metadata, uint32_t rssid) const {
    DelVector dv;
    RETURN_IF_ERROR(lake::get_del_vec(_tablet_mgr, metadata, rssid, _lake_io_opts.fill_data_cache, _lake_io_opts, &dv));
    return dv.empty() ? Roaring{} : *dv.roaring();
}

// Load a delvec page (a value from a CdcMetadataPB capture map), using |metadata| to resolve the file.
StatusOr<Roaring> ChangesReadPlanner::_load_delvec_page(const TabletMetadata& metadata,
                                                        const DelvecPagePB& page) const {
    DelVector dv;
    RETURN_IF_ERROR(lake::get_del_vec(_tablet_mgr, metadata, page, _lake_io_opts.fill_data_cache, _lake_io_opts, &dv));
    return dv.empty() ? Roaring{} : *dv.roaring();
}

// =============================================================================
// Planning layer: ChangesReadPlanner
// =============================================================================

StatusOr<VersionChangeReadPlan> ChangesReadPlanner::plan_version_diff(TabletMetadataPtr before_meta,
                                                                      TabletMetadataPtr after_meta) {
    VersionChangeReadPlan p;
    p.before_meta = before_meta;
    p.after_meta = after_meta;
    std::vector<AddedSegment> added;
    std::vector<CarriedSegment> carried;
    AfterPosition after_position;
    RETURN_IF_ERROR(_locate_changed_segments(*before_meta, *after_meta, &added, &carried, &after_position));
    RETURN_IF_ERROR(_plan_insert_change_read(p, added, carried, after_position, &p.insert_changes));
    if (_is_primary_keys) {
        RETURN_IF_ERROR(_plan_delete_change_read(p, added, carried, &p.delete_changes));
    }
    return p;
}

StatusOr<VersionChangeReadPlan> ChangesReadPlanner::plan_full_scan(TabletMetadataPtr head_meta) {
    VersionChangeReadPlan p;
    p.before_meta = nullptr; // no delete side in a FULL_SCAN read; inserts read from after_meta
    p.after_meta = head_meta;
    const auto& after = *head_meta;
    for (int rs = 0; rs < after.rowsets_size(); ++rs) {
        const auto& r = after.rowsets(rs);
        // A delete-predicate rowset removes rows from other rowsets at read time; a full scan reads
        // segments raw and cannot apply it, so those rows would surface as inserts. Reject it, as the
        // version-diff path does, rather than emit deleted rows.
        if (r.has_delete_predicate()) {
            return make_cdc_error(
                    TCdcErrorCode::CHANGE_NOT_TRACKABLE,
                    fmt::format("CDC for {} does not support delete", KeysType_Name(after.schema().keys_type())));
        }
        for (int seg = 0; seg < r.segment_metas_size(); ++seg) {
            if (!_is_primary_keys) {
                // DUP/AGG: no delete vector, so every row in the segment is an insert.
                p.insert_changes.push_back({rs, seg, false, std::nullopt, false});
                continue;
            }
            // Primary key: the segment's live rows are the whole segment minus its delete vector.
            const uint32_t rssid = lake::get_rssid(r, seg);
            ASSIGN_OR_RETURN(Roaring delvec_after, _load_delvec(after, rssid));
            Roaring rows;
            rows.addRange(0, static_cast<uint64_t>(r.segment_metas(seg).num_rows()));
            rows -= delvec_after;
            if (!rows.isEmpty()) {
                p.insert_changes.push_back({rs, seg, false, std::move(rows), true});
            }
        }
    }
    return p;
}

Status ChangesReadPlanner::_locate_changed_segments(const TabletMetadataPB& before_meta,
                                                    const TabletMetadataPB& after_meta,
                                                    std::vector<AddedSegment>* added,
                                                    std::vector<CarriedSegment>* carried,
                                                    AfterPosition* after_position) {
    const int64_t before_version = before_meta.version();
    // after_meta pass: record every segment's position; an added rowset's segments become AddedSegment.
    // A rowset is added iff its version exceeds before_meta's version. This equals the set difference
    // "id in after_meta not in before_meta" given: a surviving rowset keeps its version, and a rowset's
    // version never exceeds its containing metadata's version.
    for (int rs = 0; rs < after_meta.rowsets_size(); ++rs) {
        const auto& r = after_meta.rowsets(rs);
        const bool is_added = r.version() > before_version;
        if (is_added && r.has_delete_predicate()) {
            return make_cdc_error(
                    TCdcErrorCode::CHANGE_NOT_TRACKABLE,
                    fmt::format("CDC for {} does not support delete", KeysType_Name(after_meta.schema().keys_type())));
        }
        for (int seg = 0; seg < r.segment_metas_size(); ++seg) {
            const uint32_t rssid = lake::get_rssid(r, seg);
            (*after_position)[rssid] = {rs, seg};
            if (is_added) {
                added->push_back(AddedSegment{rs, seg, r.has_max_compact_input_rowset_id()});
            }
        }
    }
    // before_meta pass: each segment either survived into after_meta or was merged away by this
    // publish's compaction. A survivor reads its after-side delvec from after_meta's live delvecs; a
    // merged-away one reads it from compaction_input_delvecs, where the pre-merge delvec was captured
    // (after_meta no longer lists the segment). A grown delvec or an overlaid column makes it carried.
    const auto& cdc = after_meta.cdc_metadata();
    for (int rs = 0; rs < before_meta.rowsets_size(); ++rs) {
        const auto& r = before_meta.rowsets(rs);
        for (int seg = 0; seg < r.segment_metas_size(); ++seg) {
            const uint32_t rssid = lake::get_rssid(r, seg);
            const bool is_compaction_input = after_position->find(rssid) == after_position->end();
            const auto& after_delvecs = is_compaction_input ? cdc.pk_change_locator().compaction_input_delvecs()
                                                            : after_meta.delvec_meta().delvecs();
            bool delvec_changed = false;
            auto dv_it = after_delvecs.find(rssid);
            if (dv_it != after_delvecs.end() && dv_it->second.version() > before_version) {
                delvec_changed = true;
            }
            // A missing column_overlay_vecs entry means this publish overlaid no column on this
            // segment, so it is correctly left uncarried below -- never a captured-but-unrecorded
            // overlay. Every column-mode partial update on a traversed publish records its rows
            // (append_dcg takes updated_rowids as required); a publish that cannot record them
            // (primary-key recover, replication) marks the version NotSupported, which the traversal
            // rejects before reaching this step. So the plain presence lookup here cannot drop an
            // update: no entry here is always "no column change on this segment", not "lost rows".
            const bool column_overlaid = cdc.pk_change_locator().column_overlay_vecs().count(rssid) > 0;
            if (delvec_changed || column_overlaid) {
                carried->push_back(CarriedSegment{rs, seg, is_compaction_input, delvec_changed, column_overlaid});
            }
        }
    }
    return Status::OK();
}

Status ChangesReadPlanner::_plan_insert_change_read(const VersionChangeReadPlan& p,
                                                    const std::vector<AddedSegment>& added,
                                                    const std::vector<CarriedSegment>& carried,
                                                    const AfterPosition& after_position,
                                                    std::vector<SegmentChangeReadPlan>* insert_changes) {
    const auto& after = *p.after_meta;
    for (const auto& a : added) {
        const auto& r = after.rowsets(a.rowset_pos);
        const uint32_t rssid = lake::get_rssid(r, a.segment_pos);
        if (!_is_primary_keys) {
            if (a.is_compaction_output) continue; // DUP/AGG compaction output: rows pre-existed
            insert_changes->push_back({a.rowset_pos, a.segment_pos, false, std::nullopt, false});
            continue;
        }
        ASSIGN_OR_RETURN(Roaring delvec_after, _load_delvec(after, rssid));
        Roaring rows;
        if (a.is_compaction_output) {
            // rows it column-updated, minus any the same publish then deleted (delvec_after).
            const auto& upd = after.cdc_metadata().pk_change_locator().column_overlay_vecs();
            auto it = upd.find(rssid);
            if (it != upd.end()) {
                ASSIGN_OR_RETURN(Roaring updated, _load_delvec_page(after, it->second));
                rows = updated - delvec_after;
            }
        } else {
            rows.addRange(0, static_cast<uint64_t>(r.segment_metas(a.segment_pos).num_rows()));
            rows -= delvec_after;
        }
        if (!rows.isEmpty()) {
            insert_changes->push_back({a.rowset_pos, a.segment_pos, false, std::move(rows), true});
        }
    }
    const auto& upd = after.cdc_metadata().pk_change_locator().column_overlay_vecs();
    for (const auto& c : carried) {
        // is_compaction_input is unreachable here: the compaction/partial-update conflict resolver
        // drops any compaction whose input segment got a newer delta column group, so a carried
        // segment is never both column-updated and merged away in one publish.
        if (!c.column_overlaid || c.is_compaction_input) continue;
        const auto& before_r = p.before_meta->rowsets(c.rowset_pos);
        const uint32_t rssid = lake::get_rssid(before_r, c.segment_pos);
        auto pos = after_position.find(rssid);
        if (pos == after_position.end()) continue;
        auto it = upd.find(rssid);
        if (it == upd.end()) continue;
        ASSIGN_OR_RETURN(Roaring updated, _load_delvec_page(after, it->second));
        ASSIGN_OR_RETURN(Roaring delvec_after, _load_delvec(after, rssid));
        Roaring rows = updated - delvec_after;
        if (!rows.isEmpty()) {
            insert_changes->push_back({pos->second.first, pos->second.second, false, std::move(rows), true});
        }
    }
    return Status::OK();
}

Status ChangesReadPlanner::_plan_delete_change_read(const VersionChangeReadPlan& p,
                                                    const std::vector<AddedSegment>& added,
                                                    const std::vector<CarriedSegment>& carried,
                                                    std::vector<SegmentChangeReadPlan>* delete_changes) {
    const auto& after = *p.after_meta;
    const auto& before = *p.before_meta;
    const auto& cdc = after.cdc_metadata();
    // Compaction-output rows changed this publish: deleted (delvec_after) plus column-updated
    // (overlay), minus rows already non-live when the output was produced (output-delvec baseline).
    for (const auto& a : added) {
        if (!a.is_compaction_output) continue;
        const auto& r = after.rowsets(a.rowset_pos);
        const uint32_t rssid = lake::get_rssid(r, a.segment_pos);
        ASSIGN_OR_RETURN(Roaring rows, _load_delvec(after, rssid)); // delvec_after
        auto upd = cdc.pk_change_locator().column_overlay_vecs().find(rssid);
        if (upd != cdc.pk_change_locator().column_overlay_vecs().end()) {
            ASSIGN_OR_RETURN(Roaring updated, _load_delvec_page(after, upd->second));
            rows |= updated;
        }
        auto base = cdc.pk_change_locator().compaction_output_delvecs().find(rssid);
        if (base != cdc.pk_change_locator().compaction_output_delvecs().end()) {
            ASSIGN_OR_RETURN(Roaring baseline, _load_delvec_page(after, base->second));
            rows -= baseline;
        }
        if (!rows.isEmpty()) {
            delete_changes->push_back({a.rowset_pos, a.segment_pos, false, std::move(rows), false});
        }
    }
    // Carried segments: newly deleted rows (after_delvec - before_delvec) and column-updated before
    // values, unioned into one read from before_meta.
    for (const auto& c : carried) {
        const auto& before_r = before.rowsets(c.rowset_pos);
        const uint32_t rssid = lake::get_rssid(before_r, c.segment_pos);
        Roaring rows;
        if (c.delvec_changed) {
            Roaring after_delvec;
            if (c.is_compaction_input) {
                auto it = cdc.pk_change_locator().compaction_input_delvecs().find(rssid);
                if (it != cdc.pk_change_locator().compaction_input_delvecs().end()) {
                    ASSIGN_OR_RETURN(after_delvec, _load_delvec_page(after, it->second));
                }
            } else {
                ASSIGN_OR_RETURN(after_delvec, _load_delvec(after, rssid));
            }
            ASSIGN_OR_RETURN(Roaring before_delvec, _load_delvec(before, rssid));
            rows |= (after_delvec - before_delvec);
        }
        if (c.column_overlaid) {
            auto upd = cdc.pk_change_locator().column_overlay_vecs().find(rssid);
            if (upd != cdc.pk_change_locator().column_overlay_vecs().end()) {
                ASSIGN_OR_RETURN(Roaring updated, _load_delvec_page(after, upd->second));
                ASSIGN_OR_RETURN(Roaring delvec_after, _load_delvec(after, rssid));
                rows |= (updated - delvec_after);
            }
        }
        if (!rows.isEmpty()) {
            delete_changes->push_back({c.rowset_pos, c.segment_pos, true, std::move(rows), true});
        }
    }
    return Status::OK();
}

// =============================================================================
// Connector layer: ChangesConnector / ChangesDataSourceProvider / ChangesDataSource
// =============================================================================

// --- ChangesConnector ---

DataSourceProviderPtr ChangesConnector::create_data_source_provider(ConnectorScanNode* /*scan_node*/,
                                                                    const TPlanNode& plan_node) const {
    return std::make_unique<ChangesDataSourceProvider>(plan_node);
}

// --- ChangesDataSourceProvider ---

ChangesDataSourceProvider::ChangesDataSourceProvider(const TPlanNode& plan_node)
        : _changes_scan_node(plan_node.changes_scan_node) {}

DataSourcePtr ChangesDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<ChangesDataSource>(this, scan_range);
}

const TupleDescriptor* ChangesDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_changes_scan_node.tuple_id);
}

// --- ChangesDataSource ---

ChangesDataSource::ChangesDataSource(const ChangesDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider) {
    const auto& range = scan_range.changes_scan_range;
    _tablet_id = range.tablet_id;
    const auto& spec = range.scan_spec;
    _derivation_mode = spec.derivation_mode;
    _head_version = spec.head_version;
    if (_derivation_mode != TChangeDerivationMode::FULL_SCAN) {
        _base_version = spec.base_version;
    }
    _resolve_cache_mode(range);
}

// Folds the scan range's four cache fields into the three settings the read paths consume.
// The mode is the outer gate: NEVER stops this scan from filling the caches these three settings
// reach, ALWAYS defers to the per-partition and per-session controls FE already resolved. The
// table's schema entry is outside all of them -- TableSchemaService caches it unconditionally and
// shares one entry across every reader of the table, so no per-scan control exists for it.
//
// How far NEVER reaches differs by cache, because the layers below expose different controls.
// The data cache separates the two directions -- skip_disk_cache decides lookups, fill_data_cache
// decides writes -- so NEVER still reads whatever another path has already warmed there. The page
// cache has a single flag covering both (see page_io.cpp: use_page_cache guards the lookup as well
// as the insert), so NEVER gives up its hits too. That is the right trade for the workload NEVER
// exists to serve: a backfill wide enough to be worth keeping out of the cache gets little from
// pages a different query happened to warm.
void ChangesDataSource::_resolve_cache_mode(const TChangesScanRange& range) {
    auto mode = range.__isset.cache_mode ? range.cache_mode : TChangesScanCacheMode::ALWAYS;
    // A mode this BE does not recognize falls back to ALWAYS: during a rolling upgrade a newer FE
    // must not silently disable caching here.
    if (mode != TChangesScanCacheMode::NEVER && mode != TChangesScanCacheMode::ALWAYS) {
        LOG(WARNING) << "unrecognized CHANGES scan cache mode " << static_cast<int>(mode) << " on tablet " << _tablet_id
                     << ", falling back to ALWAYS";
        mode = TChangesScanCacheMode::ALWAYS;
    }
    const bool may_fill = mode == TChangesScanCacheMode::ALWAYS;
    const bool fill_data_cache = !range.__isset.fill_data_cache || range.fill_data_cache;
    const bool skip_page_cache = range.__isset.skip_page_cache && range.skip_page_cache;

    _lake_io_opts.fill_data_cache = may_fill && fill_data_cache;
    // Match TabletReaderParams' default: a scan keeps the Segment objects it opens.
    _lake_io_opts.fill_metadata_cache = may_fill;
    _lake_io_opts.skip_disk_cache = range.__isset.skip_disk_cache && range.skip_disk_cache;
    _use_page_cache = may_fill && fill_data_cache && !skip_page_cache && !config::disable_storage_page_cache;
    _cache_tablet_metadata = may_fill;
}

Status ChangesDataSource::open(RuntimeState* state) {
    _runtime_state = state;
    _init_counter();

    const auto& scan_node = _provider->_changes_scan_node;
    const auto* tuple_desc = state->desc_tbl().get_tuple_descriptor(scan_node.tuple_id);
    if (tuple_desc == nullptr) {
        return Status::InternalError("tuple descriptor not found");
    }
    _all_slots = &tuple_desc->slots();
    // Classify each tuple slot as data vs. CHANGES metadata by matching its
    // col_name against the descriptor names in the plan node. A descriptor
    // whose name has no matching slot was dropped by projection pruning.
    const auto& meta_descriptors = scan_node.meta_descriptors;
    std::unordered_map<std::string, const TChangesMetaDescriptor*> name_to_descriptor;
    name_to_descriptor.reserve(meta_descriptors.size());
    for (const auto& descriptor : meta_descriptors) {
        name_to_descriptor.emplace(descriptor.name, &descriptor);
    }
    for (auto* slot : tuple_desc->slots()) {
        auto it = name_to_descriptor.find(std::string(slot->col_name()));
        if (it != name_to_descriptor.end()) {
            _changes_meta_slots.push_back(ChangesMetaSlot{it->second->kind, slot});
        } else {
            _data_slots.push_back(slot);
        }
    }

    auto* tablet_mgr = lake_tablet_manager();
    if (tablet_mgr == nullptr) {
        return Status::InternalError("lake tablet manager not available");
    }
    ASSIGN_OR_RETURN(_head_metadata,
                     tablet_mgr->get_tablet_metadata(_tablet_id, _head_version, _cache_tablet_metadata));
    RETURN_IF_ERROR(_init_tablet_schema());
    RETURN_IF_ERROR(_init_pushdown_predicates());
    RETURN_IF_ERROR(_init_storage_read_schema());
    RETURN_IF_ERROR(_init_output_columns());

    // Initialize the changes read planner
    if (_derivation_mode != TChangeDerivationMode::FULL_SCAN && _base_version > _head_version) {
        return Status::InvalidArgument(fmt::format("CHANGES version range invalid: base_version({}) > head_version({})",
                                                   _base_version, _head_version));
    }
    _changes_read_planner.emplace(tablet_mgr, _is_primary_key_table(), _lake_io_opts);
    _current_meta = _head_metadata;

    return Status::OK();
}

void ChangesDataSource::close(RuntimeState* state) {
    if (_scan_counters != nullptr) {
        _update_counter();
    }
    if (_current_segment_iterator != nullptr) _current_segment_iterator->close();
    _current_segment_iterator.reset();
    _current_plan.reset();
    _current_meta.reset();
    _changes_read_planner.reset();
    _conjuncts_manager.reset();
    _residual_conjunct_ctxs.clear();
    _data_slot_conjunct_ctxs.clear();
    _head_metadata.reset();
    _tablet_schema.reset();
}

Status ChangesDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    return _read_next_chunk(chunk);
}

void ChangesDataSource::_init_counter() {
    if (_runtime_profile == nullptr) return;
    _scan_counters = std::make_unique<ScanCounters>();
    _scan_counters->raw_rows = ADD_COUNTER(_runtime_profile, "RawRowsRead", TUnit::UNIT);
    _scan_counters->zonemap_filtered = ADD_COUNTER(_runtime_profile, "ZoneMapIndexFilterRows", TUnit::UNIT);
    _scan_counters->bloom_filter_filtered = ADD_COUNTER(_runtime_profile, "BloomFilterFilterRows", TUnit::UNIT);
    _scan_counters->short_key_filtered = ADD_COUNTER(_runtime_profile, "ShortKeyFilterRows", TUnit::UNIT);
    _scan_counters->predicate_filtered = ADD_COUNTER(_runtime_profile, "PredFilterRows", TUnit::UNIT);
    _scan_counters->runtime_filter_input = ADD_COUNTER(_runtime_profile, "RuntimeFilterInputRows", TUnit::UNIT);
    _scan_counters->runtime_filter_output = ADD_COUNTER(_runtime_profile, "RuntimeFilterOutputRows", TUnit::UNIT);
}

Status ChangesDataSource::_init_tablet_schema() {
    DCHECK(_all_slots != nullptr);
    DCHECK(_head_metadata != nullptr);

    auto* tablet_mgr = lake_tablet_manager();
    const auto& t_schema_key = _provider->_changes_scan_node.schema_key;
    TableSchemaKeyPB schema_key_pb;
    schema_key_pb.set_db_id(t_schema_key.db_id);
    schema_key_pb.set_table_id(t_schema_key.table_id);
    schema_key_pb.set_schema_id(t_schema_key.schema_id);
    ASSIGN_OR_RETURN(_tablet_schema, tablet_mgr->table_schema_service()->get_schema_for_scan(
                                             schema_key_pb, _tablet_id, _runtime_state->query_id(),
                                             _runtime_state->fragment_runtime_state()->fe_addr(), _head_metadata));
    ASSIGN_OR_RETURN(_tablet_schema, extend_schema_by_virtual_columns(_tablet_schema, *_all_slots));
    return Status::OK();
}

Status ChangesDataSource::_init_pushdown_predicates() {
    DCHECK(_runtime_state != nullptr);
    DCHECK(_tablet_schema != nullptr);

    // A constant-false/null predicate (e.g. WHERE 1=0) makes the scan empty; short-circuit to EndOfFile.
    Status const_conjuncts_status;
    RETURN_IF_ERROR(ScanConjunctsManager::eval_const_conjuncts(_conjunct_ctxs, &const_conjuncts_status));
    if (!const_conjuncts_status.ok()) return const_conjuncts_status;

    const auto* tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(_provider->_changes_scan_node.tuple_id);

    // Split by metadata-slot membership: a conjunct touching a CHANGES metadata column
    // (__CHANGE_TYPE__ / __ROW_VERSION__) becomes a residual and never reaches the parser. Those
    // columns are appended after the segment read and absent from _tablet_schema, so
    // OlapPredicateParser would map their column_id to (uint32_t)-1 and crash the segment iterator.
    // Only data-column conjuncts are pushed down.
    std::unordered_set<SlotId> meta_slot_ids;
    for (const auto& m : _changes_meta_slots) {
        meta_slot_ids.insert(m.slot->id());
    }
    for (auto* ctx : _conjunct_ctxs) {
        bool touches_meta = false;
        ctx->root()->for_each_slot_id([&](SlotId id) { touches_meta |= (meta_slot_ids.count(id) > 0); });
        if (touches_meta) {
            _residual_conjunct_ctxs.push_back(ctx);
        } else {
            _data_slot_conjunct_ctxs.push_back(ctx);
        }
    }

    if (_runtime_filters != nullptr) {
        for (const auto& [filter_id, desc] : _runtime_filters->descriptors()) {
            SlotId slot_id;
            if (desc->is_probe_slot_ref(&slot_id) && meta_slot_ids.count(slot_id) > 0) {
                continue; // metadata-column RF: keep it out of the storage path; operator-level eval still applies it.
            }
            _data_column_runtime_filters.add_descriptor(desc);
        }
    }

    for (uint32_t idx : _tablet_schema->sort_key_idxes()) {
        _sort_key_column_names.emplace_back(_tablet_schema->column(idx).name());
    }

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_data_slot_conjunct_ctxs;
    opts.tuple_desc = tuple_desc;
    opts.obj_pool = &_obj_pool;
    opts.runtime_state = _runtime_state;
    opts.scan_keys_unlimited = true;
    opts.key_column_names = &_sort_key_column_names;
    const auto& query_options = _runtime_state->query_options();
    opts.max_scan_key_num = (query_options.__isset.max_scan_key_num && query_options.max_scan_key_num > 0)
                                    ? query_options.max_scan_key_num
                                    : config::max_scan_key_num;
    // _data_column_runtime_filters (built above, excluding any probe on a CHANGES metadata slot) is
    // what ScanConjunctsManager sees: OlapPredicateParser has no column for those slots.
    opts.runtime_filters = &_data_column_runtime_filters;
    opts.driver_sequence = runtime_membership_filter_eval_context.driver_sequence;
    opts.pred_tree_params = _runtime_state->fragment_runtime_state()->pred_tree_params();
    _conjuncts_manager = std::make_unique<ScanConjunctsManager>(opts);
    RETURN_IF_ERROR(_conjuncts_manager->parse_conjuncts());

    OlapPredicateParser* parser = _obj_pool.add(new OlapPredicateParser(_tablet_schema));
    ASSIGN_OR_RETURN(auto pred_tree, _conjuncts_manager->get_predicate_tree(parser, _parsed_column_predicates));
    PredicateAndNode pushdown_root;
    PredicateAndNode non_pushdown_root;
    pred_tree.root().partition_copy([parser](const auto& node) { return parser->can_pushdown(node); }, &pushdown_root,
                                    &non_pushdown_root);
    _pushdown_pred_tree = PredicateTree::create(std::move(pushdown_root));
    _residual_pred_tree = PredicateTree::create(std::move(non_pushdown_root));
    RETURN_IF_ERROR(ZonemapPredicatesRewriter::rewrite_predicate_tree(&_obj_pool, _pushdown_pred_tree,
                                                                      _pushdown_pred_tree_for_zone_map));

    if (_runtime_state->enable_join_runtime_filter_pushdown()) {
        ASSIGN_OR_RETURN(_runtime_filter_preds, _conjuncts_manager->get_runtime_filter_predicates(&_obj_pool, parser));
        _runtime_range_pruner = RuntimeScanRangePruner(parser, _conjuncts_manager->unarrived_runtime_filters());
    }

    std::vector<ExprContext*> not_pushdown;
    _conjuncts_manager->get_not_push_down_conjuncts(&not_pushdown);
    _residual_conjunct_ctxs.insert(_residual_conjunct_ctxs.end(), not_pushdown.begin(), not_pushdown.end());

    std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
    RETURN_IF_ERROR(_conjuncts_manager->get_key_ranges(&key_ranges));
    std::vector<OlapTuple> start_keys;
    std::vector<OlapTuple> end_keys;
    auto range_op = TabletReaderParams::RangeStartOperation::GT;
    auto end_range_op = TabletReaderParams::RangeEndOperation::LT;
    for (const auto& kr : key_ranges) {
        if (kr->begin_scan_range.size() == 1 && kr->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }
        range_op = kr->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                     : TabletReaderParams::RangeStartOperation::GT;
        end_range_op =
                kr->end_include ? TabletReaderParams::RangeEndOperation::LE : TabletReaderParams::RangeEndOperation::LT;
        start_keys.push_back(kr->begin_scan_range);
        end_keys.push_back(kr->end_scan_range);
    }
    RETURN_IF_ERROR(lake::TabletReader::parse_seek_range(*_tablet_schema, range_op, end_range_op, start_keys, end_keys,
                                                         &_pushdown_key_ranges, &_mem_pool));
    return Status::OK();
}

Status ChangesDataSource::_init_storage_read_schema() {
    DCHECK(_tablet_schema != nullptr);

    std::vector<uint32_t> cids;
    cids.reserve(_data_slots.size());
    for (auto* slot : _data_slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        cids.push_back(static_cast<uint32_t>(index));
    }
    if (cids.empty()) {
        // Metadata-only projection (e.g. SELECT __ROW_VERSION__) has no data slot, so the segment
        // iterator would open over an empty schema and every chunk would surface with num_rows() == 0,
        // dropping the whole read before the metadata columns are appended. Force-include the first
        // tablet column so the iterator drives the row count from real segment data.
        _read_schema_has_forced_column = true;
        _storage_read_schema = ChunkHelper::convert_schema(_tablet_schema, std::vector<uint32_t>{0});
    } else {
        std::sort(cids.begin(), cids.end());
        _storage_read_schema = ChunkHelper::convert_schema(_tablet_schema, cids);
    }
    return Status::OK();
}

Status ChangesDataSource::_init_output_columns() {
    DCHECK(_tablet_schema != nullptr);

    // Classify each data slot and map it to its tablet cid. _unused_cids_after_pushdown starts as every
    // read data cid; the residual/output passes below whittle it down to the pushdown-predicate-only set.
    std::unordered_map<SlotId, uint32_t> slot_to_cid;
    for (auto* slot : _data_slots) {
        auto cid = static_cast<uint32_t>(_tablet_schema->field_index(slot->col_name()));
        slot_to_cid[slot->id()] = cid;
        _unused_cids_after_pushdown.insert(cid);
        if (slot->is_output_column()) {
            _has_output_column = true;
        } else {
            _unused_slot_ids.insert(slot->id());
        }
    }
    // A predicate-only metadata column (e.g. WHERE __CHANGE_TYPE__ = 1) is appended for the residual eval
    // but must not surface, so it joins the surface drop-set too.
    for (const auto& m : _changes_meta_slots) {
        if (m.slot->is_output_column()) {
            _has_output_column = true;
        } else {
            _unused_slot_ids.insert(m.slot->id());
        }
    }

    // Keep in _unused_cids_after_pushdown only columns nothing surfaces: not an output slot and not read
    // by any residual predicate (cid-space tree plus slot-space conjuncts translated to cids).
    for (auto cid : _residual_pred_tree.column_ids()) {
        _unused_cids_after_pushdown.erase(cid);
    }
    for (auto* ctx : _residual_conjunct_ctxs) {
        ctx->root()->for_each_slot_id([&](SlotId id) {
            auto it = slot_to_cid.find(id);
            if (it != slot_to_cid.end()) _unused_cids_after_pushdown.erase(it->second);
        });
    }
    for (auto* slot : _data_slots) {
        if (slot->is_output_column()) _unused_cids_after_pushdown.erase(slot_to_cid[slot->id()]);
    }

    // The output chunk drives its row count from its columns, so the read must keep at least one column
    // materialized. When every read column is pushdown-predicate-only, dropping them all would leave an
    // empty output schema whose chunks report zero rows and silently drop every change. Retain the last
    // one; it is non-output, so the surface step still strips it.
    if (_storage_read_schema.num_fields() > 0 &&
        _unused_cids_after_pushdown.size() >= _storage_read_schema.num_fields()) {
        _unused_cids_after_pushdown.erase(_storage_read_schema.field(_storage_read_schema.num_fields() - 1)->id());
    }
    return Status::OK();
}

void ChangesDataSource::_update_counter() {
    const auto& i = _insert_read_stats;
    const auto& d = _delete_read_stats;
    COUNTER_UPDATE(_scan_counters->raw_rows, i.raw_rows_read + d.raw_rows_read);
    COUNTER_UPDATE(_scan_counters->zonemap_filtered, i.rows_stats_filtered + d.rows_stats_filtered);
    COUNTER_UPDATE(_scan_counters->bloom_filter_filtered, i.rows_bf_filtered + d.rows_bf_filtered);
    COUNTER_UPDATE(_scan_counters->short_key_filtered, i.rows_key_range_filtered + d.rows_key_range_filtered);
    COUNTER_UPDATE(_scan_counters->predicate_filtered, i.rows_vec_cond_filtered + d.rows_vec_cond_filtered);
    COUNTER_UPDATE(_scan_counters->runtime_filter_input, i.rf_cond_input_rows + d.rf_cond_input_rows);
    COUNTER_UPDATE(_scan_counters->runtime_filter_output, i.rf_cond_output_rows + d.rf_cond_output_rows);
}

Status ChangesDataSource::_read_next_chunk(ChunkPtr* chunk) {
    while (true) {
        // Advance the lazy traversal until an open iterator is ready. Within a publish, drain
        // insert_changes (INSERT) then delete_changes (DELETE); then step to the next publish, whose
        // plan is built only on reach.
        while (_current_segment_iterator == nullptr) {
            if (!_current_plan.has_value()) {
                ASSIGN_OR_RETURN(bool has, _advance_to_next_version());
                if (!has) return Status::EndOfFile("end of changes data");
                continue;
            }
            const auto& changes = (_current_change_type == ChangeType::DELETE) ? _current_plan->delete_changes
                                                                               : _current_plan->insert_changes;
            if (_current_segment_index < changes.size()) {
                ASSIGN_OR_RETURN(
                        _current_segment_iterator,
                        _build_segment_iterator(*_current_plan, changes[_current_segment_index], _current_change_type));
                ++_current_segment_index; // a null result (empty segment) re-loops and steps to the next read
            } else if (_current_change_type == ChangeType::INSERT) {
                _current_change_type = ChangeType::DELETE;
                _current_segment_index = 0;
            } else {
                ASSIGN_OR_RETURN(bool has, _advance_to_next_version());
                if (!has) return Status::EndOfFile("end of changes data");
            }
        }
        auto data_chunk =
                ChunkFactory::new_chunk(_current_segment_iterator->output_schema(), _runtime_state->chunk_size());
        Status st = _current_segment_iterator->get_next(data_chunk.get());
        if (st.is_end_of_file()) {
            _current_segment_iterator->close();
            _current_segment_iterator.reset();
            continue;
        }
        RETURN_IF_ERROR(st);
        if (data_chunk->num_rows() == 0) continue;
        // Residual predicates the storage read could not enforce, in two phases. Pushed-down
        // predicates already filtered at storage and are NOT re-checked here.
        // (1) ColumnPredicates that could not be pushed down, in cid space. Runs on the data-only
        //     chunk — before the metadata columns are appended — so every cid resolves against a
        //     schema that still describes exactly the columns present.
        if (!_residual_pred_tree.empty()) {
            size_t nrows = data_chunk->num_rows();
            _reused_selection.resize(nrows);
            RETURN_IF_ERROR(_residual_pred_tree.evaluate(data_chunk.get(), _reused_selection.data(), 0,
                                                         static_cast<uint16_t>(nrows)));
            data_chunk->filter(_reused_selection);
            if (data_chunk->num_rows() == 0) continue;
        }

        // Slot-space residual evaluation and the tuple resolve SlotRef by slot id, so the data columns need
        // a slot-id map. Match each data slot to its chunk column by name against the read chunk's schema,
        // which reflects init_output_schema: a pushdown-predicate-only column dropped there is absent and
        // simply gets no mapping. The forced row-count column (metadata-only projection) carries no data
        // slot's name and stays unmapped too.
        const auto& schema = data_chunk->schema();
        for (auto* slot : _data_slots) {
            for (size_t i = 0; i < schema->num_fields(); i++) {
                if (schema->field(i)->name() == slot->col_name()) {
                    data_chunk->set_slot_id_to_index(slot->id(), i);
                    break;
                }
            }
        }
        data_chunk->reset_schema();

        // (2) Append the CHANGES metadata columns, then evaluate the slot-space residual conjuncts
        //     (conjuncts that did not reduce to a ColumnPredicate + metadata-column conjuncts), which
        //     need every column present.
        RETURN_IF_ERROR(
                _append_meta_columns(data_chunk.get(), _current_change_type, _current_plan->after_meta->version()));
        if (!_residual_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_residual_conjunct_ctxs, data_chunk.get()));
            if (data_chunk->num_rows() == 0) continue;
        }

        // (3) Narrow to strictly the isOutputColumn slots. Non-output columns (residual-only data
        //     columns, predicate-only metadata columns) were needed by the eval phases above but must
        //     not reach the tuple. When nothing is an output column (every slot is predicate-only, e.g.
        //     SELECT <const> over a predicate-only column), stripping every column would zero the row
        //     count and silently drop every change; surface the chunk unchanged instead — the caller
        //     addresses columns by slot id and ignores the non-output ones.
        if (_has_output_column) {
            for (auto slot_id : _unused_slot_ids) {
                data_chunk->remove_column_by_slot_id(slot_id);
            }
            if (_read_schema_has_forced_column) {
                // A metadata-only projection forced tablet column 0 into the read to drive the row count;
                // no slot references it, so remove_column_by_slot_id can't reach it. The metadata columns
                // now carry the row count, so drop the filler (index 0, appended before them) and reindex
                // the surviving output metadata columns.
                data_chunk->remove_column_by_index(0);
                data_chunk->reset_slot_id_to_index();
                size_t idx = 0;
                for (const auto& m : _changes_meta_slots) {
                    if (m.slot->is_output_column()) data_chunk->set_slot_id_to_index(m.slot->id(), idx++);
                }
            }
        }
        _rows_read += data_chunk->num_rows();
        _bytes_read += data_chunk->bytes_usage();
        *chunk = std::move(data_chunk);
        return Status::OK();
    }
}

StatusOr<bool> ChangesDataSource::_advance_to_next_version() {
    if (_derivation_mode == TChangeDerivationMode::FULL_SCAN) {
        // FULL_SCAN emits head once as all inserts, then stops. Only head's metadata is read; the
        // ancestor chain is never walked, so a reclaimed sub-head version cannot fail the scan.
        if (_full_scan_planned) return false;
        ASSIGN_OR_RETURN(VersionChangeReadPlan plan, _changes_read_planner->plan_full_scan(_head_metadata));
        _current_plan = std::move(plan);
        _current_change_type = ChangeType::INSERT;
        _current_segment_index = 0;
        _current_segment_iterator.reset();
        _full_scan_planned = true;
        return true;
    }
    const int64_t current_version = _current_meta->version();
    if (current_version <= _base_version) return false;
    RETURN_IF_ERROR(_check_degradation(_current_meta));
    int64_t parent_version = -1;
    if (_current_meta->metadata_ancestors_size() > 0) {
        const int64_t direct_parent = _current_meta->metadata_ancestors(0);
        if (direct_parent >= _base_version && direct_parent < current_version) {
            parent_version = direct_parent;
        }
    }
    if (parent_version < 0) {
        return make_cdc_error(
                TCdcErrorCode::CHANGE_NOT_TRACKABLE,
                fmt::format("CHANGES ancestor chain on tablet {} cannot reach base version {} from version {}",
                            _tablet_id, _base_version, current_version));
    }
    auto* tablet_mgr = lake_tablet_manager();
    ASSIGN_OR_RETURN(auto parent_meta,
                     tablet_mgr->get_tablet_metadata(_tablet_id, parent_version, _cache_tablet_metadata));
    ASSIGN_OR_RETURN(VersionChangeReadPlan plan, _changes_read_planner->plan_version_diff(parent_meta, _current_meta));
    _current_plan = std::move(plan);
    _current_change_type = ChangeType::INSERT;
    _current_segment_index = 0;
    _current_segment_iterator.reset();
    _current_meta = parent_meta;
    return true;
}

Status ChangesDataSource::_check_degradation(const TabletMetadataPtr& meta) const {
    const auto& cdc = meta->cdc_metadata();
    // enable_cdc gates only primary-key CDC, which needs the recorded change locator. Duplicate-key and
    // aggregate changes are derivable from the base rowset metadata, so their CDC is always available.
    if (_is_primary_key_table() && !cdc.enable_cdc()) {
        return make_cdc_error(TCdcErrorCode::CHANGE_NOT_TRACKABLE,
                              fmt::format("CHANGES window on tablet {} spans version {} which was not recorded "
                                          "(change data capture was not enabled at that version)",
                                          meta->id(), meta->version()));
    }
    if (meta->has_cdc_metadata() && cdc.has_capture_status()) {
        Status status(cdc.capture_status());
        if (!status.ok()) {
            return make_cdc_error(
                    TCdcErrorCode::CHANGE_NOT_TRACKABLE,
                    fmt::format("CHANGES window on tablet {} spans version {} whose changes were not captured: {}",
                                meta->id(), meta->version(), status.to_string(false)));
        }
    }
    return Status::OK();
}

StatusOr<ChunkIteratorPtr> ChangesDataSource::_build_segment_iterator(const VersionChangeReadPlan& plan,
                                                                      const SegmentChangeReadPlan& seg,
                                                                      ChangeType change_type) {
    auto* tablet_mgr = lake_tablet_manager();
    const TabletMetadataPtr& read_meta = seg.from_before_meta ? plan.before_meta : plan.after_meta;
    OlapReaderStatistics* stats = (change_type == ChangeType::INSERT) ? &_insert_read_stats : &_delete_read_stats;
    // Single-segment range so only this segment's footer loads.
    auto rowset =
            std::make_shared<lake::Rowset>(tablet_mgr, read_meta, seg.rowset_pos, seg.segment_pos, seg.segment_pos + 1);

    std::vector<ChunkIteratorPtr> iters;
    if (_is_primary_key_table()) {
        // PK: read exactly seg.rowids with no delete vector (the rows were already selected and must
        // not be re-filtered). Apply the dcg overlay only for a column-update read; a raw read
        // (read_with_dcg == false) returns the stored bytes as-is. Every PK read carries rowids.
        auto range = std::make_shared<SparseRange<>>(roaring2range(*seg.rowids));
        std::vector<SparseRangePtr> ranges{std::move(range)};
        RowsetReadOptions opts;
        opts.stats = stats;
        opts.version = read_meta->version();
        // A primary-key read; the delvec-free segment read below requires this flag.
        opts.is_primary_keys = true;
        // Resolve pushed predicates and the read schema against the scan schema, not the rowset's
        // historical schema, so a CHANGES range spanning a schema change reads the intended columns.
        opts.tablet_schema = _tablet_schema;
        opts.pred_tree = _pushdown_pred_tree;
        opts.pred_tree_for_zone_map = _pushdown_pred_tree_for_zone_map;
        opts.ranges = _pushdown_key_ranges;
        opts.enable_join_runtime_filter_pushdown = _runtime_state->enable_join_runtime_filter_pushdown();
        opts.runtime_filter_preds = _runtime_filter_preds;
        opts.runtime_range_pruner = _runtime_range_pruner;
        opts.runtime_state = _runtime_state;
        opts.lake_io_opts = _lake_io_opts;
        opts.use_page_cache = _use_page_cache;
        ASSIGN_OR_RETURN(iters, rowset->get_each_segment_iterator_no_delvec(_storage_read_schema, opts,
                                                                            /*apply_dcg=*/seg.read_with_dcg, &ranges));
    } else {
        // DUP/AGG: a plain whole-segment read; these tables have no delete vector or dcg overlay.
        RowsetReadOptions opts;
        opts.stats = stats;
        opts.chunk_size = _runtime_state->chunk_size();
        opts.tablet_schema = _tablet_schema;
        opts.lake_io_opts = _lake_io_opts;
        opts.use_page_cache = _use_page_cache;
        opts.is_primary_keys = false;
        opts.pred_tree = _pushdown_pred_tree;
        opts.pred_tree_for_zone_map = _pushdown_pred_tree_for_zone_map;
        opts.ranges = _pushdown_key_ranges;
        opts.enable_join_runtime_filter_pushdown = _runtime_state->enable_join_runtime_filter_pushdown();
        opts.runtime_filter_preds = _runtime_filter_preds;
        opts.runtime_range_pruner = _runtime_range_pruner;
        opts.runtime_state = _runtime_state;
        ASSIGN_OR_RETURN(iters, rowset->read(_storage_read_schema, opts));
    }

    if (iters.empty()) return ChunkIteratorPtr{nullptr};
    if (iters.size() > 1) {
        return Status::InternalError(fmt::format("CHANGES single-segment read returned {} iterators on tablet {}",
                                                 iters.size(), _tablet_id));
    }
    // Drop the pushdown-predicate-only columns from the iterator's output: the read still filters on
    // them, but their dict-decode and materialization are skipped. Neither CHANGES read path passes
    // delete predicates, so narrowing the output schema is safe here.
    if (!_unused_cids_after_pushdown.empty()) {
        RETURN_IF_ERROR(iters[0]->init_output_schema(_unused_cids_after_pushdown));
    }
    return std::move(iters[0]);
}

Status ChangesDataSource::_append_meta_columns(Chunk* chunk, ChangeType change_type, int64_t row_version) {
    size_t nrows = chunk->num_rows();
    for (const auto& meta : _changes_meta_slots) {
        ColumnPtr col;
        switch (meta.kind) {
        case TChangesMetaKind::CHANGE_TYPE: {
            auto val_col = Int8Column::create();
            auto v = static_cast<int8_t>(change_type);
            val_col->append_value_multiple_times(&v, nrows);
            col = ColumnPtr(std::move(val_col));
            break;
        }
        case TChangesMetaKind::ROW_VERSION: {
            auto val_col = Int64Column::create();
            val_col->append_value_multiple_times(&row_version, nrows);
            col = ColumnPtr(std::move(val_col));
            break;
        }
        default:
            return Status::InternalError(fmt::format("unhandled TChangesMetaKind: {}", static_cast<int>(meta.kind)));
        }
        if (meta.slot->is_nullable()) {
            auto null_col = NullColumn::create(nrows, 0);
            col = NullableColumn::create(std::move(col), std::move(null_col));
        }
        chunk->append_column(std::move(col), meta.slot->id());
    }
    return Status::OK();
}

} // namespace starrocks::connector
