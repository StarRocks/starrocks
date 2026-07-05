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

#include "connector/changes_connector.h"

#include <fmt/format.h>

#include <algorithm>
#include <unordered_map>

#include "column/chunk_factory.h"
#include "column/nullable_column.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/rowset.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/rowset_options.h"
#include "storage/storage_env.h"
#include "storage/virtual_column_utils.h"
#include "storage_primitive/range.h"
#include "storage_primitive/roaring2range.h"

namespace starrocks::connector {

namespace {

// Per-segment ChunkIterator wrapper that appends one column per requested
// CHANGES metadata kind onto every chunk it surfaces. Each instance is
// bound to its source rowset's version, so the right version is written into
// each row regardless of which publish the rows came from.
class ChangesMetaColumnIterator final : public ChunkIterator {
public:
    ChangesMetaColumnIterator(ChunkIteratorPtr inner, int64_t version,
                              const std::vector<std::pair<SlotId, size_t>>& data_slot_chunk_indices,
                              const std::vector<ChangesMetaSlot>& meta_slots, int8_t change_type = 0)
            : ChunkIterator(inner->schema(), inner->chunk_size()),
              _inner(std::move(inner)),
              _version(version),
              _change_type(change_type),
              _data_slot_chunk_indices(&data_slot_chunk_indices),
              _meta_slots(&meta_slots) {}

    void close() override {
        if (_inner != nullptr) {
            _inner->close();
            _inner.reset();
        }
    }

protected:
    Status do_get_next(Chunk* chunk) override {
        RETURN_IF_ERROR(_inner->get_next(chunk));
        // Post-read conjunct evaluation resolves SlotRef by slot id, so the
        // inner segment iterator's name-indexed columns need a slot-id map.
        // The mapping is precomputed against the projected read schema and
        // stays valid for every chunk this iterator surfaces.
        for (const auto& [slot_id, col_idx] : *_data_slot_chunk_indices) {
            chunk->set_slot_id_to_index(slot_id, col_idx);
        }
        size_t nrows = chunk->num_rows();
        for (const auto& meta : *_meta_slots) {
            ASSIGN_OR_RETURN(ColumnPtr col, _build_metadata_column(meta.kind, nrows));
            if (meta.slot->is_nullable()) {
                auto null_col = NullColumn::create(nrows, 0);
                col = NullableColumn::create(std::move(col), std::move(null_col));
            }
            chunk->append_column(std::move(col), meta.slot->id());
        }
        return Status::OK();
    }

private:
    StatusOr<ColumnPtr> _build_metadata_column(TChangesMetaKind::type kind, size_t nrows) const {
        switch (kind) {
        case TChangesMetaKind::CHANGE_TYPE: {
            // 0 = INSERT (after value), 1 = DELETE (before value). Each wrapper is
            // built for one side, so every row it surfaces carries the same tag.
            auto val_col = Int8Column::create();
            val_col->reserve(nrows);
            for (size_t r = 0; r < nrows; r++) {
                val_col->append(_change_type);
            }
            return ColumnPtr(std::move(val_col));
        }
        case TChangesMetaKind::ROW_VERSION: {
            auto val_col = Int64Column::create();
            val_col->reserve(nrows);
            for (size_t r = 0; r < nrows; r++) {
                val_col->append(_version);
            }
            return ColumnPtr(std::move(val_col));
        }
        }
        return Status::InternalError(fmt::format("unhandled TChangesMetaKind: {}", static_cast<int>(kind)));
    }

    ChunkIteratorPtr _inner;
    int64_t _version;
    int8_t _change_type;
    const std::vector<std::pair<SlotId, size_t>>* _data_slot_chunk_indices;
    const std::vector<ChangesMetaSlot>* _meta_slots;
};

lake::TabletManager* lake_tablet_manager() {
    return StorageEnv::GetInstance()->lake_tablet_manager();
}

// Load a segment's delete vector by rssid from |metadata|'s live delvecs map; empty Roaring if absent.
static StatusOr<Roaring> load_delvec(lake::TabletManager* mgr, const TabletMetadata& metadata, uint32_t rssid) {
    DelVector dv;
    RETURN_IF_ERROR(lake::get_del_vec(mgr, metadata, rssid, /*fill_cache=*/false, LakeIOOptions{}, &dv));
    return dv.empty() ? Roaring{} : *dv.roaring();
}
// Load a delvec page (a value from a CdcMetadataPB capture map), using |metadata| to resolve the file.
static StatusOr<Roaring> load_delvec_page(lake::TabletManager* mgr, const TabletMetadata& metadata,
                                          const DelvecPagePB& page) {
    DelVector dv;
    RETURN_IF_ERROR(lake::get_del_vec(mgr, metadata, page, /*fill_cache=*/false, LakeIOOptions{}, &dv));
    return dv.empty() ? Roaring{} : *dv.roaring();
}

} // namespace

// =============================================================================
// Planning layer: ChangesReadPlanner
// =============================================================================

StatusOr<VersionChangeReadPlan> ChangesReadPlanner::plan(TabletMetadataPtr before_meta, TabletMetadataPtr after_meta) {
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
            return Status::NotSupported(
                    fmt::format("DELETE_PREDICATE_FOUND: CHANGES not supported for DELETE operations on tablet {}",
                                after_meta.id()));
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
            const auto& after_delvecs =
                    is_compaction_input ? cdc.compaction_input_delvecs() : after_meta.delvec_meta().delvecs();
            bool delvec_changed = false;
            auto dv_it = after_delvecs.find(rssid);
            if (dv_it != after_delvecs.end() && dv_it->second.version() > before_version) {
                delvec_changed = true;
            }
            // A missing column_overlay_vecs entry means this publish overlaid no column on this
            // segment, so it is correctly left uncarried below -- never a captured-but-unrecorded
            // overlay. Every column-mode partial update on a walked publish records its rows
            // (append_dcg takes updated_rowids as required); a publish that cannot record them
            // (primary-key recover, replication) marks the version NotSupported, which the walk
            // rejects before reaching this step. So the plain presence lookup here cannot drop an
            // update: no entry here is always "no column change on this segment", not "lost rows".
            const bool column_overlaid = cdc.column_overlay_vecs().count(rssid) > 0;
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
        ASSIGN_OR_RETURN(Roaring delvec_after, load_delvec(_tablet_mgr, after, rssid));
        Roaring rows;
        if (a.is_compaction_output) {
            // rows it column-updated, minus any the same publish then deleted (delvec_after).
            const auto& upd = after.cdc_metadata().column_overlay_vecs();
            auto it = upd.find(rssid);
            if (it != upd.end()) {
                ASSIGN_OR_RETURN(Roaring updated, load_delvec_page(_tablet_mgr, after, it->second));
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
    const auto& upd = after.cdc_metadata().column_overlay_vecs();
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
        ASSIGN_OR_RETURN(Roaring updated, load_delvec_page(_tablet_mgr, after, it->second));
        ASSIGN_OR_RETURN(Roaring delvec_after, load_delvec(_tablet_mgr, after, rssid));
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
        ASSIGN_OR_RETURN(Roaring rows, load_delvec(_tablet_mgr, after, rssid)); // delvec_after
        auto upd = cdc.column_overlay_vecs().find(rssid);
        if (upd != cdc.column_overlay_vecs().end()) {
            ASSIGN_OR_RETURN(Roaring updated, load_delvec_page(_tablet_mgr, after, upd->second));
            rows |= updated;
        }
        auto base = cdc.compaction_output_delvecs().find(rssid);
        if (base != cdc.compaction_output_delvecs().end()) {
            ASSIGN_OR_RETURN(Roaring baseline, load_delvec_page(_tablet_mgr, after, base->second));
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
                auto it = cdc.compaction_input_delvecs().find(rssid);
                if (it != cdc.compaction_input_delvecs().end()) {
                    ASSIGN_OR_RETURN(after_delvec, load_delvec_page(_tablet_mgr, after, it->second));
                }
            } else {
                ASSIGN_OR_RETURN(after_delvec, load_delvec(_tablet_mgr, after, rssid));
            }
            ASSIGN_OR_RETURN(Roaring before_delvec, load_delvec(_tablet_mgr, before, rssid));
            rows |= (after_delvec - before_delvec);
        }
        if (c.column_overlaid) {
            auto upd = cdc.column_overlay_vecs().find(rssid);
            if (upd != cdc.column_overlay_vecs().end()) {
                ASSIGN_OR_RETURN(Roaring updated, load_delvec_page(_tablet_mgr, after, upd->second));
                ASSIGN_OR_RETURN(Roaring delvec_after, load_delvec(_tablet_mgr, after, rssid));
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

DataSourceProviderPtr ChangesConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                    const TPlanNode& plan_node) const {
    return std::make_unique<ChangesDataSourceProvider>(scan_node, plan_node);
}

// --- ChangesDataSourceProvider ---

ChangesDataSourceProvider::ChangesDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _changes_scan_node(plan_node.changes_scan_node) {}

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
    _base_version = range.base_version;
    _head_version = range.head_version;
}

Status ChangesDataSource::open(RuntimeState* state) {
    _runtime_state = state;

    const auto& scan_node = _provider->_changes_scan_node;
    const auto* tuple_desc = state->desc_tbl().get_tuple_descriptor(scan_node.tuple_id);
    if (tuple_desc == nullptr) {
        return Status::InternalError("tuple descriptor not found");
    }

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
    ASSIGN_OR_RETURN(_head_metadata, tablet_mgr->get_tablet_metadata(_tablet_id, _head_version));
    RETURN_IF_ERROR(_init_read_schema());
    _is_primary_keys = _tablet_schema->keys_type() == KeysType::PRIMARY_KEYS;
    if (_base_version > _head_version) {
        return Status::InvalidArgument(fmt::format("CHANGES version range invalid: base_version({}) > head_version({})",
                                                   _base_version, _head_version));
    }
    _planner.emplace(tablet_mgr, _is_primary_keys);
    _walk_meta = _head_metadata;
    _walk_version = _head_version;
    return Status::OK();
}

void ChangesDataSource::close(RuntimeState* state) {
    if (_active_iterator != nullptr) _active_iterator->close();
    _active_iterator.reset();
    _segment_read_index = 0;
    _draining_delete_changes = false;
    _current_plan.reset();
    _walk_meta.reset();
    _planner.reset();
    _head_metadata.reset();
    _tablet_schema.reset();
}

Status ChangesDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    return _read_next_chunk(chunk);
}

Status ChangesDataSource::_init_read_schema() {
    DCHECK(_head_metadata != nullptr);

    auto* tablet_mgr = lake_tablet_manager();
    const auto& t_schema_key = _provider->_changes_scan_node.schema_key;
    TableSchemaKeyPB schema_key_pb;
    schema_key_pb.set_db_id(t_schema_key.db_id);
    schema_key_pb.set_table_id(t_schema_key.table_id);
    schema_key_pb.set_schema_id(t_schema_key.schema_id);
    ASSIGN_OR_RETURN(_tablet_schema, tablet_mgr->table_schema_service()->get_schema_for_scan(
                                             schema_key_pb, _tablet_id, _runtime_state->query_id(),
                                             _runtime_state->fragment_ctx()->fe_addr(), _head_metadata));
    // Virtual columns (e.g. _tablet_id_) aren't part of the storage schema; the
    // analyzer attaches them to every OlapTable relation, so add them to the
    // schema we look slots up in. Matches LakeDataSource::build_tablet_reader.
    ASSIGN_OR_RETURN(_tablet_schema, extend_schema_by_virtual_columns(_tablet_schema, _data_slots));

    // Tablet column index resolved per slot; we sort the index list before
    // building _read_schema, so each slot's position in the sorted list is
    // exactly the column index it will occupy in surfaced chunks.
    std::vector<std::pair<SlotId, uint32_t>> slot_tablet_indices;
    slot_tablet_indices.reserve(_data_slots.size());
    std::vector<uint32_t> column_indices;
    column_indices.reserve(_data_slots.size());
    for (auto* slot : _data_slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        column_indices.push_back(static_cast<uint32_t>(index));
        slot_tablet_indices.emplace_back(slot->id(), static_cast<uint32_t>(index));
    }
    // Metadata-only projection (e.g. SELECT __ROW_VERSION__) yields zero data
    // slots, so the segment iterator would be opened over an empty schema and
    // every chunk would surface with num_rows() == 0. Force-include the first
    // tablet column so the iterator drives row count from real segment data;
    // _data_slot_chunk_indices stays empty, so this anonymous column is not
    // resolvable via slot id and never reaches the tuple.
    if (column_indices.empty()) {
        if (_tablet_schema->num_columns() == 0) {
            return Status::InternalError("tablet schema has no columns");
        }
        column_indices.push_back(0);
    }
    std::sort(column_indices.begin(), column_indices.end());
    _read_schema = ChunkHelper::convert_schema(_tablet_schema, column_indices);

    _data_slot_chunk_indices.clear();
    _data_slot_chunk_indices.reserve(slot_tablet_indices.size());
    for (const auto& [slot_id, tablet_idx] : slot_tablet_indices) {
        auto it = std::lower_bound(column_indices.begin(), column_indices.end(), tablet_idx);
        DCHECK(it != column_indices.end() && *it == tablet_idx);
        _data_slot_chunk_indices.emplace_back(slot_id, static_cast<size_t>(it - column_indices.begin()));
    }
    return Status::OK();
}

Status ChangesDataSource::_read_next_chunk(ChunkPtr* chunk) {
    while (true) {
        // Advance the lazy walk until an open iterator is ready. Per publish, drain
        // insert_changes (-> INSERT) first, then delete_changes (-> DELETE), then step
        // to the next publish. The next publish's plan is built lazily, only on reach.
        while (_active_iterator == nullptr) {
            if (!_current_plan.has_value()) {
                ASSIGN_OR_RETURN(bool has, _advance_to_next_publish());
                if (!has) return Status::EndOfFile("end of changes data");
                continue;
            }
            const auto& changes =
                    _draining_delete_changes ? _current_plan->delete_changes : _current_plan->insert_changes;
            const int8_t change_type = _draining_delete_changes ? 1 : 0;
            if (_segment_read_index < changes.size()) {
                ASSIGN_OR_RETURN(_active_iterator,
                                 _build_segment_iterator(*_current_plan, changes[_segment_read_index], change_type));
                ++_segment_read_index; // a null result (empty segment) re-loops and steps to the next read
            } else if (!_draining_delete_changes) {
                _draining_delete_changes = true;
                _segment_read_index = 0;
            } else {
                ASSIGN_OR_RETURN(bool has, _advance_to_next_publish());
                if (!has) return Status::EndOfFile("end of changes data");
            }
        }
        auto data_chunk = ChunkFactory::new_chunk(_read_schema, _runtime_state->chunk_size());
        Status st = _active_iterator->get_next(data_chunk.get());
        if (st.is_end_of_file()) {
            _active_iterator->close();
            _active_iterator.reset();
            continue;
        }
        RETURN_IF_ERROR(st);
        if (data_chunk->num_rows() == 0) continue;
        // Post-read fallback: evaluate the full _conjunct_ctxs list as a correctness
        // backstop. Must run after every column (data + metadata) is populated.
        if (!_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, data_chunk.get()));
            if (data_chunk->num_rows() == 0) continue;
        }
        _rows_read += data_chunk->num_rows();
        _bytes_read += data_chunk->bytes_usage();
        *chunk = std::move(data_chunk);
        return Status::OK();
    }
}

StatusOr<bool> ChangesDataSource::_advance_to_next_publish() {
    if (_walk_version <= _base_version) return false;
    RETURN_IF_ERROR(_check_degradation(_walk_meta));
    int64_t parent_version = -1;
    if (_walk_meta->metadata_ancestors_size() > 0) {
        const int64_t direct_parent = _walk_meta->metadata_ancestors(0);
        if (direct_parent >= _base_version && direct_parent < _walk_version) {
            parent_version = direct_parent;
        }
    }
    if (parent_version < 0) {
        return Status::NotSupported(
                fmt::format("CHANGES ancestor chain on tablet {} cannot reach base version {} from version {}",
                            _tablet_id, _base_version, _walk_version));
    }
    auto* tablet_mgr = lake_tablet_manager();
    ASSIGN_OR_RETURN(auto parent_meta, tablet_mgr->get_tablet_metadata(_tablet_id, parent_version));
    ASSIGN_OR_RETURN(VersionChangeReadPlan plan, _planner->plan(parent_meta, _walk_meta));
    _current_plan = std::move(plan);
    _draining_delete_changes = false;
    _segment_read_index = 0;
    _active_iterator.reset();
    _walk_meta = parent_meta;
    _walk_version = parent_version;
    return true;
}

Status ChangesDataSource::_check_degradation(const TabletMetadataPtr& meta) {
    if (meta->has_cdc_metadata() && meta->cdc_metadata().has_capture_status()) {
        Status status(meta->cdc_metadata().capture_status());
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

StatusOr<ChunkIteratorPtr> ChangesDataSource::_build_segment_iterator(const VersionChangeReadPlan& plan,
                                                                      const SegmentChangeReadPlan& seg,
                                                                      int8_t change_type) {
    auto* tablet_mgr = lake_tablet_manager();
    const TabletMetadataPtr& read_meta = seg.from_before_meta ? plan.before_meta : plan.after_meta;
    const int64_t tag_version = plan.after_meta->version();
    OlapReaderStatistics* stats = (change_type == 0) ? &_insert_read_stats : &_delete_read_stats;
    // Single-segment range so only this segment's footer loads.
    auto rowset =
            std::make_shared<lake::Rowset>(tablet_mgr, read_meta, seg.rowset_pos, seg.segment_pos, seg.segment_pos + 1);

    std::vector<ChunkIteratorPtr> iters;
    if (_is_primary_keys) {
        // PK: read exactly seg.rowids with no delete vector (the rows were already selected and must
        // not be re-filtered). Apply the dcg overlay only for a column-update read; a raw read
        // (read_with_dcg == false) returns the stored bytes as-is. Every PK read carries rowids.
        auto range = std::make_shared<SparseRange<>>(roaring2range(*seg.rowids));
        std::vector<SparseRangePtr> ranges{std::move(range)};
        ASSIGN_OR_RETURN(iters, rowset->get_each_segment_iterator_no_delvec(_read_schema, read_meta->version(), stats,
                                                                            /*apply_dcg=*/seg.read_with_dcg, &ranges));
    } else {
        // DUP/AGG: a plain whole-segment read; these tables have no delete vector or dcg overlay.
        RowsetReadOptions opts;
        opts.stats = stats;
        opts.chunk_size = _runtime_state->chunk_size();
        opts.tablet_schema = _tablet_schema;
        opts.use_page_cache = false;
        opts.is_primary_keys = false;
        ASSIGN_OR_RETURN(iters, rowset->read(_read_schema, opts));
    }

    if (iters.empty()) return ChunkIteratorPtr{nullptr};
    if (iters.size() > 1) {
        return Status::InternalError(fmt::format("CHANGES single-segment read returned {} iterators on tablet {}",
                                                 iters.size(), _tablet_id));
    }
    return std::make_shared<ChangesMetaColumnIterator>(std::move(iters[0]), tag_version, _data_slot_chunk_indices,
                                                       _changes_meta_slots, change_type);
}

} // namespace starrocks::connector
