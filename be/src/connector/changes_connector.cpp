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

#include <algorithm>
#include <fmt/format.h>

#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"

#include "column/nullable_column.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/empty_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/rowset_options.h"
#include "storage/union_iterator.h"

namespace starrocks::connector {

namespace {

// Wraps a per-segment ChunkIterator and stamps the two CDC metadata columns
// (__CHANGE_TYPE__, __ROW_VERSION__) onto each chunk before it surfaces to
// the union iterator above. Each instance carries its source rowset's
// version, so a union over multiple wrappers can fan all segments into a
// single chunk stream without losing per-rowset version information.
class CdcStampingIterator final : public ChunkIterator {
public:
    CdcStampingIterator(ChunkIteratorPtr inner, int64_t version,
                        const std::vector<SlotDescriptor*>& data_slots,
                        std::optional<int> change_type_slot_id, bool change_type_nullable,
                        std::optional<int> row_version_slot_id, bool row_version_nullable)
            : ChunkIterator(inner->schema(), inner->chunk_size()),
              _inner(std::move(inner)),
              _version(version),
              _data_slots(&data_slots),
              _change_type_slot_id(change_type_slot_id),
              _change_type_nullable(change_type_nullable),
              _row_version_slot_id(row_version_slot_id),
              _row_version_nullable(row_version_nullable) {}

    void close() override {
        if (_inner != nullptr) {
            _inner->close();
            _inner.reset();
        }
    }

protected:
    Status do_get_next(Chunk* chunk) override {
        RETURN_IF_ERROR(_inner->get_next(chunk));
        // Map data-slot ids onto the chunk so post-read predicate evaluation
        // can resolve SlotRef references by id.
        for (auto* slot : *_data_slots) {
            size_t col_idx = chunk->schema()->get_field_index_by_name(slot->col_name());
            if (col_idx != -1UL) {
                chunk->set_slot_id_to_index(slot->id(), col_idx);
            }
        }
        size_t nrows = chunk->num_rows();
        // spec §2 enum: 0 = INSERT.
        if (_change_type_slot_id.has_value()) {
            auto val_col = Int8Column::create();
            val_col->reserve(nrows);
            for (size_t r = 0; r < nrows; r++) {
                val_col->append(0);
            }
            ColumnPtr col = std::move(val_col);
            if (_change_type_nullable) {
                auto null_col = NullColumn::create(nrows, 0);
                col = NullableColumn::create(std::move(col), std::move(null_col));
            }
            chunk->append_column(std::move(col), _change_type_slot_id.value());
        }
        if (_row_version_slot_id.has_value()) {
            auto val_col = Int64Column::create();
            val_col->reserve(nrows);
            for (size_t r = 0; r < nrows; r++) {
                val_col->append(_version);
            }
            ColumnPtr col = std::move(val_col);
            if (_row_version_nullable) {
                auto null_col = NullColumn::create(nrows, 0);
                col = NullableColumn::create(std::move(col), std::move(null_col));
            }
            chunk->append_column(std::move(col), _row_version_slot_id.value());
        }
        return Status::OK();
    }

private:
    ChunkIteratorPtr _inner;
    int64_t _version;
    const std::vector<SlotDescriptor*>* _data_slots;
    std::optional<int> _change_type_slot_id;
    bool _change_type_nullable;
    std::optional<int> _row_version_slot_id;
    bool _row_version_nullable;
};

} // namespace

// --- ChangesConnector ---

DataSourceProviderPtr ChangesConnector::create_data_source_provider(
        ConnectorScanNode* scan_node, const TPlanNode& plan_node) const {
    return std::make_unique<ChangesDataSourceProvider>(scan_node, plan_node);
}

// --- ChangesDataSourceProvider ---

ChangesDataSourceProvider::ChangesDataSourceProvider(
        ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node),
          _changes_scan_node(plan_node.changes_scan_node) {}

DataSourcePtr ChangesDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<ChangesDataSource>(this, scan_range);
}

const TupleDescriptor* ChangesDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    return state->desc_tbl().get_tuple_descriptor(_changes_scan_node.tuple_id);
}

// --- ChangesDataSource ---

ChangesDataSource::ChangesDataSource(const ChangesDataSourceProvider* provider,
                                     const TScanRange& scan_range)
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
    _tuple_desc = tuple_desc;

    // Identify metadata column slots by name and separate data slots
    for (auto* slot : tuple_desc->slots()) {
        if (slot->col_name() == "__CHANGE_TYPE__") {
            _change_type_slot_id = slot->id();
            _change_type_slot_is_nullable = slot->is_nullable();
        } else if (slot->col_name() == "__ROW_VERSION__") {
            _row_version_slot_id = slot->id();
            _row_version_slot_is_nullable = slot->is_nullable();
        } else {
            _data_slots.push_back(slot);
        }
    }

    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    if (tablet_mgr == nullptr) {
        return Status::InternalError("lake tablet manager not available");
    }
    ASSIGN_OR_RETURN(_head_metadata, tablet_mgr->get_tablet_metadata(_tablet_id, _head_version));
    RETURN_IF_ERROR(_init_read_schema());
    RETURN_IF_ERROR(_do_metadata_traversal());

    if (_has_delete_predicate) {
        return Status::NotSupported(fmt::format(
                "DELETE_PREDICATE_FOUND: CDC not supported for DELETE operations on tablet {}",
                _tablet_id));
    }

    // Build the unified chunk stream: per-rowset segment iterators get wrapped
    // in CdcStampingIterator so each chunk carries its source rowset's version
    // and CDC metadata columns; the union iterator then drains them in order.
    RowsetReadOptions opts;
    opts.stats = &_read_stats;
    opts.chunk_size = _runtime_state->chunk_size();
    opts.tablet_schema = _tablet_schema;
    opts.use_page_cache = false;
    // CDC MVP: no predicate / runtime-filter / delvec / range pushdown.

    std::vector<ChunkIteratorPtr> seg_iters;
    for (auto& rowset : _changes_rowsets) {
        ASSIGN_OR_RETURN(auto iters, rowset->read(_read_schema, opts));
        for (auto& it : iters) {
            seg_iters.push_back(std::make_shared<CdcStampingIterator>(
                    std::move(it), rowset->version(), _data_slots,
                    _change_type_slot_id, _change_type_slot_is_nullable,
                    _row_version_slot_id, _row_version_slot_is_nullable));
        }
    }
    _chunk_iter = seg_iters.empty()
                          ? new_empty_iterator(_read_schema, _runtime_state->chunk_size())
                          : new_union_iterator(std::move(seg_iters));

    return Status::OK();
}

void ChangesDataSource::close(RuntimeState* state) {
    if (_chunk_iter != nullptr) {
        _chunk_iter->close();
        _chunk_iter.reset();
    }
    _head_metadata.reset();
    _tablet_schema.reset();
    _changes_rowsets.clear();
}

Status ChangesDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    return _read_next_chunk(chunk);
}

// Phase 1: Delta Replay metadata traversal algorithm
Status ChangesDataSource::_do_metadata_traversal() {
    if (_base_version > _head_version) {
        return Status::InvalidArgument(
                fmt::format("CDC version range invalid: base_version({}) > head_version({})",
                            _base_version, _head_version));
    }
    if (_base_version == _head_version) {
        return Status::OK();
    }

    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    DCHECK(tablet_mgr != nullptr);
    DCHECK(_head_metadata != nullptr);
    // seen_rowset_ids dedupes a rowset that reappears across ancestor metadata snapshots.
    std::unordered_set<uint32_t> seen_rowset_ids;

    auto head_meta = _head_metadata;
    // Reverse traverse from V_head
    auto current_meta = head_meta;
    int64_t current_version = _head_version;

    while (current_version > _base_version) {
        _scan_metadata_for_changes_rowsets(current_meta, seen_rowset_ids);

        if (current_meta->metadata_ancestors_size() == 0) {
            break;
        }

        std::vector<int64_t> versions_to_read;
        for (int i = 0; i < current_meta->metadata_ancestors_size(); i++) {
            int64_t v = current_meta->metadata_ancestors(i);
            if (v > _base_version) {
                versions_to_read.push_back(v);
            }
        }

        if (versions_to_read.empty()) {
            break;
        }

        // TODO: fetch the ancestor metadata in parallel — when it is not in
        // cache each get_tablet_metadata() costs one object-store round-trip,
        // so the current serial loop is bound by N * RTT for an N-deep
        // ancestor chain.
        for (int64_t v : versions_to_read) {
            ASSIGN_OR_RETURN(auto meta, tablet_mgr->get_tablet_metadata(_tablet_id, v));
            _scan_metadata_for_changes_rowsets(meta, seen_rowset_ids);

            current_meta = meta;
            current_version = v;
        }
    }

    return Status::OK();
}

void ChangesDataSource::_scan_metadata_for_changes_rowsets(const TabletMetadataPtr& meta,
                                                            std::unordered_set<uint32_t>& seen_rowset_ids) {
    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();

    for (int rowset_index = 0; rowset_index < meta->rowsets_size(); ++rowset_index) {
        const auto& r = meta->rowsets(rowset_index);
        if (!r.has_version() || r.version() <= _base_version) {
            continue;
        }
        if (seen_rowset_ids.count(r.id()) > 0) {
            continue;
        }
        seen_rowset_ids.insert(r.id());

        if (r.has_delete_predicate()) {
            _has_delete_predicate = true;
            continue;
        }
        if (r.has_max_compact_input_rowset_id()) {
            continue;
        }

        // Use lake::Rowset's standard ctor (the one Rowset::get_rowsets uses).
        // It keeps |meta| alive internally and derives the rowset's write
        // schema from meta->rowset_to_schema() / historical_schemas / fallback
        // meta->schema(). compaction_segment_limit=0 means "all segments".
        _changes_rowsets.push_back(std::make_shared<lake::Rowset>(
                tablet_mgr, meta, rowset_index, /*compaction_segment_limit=*/0));
    }
}


Status ChangesDataSource::_read_next_chunk(ChunkPtr* chunk) {
    DCHECK(_chunk_iter != nullptr);
    while (true) {
        auto data_chunk = ChunkHelper::new_chunk(_read_schema, _runtime_state->chunk_size());
        Status st = _chunk_iter->get_next(data_chunk.get());
        if (st.is_end_of_file()) {
            return Status::EndOfFile("end of changes data");
        }
        RETURN_IF_ERROR(st);
        if (data_chunk->num_rows() == 0) {
            continue;
        }
        // Post-read fallback: evaluate the full _conjunct_ctxs list as a correctness
        // backstop. Must run after every column (data + metadata) is populated.
        if (!_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, data_chunk.get()));
            if (data_chunk->num_rows() == 0) {
                continue;
            }
        }
        _rows_read += data_chunk->num_rows();
        _bytes_read += data_chunk->bytes_usage();
        *chunk = std::move(data_chunk);
        return Status::OK();
    }
}

// Resolve the read schema for Phase 2 + pre-compute the projected Schema reused
// across every rowset. Two layers:
// 1. `_tablet_schema` — the live read schema, fetched via TableSchemaService
//    keyed by the FE-supplied schema_key (mirrors LakeDataSource::get_tablet,
//    lake_connector.cpp:233-247). Head-metadata's embedded schema would lag FE
//    when a commit creates a new schema version before the matching tablet
//    metadata is published.
// 2. `_read_schema` — the projection over `_tablet_schema` selecting only
//    the slots the caller asked for (`_data_slots`).
Status ChangesDataSource::_init_read_schema() {
    DCHECK(_head_metadata != nullptr);

    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    const auto& t_schema_key = _provider->_changes_scan_node.schema_key;
    TableSchemaKeyPB schema_key_pb;
    schema_key_pb.set_db_id(t_schema_key.db_id);
    schema_key_pb.set_table_id(t_schema_key.table_id);
    schema_key_pb.set_schema_id(t_schema_key.schema_id);
    ASSIGN_OR_RETURN(_tablet_schema,
                     tablet_mgr->table_schema_service()->get_schema_for_scan(
                             schema_key_pb, _tablet_id, _runtime_state->query_id(),
                             _runtime_state->fragment_ctx()->fe_addr(), _head_metadata));

    std::vector<uint32_t> column_indices;
    for (auto* slot : _data_slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index >= 0) {
            column_indices.push_back(static_cast<uint32_t>(index));
        }
    }
    std::sort(column_indices.begin(), column_indices.end());
    _read_schema = ChunkHelper::convert_schema(_tablet_schema, column_indices);
    return Status::OK();
}

} // namespace starrocks::connector
