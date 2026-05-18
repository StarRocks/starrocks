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
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema_map.h"

namespace starrocks::connector {

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

    RETURN_IF_ERROR(_read_head_metadata());

    // Phase 1: metadata traversal
    RETURN_IF_ERROR(_do_metadata_traversal());

    return Status::OK();
}

void ChangesDataSource::close(RuntimeState* state) {
    if (_segment_iter) {
        _segment_iter->close();
        _segment_iter.reset();
    }
    _fs.reset();
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
    if (tablet_mgr == nullptr) {
        return Status::InternalError("lake tablet manager not available");
    }

    // _head_metadata may already have been loaded by open() → _read_head_metadata().
    if (_head_metadata == nullptr) {
        ASSIGN_OR_RETURN(_head_metadata, tablet_mgr->get_tablet_metadata(_tablet_id, _head_version));
    }
    // Phase 1 traversal state — local to this method.
    // seen_rowset_ids: rowset.id() set, dedupes the same rowset reappearing across ancestor metadata.
    // discovered_versions: distinct rowset versions in (V_base, V_head] — used as the early-stop count.
    std::unordered_set<uint32_t> seen_rowset_ids;
    std::unordered_set<int64_t> discovered_versions;

    auto head_meta = _head_metadata;
    // Reverse traverse from V_head
    auto current_meta = head_meta;
    int64_t current_version = _head_version;

    while (current_version > _base_version) {
        _scan_metadata_for_changes_rowsets(*current_meta, seen_rowset_ids, discovered_versions);

        // The size check is a one-way termination hint: equal-or-more means
        // (V_base, V_head] is fully covered. Less is inconclusive — versions
        // are not 1:1 with rowsets (batch publish shares one version across
        // rowsets, empty publish has no rowset at all). Fall through to the
        // ancestor walk and let it run out naturally.
        if (static_cast<int64_t>(discovered_versions.size()) >= _head_version - _base_version) {
            break;
        }

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
            _scan_metadata_for_changes_rowsets(*meta, seen_rowset_ids, discovered_versions);

            current_meta = meta;
            current_version = v;

            if (static_cast<int64_t>(discovered_versions.size()) >= _head_version - _base_version) {
                break;
            }
        }

        if (versions_to_read.back() <= _base_version) {
            break;
        }
    }

    return Status::OK();
}

void ChangesDataSource::_scan_metadata_for_changes_rowsets(const TabletMetadataPB& meta,
                                                            std::unordered_set<uint32_t>& seen_rowset_ids,
                                                            std::unordered_set<int64_t>& discovered_versions) {
    auto process_rowset = [&](const RowsetMetadataPB& r) {
        if (!r.has_version() || r.version() <= _base_version) {
            return;
        }
        if (seen_rowset_ids.count(r.id()) > 0) {
            return;
        }
        seen_rowset_ids.insert(r.id());
        discovered_versions.insert(r.version());

        if (r.has_delete_predicate()) {
            _has_delete_predicate = true;
            return;
        }

        if (r.has_max_compact_input_rowset_id()) {
            return;
        }

        ChangesRowset changes_rs;
        changes_rs.version = r.version();
        for (const auto& seg : r.segments()) {
            changes_rs.segments.push_back(seg);
        }
        _changes_rowsets.push_back(std::move(changes_rs));
    };

    for (const auto& r : meta.rowsets()) {
        process_rowset(r);
    }
    for (const auto& r : meta.compaction_inputs()) {
        process_rowset(r);
    }
}

Schema ChangesDataSource::_build_read_schema() {
    DCHECK(_head_metadata != nullptr);
    if (_tablet_schema == nullptr) {
        _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_head_metadata->schema()).first;
    }

    std::vector<uint32_t> column_indices;
    for (auto* slot : _data_slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index >= 0) {
            column_indices.push_back(static_cast<uint32_t>(index));
        }
    }

    std::sort(column_indices.begin(), column_indices.end());

    return ChunkHelper::convert_schema(_tablet_schema, column_indices);
}

Status ChangesDataSource::_open_next_segment() {
    DCHECK(_current_rowset_index < _changes_rowsets.size());
    const auto& changes_rs = _changes_rowsets[_current_rowset_index];
    DCHECK(_current_segment_index < changes_rs.segments.size());

    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    if (tablet_mgr == nullptr) {
        return Status::InternalError("lake tablet manager not available");
    }

    if (_fs == nullptr) {
        auto root_loc = tablet_mgr->tablet_root_location(_tablet_id);
        ASSIGN_OR_RETURN(_fs, FileSystemFactory::CreateSharedFromString(root_loc));
    }

    // Build read schema first (initializes _tablet_schema needed by load_segment)
    if (!_cached_read_schema.has_value()) {
        _cached_read_schema = _build_read_schema();
    }
    const Schema& schema = _cached_read_schema.value();

    const std::string& seg_name = changes_rs.segments[_current_segment_index];
    std::string segment_path = tablet_mgr->segment_location(_tablet_id, seg_name);
    FileInfo file_info{.path = segment_path, .fs = _fs};
    uint32_t segment_id = static_cast<uint32_t>(_current_segment_index);
    LakeIOOptions lake_io_opts;
    auto segment_or = tablet_mgr->load_segment(file_info, segment_id, lake_io_opts,
                                               /*fill_meta_cache=*/true, _tablet_schema);
    if (!segment_or.ok()) {
        return Status::InternalError(
                fmt::format("CHANGES failed to load segment '{}' for tablet {}: {}",
                            segment_path, _tablet_id, segment_or.status().to_string()));
    }
    auto segment = std::move(segment_or).value();

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    seg_options.stats = &_seg_stats;
    seg_options.chunk_size = _runtime_state->chunk_size();

    auto iter_or = segment->new_iterator(schema, seg_options);
    if (iter_or.status().is_end_of_file()) {
        _segment_iter = nullptr;
        return Status::OK();
    }
    RETURN_IF_ERROR(iter_or.status());
    _segment_iter = std::move(iter_or).value();

    return Status::OK();
}

Status ChangesDataSource::_read_next_chunk(ChunkPtr* chunk) {
    if (_has_delete_predicate) {
        return Status::NotSupported(fmt::format(
                "DELETE_PREDICATE_FOUND: CDC not supported for DELETE operations on tablet {}",
                _tablet_id));
    }

    while (_current_rowset_index < _changes_rowsets.size()) {
        const auto& changes_rs = _changes_rowsets[_current_rowset_index];

        if (_segment_iter == nullptr) {
            if (_current_segment_index >= changes_rs.segments.size()) {
                _current_rowset_index++;
                _current_segment_index = 0;
                continue;
            }
            RETURN_IF_ERROR(_open_next_segment());
            if (_segment_iter == nullptr) {
                _current_segment_index++;
                continue;
            }
        }

        if (!_cached_read_schema.has_value()) {
            _cached_read_schema = _build_read_schema();
        }
        const Schema& read_schema = _cached_read_schema.value();

        auto data_chunk = ChunkHelper::new_chunk(read_schema, _runtime_state->chunk_size());

        Status st = _segment_iter->get_next(data_chunk.get());
        if (st.is_end_of_file()) {
            _segment_iter->close();
            _segment_iter.reset();
            _current_segment_index++;
            continue;
        }
        RETURN_IF_ERROR(st);

        size_t nrows = data_chunk->num_rows();
        if (nrows == 0) {
            continue;
        }

        // Map SlotIds for data columns
        for (auto* slot : _data_slots) {
            size_t col_idx = data_chunk->schema()->get_field_index_by_name(slot->col_name());
            if (col_idx != -1UL) {
                data_chunk->set_slot_id_to_index(slot->id(), col_idx);
            }
        }

        _append_metadata_columns(data_chunk.get(), changes_rs.version);

        // Post-read fallback: evaluate the full _conjunct_ctxs list as a correctness backstop.
        // Ordering constraint: this must run after every column (data + metadata) is populated.
        if (!_conjunct_ctxs.empty()) {
            RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, data_chunk.get()));
            if (data_chunk->num_rows() == 0) {
                _cpu_time_ns += _seg_stats.block_fetch_ns - _prev_block_fetch_ns;
                _prev_block_fetch_ns = _seg_stats.block_fetch_ns;
                continue;
            }
        }

        _rows_read += data_chunk->num_rows();
        _bytes_read += data_chunk->bytes_usage();
        _cpu_time_ns += _seg_stats.block_fetch_ns - _prev_block_fetch_ns;
        _prev_block_fetch_ns = _seg_stats.block_fetch_ns;

        *chunk = std::move(data_chunk);
        return Status::OK();
    }

    return Status::EndOfFile("end of changes data");
}

void ChangesDataSource::_append_metadata_columns(Chunk* chunk, int64_t version) {
    size_t nrows = chunk->num_rows();

    // __CHANGE_TYPE__: INSERT for all rows
    if (_change_type_slot_id.has_value()) {
        auto val_col = Int8Column::create();
        val_col->reserve(nrows);
        for (size_t r = 0; r < nrows; r++) {
            val_col->append(kChangeTypeInsert);
        }
        ColumnPtr col = std::move(val_col);
        if (_change_type_slot_is_nullable) {
            auto null_col = NullColumn::create(nrows, 0);
            col = NullableColumn::create(std::move(col), std::move(null_col));
        }
        chunk->append_column(std::move(col), _change_type_slot_id.value());
    }

    // __ROW_VERSION__: rowset version for all rows
    if (_row_version_slot_id.has_value()) {
        auto val_col = Int64Column::create();
        val_col->reserve(nrows);
        for (size_t r = 0; r < nrows; r++) {
            val_col->append(version);
        }
        ColumnPtr col = std::move(val_col);
        if (_row_version_slot_is_nullable) {
            auto null_col = NullColumn::create(nrows, 0);
            col = NullableColumn::create(std::move(col), std::move(null_col));
        }
        chunk->append_column(std::move(col), _row_version_slot_id.value());
    }
}

Status ChangesDataSource::_read_head_metadata() {
    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    ASSIGN_OR_RETURN(_head_metadata, tablet_mgr->get_tablet_metadata(_tablet_id, _head_version));
    _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_head_metadata->schema()).first;
    return Status::OK();
}

} // namespace starrocks::connector
