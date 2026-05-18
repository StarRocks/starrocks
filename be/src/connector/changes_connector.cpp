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

#include "exec/olap_scan_prepare.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/literal.h"
#include "storage/predicate_parser.h"

#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/fragment_context.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "common/config.h"
#include "storage/tablet_schema_map.h"

namespace starrocks::connector {

std::string format_ancestor_chain_exhausted_error(int64_t tablet_id) {
    return fmt::format(
            "CHANGES_NOT_FOUND: ancestor chain insufficient for tablet {}; "
            "consider raising cloud_native_tablet_metadata_ancestors_recorded",
            tablet_id);
}

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

    // Split data-column and metadata-column predicates for storage-layer pushdown.
    // Post-read behavior is unchanged: _conjunct_ctxs is still fully evaluated.
    _classify_predicates();

    // Profile counters — must be initialized before _init_predicate_tree, which uses COUNTER_SET internally.
    _expr_filter_timer = ADD_TIMER(_runtime_profile, "ExprFilterTime");
    _expr_filter_counter = ADD_COUNTER(_runtime_profile, "ExprFilterRows", TUnit::UNIT);
    _pred_pushdown_counter = ADD_COUNTER(_runtime_profile, "PredPushdownToStorage", TUnit::UNIT);
    _rowset_skipped_counter = ADD_COUNTER(_runtime_profile, "RowsetSkippedByVersionFilter", TUnit::UNIT);

    // Read head metadata up front — _tablet_schema and _key_column_names are initialized here.
    RETURN_IF_ERROR(_read_head_metadata());

    // Initialize the predicate tree (requires _tablet_schema, ready after _read_head_metadata).
    RETURN_IF_ERROR(_init_predicate_tree());

    // Phase 1: metadata traversal
    RETURN_IF_ERROR(_do_metadata_traversal());

    // Sort and RowVersionFilter
    std::sort(_changes_rowsets.begin(), _changes_rowsets.end(),
              [](const ChangesRowset& a, const ChangesRowset& b) {
                  return a.version < b.version;
              });

    if (!_row_version_filters.empty()) {
        size_t before = _changes_rowsets.size();
        _changes_rowsets.erase(
                std::remove_if(_changes_rowsets.begin(), _changes_rowsets.end(),
                               [this](const ChangesRowset& rs) {
                                   return std::any_of(
                                           _row_version_filters.begin(), _row_version_filters.end(),
                                           [&](const RowVersionFilter& f) { return !f.evaluate(rs.version); });
                               }),
                _changes_rowsets.end());
        COUNTER_UPDATE(_rowset_skipped_counter, before - _changes_rowsets.size());
    }

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
    // Serial read path (sort/filter already done in open())
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
    auto head_meta = _head_metadata;
    _stats.metadata_count++;

    // Collect head version stats
    for (const auto& rs : head_meta->rowsets()) {
        _stats.head_version_rows += rs.num_rows();
        _stats.head_version_data_size += rs.data_size();
        _stats.head_version_segment_count += rs.segments_size();
    }

    // Reverse traverse from V_head
    auto current_meta = head_meta;
    int64_t current_version = _head_version;

    while (current_version > _base_version) {
        _scan_metadata_for_changes_rowsets(*current_meta);

        if (static_cast<int64_t>(_found_cv.size()) >= _head_version - _base_version) {
            break;
        }

        if (current_meta->metadata_ancestors_size() == 0) {
            return Status::InternalError(format_ancestor_chain_exhausted_error(_tablet_id));
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

        for (int64_t v : versions_to_read) {
            ASSIGN_OR_RETURN(auto meta, tablet_mgr->get_tablet_metadata(_tablet_id, v));
            _stats.metadata_count++;
            _scan_metadata_for_changes_rowsets(*meta);

            current_meta = meta;
            current_version = v;

            if (static_cast<int64_t>(_found_cv.size()) >= _head_version - _base_version) {
                break;
            }
        }

        if (versions_to_read.back() <= _base_version) {
            break;
        }
    }

    return Status::OK();
}

void ChangesDataSource::_scan_metadata_for_changes_rowsets(const TabletMetadataPB& meta) {
    auto process_rowset = [&](const RowsetMetadataPB& r) {
        if (!r.has_version() || r.version() <= _base_version) {
            return;
        }
        if (_found_ids.count(r.id()) > 0) {
            return;
        }
        _found_ids.insert(r.id());
        _found_cv.insert(r.version());

        if (r.has_delete_predicate()) {
            _stats.has_delete_predicate = true;
            return;
        }

        if (r.has_max_compact_input_rowset_id()) {
            _stats.compaction_count++;
            return;
        }

        ChangesRowset changes_rs;
        changes_rs.id = r.id();
        changes_rs.version = r.version();

        changes_rs.num_rows = r.num_rows();
        changes_rs.data_size = r.data_size();
        for (const auto& seg : r.segments()) {
            changes_rs.segments.push_back(seg);
        }
        // Collect per-segment row counts from segment_metas (optional field)
        if (r.segment_metas_size() == static_cast<int>(r.segments_size())) {
            for (int i = 0; i < r.segment_metas_size(); ++i) {
                if (r.segment_metas(i).has_num_rows()) {
                    changes_rs.segment_num_rows.push_back(r.segment_metas(i).num_rows());
                } else {
                    // SegmentMetadataPB entry exists but lacks num_rows — clear and use fallback
                    changes_rs.segment_num_rows.clear();
                    break;
                }
            }
        }
        // If segment_num_rows is empty after this, get_segment_num_rows() uses proportional fallback
        _changes_rowsets.push_back(std::move(changes_rs));

        _stats.insertion_rows += r.num_rows();
        _stats.insertion_data_size += r.data_size();
        _stats.insertion_segment_count += r.segments_size();
        _stats.insertion_segment_total_rows += r.num_rows();
        _stats.load_count++;
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
        return Status::InternalError(format_ancestor_chain_exhausted_error(_tablet_id));
    }
    auto segment = std::move(segment_or).value();

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    seg_options.stats = &_seg_stats;
    seg_options.chunk_size = _runtime_state->chunk_size();

    // Storage-layer predicate pushdown.
    seg_options.pred_tree = _cached_pred_tree;
    seg_options.runtime_range_pruner = _runtime_range_pruner;

    // Runtime filter predicates: re-fetched per segment to capture late-arriving bloom filters.
    if (_conjuncts_manager && _runtime_state->enable_join_runtime_filter_pushdown()) {
        auto rf_or = _conjuncts_manager->get_runtime_filter_predicates(&_obj_pool, _parser);
        if (rf_or.ok()) {
            seg_options.runtime_filter_preds = std::move(rf_or).value();
        }
    }

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
    if (_stats.has_delete_predicate) {
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

        // Post-read fallback: evaluate the full _conjunct_ctxs list as a correctness backstop —
        // every predicate is evaluated here even if storage-layer pushdown was not enabled.
        // Ordering constraint: this must run after every column (data + metadata) is populated.
        if (!_conjunct_ctxs.empty()) {
            SCOPED_TIMER(_expr_filter_timer);
            size_t before = data_chunk->num_rows();
            RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, data_chunk.get()));
            COUNTER_UPDATE(_expr_filter_counter, before - data_chunk->num_rows());
            if (data_chunk->num_rows() == 0) {
                // All rows filtered out — read the next chunk.
                _cpu_time_ns += _seg_stats.block_fetch_ns - _prev_block_fetch_ns;
                _prev_block_fetch_ns = _seg_stats.block_fetch_ns;
                continue;
            }
        }

        // Stat updates (post-filter).
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

void ChangesDataSource::_try_extract_row_version_predicate(ExprContext* ctx) {
    if (!_row_version_slot_id.has_value()) return;

    Expr* root = ctx->root();
    if (root->get_num_children() != 2) return;

    Expr* left = root->get_child(0);
    Expr* right = root->get_child(1);
    TExprOpcode::type op = root->op();

    Expr* slot_expr = nullptr;
    Expr* const_expr = nullptr;

    if (left->is_slotref() && right->is_literal()) {
        slot_expr = left;
        const_expr = right;
    } else if (left->is_literal() && right->is_slotref()) {
        slot_expr = right;
        const_expr = left;
        // const <op> slot → flip the comparison operator.
        switch (op) {
        case TExprOpcode::LT: op = TExprOpcode::GT; break;
        case TExprOpcode::LE: op = TExprOpcode::GE; break;
        case TExprOpcode::GT: op = TExprOpcode::LT; break;
        case TExprOpcode::GE: op = TExprOpcode::LE; break;
        default: break; // EQ / NE are symmetric.
        }
    } else {
        return;
    }

    auto* col_ref = static_cast<ColumnRef*>(slot_expr);
    if (col_ref->slot_id() != _row_version_slot_id.value()) return;

    // Extract the constant value.
    auto* literal = static_cast<VectorizedLiteral*>(const_expr);
    ColumnPtr col = literal->value();
    if (col == nullptr || col->size() == 0) return;
    int64_t const_value = col->get(0).get_int64();

    _row_version_filters.emplace_back(op, const_value);
}

Status ChangesDataSource::_read_head_metadata() {
    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    ASSIGN_OR_RETURN(_head_metadata, tablet_mgr->get_tablet_metadata(_tablet_id, _head_version));
    _tablet_schema = GlobalTabletSchemaMap::Instance()->emplace(_head_metadata->schema()).first;

    // Capture key column names — ScanConjunctsManager needs them when building the pred_tree.
    for (auto idx : _tablet_schema->sort_key_idxes()) {
        _key_column_names.emplace_back(_tablet_schema->column(idx).name());
    }
    return Status::OK();
}

Status ChangesDataSource::_init_predicate_tree() {
    if (_data_column_conjuncts.empty()) {
        return Status::OK();
    }

    ScanConjunctsManagerOptions opts;
    opts.conjunct_ctxs_ptr = &_data_column_conjuncts;
    opts.tuple_desc = _tuple_desc;
    opts.obj_pool = &_obj_pool;
    opts.key_column_names = &_key_column_names;
    opts.runtime_filters = _runtime_filters;
    opts.runtime_state = _runtime_state;
    if (_runtime_state->fragment_ctx() != nullptr) {
        opts.pred_tree_params = _runtime_state->fragment_ctx()->pred_tree_params();
    }

    _conjuncts_manager = std::make_unique<ScanConjunctsManager>(opts);
    RETURN_IF_ERROR(_conjuncts_manager->parse_conjuncts());

    _parser = _obj_pool.add(new OlapPredicateParser(_tablet_schema));

    // Fetch the full predicate tree, then partition by storage-layer pushdown capability.
    ASSIGN_OR_RETURN(auto full_pred_tree, _conjuncts_manager->get_predicate_tree(_parser, _col_preds_owner));
    PredicateAndNode pushdown_root;
    PredicateAndNode non_pushdown_root;
    full_pred_tree.root().partition_copy(
        [this](const auto& node) { return _parser->can_pushdown(node); },
        &pushdown_root, &non_pushdown_root);
    _cached_pred_tree = PredicateTree::create(std::move(pushdown_root));
    // non_pushdown_root is unused in Phase 1 — the post-read _conjunct_ctxs fallback covers it.

    _runtime_range_pruner = RuntimeScanRangePruner(_parser, _conjuncts_manager->unarrived_runtime_filters());

    COUNTER_SET(_pred_pushdown_counter, static_cast<int64_t>(_data_column_conjuncts.size()));
    return Status::OK();
}

void ChangesDataSource::_classify_predicates() {
    for (auto* ctx : _conjunct_ctxs) {
        std::vector<SlotId> slot_ids;
        ctx->root()->get_slot_ids(&slot_ids);

        bool is_data_column_only = true;
        for (auto slot_id : slot_ids) {
            if ((_change_type_slot_id.has_value() && slot_id == _change_type_slot_id.value()) ||
                (_row_version_slot_id.has_value() && slot_id == _row_version_slot_id.value())) {
                is_data_column_only = false;
                break;
            }
        }

        if (is_data_column_only) {
            _data_column_conjuncts.push_back(ctx);
        }

        // Try to extract a simple __ROW_VERSION__ predicate for rowset skipping.
        if (_row_version_slot_id.has_value()) {
            for (auto slot_id : slot_ids) {
                if (slot_id == _row_version_slot_id.value()) {
                    _try_extract_row_version_predicate(ctx);
                    break;
                }
            }
        }
    }
}

} // namespace starrocks::connector
