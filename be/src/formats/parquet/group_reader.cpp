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

#include "formats/parquet/group_reader.h"

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>

#include "base/simd/simd.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_scan_io_fwd.h"
#include "common/statusor.h"
#include "common/system/master_info.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/iceberg_row_id_reader.h"
#include "formats/parquet/lazy_materialization_context.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_pos_reader.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/read_range_planner.h"
#include "formats/parquet/row_source_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/variant_projection.h"
#include "formats/reserved_columns.h"
#include "gen_cpp/Exprs_types.h"
#include "types/type_descriptor.h"
#include "utils.h"

namespace starrocks::parquet {

// ── GroupReader: construction / destruction ─────────────────────────────────

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row)
        : _row_group_first_row(row_group_first_row), _skip_rows_ctx(std::move(skip_rows_ctx)), _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
    _column_materializer = std::make_unique<ColumnMaterializer>(_param, &_column_readers);
    _variant = std::make_unique<VariantProjectionHandler>(this, _param, _row_group_metadata);
}

GroupReader::~GroupReader() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    if (_has_prepared) {
        if (_column_materializer->lazy_column_needed()) {
            _param.lazy_column_coalesce_counter->fetch_add(1, std::memory_order_relaxed);
        } else {
            _param.lazy_column_coalesce_counter->fetch_sub(1, std::memory_order_relaxed);
        }
        // Lazy-materialization diagnostic: count row groups where every lazy slot
        // was triggered during predicate evaluation.  A consistently high count
        // suggests the active/lazy classification (Phase 3b) would help.
        if (_param.stats) {
            size_t total_lazy = _column_materializer->lazy_slot_ids().size() + _variant->lazy_hidden_slot_ids().size();
            size_t triggered = _column_materializer->lazy_triggered_count();
            if (total_lazy > 0 && static_cast<size_t>(triggered) >= total_lazy) {
                _param.stats->parquet_lazy_full_trigger_count++;
            }
        }
        _param.stats->group_min_round_cost =
                _param.stats->group_min_round_cost == 0
                        ? _column_materializer->min_round_cost()
                        : std::min(_param.stats->group_min_round_cost, int64_t(_column_materializer->min_round_cost()));
    }
}

// ── init / prepare ──────────────────────────────────────────────────────────

Status GroupReader::init() {
    RETURN_IF_ERROR(_create_column_readers());
    _process_columns_and_conjunct_ctxs();
    _range = SparseRange<uint64_t>(_row_group_first_row, _row_group_first_row + _row_group_metadata->num_rows);
    return Status::OK();
}

Status GroupReader::prepare() {
    RETURN_IF_ERROR(_prepare_column_readers());

    if (_range.span_size() != get_row_group_metadata()->num_rows) {
        for (const auto& pair : _column_readers) {
            pair.second->select_offset_index(_range, _row_group_first_row);
        }
        _variant->select_hidden_source_offset_index();
    }

    // Promote variant virtual columns to typed-value proxy readers.
    _variant->try_promote();

    // Coalesce IO ranges using ReadRangePlanner's staged planning.
    if (config::parquet_coalesce_read_enable && _param.sb_stream != nullptr) {
        std::vector<SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGES);
        auto* planner = _column_materializer->read_range_planner();
        bool coalesce_lazy = planner->should_coalesce_active_lazy();
        if (coalesce_lazy || !config::io_coalesce_adaptive_lazy_active) {
            _param.stats->active_lazy_coalesce_together += 1;
        } else {
            _param.stats->active_lazy_coalesce_seperately += 1;
        }
        _set_end_offset(end_offset);
        RETURN_IF_ERROR(_param.sb_stream->set_io_ranges(ranges, coalesce_lazy));
    }

    RETURN_IF_ERROR(_column_materializer->rewrite_dict_conjuncts_to_predicate(&_is_group_filtered));
    RETURN_IF_ERROR(_column_materializer->init_read_chunk());
    _variant->init_read_chunk_slots();

    if (!_is_group_filtered) {
        _range_iter = _range.new_iterator();
    }

    _has_prepared = true;
    return Status::OK();
}

// ── Simple accessors ────────────────────────────────────────────────────────

const tparquet::ColumnChunk* GroupReader::get_chunk_metadata(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second->get_chunk_metadata();
}

ColumnReader* GroupReader::get_column_reader(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second.get();
}

const ParquetField* GroupReader::get_column_parquet_field(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second->get_column_parquet_field();
}

const tparquet::RowGroup* GroupReader::get_row_group_metadata() const {
    return _row_group_metadata;
}

// ── get_next: materialise one chunk from the current row group ──────────────
//
// Pipeline (7 stages):
//   1. Prune deleted rows           — deletion bitmap
//   2. Read & filter active columns — dict / expression predicate pushdown
//   3. Evaluate compound predicates — multi-slot conjuncts from scanner_ctxs
//   4. Evaluate variant predicates  — fetch sources + deferred subfield conjuncts
//   5. Filter & backfill lazy       — apply combined filter + lazy column backfill
//   6. Append output side columns   — partition / not-existed / extended / count
//   7. Emit output                  — variant projections + physical columns

Status GroupReader::get_next(ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    while (true) {
        if (!_range_iter.has_more()) {
            *row_count = 0;
            return Status::EndOfFile("");
        }

        auto r = _range_iter.next(*row_count);
        _param.stats->raw_rows_read += r.span_size();

        _column_materializer->reset_read_chunk();
        _variant->reset_iteration_state();

        RowGroupScanState state;
        state.active_chunk = _column_materializer->create_active_chunk();
        state.row_count = r.span_size();
        state.chunk_filter = Filter(state.row_count, 1);

        LazyMaterializationContext lazy_ctx(_column_materializer.get(), _variant.get(), r, nullptr, state.active_chunk);

        // 1. Prune deleted rows
        ASSIGN_OR_RETURN(bool rows_survive, _prune_deleted_rows(r, state));
        if (!rows_survive) continue;

        // 2. Read & filter active columns
        state.active_chunk->set_missing_column_provider(&lazy_ctx);
        ASSIGN_OR_RETURN(rows_survive, _read_and_filter_active_columns(r, state, &lazy_ctx));
        if (!rows_survive) {
            state.active_chunk->set_missing_column_provider(nullptr);
            continue;
        }

        // 3. Evaluate compound predicates
        ASSIGN_OR_RETURN(rows_survive, _evaluate_compound_predicates(r, state));
        state.active_chunk->set_missing_column_provider(nullptr);
        if (!rows_survive) continue;

        // 4. Evaluate variant predicates
        ASSIGN_OR_RETURN(rows_survive, _evaluate_variant_predicates(r, state));
        if (!rows_survive) continue;

        // 5. Apply combined filter and backfill lazy columns
        ASSIGN_OR_RETURN(rows_survive, _filter_and_backfill_lazy(r, state));
        if (!rows_survive) continue;

        // 6. Emit output
        RETURN_IF_ERROR(_emit_output_columns(state, chunk, row_count));

        // 7. Append output side columns AFTER emit.
        //    Use *row_count (captured before emit swaps columns out of
        //    active_chunk) rather than (*chunk)->num_rows() — the latter
        //    reads the first column's size, which can be 0 when the first
        //    slot is a partition/not-existed/extended side column that
        //    hasn't been populated yet.
        if ((*row_count) > 0) {
            RETURN_IF_ERROR(_param.scanner_ctx->append_side_columns_to_chunk(chunk, (*row_count)));
        }
        break;
    }

    return _range_iter.has_more() ? Status::OK() : Status::EndOfFile("");
}

// ── 1. Prune deleted rows ──────

StatusOr<bool> GroupReader::_prune_deleted_rows(const Range<uint64_t>& r, RowGroupScanState& state) {
    if (nullptr == _skip_rows_ctx || !_skip_rows_ctx->has_skip_rows()) {
        return true;
    }
    SCOPED_RAW_TIMER(&_param.stats->build_rowid_filter_ns);
    ASSIGN_OR_RETURN(state.has_filter,
                     _skip_rows_ctx->deletion_bitmap->fill_filter(r.begin(), r.end(), state.chunk_filter));
    if (SIMD::count_nonzero(state.chunk_filter.data(), state.row_count) == 0) {
        return false;
    }
    return true;
}

// ── 2. Read & filter active columns ──────

StatusOr<bool> GroupReader::_read_and_filter_active_columns(const Range<uint64_t>& r, RowGroupScanState& state,
                                                            LazyMaterializationContext* lazy_ctx) {
    if (_column_materializer->has_predicate_filter()) {
        state.has_filter = true;
        ASSIGN_OR_RETURN(size_t hit_count, _column_materializer->read_active_range_round_by_round(
                                                   r, &state.chunk_filter, &state.active_chunk, lazy_ctx));
        if (hit_count == 0) {
            _param.stats->late_materialize_skip_rows += state.row_count;
            return false;
        }
    } else if (state.has_filter) {
        RETURN_IF_ERROR(_column_materializer->read_active_range(r, &state.chunk_filter, &state.active_chunk));
    } else {
        RETURN_IF_ERROR(_column_materializer->read_active_range(r, nullptr, &state.active_chunk));
    }
    return true;
}

// ── 3. Evaluate compound predicates ──────

StatusOr<bool> GroupReader::_evaluate_compound_predicates(const Range<uint64_t>& r, RowGroupScanState& state) {
    if (_param.scanner_ctx->conjuncts.scanner_ctxs.empty()) {
        return true;
    }

    // VARIANT virtual projection slots must be materialised before compound
    // conjuncts are evaluated.  When a compound conjunct references a variant
    // virtual slot, fetch the needed hidden sources and project the virtual
    // slots early.  active_chunk is rebuilt per range so stale slots cannot leak.
    if (!_variant->empty()) {
        auto early_projected =
                _variant->referenced_variant_virtual_slot_ids(_param.scanner_ctx->conjuncts.scanner_ctxs);
        if (!early_projected.empty()) {
            RETURN_IF_ERROR(_variant->fetch_and_project_virtual_slots(early_projected, r, state.active_chunk,
                                                                      _variant->projection_timezone()));
        }
    }

    // Append side columns to active_chunk so compound conjuncts referencing
    // partition / not-existed / extended slots can be evaluated correctly.
    if (state.active_chunk->num_rows() > 0) {
        RETURN_IF_ERROR(
                _param.scanner_ctx->append_side_columns_to_chunk(&state.active_chunk, state.active_chunk->num_rows()));
    }

    // Finalize all active columns to logical form before compound conjunct eval.
    for (int col_idx : _column_materializer->active_column_indices()) {
        SlotId slot_id = _param.read_cols[col_idx].slot_id();
        RETURN_IF_ERROR(_column_materializer->finalize_active_slot(slot_id, state.active_chunk));
    }

    ASSIGN_OR_RETURN(size_t compound_hit, ChunkPredicateEvaluator::eval_conjuncts_into_filter(
                                                  _param.scanner_ctx->conjuncts.scanner_ctxs, state.active_chunk.get(),
                                                  &state.chunk_filter));
    if (compound_hit == 0) {
        _param.stats->late_materialize_skip_rows += state.row_count;
        return false;
    }
    state.has_filter = true;
    return true;
}

// ── 4. Evaluate variant predicates ──────

StatusOr<bool> GroupReader::_evaluate_variant_predicates(const Range<uint64_t>& r, RowGroupScanState& state) {
    // fetch_sources() runs unconditionally: active variant hidden sources are
    // needed for output even when no deferred conjuncts exist.
    // _fetched_hidden_slots tracks already-populated columns from any early
    // fetch during compound eval; fetch_sources() skips those.
    RETURN_IF_ERROR(_variant->fetch_sources(r, state.active_chunk));

    if (!_variant->has_deferred_conjuncts()) {
        return true;
    }

    ASSIGN_OR_RETURN(Filter vr, _variant->filter_subfields(state.active_chunk, state.row_count, _param.stats,
                                                           _variant->projection_timezone()));
    if (!vr.empty()) {
        if (SIMD::count_nonzero(vr.data(), vr.size()) == 0) {
            return false;
        }
        DCHECK_EQ(vr.size(), state.row_count);
        for (size_t i = 0; i < state.row_count; i++) {
            state.chunk_filter[i] &= vr[i];
        }
        state.has_filter = true;
    }
    return true;
}

// ── 5. Filter & backfill lazy ──────

StatusOr<bool> GroupReader::_filter_and_backfill_lazy(const Range<uint64_t>& r, RowGroupScanState& state) {
    Range<uint64_t> post_filter_range;
    Filter post_filter;

    if (state.has_filter) {
        state.active_chunk->filter(state.chunk_filter);
        if (state.active_chunk->num_rows() == 0) {
            return false;
        }
        RETURN_IF_ERROR(_variant->align_after_combined_filter(state.active_chunk, state.chunk_filter, state.row_count));

        post_filter_range = r.filter(&state.chunk_filter);
        DCHECK(post_filter_range.span_size() > 0);
        post_filter = {state.chunk_filter.begin() + post_filter_range.begin() - r.begin(),
                       state.chunk_filter.begin() + post_filter_range.end() - r.begin()};
    }

    bool has_any_lazy =
            !_column_materializer->lazy_column_indices().empty() || !_variant->lazy_hidden_slot_ids().empty();
    if (has_any_lazy) {
        _param.stats->parquet_lazy_col_skip_rows += state.row_count - state.active_chunk->num_rows();
    }
    if (!_column_materializer->lazy_column_indices().empty()) {
        RETURN_IF_ERROR(_column_materializer->read_lazy_columns(r, post_filter_range, post_filter, state.chunk_filter,
                                                                state.has_filter, state.active_chunk));
    }

    RETURN_IF_ERROR(_variant->backfill_sources(r, state.has_filter ? &post_filter_range : nullptr,
                                               state.has_filter ? &post_filter : nullptr, state.has_filter,
                                               state.active_chunk));
    return true;
}

// ── 7. Emit output ──────

Status GroupReader::_emit_output_columns(RowGroupScanState& state, ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
    *row_count = state.active_chunk->num_rows();

    if (_variant->has_projections()) {
        RETURN_IF_ERROR(_variant->emit_projections(state.active_chunk, chunk, _variant->projection_timezone()));
    }
    {
        auto skip_slots = _variant->projection_slot_ids();
        RETURN_IF_ERROR(_column_materializer->emit_physical_columns(state.active_chunk, chunk, &skip_slots));
    }
    return Status::OK();
}

// ── Column reader creation ─────────────────────────────────────────────────

StatusOr<ColumnReaderPtr> GroupReader::_create_reserved_iceberg_column_reader(const SlotDescriptor* slot,
                                                                              int32_t field_id) {
    int32_t field_idx = _param.file_metadata->schema().get_field_idx_by_field_id(field_id);
    if (field_idx < 0) {
        field_idx = _param.file_metadata->schema().get_field_idx_by_column_name(slot->col_name());
    }
    if (field_idx < 0) {
        return ColumnReaderPtr(nullptr);
    }

    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(field_idx);
    GroupReaderParam::Column column{};
    column.idx_in_parquet = field_idx;
    column.type_in_parquet = schema_node->physical_type;
    column.slot_desc = const_cast<SlotDescriptor*>(slot);
    column.t_lake_schema_field = nullptr;
    column.decode_needed = true;
    return _create_column_reader(column);
}

StatusOr<Datum> GroupReader::_get_extended_bigint_value(SlotId slot_id) const {
    if (_param.scan_range == nullptr || !_param.scan_range->__isset.extended_columns) {
        return Status::NotFound(fmt::format("Cannot find extended column for slot {}", slot_id));
    }

    const auto& extended_columns = _param.scan_range->extended_columns;
    auto it = extended_columns.find(slot_id);
    if (it == extended_columns.end()) {
        return Status::NotFound(fmt::format("Cannot find extended column value for slot {}", slot_id));
    }

    const auto& expr = it->second;
    if (expr.nodes.empty()) {
        return Status::InvalidArgument(fmt::format("Invalid extended column expression for slot {}", slot_id));
    }

    const auto& node = expr.nodes[0];
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        return kNullDatum;
    }
    if (node.node_type != TExprNodeType::INT_LITERAL || !node.__isset.int_literal) {
        return Status::InvalidArgument(fmt::format("Unsupported extended column expression for slot {}", slot_id));
    }

    return Datum(node.int_literal.value);
}

Status GroupReader::_create_column_readers() {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    _global_dict_applied_in_group = false;
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.file_meta_data = _param.file_metadata;
    if (_param.scanner_ctx == nullptr) {
        return Status::InternalError("GroupReader: scanner_ctx must not be null");
    }
    opts.timezone = _param.scanner_ctx->timezone;
    opts.case_sensitive = _param.scanner_ctx->format_scan_context.options.case_sensitive;
    opts.use_file_pagecache = _param.scanner_ctx->format_scan_context.options.use_file_pagecache;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata;
    opts.first_row_index = _row_group_first_row;
    opts.modification_time = _param.modification_time;
    opts.file_size = _param.file_size;
    opts.datacache_options = _param.datacache_options;

    // Setup variant handler (idempotent: no-op when no variant virtual columns exist).
    RETURN_IF_ERROR(_variant->setup_readers());

    for (const auto& column : _param.read_cols) {
        // Extended variant virtual columns are handled by _variant->setup_readers above.
        if (column.is_extended_variant_virtual) continue;
        ASSIGN_OR_RETURN(ColumnReaderPtr column_reader, _create_column_reader(column));
        _column_readers[column.slot_id()] = std::move(column_reader);
    }

    // Register zone-map readers AFTER physical column readers are created.
    _variant->register_zone_map_readers();

    // create for partition values
    const auto& partition_columns = _param.scanner_ctx->partition_columns;
    const auto& partition_values = _param.scanner_ctx->partition_values;
    for (size_t i = 0; i < partition_columns.size(); i++) {
        const auto& column = partition_columns[i];
        const auto* slot_desc = column.slot_desc;
        const auto value = partition_values[i];
        _column_readers.emplace(slot_desc->id(), std::make_unique<FixedValueColumnReader>(value->get(0)));
    }

    // create for not existed column
    for (const auto* slot : _param.scanner_ctx->not_existed_slots) {
        _column_readers.emplace(slot->id(), std::make_unique<FixedValueColumnReader>(kNullDatum));
    }

    const auto& reserved_slots = _param.scanner_ctx->reserved_field_slots;
    if (!reserved_slots.empty()) {
        bool use_legacy_lookup_row_id =
                std::any_of(reserved_slots.begin(), reserved_slots.end(), [](const SlotDescriptor* slot) {
                    return slot->col_name() == formats::kRowSourceIdColumnName ||
                           slot->col_name() == formats::kScanRangeIdColumnName;
                });
        for (const auto* slot : reserved_slots) {
            if (slot->col_name() == formats::kIcebergRowIdColumnName) {
                ASSIGN_OR_RETURN(auto reader,
                                 _create_reserved_iceberg_column_reader(slot, formats::kIcebergRowIdColumnId));
                std::optional<int64_t> first_row_id = std::nullopt;
                if (_param.scan_range != nullptr && _param.scan_range->__isset.first_row_id) {
                    // IcebergRowIdReader emits `first_row_id + i` where `i` is a file-local row
                    // index (it already includes the row-group start), so the base must be the
                    // file-level first_row_id. A row-group-level base double-counts the row-group
                    // start and shifts the emitted _row_id for every row group after the first,
                    // diverging from the physical _row_id column of compacted files. The file-level
                    // base also keeps the reader's zone-map filters (base + rg_first_row) correct.
                    first_row_id = std::optional<int64_t>(_param.scan_range->first_row_id);
                } else if (use_legacy_lookup_row_id) {
                    // Legacy (non-lineage) GLM keys rows by file-local position: the base is 0.
                    first_row_id = std::optional<int64_t>(0);
                }
                ColumnReaderPtr row_id_reader =
                        reader != nullptr ? std::make_unique<IcebergRowIdReader>(std::move(reader), first_row_id)
                                          : std::make_unique<IcebergRowIdReader>(first_row_id);
                _column_readers.emplace(slot->id(), std::move(row_id_reader));
            } else if (slot->col_name() == formats::kIcebergLastUpdatedSequenceNumberColumnName) {
                ASSIGN_OR_RETURN(auto reader, _create_reserved_iceberg_column_reader(
                                                      slot, formats::kIcebergLastUpdatedSequenceNumberColumnId));
                Datum sequence_number = kNullDatum;
                bool can_use_fallback = false;
                auto sequence_number_or = _get_extended_bigint_value(slot->id());
                if (sequence_number_or.ok()) {
                    sequence_number = sequence_number_or.value();
                    can_use_fallback = true;
                } else if (!sequence_number_or.status().is_not_found()) {
                    return sequence_number_or.status();
                }
                ColumnReaderPtr seq_reader =
                        reader != nullptr ? std::make_unique<IcebergLastUpdatedSequenceNumberReader>(
                                                    std::move(reader), can_use_fallback, sequence_number)
                                          : std::make_unique<IcebergLastUpdatedSequenceNumberReader>(sequence_number);
                _column_readers.emplace(slot->id(), std::move(seq_reader));
            } else if (slot->col_name() == formats::kRowSourceIdColumnName) {
                if (auto opt = get_backend_id(); opt.has_value()) {
                    _column_readers.emplace(slot->id(), std::make_unique<RowSourceReader>(opt.value()));
                } else {
                    return Status::InternalError("get_backend_id failed");
                }
            } else if (slot->col_name() == formats::kScanRangeIdColumnName) {
                _column_readers.emplace(slot->id(), std::make_unique<FixedValueColumnReader>(_param.scan_range_id));
            } else if (slot->col_name() == formats::kIcebergRowPositionColumnName) {
                _column_readers.emplace(slot->id(), std::make_unique<ParquetPosReader>());
            }
        }
    }

    if (_param.stats != nullptr) {
        _param.stats->global_dict_total_row_groups++;
        if (_global_dict_applied_in_group) {
            _param.stats->global_dict_applied_row_groups++;
        }
    }

    return Status::OK();
}

StatusOr<ColumnReaderPtr> GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(column.idx_in_parquet);
    {
        if (column.slot_type().type == LogicalType::TYPE_VARIANT && schema_node != nullptr &&
            schema_node->type == ColumnType::STRUCT) {
            // Physical VARIANT columns use _get_variant_shredded_hints; this path
            // is for non-virtual VARIANT columns that appear directly in the SELECT list.
            VariantShreddedReadHints hints = build_variant_shredded_hints(&_param.scanner_ctx->column_access_paths,
                                                                          column.slot_desc->col_name());
            ASSIGN_OR_RETURN(column_reader, ColumnReaderFactory::create_variant_column_reader(_column_reader_opts,
                                                                                              schema_node, hints));
        } else if (column.t_lake_schema_field == nullptr) {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type()));
        } else {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type(),
                                                         column.t_lake_schema_field));
        }
        auto* global_dictmaps = _param.scanner_ctx->global_dictmaps;
        if (global_dictmaps->contains(column.slot_id())) {
            GlobalDictReaderKind kind = GlobalDictReaderKind::kNone;
            ASSIGN_OR_RETURN(column_reader, ColumnReaderFactory::create(
                                                    std::move(column_reader), global_dictmaps->at(column.slot_id()),
                                                    column.slot_id(), _row_group_metadata->num_rows, &kind));
            if (_param.stats != nullptr && kind != GlobalDictReaderKind::kNone) {
                _param.stats->global_dict_applied_slots++;
                if (kind == GlobalDictReaderKind::kDictCode) {
                    _param.stats->global_dict_dict_code_reader_slots++;
                } else if (kind == GlobalDictReaderKind::kLowRowsEncode) {
                    _param.stats->global_dict_encode_reader_slots++;
                }
                _global_dict_applied_in_group = true;
            }
        }
        if (column_reader == nullptr) {
            return Status::InternalError("No valid column reader.");
        }
    }
    return column_reader;
}

// ── Column / conjunct classification ────────────────────────────────────────

Status GroupReader::_prepare_column_readers() const {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    for (const auto& [slot_id, column_reader] : _column_readers) {
        RETURN_IF_ERROR(column_reader->prepare());
        if (column_reader->get_column_parquet_field() != nullptr &&
            column_reader->get_column_parquet_field()->is_complex_type()) {
            column_reader->set_need_parse_levels(true);
        }
    }
    RETURN_IF_ERROR(_variant->prepare_hidden_readers());
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    // ── Variant setup ─────────────────────────────────────────────────────────
    auto deferred_slots = _variant->deferred_conjunct_physical_source_slots();
    _variant->collect_deferred_conjuncts();

    // ── Classify physical columns and conjuncts ───────────────────────────────
    bool has_reserved_field_filter = false;
    _column_materializer->classify_columns(deferred_slots, &has_reserved_field_filter);

    // ── Variant hidden source classification ──────────────────────────────────
    _variant->classify_hidden_sources();
    for (SlotId sid : _variant->active_hidden_slot_ids()) {
        _column_materializer->add_active_slot(sid);
    }
    if (!config::parquet_late_materialization_enable) {
        _column_materializer->promote_lazy_to_active();
        _variant->promote_lazy_to_active();
    }

    // ── Promote lazy to active when no active columns exist ───────────────────
    if (_column_materializer->active_slot_ids().empty() && !has_reserved_field_filter) {
        _column_materializer->promote_lazy_to_active();
        _variant->promote_lazy_to_active();
    }
}

// ── IO range collection ─────────────────────────────────────────────────────

void GroupReader::collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                    ColumnIOTypeFlags types) {
    int64_t end = 0;
    _column_materializer->collect_io_ranges(ranges, &end, types);
    _variant->collect_io_ranges(ranges, &end, types);
    ReadRangePlanner::deduplicate(ranges);
    *end_offset = end;
}

} // namespace starrocks::parquet
