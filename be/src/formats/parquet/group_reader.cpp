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
#include <utility>

#include "agent/master_info.h"
#include "column/chunk.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/iceberg_row_id_reader.h"
#include "formats/parquet/lazy_materialization_context.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_pos_reader.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/read_range_planner.h"
#include "formats/parquet/row_source_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "utils.h"

namespace starrocks::parquet {

// ── GroupReader: construction / destruction ─────────────────────────────────

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row)
        : _row_group_first_row(row_group_first_row), _skip_rows_ctx(std::move(skip_rows_ctx)), _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
    _column_materializer = std::make_unique<ColumnMaterializer>(_param, &_column_readers);
}

<<<<<<< HEAD
GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row, int64_t row_group_first_row_id)
        : _row_group_first_row(row_group_first_row),
          _row_group_first_row_id(row_group_first_row_id),
          _skip_rows_ctx(std::move(skip_rows_ctx)),
          _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
    _column_materializer = std::make_unique<ColumnMaterializer>(_param, &_column_readers);
}

=======
>>>>>>> d139404a3d ([BugFix] Iceberg _row_id: use the file-level first_row_id as the reader base (#75758))
GroupReader::~GroupReader() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    // If GroupReader is filtered by statistics, it's _has_prepared = false
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
            size_t total_lazy = _column_materializer->lazy_slot_ids().size();
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

Status GroupReader::init() {
    // Create column readers and bind ParquetField & ColumnChunkMetaData(except complex type) to each ColumnReader
    RETURN_IF_ERROR(_create_column_readers());
    _process_columns_and_conjunct_ctxs();
    _range = SparseRange<uint64_t>(_row_group_first_row, _row_group_first_row + _row_group_metadata->num_rows);
    return Status::OK();
}

Status GroupReader::prepare() {
    RETURN_IF_ERROR(_prepare_column_readers());
    // we need deal with page index first, so that it can work on collect_io_range,
    // and pageindex's io has been collected in FileReader

    if (_range.span_size() != get_row_group_metadata()->num_rows) {
        for (const auto& pair : _column_readers) {
            pair.second->select_offset_index(_range, _row_group_first_row);
        }
    }

    // if coalesce read enabled, we have to
    // 1. allocate shared buffered input stream and
    // 2. collect io ranges of every row group reader.
    // 3. set io ranges to the stream.
    if (config::parquet_coalesce_read_enable && _param.sb_stream != nullptr) {
        std::vector<starrocks::io::SharedBufferedInputStream::IORange> ranges;
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

    if (!_is_group_filtered) {
        _range_iter = _range.new_iterator();
    }

    _has_prepared = true;
    return Status::OK();
}

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

        RowGroupScanState state;
        state.active_chunk = _column_materializer->create_active_chunk();
        state.row_count = r.span_size();
        state.chunk_filter = Filter(state.row_count, 1);

        LazyMaterializationContext lazy_ctx(_column_materializer.get(), r, nullptr, state.active_chunk);

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

        // 4. Apply combined filter and backfill lazy columns
        ASSIGN_OR_RETURN(rows_survive, _filter_and_backfill_lazy(r, state));
        if (!rows_survive) continue;

        // 5. Emit output
        RETURN_IF_ERROR(_emit_output_columns(state, chunk, row_count));

        // 6. Append output side columns AFTER emit.
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

    ASSIGN_OR_RETURN(size_t compound_hit,
                     ExecNode::eval_conjuncts_into_filter(_param.scanner_ctx->conjuncts.scanner_ctxs,
                                                          state.active_chunk.get(), &state.chunk_filter));
    if (compound_hit == 0) {
        _param.stats->late_materialize_skip_rows += state.row_count;
        return false;
    }
    state.has_filter = true;
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

        post_filter_range = r.filter(&state.chunk_filter);
        DCHECK(post_filter_range.span_size() > 0);
        post_filter = {state.chunk_filter.begin() + post_filter_range.begin() - r.begin(),
                       state.chunk_filter.begin() + post_filter_range.end() - r.begin()};
    }

    bool has_any_lazy = !_column_materializer->lazy_column_indices().empty();
    if (has_any_lazy) {
        _param.stats->parquet_lazy_col_skip_rows += state.row_count - state.active_chunk->num_rows();
    }
    if (!_column_materializer->lazy_column_indices().empty()) {
        RETURN_IF_ERROR(_column_materializer->read_lazy_columns(r, post_filter_range, post_filter, state.chunk_filter,
                                                                state.has_filter, state.active_chunk));
    }

    return true;
}

// ── 7. Emit output ──────

Status GroupReader::_emit_output_columns(RowGroupScanState& state, ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
    *row_count = state.active_chunk->num_rows();
    RETURN_IF_ERROR(_column_materializer->emit_physical_columns(state.active_chunk, chunk));
    return Status::OK();
}

// ── Column reader creation ─────────────────────────────────────────────────

StatusOr<ColumnReaderPtr> GroupReader::_create_reserved_iceberg_column_reader(const SlotDescriptor* slot,
                                                                              int32_t field_id) {
    // Try to find the physical column in the Parquet file by Iceberg spec field ID first (canonical),
    // then fall back to column name lookup for compatibility.
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
        return Status::NotFound(strings::Substitute("Cannot find extended column for slot $0", slot_id));
    }

    const auto& extended_columns = _param.scan_range->extended_columns;
    auto it = extended_columns.find(slot_id);
    if (it == extended_columns.end()) {
        return Status::NotFound(strings::Substitute("Cannot find extended column value for slot $0", slot_id));
    }

    const auto& expr = it->second;
    if (expr.nodes.empty()) {
        return Status::InvalidArgument(strings::Substitute("Invalid extended column expression for slot $0", slot_id));
    }

    const auto& node = expr.nodes[0];
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        return kNullDatum;
    }
    if (node.node_type != TExprNodeType::INT_LITERAL || !node.__isset.int_literal) {
        return Status::InvalidArgument(
                strings::Substitute("Unsupported extended column expression for slot $0", slot_id));
    }

    return Datum(node.int_literal.value);
}

Status GroupReader::_create_column_readers() {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    // ColumnReaderOptions is used by all column readers in one row group
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.file_meta_data = _param.file_metadata;
    if (_param.scanner_ctx == nullptr) {
        return Status::InternalError("GroupReader: scanner_ctx must not be null");
    }
    opts.timezone = _param.scanner_ctx->timezone;
    opts.case_sensitive = _param.scanner_ctx->options.case_sensitive;
    opts.use_file_pagecache = _param.scanner_ctx->options.use_file_pagecache;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata;
    opts.first_row_index = _row_group_first_row;
    opts.modification_time = _param.modification_time;
    opts.file_size = _param.file_size;
    opts.datacache_options = _param.datacache_options;
    for (const auto& column : _param.read_cols) {
        ASSIGN_OR_RETURN(ColumnReaderPtr column_reader, _create_column_reader(column));
        _column_readers[column.slot_id()] = std::move(column_reader);
    }

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
                    return slot->col_name() == "_row_source_id" || slot->col_name() == "_scan_range_id";
                });
        for (const auto* slot : reserved_slots) {
            if (slot->col_name() == HdfsScanner::ICEBERG_ROW_ID) {
                // Iceberg v3 row lineage: try physical column first (post-compaction files),
                // fall back to computed row_id (firstRowId + position) for non-compacted files.
                ASSIGN_OR_RETURN(auto reader,
                                 _create_reserved_iceberg_column_reader(slot, HdfsScanner::kIcebergRowIdColumnId));
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
            } else if (slot->col_name() == HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER) {
                // Iceberg v3 row lineage: try physical column first (post-compaction files),
                // fall back to file-level dataSequenceNumber passed via extended_columns from FE.
                ASSIGN_OR_RETURN(auto reader,
                                 _create_reserved_iceberg_column_reader(
                                         slot, HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_ID));
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
            } else if (slot->col_name() == "_row_source_id") {
                if (auto opt = get_backend_id(); opt.has_value()) {
                    _column_readers.emplace(slot->id(), std::make_unique<RowSourceReader>(opt.value()));
                } else {
                    return Status::InternalError("get_backend_id failed");
                }
            } else if (slot->col_name() == "_scan_range_id") {
                _column_readers.emplace(slot->id(), std::make_unique<FixedValueColumnReader>(_param.scan_range_id));
            } else if (slot->col_name() == HdfsScanner::ICEBERG_ROW_POSITION) {
                _column_readers.emplace(slot->id(), std::make_unique<ParquetPosReader>());
            }
        }
    }
    return Status::OK();
}

StatusOr<ColumnReaderPtr> GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(column.idx_in_parquet);
    {
        if (column.t_lake_schema_field == nullptr) {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type()));
        } else {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type(),
                                                         column.t_lake_schema_field));
        }
        auto* global_dictmaps = _param.scanner_ctx->global_dictmaps;
        if (global_dictmaps->contains(column.slot_id())) {
            ASSIGN_OR_RETURN(column_reader, ColumnReaderFactory::create(
                                                    std::move(column_reader), global_dictmaps->at(column.slot_id()),
                                                    column.slot_id(), _row_group_metadata->num_rows));
        }
        if (column_reader == nullptr) {
            // this shouldn't happen but guard
            return Status::InternalError("No valid column reader.");
        }
    }
    return column_reader;
}

Status GroupReader::_prepare_column_readers() const {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    for (const auto& [slot_id, column_reader] : _column_readers) {
        RETURN_IF_ERROR(column_reader->prepare());
        if (column_reader->get_column_parquet_field() != nullptr &&
            column_reader->get_column_parquet_field()->is_complex_type()) {
            // For complex type columns, we need parse def & rep levels.
            // For OptionalColumnReader, by default, we will not parse it's def level for performance. But if
            // column is a complex type, we have to parse def level to calculate nullability.
            column_reader->set_need_parse_levels(true);
        }
    }
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    // ── Classify physical columns and conjuncts ───────────────────────────────
    bool has_reserved_field_filter = false;
    std::unordered_set<SlotId> deferred_slots;
    _column_materializer->classify_columns(deferred_slots, &has_reserved_field_filter);

    if (!config::parquet_late_materialization_enable) {
        _column_materializer->promote_lazy_to_active();
    }

    // ── Promote lazy to active when no active columns exist ───────────────────
    if (_column_materializer->active_slot_ids().empty() && !has_reserved_field_filter) {
        _column_materializer->promote_lazy_to_active();
    }
}

// ── IO range collection ─────────────────────────────────────────────────────

void GroupReader::collect_io_ranges(std::vector<starrocks::io::SharedBufferedInputStream::IORange>* ranges,
                                    int64_t* end_offset, ColumnIOTypeFlags types) {
    int64_t end = 0;
    _column_materializer->collect_io_ranges(ranges, &end, types);
    ReadRangePlanner::deduplicate(ranges);
    *end_offset = end;
}

} // namespace starrocks::parquet
