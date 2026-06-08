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

<<<<<<< HEAD
#include "agent/master_info.h"
#include "column/chunk.h"
#include "common/config.h"
#include "common/status.h"
=======
#include "base/simd/simd.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/config_scan_io_fwd.h"
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
<<<<<<< HEAD
=======
#include "exprs/chunk_predicate_evaluator.h"
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/iceberg_row_id_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_pos_reader.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/row_source_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/variant_projection.h"
#include "gen_cpp/Exprs_types.h"
<<<<<<< HEAD
#include "gutil/strings/substitute.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
=======
#include "runtime/chunk_helper.h"
#include "types/type_descriptor.h"
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
#include "utils.h"

namespace starrocks::parquet {

<<<<<<< HEAD
=======
namespace {

// Deduplicate exact-duplicate IO ranges collected from multiple column readers.
void deduplicate_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges) {
    if (ranges == nullptr || ranges->size() <= 1) {
        return;
    }

    std::sort(ranges->begin(), ranges->end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.offset != rhs.offset) {
            return lhs.offset < rhs.offset;
        }
        if (lhs.size != rhs.size) {
            return lhs.size < rhs.size;
        }
        return lhs.is_active < rhs.is_active;
    });

    size_t write_idx = 0;
    for (size_t read_idx = 1; read_idx < ranges->size(); ++read_idx) {
        auto& current = (*ranges)[write_idx];
        const auto& next = (*ranges)[read_idx];
        if (current.offset == next.offset && current.size == next.size) {
            current.is_active = current.is_active || next.is_active;
            continue;
        }
        ++write_idx;
        if (write_idx != read_idx) {
            (*ranges)[write_idx] = next;
        }
    }
    ranges->erase(ranges->begin() + static_cast<std::ptrdiff_t>(write_idx + 1), ranges->end());
}

} // namespace

// ── GroupReader: construction / destruction ─────────────────────────────────

>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row)
        : _row_group_first_row(row_group_first_row), _skip_rows_ctx(std::move(skip_rows_ctx)), _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
    _variant = std::make_unique<VariantProjectionHandler>(this, _param, _row_group_metadata);
}

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row, int64_t row_group_first_row_id)
        : _row_group_first_row(row_group_first_row),
          _row_group_first_row_id(row_group_first_row_id),
          _skip_rows_ctx(std::move(skip_rows_ctx)),
          _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
    _variant = std::make_unique<VariantProjectionHandler>(this, _param, _row_group_metadata);
}

GroupReader::~GroupReader() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    if (_has_prepared) {
        if (_lazy_column_needed) {
            _param.lazy_column_coalesce_counter->fetch_add(1, std::memory_order_relaxed);
        } else {
            _param.lazy_column_coalesce_counter->fetch_sub(1, std::memory_order_relaxed);
        }
        _param.stats->group_min_round_cost = _param.stats->group_min_round_cost == 0
                                                     ? _column_read_order_ctx->get_min_round_cost()
                                                     : std::min(_param.stats->group_min_round_cost,
                                                                int64_t(_column_read_order_ctx->get_min_round_cost()));
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
<<<<<<< HEAD
    }

    // if coalesce read enabled, we have to
    // 1. allocate shared buffered input stream and
    // 2. collect io ranges of every row group reader.
    // 3. set io ranges to the stream.
=======
        _variant->select_hidden_source_offset_index();
    }

    // Promote variant virtual columns to typed-value proxy readers.
    _variant->try_promote();

    // Coalesce IO ranges.
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
    if (config::parquet_coalesce_read_enable && _param.sb_stream != nullptr) {
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGES);
        int32_t counter = _param.lazy_column_coalesce_counter->load(std::memory_order_relaxed);
        if (counter >= 0 || !config::io_coalesce_adaptive_lazy_active) {
            _param.stats->group_active_lazy_coalesce_together += 1;
        } else {
            _param.stats->group_active_lazy_coalesce_seperately += 1;
        }
        _set_end_offset(end_offset);
        RETURN_IF_ERROR(_param.sb_stream->set_io_ranges(ranges, counter >= 0));
    }

    RETURN_IF_ERROR(_rewrite_conjunct_ctxs_to_predicates(&_is_group_filtered));
    RETURN_IF_ERROR(_init_read_chunk());

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

<<<<<<< HEAD
=======
// ── get_next: materialise one chunk from the current row group ──────────────
//
// Pipeline:
//   1. Prune deleted rows  — deletion bitmap
//   2. Read & filter active columns — dict / expression predicate pushdown
//   3. Fetch variant sources (via VariantProjectionHandler)
//   4. Filter by subfields   (via VariantProjectionHandler)
//   5. Backfill lazy columns — physical columns without predicates
//   6. Backfill variant sources (via VariantProjectionHandler)
//   7. Emit output — decode physical columns + variant projections

>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
Status GroupReader::get_next(ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }
<<<<<<< HEAD
    _read_chunk->reset();

    ChunkPtr active_chunk = _create_read_chunk(_active_column_indices, false);
    // to complicity with _do_get_next will break and return even active_row is all filtered.
    // but a better choice is don't return until really have some results.
=======

    _read_chunk->reset();
    _variant->reset_iteration_state();
    ChunkPtr active_chunk = _create_read_chunk(_active_slot_ids);

>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
    while (true) {
        if (!_range_iter.has_more()) {
            *row_count = 0;
            return Status::EndOfFile("");
        }

        auto r = _range_iter.next(*row_count);
        auto count = r.span_size();
        _param.stats->raw_rows_read += count;

        active_chunk->reset();

<<<<<<< HEAD
        bool has_filter = false;
        Filter chunk_filter(count, 1);

        // row id filter
        if (nullptr != _skip_rows_ctx && _skip_rows_ctx->has_skip_rows()) {
            {
                SCOPED_RAW_TIMER(&_param.stats->build_rowid_filter_ns);
                ASSIGN_OR_RETURN(has_filter,
                                 _skip_rows_ctx->deletion_bitmap->fill_filter(r.begin(), r.end(), chunk_filter));

                if (SIMD::count_nonzero(chunk_filter.data(), count) == 0) {
                    continue;
                }
            }
        }

        // we really have predicate to run round by round
        if (!_dict_column_indices.empty() || !_left_no_dict_filter_conjuncts_by_slot.empty()) {
            has_filter = true;
            ASSIGN_OR_RETURN(size_t hit_count, _read_range_round_by_round(r, &chunk_filter, &active_chunk));
            if (hit_count == 0) {
                _param.stats->late_materialize_skip_rows += count;
                continue;
            }
            active_chunk->filter_range(chunk_filter, 0, count);
        } else if (has_filter) {
            RETURN_IF_ERROR(_read_range(_active_column_indices, r, &chunk_filter, &active_chunk));
            active_chunk->filter_range(chunk_filter, 0, count);
        } else {
            RETURN_IF_ERROR(_read_range(_active_column_indices, r, nullptr, &active_chunk));
        }

        // deal with lazy columns
        if (!_lazy_column_indices.empty()) {
            _lazy_column_needed = true;
            ChunkPtr lazy_chunk = _create_read_chunk(_lazy_column_indices, true);

            if (has_filter) {
                Range<uint64_t> lazy_read_range = r.filter(&chunk_filter);
                // if all data is filtered, we have skipped early.
                DCHECK(lazy_read_range.span_size() > 0);
                Filter lazy_filter = {chunk_filter.begin() + lazy_read_range.begin() - r.begin(),
                                      chunk_filter.begin() + lazy_read_range.end() - r.begin()};
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, lazy_read_range, &lazy_filter, &lazy_chunk, true));
                lazy_chunk->filter_range(lazy_filter, 0, lazy_read_range.span_size());
            } else {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, r, nullptr, &lazy_chunk, true));
            }

            if (lazy_chunk->num_rows() != active_chunk->num_rows()) {
                return Status::InternalError(strings::Substitute("Unmatched row count, active_rows=$0, lazy_rows=$1",
                                                                 active_chunk->num_rows(), lazy_chunk->num_rows()));
            }
            active_chunk->merge(std::move(*lazy_chunk));
        }

        *row_count = active_chunk->num_rows();

        SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
        // convert from _read_chunk to chunk.
        RETURN_IF_ERROR(_fill_dst_chunk(active_chunk, chunk));
=======
        // 1. Prune deleted rows
        ASSIGN_OR_RETURN(bool rows_survive, _prune_deleted_rows(r, chunk_filter, has_filter, count));
        if (!rows_survive) continue;

        // 2. Read & filter active columns
        ASSIGN_OR_RETURN(rows_survive,
                         _read_and_filter_active_columns(r, chunk_filter, active_chunk, has_filter, count));
        if (!rows_survive) continue;

        // 3. Fetch variant sources (for subfield conjuncts)
        RETURN_IF_ERROR(_variant->fetch_sources(r, active_chunk));

        // 4. Filter by subfields (variant deferred conjuncts)
        if (_variant->has_deferred_conjuncts()) {
            ASSIGN_OR_RETURN(Filter vr, _variant->filter_subfields(active_chunk, count, _param.stats,
                                                                   _variant->projection_timezone()));
            if (!vr.empty()) {
                if (SIMD::count_nonzero(vr.data(), vr.size()) == 0) {
                    continue;
                }
                DCHECK_EQ(vr.size(), count);
                for (size_t i = 0; i < count; i++) {
                    chunk_filter[i] &= vr[i];
                }
                has_filter = true;
            }
        }

        // Apply combined chunk_filter to physically reduce active_chunk.
        if (has_filter) {
            active_chunk->filter(chunk_filter);
            if (active_chunk->num_rows() == 0) {
                continue;
            }
            RETURN_IF_ERROR(_variant->align_after_combined_filter(active_chunk, chunk_filter, count));
        }

        // Compute post-filter range for lazy reads (Phase 5/6).
        Range<uint64_t> post_filter_range;
        Filter post_filter;
        if (has_filter) {
            post_filter_range = r.filter(&chunk_filter);
            DCHECK(post_filter_range.span_size() > 0);
            post_filter = {chunk_filter.begin() + post_filter_range.begin() - r.begin(),
                           chunk_filter.begin() + post_filter_range.end() - r.begin()};
        }

        // 5. Backfill lazy physical columns
        if (!_lazy_column_indices.empty()) {
            _lazy_column_needed = true;
            RETURN_IF_ERROR(_read_lazy_columns(r, post_filter_range, post_filter, has_filter, active_chunk));
        }

        // 6. Backfill lazy variant sources
        RETURN_IF_ERROR(_variant->backfill_sources(r, has_filter ? &post_filter_range : nullptr,
                                                   has_filter ? &post_filter : nullptr, has_filter, active_chunk));

        // 7. Emit output
        {
            SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
            *row_count = active_chunk->num_rows();

            if (_variant->has_projections()) {
                RETURN_IF_ERROR(_variant->emit_projections(active_chunk, chunk, _variant->projection_timezone()));
            }
            RETURN_IF_ERROR(_emit_physical_columns(active_chunk, chunk));
        }
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
        break;
    }

    return _range_iter.has_more() ? Status::OK() : Status::EndOfFile("");
}

// ── 1. Prune deleted rows ──────

StatusOr<bool> GroupReader::_prune_deleted_rows(const Range<uint64_t>& r, Filter& chunk_filter, bool& has_filter,
                                                size_t count) {
    if (nullptr == _skip_rows_ctx || !_skip_rows_ctx->has_skip_rows()) {
        return true;
    }
    SCOPED_RAW_TIMER(&_param.stats->build_rowid_filter_ns);
    ASSIGN_OR_RETURN(has_filter, _skip_rows_ctx->deletion_bitmap->fill_filter(r.begin(), r.end(), chunk_filter));
    if (SIMD::count_nonzero(chunk_filter.data(), count) == 0) {
        return false;
    }
    return true;
}

// ── 2. Read & filter active columns ──────

StatusOr<bool> GroupReader::_read_and_filter_active_columns(const Range<uint64_t>& r, Filter& chunk_filter,
                                                            ChunkPtr& active_chunk, bool& has_filter, size_t count) {
    if (!_dict_column_indices.empty() || !_left_no_dict_filter_conjuncts_by_slot.empty()) {
        has_filter = true;
        ASSIGN_OR_RETURN(size_t hit_count, _read_range_round_by_round(r, &chunk_filter, &active_chunk));
        if (hit_count == 0) {
            _param.stats->late_materialize_skip_rows += count;
            return false;
        }
    } else if (has_filter) {
        RETURN_IF_ERROR(_read_range(_active_column_indices, r, &chunk_filter, &active_chunk));
    } else {
        RETURN_IF_ERROR(_read_range(_active_column_indices, r, nullptr, &active_chunk));
    }
    return true;
}

// ── 5. Backfill lazy physical columns ──────

Status GroupReader::_read_lazy_columns(const Range<uint64_t>& full_range, const Range<uint64_t>& post_filter_range,
                                       const Filter& post_filter, bool has_filter, ChunkPtr& active_chunk) {
    ChunkPtr lazy_chunk = _create_read_chunk(_lazy_slot_ids, false);
    if (has_filter) {
        RETURN_IF_ERROR(_read_range(_lazy_column_indices, post_filter_range, &post_filter, &lazy_chunk, true));
        lazy_chunk->filter_range(post_filter, 0, post_filter_range.span_size());
    } else {
        RETURN_IF_ERROR(_read_range(_lazy_column_indices, full_range, nullptr, &lazy_chunk, true));
    }
    if (lazy_chunk->num_rows() != active_chunk->num_rows()) {
        return Status::InternalError(fmt::format("Unmatched row count, active_rows={}, lazy_rows={}",
                                                 active_chunk->num_rows(), lazy_chunk->num_rows()));
    }
    active_chunk->merge(std::move(*lazy_chunk));
    return Status::OK();
}

// ── 7. Emit physical columns ──────

Status GroupReader::_emit_physical_columns(ChunkPtr& active_chunk, ChunkPtr* dst) {
    for (const auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id();
        // Skip virtual projection slots: handled by emit_projections.
        if (_variant->has_projections() && _variant->is_virtual_slot(slot_id)) continue;
        RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*dst)->get_column_by_slot_id(slot_id),
                                                                  active_chunk->get_column_by_slot_id(slot_id)));
    }
    if (_param.reserved_field_slots != nullptr) {
        for (const auto* slot : *_param.reserved_field_slots) {
            SlotId slot_id = slot->id();
            RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*dst)->get_column_by_slot_id(slot_id),
                                                                      active_chunk->get_column_by_slot_id(slot_id)));
        }
    }
    return Status::OK();
}

void GroupReader::_rebuild_column_read_order_ctx() {
    std::unordered_map<int, size_t> col_cost;
    size_t all_cost = 0;
    for (int col_idx : _active_column_indices) {
        size_t flat_size = _param.read_cols[col_idx].slot_type().get_flat_size();
        col_cost[col_idx] = flat_size;
        all_cost += flat_size;
    }
    _column_read_order_ctx =
            std::make_unique<ColumnReadOrderCtx>(_active_column_indices, all_cost, std::move(col_cost));
}

// ── _read_range / _read_range_round_by_round ──────

Status GroupReader::_read_range(const std::vector<int>& read_columns, const Range<uint64_t>& range,
                                const Filter* filter, ChunkPtr* chunk, bool ignore_reserved_field) {
    if (read_columns.empty() && _param.reserved_field_slots == nullptr) {
        return Status::OK();
    }
    if (!ignore_reserved_field && _param.reserved_field_slots != nullptr) {
        for (const auto& slot : *_param.reserved_field_slots) {
            SlotId slot_id = slot->id();
            RETURN_IF_ERROR(
                    _column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id)));
        }
    }

    for (int col_idx : read_columns) {
        auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id();
        RETURN_IF_ERROR(_column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id)));
    }

    return Status::OK();
}

StatusOr<size_t> GroupReader::_read_range_round_by_round(const Range<uint64_t>& range, Filter* filter,
                                                         ChunkPtr* chunk) {
    const std::vector<int>& read_order = _column_read_order_ctx->get_column_read_order();
    size_t round_cost = 0;
    double first_selectivity = -1;
    DeferOp defer([&]() { _column_read_order_ctx->update_ctx(round_cost, first_selectivity); });
    size_t hit_count = 0;

    if (_param.reserved_field_slots != nullptr) {
        for (const auto* slot : *_param.reserved_field_slots) {
            SlotId slot_id = slot->id();
            RETURN_IF_ERROR(
                    _column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id)));
            if (_left_no_dict_filter_conjuncts_by_slot.find(slot_id) != _left_no_dict_filter_conjuncts_by_slot.end()) {
                SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
                std::vector<ExprContext*> ctxs = _left_no_dict_filter_conjuncts_by_slot.at(slot_id);
                auto temp_chunk = std::make_shared<Chunk>();
                temp_chunk->columns().reserve(1);
                ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_id);
                temp_chunk->append_column(column, slot_id);
                ASSIGN_OR_RETURN(hit_count, ExecNode::eval_conjuncts_into_filter(ctxs, temp_chunk.get(), filter));
                if (hit_count == 0) {
                    break;
                }
            }
        }
    }
    for (int col_idx : read_order) {
        auto& column = _param.read_cols[col_idx];
        round_cost += _column_read_order_ctx->get_column_cost(col_idx);
        SlotId slot_id = column.slot_id();
        RETURN_IF_ERROR(_column_readers[slot_id]->read_range(range, filter, (*chunk)->get_column_by_slot_id(slot_id)));

        if (std::find(_dict_column_indices.begin(), _dict_column_indices.end(), col_idx) !=
            _dict_column_indices.end()) {
            SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
            SCOPED_RAW_TIMER(&_param.stats->group_dict_filter_ns);
            for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
                RETURN_IF_ERROR(_column_readers[slot_id]->filter_dict_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                             filter, sub_field_path, 0));
                hit_count = SIMD::count_nonzero(*filter);
                if (hit_count == 0) {
                    return hit_count;
                }
            }
        }

        if (_left_no_dict_filter_conjuncts_by_slot.find(slot_id) != _left_no_dict_filter_conjuncts_by_slot.end()) {
            SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);
            std::vector<ExprContext*> ctxs = _left_no_dict_filter_conjuncts_by_slot.at(slot_id);
            auto temp_chunk = std::make_shared<Chunk>();
            temp_chunk->columns().reserve(1);
            ColumnPtr& column = (*chunk)->get_column_by_slot_id(slot_id);
            temp_chunk->append_column(column, slot_id);
            ASSIGN_OR_RETURN(hit_count, ExecNode::eval_conjuncts_into_filter(ctxs, temp_chunk.get(), filter));
            if (hit_count == 0) {
                break;
            }
        }
        first_selectivity = first_selectivity < 0 ? hit_count * 1.0 / filter->size() : first_selectivity;
    }

    return hit_count;
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
<<<<<<< HEAD
    // ColumnReaderOptions is used by all column readers in one row group
=======
    _global_dict_applied_in_group = false;
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.file_meta_data = _param.file_metadata;
    opts.timezone = _param.timezone;
    opts.case_sensitive = _param.case_sensitive;
    opts.use_file_pagecache = _param.use_file_pagecache;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata;
    opts.first_row_index = _row_group_first_row;
    opts.modification_time = _param.modification_time;
    opts.file_size = _param.file_size;
    opts.datacache_options = _param.datacache_options;
<<<<<<< HEAD
    for (const auto& column : _param.read_cols) {
=======

    // Setup variant handler (idempotent: no-op when no variant virtual columns exist).
    RETURN_IF_ERROR(_variant->setup_readers());

    for (const auto& column : _param.read_cols) {
        // Extended variant virtual columns are handled by _variant->setup_readers above.
        if (column.is_extended_variant_virtual) continue;
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
        ASSIGN_OR_RETURN(ColumnReaderPtr column_reader, _create_column_reader(column));
        _column_readers[column.slot_id()] = std::move(column_reader);
    }

    // Register zone-map readers AFTER physical column readers are created.
    _variant->register_zone_map_readers();

    // create for partition values
    if (_param.partition_columns != nullptr && _param.partition_values != nullptr) {
        for (size_t i = 0; i < _param.partition_columns->size(); i++) {
            const auto& column = (*_param.partition_columns)[i];
            const auto* slot_desc = column.slot_desc;
            const auto value = (*_param.partition_values)[i];
            _column_readers.emplace(slot_desc->id(), std::make_unique<FixedValueColumnReader>(value->get(0)));
        }
    }

    // create for not existed column
    if (_param.not_existed_slots != nullptr) {
        for (size_t i = 0; i < _param.not_existed_slots->size(); i++) {
            const auto* slot = (*_param.not_existed_slots)[i];
            _column_readers.emplace(slot->id(), std::make_unique<FixedValueColumnReader>(kNullDatum));
        }
    }

    if (_param.reserved_field_slots != nullptr && !_param.reserved_field_slots->empty()) {
        bool use_legacy_lookup_row_id =
                std::any_of(_param.reserved_field_slots->begin(), _param.reserved_field_slots->end(),
                            [](const SlotDescriptor* slot) {
                                return slot->col_name() == "_row_source_id" || slot->col_name() == "_scan_range_id";
                            });
        for (const auto* slot : *_param.reserved_field_slots) {
            if (slot->col_name() == HdfsScanner::ICEBERG_ROW_ID) {
                ASSIGN_OR_RETURN(auto reader,
                                 _create_reserved_iceberg_column_reader(slot, HdfsScanner::ICEBERG_ROW_ID_COLUMN_ID));
                std::optional<int64_t> first_row_id =
                        _param.scan_range != nullptr && _param.scan_range->__isset.first_row_id
                                ? std::optional<int64_t>(_row_group_first_row_id)
                                : use_legacy_lookup_row_id ? std::optional<int64_t>(_row_group_first_row_id)
                                                           : std::nullopt;
                ColumnReaderPtr row_id_reader =
                        reader != nullptr ? std::make_unique<IcebergRowIdReader>(std::move(reader), first_row_id)
                                          : std::make_unique<IcebergRowIdReader>(first_row_id);
                _column_readers.emplace(slot->id(), std::move(row_id_reader));
            } else if (slot->col_name() == HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER) {
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
<<<<<<< HEAD
=======

    if (_param.stats != nullptr) {
        _param.stats->global_dict_total_row_groups++;
        if (_global_dict_applied_in_group) {
            _param.stats->global_dict_applied_row_groups++;
        }
    }

>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
    return Status::OK();
}

StatusOr<ColumnReaderPtr> GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(column.idx_in_parquet);
    {
<<<<<<< HEAD
        if (column.t_lake_schema_field == nullptr) {
=======
        if (column.slot_type().type == LogicalType::TYPE_VARIANT && schema_node != nullptr &&
            schema_node->type == ColumnType::STRUCT) {
            // Physical VARIANT columns use _get_variant_shredded_hints; this path
            // is for non-virtual VARIANT columns that appear directly in the SELECT list.
            VariantShreddedReadHints hints =
                    build_variant_shredded_hints(_param.column_access_paths, column.slot_desc->col_name());
            ASSIGN_OR_RETURN(column_reader, ColumnReaderFactory::create_variant_column_reader(_column_reader_opts,
                                                                                              schema_node, hints));
        } else if (column.t_lake_schema_field == nullptr) {
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type()));
        } else {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type(),
                                                         column.t_lake_schema_field));
        }
        if (_param.global_dictmaps->contains(column.slot_id())) {
            ASSIGN_OR_RETURN(
                    column_reader,
                    ColumnReaderFactory::create(std::move(column_reader), _param.global_dictmaps->at(column.slot_id()),
                                                column.slot_id(), _row_group_metadata->num_rows));
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
<<<<<<< HEAD
=======
    RETURN_IF_ERROR(_variant->prepare_hidden_readers());
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
<<<<<<< HEAD
=======

    // ── Step 1: Classify physical read_cols into active / lazy ───────────────
    std::unordered_set<SlotId> deferred_physical_source_slots = _variant->deferred_conjunct_physical_source_slots();
    _variant->collect_deferred_conjuncts();

>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
    int read_col_idx = 0;

    for (auto& column : _param.read_cols) {
<<<<<<< HEAD
=======
        if (column.is_extended_variant_virtual) {
            ++read_col_idx;
            continue;
        }
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
        SlotId slot_id = column.slot_id();
        if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
            for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
                std::vector<std::string> sub_field_path;
                if (_try_to_use_dict_filter(column, ctx, sub_field_path, column.decode_needed)) {
                    _use_as_dict_filter_column(read_col_idx, slot_id, sub_field_path);
                } else {
<<<<<<< HEAD
                    _left_conjunct_ctxs.emplace_back(ctx);
                    // used for struct col, some dict filter conjunct pushed down to leaf some left
                    if (_left_no_dict_filter_conjuncts_by_slot.find(slot_id) ==
                        _left_no_dict_filter_conjuncts_by_slot.end()) {
                        _left_no_dict_filter_conjuncts_by_slot.insert({slot_id, std::vector<ExprContext*>{ctx}});
                    } else {
                        _left_no_dict_filter_conjuncts_by_slot[slot_id].emplace_back(ctx);
                    }
                }
            }
            _active_column_indices.emplace_back(read_col_idx);
=======
                    _left_conjunct_ctxs.push_back(ctx);
                    _left_no_dict_filter_conjuncts_by_slot[slot_id].push_back(ctx);
                }
            }
            _active_column_indices.push_back(read_col_idx);
        } else if (config::parquet_late_materialization_enable && deferred_physical_source_slots.count(slot_id) == 0) {
            _lazy_column_indices.push_back(read_col_idx);
            _column_readers[slot_id]->set_can_lazy_decode(true);
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
        } else {
            if (config::parquet_late_materialization_enable) {
                _lazy_column_indices.emplace_back(read_col_idx);
                _column_readers[slot_id]->set_can_lazy_decode(true);
            } else {
                _active_column_indices.emplace_back(read_col_idx);
            }
        }
        ++read_col_idx;
    }

    bool has_reserved_field_filter = false;
    if (_param.reserved_field_slots != nullptr) {
        for (auto* slot : *_param.reserved_field_slots) {
            SlotId slot_id = slot->id();
            if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
                    if (_left_no_dict_filter_conjuncts_by_slot.find(slot_id) ==
                        _left_no_dict_filter_conjuncts_by_slot.end()) {
                        _left_no_dict_filter_conjuncts_by_slot.insert({slot_id, std::vector<ExprContext*>{ctx}});
                    } else {
                        _left_no_dict_filter_conjuncts_by_slot[slot_id].emplace_back(ctx);
                    }
                }
                has_reserved_field_filter = true;
            }
        }
    }

<<<<<<< HEAD
    std::unordered_map<int, size_t> col_cost;
    size_t all_cost = 0;
    for (int col_idx : _active_column_indices) {
        size_t flat_size = _param.read_cols[col_idx].slot_type().get_flat_size();
        col_cost.insert({col_idx, flat_size});
        all_cost += flat_size;
    }

    _column_read_order_ctx =
            std::make_unique<ColumnReadOrderCtx>(_active_column_indices, all_cost, std::move(col_cost));

    if (_active_column_indices.empty() && !has_reserved_field_filter) {
        _active_column_indices.swap(_lazy_column_indices);
    }
}

=======
    // ── Step 3: Build ColumnReadOrderCtx ─────────────────────────────────────
    {
        std::unordered_map<int, size_t> col_cost;
        size_t all_cost = 0;
        for (int col_idx : _active_column_indices) {
            size_t flat_size = _param.read_cols[col_idx].slot_type().get_flat_size();
            col_cost[col_idx] = flat_size;
            all_cost += flat_size;
        }
        _column_read_order_ctx =
                std::make_unique<ColumnReadOrderCtx>(_active_column_indices, all_cost, std::move(col_cost));
    }

    // ── Step 4: Classify hidden variant sources (active vs lazy) ─────────────
    _variant->classify_hidden_sources();

    // ── Step 5: Build unified SlotId lists for _create_read_chunk ────────────
    for (int idx : _active_column_indices) {
        _active_slot_ids.push_back(_param.read_cols[idx].slot_id());
    }
    for (SlotId slot_id : _variant->active_hidden_slot_ids()) {
        _active_slot_ids.push_back(slot_id);
    }
    for (int idx : _lazy_column_indices) {
        _lazy_slot_ids.push_back(_param.read_cols[idx].slot_id());
    }

    // ── Step 6: Promote all lazy columns to active when active_chunk is empty ─
    if (_active_slot_ids.empty() && !has_reserved_field_filter) {
        _active_column_indices.swap(_lazy_column_indices);
        _active_slot_ids.swap(_lazy_slot_ids);
        _variant->promote_lazy_to_active();
    }
}

// ── Dict-filter support ─────────────────────────────────────────────────────

>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
bool GroupReader::_try_to_use_dict_filter(const GroupReaderParam::Column& column, ExprContext* ctx,
                                          std::vector<std::string>& sub_field_path, bool is_decode_needed) {
    const Expr* root_expr = ctx->root();
    std::vector<std::vector<std::string>> subfields;
    root_expr->get_subfields(&subfields);

    for (int i = 1; i < subfields.size(); i++) {
        if (subfields[i] != subfields[0]) {
            return false;
        }
    }

    if (subfields.size() != 0) {
        sub_field_path = subfields[0];
    }

    if (_column_readers[column.slot_id()]->try_to_use_dict_filter(ctx, is_decode_needed, column.slot_id(),
                                                                  sub_field_path, 0)) {
        return true;
    } else {
        return false;
    }
}

<<<<<<< HEAD
ChunkPtr GroupReader::_create_read_chunk(const std::vector<int>& column_indices, bool ignore_reserved_fields) {
    auto chunk = std::make_shared<Chunk>();
    chunk->columns().reserve(column_indices.size());
    for (auto col_idx : column_indices) {
        SlotId slot_id = _param.read_cols[col_idx].slot_id();
        ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot_id);
        chunk->append_column(column, slot_id);
    }
    if (!ignore_reserved_fields && _param.reserved_field_slots != nullptr) {
        for (const auto* slot : *_param.reserved_field_slots) {
            ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot->id());
            chunk->append_column(column, slot->id());
        }
    }
    return chunk;
}

void GroupReader::collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                    ColumnIOTypeFlags types) {
    int64_t end = 0;
    // collect io of active column
    for (const auto& index : _active_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id();
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, types, true);
    }

    // collect io of lazy column
    for (const auto& index : _lazy_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id();
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, types, false);
    }
    *end_offset = end;
}

Status GroupReader::_init_read_chunk() {
    std::vector<SlotDescriptor*> read_slots;
    for (const auto& column : _param.read_cols) {
        read_slots.emplace_back(column.slot_desc);
    }
    if (_param.reserved_field_slots != nullptr) {
        for (auto* slot : *_param.reserved_field_slots) {
            read_slots.push_back(slot);
        }
    }
    size_t chunk_size = _param.chunk_size;
    ASSIGN_OR_RETURN(_read_chunk, ChunkHelper::new_chunk_checked(read_slots, chunk_size));
    return Status::OK();
}

=======
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
void GroupReader::_use_as_dict_filter_column(int col_idx, SlotId slot_id, std::vector<std::string>& sub_field_path) {
    _dict_column_indices.emplace_back(col_idx);
    if (_dict_column_sub_field_paths.find(col_idx) == _dict_column_sub_field_paths.end()) {
        _dict_column_sub_field_paths.insert({col_idx, std::vector<std::vector<std::string>>({sub_field_path})});
    } else {
        _dict_column_sub_field_paths[col_idx].emplace_back(sub_field_path);
    }
}

Status GroupReader::_rewrite_conjunct_ctxs_to_predicates(bool* is_group_filtered) {
    for (int col_idx : _dict_column_indices) {
        const auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id();
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            if (*is_group_filtered) {
                return Status::OK();
            }
            RETURN_IF_ERROR(
                    _column_readers[slot_id]->rewrite_conjunct_ctxs_to_predicate(is_group_filtered, sub_field_path, 0));
        }
    }

    return Status::OK();
}

StatusOr<bool> GroupReader::_filter_chunk_with_dict_filter(ChunkPtr* chunk, Filter* filter) {
    if (_dict_column_indices.size() == 0) {
        return false;
    }
    for (int col_idx : _dict_column_indices) {
        const auto& column = _param.read_cols[col_idx];
        SlotId slot_id = column.slot_id();
        for (const auto& sub_field_path : _dict_column_sub_field_paths[col_idx]) {
            RETURN_IF_ERROR(_column_readers[slot_id]->filter_dict_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                         filter, sub_field_path, 0));
        }
    }
    return true;
}

<<<<<<< HEAD
Status GroupReader::_fill_dst_chunk(ChunkPtr& read_chunk, ChunkPtr* chunk) {
    read_chunk->check_or_die();
    for (const auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id();
        RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                  read_chunk->get_column_by_slot_id(slot_id)));
    }
    if (_param.reserved_field_slots != nullptr) {
        for (const auto* slot : *_param.reserved_field_slots) {
            SlotId slot_id = slot->id();
            RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                      read_chunk->get_column_by_slot_id(slot_id)));
        }
    }
    read_chunk->check_or_die();
=======
// ── Read chunk management ───────────────────────────────────────────────────

ChunkPtr GroupReader::_create_read_chunk(const std::vector<SlotId>& slot_ids, bool include_reserved_fields) {
    auto chunk = std::make_shared<Chunk>();
    chunk->columns().reserve(slot_ids.size());
    for (SlotId slot_id : slot_ids) {
        ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot_id);
        chunk->append_column(column, slot_id);
    }
    if (include_reserved_fields && _param.reserved_field_slots != nullptr) {
        for (const auto* slot : *_param.reserved_field_slots) {
            ColumnPtr& column = _read_chunk->get_column_by_slot_id(slot->id());
            chunk->append_column(column, slot->id());
        }
    }
    return chunk;
}

Status GroupReader::_init_read_chunk() {
    std::vector<SlotDescriptor*> read_slots;
    read_slots.reserve(_param.read_cols.size());
    for (const auto& column : _param.read_cols) {
        read_slots.emplace_back(column.slot_desc);
    }
    if (_param.reserved_field_slots != nullptr) {
        for (auto* slot : *_param.reserved_field_slots) {
            read_slots.push_back(slot);
        }
    }
    size_t chunk_size = _param.chunk_size;
    ASSIGN_OR_RETURN(_read_chunk, RuntimeChunkHelper::new_chunk_checked(read_slots, chunk_size));
    _variant->init_read_chunk_slots();
>>>>>>> 5c371c2226 ([Refactor] Extract VariantProjectionHandler from GroupReader to decouple variant projection logic (#74426))
    return Status::OK();
}

// ── IO range collection ─────────────────────────────────────────────────────

void GroupReader::collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                    ColumnIOTypeFlags types) {
    int64_t end = 0;
    for (const auto& index : _active_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id();
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, types, true);
    }

    for (const auto& index : _lazy_column_indices) {
        const auto& column = _param.read_cols[index];
        SlotId slot_id = column.slot_id();
        _column_readers[slot_id]->collect_column_io_range(ranges, &end, types, false);
    }
    _variant->collect_io_ranges(ranges, &end, types);
    deduplicate_io_ranges(ranges);
    *end_offset = end;
}

} // namespace starrocks::parquet
