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

#include "agent/master_info.h"
#include "base/concurrency/stopwatch.hpp"
#include "base/simd/simd.h"
#include "base/time/timezone_utils.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/variant_column.h"
#include "column/variant_converter.h"
#include "column/variant_path_parser.h"
#include "common/config_scan_io_fwd.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/variant_path_reader.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/iceberg_row_id_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_pos_reader.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/row_source_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "types/type_descriptor.h"
#include "utils.h"

namespace starrocks::parquet {

namespace {

// Deduplicate exact-duplicate IO ranges collected from multiple column readers.
//
// VariantColumnReader::collect_column_io_range walks both the top-level field readers
// (_top_level.value_reader, _top_level.metadata_reader, etc.) and the shredded-field
// subtree.  A shredded field's value_reader may point to the same underlying parquet
// column chunk as the top-level value_reader, causing the same (offset, size) pair to
// be registered more than once.  When that happens the two entries are collapsed into
// one, with is_active set to the logical OR so that a range used by either an active
// or a lazy reader is treated as active.
//
// Note: this only handles *exact* duplicates (identical offset and size).  True byte-range
// overlaps are not expected because the parquet format guarantees that column chunks for
// different columns occupy non-overlapping byte ranges within the file.
void deduplicate_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges) {
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

StatusOr<ColumnPtr> build_exact_typed_variant_projection(const VariantColumn* variant_column,
                                                         const ColumnPtr& variant_src, const VariantPath& path,
                                                         const TypeDescriptor& target_type) {
    VariantPathReader reader;
    reader.prepare(variant_column, &path);
    if (!reader.is_typed_exact()) {
        return Status::NotFound("variant path is not an exact typed leaf");
    }
    if (reader.typed_type_desc() != target_type) {
        return Status::NotFound("variant typed leaf type does not match target slot type");
    }

    const size_t num_rows = variant_src->size();
    const Column* typed_col = reader.typed_column();

    // Early exit: const variant whose single row is null → broadcast null to all rows.
    if (variant_src->is_constant() && variant_src->is_null(0)) {
        auto all_null = ColumnHelper::create_column(target_type, true);
        all_null->append_nulls(num_rows);
        return all_null;
    }

    // Normalise: expand a const variant source so both typed_col and variant_src have
    // num_rows rows, letting the null-merge and data-clone logic below work uniformly.
    ColumnPtr expanded_typed;
    if (variant_src->is_constant()) {
        // typed_col has 1 row (the single VariantColumn row); replicate it to num_rows.
        expanded_typed = typed_col->clone();
        expanded_typed->as_mutable_ptr()->assign(num_rows, 0);
    }
    const Column* eff_typed = variant_src->is_constant() ? expanded_typed.get() : typed_col;
    if (eff_typed->size() != num_rows) {
        return Status::InternalError(strings::Substitute(
                "variant typed column size mismatch: typed_size=$0, variant_src_size=$1", eff_typed->size(), num_rows));
    }

    // Merge null masks: result is null when either the outer variant or the typed leaf
    // is null.  Outer nulls only exist for non-const variant_src (const-null was handled
    // above; const-non-null has no outer null mask to propagate).
    const bool has_outer_nulls = !variant_src->is_constant() && variant_src->is_nullable() &&
                                 down_cast<const NullableColumn*>(variant_src.get())->has_null();
    const bool has_typed_nulls = eff_typed->is_nullable() && down_cast<const NullableColumn*>(eff_typed)->has_null();

    auto result_null = NullColumn::create(num_rows, 0);
    NullData& result_null_data = result_null->get_data();

    if (has_outer_nulls && has_typed_nulls) {
        const auto outer = down_cast<const NullableColumn*>(variant_src.get())->immutable_null_column_data();
        const auto typed = down_cast<const NullableColumn*>(eff_typed)->immutable_null_column_data();
        for (size_t i = 0; i < num_rows; ++i) {
            result_null_data[i] = outer[i] | typed[i];
        }
    } else if (has_outer_nulls) {
        const auto outer = down_cast<const NullableColumn*>(variant_src.get())->immutable_null_column_data();
        std::copy(outer.begin(), outer.end(), result_null_data.begin());
    } else if (has_typed_nulls) {
        const auto typed = down_cast<const NullableColumn*>(eff_typed)->immutable_null_column_data();
        std::copy(typed.begin(), typed.end(), result_null_data.begin());
    }
    // else: no nulls anywhere — result_null stays all-zero.

    auto result_data = ColumnHelper::get_data_column(eff_typed)->clone();
    auto result = NullableColumn::create(std::move(result_data), std::move(result_null));
    result->update_has_null();
    return result;
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> build_variant_projection_column(const VariantColumn* variant_column, const ColumnPtr& variant_src,
                                                    const VariantPath& path, const cctz::time_zone& zone) {
    const size_t num_rows = variant_src->size();

    ColumnBuilder<ResultType> builder(num_rows);
    VariantPathReader reader;
    reader.prepare(variant_column, &path);

    // Hoist is_constant() out of the loop: it does not change per row.
    const bool src_is_const = variant_src->is_constant();
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_src->is_null(row)) {
            builder.append_null();
            continue;
        }
        size_t variant_row = src_is_const ? 0 : row;
        VariantReadResult read = reader.read_row(variant_row);
        if (read.state != VariantReadState::kValue) {
            builder.append_null();
            continue;
        }
        auto cast_status = VariantRowConverter::cast_to<ResultType, false>(read.value.as_ref(), zone, builder);
        RETURN_IF_ERROR(cast_status);
    }

    return builder.build(false);
}

StatusOr<ColumnPtr> project_variant_leaf_column(const ColumnPtr& variant_src, const VariantPath& path,
                                                const TypeDescriptor& target_type, const cctz::time_zone& zone) {
    auto* variant_column = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(variant_src.get()));
    if (variant_column == nullptr) {
        return Status::InternalError("variant source column is invalid");
    }

    auto exact_typed_result = build_exact_typed_variant_projection(variant_column, variant_src, path, target_type);
    if (exact_typed_result.ok()) {
        return exact_typed_result;
    }

    switch (target_type.type) {
    case TYPE_BOOLEAN:
        return build_variant_projection_column<TYPE_BOOLEAN>(variant_column, variant_src, path, zone);
    case TYPE_TINYINT:
        return build_variant_projection_column<TYPE_TINYINT>(variant_column, variant_src, path, zone);
    case TYPE_SMALLINT:
        return build_variant_projection_column<TYPE_SMALLINT>(variant_column, variant_src, path, zone);
    case TYPE_INT:
        return build_variant_projection_column<TYPE_INT>(variant_column, variant_src, path, zone);
    case TYPE_BIGINT:
        return build_variant_projection_column<TYPE_BIGINT>(variant_column, variant_src, path, zone);
    case TYPE_LARGEINT:
        return build_variant_projection_column<TYPE_LARGEINT>(variant_column, variant_src, path, zone);
    case TYPE_FLOAT:
        return build_variant_projection_column<TYPE_FLOAT>(variant_column, variant_src, path, zone);
    case TYPE_DOUBLE:
        return build_variant_projection_column<TYPE_DOUBLE>(variant_column, variant_src, path, zone);
    case TYPE_VARCHAR:
        return build_variant_projection_column<TYPE_VARCHAR>(variant_column, variant_src, path, zone);
    case TYPE_DATE:
        return build_variant_projection_column<TYPE_DATE>(variant_column, variant_src, path, zone);
    case TYPE_DATETIME:
        return build_variant_projection_column<TYPE_DATETIME>(variant_column, variant_src, path, zone);
    case TYPE_TIME:
        return build_variant_projection_column<TYPE_TIME>(variant_column, variant_src, path, zone);
    case TYPE_VARIANT:
        return build_variant_projection_column<TYPE_VARIANT>(variant_column, variant_src, path, zone);
    default:
        return Status::NotSupported("unsupported variant virtual column target type");
    }
}

StatusOr<ColumnPtr> get_variant_projection_source_column(const ChunkPtr& output_chunk, const ChunkPtr& read_chunk,
                                                         const ChunkPtr& hidden_read_chunk, SlotId slot_id) {
    if (output_chunk->is_slot_exist(slot_id)) {
        return output_chunk->get_column_by_slot_id(slot_id);
    }
    if (read_chunk->is_slot_exist(slot_id)) {
        return read_chunk->get_column_by_slot_id(slot_id);
    }
    if (!hidden_read_chunk->is_slot_exist(slot_id)) {
        return Status::InternalError(
                strings::Substitute("variant virtual column source slot $0 not found in any chunk", slot_id));
    }
    return hidden_read_chunk->get_column_by_slot_id(slot_id);
}

bool collect_variant_leaf_paths(const ColumnAccessPath* node, std::vector<VariantSegment>* segments,
                                std::vector<std::string>* shredded_paths) {
    if (!node->is_field() || node->path().empty()) {
        return false;
    }

    segments->emplace_back(VariantSegment::make_object(node->path()));
    if (node->children().empty()) {
        VariantPath path(*segments);
        auto shredded_path = path.to_shredded_path();
        if (!shredded_path.has_value()) {
            segments->pop_back();
            return false;
        }
        shredded_paths->emplace_back(std::move(*shredded_path));
        segments->pop_back();
        return true;
    }

    bool valid = true;
    for (const auto& child : node->children()) {
        valid = collect_variant_leaf_paths(child.get(), segments, shredded_paths) && valid;
    }
    segments->pop_back();
    return valid;
}

} // namespace

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row)
        : _row_group_first_row(row_group_first_row), _skip_rows_ctx(std::move(skip_rows_ctx)), _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
}

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row, int64_t row_group_first_row_id)
        : _row_group_first_row(row_group_first_row),
          _row_group_first_row_id(row_group_first_row_id),
          _skip_rows_ctx(std::move(skip_rows_ctx)),
          _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
}

GroupReader::~GroupReader() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    // If GroupReader is filtered by statistics, it's _has_prepared = false
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
        // Hidden variant source readers are not in _column_readers; apply the same
        // page-index range restriction so they decode only the surviving rows.
        for (const auto& [name, hidden_source] : _hidden_variant_sources) {
            hidden_source.reader->select_offset_index(_range, _row_group_first_row);
        }
    }

    // if coalesce read enabled, we have to
    // 1. allocate shared buffered input stream and
    // 2. collect io ranges of every row group reader.
    // 3. set io ranges to the stream.
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

// Three-chunk model used in get_next:
//
//  _read_chunk  (member)
//    The column backing store.  Allocated once in _init_read_chunk() and reset each
//    iteration.  Holds two categories of columns:
//      • Physical slots  – TYPE_* columns for every entry in _param.read_cols plus
//        _param.reserved_field_slots.  These objects are SHARED with active_chunk and
//        lazy_chunk via _create_read_chunk(), so filtering or resetting either view
//        chunk operates on the same underlying column storage.
//      • Hidden variant sources – TYPE_VARIANT columns keyed by synthetic *negative*
//        slot ids (one per _hidden_variant_sources entry).  These are VARIANT columns
//        that serve only as projection sources for virtual columns; they are never
//        directly returned to the caller and are not visible in active_chunk.
//
//  active_chunk  (local)
//    A lightweight VIEW of _read_chunk created by _create_read_chunk(_active_column_indices).
//    Contains only the active physical columns (those not deferred for lazy read) plus
//    reserved_field_slots.  Because its column objects are shared with _read_chunk,
//    active_chunk->filter() / active_chunk->reset() also modifies those columns in
//    _read_chunk.
//
//  lazy_chunk  (local, created inside loop when needed)
//    A lightweight VIEW of _read_chunk created by _create_read_chunk(_lazy_column_indices).
//    Same sharing semantics as active_chunk.  After reading, it is merged into
//    active_chunk so that active_chunk becomes the single source for _fill_dst_chunk.
//
//  *chunk  (output)
//    The caller-provided destination chunk.  May already contain extra columns
//    (e.g. partition columns) before we are called.  We call _fill_dst_chunk to copy
//    physical columns from active_chunk (= the shared _read_chunk slots) into *chunk
//    and to compute virtual projections.  Because *chunk->num_rows() returns the size
//    of its *first* column, and that first column may be a partition column appended
//    by the caller, we must not use (*chunk)->num_rows() as the authoritative row
//    count – use active_chunk->num_rows() instead.

Status GroupReader::get_next(ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    // active_chunk is a shared-reference VIEW of _read_chunk physical columns.
    // See the three-chunk model comment above _fill_dst_chunk declaration.
    _read_chunk->reset();
    ChunkPtr active_chunk = _create_read_chunk(_active_column_indices, false);
    // Loop until we find a range with surviving rows, skipping ranges filtered entirely
    // by deletion bitmap, predicate pushdown, or deferred variant virtual conjuncts.
    while (true) {
        if (!_range_iter.has_more()) {
            *row_count = 0;
            return Status::EndOfFile("");
        }

        auto r = _range_iter.next(*row_count);
        auto count = r.span_size();
        _param.stats->raw_rows_read += count;

        // Reset per-iteration scratch buffers.
        active_chunk->reset();

        bool has_filter = false;
        Filter chunk_filter(count, 1);

        // ── Phase 1: row-id (deletion bitmap) filter ──────────────────────────
        if (nullptr != _skip_rows_ctx && _skip_rows_ctx->has_skip_rows()) {
            SCOPED_RAW_TIMER(&_param.stats->build_rowid_filter_ns);
            ASSIGN_OR_RETURN(has_filter,
                             _skip_rows_ctx->deletion_bitmap->fill_filter(r.begin(), r.end(), chunk_filter));
            if (SIMD::count_nonzero(chunk_filter.data(), count) == 0) {
                continue;
            }
        }

        // ── Phase 2: active columns (with optional predicate pushdown) ─────────
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

        // ── Phase 3: secondary post-filter reads ──────────────────────────────
        // Both lazy columns and hidden variant sources are read after the active-column
        // filter so we only decode rows that survived predicate pushdown.
        //
        // Lazy columns   → merged into active_chunk (physical slots, used directly by
        //                  _fill_dst_chunk via the read_chunk argument).
        // Hidden variant → written into _read_chunk under synthetic negative slot ids.
        //                  They are VARIANT columns needed only as sources for virtual
        //                  projection; they are not in the user's SELECT list and are
        //                  therefore not part of active_chunk or *chunk.
        //
        // Both share the same post-filter range / filter vector, computed once here.
        // Declared unconditionally so the lazy and hidden-variant blocks below can
        // reference them without extra scope nesting; values are only valid when
        // has_filter is true, and both blocks guard their use behind the same check.
        Range<uint64_t> post_filter_range;
        Filter post_filter;
        if (has_filter) {
            post_filter_range = r.filter(&chunk_filter);
            // Active-column filtering already exited early if hit_count==0, so there
            // must be surviving rows at this point.
            DCHECK(post_filter_range.span_size() > 0);
            post_filter = {chunk_filter.begin() + post_filter_range.begin() - r.begin(),
                           chunk_filter.begin() + post_filter_range.end() - r.begin()};
        }

        if (!_lazy_column_indices.empty()) {
            _lazy_column_needed = true;
            ChunkPtr lazy_chunk = _create_read_chunk(_lazy_column_indices, true);
            if (has_filter) {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, post_filter_range, &post_filter, &lazy_chunk, true));
                lazy_chunk->filter_range(post_filter, 0, post_filter_range.span_size());
            } else {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, r, nullptr, &lazy_chunk, true));
            }
            if (lazy_chunk->num_rows() != active_chunk->num_rows()) {
                return Status::InternalError(strings::Substitute("Unmatched row count, active_rows=$0, lazy_rows=$1",
                                                                 active_chunk->num_rows(), lazy_chunk->num_rows()));
            }
            active_chunk->merge(std::move(*lazy_chunk));
        }

        if (!_hidden_variant_sources.empty()) {
            for (const auto& [name, hidden_source] : _hidden_variant_sources) {
                auto& hidden_column = _read_chunk->get_column_by_slot_id(hidden_source.slot_id);
                if (has_filter) {
                    RETURN_IF_ERROR(hidden_source.reader->read_range(post_filter_range, &post_filter, hidden_column));
                    hidden_column->as_mutable_ptr()->filter_range(post_filter, 0, post_filter_range.span_size());
                } else {
                    RETURN_IF_ERROR(hidden_source.reader->read_range(r, nullptr, hidden_column));
                }
            }
        }

        // ── Phase 4: deferred variant virtual conjunct pre-evaluation ─────────
        // Must run BEFORE _fill_dst_chunk so active_chunk is filtered first and
        // remains the single source of truth for row counts.
        {
            ASSIGN_OR_RETURN(bool survived, _apply_deferred_variant_conjuncts(active_chunk, count));
            if (!survived) continue;
        }

        // ── Phase 5: decode + virtual projection → dst chunk ──────────────────
        // active_chunk (and _read_chunk hidden sources) are already filtered.
        // _fill_dst_chunk copies physical columns and re-derives virtual projections
        // from the filtered source data into *chunk.
        // active_chunk carries all rows that survived every filter stage; use it as
        // the row-count source.  Fall back to *chunk only in schema-evolution paths
        // where active_chunk has no physical columns but _fill_dst_chunk backfills
        // default values for missing slots.
        {
            SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
            // row_count must be get from active_chunk. chunk could contains some partition columns
            // which have been filled before, and those partition columns may have different row count
            // with active_chunk. So we can't use (*chunk)->num_rows() as row count.
            *row_count = active_chunk->num_rows();
            RETURN_IF_ERROR(_fill_dst_chunk(active_chunk, chunk));
        }
        break;
    }

    return _range_iter.has_more() ? Status::OK() : Status::EndOfFile("");
}

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
                ASSIGN_OR_RETURN(hit_count,
                                 ChunkPredicateEvaluator::eval_conjuncts_into_filter(ctxs, temp_chunk.get(), filter));
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
            ASSIGN_OR_RETURN(hit_count,
                             ChunkPredicateEvaluator::eval_conjuncts_into_filter(ctxs, temp_chunk.get(), filter));
            if (hit_count == 0) {
                break;
            }
        }
        first_selectivity = first_selectivity < 0 ? hit_count * 1.0 / filter->size() : first_selectivity;
    }

    return hit_count;
}

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

VariantShreddedReadHints GroupReader::_get_variant_shredded_hints(const std::string& column_name) const {
    VariantShreddedReadHints hints;
    if (_param.column_access_paths == nullptr || _param.column_access_paths->empty()) {
        return hints;
    }

    std::unordered_set<std::string> unique_paths;
    for (const auto& access_path : *_param.column_access_paths) {
        if (access_path == nullptr || access_path->path() != column_name) {
            continue;
        }
        if (access_path->children().empty()) {
            hints.shredded_paths.clear();
            return hints;
        }

        std::vector<VariantSegment> segments;
        for (const auto& child : access_path->children()) {
            size_t old_size = hints.shredded_paths.size();
            if (!collect_variant_leaf_paths(child.get(), &segments, &hints.shredded_paths)) {
                hints.shredded_paths.clear();
                return hints;
            }
            for (size_t i = old_size; i < hints.shredded_paths.size(); ++i) {
                if (!unique_paths.emplace(hints.shredded_paths[i]).second) {
                    hints.shredded_paths[i].clear();
                }
            }
        }
    }

    hints.shredded_paths.erase(std::remove_if(hints.shredded_paths.begin(), hints.shredded_paths.end(),
                                              [](const std::string& path) { return path.empty(); }),
                               hints.shredded_paths.end());

    // Remove paths that are strict prefixes of another hint path.
    // e.g. if both "a.b" and "a.b.c" are collected, "a.b" is redundant.
    // A shredded path p is a prefix of q if q starts with p followed by '.' or '['.
    auto is_prefix_of_another = [&](const std::string& p) {
        for (const auto& q : hints.shredded_paths) {
            if (q.size() <= p.size()) continue;
            if (q[p.size()] != '.' && q[p.size()] != '[') continue;
            if (q.compare(0, p.size(), p) == 0) return true;
        }
        return false;
    };
    hints.shredded_paths.erase(
            std::remove_if(hints.shredded_paths.begin(), hints.shredded_paths.end(), is_prefix_of_another),
            hints.shredded_paths.end());
    return hints;
}

Status GroupReader::_create_column_readers() {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    // ColumnReaderOptions is used by all column readers in one row group
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

    std::unordered_map<std::string, SlotId> physical_variant_slots_by_name;
    for (const auto& column : _param.read_cols) {
        if (!column.is_extended_variant_virtual && column.slot_type().type == LogicalType::TYPE_VARIANT) {
            physical_variant_slots_by_name.emplace(column.slot_desc->col_name(), column.slot_id());
        }
    }

    for (const auto& column : _param.read_cols) {
        if (column.is_extended_variant_virtual) {
            ASSIGN_OR_RETURN(auto parsed_path, VariantPathParser::parse_shredded_path(
                                                       std::string_view(column.variant_virtual_leaf_path)));
            VariantVirtualProjection projection{
                    .parsed_path = std::move(parsed_path), .target_type = column.slot_type(), .source_slot_id = 0};
            auto physical_it = physical_variant_slots_by_name.find(column.source_variant_column_name);
            if (physical_it != physical_variant_slots_by_name.end()) {
                projection.source_slot_id = physical_it->second;
                _variant_virtual_projections.emplace(column.slot_id(), std::move(projection));
                continue;
            }

            // Find or create the hidden variant source for this column name.
            auto hidden_it = _hidden_variant_sources.find(column.source_variant_column_name);
            if (hidden_it == _hidden_variant_sources.end()) {
                const auto* source_schema_node =
                        _param.file_metadata->schema().get_stored_column_by_field_idx(column.idx_in_parquet);
                if (source_schema_node == nullptr) {
                    return Status::InternalError(strings::Substitute(
                            "invalid source parquet field idx for variant virtual column, idx=$0, slot=$1",
                            column.idx_in_parquet, column.slot_id()));
                }
                if (source_schema_node->type != ColumnType::STRUCT) {
                    return Status::InternalError(strings::Substitute(
                            "invalid source parquet field type for variant virtual column, idx=$0, type=$1",
                            column.idx_in_parquet, static_cast<int>(source_schema_node->type)));
                }
                VariantShreddedReadHints hints = _get_variant_shredded_hints(column.source_variant_column_name);
                ASSIGN_OR_RETURN(auto hidden_reader, ColumnReaderFactory::create_variant_column_reader(
                                                             _column_reader_opts, source_schema_node, hints));
                auto [inserted_it, _] = _hidden_variant_sources.emplace(
                        column.source_variant_column_name,
                        HiddenVariantSource{.slot_id = _next_hidden_slot_id--, .reader = std::move(hidden_reader)});
                hidden_it = inserted_it;
            }
            projection.source_slot_id = hidden_it->second.slot_id;
            _variant_virtual_projections.emplace(column.slot_id(), std::move(projection));
            continue;
        }
        ASSIGN_OR_RETURN(ColumnReaderPtr column_reader, _create_column_reader(column));
        _column_readers[column.slot_id()] = std::move(column_reader);
    }

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
                // Iceberg v3 row lineage: try physical column first (post-compaction files),
                // fall back to computed row_id (firstRowId + position) for non-compacted files.
                ASSIGN_OR_RETURN(auto reader,
                                 _create_reserved_iceberg_column_reader(slot, HdfsScanner::ICEBERG_ROW_ID_COLUMN_ID));
                std::optional<int64_t> first_row_id = std::nullopt;
                if (_param.scan_range != nullptr && _param.scan_range->__isset.first_row_id) {
                    first_row_id = std::optional<int64_t>(_row_group_first_row_id);
                } else if (use_legacy_lookup_row_id) {
                    first_row_id = std::optional<int64_t>(_row_group_first_row_id);
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
        if (column.slot_type().type == LogicalType::TYPE_VARIANT && schema_node != nullptr &&
            schema_node->type == ColumnType::STRUCT && column.t_lake_schema_field == nullptr) {
            VariantShreddedReadHints hints = _get_variant_shredded_hints(column.slot_desc->col_name());
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
        if (_param.global_dictmaps->contains(column.slot_id())) {
            ASSIGN_OR_RETURN(
                    column_reader,
                    ColumnReaderFactory::create(std::move(column_reader), _param.global_dictmaps->at(column.slot_id()),
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
    for (const auto& [name, hidden_source] : _hidden_variant_sources) {
        RETURN_IF_ERROR(hidden_source.reader->prepare());
        if (hidden_source.reader->get_column_parquet_field() != nullptr &&
            hidden_source.reader->get_column_parquet_field()->is_complex_type()) {
            // Hidden VARIANT sources share the same complex reader path and also rely on
            // def/rep levels for correct nullability reconstruction.
            hidden_source.reader->set_need_parse_levels(true);
        }
    }
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    int read_col_idx = 0;

    for (auto& column : _param.read_cols) {
        if (column.is_extended_variant_virtual) {
            SlotId slot_id = column.slot_id();
            if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
                    _deferred_variant_virtual_conjunct_ctxs.emplace_back(ctx);
                }
            }
            ++read_col_idx;
            continue;
        }
        SlotId slot_id = column.slot_id();
        if (conjunct_ctxs_by_slot.find(slot_id) != conjunct_ctxs_by_slot.end()) {
            for (ExprContext* ctx : conjunct_ctxs_by_slot.at(slot_id)) {
                std::vector<std::string> sub_field_path;
                if (_try_to_use_dict_filter(column, ctx, sub_field_path, column.decode_needed)) {
                    _use_as_dict_filter_column(read_col_idx, slot_id, sub_field_path);
                } else {
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

// Creates a lightweight VIEW of _read_chunk: the returned Chunk holds *shared*
// ColumnPtr references to the same column objects owned by _read_chunk.  Calling
// reset(), filter(), or filter_range() on the returned chunk modifies the same
// underlying column storage as _read_chunk.
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
    for (const auto& [name, hidden_source] : _hidden_variant_sources) {
        hidden_source.reader->collect_column_io_range(ranges, &end, types, true);
    }
    deduplicate_io_ranges(ranges);
    *end_offset = end;
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
    ASSIGN_OR_RETURN(_read_chunk, ChunkHelper::new_chunk_checked(read_slots, chunk_size));
    for (const auto& [name, hidden_source] : _hidden_variant_sources) {
        auto hidden_column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARIANT), true);
        _read_chunk->append_column(std::move(hidden_column), hidden_source.slot_id);
    }
    return Status::OK();
}

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

const cctz::time_zone& GroupReader::_get_variant_projection_timezone() {
    if (_timezone_resolved) {
        return _timezone_obj;
    }

    _timezone_resolved = true;
    if (_param.timezone.empty()) {
        return _timezone_obj;
    }
    if (!TimezoneUtils::find_cctz_time_zone(_param.timezone, _timezone_obj)) {
        LOG(WARNING) << "GroupReader: fallback to UTC for invalid timezone: " << _param.timezone;
        _timezone_obj = cctz::utc_time_zone();
    }
    return _timezone_obj;
}

// Evaluates deferred variant virtual conjuncts (e.g. v:a.b > 0) against active_chunk.
// Projects each virtual column into a temporary eval_chunk, runs the conjuncts to
// produce a row filter, then applies the filter to active_chunk and the hidden VARIANT
// source columns in _read_chunk.
//
// Returns true if any rows survived, false if all rows were filtered out.
// raw_count is the original unfiltered row count in this range, used only for stats.
StatusOr<bool> GroupReader::_apply_deferred_variant_conjuncts(ChunkPtr& active_chunk, size_t raw_count) {
    if (_deferred_variant_virtual_conjunct_ctxs.empty()) {
        return true;
    }
    SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);

    // Build eval_chunk: one projected column per virtual slot.
    // Iterate _variant_virtual_projections directly to avoid scanning _param.read_cols.
    // Physical variant source → found in active_chunk (shared with _read_chunk).
    // Hidden variant source   → found in _read_chunk (synthetic negative slot ids).
    ChunkPtr eval_chunk = std::make_shared<Chunk>();
    for (const auto& [slot_id, projection] : _variant_virtual_projections) {
        ASSIGN_OR_RETURN(ColumnPtr source_col,
                         get_variant_projection_source_column(active_chunk, active_chunk, _read_chunk,
                                                              projection.source_slot_id));
        ASSIGN_OR_RETURN(auto result_col,
                         project_variant_leaf_column(source_col, projection.parsed_path, projection.target_type,
                                                     _get_variant_projection_timezone()));
        eval_chunk->append_column(std::move(result_col), slot_id);
    }

    // Guard: if no columns were projected (shouldn't happen in normal flow), treat as
    // all-pass to avoid undefined behaviour in eval_conjuncts_into_filter.
    if (eval_chunk->num_columns() == 0) {
        return true;
    }

    // Use eval_chunk->num_rows() rather than active_chunk->num_rows(): when all
    // requested columns are virtual, active_chunk has no physical columns and
    // num_rows() == 0, while eval_chunk is built from hidden sources with real rows.
    Filter filter(eval_chunk->num_rows(), 1);
    ASSIGN_OR_RETURN(size_t hit_count, ChunkPredicateEvaluator::eval_conjuncts_into_filter(
                                               _deferred_variant_virtual_conjunct_ctxs, eval_chunk.get(), &filter));
    if (hit_count == 0) {
        _param.stats->late_materialize_skip_rows += raw_count;
        return false;
    }

    // Filter active_chunk (its columns are shared with _read_chunk physical slots) and
    // the hidden VARIANT source columns that live only in _read_chunk.
    active_chunk->filter(filter);
    for (const auto& [name, hidden_source] : _hidden_variant_sources) {
        _read_chunk->get_column_by_slot_id(hidden_source.slot_id)->as_mutable_ptr()->filter(filter);
    }
    return true;
}

// _fill_dst_chunk copies physical columns from active_chunk (a shared-reference VIEW
// of _read_chunk) into *chunk, then computes variant virtual projections.
//
// Source lookup order for virtual projections (get_variant_projection_source_column):
//   1. *chunk     – physical columns already written in the loop above (positive slot ids)
//   2. active_chunk – same physical slots via shared _read_chunk backing store
//   3. _read_chunk  – hidden variant sources (synthetic *negative* slot ids)
Status GroupReader::_fill_dst_chunk(ChunkPtr& active_chunk, ChunkPtr* chunk) {
    active_chunk->check_or_die();
    // Pass 1: physical columns.  Skip slots that have a virtual projection so we don't
    // call fill_dst_column for them (they have no entry in _column_readers).
    for (const auto& column : _param.read_cols) {
        SlotId slot_id = column.slot_id();
        if (_variant_virtual_projections.count(slot_id)) continue;
        RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                  active_chunk->get_column_by_slot_id(slot_id)));
    }
    if (_param.reserved_field_slots != nullptr) {
        for (const auto* slot : *_param.reserved_field_slots) {
            SlotId slot_id = slot->id();
            RETURN_IF_ERROR(_column_readers[slot_id]->fill_dst_column((*chunk)->get_column_by_slot_id(slot_id),
                                                                      active_chunk->get_column_by_slot_id(slot_id)));
        }
    }

    // Pass 2: virtual projections.  Iterate the projection map directly to avoid
    // scanning _param.read_cols again.
    for (const auto& [slot_id, projection] : _variant_virtual_projections) {
        // Search order: *chunk (physical cols just filled) → active_chunk (same backing
        // store) → _read_chunk (hidden variant sources with negative slot ids).
        ASSIGN_OR_RETURN(
                ColumnPtr source_column,
                get_variant_projection_source_column(*chunk, active_chunk, _read_chunk, projection.source_slot_id));
        ASSIGN_OR_RETURN(auto result_column,
                         project_variant_leaf_column(source_column, projection.parsed_path, projection.target_type,
                                                     _get_variant_projection_timezone()));
        (*chunk)->get_column_by_slot_id(slot_id) = std::move(result_column);
    }

    active_chunk->check_or_die();
    return Status::OK();
}

} // namespace starrocks::parquet
