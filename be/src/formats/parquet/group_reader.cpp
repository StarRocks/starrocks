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
#include "common/system/master_info.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/decimal_cast_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/variant_path_reader.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/iceberg_row_id_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_pos_reader.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/row_source_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_helper.h"
#include "storage/convert_helper.h"
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
        // VARCHAR / CHAR / VARBINARY / BINARY all share the same physical BinaryColumn
        // representation (raw bytes).  Allow the fast typed-column path when both the
        // shredded leaf type and the target type are in this "string-like" family.
        auto is_string_like = [](LogicalType t) {
            return t == TYPE_VARCHAR || t == TYPE_CHAR || t == TYPE_VARBINARY || t == TYPE_BINARY;
        };
        if (!is_string_like(reader.typed_type_desc().type) || !is_string_like(target_type.type)) {
            return Status::NotFound("variant typed leaf type does not match target slot type");
        }
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
        return Status::InternalError(fmt::format(
                "variant typed column size mismatch: typed_size={}, variant_src_size={}", eff_typed->size(), num_rows));
    }

    // Merge null masks: result is null when either the outer variant or the typed leaf
    // is null.  Outer nulls only exist for non-const variant_src (const-null was handled
    // above; const-non-null has no outer null mask to propagate).
    const bool has_outer_nulls = !variant_src->is_constant() && variant_src->is_nullable() &&
                                 down_cast<const NullableColumn*>(variant_src.get())->has_null();

    // Fast path: no outer nulls to merge.  eff_typed already carries the complete null
    // structure (rows where the shredded leaf is absent are already null in typed_col).
    // Clone it directly instead of splitting into data + null and reassembling.
    if (!has_outer_nulls) {
        if (eff_typed->is_nullable()) {
            return eff_typed->clone();
        }
        // Typed column is non-nullable (all values present): wrap with an all-zero null mask.
        return NullableColumn::create(eff_typed->clone(), NullColumn::create(num_rows, 0));
    }

    // Slow path: merge outer nulls from variant_src into the typed null mask.
    const bool has_typed_nulls = eff_typed->is_nullable() && down_cast<const NullableColumn*>(eff_typed)->has_null();

    auto result_null = NullColumn::create(num_rows, 0);
    NullData& result_null_data = result_null->get_data();

    if (has_typed_nulls) {
        const auto outer = down_cast<const NullableColumn*>(variant_src.get())->immutable_null_column_data();
        const auto typed = down_cast<const NullableColumn*>(eff_typed)->immutable_null_column_data();
        for (size_t i = 0; i < num_rows; ++i) {
            result_null_data[i] = outer[i] | typed[i];
        }
    } else {
        const auto outer = down_cast<const NullableColumn*>(variant_src.get())->immutable_null_column_data();
        std::copy(outer.begin(), outer.end(), result_null_data.begin());
    }

    auto result_data = ColumnHelper::get_data_column(eff_typed)->clone();
    auto result = NullableColumn::create(std::move(result_data), std::move(result_null));
    result->update_has_null();
    return result;
}

// Cast a typed-shredded decimal column to a (possibly different) decimal target type.
//
// This function is needed because the shredded typed column already holds native decimal
// storage (int32 / int64 / int128 physical values with precision+scale baked into the
// TypeDescriptor). That is NOT a variant-binary-encoded value, so VariantRowConverter::cast_to
// cannot be used: that path decodes variant binary wire format and explicitly excludes
// decimal targets (see the "VARIANT -> Decimal types: DecimalNonDecimalCast" comment in
// variant_row_converter.cpp). Instead we go through TypeConverter::convert_column, which
// understands native decimal storage and handles precision/scale widening correctly.
//
// If source_type == target_type (same precision and scale) no conversion is necessary and the
// column is returned as-is.
StatusOr<ColumnPtr> cast_decimal_projection_column(const ColumnPtr& source_column, const TypeDescriptor& source_type,
                                                   const TypeDescriptor& target_type) {
    if (source_type == target_type) {
        return source_column;
    }

    const TypeConverter* converter = get_type_converter(source_type.type, target_type.type);
    if (converter == nullptr) {
        return Status::NotFound("no decimal converter for variant typed projection");
    }

    TypeInfoPtr source_type_info = get_type_info(source_type);
    TypeInfoPtr target_type_info = get_type_info(target_type);
    if (source_type_info == nullptr || target_type_info == nullptr) {
        return Status::NotSupported("missing type info for decimal variant projection");
    }

    auto result = ColumnHelper::create_column(target_type, true);
    MemPool mem_pool;
    RETURN_IF_ERROR(converter->convert_column(source_type_info.get(), *source_column, target_type_info.get(),
                                              result.get(), &mem_pool));
    return result;
}

StatusOr<ColumnPtr> build_decimal_typed_variant_projection(const VariantColumn* variant_column,
                                                           const ColumnPtr& variant_src, const VariantPath& path,
                                                           const TypeDescriptor& target_type) {
    VariantPathReader reader;
    reader.prepare(variant_column, &path);
    if (!reader.is_typed_exact()) {
        return Status::NotFound("variant path is not an exact typed leaf");
    }

    const TypeDescriptor& source_type = reader.typed_type_desc();
    if (!source_type.is_decimalv3_type() || !target_type.is_decimalv3_type()) {
        return Status::NotFound("variant typed leaf is not decimal");
    }

    ASSIGN_OR_RETURN(auto exact_source_projection,
                     build_exact_typed_variant_projection(variant_column, variant_src, path, source_type));
    return cast_decimal_projection_column(exact_source_projection, source_type, target_type);
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

    // Keep virtual-column schema stable across chunks: variant leaf extraction is semantically nullable,
    // so always return NullableColumn instead of data-dependent nullable/non-nullable output.
    return builder.build_nullable_column();
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> build_decimal_variant_projection_column(const VariantColumn* variant_column,
                                                            const ColumnPtr& variant_src, const VariantPath& path,
                                                            const TypeDescriptor& target_type) {
    const size_t num_rows = variant_src->size();

    ColumnBuilder<ResultType> builder(num_rows, target_type.precision, target_type.scale);
    VariantPathReader reader;
    reader.prepare(variant_column, &path);

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
        auto variant_ref = read.value.as_ref();
        const VariantValue& variant_value = variant_ref.get_value();
        if (variant_value.type() == VariantType::NULL_TYPE) {
            builder.append_null();
            continue;
        }
        RunTimeCppType<ResultType> decimal_value{};
        ASSIGN_OR_RETURN(bool overflow,
                         cast_variant_to_decimal<RunTimeCppType<ResultType>>(&decimal_value, variant_value,
                                                                             target_type.precision, target_type.scale));
        if (overflow) {
            builder.append_null();
        } else {
            builder.append(decimal_value);
        }
    }

    // Keep virtual-column schema stable across chunks: variant leaf extraction is semantically nullable,
    // so always return NullableColumn instead of data-dependent nullable/non-nullable output.
    return builder.build_nullable_column();
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
    if (target_type.is_decimalv3_type()) {
        auto decimal_typed_result =
                build_decimal_typed_variant_projection(variant_column, variant_src, path, target_type);
        if (decimal_typed_result.ok()) {
            return decimal_typed_result;
        }
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
    case TYPE_DECIMAL32:
        return build_decimal_variant_projection_column<TYPE_DECIMAL32>(variant_column, variant_src, path, target_type);
    case TYPE_DECIMAL64:
        return build_decimal_variant_projection_column<TYPE_DECIMAL64>(variant_column, variant_src, path, target_type);
    case TYPE_DECIMAL128:
        return build_decimal_variant_projection_column<TYPE_DECIMAL128>(variant_column, variant_src, path, target_type);
    case TYPE_VARIANT:
        return build_variant_projection_column<TYPE_VARIANT>(variant_column, variant_src, path, zone);
    default:
        return Status::NotSupported("unsupported variant virtual column target type");
    }
}

bool collect_variant_leaf_paths(const ColumnAccessPath* node, std::vector<VariantSegment>* segments,
                                VariantShreddedReadHints* hints) {
    if (!node->is_field() || node->path().empty()) {
        return false;
    }

    segments->emplace_back(VariantSegment::make_object(node->path()));
    if (node->children().empty()) {
        // A VARIANT-typed leaf hint is not precise enough for shredded-path pruning.
        // FE may truncate paths with unsupported JSON semantics (e.g. array index in
        // "$.a[0].b"), producing an intermediate VARIANT leaf like "a". Restricting
        // reads to that lossy prefix can drop needed descendants and return NULL.
        if (node->value_type().type == LogicalType::TYPE_VARIANT) {
            segments->pop_back();
            return false;
        }
        VariantPath path(*segments);
        auto shredded_path = path.to_shredded_path();
        if (!shredded_path.has_value()) {
            segments->pop_back();
            return false;
        }
        auto st = hints->add_path(std::move(*shredded_path));
        if (!st.ok()) {
            segments->pop_back();
            return false;
        }
        segments->pop_back();
        return true;
    }

    bool valid = true;
    for (const auto& child : node->children()) {
        valid = collect_variant_leaf_paths(child.get(), segments, hints) && valid;
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

    // Promote shredded variant virtual columns to direct typed_value proxy readers (Phase 3.5).
    // Must run after _prepare_column_readers() (which sets skip_base_payload) and after
    // select_offset_index (so the underlying typed_value readers already have the correct range).
    RETURN_IF_ERROR(_promote_variant_virtual_columns());

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

// Per-iteration chunk model used in get_next:
//
//  _read_chunk  (member)
//    The column backing store.  Reset at the start of every range iteration.
//    Holds physical slots (shared with active_chunk / lazy_chunk via _create_read_chunk)
//    and active hidden variant source columns (shared with active_chunk).
//
//  active_chunk  (local, created fresh each iteration)
//    A lightweight VIEW of _read_chunk for _active_slot_ids (active physical columns +
//    active hidden variant sources + reserved_field_slots).  Filtering active_chunk
//    also filters the shared columns in _read_chunk.
//
//  lazy_chunk  (local, created when lazy physical columns exist)
//    A lightweight VIEW of _read_chunk for _lazy_slot_ids (no reserved fields).
//    Merged into active_chunk after reading so active_chunk is the single source
//    for _fill_dst_chunk.
//
//  lazy_hidden_chunk  (local, created when lazy hidden variant sources exist)
//    A temporary chunk for projection-only hidden variant sources.  Read AFTER
//    variant conjunct evaluation to skip rows discarded by those conjuncts.
//    Merged into active_chunk so that _fill_dst_chunk only needs to look in active_chunk.
//
//  *chunk  (output)
//    The caller-provided destination chunk.  May already contain partition columns.
//    We do not use (*chunk)->num_rows() as the row count; use active_chunk->num_rows().

Status GroupReader::get_next(ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    // Reset per-iteration scratch buffers and create a fresh active_chunk view.
    // active_chunk is recreated (not reset) so that merged lazy columns from a
    // previous iteration's Phase 5/6 do not carry over.
    _read_chunk->reset();
    ChunkPtr active_chunk = _create_read_chunk(_active_slot_ids);

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

        bool has_filter = false;
        Filter chunk_filter(count, 1);
        active_chunk->reset();

        // ── Phase 1: row-id (deletion bitmap) filter ──────────────────────────
        if (nullptr != _skip_rows_ctx && _skip_rows_ctx->has_skip_rows()) {
            SCOPED_RAW_TIMER(&_param.stats->build_rowid_filter_ns);
            ASSIGN_OR_RETURN(has_filter,
                             _skip_rows_ctx->deletion_bitmap->fill_filter(r.begin(), r.end(), chunk_filter));
            if (SIMD::count_nonzero(chunk_filter.data(), count) == 0) {
                continue;
            }
        }

        // ── Phase 2: active physical columns (with optional predicate pushdown) ──
        // chunk_filter is populated here but active_chunk is NOT physically reduced yet.
        // Physical reduction is deferred to after Phase 4 so both chunk_filter and
        // variant_filter share size == count, enabling a simple element-wise AND merge
        // instead of a walk-and-apply over mismatched indices.
        if (!_dict_column_indices.empty() || !_left_no_dict_filter_conjuncts_by_slot.empty()) {
            has_filter = true;
            ASSIGN_OR_RETURN(size_t hit_count, _read_range_round_by_round(r, &chunk_filter, &active_chunk));
            if (hit_count == 0) {
                _param.stats->late_materialize_skip_rows += count;
                continue;
            }
        } else if (has_filter) {
            RETURN_IF_ERROR(_read_range(_active_column_indices, r, &chunk_filter, &active_chunk));
        } else {
            RETURN_IF_ERROR(_read_range(_active_column_indices, r, nullptr, &active_chunk));
        }

        // ── Phase 3: active hidden variant sources ─────────────────────────────
        // Read over the full range r because active_chunk has not been physically
        // reduced yet.  The combined filter applied after Phase 4 covers all columns
        // in active_chunk including these hidden sources.
        for (SlotId slot_id : _active_hidden_slot_ids) {
            auto* hidden_src = _hidden_slot_index.at(slot_id);
            auto& hidden_col = active_chunk->get_column_by_slot_id(slot_id);
            RETURN_IF_ERROR(hidden_src->reader->read_range(r, nullptr, hidden_col));
        }

        // ── Phase 4: variant virtual conjunct evaluation ───────────────────────
        // active_chunk has count rows; variant_filter is also size count.
        // Simple element-wise AND merges it with chunk_filter (also size count).
        auto deferred_projected_chunk = std::make_shared<Chunk>();
        ASSIGN_OR_RETURN(Filter variant_filter,
                         _apply_deferred_variant_conjuncts(active_chunk, count, &deferred_projected_chunk));
        if (!variant_filter.empty()) {
            if (SIMD::count_nonzero(variant_filter.data(), variant_filter.size()) == 0) {
                continue;
            }
            DCHECK_EQ(variant_filter.size(), count);
            for (size_t i = 0; i < count; i++) {
                chunk_filter[i] &= variant_filter[i];
            }
            has_filter = true;
        }

        // Apply the combined chunk_filter once to physically reduce active_chunk.
        // All columns (physical active + active hidden sources) are filtered here.
        if (has_filter) {
            active_chunk->filter(chunk_filter);
            if (active_chunk->num_rows() == 0) {
                continue;
            }
            // Keep deferred projected virtual columns aligned with active_chunk rows.
            // Both were built on the same pre-filter row space [0, count).
            RETURN_IF_ERROR(_align_deferred_projected_chunk_after_filter(active_chunk, deferred_projected_chunk,
                                                                         chunk_filter, count));
        }

        // Compute the post-filter range / slice for Phase 5 and Phase 6 lazy reads.
        Range<uint64_t> post_filter_range;
        Filter post_filter;
        if (has_filter) {
            post_filter_range = r.filter(&chunk_filter);
            DCHECK(post_filter_range.span_size() > 0);
            post_filter = {chunk_filter.begin() + post_filter_range.begin() - r.begin(),
                           chunk_filter.begin() + post_filter_range.end() - r.begin()};
        }

        // ── Phase 5: lazy physical columns ────────────────────────────────────
        // Read lazy physical columns for rows surviving Phases 1–4.
        if (!_lazy_column_indices.empty()) {
            _lazy_column_needed = true;
            ChunkPtr lazy_chunk = _create_read_chunk(_lazy_slot_ids, false);
            if (has_filter) {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, post_filter_range, &post_filter, &lazy_chunk, true));
                lazy_chunk->filter_range(post_filter, 0, post_filter_range.span_size());
            } else {
                RETURN_IF_ERROR(_read_range(_lazy_column_indices, r, nullptr, &lazy_chunk, true));
            }
            if (lazy_chunk->num_rows() != active_chunk->num_rows()) {
                return Status::InternalError(fmt::format("Unmatched row count, active_rows={}, lazy_rows={}",
                                                         active_chunk->num_rows(), lazy_chunk->num_rows()));
            }
            active_chunk->merge(std::move(*lazy_chunk));
        }

        // ── Phase 6: lazy hidden variant sources ───────────────────────────────
        // Projection-only hidden sources (no conjuncts).  Read AFTER Phase 4 so that
        // rows discarded by variant conjuncts are never decoded.
        // Use the same combined chunk_filter/post_filter_range as Phase 5.
        if (!_lazy_hidden_slot_ids.empty()) {
            auto lazy_hidden_chunk = std::make_shared<Chunk>();
            for (SlotId slot_id : _lazy_hidden_slot_ids) {
                auto* hidden_src = _hidden_slot_index.at(slot_id);
                ColumnPtr col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARIANT), true);
                if (has_filter) {
                    RETURN_IF_ERROR(hidden_src->reader->read_range(post_filter_range, &post_filter, col));
                    col->as_mutable_ptr()->filter_range(post_filter, 0, post_filter_range.span_size());
                } else {
                    RETURN_IF_ERROR(hidden_src->reader->read_range(r, nullptr, col));
                }
                lazy_hidden_chunk->append_column(std::move(col), slot_id);
            }
            if (lazy_hidden_chunk->num_rows() != active_chunk->num_rows()) {
                return Status::InternalError(fmt::format("Unmatched row count, active_rows={}, lazy_hidden_rows={}",
                                                         active_chunk->num_rows(), lazy_hidden_chunk->num_rows()));
            }
            active_chunk->merge(std::move(*lazy_hidden_chunk));
        }

        // ── Phase 7: decode + virtual projection → dst chunk ──────────────────
        {
            SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
            *row_count = active_chunk->num_rows();
            RETURN_IF_ERROR(_fill_dst_chunk(active_chunk, deferred_projected_chunk, chunk));
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

// Derives the set of shredded paths that the planner wants to materialise for the given
// variant column, based on the column_access_paths pushed down from the FE.
//
// The result is a VariantShreddedReadHints whose shredded_paths / parsed_shredded_paths are
// the minimal, non-redundant set of leaf paths, suitable for passing to ColumnReaderFactory.
//
// Return value semantics:
//   - Empty hints  → no path restriction; the reader auto-discovers paths from the schema.
//   - Non-empty    → only the listed paths (and their ancestors, for traversal) are read.
//
// Special cases that cause an early return with empty hints:
//   1. No column_access_paths present at all.
//   2. The access-path entry for this column has no children (full-column scan requested).
//   3. Any child path fails to produce a valid shredded-path string (e.g. contains array index).
//
// Post-processing steps applied before returning:
//   - Deduplication: paths collected from multiple access-path entries are de-duped by string.
//   - Ancestor pruning: if both "a.b" and "a.b.c" are present, "a.b.c" is redundant (the
//     "a.b" request already covers the "a.b.c" subtree) and is dropped. Pruning uses parsed
//     segment comparison rather than string prefix matching to avoid false matches like
//     "a.bc" vs "a.b".
VariantShreddedReadHints GroupReader::_get_variant_shredded_hints(std::string_view column_name) const {
    VariantShreddedReadHints hints;
    if (_param.column_access_paths == nullptr || _param.column_access_paths->empty()) {
        return hints;
    }

    std::unordered_set<std::string> unique_paths;
    for (const auto& access_path : *_param.column_access_paths) {
        if (access_path == nullptr || access_path->path() != column_name) {
            continue;
        }
        // No children means the whole variant column is accessed; disable path-level pruning.
        if (access_path->children().empty()) {
            hints.clear();
            return hints;
        }

        std::vector<VariantSegment> segments;
        for (const auto& child : access_path->children()) {
            // collect_variant_leaf_paths walks the access-path subtree and appends each
            // leaf as a shredded-path string + parsed VariantPath into `hints`.
            // Returns false if any path is invalid (e.g. contains an array-index segment).
            if (!collect_variant_leaf_paths(child.get(), &segments, &hints)) {
                hints.clear();
                return hints;
            }
        }
    }

    // Deduplicate and prune redundant descendant paths.
    // A path p is redundant if another collected path q is a strict ancestor of p:
    // requesting q already covers p's entire subtree, so keeping p causes unnecessary work.
    // Example: if both "a.b" and "a.b.c" are present, "a.b.c" is dropped because the
    // "a.b" request already causes "a.b.c" columns to be read.
    //
    // Determine which paths to keep in a read-only pass over the original vectors, then
    // build the output in a separate pass.  Moving elements during the comparison pass
    // would leave moved-from slots (empty segments) that act as universal ancestors and
    // incorrectly prune all subsequent paths.
    const size_t n = hints.shredded_paths.size();
    std::vector<bool> keep(n, false);
    for (size_t i = 0; i < n; ++i) {
        // Drop exact string duplicates.
        if (!unique_paths.emplace(hints.shredded_paths[i]).second) {
            continue;
        }
        // Drop path i if any other path j is a strict ancestor of i.
        bool has_ancestor = false;
        for (size_t j = 0; j < n; ++j) {
            if (i == j) {
                continue;
            }
            if (hints.parsed_shredded_paths[j].is_strict_prefix_of(hints.parsed_shredded_paths[i])) {
                has_ancestor = true;
                break;
            }
        }
        if (!has_ancestor) {
            keep[i] = true;
        }
    }
    std::vector<std::string> pruned_paths;
    std::vector<VariantPath> pruned_parsed_paths;
    pruned_paths.reserve(n);
    pruned_parsed_paths.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        if (keep[i]) {
            pruned_paths.emplace_back(std::move(hints.shredded_paths[i]));
            pruned_parsed_paths.emplace_back(std::move(hints.parsed_shredded_paths[i]));
        }
    }
    hints.shredded_paths = std::move(pruned_paths);
    hints.parsed_shredded_paths = std::move(pruned_parsed_paths);
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
            physical_variant_slots_by_name.emplace(std::string(column.slot_desc->col_name()), column.slot_id());
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
                    return Status::InternalError(
                            fmt::format("invalid source parquet field idx for variant virtual column, idx={}, slot={}",
                                        column.idx_in_parquet, column.slot_id()));
                }
                if (source_schema_node->type != ColumnType::STRUCT) {
                    return Status::InternalError(
                            fmt::format("invalid source parquet field type for variant virtual column, idx={}, type={}",
                                        column.idx_in_parquet, static_cast<int>(source_schema_node->type)));
                }
                VariantShreddedReadHints hints = _get_variant_shredded_hints(column.source_variant_column_name);
                ASSIGN_OR_RETURN(auto hidden_reader, ColumnReaderFactory::create_variant_column_reader(
                                                             _column_reader_opts, source_schema_node, hints));
                auto [inserted_it, _] = _hidden_variant_sources.emplace(
                        column.source_variant_column_name,
                        HiddenVariantSource{.slot_id = _next_hidden_slot_id--, .reader = std::move(hidden_reader)});
                hidden_it = inserted_it;
                _hidden_slot_index[hidden_it->second.slot_id] = &hidden_it->second;
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

    // Register lightweight zone-map readers for virtual variant columns.
    // These allow PredicateFilterEvaluator to apply row-group and page-level zone-map
    // filtering on shredded typed-leaf columns without reading any actual data.
    // The readers are keyed by virtual slot id so that get_column_reader(virtual_slot_id)
    // returns them; collect_io_ranges does not touch them (they are not in active/lazy
    // column index lists).
    for (const auto& [virtual_slot_id, projection] : _variant_virtual_projections) {
        SlotId source_slot_id = projection.source_slot_id;
        ColumnReader* source_reader = nullptr;
        if (source_slot_id >= 0) {
            // Physical VARIANT source: look up in _column_readers.
            auto it = _column_readers.find(source_slot_id);
            if (it != _column_readers.end()) {
                source_reader = it->second.get();
            }
        } else {
            // Hidden VARIANT source: look up in _hidden_slot_index.
            auto it = _hidden_slot_index.find(source_slot_id);
            if (it != _hidden_slot_index.end()) {
                source_reader = it->second->reader.get();
            }
        }
        if (source_reader == nullptr) continue;
        auto* variant_reader = down_cast<VariantColumnReader*>(source_reader);
        _column_readers.emplace(virtual_slot_id,
                                std::make_unique<VariantVirtualZoneMapReader>(variant_reader, projection.parsed_path,
                                                                              projection.target_type));
    }

    return Status::OK();
}

StatusOr<ColumnReaderPtr> GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(column.idx_in_parquet);
    {
        if (column.slot_type().type == LogicalType::TYPE_VARIANT && schema_node != nullptr &&
            schema_node->type == ColumnType::STRUCT) {
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

    // ── Step 1: Classify physical read_cols into active / lazy ───────────────
    // Virtual columns contribute only to deferred conjunct evaluation; they have
    // no entry in _column_readers and are never added to active/lazy indices.
    // Physical columns with conjuncts go into _active_column_indices; the rest are
    // deferred to _lazy_column_indices when late materialisation is enabled.
    //
    // Pre-pass: collect physical VARIANT slot IDs that are sources for deferred
    // virtual conjuncts.  A virtual column's source_slot_id may point at a physical
    // VARIANT column (positive slot ID) rather than a hidden variant source
    // (negative slot ID).  Such physical columns have no direct conjuncts and would
    // normally be classified as lazy, but deferred Phase-4 evaluation reads them
    // via active_chunk, so they must be active.
    std::unordered_set<SlotId> deferred_conjunct_physical_source_slots;
    for (const auto& column : _param.read_cols) {
        if (!column.is_extended_variant_virtual) continue;
        if (conjunct_ctxs_by_slot.count(column.slot_id()) == 0) continue;
        auto proj_it = _variant_virtual_projections.find(column.slot_id());
        if (proj_it == _variant_virtual_projections.end()) continue;
        // Hidden sources have negative slot IDs; non-negative IDs are physical columns.
        SlotId src = proj_it->second.source_slot_id;
        if (src >= 0) {
            deferred_conjunct_physical_source_slots.insert(src);
        }
    }

    int read_col_idx = 0;
    for (auto& column : _param.read_cols) {
        if (column.is_extended_variant_virtual) {
            auto it = conjunct_ctxs_by_slot.find(column.slot_id());
            if (it != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : it->second) {
                    _deferred_variant_virtual_conjunct_ctxs.push_back(ctx);
                }
                _deferred_conjunct_slot_ids.insert(column.slot_id());
            }
            ++read_col_idx;
            continue;
        }
        SlotId slot_id = column.slot_id();
        auto it = conjunct_ctxs_by_slot.find(slot_id);
        if (it != conjunct_ctxs_by_slot.end()) {
            for (ExprContext* ctx : it->second) {
                std::vector<std::string> sub_field_path;
                if (_try_to_use_dict_filter(column, ctx, sub_field_path, column.decode_needed)) {
                    _use_as_dict_filter_column(read_col_idx, slot_id, sub_field_path);
                } else {
                    _left_conjunct_ctxs.push_back(ctx);
                    // For struct columns some conjuncts may be pushed to dict-filter leaves
                    // while others remain here; track per-slot for later evaluation.
                    _left_no_dict_filter_conjuncts_by_slot[slot_id].push_back(ctx);
                }
            }
            _active_column_indices.push_back(read_col_idx);
        } else if (config::parquet_late_materialization_enable &&
                   deferred_conjunct_physical_source_slots.count(slot_id) == 0) {
            // Do not lazify a physical column that is referenced as a source by a
            // deferred virtual conjunct; Phase 4 must be able to project from it.
            _lazy_column_indices.push_back(read_col_idx);
            _column_readers[slot_id]->set_can_lazy_decode(true);
        } else {
            _active_column_indices.push_back(read_col_idx);
        }
        ++read_col_idx;
    }

    // ── Step 2: Reserved field conjuncts ─────────────────────────────────────
    bool has_reserved_field_filter = false;
    if (_param.reserved_field_slots != nullptr) {
        for (auto* slot : *_param.reserved_field_slots) {
            SlotId slot_id = slot->id();
            auto it = conjunct_ctxs_by_slot.find(slot_id);
            if (it != conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : it->second) {
                    _left_no_dict_filter_conjuncts_by_slot[slot_id].push_back(ctx);
                }
                has_reserved_field_filter = true;
            }
        }
    }

    // ── Step 3: Build ColumnReadOrderCtx ─────────────────────────────────────
    // Built from the predicate-bearing active columns before any lazy→active
    // promotion, so it reflects only columns that need round-by-round evaluation.
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
    // A hidden source is active if it backs at least one virtual column that carries
    // a conjunct (those conjuncts are evaluated in Phase 4 and need the source data
    // before conjunct evaluation).  Projection-only hidden sources are lazy.
    {
        std::unordered_set<SlotId> conjunct_source_slot_ids;
        for (const auto& column : _param.read_cols) {
            if (!column.is_extended_variant_virtual) continue;
            if (conjunct_ctxs_by_slot.count(column.slot_id()) == 0) continue;
            auto proj_it = _variant_virtual_projections.find(column.slot_id());
            if (proj_it != _variant_virtual_projections.end()) {
                conjunct_source_slot_ids.insert(proj_it->second.source_slot_id);
            }
        }
        for (auto& [name, src] : _hidden_variant_sources) {
            src.is_active = conjunct_source_slot_ids.count(src.slot_id) > 0;
            if (src.is_active) {
                _active_hidden_slot_ids.push_back(src.slot_id);
            } else {
                _lazy_hidden_slot_ids.push_back(src.slot_id);
            }
        }
    }

    // ── Step 5: Build unified SlotId lists for _create_read_chunk ────────────
    // _active_slot_ids = active physical columns  +  active hidden sources
    // _lazy_slot_ids   = lazy physical columns
    // (lazy hidden sources remain in _lazy_hidden_slot_ids; read directly in Phase 6)
    for (int idx : _active_column_indices) {
        _active_slot_ids.push_back(_param.read_cols[idx].slot_id());
    }
    for (SlotId slot_id : _active_hidden_slot_ids) {
        _active_slot_ids.push_back(slot_id);
    }
    for (int idx : _lazy_column_indices) {
        _lazy_slot_ids.push_back(_param.read_cols[idx].slot_id());
    }

    // ── Step 6: Promote all lazy columns to active when active_chunk is empty ─
    // _active_slot_ids is empty when:
    //   • No physical column has a predicate (→ _active_column_indices is empty), AND
    //   • No hidden variant source backs a conjunct virtual column.
    //
    // In this case late-materialising provides no benefit (there's nothing to filter
    // on before reading lazy columns) and active_chunk would have 0 columns.
    // Promote ALL lazy columns — physical and hidden — to active so they are read
    // in one pass and active_chunk always has a valid row count.
    //
    // Note: when active hidden sources exist (_active_slot_ids is non-empty), lazy
    // physical columns correctly remain lazy: they are read in Phase 5 AFTER the
    // variant conjuncts (Phase 4) have filtered rows, avoiding unnecessary IO.
    if (_active_slot_ids.empty() && !has_reserved_field_filter) {
        // Promote lazy physical columns.
        _active_column_indices.swap(_lazy_column_indices);
        _active_slot_ids.swap(_lazy_slot_ids);
        // Promote lazy hidden sources.
        for (SlotId slot_id : _lazy_hidden_slot_ids) {
            _active_slot_ids.push_back(slot_id);
            _active_hidden_slot_ids.push_back(slot_id);
            _hidden_slot_index.at(slot_id)->is_active = true;
        }
        _lazy_hidden_slot_ids.clear();
    }
}

Status GroupReader::_promote_variant_virtual_columns() {
    // For each hidden variant source whose VariantColumnReader has _skip_base_payload=true
    // (all requested shredded paths are scalar typed-value leaves), promote ALL virtual slots
    // backed by that source to VariantTypedValueProxy readers in _column_readers.  This lets
    // the proxy participate in Phase 2 dict-filter and eliminates the Phase 3 hidden-source
    // read and Phase 4 deferred variant conjunct evaluation entirely.
    //
    // Full-promotion is all-or-nothing per source: if any virtual slot lacks a typed_value
    // leaf, we skip the source entirely.  Partial promotion would corrupt the hidden source's
    // read path (dict-filtered readers output INT codes, not decoded values).

    // Build: hidden source slot_id → list of (virtual slot info) backed by that source.
    struct VirtualSlotInfo {
        SlotId virtual_slot_id;
        int read_col_idx;
        bool decode_needed;
        bool has_conjunct;
    };
    std::unordered_map<SlotId, std::vector<VirtualSlotInfo>> slots_by_source;
    {
        int idx = 0;
        for (const auto& column : _param.read_cols) {
            if (column.is_extended_variant_virtual) {
                auto proj_it = _variant_virtual_projections.find(column.slot_id());
                if (proj_it != _variant_virtual_projections.end()) {
                    SlotId src_id = proj_it->second.source_slot_id;
                    // Only promote hidden sources (negative slot IDs).
                    if (src_id < 0) {
                        bool has_conj = _param.conjunct_ctxs_by_slot.count(column.slot_id()) > 0;
                        slots_by_source[src_id].push_back({column.slot_id(), idx, column.decode_needed, has_conj});
                    }
                }
            }
            ++idx;
        }
    }

    bool any_promoted = false;

    for (auto& [src_name, hidden_source] : _hidden_variant_sources) {
        SlotId src_id = hidden_source.slot_id;
        auto slots_it = slots_by_source.find(src_id);
        if (slots_it == slots_by_source.end()) continue;

        auto* vreader = dynamic_cast<VariantColumnReader*>(hidden_source.reader.get());
        if (vreader == nullptr || !vreader->skip_base_payload()) continue;

        const auto& virtual_slots = slots_it->second;

        // Check: can ALL virtual slots be promoted?  Three conditions must hold for each slot:
        // 1. Has a scalar typed_value leaf reader.
        // 2. Fallback value column is all-null for this row group (data is exclusively in
        //    typed_value).  If any path has non-null fallback values, _read_range_skip_base_payload's
        //    runtime detection would fall back to the base payload — which promotion bypasses,
        //    producing all-NULLs.
        // 3. The typed_value leaf's read type is directly compatible with the virtual slot's
        //    target column type for raw writing.  Variable-length types (VARCHAR/VARBINARY etc.)
        //    share the same BinaryColumn layout and are mutually compatible.  Fixed-size types
        //    must match exactly; a mismatch (e.g. INT32 typed_value → BIGINT slot) causes the
        //    PlainDecoder to write the wrong element width into the destination column, crashing.
        const uint64_t rg_num_rows = get_row_group_metadata()->num_rows;
        auto var_len_type = [](LogicalType t) {
            return t == TYPE_VARCHAR || t == TYPE_CHAR || t == TYPE_VARBINARY || t == TYPE_BINARY;
        };
        bool all_promotable = true;
        std::unordered_set<ColumnReader*> promoted_leaf_readers;
        std::unordered_map<SlotId, ColumnReader*> leaf_readers_by_slot;
        for (const auto& vsi : virtual_slots) {
            auto proj_it = _variant_virtual_projections.find(vsi.virtual_slot_id);
            if (proj_it == _variant_virtual_projections.end()) {
                all_promotable = false;
                break;
            }
            const auto& parsed_path = proj_it->second.parsed_path;
            ColumnReader* leaf = vreader->scalar_typed_value_reader_for_path(parsed_path);
            if (leaf == nullptr) {
                all_promotable = false;
                break;
            }
            if (!promoted_leaf_readers.insert(leaf).second) {
                all_promotable = false;
                break;
            }
            leaf_readers_by_slot.emplace(vsi.virtual_slot_id, leaf);
            if (!vreader->fallback_values_all_null_in_row_group_for_path(parsed_path, rg_num_rows)) {
                all_promotable = false;
                break;
            }
            const TypeDescriptor* leaf_type = vreader->typed_value_read_type_for_path(parsed_path);
            const TypeDescriptor& target = proj_it->second.target_type;
            bool type_ok = (leaf_type != nullptr) &&
                           (var_len_type(leaf_type->type) && var_len_type(target.type) ? true : *leaf_type == target);
            if (!type_ok) {
                all_promotable = false;
                break;
            }
        }
        if (!all_promotable) continue;

        // Fully promote: create VariantTypedValueProxy for each virtual slot.
        for (const auto& vsi : virtual_slots) {
            auto& proj = _variant_virtual_projections.at(vsi.virtual_slot_id);
            ColumnReader* leaf = leaf_readers_by_slot.at(vsi.virtual_slot_id);

            auto proxy = std::make_unique<VariantTypedValueProxy>(leaf);

            if (vsi.has_conjunct) {
                // Attempt dict filter for each conjunct; fall back to expression eval if failed.
                const auto& conjuncts = _param.conjunct_ctxs_by_slot.at(vsi.virtual_slot_id);
                for (ExprContext* ctx : conjuncts) {
                    std::vector<std::string> sub_field_path;
                    // sub_field_path is empty: virtual slot IS the leaf value directly.
                    if (proxy->try_to_use_dict_filter(ctx, vsi.decode_needed, vsi.virtual_slot_id, sub_field_path, 0)) {
                        _use_as_dict_filter_column(vsi.read_col_idx, vsi.virtual_slot_id, sub_field_path);
                    } else {
                        _left_no_dict_filter_conjuncts_by_slot[vsi.virtual_slot_id].push_back(ctx);
                    }
                }
            }
            // Always place promoted slots in active indices regardless of conjunct presence.
            // Projection-only promoted slots must also be active to guarantee active_chunk
            // has a valid row count when it is the only column set being read.
            _active_column_indices.push_back(vsi.read_col_idx);
            _active_slot_ids.push_back(vsi.virtual_slot_id);

            _column_readers[vsi.virtual_slot_id] = std::move(proxy);
            _promoted_virtual_slots.insert(vsi.virtual_slot_id);
            _variant_virtual_projections.erase(vsi.virtual_slot_id);
        }

        // Mark source as fully promoted and remove it from the active/lazy hidden-source lists.
        hidden_source.fully_promoted = true;
        _active_hidden_slot_ids.erase(
                std::remove(_active_hidden_slot_ids.begin(), _active_hidden_slot_ids.end(), src_id),
                _active_hidden_slot_ids.end());
        _lazy_hidden_slot_ids.erase(std::remove(_lazy_hidden_slot_ids.begin(), _lazy_hidden_slot_ids.end(), src_id),
                                    _lazy_hidden_slot_ids.end());
        // Also remove the hidden source's slot_id from _active_slot_ids (added in Step 4 of
        // _process_columns_and_conjunct_ctxs when the source was classified as active).
        _active_slot_ids.erase(std::remove(_active_slot_ids.begin(), _active_slot_ids.end(), src_id),
                               _active_slot_ids.end());

        any_promoted = true;
    }

    if (!any_promoted) return Status::OK();

    // Rebuild _column_read_order_ctx to include newly promoted active columns.
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

    // Rebuild deferred conjunct list, excluding promoted slots.
    {
        std::vector<ExprContext*> remaining;
        std::unordered_set<SlotId> remaining_ids;
        for (const auto& column : _param.read_cols) {
            if (!column.is_extended_variant_virtual) continue;
            SlotId slot_id = column.slot_id();
            if (_deferred_conjunct_slot_ids.count(slot_id) == 0) continue;
            if (_promoted_virtual_slots.count(slot_id) > 0) continue;
            auto it = _param.conjunct_ctxs_by_slot.find(slot_id);
            if (it != _param.conjunct_ctxs_by_slot.end()) {
                for (ExprContext* ctx : it->second) {
                    remaining.push_back(ctx);
                }
                remaining_ids.insert(slot_id);
            }
        }
        _deferred_variant_virtual_conjunct_ctxs = std::move(remaining);
        _deferred_conjunct_slot_ids = std::move(remaining_ids);
    }

    return Status::OK();
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
        // Fully-promoted sources have all their virtual slots promoted to VariantTypedValueProxy
        // readers in _column_readers (Phase 2).  Their IO is registered via the proxy entries
        // in _active_column_indices above; skip the source here to avoid duplicate IO ranges.
        if (hidden_source.fully_promoted) continue;
        hidden_source.reader->collect_column_io_range(ranges, &end, types, hidden_source.is_active);
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
    // Only pre-allocate active hidden sources in _read_chunk so they can be shared with
    // active_chunk.  Lazy hidden sources are created fresh each iteration in Phase 6
    // and merged into active_chunk there; they do not need a permanent slot here.
    for (SlotId slot_id : _active_hidden_slot_ids) {
        auto hidden_column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARIANT), true);
        _read_chunk->append_column(std::move(hidden_column), slot_id);
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
// Projects each virtual column whose source is already in active_chunk into a temporary
// eval_chunk, runs the conjuncts, and filters active_chunk with the result.
//
// Active hidden source columns are part of active_chunk (added via _active_hidden_slot_ids
// in _create_read_chunk) so active_chunk->filter() covers them automatically.
// Lazy hidden sources are not yet read at this point; they are handled in Phase 4.5 and
// are identified by being absent from active_chunk.
//
// Returns an empty Filter when there are no conjuncts (all rows pass).
// Returns the applied filter otherwise; caller checks for all-zero to skip the range.
StatusOr<Filter> GroupReader::_apply_deferred_variant_conjuncts(ChunkPtr& active_chunk, size_t raw_count,
                                                                ChunkPtr* projected_chunk) {
    DCHECK(projected_chunk != nullptr);
    *projected_chunk = std::make_shared<Chunk>();
    if (_deferred_variant_virtual_conjunct_ctxs.empty()) {
        return Filter{};
    }
    SCOPED_RAW_TIMER(&_param.stats->expr_filter_ns);

    // Build eval_chunk: one projected column per deferred-conjunct virtual slot whose source is available.
    // Physical sources and active hidden sources are in active_chunk.
    // Lazy hidden sources are absent from active_chunk and are skipped here; by design,
    // virtual columns with conjuncts always use active sources, so this is correct.
    ChunkPtr eval_chunk = std::make_shared<Chunk>();
    for (const auto& [slot_id, projection] : _variant_virtual_projections) {
        bool has_conjunct = _deferred_conjunct_slot_ids.count(slot_id) > 0;
        if (!has_conjunct) {
            continue;
        }
        bool source_in_chunk = active_chunk->is_slot_exist(projection.source_slot_id);
        if (!source_in_chunk) {
            return Status::InternalError(
                    fmt::format("variant virtual column {} has deferred conjunct but source slot {} "
                                "is not in active_chunk",
                                slot_id, projection.source_slot_id));
        }
        const ColumnPtr& source_col = active_chunk->get_column_by_slot_id(projection.source_slot_id);
        ASSIGN_OR_RETURN(auto result_col,
                         project_variant_leaf_column(source_col, projection.parsed_path, projection.target_type,
                                                     _get_variant_projection_timezone()));
        eval_chunk->append_column(std::move(result_col), slot_id);
    }
    *projected_chunk = eval_chunk;

    // Guard: if no columns were projected, treat as all-pass.
    if (eval_chunk->num_columns() == 0) {
        return Filter{};
    }

    Filter filter(eval_chunk->num_rows(), 1);
    ASSIGN_OR_RETURN(size_t hit_count, ChunkPredicateEvaluator::eval_conjuncts_into_filter(
                                               _deferred_variant_virtual_conjunct_ctxs, eval_chunk.get(), &filter));
    if (hit_count == 0) {
        _param.stats->late_materialize_skip_rows += raw_count;
        // eval_conjuncts_into_filter returns 0 (all rows rejected) without zeroing the filter
        // when it short-circuits early (e.g. true_count == 0 path). Zero it out explicitly so
        // the caller's count_nonzero check correctly skips the chunk.
        filter.assign(filter.size(), 0);
    }

    // active_chunk is NOT filtered here; the caller merges this filter with
    // chunk_filter and applies the combined result once after Phase 4.
    return filter;
}

Status GroupReader::_align_deferred_projected_chunk_after_filter(const ChunkPtr& active_chunk,
                                                                 const ChunkPtr& deferred_projected_chunk,
                                                                 const Filter& chunk_filter, size_t pre_filter_rows) {
    if (deferred_projected_chunk == nullptr || deferred_projected_chunk->num_columns() == 0) {
        return Status::OK();
    }
    if (deferred_projected_chunk->num_rows() == pre_filter_rows) {
        deferred_projected_chunk->filter_range(chunk_filter, 0, pre_filter_rows);
        return Status::OK();
    }
    if (deferred_projected_chunk->num_rows() != active_chunk->num_rows()) {
        return Status::InternalError(
                fmt::format("variant deferred projected chunk row count mismatch: projected_rows={}, "
                            "pre_filter_rows={}, active_rows={}",
                            deferred_projected_chunk->num_rows(), pre_filter_rows, active_chunk->num_rows()));
    }
    return Status::OK();
}

// _fill_dst_chunk copies physical columns from active_chunk into *chunk, then computes
// variant virtual projections.
//
// By Phase 6, active_chunk contains all required sources:
//   - Physical slots  (active + lazy physical, merged in Phase 5)
//   - Active hidden variant sources (read in Phase 3, included via _active_slot_ids)
//   - Lazy hidden variant sources (read in Phase 6, merged into active_chunk)
// All projection sources are therefore found directly in active_chunk.
//
Status GroupReader::_fill_dst_chunk(ChunkPtr& active_chunk, const ChunkPtr& projected_chunk, ChunkPtr* chunk) {
    active_chunk->check_or_die();

    // Pass 1: virtual projections — must run BEFORE Pass 2.
    // source_slot_id may point to either a hidden variant source (negative slot ID)
    // or a physical VARIANT column (positive slot ID, see _create_column_readers lines
    // 791-793).  Pass 2 performs a destructive swap that would move the physical source
    // column out of active_chunk, so virtual projections must read their sources first.
    for (const auto& [slot_id, projection] : _variant_virtual_projections) {
        // Reuse deferred-conjunct projection if available; this avoids a second
        // project_variant_leaf_column(path seek + decode) for the same virtual slot.
        if (projected_chunk != nullptr && projected_chunk->is_slot_exist(slot_id)) {
            const ColumnPtr& projected_col = projected_chunk->get_column_by_slot_id(slot_id);
            if (projected_col == nullptr) {
                return Status::InternalError(
                        fmt::format("variant deferred projected column for slot {} is null", slot_id));
            }
            if (projected_col->size() != active_chunk->num_rows()) {
                return Status::InternalError(
                        fmt::format("variant deferred projected column row count mismatch for slot {}: {} vs {}",
                                    slot_id, projected_col->size(), active_chunk->num_rows()));
            }
            (*chunk)->get_column_by_slot_id(slot_id) = projected_col;
            continue;
        }
        if (!active_chunk->is_slot_exist(projection.source_slot_id)) {
            return Status::InternalError(fmt::format("variant virtual column source slot {} not found in active_chunk",
                                                     projection.source_slot_id));
        }
        const ColumnPtr& source_column = active_chunk->get_column_by_slot_id(projection.source_slot_id);
        ASSIGN_OR_RETURN(auto result_column,
                         project_variant_leaf_column(source_column, projection.parsed_path, projection.target_type,
                                                     _get_variant_projection_timezone()));
        (*chunk)->get_column_by_slot_id(slot_id) = std::move(result_column);
    }

    // Pass 2: physical columns — destructive swap from active_chunk into *chunk.
    // Virtual projection slots are skipped here; their output was filled in Pass 1.
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

    return Status::OK();
}

} // namespace starrocks::parquet
