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

#include "formats/parquet/variant_projection.h"

#include <glog/logging.h>

#include <unordered_set>

#include "base/simd/simd.h"
#include "base/time/timezone_utils.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/variant_column.h"
#include "column/variant_converter.h"
#include "column/variant_path_parser.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/decimal_cast_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/variant_path_reader.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/complex_column_reader.h"
#include "formats/parquet/group_reader.h" // for GroupReaderParam
#include "formats/parquet/scalar_column_reader.h"
#include "runtime/mem_pool.h"
#include "storage/convert_helper.h"
#include "types/type_info.h"

namespace starrocks::parquet {

// ── Anonymous namespace: internal helpers (free functions) ─────────────────

namespace {

StatusOr<ColumnPtr> build_exact_typed_variant_projection(const VariantColumn* variant_column,
                                                         const ColumnPtr& variant_src, const VariantPath& path,
                                                         const TypeDescriptor& target_type) {
    VariantPathReader reader;
    reader.prepare(variant_column, &path);
    if (!reader.is_typed_exact()) {
        return Status::NotFound("variant path is not an exact typed leaf");
    }
    if (reader.typed_type_desc() != target_type) {
        auto is_string_like = [](LogicalType t) {
            return t == TYPE_VARCHAR || t == TYPE_CHAR || t == TYPE_VARBINARY || t == TYPE_BINARY;
        };
        if (!is_string_like(reader.typed_type_desc().type) || !is_string_like(target_type.type)) {
            return Status::NotFound("variant typed leaf type does not match target slot type");
        }
    }

    const size_t num_rows = variant_src->size();
    const Column* typed_col = reader.typed_column();

    if (variant_src->is_constant() && variant_src->is_null(0)) {
        auto all_null = ColumnHelper::create_column(target_type, true);
        all_null->append_nulls(num_rows);
        return all_null;
    }

    ColumnPtr expanded_typed;
    if (variant_src->is_constant()) {
        expanded_typed = typed_col->clone();
        expanded_typed->as_mutable_ptr()->assign(num_rows, 0);
    }
    const Column* eff_typed = variant_src->is_constant() ? expanded_typed.get() : typed_col;
    if (eff_typed->size() != num_rows) {
        return Status::InternalError(fmt::format(
                "variant typed column size mismatch: typed_size={}, variant_src_size={}", eff_typed->size(), num_rows));
    }

    const bool has_outer_nulls = !variant_src->is_constant() && variant_src->is_nullable() &&
                                 down_cast<const NullableColumn*>(variant_src.get())->has_null();

    if (!has_outer_nulls) {
        if (eff_typed->is_nullable()) {
            return eff_typed->clone();
        }
        return NullableColumn::create(eff_typed->clone(), NullColumn::create(num_rows, 0));
    }

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

    return builder.build_nullable_column();
}

} // namespace

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

VariantShreddedReadHints build_variant_shredded_hints(const std::vector<ColumnAccessPathPtr>* column_access_paths,
                                                      std::string_view column_name) {
    VariantShreddedReadHints hints;
    if (column_access_paths == nullptr || column_access_paths->empty()) {
        return hints;
    }

    std::unordered_set<std::string> unique_paths;
    for (const auto& access_path : *column_access_paths) {
        if (access_path == nullptr || access_path->path() != column_name) {
            continue;
        }
        if (access_path->children().empty()) {
            hints.clear();
            return hints;
        }

        std::vector<VariantSegment> segments;
        for (const auto& child : access_path->children()) {
            if (!collect_variant_leaf_paths(child.get(), &segments, &hints)) {
                hints.clear();
                return hints;
            }
        }
    }

    const size_t n = hints.shredded_paths.size();
    std::vector<bool> keep(n, false);
    for (size_t i = 0; i < n; ++i) {
        if (!unique_paths.emplace(hints.shredded_paths[i]).second) {
            continue;
        }
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

// ── VariantProjectionHandler ────────────────────────────────────────────────

VariantProjectionHandler::VariantProjectionHandler(GroupReader* group_reader, const GroupReaderParam& param,
                                                   const tparquet::RowGroup* row_group_metadata)
        : _param(param), _row_group_metadata(row_group_metadata), _group_reader(group_reader) {}

VariantProjectionHandler::~VariantProjectionHandler() = default;

// ── _build_shredded_hints ────────────────────────────────────────────────────

VariantShreddedReadHints VariantProjectionHandler::_build_shredded_hints(std::string_view column_name) const {
    return build_variant_shredded_hints(&_param.scanner_ctx->column_access_paths, column_name);
}

// ── setup_readers ────────────────────────────────────────────────────────────

Status VariantProjectionHandler::setup_readers() {
    const auto& opts = _group_reader->_column_reader_opts;
    std::unordered_map<std::string, SlotId> physical_variant_slots_by_name;
    for (const auto& column : _param.read_cols) {
        if (!column.is_extended_variant_virtual && column.slot_type().type == LogicalType::TYPE_VARIANT) {
            physical_variant_slots_by_name.emplace(std::string(column.slot_desc->col_name()), column.slot_id());
        }
    }

    for (const auto& column : _param.read_cols) {
        if (!column.is_extended_variant_virtual) continue;

        ASSIGN_OR_RETURN(auto parsed_path,
                         VariantPathParser::parse_shredded_path(std::string_view(column.variant_virtual_leaf_path)));
        Projection projection{
                .parsed_path = std::move(parsed_path), .target_type = column.slot_type(), .source_slot_id = 0};

        auto physical_it = physical_variant_slots_by_name.find(column.source_variant_column_name);
        if (physical_it != physical_variant_slots_by_name.end()) {
            projection.source_slot_id = physical_it->second;
            _projections.emplace(column.slot_id(), std::move(projection));
            continue;
        }

        auto hidden_it = _hidden_sources.find(column.source_variant_column_name);
        if (hidden_it == _hidden_sources.end()) {
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
            VariantShreddedReadHints hints = _build_shredded_hints(column.source_variant_column_name);
            ASSIGN_OR_RETURN(auto hidden_reader,
                             ColumnReaderFactory::create_variant_column_reader(opts, source_schema_node, hints));
            auto [inserted_it, _] = _hidden_sources.emplace(
                    column.source_variant_column_name,
                    HiddenSource{.slot_id = _next_hidden_slot_id--, .reader = std::move(hidden_reader)});
            hidden_it = inserted_it;
            _hidden_slot_index[hidden_it->second.slot_id] = &hidden_it->second;
        }
        projection.source_slot_id = hidden_it->second.slot_id;
        _projections.emplace(column.slot_id(), std::move(projection));
    }

    return Status::OK();
}

void VariantProjectionHandler::register_zone_map_readers() {
    auto& column_readers = _group_reader->_column_readers;
    for (const auto& [virtual_slot_id, projection] : _projections) {
        SlotId source_slot_id = projection.source_slot_id;
        ColumnReader* source_reader = nullptr;
        if (source_slot_id >= 0) {
            auto it = column_readers.find(source_slot_id);
            if (it != column_readers.end()) {
                source_reader = it->second.get();
            }
        } else {
            auto it = _hidden_slot_index.find(source_slot_id);
            if (it != _hidden_slot_index.end()) {
                source_reader = it->second->reader.get();
            }
        }
        if (source_reader == nullptr) continue;
        auto* variant_reader = down_cast<VariantColumnReader*>(source_reader);
        column_readers.emplace(virtual_slot_id,
                               std::make_unique<VariantVirtualZoneMapReader>(variant_reader, projection.parsed_path,
                                                                             projection.target_type));
    }
}

// ── Classification helpers ───────────────────────────────────────────────────

std::unordered_set<SlotId> VariantProjectionHandler::deferred_conjunct_physical_source_slots() const {
    const auto& read_cols = _param.read_cols;
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    std::unordered_set<SlotId> result;
    for (const auto& column : read_cols) {
        if (!column.is_extended_variant_virtual) continue;
        if (conjunct_ctxs_by_slot.count(column.slot_id()) == 0) continue;
        auto proj_it = _projections.find(column.slot_id());
        if (proj_it == _projections.end()) continue;
        SlotId src = proj_it->second.source_slot_id;
        if (src >= 0) {
            result.insert(src);
        }
    }
    // Also force physical sources of virtual slots referenced in compound (multi-slot) conjuncts.
    if (_param.scanner_ctx != nullptr) {
        for (SlotId vsid : referenced_variant_virtual_slot_ids(_param.scanner_ctx->conjuncts.scanner_ctxs)) {
            auto proj_it = _projections.find(vsid);
            if (proj_it != _projections.end() && proj_it->second.source_slot_id >= 0) {
                result.insert(proj_it->second.source_slot_id);
            }
        }
    }
    return result;
}

void VariantProjectionHandler::collect_deferred_conjuncts() {
    const auto& read_cols = _param.read_cols;
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    for (const auto& column : read_cols) {
        if (!column.is_extended_variant_virtual) continue;
        auto it = conjunct_ctxs_by_slot.find(column.slot_id());
        if (it == conjunct_ctxs_by_slot.end()) continue;
        for (ExprContext* ctx : it->second) {
            _deferred_variant_virtual_conjunct_ctxs.push_back(ctx);
        }
        _deferred_conjunct_slot_ids.insert(column.slot_id());
    }
}

void VariantProjectionHandler::classify_hidden_sources() {
    const auto& read_cols = _param.read_cols;
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    std::unordered_set<SlotId> conjunct_source_slot_ids;
    for (const auto& column : read_cols) {
        if (!column.is_extended_variant_virtual) continue;
        if (conjunct_ctxs_by_slot.count(column.slot_id()) == 0) continue;
        auto proj_it = _projections.find(column.slot_id());
        if (proj_it != _projections.end()) {
            conjunct_source_slot_ids.insert(proj_it->second.source_slot_id);
        }
    }
    _active_hidden_slot_ids.clear();
    _lazy_hidden_slot_ids.clear();
    for (auto& [name, src] : _hidden_sources) {
        src.is_active = conjunct_source_slot_ids.count(src.slot_id) > 0;
        if (src.is_active) {
            _active_hidden_slot_ids.push_back(src.slot_id);
        } else {
            _lazy_hidden_slot_ids.push_back(src.slot_id);
        }
    }
}

// ── Prepare ─────────────────────────────────────────────────────────────────

Status VariantProjectionHandler::prepare_hidden_readers() {
    for (auto& [name, src] : _hidden_sources) {
        RETURN_IF_ERROR(src.reader->prepare());
        if (src.reader->get_column_parquet_field() != nullptr &&
            src.reader->get_column_parquet_field()->is_complex_type()) {
            src.reader->set_need_parse_levels(true);
        }
    }
    return Status::OK();
}

void VariantProjectionHandler::select_hidden_source_offset_index() {
    const auto& range = _group_reader->_range;
    auto rg_first_row = _group_reader->_row_group_first_row;
    for (auto& [name, src] : _hidden_sources) {
        src.reader->select_offset_index(range, rg_first_row);
    }
}

// ── Promotion ────────────────────────────────────────────────────────────────

bool VariantProjectionHandler::try_promote() {
    auto& column_readers = _group_reader->_column_readers;
    auto& active_column_indices = _group_reader->_column_materializer->mutable_active_column_indices();
    auto& active_slot_ids = _group_reader->_column_materializer->mutable_active_slot_ids();
    const auto& read_cols = _param.read_cols;
    const auto& conjunct_ctxs_by_slot = _param.conjunct_ctxs_by_slot;
    const uint64_t rg_num_rows = _row_group_metadata->num_rows;

    struct VirtualSlotInfo {
        SlotId virtual_slot_id;
        int read_col_idx;
        bool decode_needed;
        bool has_conjunct;
    };
    std::unordered_map<SlotId, std::vector<VirtualSlotInfo>> slots_by_source;
    {
        int idx = 0;
        for (const auto& column : read_cols) {
            if (column.is_extended_variant_virtual) {
                auto proj_it = _projections.find(column.slot_id());
                if (proj_it != _projections.end()) {
                    SlotId src_id = proj_it->second.source_slot_id;
                    if (src_id < 0) {
                        bool has_conj = conjunct_ctxs_by_slot.count(column.slot_id()) > 0;
                        slots_by_source[src_id].push_back({column.slot_id(), idx, column.decode_needed, has_conj});
                    }
                }
            }
            ++idx;
        }
    }

    bool any_promoted = false;

    for (auto& [src_name, hidden_source] : _hidden_sources) {
        SlotId src_id = hidden_source.slot_id;
        auto slots_it = slots_by_source.find(src_id);
        if (slots_it == slots_by_source.end()) continue;

        auto* vreader = dynamic_cast<VariantColumnReader*>(hidden_source.reader.get());
        if (vreader == nullptr || !vreader->skip_base_payload()) continue;

        const auto& virtual_slots = slots_it->second;

        auto var_len_type = [](LogicalType t) {
            return t == TYPE_VARCHAR || t == TYPE_CHAR || t == TYPE_VARBINARY || t == TYPE_BINARY;
        };
        bool all_promotable = true;
        std::unordered_set<ColumnReader*> promoted_leaf_readers;
        std::unordered_map<SlotId, ColumnReader*> leaf_readers_by_slot;
        for (const auto& vsi : virtual_slots) {
            auto proj_it = _projections.find(vsi.virtual_slot_id);
            if (proj_it == _projections.end()) {
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

        for (const auto& vsi : virtual_slots) {
            ColumnReader* leaf = leaf_readers_by_slot.at(vsi.virtual_slot_id);
            auto proxy = std::make_unique<VariantTypedValueProxy>(leaf);

            if (vsi.has_conjunct) {
                const auto& conjuncts = conjunct_ctxs_by_slot.at(vsi.virtual_slot_id);
                for (ExprContext* ctx : conjuncts) {
                    std::vector<std::string> sub_field_path;
                    if (proxy->try_to_use_dict_filter(ctx, vsi.decode_needed, vsi.virtual_slot_id, sub_field_path, 0)) {
                        _group_reader->_column_materializer->add_dict_filter_column(vsi.read_col_idx, sub_field_path);
                    } else {
                        _group_reader->_column_materializer->add_post_read_conjunct(vsi.virtual_slot_id, ctx);
                    }
                }
            }
            active_column_indices.push_back(vsi.read_col_idx);
            active_slot_ids.push_back(vsi.virtual_slot_id);

            column_readers[vsi.virtual_slot_id] = std::move(proxy);
            _promoted_virtual_slots.insert(vsi.virtual_slot_id);
            _projections.erase(vsi.virtual_slot_id);
        }

        hidden_source.fully_promoted = true;
        _active_hidden_slot_ids.erase(
                std::remove(_active_hidden_slot_ids.begin(), _active_hidden_slot_ids.end(), src_id),
                _active_hidden_slot_ids.end());
        _lazy_hidden_slot_ids.erase(std::remove(_lazy_hidden_slot_ids.begin(), _lazy_hidden_slot_ids.end(), src_id),
                                    _lazy_hidden_slot_ids.end());
        active_slot_ids.erase(std::remove(active_slot_ids.begin(), active_slot_ids.end(), src_id),
                              active_slot_ids.end());

        any_promoted = true;
    }

    if (!any_promoted) return false;

    _group_reader->_column_materializer->rebuild_read_order_ctx();

    // Rebuild deferred conjunct list excluding promoted slots.
    std::vector<ExprContext*> remaining;
    std::unordered_set<SlotId> remaining_ids;
    for (const auto& column : read_cols) {
        if (!column.is_extended_variant_virtual) continue;
        SlotId slot_id = column.slot_id();
        if (_deferred_conjunct_slot_ids.count(slot_id) == 0) continue;
        if (_promoted_virtual_slots.count(slot_id) > 0) continue;
        auto it = conjunct_ctxs_by_slot.find(slot_id);
        if (it != conjunct_ctxs_by_slot.end()) {
            for (ExprContext* ctx : it->second) {
                remaining.push_back(ctx);
            }
            remaining_ids.insert(slot_id);
        }
    }
    _deferred_variant_virtual_conjunct_ctxs = std::move(remaining);
    _deferred_conjunct_slot_ids = std::move(remaining_ids);

    return true;
}

// ── Lazy→active promotion ───────────

void VariantProjectionHandler::promote_lazy_to_active() {
    auto& active_slot_ids = _group_reader->_column_materializer->mutable_active_slot_ids();
    // When active_chunk is empty (no physical active columns + no active hidden sources),
    // promote lazy hidden sources to active so they contribute a row count.
    // The caller already swapped physical active/lazy; we just migrate hidden sources.
    for (SlotId slot_id : _lazy_hidden_slot_ids) {
        active_slot_ids.push_back(slot_id);
        _active_hidden_slot_ids.push_back(slot_id);
        _hidden_slot_index.at(slot_id)->is_active = true;
    }
    _lazy_hidden_slot_ids.clear();
}

// ── IO range collection ─────────────────────────────────────────────────────

void VariantProjectionHandler::collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges,
                                                 int64_t* end_offset, ColumnIOTypeFlags types) {
    for (auto& [name, src] : _hidden_sources) {
        if (src.fully_promoted) continue;
        src.reader->collect_column_io_range(ranges, end_offset, types, src.is_active);
    }
}

// ── Read chunk init ──────────────────────────────────────────────────────────

void VariantProjectionHandler::init_read_chunk_slots() {
    ChunkPtr& read_chunk = _group_reader->_column_materializer->mutable_read_chunk();
    for (SlotId slot_id : _active_hidden_slot_ids) {
        auto hidden_column = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARIANT), true);
        read_chunk->append_column(std::move(hidden_column), slot_id);
    }
}

// ── get_next() phases ────────────────────────────────────────────────────────

std::unordered_set<SlotId> VariantProjectionHandler::referenced_variant_virtual_slot_ids(
        const std::vector<ExprContext*>& conjunct_ctxs) const {
    std::unordered_set<SlotId> result;
    std::vector<SlotId> slot_ids;
    for (ExprContext* ctx : conjunct_ctxs) {
        if (ctx == nullptr) continue;
        slot_ids.clear();
        ctx->root()->get_slot_ids(&slot_ids);
        for (SlotId id : slot_ids) {
            if (is_virtual_slot(id)) {
                result.insert(id);
            }
        }
    }
    return result;
}

Status VariantProjectionHandler::fetch_and_project_virtual_slots(const std::unordered_set<SlotId>& virtual_slot_ids,
                                                                 const Range<uint64_t>& range, ChunkPtr& active_chunk,
                                                                 const cctz::time_zone& zone) {
    // 1. Collect hidden source slots that are needed but not yet fetched.
    for (SlotId virtual_slot_id : virtual_slot_ids) {
        auto proj_it = _projections.find(virtual_slot_id);
        if (proj_it == _projections.end()) {
            return Status::InternalError(
                    fmt::format("variant virtual slot {} not found in projections", virtual_slot_id));
        }
        SlotId source_slot_id = proj_it->second.source_slot_id;
        // Physical sources (slot_id >= 0) are already populated as active columns.
        if (source_slot_id < 0 && !_fetched_hidden_slots.count(source_slot_id)) {
            auto it = _hidden_slot_index.find(source_slot_id);
            if (it == _hidden_slot_index.end()) {
                return Status::InternalError(fmt::format("hidden source slot {} not found in index", source_slot_id));
            }
            // Read into the pre-created column (from init_read_chunk_slots) if it
            // exists; otherwise create a new one.
            if (active_chunk->is_slot_exist(source_slot_id)) {
                auto& col = active_chunk->get_column_by_slot_id(source_slot_id);
                RETURN_IF_ERROR(it->second->reader->read_range(range, nullptr, col));
                RETURN_IF_ERROR(it->second->reader->finalize_lazy_state(col));
            } else {
                ColumnPtr col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARIANT), true);
                RETURN_IF_ERROR(it->second->reader->read_range(range, nullptr, col));
                RETURN_IF_ERROR(it->second->reader->finalize_lazy_state(col));
                active_chunk->append_column(std::move(col), source_slot_id);
            }
            _fetched_hidden_slots.insert(source_slot_id);
        }
    }

    // 2. Project each needed virtual slot from its source column.
    for (SlotId virtual_slot_id : virtual_slot_ids) {
        if (active_chunk->is_slot_exist(virtual_slot_id)) continue;
        auto proj_it = _projections.find(virtual_slot_id);
        DCHECK(proj_it != _projections.end());
        const auto& projection = proj_it->second;
        DCHECK(active_chunk->is_slot_exist(projection.source_slot_id));
        const ColumnPtr& source_col = active_chunk->get_column_by_slot_id(projection.source_slot_id);
        ASSIGN_OR_RETURN(auto result_col,
                         project_variant_leaf_column(source_col, projection.parsed_path, projection.target_type, zone));
        active_chunk->append_column(std::move(result_col), virtual_slot_id);
    }
    return Status::OK();
}

void VariantProjectionHandler::reset_iteration_state() {
    _deferred_projected_chunk.reset();
    _fetched_hidden_slots.clear();
}

Status VariantProjectionHandler::fetch_sources(const Range<uint64_t>& range, ChunkPtr& active_chunk) {
    for (SlotId slot_id : _active_hidden_slot_ids) {
        // Skip columns that were already populated by an early fetch in Phase 2b.
        if (_fetched_hidden_slots.count(slot_id)) continue;
        auto* hidden_src = _hidden_slot_index.at(slot_id);
        auto& hidden_col = active_chunk->get_column_by_slot_id(slot_id);
        RETURN_IF_ERROR(hidden_src->reader->read_range(range, nullptr, hidden_col));
        RETURN_IF_ERROR(hidden_src->reader->finalize_lazy_state(hidden_col));
        _fetched_hidden_slots.insert(slot_id);
    }
    return Status::OK();
}

StatusOr<Filter> VariantProjectionHandler::filter_subfields(ChunkPtr& active_chunk, size_t raw_count,
                                                            HdfsScannerStats* stats, const cctz::time_zone& zone) {
    if (_deferred_variant_virtual_conjunct_ctxs.empty()) {
        return Filter{};
    }

    ChunkPtr eval_chunk = std::make_shared<Chunk>();
    for (const auto& [slot_id, projection] : _projections) {
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
                         project_variant_leaf_column(source_col, projection.parsed_path, projection.target_type, zone));
        eval_chunk->append_column(std::move(result_col), slot_id);
    }
    _deferred_projected_chunk = eval_chunk;

    if (eval_chunk->num_columns() == 0) {
        return Filter{};
    }

    int64_t expr_filter_ns = 0;
    SCOPED_RAW_TIMER(stats != nullptr ? &stats->expr_filter_ns : &expr_filter_ns);
    Filter filter(eval_chunk->num_rows(), 1);
    ASSIGN_OR_RETURN(size_t hit_count, ChunkPredicateEvaluator::eval_conjuncts_into_filter(
                                               _deferred_variant_virtual_conjunct_ctxs, eval_chunk.get(), &filter));
    if (hit_count == 0) {
        if (stats) stats->late_materialize_skip_rows += raw_count;
        filter.assign(filter.size(), 0);
    }

    return filter;
}

Status VariantProjectionHandler::align_after_combined_filter(const ChunkPtr& active_chunk, const Filter& filter,
                                                             size_t pre_filter_rows) {
    if (_deferred_projected_chunk == nullptr || _deferred_projected_chunk->num_columns() == 0) {
        return Status::OK();
    }
    if (_deferred_projected_chunk->num_rows() == pre_filter_rows) {
        _deferred_projected_chunk->filter_range(filter, 0, pre_filter_rows);
        return Status::OK();
    }
    if (_deferred_projected_chunk->num_rows() != active_chunk->num_rows()) {
        return Status::InternalError(
                fmt::format("variant deferred projected chunk row count mismatch: projected_rows={}, "
                            "pre_filter_rows={}, active_rows={}",
                            _deferred_projected_chunk->num_rows(), pre_filter_rows, active_chunk->num_rows()));
    }
    return Status::OK();
}

Status VariantProjectionHandler::backfill_sources(const Range<uint64_t>& full_range,
                                                  const Range<uint64_t>* post_filter_range, const Filter* post_filter,
                                                  bool has_filter, ChunkPtr& active_chunk) {
    if (_lazy_hidden_slot_ids.empty()) {
        return Status::OK();
    }

    auto lazy_hidden_chunk = std::make_shared<Chunk>();
    for (SlotId slot_id : _lazy_hidden_slot_ids) {
        // Skip slots already triggered via LazyMaterializationContext.
        if (active_chunk->is_slot_exist(slot_id)) continue;

        auto* hidden_src = _hidden_slot_index.at(slot_id);
        ColumnPtr col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARIANT), true);
        if (has_filter) {
            RETURN_IF_ERROR(hidden_src->reader->read_range(*post_filter_range, post_filter, col));
            col->as_mutable_ptr()->filter_range(*post_filter, 0, post_filter_range->span_size());
        } else {
            RETURN_IF_ERROR(hidden_src->reader->read_range(full_range, nullptr, col));
        }
        RETURN_IF_ERROR(hidden_src->reader->finalize_lazy_state(col));
        lazy_hidden_chunk->append_column(std::move(col), slot_id);
    }
    if (lazy_hidden_chunk->num_columns() == 0) {
        return Status::OK();
    }
    if (lazy_hidden_chunk->num_rows() != active_chunk->num_rows()) {
        return Status::InternalError(fmt::format("Unmatched row count, active_rows={}, lazy_hidden_rows={}",
                                                 active_chunk->num_rows(), lazy_hidden_chunk->num_rows()));
    }
    active_chunk->merge(std::move(*lazy_hidden_chunk));
    return Status::OK();
}

Status VariantProjectionHandler::materialize_hidden_source(SlotId slot_id, const Range<uint64_t>& range,
                                                           const Filter* filter, ChunkPtr& active_chunk) {
    // Check this is actually a lazy hidden source slot.
    auto& lazy_ids = _lazy_hidden_slot_ids;
    if (std::find(lazy_ids.begin(), lazy_ids.end(), slot_id) == lazy_ids.end()) {
        return Status::OK();
    }
    // Already present (triggered earlier this iteration)?
    if (active_chunk->is_slot_exist(slot_id)) {
        return Status::OK();
    }
    auto it = _hidden_slot_index.find(slot_id);
    if (it == _hidden_slot_index.end()) {
        return Status::InternalError(
                fmt::format("materialize_hidden_source: slot {} not in hidden_slot_index", slot_id));
    }
    ColumnPtr col = ColumnHelper::create_column(TypeDescriptor::from_logical_type(TYPE_VARIANT), true);
    RETURN_IF_ERROR(it->second->reader->read_range(range, filter, col));
    RETURN_IF_ERROR(it->second->reader->finalize_lazy_state(col));
    active_chunk->append_column(std::move(col), slot_id);
    return Status::OK();
}

Status VariantProjectionHandler::emit_projections(ChunkPtr& active_chunk, ChunkPtr* dst, const cctz::time_zone& zone) {
    for (const auto& [slot_id, projection] : _projections) {
        if (_deferred_projected_chunk != nullptr && _deferred_projected_chunk->is_slot_exist(slot_id)) {
            const ColumnPtr& projected_col = _deferred_projected_chunk->get_column_by_slot_id(slot_id);
            if (projected_col == nullptr) {
                return Status::InternalError(
                        fmt::format("variant deferred projected column for slot {} is null", slot_id));
            }
            if (projected_col->size() != active_chunk->num_rows()) {
                return Status::InternalError(
                        fmt::format("variant deferred projected column row count mismatch for slot {}: {} vs {}",
                                    slot_id, projected_col->size(), active_chunk->num_rows()));
            }
            (*dst)->get_column_by_slot_id(slot_id) = projected_col;
            continue;
        }
        // Early-projected virtual slot (Phase 2b compound-conjunct prep) —
        // already filtered and ready to move to dst.
        if (active_chunk->is_slot_exist(slot_id)) {
            (*dst)->get_column_by_slot_id(slot_id) = active_chunk->get_column_by_slot_id(slot_id);
            continue;
        }
        if (!active_chunk->is_slot_exist(projection.source_slot_id)) {
            return Status::InternalError(fmt::format("variant virtual column source slot {} not found in active_chunk",
                                                     projection.source_slot_id));
        }
        const ColumnPtr& source_column = active_chunk->get_column_by_slot_id(projection.source_slot_id);
        ASSIGN_OR_RETURN(auto result_column, project_variant_leaf_column(source_column, projection.parsed_path,
                                                                         projection.target_type, zone));
        (*dst)->get_column_by_slot_id(slot_id) = std::move(result_column);
    }
    return Status::OK();
}

const cctz::time_zone& VariantProjectionHandler::projection_timezone() {
    if (_timezone_resolved) {
        return _timezone_obj;
    }
    _timezone_resolved = true;
    if (_param.scanner_ctx->timezone.empty()) {
        return _timezone_obj;
    }
    if (!TimezoneUtils::find_cctz_time_zone(_param.scanner_ctx->timezone, _timezone_obj)) {
        LOG(WARNING) << "VariantProjectionHandler: fallback to UTC for invalid timezone: "
                     << _param.scanner_ctx->timezone;
        _timezone_obj = cctz::utc_time_zone();
    }
    return _timezone_obj;
}

std::unordered_set<SlotId> VariantProjectionHandler::projection_slot_ids() const {
    std::unordered_set<SlotId> ids;
    ids.reserve(_projections.size());
    for (const auto& [id, _] : _projections) ids.insert(id);
    return ids;
}

} // namespace starrocks::parquet
