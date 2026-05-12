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

#include "column/variant_merger.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/variant_column.h"
#include "column/variant_converter.h"
#include "column/variant_encoder.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "types/datum.h"
#include "types/logical_type.h"

namespace starrocks {

static StatusOr<const VariantColumn*> get_variant_data_column(const Column* column) {
    if (column == nullptr) {
        return Status::InvalidArgument("null input column");
    }
    const Column* data = ColumnHelper::get_data_column(column);
    if (data == nullptr || !data->is_variant()) {
        return Status::InvalidArgument("all inputs must be variant columns");
    }
    return down_cast<const VariantColumn*>(data);
}

static int get_integer_rank(LogicalType type) {
    switch (type) {
    case TYPE_TINYINT:
        return 1;
    case TYPE_SMALLINT:
        return 2;
    case TYPE_INT:
        return 3;
    case TYPE_BIGINT:
        return 4;
    case TYPE_LARGEINT:
        return 5;
    default:
        return -1;
    }
}

static LogicalType wider_integer_type(LogicalType lhs, LogicalType rhs) {
    return get_integer_rank(lhs) >= get_integer_rank(rhs) ? lhs : rhs;
}

static bool is_supported_numeric_for_variant_merge(LogicalType type) {
    return is_integer_type(type) || is_float_type(type);
}

static StatusOr<TypeDescriptor> choose_common_decimalv3_type(const TypeDescriptor& lhs, const TypeDescriptor& rhs) {
    if (!lhs.is_decimalv3_type() || !rhs.is_decimalv3_type()) {
        return TypeDescriptor(TYPE_VARIANT);
    }
    if (lhs.scale != rhs.scale) {
        return TypeDescriptor(TYPE_VARIANT);
    }

    int common_precision = std::max(lhs.precision, rhs.precision);
    int common_scale = lhs.scale;
    if (common_precision <= TypeDescriptor::MAX_DECIMAL4_PRECISION) {
        return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, common_precision, common_scale);
    }
    if (common_precision <= TypeDescriptor::MAX_DECIMAL8_PRECISION) {
        return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, common_precision, common_scale);
    }
    if (common_precision <= 38) {
        return TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, common_precision, common_scale);
    }
    return TypeDescriptor(TYPE_VARIANT);
}

static StatusOr<LogicalType> choose_common_logical_type(LogicalType lhs, LogicalType rhs) {
    if (lhs == rhs) {
        return lhs;
    }
    if (lhs == TYPE_VARIANT || rhs == TYPE_VARIANT) {
        return TYPE_VARIANT;
    }
    if (!is_supported_numeric_for_variant_merge(lhs) || !is_supported_numeric_for_variant_merge(rhs)) {
        return TYPE_VARIANT;
    }
    if (is_float_type(lhs) || is_float_type(rhs)) {
        return TYPE_DOUBLE;
    }
    return wider_integer_type(lhs, rhs);
}

static StatusOr<MutableColumnPtr> cast_column_to_variant(const Column& src_col, const TypeDescriptor& src_type_desc) {
    if (src_type_desc.type == TYPE_VARIANT) {
        return src_col.clone();
    }
    ColumnBuilder<TYPE_VARIANT> builder(src_col.size());
    auto src_cloned = src_col.clone();
    RETURN_IF_ERROR(VariantEncoder::encode_column(src_cloned, src_type_desc, &builder, false));
    return builder.build_nullable_column();
}

static StatusOr<long double> datum_to_long_double(const Datum& datum, LogicalType src_type) {
    switch (src_type) {
    case TYPE_TINYINT:
        return static_cast<long double>(datum.get_int8());
    case TYPE_SMALLINT:
        return static_cast<long double>(datum.get_int16());
    case TYPE_INT:
        return static_cast<long double>(datum.get_int32());
    case TYPE_BIGINT:
        return static_cast<long double>(datum.get_int64());
    case TYPE_LARGEINT:
        return static_cast<long double>(datum.get_int128());
    case TYPE_FLOAT:
        return static_cast<long double>(datum.get_float());
    case TYPE_DOUBLE:
        return static_cast<long double>(datum.get_double());
    default:
        return Status::NotSupported(
                strings::Substitute("unsupported source type for cast: $0", logical_type_to_string(src_type)));
    }
}

static StatusOr<MutableColumnPtr> cast_numeric_column(const Column& src_col, LogicalType src_type,
                                                      LogicalType dst_type) {
    if (src_type == dst_type) {
        return src_col.clone();
    }
    auto dst_col = ColumnHelper::create_column(TypeDescriptor(dst_type), true);
    for (size_t i = 0; i < src_col.size(); ++i) {
        Datum datum = src_col.get(i);
        if (datum.is_null()) {
            dst_col->append_nulls(1);
            continue;
        }

        ASSIGN_OR_RETURN(long double value, datum_to_long_double(datum, src_type));
        Datum out;
        switch (dst_type) {
        case TYPE_TINYINT:
            out.set_int8(static_cast<int8_t>(value));
            break;
        case TYPE_SMALLINT:
            out.set_int16(static_cast<int16_t>(value));
            break;
        case TYPE_INT:
            out.set_int32(static_cast<int32_t>(value));
            break;
        case TYPE_BIGINT:
            out.set_int64(static_cast<int64_t>(value));
            break;
        case TYPE_LARGEINT:
            out.set_int128(static_cast<int128_t>(value));
            break;
        case TYPE_FLOAT:
            out.set_float(static_cast<float>(value));
            break;
        case TYPE_DOUBLE:
            out.set_double(static_cast<double>(value));
            break;
        default:
            return Status::NotSupported(
                    strings::Substitute("unsupported target type for cast: $0", logical_type_to_string(dst_type)));
        }
        dst_col->append_datum(out);
    }
    return dst_col;
}

template <LogicalType ResultType>
static StatusOr<MutableColumnPtr> cast_variant_column_to_scalar(const Column& src_col, const cctz::time_zone& zone) {
    const auto* variant_col = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(&src_col));
    if (variant_col == nullptr) {
        return Status::InvalidArgument("input variant column is invalid");
    }
    ColumnBuilder<ResultType> builder(src_col.size());
    const bool is_const = src_col.is_constant();
    VariantRowRef row_ref;
    VariantRowValue row_buffer;
    for (size_t i = 0; i < src_col.size(); ++i) {
        if (src_col.is_null(i)) {
            builder.append_null();
            continue;
        }
        size_t variant_row = is_const ? 0 : i;
        if (variant_col->try_get_row_ref(variant_row, &row_ref)) {
            Status cast_status = VariantRowConverter::cast_to<ResultType, false>(row_ref, zone, builder);
            RETURN_IF_ERROR(cast_status);
            continue;
        }
        const VariantRowValue* row = variant_col->get_row_value(variant_row, &row_buffer);
        if (row == nullptr) {
            builder.append_null();
            continue;
        }
        Status cast_status = VariantRowConverter::cast_to<ResultType, false>(row->as_ref(), zone, builder);
        RETURN_IF_ERROR(cast_status);
    }
    return builder.build(false);
}

static StatusOr<int128_t> datum_to_decimalv3_int128(const Datum& datum, LogicalType src_type) {
    switch (src_type) {
    case TYPE_DECIMAL32:
        return static_cast<int128_t>(datum.get_int32());
    case TYPE_DECIMAL64:
        return static_cast<int128_t>(datum.get_int64());
    case TYPE_DECIMAL128:
        return datum.get_int128();
    default:
        return Status::NotSupported(
                strings::Substitute("unsupported decimal source type for cast: $0", logical_type_to_string(src_type)));
    }
}

static StatusOr<MutableColumnPtr> cast_decimalv3_column(const Column& src_col, const TypeDescriptor& src_type_desc,
                                                        const TypeDescriptor& dst_type_desc) {
    if (src_type_desc == dst_type_desc) {
        return src_col.clone();
    }
    if (!src_type_desc.is_decimalv3_type() || !dst_type_desc.is_decimalv3_type()) {
        return Status::NotSupported("unsupported decimalv3 cast type for variant merge");
    }
    if (src_type_desc.scale != dst_type_desc.scale) {
        return Status::NotSupported("decimalv3 cast with scale change is not supported in variant merge");
    }

    auto dst_col = ColumnHelper::create_column(dst_type_desc, true);
    for (size_t i = 0; i < src_col.size(); ++i) {
        Datum datum = src_col.get(i);
        if (datum.is_null()) {
            dst_col->append_nulls(1);
            continue;
        }

        ASSIGN_OR_RETURN(int128_t value, datum_to_decimalv3_int128(datum, src_type_desc.type));
        Datum out;
        switch (dst_type_desc.type) {
        case TYPE_DECIMAL32:
            out.set_int32(static_cast<int32_t>(value));
            break;
        case TYPE_DECIMAL64:
            out.set_int64(static_cast<int64_t>(value));
            break;
        case TYPE_DECIMAL128:
            out.set_int128(value);
            break;
        default:
            return Status::NotSupported("unsupported decimalv3 cast target for variant merge");
        }
        dst_col->append_datum(out);
    }
    return dst_col;
}

static bool is_string_like_type(LogicalType t) {
    return t == TYPE_VARCHAR || t == TYPE_CHAR || t == TYPE_VARBINARY || t == TYPE_BINARY;
}

StatusOr<TypeDescriptor> VariantColumnMerger::choose_common_type(const TypeDescriptor& lhs, const TypeDescriptor& rhs) {
    if (lhs == rhs) {
        return lhs;
    }
    if (lhs.type == TYPE_VARIANT || rhs.type == TYPE_VARIANT) {
        return TypeDescriptor(TYPE_VARIANT);
    }
    if (lhs.type == TYPE_ARRAY || rhs.type == TYPE_ARRAY) {
        return TypeDescriptor(TYPE_VARIANT);
    }
    // VARCHAR / CHAR / VARBINARY / BINARY all share the same physical BinaryColumn
    // representation (raw bytes).  Treat them as compatible and keep the lhs type.
    if (is_string_like_type(lhs.type) && is_string_like_type(rhs.type)) {
        return lhs;
    }
    if (lhs.is_decimal_type() || rhs.is_decimal_type()) {
        if (lhs.is_decimalv3_type() && rhs.is_decimalv3_type()) {
            return choose_common_decimalv3_type(lhs, rhs);
        }
        return TypeDescriptor(TYPE_VARIANT);
    }
    ASSIGN_OR_RETURN(LogicalType common_type, choose_common_logical_type(lhs.type, rhs.type));
    return TypeDescriptor(common_type);
}

StatusOr<MutableColumnPtr> VariantColumnMerger::cast_typed_column(const Column& src_col,
                                                                  const TypeDescriptor& src_type_desc,
                                                                  const TypeDescriptor& dst_type_desc) {
    if (src_type_desc.type == TYPE_VARIANT &&
        (dst_type_desc.type == TYPE_DATE || dst_type_desc.type == TYPE_DATETIME || dst_type_desc.type == TYPE_TIME)) {
        return Status::NotSupported("variant merge cast to temporal type requires explicit timezone context");
    }
    return cast_typed_column(src_col, src_type_desc, dst_type_desc, cctz::local_time_zone());
}

StatusOr<MutableColumnPtr> VariantColumnMerger::cast_typed_column(const Column& src_col,
                                                                  const TypeDescriptor& src_type_desc,
                                                                  const TypeDescriptor& dst_type_desc,
                                                                  const cctz::time_zone& timezone) {
    if (src_type_desc == dst_type_desc) {
        return src_col.clone();
    }
    // String-like types (VARCHAR/CHAR/VARBINARY/BINARY) share the same physical storage;
    // no byte-level conversion is needed.
    if (is_string_like_type(src_type_desc.type) && is_string_like_type(dst_type_desc.type)) {
        return src_col.clone();
    }
    if (dst_type_desc.type == TYPE_VARIANT) {
        return cast_column_to_variant(src_col, src_type_desc);
    }
    if (src_type_desc.type == TYPE_VARIANT) {
        switch (dst_type_desc.type) {
        case TYPE_BOOLEAN:
            return cast_variant_column_to_scalar<TYPE_BOOLEAN>(src_col, timezone);
        case TYPE_TINYINT:
            return cast_variant_column_to_scalar<TYPE_TINYINT>(src_col, timezone);
        case TYPE_SMALLINT:
            return cast_variant_column_to_scalar<TYPE_SMALLINT>(src_col, timezone);
        case TYPE_INT:
            return cast_variant_column_to_scalar<TYPE_INT>(src_col, timezone);
        case TYPE_BIGINT:
            return cast_variant_column_to_scalar<TYPE_BIGINT>(src_col, timezone);
        case TYPE_LARGEINT:
            return cast_variant_column_to_scalar<TYPE_LARGEINT>(src_col, timezone);
        case TYPE_FLOAT:
            return cast_variant_column_to_scalar<TYPE_FLOAT>(src_col, timezone);
        case TYPE_DOUBLE:
            return cast_variant_column_to_scalar<TYPE_DOUBLE>(src_col, timezone);
        case TYPE_VARCHAR:
            return cast_variant_column_to_scalar<TYPE_VARCHAR>(src_col, timezone);
        case TYPE_DATE:
            return cast_variant_column_to_scalar<TYPE_DATE>(src_col, timezone);
        case TYPE_DATETIME:
            return cast_variant_column_to_scalar<TYPE_DATETIME>(src_col, timezone);
        case TYPE_TIME:
            return cast_variant_column_to_scalar<TYPE_TIME>(src_col, timezone);
        default:
            break;
        }
    }
    if (src_type_desc.is_decimalv3_type() && dst_type_desc.is_decimalv3_type()) {
        return cast_decimalv3_column(src_col, src_type_desc, dst_type_desc);
    }
    if (is_supported_numeric_for_variant_merge(src_type_desc.type) &&
        is_supported_numeric_for_variant_merge(dst_type_desc.type)) {
        return cast_numeric_column(src_col, src_type_desc.type, dst_type_desc.type);
    }
    return Status::NotSupported(strings::Substitute("unsupported typed column cast for variant merge: $0 -> $1",
                                                    src_type_desc.debug_string(), dst_type_desc.debug_string()));
}

static Status build_path_index(const VariantColumn& column, std::unordered_map<std::string_view, size_t>* out) {
    out->clear();
    out->reserve(column.shredded_paths().size());
    for (size_t i = 0; i < column.shredded_paths().size(); ++i) {
        if (!out->emplace(column.shredded_paths()[i], i).second) {
            return Status::InvalidArgument(
                    strings::Substitute("duplicate shredded path found during merge: $0", column.shredded_paths()[i]));
        }
    }
    return Status::OK();
}

static Status check_duplicate_shredded_paths(const VariantColumn& column) {
    std::unordered_set<std::string_view> path_set;
    path_set.reserve(column.shredded_paths().size());
    for (const auto& path : column.shredded_paths()) {
        if (!path_set.emplace(path).second) {
            return Status::InvalidArgument(strings::Substitute("duplicate shredded path found during merge: $0", path));
        }
    }
    return Status::OK();
}

Status VariantColumnMerger::arbitrate_type_conflicts(VariantColumn* dst, VariantColumn* src) {
    DCHECK(dst != nullptr);
    DCHECK(src != nullptr);
    if (dst == nullptr || src == nullptr) {
        return Status::InvalidArgument("dst/src should not be null");
    }

    std::unordered_map<std::string_view, size_t> dst_index_by_path;
    std::unordered_map<std::string_view, size_t> src_index_by_path;
    RETURN_IF_ERROR(build_path_index(*dst, &dst_index_by_path));
    RETURN_IF_ERROR(build_path_index(*src, &src_index_by_path));

    auto& dst_types = dst->mutable_shredded_types();
    auto& src_types = src->mutable_shredded_types();
    auto& dst_typed_columns = dst->mutable_typed_columns();
    auto& src_typed_columns = src->mutable_typed_columns();

    for (const auto& [path, src_index] : src_index_by_path) {
        auto dst_it = dst_index_by_path.find(path);
        if (dst_it == dst_index_by_path.end()) {
            continue;
        }
        size_t dst_index = dst_it->second;
        LogicalType dst_type = dst_types[dst_index].type;
        LogicalType src_type = src_types[src_index].type;
        if (dst_types[dst_index] == src_types[src_index]) {
            continue;
        }

        ASSIGN_OR_RETURN(TypeDescriptor common_type_desc,
                         VariantColumnMerger::choose_common_type(dst_types[dst_index], src_types[src_index]));
        LogicalType common_type = common_type_desc.type;

        if (common_type == TYPE_VARIANT) {
            // Cast both sides before mutating either, to avoid leaving dst in an inconsistent
            // state if the src cast fails after dst has already been replaced.
            MutableColumnPtr new_dst_col;
            MutableColumnPtr new_src_col;
            if (dst_type != TYPE_VARIANT) {
                auto casted_dst_st = VariantColumnMerger::cast_typed_column(
                        *dst_typed_columns[dst_index], dst_types[dst_index], TypeDescriptor(TYPE_VARIANT));
                if (!casted_dst_st.ok()) {
                    return Status::NotSupported(strings::Substitute(
                            "failed to hoist dst typed column to VARIANT on path=$0, src_type=$1, dst_type=$2, err=$3",
                            path, logical_type_to_string(src_type), logical_type_to_string(dst_type),
                            casted_dst_st.status().to_string()));
                }
                new_dst_col = std::move(casted_dst_st).value();
            }
            if (src_type != TYPE_VARIANT) {
                auto casted_src_st = VariantColumnMerger::cast_typed_column(
                        *src_typed_columns[src_index], src_types[src_index], TypeDescriptor(TYPE_VARIANT));
                if (!casted_src_st.ok()) {
                    return Status::NotSupported(strings::Substitute(
                            "failed to hoist src typed column to VARIANT on path=$0, src_type=$1, dst_type=$2, err=$3",
                            path, logical_type_to_string(src_type), logical_type_to_string(dst_type),
                            casted_src_st.status().to_string()));
                }
                new_src_col = std::move(casted_src_st).value();
            }
            if (new_dst_col) {
                dst_typed_columns[dst_index] = std::move(new_dst_col);
                dst_types[dst_index] = TypeDescriptor(TYPE_VARIANT);
            }
            if (new_src_col) {
                src_typed_columns[src_index] = std::move(new_src_col);
                src_types[src_index] = TypeDescriptor(TYPE_VARIANT);
            }
            continue;
        }

        if (common_type_desc.is_decimalv3_type()) {
            // Cast both sides before mutating either.
            MutableColumnPtr new_dst_col;
            MutableColumnPtr new_src_col;
            if (dst_types[dst_index] != common_type_desc) {
                auto casted_dst_st = VariantColumnMerger::cast_typed_column(*dst_typed_columns[dst_index],
                                                                            dst_types[dst_index], common_type_desc);
                if (!casted_dst_st.ok()) {
                    return Status::NotSupported(strings::Substitute(
                            "failed to widen dst decimal typed column on path=$0 from $1 to $2, err=$3", path,
                            dst_types[dst_index].debug_string(), common_type_desc.debug_string(),
                            casted_dst_st.status().to_string()));
                }
                new_dst_col = std::move(casted_dst_st).value();
            }
            if (src_types[src_index] != common_type_desc) {
                auto casted_src_st = VariantColumnMerger::cast_typed_column(*src_typed_columns[src_index],
                                                                            src_types[src_index], common_type_desc);
                if (!casted_src_st.ok()) {
                    return Status::NotSupported(strings::Substitute(
                            "failed to widen src decimal typed column on path=$0 from $1 to $2, err=$3", path,
                            src_types[src_index].debug_string(), common_type_desc.debug_string(),
                            casted_src_st.status().to_string()));
                }
                new_src_col = std::move(casted_src_st).value();
            }
            if (new_dst_col) {
                dst_typed_columns[dst_index] = std::move(new_dst_col);
                dst_types[dst_index] = common_type_desc;
            }
            if (new_src_col) {
                src_typed_columns[src_index] = std::move(new_src_col);
                src_types[src_index] = common_type_desc;
            }
            continue;
        }

        // Cast both sides before mutating either.
        MutableColumnPtr new_dst_col;
        MutableColumnPtr new_src_col;
        if (dst_type != common_type) {
            auto casted_dst_st = VariantColumnMerger::cast_typed_column(
                    *dst_typed_columns[dst_index], dst_types[dst_index], TypeDescriptor(common_type));
            if (!casted_dst_st.ok()) {
                return Status::NotSupported(
                        strings::Substitute("failed to widen dst typed column on path=$0 from $1 to $2, err=$3", path,
                                            logical_type_to_string(dst_type), logical_type_to_string(common_type),
                                            casted_dst_st.status().to_string()));
            }
            new_dst_col = std::move(casted_dst_st).value();
        }
        if (src_type != common_type) {
            auto casted_src_st = VariantColumnMerger::cast_typed_column(
                    *src_typed_columns[src_index], src_types[src_index], TypeDescriptor(common_type));
            if (!casted_src_st.ok()) {
                return Status::NotSupported(
                        strings::Substitute("failed to widen src typed column on path=$0 from $1 to $2, err=$3", path,
                                            logical_type_to_string(src_type), logical_type_to_string(common_type),
                                            casted_src_st.status().to_string()));
            }
            new_src_col = std::move(casted_src_st).value();
        }
        if (new_dst_col) {
            dst_typed_columns[dst_index] = std::move(new_dst_col);
            dst_types[dst_index] = TypeDescriptor(common_type);
        }
        if (new_src_col) {
            src_typed_columns[src_index] = std::move(new_src_col);
            src_types[src_index] = TypeDescriptor(common_type);
        }
    }
    return Status::OK();
}

static StatusOr<bool> has_overlapped_type_conflict(const VariantColumn& dst, const VariantColumn& src) {
    std::unordered_map<std::string_view, size_t> dst_index_by_path;
    RETURN_IF_ERROR(build_path_index(dst, &dst_index_by_path));
    for (size_t i = 0; i < src.shredded_paths().size(); ++i) {
        auto dst_it = dst_index_by_path.find(src.shredded_paths()[i]);
        if (dst_it == dst_index_by_path.end()) {
            continue;
        }
        if (dst.shredded_types()[dst_it->second] != src.shredded_types()[i]) {
            return true;
        }
    }
    return false;
}

static Status append_with_schema_arbitration(VariantColumn* dst, const VariantColumn& src) {
    DCHECK(dst != nullptr);
    if (dst->is_equal_schema(&src)) {
        dst->append(src, 0, src.size());
        return Status::OK();
    }

    ASSIGN_OR_RETURN(bool has_conflict, has_overlapped_type_conflict(*dst, src));
    if (!has_conflict) {
        dst->append(src, 0, src.size());
        return Status::OK();
    }

    auto src_mutable = VariantColumn::deep_copy_shredded(src);
    auto* src_variant = down_cast<VariantColumn*>(src_mutable.get());
    RETURN_IF_ERROR(VariantColumnMerger::arbitrate_type_conflicts(dst, src_variant));
    dst->append(*src_variant, 0, src_variant->size());
    return Status::OK();
}

Status VariantColumnMerger::merge_into(VariantColumn* dst, const VariantColumn& src) {
    if (dst == nullptr) {
        return Status::InvalidArgument("dst variant column is null");
    }
    RETURN_IF_ERROR(check_duplicate_shredded_paths(*dst));
    RETURN_IF_ERROR(check_duplicate_shredded_paths(src));
    RETURN_IF_ERROR(dst->ensure_base_variant_column());

    return append_with_schema_arbitration(dst, src);
}

StatusOr<MutableColumnPtr> VariantColumnMerger::merge(const Columns& inputs) {
    if (inputs.empty()) {
        return Status::InvalidArgument("variant column merger requires non-empty input");
    }

    auto merged = VariantColumn::create();
    auto* merged_variant = down_cast<VariantColumn*>(merged.get());
    ASSIGN_OR_RETURN(const auto* first_variant, get_variant_data_column(inputs[0].get()));
    if (!merged_variant->align_schema_from(*first_variant)) {
        return Status::InvalidArgument("failed to initialize merged variant schema");
    }

    for (const auto& input : inputs) {
        ASSIGN_OR_RETURN(const auto* variant, get_variant_data_column(input.get()));
        RETURN_IF_ERROR(merge_into(merged_variant, *variant));
    }

    return merged;
}

} // namespace starrocks
