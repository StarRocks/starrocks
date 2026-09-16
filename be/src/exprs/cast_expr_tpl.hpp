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

#pragma once

#include "exprs/cast_expr.h"

#ifdef STARROCKS_JIT_ENABLE
#include <llvm/ADT/APInt.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Value.h>
#endif

#include <cctz/time_zone.h>
#include <ryu/ryu.h>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "base/time/date_func.h"
#include "base/types/int128.h"
#include "base/types/numeric_types.h"
#include "base/utility/mysql_global.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_converter.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "column/variant_column.h"
#include "column/variant_converter.h"
#include "column/variant_encoder.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/binary_function.h"
#include "exprs/column_ref.h"
#include "exprs/decimal_cast_expr.h"
#include "exprs/expr_context.h"
#include "exprs/unary_function.h"
#include "gutil/casts.h"
#include "runtime/exception.h"
#include "runtime/runtime_state.h"
#include "types/datetime_value.h"
#include "types/hll.h"
#include "types/json_value.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"

#ifdef STARROCKS_JIT_ENABLE
#include "exprs/jit/expr_jit_codegen.h"
#include "exprs/jit/ir_helper.h"
#endif

namespace starrocks {

#define THROW_RUNTIME_ERROR_WITH_TYPE(TYPE)              \
    std::stringstream ss;                                \
    ss << "not supported type " << type_to_string(TYPE); \
    throw RuntimeException(ss.str())

#define THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FROMTYPE, TOTYPE, VALUE) \
    std::stringstream ss;                                                 \
    ss << "cast from " << type_to_string(FROMTYPE) << "(" << VALUE << ")" \
       << " to " << type_to_string(TOTYPE) << " failed";                  \
    throw RuntimeException(ss.str())

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException = false>
struct CastFn {
    static ColumnPtr cast_fn(ColumnPtr&& column);
};

// clang-format off
// All cast implements
#define SELF_CAST(FROM_TYPE)                                                    \
    template <bool AllowThrowException>                                         \
    struct CastFn<FROM_TYPE, FROM_TYPE, AllowThrowException> {                  \
        static ColumnPtr cast_fn(ColumnPtr&& column) { return Column::mutate(std::move(column)); } \
    };

#define UNARY_FN_CAST(FROM_TYPE, TO_TYPE, UNARY_IMPL)                                                        \
    template <bool AllowThrowException>                                                                      \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                                                 \
        static ColumnPtr cast_fn(ColumnPtr&& column) {                                                        \
            return VectorizedStrictUnaryFunction<UNARY_IMPL>::template evaluate<FROM_TYPE, TO_TYPE>(column); \
        }                                                                                                    \
    };

#define UNARY_FN_CAST_VALID(FROM_TYPE, TO_TYPE, UNARY_IMPL)                                                            \
    template <bool AllowThrowException>                                                                                \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                                                           \
        static ColumnPtr cast_fn(ColumnPtr&& column) {                                                                  \
            if constexpr (std::numeric_limits<RunTimeCppType<TO_TYPE>>::max() <                                        \
                          std::numeric_limits<RunTimeCppType<FROM_TYPE>>::max()) {                                     \
                if constexpr (!AllowThrowException) {                                                                  \
                    return VectorizedInputCheckUnaryFunction<UNARY_IMPL, NumberCheck>::template evaluate<FROM_TYPE,    \
                                                                                                         TO_TYPE>(     \
                            column);                                                                                   \
                } else {                                                                                               \
                    return NullAwareInputCheckUnaryFunction<UNARY_IMPL, NumberCheckWithThrowException,                 \
                                                            NumberCheck>::template evaluate<FROM_TYPE, TO_TYPE>(       \
                            column);                                                                                   \
                }                                                                                                      \
            }                                                                                                          \
            return VectorizedStrictUnaryFunction<UNARY_IMPL>::template evaluate<FROM_TYPE, TO_TYPE>(column);           \
        }                                                                                                              \
    };

#define UNARY_FN_CAST_TIME_VALID(FROM_TYPE, TO_TYPE, UNARY_IMPL)                                                    \
    template <bool AllowThrowException>                                                                             \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                                                        \
        static ColumnPtr cast_fn(ColumnPtr&& column) {                                                               \
            return VectorizedInputCheckUnaryFunction<UNARY_IMPL, TimeCheck>::template evaluate<FROM_TYPE, TO_TYPE>( \
                    column);                                                                                        \
        }                                                                                                           \
    };

#define CUSTOMIZE_FN_CAST(FROM_TYPE, TO_TYPE, CUSTOMIZE_IMPL)                       \
    template <bool AllowThrowException>                                             \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                        \
        static ColumnPtr cast_fn(ColumnPtr&& column) {                               \
            return CUSTOMIZE_IMPL<FROM_TYPE, TO_TYPE, AllowThrowException>(column); \
        }                                                                           \
    };
// clang-format on

DEFINE_UNARY_FN_WITH_IMPL(TimeCheck, value) {
    return ((uint64_t)value % 100 > 59 || (uint64_t)value % 10000 > 5959);
}

// boolean cast implements
DEFINE_UNARY_FN_WITH_IMPL(ImplicitToBoolean, value) {
    return value != 0;
}
DEFINE_UNARY_FN_WITH_IMPL(DecimalToBoolean, value) {
    return value != DecimalV2Value::ZERO;
}
DEFINE_UNARY_FN_WITH_IMPL(DateToBoolean, value) {
    return value.to_date_literal() != 0;
}
DEFINE_UNARY_FN_WITH_IMPL(TimestampToBoolean, value) {
    return value.to_timestamp_literal() != 0;
}
DEFINE_UNARY_FN_WITH_IMPL(TimeToNumber, value) {
    return timestamp::time_to_literal(value);
}

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_to_json_fn(ColumnPtr& column) {
    ColumnViewer<FromType> viewer(column);
    ColumnBuilder<TYPE_JSON> builder(viewer.size());

    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        JsonValue value;
        bool overflow = false;
        if constexpr (lt_is_integer<FromType>) {
            constexpr int64_t min = RunTimeTypeLimits<TYPE_BIGINT>::min_value();
            constexpr int64_t max = RunTimeTypeLimits<TYPE_BIGINT>::max_value();
            overflow = viewer.value(row) < min || viewer.value(row) > max;
            value = JsonValue::from_int(viewer.value(row));
        } else if constexpr (lt_is_float<FromType>) {
            constexpr double min = RunTimeTypeLimits<TYPE_DOUBLE>::min_value();
            constexpr double max = RunTimeTypeLimits<TYPE_DOUBLE>::max_value();
            overflow = viewer.value(row) < min || viewer.value(row) > max;
            value = JsonValue::from_double(viewer.value(row));
        } else if constexpr (lt_is_boolean<FromType>) {
            value = JsonValue::from_bool(viewer.value(row));
        } else if constexpr (lt_is_string<FromType>) {
            auto maybe = JsonValue::parse_json_or_string(viewer.value(row));
            if (maybe.ok()) {
                value = maybe.value();
            } else {
                overflow = true;
            }
        } else if constexpr (CastToString::extend_type<RunTimeCppType<FromType>>()) {
            // Cast these types to string in json
            auto v = viewer.value(row);
            std::string str = CastToString::apply<RunTimeCppType<FromType>, std::string>(v);
            value = JsonValue::from_string(str);
        } else if constexpr (lt_is_variant<FromType>) {
            const auto* variant_data_column =
                    down_cast<const VariantColumn*>(ColumnHelper::get_data_column(column.get()));
            const size_t variant_row = column->is_constant() ? 0 : row;
            VariantRowRef row_ref;
            if (!variant_data_column->try_get_row_ref(variant_row, &row_ref)) {
                VariantRowValue variant_buffer;
                const VariantRowValue* variant = variant_data_column->get_row_value(variant_row, &variant_buffer);
                if (variant == nullptr) {
                    overflow = true;
                } else {
                    row_ref = variant->as_ref();
                }
            }

            if (!overflow) {
                std::stringstream ss;
                auto st = VariantUtil::variant_to_json(row_ref.get_metadata(), row_ref.get_value(), ss,
                                                       cctz::local_time_zone());
                if (!st.ok()) {
                    overflow = true;
                } else {
                    auto parsed = JsonValue::parse_json_or_string(ss.str());
                    if (parsed.ok()) {
                        value = parsed.value();
                    } else {
                        overflow = true;
                    }
                }
            }
        } else {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPE(FromType);
            }
            DCHECK(false) << "not supported type " << FromType;
        }
        if (overflow || value.is_null()) {
            if constexpr (AllowThrowException) {
                if constexpr (FromType == TYPE_LARGEINT) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, int128_to_string(viewer.value(row)));
                } else {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, viewer.value(row));
                }
            }
            builder.append_null();
        } else {
            builder.append(std::move(value));
        }
    }
    return builder.build(column->is_constant());
}

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_json_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_JSON> viewer(column);
    ColumnBuilder<ToType> builder(viewer.size());

    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        JsonValue* json = viewer.value(row);
        auto st = cast_vpjson_to<ToType, AllowThrowException>(json->to_vslice(), builder);
        if (!st.ok()) {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, json->to_string().value_or(""));
            }
            builder.append_null();
        }
    }

    return builder.build(column->is_constant());
}

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_variant_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_VARIANT> viewer(column);
    ColumnBuilder<ToType> builder(viewer.size());
    const auto* variant_data_column = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(column.get()));

    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        const size_t variant_row = column->is_constant() ? 0 : row;
        VariantRowRef row_ref;
        if (!variant_data_column->try_get_row_ref(variant_row, &row_ref)) {
            VariantRowValue variant_buffer;
            const VariantRowValue* variant = variant_data_column->get_row_value(variant_row, &variant_buffer);
            if (variant == nullptr) {
                builder.append_null();
                continue;
            }
            row_ref = variant->as_ref();
        }

        auto status =
                VariantRowConverter::cast_to<ToType, AllowThrowException>(row_ref, cctz::local_time_zone(), builder);
        if (!status.ok()) {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, row_ref.to_owned().to_string());
            }
            builder.append_null();
        }
    }

    return builder.build(column->is_constant());
}

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_to_variant_fn(ColumnPtr& column) {
    if constexpr (FromType != TYPE_JSON) {
        THROW_RUNTIME_ERROR_WITH_TYPE(ToType);
    }

    ColumnViewer<TYPE_JSON> viewer(column);
    ColumnBuilder<TYPE_VARIANT> builder(viewer.size());

    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        JsonValue* json = viewer.value(row);
        if (json == nullptr) {
            builder.append_null();
            continue;
        }

        auto encoded = VariantEncoder::encode_json_to_variant(*json);
        if (!encoded.ok()) {
            VLOG_ROW << "encode json to variant failed: " << encoded.status();
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, json->to_string_uncheck());
            }
            builder.append_null();
            continue;
        }
        builder.append(std::move(encoded.value()));
    }

    return builder.build(column->is_constant());
}

SELF_CAST(TYPE_BOOLEAN);
UNARY_FN_CAST(TYPE_TINYINT, TYPE_BOOLEAN, ImplicitToBoolean);
UNARY_FN_CAST(TYPE_SMALLINT, TYPE_BOOLEAN, ImplicitToBoolean);
UNARY_FN_CAST(TYPE_INT, TYPE_BOOLEAN, ImplicitToBoolean);
UNARY_FN_CAST(TYPE_BIGINT, TYPE_BOOLEAN, ImplicitToBoolean);
UNARY_FN_CAST(TYPE_LARGEINT, TYPE_BOOLEAN, ImplicitToBoolean);
UNARY_FN_CAST(TYPE_FLOAT, TYPE_BOOLEAN, ImplicitToBoolean);
UNARY_FN_CAST(TYPE_DOUBLE, TYPE_BOOLEAN, ImplicitToBoolean);
UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_BOOLEAN, DecimalToBoolean);
UNARY_FN_CAST(TYPE_DATE, TYPE_BOOLEAN, DateToBoolean);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_BOOLEAN, TimestampToBoolean);
UNARY_FN_CAST(TYPE_TIME, TYPE_BOOLEAN, ImplicitToBoolean);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_BOOLEAN, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_BOOLEAN, cast_from_variant_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_VARIANT, cast_to_variant_fn);

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_string_to_bool_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_VARCHAR> viewer(column);
    ColumnBuilder<TYPE_BOOLEAN> builder(viewer.size());

    StringParser::ParseResult result;

    if (!column->has_null()) {
        for (int row = 0; row < viewer.size(); ++row) {
            auto value = viewer.value(row);
            auto r = StringParser::string_to_int<int32_t>(value.data, value.size, &result);

            if (result != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r)) {
                bool b = StringParser::string_to_bool(value.data, value.size, &result);
                if constexpr (AllowThrowException) {
                    if (result != StringParser::PARSE_SUCCESS) {
                        THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_BOOLEAN, value.to_string());
                    }
                }
                builder.append(b, result != StringParser::PARSE_SUCCESS);
            } else {
                builder.append(r != 0);
            }
        }
    } else {
        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            auto value = viewer.value(row);
            auto r = StringParser::string_to_int<int32_t>(value.data, value.size, &result);

            if (result != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r)) {
                bool b = StringParser::string_to_bool(value.data, value.size, &result);
                if constexpr (AllowThrowException) {
                    if (result != StringParser::PARSE_SUCCESS) {
                        THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_BOOLEAN, value.to_string());
                    }
                }
                builder.append(b, result != StringParser::PARSE_SUCCESS);
            } else {
                builder.append(r != 0);
            }
        }
    }

    return builder.build(column->is_constant());
}
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_BOOLEAN, cast_from_string_to_bool_fn);

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_string_to_hll_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_VARCHAR> viewer(column);
    ColumnBuilder<TYPE_HLL> builder(viewer.size());
    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto value = viewer.value(row);
        if (!HyperLogLog::is_valid(value)) {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_HLL, value.to_string());
            }
            builder.append_null();
        } else {
            HyperLogLog hll;
            hll.deserialize(value);
            builder.append(&hll);
        }
    }

    return builder.build(column->is_constant());
}
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_HLL, cast_from_string_to_hll_fn);

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_string_to_bitmap_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_VARCHAR> viewer(column);
    ColumnBuilder<TYPE_OBJECT> builder(viewer.size());
    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto value = viewer.value(row);

        BitmapValue bitmap;
        if (bitmap.valid_and_deserialize(value.data, value.size)) {
            builder.append(&bitmap);
        } else {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_OBJECT, value.to_string());
            }
            builder.append_null();
        }
    }

    return builder.build(column->is_constant());
}
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_OBJECT, cast_from_string_to_bitmap_fn);

// all int(tinyint, smallint, int, bigint, largeint) cast implements
DEFINE_UNARY_FN_WITH_IMPL(ImplicitToNumber, value) {
    return value;
}

DEFINE_UNARY_FN_WITH_IMPL(NumberCheck, value) {
    return check_signed_number_overflow<Type, ResultType>(value);
}

DEFINE_UNARY_FN_WITH_IMPL(NumberCheckWithThrowException, value) {
    // std::numeric_limits<T>::lowest() is a finite value x such that there is no other
    // finite value y where y < x.
    // This is different from std::numeric_limits<T>::min() for floating-point types.
    // So we use lowest instead of min for lower bound of all types.
    const auto result = NumberCheck::apply<Type, ResultType>(value);
    if (result) {
        std::stringstream ss;
        if constexpr (std::is_same_v<Type, __int128_t>) {
            ss << int128_to_string(value) << " conflict with range of "
               << "(" << int128_to_string((Type)std::numeric_limits<ResultType>::lowest()) << ", "
               << int128_to_string((Type)std::numeric_limits<ResultType>::max()) << ")";
        } else {
            ss << value << " conflict with range of "
               << "(" << (Type)std::numeric_limits<ResultType>::lowest() << ", "
               << (Type)std::numeric_limits<ResultType>::max() << ")";
        }
        throw std::runtime_error(ss.str());
    }
    return result;
}

DEFINE_UNARY_FN_WITH_IMPL(DateToNumber, value) {
    return value.to_date_literal();
}

DEFINE_UNARY_FN_WITH_IMPL(TimestampToNumber, value) {
    return value.to_timestamp_literal();
}

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
ColumnPtr cast_int_from_string_fn(ColumnPtr& column) {
    StringParser::ParseResult result;
    int sz = column.get()->size();
    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(sz);
    }
    if (column->is_constant()) {
        auto* input = ColumnHelper::get_binary_column(column.get());
        auto slice = input->get_slice(0);
        auto r = StringParser::string_to_int<RunTimeCppType<ToType>>(slice.data, slice.size, &result);
        if (result != StringParser::PARSE_SUCCESS) {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, slice.to_string());
            }
            return ColumnHelper::create_const_null_column(sz);
        }
        return ColumnHelper::create_const_column<ToType>(r, sz);
    }
    auto res_data_column = RunTimeColumnType<ToType>::create();
    res_data_column->resize(sz);
    auto& res_data = res_data_column->get_data();
    if (column->is_nullable()) {
        const auto* input_column = down_cast<const NullableColumn*>(column.get());
        auto null_column_ptr = input_column->null_column()->clone();
        auto* null_column = down_cast<NullColumn*>(null_column_ptr.get());
        const auto* data_column = down_cast<const BinaryColumn*>(input_column->data_column_raw_ptr());
        auto& null_data = null_column->get_data();
        for (int i = 0; i < sz; ++i) {
            if (!null_data[i]) {
                auto slice = data_column->get_slice(i);
                res_data[i] = StringParser::string_to_int<RunTimeCppType<ToType>>(slice.data, slice.size, &result);
                if constexpr (AllowThrowException) {
                    if (result != StringParser::PARSE_SUCCESS) {
                        THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, slice.to_string());
                    }
                }
                null_data[i] = (result != StringParser::PARSE_SUCCESS);
            }
        }
        return NullableColumn::create(std::move(res_data_column), std::move(null_column));
    } else {
        NullColumn::MutablePtr null_column = NullColumn::create(sz);
        auto& null_data = null_column->get_data();
        const auto* data_column = down_cast<const BinaryColumn*>(column.get());

        bool has_null = false;
        for (int i = 0; i < sz; ++i) {
            auto slice = data_column->get_slice(i);
            res_data[i] = StringParser::string_to_int<RunTimeCppType<ToType>>(slice.data, slice.size, &result);
            null_data[i] = (result != StringParser::PARSE_SUCCESS);
            if constexpr (AllowThrowException) {
                if (result != StringParser::PARSE_SUCCESS) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, slice.to_string());
                }
            }
            has_null |= (result != StringParser::PARSE_SUCCESS);
        }
        if (!has_null) {
            return res_data_column;
        }
        return NullableColumn::create(std::move(res_data_column), std::move(null_column));
    }
}

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
ColumnPtr cast_float_from_string_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_VARCHAR> viewer(column);
    ColumnBuilder<ToType> builder(viewer.size());

    StringParser::ParseResult result;

    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto value = viewer.value(row);
        auto r = StringParser::string_to_float<RunTimeCppType<ToType>>(value.data, value.size, &result);

        bool is_null = (result != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r));
        if constexpr (AllowThrowException) {
            if (is_null) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, value.to_string());
            }
        }

        builder.append(r, is_null);
    }

    return builder.build(column->is_constant());
}

// tinyint
SELF_CAST(TYPE_TINYINT);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_SMALLINT, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_INT, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_BIGINT, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_LARGEINT, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_FLOAT, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_DOUBLE, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_TINYINT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DATE, TYPE_TINYINT, DateToNumber);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_TINYINT, TimestampToNumber);
UNARY_FN_CAST(TYPE_TIME, TYPE_TINYINT, TimeToNumber);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_TINYINT, cast_int_from_string_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_TINYINT, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_TINYINT, cast_from_variant_fn);

// smallint
SELF_CAST(TYPE_SMALLINT);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_TINYINT, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_INT, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_BIGINT, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_LARGEINT, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_FLOAT, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_DOUBLE, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_SMALLINT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DATE, TYPE_SMALLINT, DateToNumber);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_SMALLINT, TimestampToNumber);
UNARY_FN_CAST(TYPE_TIME, TYPE_SMALLINT, TimeToNumber);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_SMALLINT, cast_int_from_string_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_SMALLINT, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_SMALLINT, cast_from_variant_fn);

// int
SELF_CAST(TYPE_INT);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_INT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_TINYINT, TYPE_INT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_SMALLINT, TYPE_INT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_BIGINT, TYPE_INT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_LARGEINT, TYPE_INT, ImplicitToNumber);

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
UNARY_FN_CAST_VALID(TYPE_FLOAT, TYPE_INT, ImplicitToNumber);
DIAGNOSTIC_POP

UNARY_FN_CAST_VALID(TYPE_DOUBLE, TYPE_INT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_INT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DATE, TYPE_INT, DateToNumber);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_INT, TimestampToNumber);
UNARY_FN_CAST(TYPE_TIME, TYPE_INT, TimeToNumber);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_INT, cast_int_from_string_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_INT, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_INT, cast_from_variant_fn);

// bigint
SELF_CAST(TYPE_BIGINT);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_BIGINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_TINYINT, TYPE_BIGINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_SMALLINT, TYPE_BIGINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_INT, TYPE_BIGINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_LARGEINT, TYPE_BIGINT, ImplicitToNumber);

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
UNARY_FN_CAST_VALID(TYPE_FLOAT, TYPE_BIGINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_DOUBLE, TYPE_BIGINT, ImplicitToNumber);
DIAGNOSTIC_POP

UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_BIGINT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DATE, TYPE_BIGINT, DateToNumber);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_BIGINT, TimestampToNumber);
UNARY_FN_CAST(TYPE_TIME, TYPE_BIGINT, TimeToNumber);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_BIGINT, cast_int_from_string_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_BIGINT, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_BIGINT, cast_from_variant_fn);

// largeint
SELF_CAST(TYPE_LARGEINT);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_LARGEINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_TINYINT, TYPE_LARGEINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_SMALLINT, TYPE_LARGEINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_INT, TYPE_LARGEINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_BIGINT, TYPE_LARGEINT, ImplicitToNumber);

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
UNARY_FN_CAST_VALID(TYPE_FLOAT, TYPE_LARGEINT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_DOUBLE, TYPE_LARGEINT, ImplicitToNumber);
DIAGNOSTIC_POP

UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_LARGEINT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DATE, TYPE_LARGEINT, DateToNumber);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_LARGEINT, TimestampToNumber);
UNARY_FN_CAST(TYPE_TIME, TYPE_LARGEINT, TimeToNumber);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_LARGEINT, cast_int_from_string_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_LARGEINT, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_LARGEINT, cast_from_variant_fn);

// float
SELF_CAST(TYPE_FLOAT);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_FLOAT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_TINYINT, TYPE_FLOAT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_SMALLINT, TYPE_FLOAT, ImplicitToNumber);

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
UNARY_FN_CAST_VALID(TYPE_INT, TYPE_FLOAT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_BIGINT, TYPE_FLOAT, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_LARGEINT, TYPE_FLOAT, ImplicitToNumber);
DIAGNOSTIC_POP

UNARY_FN_CAST_VALID(TYPE_DOUBLE, TYPE_FLOAT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_FLOAT, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DATE, TYPE_FLOAT, DateToNumber);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_FLOAT, TimestampToNumber);
UNARY_FN_CAST(TYPE_TIME, TYPE_FLOAT, TimeToNumber);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_FLOAT, cast_float_from_string_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_FLOAT, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_FLOAT, cast_from_variant_fn);

// double
SELF_CAST(TYPE_DOUBLE);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_DOUBLE, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_TINYINT, TYPE_DOUBLE, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_SMALLINT, TYPE_DOUBLE, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_INT, TYPE_DOUBLE, ImplicitToNumber);

DIAGNOSTIC_PUSH
#if defined(__clang__)
DIAGNOSTIC_IGNORE("-Wimplicit-int-float-conversion")
#endif
UNARY_FN_CAST_VALID(TYPE_BIGINT, TYPE_DOUBLE, ImplicitToNumber);
UNARY_FN_CAST_VALID(TYPE_LARGEINT, TYPE_DOUBLE, ImplicitToNumber);
DIAGNOSTIC_POP

UNARY_FN_CAST_VALID(TYPE_FLOAT, TYPE_DOUBLE, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DECIMALV2, TYPE_DOUBLE, ImplicitToNumber);
UNARY_FN_CAST(TYPE_DATE, TYPE_DOUBLE, DateToNumber);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_DOUBLE, TimestampToNumber);
UNARY_FN_CAST(TYPE_TIME, TYPE_DOUBLE, TimeToNumber);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_DOUBLE, cast_float_from_string_fn);
CUSTOMIZE_FN_CAST(TYPE_JSON, TYPE_DOUBLE, cast_from_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_DOUBLE, cast_from_variant_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_DATE, cast_from_variant_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_DATETIME, cast_from_variant_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_TIME, cast_from_variant_fn);

// decimal
DEFINE_UNARY_FN_WITH_IMPL(NumberToDecimal, value) {
    return DecimalV2Value(value, 0);
}

DEFINE_UNARY_FN_WITH_IMPL(FloatToDecimal, value) {
    DecimalV2Value dv;
    dv.assign_from_float(value);
    return dv;
}

DEFINE_UNARY_FN_WITH_IMPL(DoubleToDecimal, value) {
    DecimalV2Value dv;
    dv.assign_from_double(value);
    return dv;
}

DEFINE_UNARY_FN_WITH_IMPL(DateToDecimal, value) {
    return DecimalV2Value(value.to_date_literal(), 0);
}

DEFINE_UNARY_FN_WITH_IMPL(TimestampToDecimal, value) {
    return DecimalV2Value(value.to_timestamp_literal(), 0);
}
DEFINE_UNARY_FN_WITH_IMPL(TimeToDecimal, value) {
    return DecimalV2Value(timestamp::time_to_literal(value), 0);
}

SELF_CAST(TYPE_DECIMALV2);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_DECIMALV2, NumberToDecimal);
UNARY_FN_CAST(TYPE_TINYINT, TYPE_DECIMALV2, NumberToDecimal);
UNARY_FN_CAST(TYPE_SMALLINT, TYPE_DECIMALV2, NumberToDecimal);
UNARY_FN_CAST(TYPE_INT, TYPE_DECIMALV2, NumberToDecimal);
UNARY_FN_CAST(TYPE_BIGINT, TYPE_DECIMALV2, NumberToDecimal);
UNARY_FN_CAST(TYPE_LARGEINT, TYPE_DECIMALV2, NumberToDecimal);
UNARY_FN_CAST(TYPE_FLOAT, TYPE_DECIMALV2, FloatToDecimal);
UNARY_FN_CAST(TYPE_DOUBLE, TYPE_DECIMALV2, DoubleToDecimal);
UNARY_FN_CAST(TYPE_TIME, TYPE_DECIMALV2, TimeToDecimal);
UNARY_FN_CAST(TYPE_DATE, TYPE_DECIMALV2, DateToDecimal);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_DECIMALV2, TimestampToDecimal);

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_string_to_decimalv2_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_VARCHAR> viewer(column);
    ColumnBuilder<TYPE_DECIMALV2> builder(viewer.size());

    if (!column->has_null()) {
        for (int row = 0; row < viewer.size(); ++row) {
            auto value = viewer.value(row);
            DecimalV2Value v;

            bool ret = v.parse_from_str(value.data, value.size);
            if constexpr (AllowThrowException) {
                if (ret) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_DECIMALV2, value.to_string());
                }
            }
            builder.append(v, ret);
        }
    } else {
        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            auto value = viewer.value(row);
            DecimalV2Value v;

            bool ret = v.parse_from_str(value.data, value.size);
            if constexpr (AllowThrowException) {
                if (ret) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_DECIMALV2, value.to_string());
                }
            }
            builder.append(v, ret);
        }
    }

    return builder.build(column->is_constant());
}
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_DECIMALV2, cast_from_string_to_decimalv2_fn);

// date
template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
ColumnPtr cast_to_date_fn(ColumnPtr& column) {
    ColumnViewer<FromType> viewer(column);
    ColumnBuilder<TYPE_DATE> builder(viewer.size());

    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto value = viewer.value(row);
        DateValue dv;

        bool ret = dv.from_date_literal_with_check((int64_t)value);
        if constexpr (AllowThrowException) {
            if (!ret) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, (int64_t)value);
            }
        }
        builder.append(dv, !ret);
    }

    return builder.build(column->is_constant());
}

// for fast
DEFINE_UNARY_FN_WITH_IMPL(TimestampToDate, value) {
    return DateValue{timestamp::to_julian(value._timestamp)};
}

SELF_CAST(TYPE_DATE);
CUSTOMIZE_FN_CAST(TYPE_BOOLEAN, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_TINYINT, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_SMALLINT, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_INT, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_BIGINT, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_LARGEINT, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_FLOAT, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_DOUBLE, TYPE_DATE, cast_to_date_fn);
CUSTOMIZE_FN_CAST(TYPE_DECIMALV2, TYPE_DATE, cast_to_date_fn);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_DATE, TimestampToDate);
// Time to date need rewrite CastExpr

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_string_to_date_fn(ColumnPtr& column) {
    ColumnViewer<TYPE_VARCHAR> viewer(column);
    ColumnBuilder<TYPE_DATE> builder(viewer.size());

    if (!column->has_null()) {
        for (int row = 0; row < viewer.size(); ++row) {
            auto value = viewer.value(row);
            DateValue v;

            bool right = v.from_string(value.data, value.size);
            if constexpr (AllowThrowException) {
                if (!right) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_DATE, value.to_string());
                }
            }
            builder.append(v, !right);
        }
    } else {
        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            auto value = viewer.value(row);
            DateValue v;

            bool right = v.from_string(value.data, value.size);
            if constexpr (AllowThrowException) {
                if (!right) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_DATE, value.to_string());
                }
            }
            builder.append(v, !right);
        }
    }
    return builder.build(column->is_constant());
}
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_DATE, cast_from_string_to_date_fn);

// datetime(timestamp)
template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
ColumnPtr cast_to_timestamp_fn(ColumnPtr& column) {
    ColumnViewer<FromType> viewer(column);
    ColumnBuilder<TYPE_DATETIME> builder(viewer.size());

    for (int row = 0; row < viewer.size(); ++row) {
        if (viewer.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto value = viewer.value(row);
        TimestampValue tv;
        bool ret;
        if constexpr (lt_is_decimalv2<FromType>) {
            ret = value.value() > 0;
        } else {
            ret = value > 0;
        }
        ret = ret && tv.from_timestamp_literal_with_check(value);
        if constexpr (AllowThrowException) {
            if (!ret) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, (int64_t)value);
            }
        }
        builder.append(tv, !ret);
    }

    return builder.build(column->is_constant());
}

// for fast
DEFINE_UNARY_FN_WITH_IMPL(DateToTimestmap, value) {
    return TimestampValue{date::to_timestamp(value._julian)};
}

SELF_CAST(TYPE_DATETIME);
CUSTOMIZE_FN_CAST(TYPE_BOOLEAN, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_TINYINT, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_SMALLINT, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_INT, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_BIGINT, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_LARGEINT, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_FLOAT, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_DOUBLE, TYPE_DATETIME, cast_to_timestamp_fn);
CUSTOMIZE_FN_CAST(TYPE_DECIMALV2, TYPE_DATETIME, cast_to_timestamp_fn);
UNARY_FN_CAST(TYPE_DATE, TYPE_DATETIME, DateToTimestmap);
// Time to datetime need rewrite CastExpr

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_string_to_datetime_fn(ColumnPtr& column) {
    const int num_rows = column->size();

    if (column->only_null()) {
        return ColumnHelper::create_const_null_column(num_rows);
    }

    if (column->is_constant()) {
        const auto* input_column = ColumnHelper::get_binary_column(column.get());
        const auto slice_value = input_column->get_slice(0);

        TimestampValue datetime_value;
        const bool success = datetime_value.from_string(slice_value.data, slice_value.size);

        if (!success) {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, slice_value.to_string());
            }
            return ColumnHelper::create_const_null_column(num_rows);
        }
        return ColumnHelper::create_const_column<ToType>(datetime_value, num_rows);
    }

    auto res_data_column = RunTimeColumnType<ToType>::create();
    res_data_column->resize(num_rows);
    auto& res_data = res_data_column->get_data();

    if (column->is_nullable()) {
        const auto* input_column = down_cast<const NullableColumn*>(column.get());
        const auto* data_column = down_cast<const BinaryColumn*>(input_column->data_column_raw_ptr());

        auto null_column_ptr = input_column->null_column()->clone();
        auto* null_column = down_cast<NullColumn*>(null_column_ptr.get());
        auto& null_data = null_column->get_data();

        for (int i = 0; i < num_rows; ++i) {
            if (!null_data[i]) {
                auto slice_value = data_column->get_slice(i);
                const bool success = res_data[i].from_string(slice_value.data, slice_value.size);

                if constexpr (AllowThrowException) {
                    if (!success) {
                        THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, slice_value.to_string());
                    }
                }
                null_data[i] = !success;
            }
        }
        return NullableColumn::create(std::move(res_data_column), std::move(null_column));
    } else {
        const auto* data_column = down_cast<const BinaryColumn*>(column.get());
        NullColumn::MutablePtr null_column = NullColumn::create(num_rows);
        auto& null_data = null_column->get_data();

        bool has_null = false;
        for (int i = 0; i < num_rows; ++i) {
            auto slice_value = data_column->get_slice(i);
            const bool success = res_data[i].from_string(slice_value.data, slice_value.size);

            if constexpr (AllowThrowException) {
                if (!success) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, slice_value.to_string());
                }
            }
            null_data[i] = !success;
            has_null |= !success;
        }
        if (!has_null) {
            return res_data_column;
        }
        return NullableColumn::create(std::move(res_data_column), std::move(null_column));
    }
}
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_DATETIME, cast_from_string_to_datetime_fn);

// time
DEFINE_UNARY_FN_WITH_IMPL(DatetimeToTime, value) {
    Timestamp timestamp = timestamp::to_time(value.timestamp());
    return timestamp / USECS_PER_SEC;
}

DEFINE_UNARY_FN_WITH_IMPL(DateToTime, value) {
    return 0;
}

DEFINE_UNARY_FN_WITH_IMPL(NumberToTime, value) {
    uint64_t data = value;
    uint64_t hour = data / 10000;
    uint64_t min = (data / 100) % 100;
    uint64_t sec = data % 100;
    return (hour * 60 + min) * 60 + sec;
}

SELF_CAST(TYPE_TIME);
UNARY_FN_CAST(TYPE_BOOLEAN, TYPE_TIME, NumberToTime);
UNARY_FN_CAST(TYPE_TINYINT, TYPE_TIME, NumberToTime);
UNARY_FN_CAST_TIME_VALID(TYPE_SMALLINT, TYPE_TIME, NumberToTime);
UNARY_FN_CAST_TIME_VALID(TYPE_INT, TYPE_TIME, NumberToTime);
UNARY_FN_CAST_TIME_VALID(TYPE_BIGINT, TYPE_TIME, NumberToTime);
UNARY_FN_CAST_TIME_VALID(TYPE_LARGEINT, TYPE_TIME, NumberToTime);
UNARY_FN_CAST_TIME_VALID(TYPE_DOUBLE, TYPE_TIME, NumberToTime);
UNARY_FN_CAST_TIME_VALID(TYPE_FLOAT, TYPE_TIME, NumberToTime);
UNARY_FN_CAST_TIME_VALID(TYPE_DECIMALV2, TYPE_TIME, NumberToTime);
UNARY_FN_CAST(TYPE_DATE, TYPE_TIME, DateToTime);
UNARY_FN_CAST(TYPE_DATETIME, TYPE_TIME, DatetimeToTime);

SELF_CAST(TYPE_JSON);

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
static ColumnPtr cast_from_string_to_time_fn(ColumnPtr& column) {
    auto size = column->size();
    ColumnBuilder<TYPE_TIME> builder(size);
    ColumnViewer<TYPE_VARCHAR> viewer_time(column);
    for (size_t row = 0; row < size; ++row) {
        if (viewer_time.is_null(row)) {
            builder.append_null();
            continue;
        }

        auto time = viewer_time.value(row);
        char* first_char = time.data;
        char* end_char = time.data + time.size;

        int hour = 0, minute = 0, second = 0;
        char* first_colon = (char*)memchr(first_char, ':', time.size);
        if (first_colon != nullptr) {
            char* second_colon = (char*)memchr(first_colon + 1, ':', end_char - first_colon - 1);
            if (second_colon != nullptr) {
                char* third_colon = (char*)memchr(second_colon + 1, ':', end_char - second_colon - 1);
                if (third_colon != nullptr) {
                    builder.append_null();
                } else {
                    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
                    auto int_value = StringParser::string_to_unsigned_int<uint64_t>(
                            reinterpret_cast<char*>(first_char), first_colon - first_char, &parse_result);
                    if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                        if constexpr (AllowThrowException) {
                            THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_TIME, time.to_string());
                        }
                        builder.append_null();
                        continue;
                    } else {
                        hour = int_value;
                    }

                    int_value = StringParser::string_to_unsigned_int<uint64_t>(
                            reinterpret_cast<char*>(first_colon + 1), second_colon - first_colon - 1, &parse_result);
                    if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                        if constexpr (AllowThrowException) {
                            THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_TIME, time.to_string());
                        }
                        builder.append_null();
                        continue;
                    } else {
                        minute = int_value;
                    }

                    int_value = StringParser::string_to_unsigned_int<uint64_t>(
                            reinterpret_cast<char*>(second_colon + 1), end_char - second_colon - 1, &parse_result);
                    if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                        if constexpr (AllowThrowException) {
                            THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_TIME, time.to_string());
                        }
                        builder.append_null();
                        continue;
                    } else {
                        second = int_value;
                    }

                    if (minute >= 60 || second >= 60) {
                        if constexpr (AllowThrowException) {
                            THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_TIME, time.to_string());
                        }
                        builder.append_null();
                        continue;
                    }

                    int64_t seconds = hour * 3600 + minute * 60 + second;
                    builder.append(seconds);
                }
            } else {
                if constexpr (AllowThrowException) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_TIME, time.to_string());
                }
                builder.append_null();
            }
        } else {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_TIME, time.to_string());
            }
            builder.append_null();
        }
    }

    return builder.build(column->is_constant());
}
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_TIME, cast_from_string_to_time_fn);

// clang-format off
#define DEFINE_CAST_CONSTRUCT(CLASS)             \
    CLASS(const TExprNode& node) : Expr(node) {} \
    virtual ~CLASS(){};                          \
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new CLASS(*this)); }
// clang-format on

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
#ifdef STARROCKS_JIT_ENABLE
class VectorizedCastExpr final : public Expr,
                                 public JITCodegenNode
#else
class VectorizedCastExpr final : public Expr
#endif
{
public:
    DEFINE_CAST_CONSTRUCT(VectorizedCastExpr);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, ptr));

        size_t col_size = column->size();
        if (col_size != 0 && ColumnHelper::count_nulls(column) == col_size) {
            return ColumnHelper::create_const_null_column(col_size);
        }
        const TypeDescriptor& to_type = this->type();

        ColumnPtr result_column;
        // NOTE
        // For json type, it could not be converted from decimal directly, as a workaround we convert decimal
        // to double at first, then convert double to JSON
        if constexpr (FromType == TYPE_JSON || ToType == TYPE_JSON) {
            if constexpr (lt_is_decimal<FromType>) {
                ColumnPtr double_column;
                if (context != nullptr && context->error_if_overflow()) {
                    double_column = VectorizedUnaryFunction<DecimalTo<OverflowMode::REPORT_ERROR>>::evaluate<
                            FromType, TYPE_DOUBLE>(column);
                } else {
                    double_column = VectorizedUnaryFunction<DecimalTo<OverflowMode::OUTPUT_NULL>>::evaluate<
                            FromType, TYPE_DOUBLE>(column);
                }
                result_column = CastFn<TYPE_DOUBLE, TYPE_JSON, AllowThrowException>::cast_fn(std::move(double_column));
            } else {
                result_column = CastFn<FromType, ToType, AllowThrowException>::cast_fn(std::move(column));
            }
        } else if constexpr (FromType == TYPE_VARIANT || ToType == TYPE_VARIANT) {
            if constexpr (lt_is_decimal<ToType>) {
                if (context != nullptr && context->error_if_overflow()) {
                    return VectorizedUnaryFunction<DecimalFrom<OverflowMode::REPORT_ERROR>>::evaluate<FromType, ToType>(
                            column, to_type.precision, to_type.scale);
                } else {
                    return VectorizedUnaryFunction<DecimalFrom<OverflowMode::OUTPUT_NULL>>::evaluate<FromType, ToType>(
                            column, to_type.precision, to_type.scale);
                }
            } else {
                result_column =
                        CastFn<FromType, ToType, AllowThrowException>::cast_fn(std::move(column))->as_mutable_ptr();
            }
        } else if constexpr (lt_is_decimal<FromType> && lt_is_decimal<ToType>) {
            if (context != nullptr && context->error_if_overflow()) {
                return VectorizedUnaryFunction<DecimalToDecimal<OverflowMode::REPORT_ERROR>>::evaluate<FromType,
                                                                                                       ToType>(
                        column, to_type.precision, to_type.scale);
            } else {
                return VectorizedUnaryFunction<DecimalToDecimal<OverflowMode::OUTPUT_NULL>>::evaluate<FromType, ToType>(
                        column, to_type.precision, to_type.scale);
            }
        } else if constexpr (lt_is_decimal<FromType>) {
            if (context != nullptr && context->error_if_overflow()) {
                return VectorizedUnaryFunction<DecimalTo<OverflowMode::REPORT_ERROR>>::evaluate<FromType, ToType>(
                        column);
            } else {
                return VectorizedUnaryFunction<DecimalTo<OverflowMode::OUTPUT_NULL>>::evaluate<FromType, ToType>(
                        column);
            }
        } else if constexpr (lt_is_decimal<ToType>) {
            if (context != nullptr && context->error_if_overflow()) {
                return VectorizedUnaryFunction<DecimalFrom<OverflowMode::REPORT_ERROR>>::evaluate<FromType, ToType>(
                        column, to_type.precision, to_type.scale);
            } else {
                return VectorizedUnaryFunction<DecimalFrom<OverflowMode::OUTPUT_NULL>>::evaluate<FromType, ToType>(
                        column, to_type.precision, to_type.scale);
            }
        } else if constexpr (lt_is_string<FromType> && lt_is_binary<ToType>) {
            result_column = Column::mutate(std::move(column));
        } else {
            result_column = CastFn<FromType, ToType, AllowThrowException>::cast_fn(std::move(column));
        }
        DCHECK(result_column.get() != nullptr);
        if (result_column->is_constant()) {
            result_column->as_mutable_raw_ptr()->resize(col_size);
        }
        return result_column;
    };
#ifdef STARROCKS_JIT_ENABLE

    bool is_compilable(RuntimeState* state) const override {
        return state->can_jit_expr(CompilableExprType::CAST) && !AllowThrowException && FromType != TYPE_LARGEINT &&
               ToType != TYPE_LARGEINT && IRHelper::support_jit(FromType) && IRHelper::support_jit(ToType);
    }

    std::string jit_func_name_impl(RuntimeState* state) const override {
        return "{cast(" + ExprJITCodegen::func_name(_children[0], state) + ")}" + (is_constant() ? "c:" : "") +
               (is_nullable() ? "n:" : "") + type().debug_string();
    }

    StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, JITContext* jit_ctx) override {
        ASSIGN_OR_RETURN(auto datum, ExprJITCodegen::generate_ir(context, _children[0], jit_ctx))
        auto* l = datum.value;
        auto& b = jit_ctx->builder;
        if constexpr (FromType == TYPE_JSON || ToType == TYPE_JSON) {
            return Status::NotSupported("JIT casting does not support JSON");
        } else if constexpr (FromType == TYPE_VARIANT || ToType == TYPE_VARIANT) {
            return Status::NotSupported("JIT casting does not support VARIANT");
        } else if constexpr (lt_is_decimal<FromType> || lt_is_decimal<ToType>) {
            return Status::NotSupported("JIT casting does not support decimal");
        } else {
            ASSIGN_OR_RETURN(datum.value, IRHelper::cast_to_type(b, l, FromType, ToType));
            if constexpr ((lt_is_integer<FromType> || lt_is_float<FromType>)&&(lt_is_integer<ToType> ||
                                                                               lt_is_float<ToType>)) {
                typedef RunTimeCppType<FromType> FromCppType;
                typedef RunTimeCppType<ToType> ToCppType;
                if constexpr ((std::is_floating_point_v<ToCppType> || std::is_floating_point_v<FromCppType>)
                                      ? (static_cast<long double>(std::numeric_limits<ToCppType>::max()) <
                                         static_cast<long double>(std::numeric_limits<FromCppType>::max()))
                                      : (std::numeric_limits<ToCppType>::max() <
                                         std::numeric_limits<FromCppType>::max())) {
                    // Check overflow.

                    llvm::Value* max_overflow = nullptr;
                    llvm::Value* min_overflow = nullptr;
                    if constexpr (lt_is_integer<FromType>) {
                        RETURN_IF(!l->getType()->isIntegerTy(),
                                  Status::JitCompileError("Check overflow failed, data type is not integer"));

                        // TODO(Yueyang): fix __int128
                        auto* max = llvm::ConstantInt::get(l->getType(), std::numeric_limits<ToCppType>::max(), true);
                        auto* min =
                                llvm::ConstantInt::get(l->getType(), std::numeric_limits<ToCppType>::lowest(), true);
                        max_overflow = b.CreateICmpSGT(l, max);
                        min_overflow = b.CreateICmpSLT(l, min);
                    } else if constexpr (lt_is_float<FromType>) {
                        RETURN_IF(!l->getType()->isFloatingPointTy(),
                                  Status::JitCompileError("Check overflow failed, data type is not float point"));

                        auto* max = llvm::ConstantFP::get(l->getType(),
                                                          static_cast<double>(std::numeric_limits<ToCppType>::max()));
                        auto* min = llvm::ConstantFP::get(
                                l->getType(), static_cast<double>(std::numeric_limits<ToCppType>::lowest()));
                        max_overflow = b.CreateFCmpOGT(l, max);
                        min_overflow = b.CreateFCmpOLT(l, min);
                    }

                    auto* is_overflow = b.CreateOr(max_overflow, min_overflow);
                    datum.null_flag = b.CreateSelect(
                            is_overflow, llvm::ConstantInt::get(datum.null_flag->getType(), 1, false), datum.null_flag);
                }
            }

            return datum;
        }
    }
#endif

    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "VectorizedCastExpr ("
            << "from=" << _children[0]->type().debug_string() << ", to expr=" << expr_debug_string << ")";
        return out.str();
    }
};

DEFINE_BINARY_FUNCTION_WITH_IMPL(timeToDate, date, time) {
    // return current data direct
    return date;
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(timeToDatetime, date, time) {
    TimestampValue v;
    v.set_timestamp(timestamp::from_julian_and_time(date.julian(), time * USECS_PER_SEC));
    return v;
}

// for time cast to date/datetime
#define DEFINE_TIME_CAST_DATE_CLASS(TO_TYPE, IMPL, ALLOWTHROWEXCEPTION)                                         \
    template <>                                                                                                 \
    class VectorizedCastExpr<TYPE_TIME, TO_TYPE, ALLOWTHROWEXCEPTION> final : public Expr {                     \
    public:                                                                                                     \
        DEFINE_CAST_CONSTRUCT(VectorizedCastExpr);                                                              \
        Status prepare(RuntimeState* state, ExprContext* context) override {                                    \
            RETURN_IF_ERROR(Expr::prepare(state, context));                                                     \
            DateTimeValue dtv;                                                                                  \
            if (dtv.from_unixtime(state->timestamp_ms() / 1000, state->timezone())) {                           \
                DateValue dv;                                                                                   \
                dv.from_date(dtv.year(), dtv.month(), dtv.day());                                               \
                _now = ColumnHelper::create_const_column<TYPE_DATE>(dv, 1);                                     \
            } else {                                                                                            \
                _now = ColumnHelper::create_const_null_column(1);                                               \
            }                                                                                                   \
            return Status::OK();                                                                                \
        }                                                                                                       \
                                                                                                                \
        StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {                       \
            ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, ptr));                   \
            if (ColumnHelper::count_nulls(column) == column->size() && column->size() != 0) {                   \
                return ColumnHelper::create_const_null_column(column->size());                                  \
            }                                                                                                   \
                                                                                                                \
            return VectorizedStrictBinaryFunction<IMPL>::evaluate<TYPE_DATE, TYPE_TIME, TO_TYPE>(_now, column); \
        };                                                                                                      \
                                                                                                                \
        std::string debug_string() const override {                                                             \
            std::stringstream out;                                                                              \
            auto expr_debug_string = Expr::debug_string();                                                      \
            out << "VectorizedCastExpr ("                                                                       \
                << "from=" << _children[0]->type().debug_string() << ", to=" << this->type().debug_string()     \
                << ", expr=" << expr_debug_string << ")";                                                       \
            return out.str();                                                                                   \
        }                                                                                                       \
                                                                                                                \
    private:                                                                                                    \
        ColumnPtr _now;                                                                                         \
    };

DEFINE_TIME_CAST_DATE_CLASS(TYPE_DATE, timeToDate, true);
DEFINE_TIME_CAST_DATE_CLASS(TYPE_DATE, timeToDate, false);
DEFINE_TIME_CAST_DATE_CLASS(TYPE_DATETIME, timeToDatetime, true);
DEFINE_TIME_CAST_DATE_CLASS(TYPE_DATETIME, timeToDatetime, false);

/**
 * Cast float to string
 */
DEFINE_STRING_UNARY_FN_WITH_IMPL(FloatCastToString, v) {
    char buf[16] = {0};
    size_t len = f2s_buffered_n(v, buf);
    return {buf, len};
}

/**
 * Cast double to string
 */
DEFINE_STRING_UNARY_FN_WITH_IMPL(DoubleCastToString, v) {
    char buf[32] = {0};
    size_t len = d2s_buffered_n(v, buf);
    return {buf, len};
}

// clang-format off
// The StringUnaryFunction templace is defined in unary_function.h
// This place is a trait for this, it's for performance.
// CastToString will copy string when returning value,
// it will consume 400ms when casting 10^8 rows.
// It's better to eliminate the CastToString overload.
#define DEFINE_INT_CAST_TO_STRING(FROM_TYPE, TO_TYPE)                                                       \
    template <>                                                                                             \
    template <>                                                                                             \
    inline ColumnPtr StringUnaryFunction<CastToString>::evaluate<FROM_TYPE, TO_TYPE>(const ColumnPtr& v1) { \
        auto& r1 = ColumnHelper::cast_to_raw<FROM_TYPE>(v1)->get_data();                                    \
        auto result = RunTimeColumnType<TO_TYPE>::create();                                                 \
        auto& offset = result->get_offset();                                                                \
        offset.resize(v1->size() + 1);                                                                      \
        auto& bytes = result->get_bytes();                                                                  \
        bytes.reserve(sizeof(RunTimeColumnType<FROM_TYPE>) * v1->size());                                   \
        int size = v1->size();                                                                              \
        for (int i = 0; i < size; ++i) {                                                                    \
            auto f = fmt::format_int(r1[i]);                                                                \
            bytes.insert(bytes.end(), (uint8_t*)f.data(), (uint8_t*)f.data() + f.size());                   \
            offset.set(i + 1, bytes.size());                                                               \
        }                                                                                                   \
        return result;                                                                                      \
    }
// clang-format on

DEFINE_INT_CAST_TO_STRING(TYPE_BOOLEAN, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_TINYINT, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_SMALLINT, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_INT, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_BIGINT, TYPE_VARCHAR);

// Specialized temporal-to-string: writes directly into the bytes buffer via to_string(char*, n),
// avoiding per-row std::string allocation + copy that the generic StringUnaryFunction path does.
#define DEFINE_TEMPORAL_CAST_TO_STRING(FROM_TYPE, TO_TYPE, MAX_LEN)                                         \
    template <>                                                                                             \
    template <>                                                                                             \
    inline ColumnPtr StringUnaryFunction<CastToString>::evaluate<FROM_TYPE, TO_TYPE>(const ColumnPtr& v1) { \
        const auto& r1 = ColumnHelper::cast_to_raw<FROM_TYPE>(v1)->get_data();                              \
        auto result = RunTimeColumnType<TO_TYPE>::create();                                                 \
        int size = v1->size();                                                                              \
        auto& offset = result->get_offset();                                                                \
        offset.resize(size + 1);                                                                            \
        auto& bytes = result->get_bytes();                                                                  \
        bytes.resize(static_cast<size_t>(MAX_LEN) * size);                                                  \
        char* dst = reinterpret_cast<char*>(bytes.data());                                                  \
        size_t off = 0;                                                                                     \
        if constexpr (FROM_TYPE == TYPE_DATE) {                                                             \
            /* DateValue::to_string always writes MAX_LEN when n >= MAX_LEN */                              \
            for (int i = 0; i < size; ++i) {                                                                \
                r1[i].to_string(dst + off, MAX_LEN);                                                        \
                off += MAX_LEN;                                                                             \
                offset.set(i + 1, off);                                                                     \
            }                                                                                               \
        } else {                                                                                            \
            for (int i = 0; i < size; ++i) {                                                                \
                int len = r1[i].to_string(dst + off, MAX_LEN);                                              \
                if (LIKELY(len > 0)) off += len;                                                            \
                offset.set(i + 1, off);                                                                     \
            }                                                                                               \
        }                                                                                                   \
        bytes.resize(off);                                                                                  \
        return result;                                                                                      \
    }

DEFINE_TEMPORAL_CAST_TO_STRING(TYPE_DATETIME, TYPE_VARCHAR, 26);
DEFINE_TEMPORAL_CAST_TO_STRING(TYPE_DATE, TYPE_VARCHAR, 10);

// Cast SQL type to JSON
CUSTOMIZE_FN_CAST(TYPE_NULL, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_INT, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_TINYINT, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_SMALLINT, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_BIGINT, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_LARGEINT, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_BOOLEAN, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_FLOAT, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_DOUBLE, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_CHAR, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARCHAR, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_TIME, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_DATETIME, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_DATE, TYPE_JSON, cast_to_json_fn);
CUSTOMIZE_FN_CAST(TYPE_VARIANT, TYPE_JSON, cast_to_json_fn);

// Cast SQL type to VARIANT
SELF_CAST(TYPE_VARIANT);

/**
 * Resolve cast to string
 */
template <LogicalType Type, bool AllowThrowException>
class VectorizedCastToStringExpr final : public Expr {
public:
    DEFINE_CAST_CONSTRUCT(VectorizedCastToStringExpr);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, ptr));
        if (ColumnHelper::count_nulls(column) == column->size() && column->size() != 0) {
            return ColumnHelper::create_const_null_column(column->size());
        }

        if constexpr (Type == TYPE_DATE || Type == TYPE_DATETIME || Type == TYPE_DECIMALV2 || Type == TYPE_BOOLEAN ||
                      Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT || Type == TYPE_BIGINT ||
                      Type == TYPE_LARGEINT) {
            return VectorizedStringStrictUnaryFunction<CastToString>::template evaluate<Type, TYPE_VARCHAR>(column);
        }

        if constexpr (Type == TYPE_VARBINARY) {
            return Column::mutate(std::move(column));
        }

        if constexpr (lt_is_decimal<Type>) {
            if (context != nullptr && context->error_if_overflow()) {
                return VectorizedUnaryFunction<DecimalTo<OverflowMode::REPORT_ERROR>>::evaluate<Type, TYPE_VARCHAR>(
                        column);
            } else {
                return VectorizedUnaryFunction<DecimalTo<OverflowMode::OUTPUT_NULL>>::evaluate<Type, TYPE_VARCHAR>(
                        column);
            }
        }

        // must be: TYPE_FLOAT, TYPE_DOUBLE, TYPE_CHAR, TYPE_VARCHAR...
        if constexpr (Type == TYPE_FLOAT) {
            return _evaluate_float<TYPE_FLOAT>(context, column);
        }

        if constexpr (Type == TYPE_DOUBLE) {
            return _evaluate_float<TYPE_DOUBLE>(context, column);
        }

        if constexpr (Type == TYPE_TIME) {
            return _evaluate_time(context, column);
        }

        if constexpr (Type == TYPE_JSON) {
            return cast_from_json_fn<TYPE_JSON, TYPE_VARCHAR, AllowThrowException>(column);
        }

        if constexpr (Type == TYPE_VARIANT) {
            return cast_from_variant_fn<TYPE_VARIANT, TYPE_VARCHAR, AllowThrowException>(column);
        }

        return _evaluate_string(context, std::move(column));
    };

private:
    template <LogicalType FloatType>
    ColumnPtr _evaluate_float(ExprContext* context, const ColumnPtr& column) {
        if (type().len == -1) {
            if constexpr (FloatType == TYPE_FLOAT) {
                return VectorizedStringStrictUnaryFunction<FloatCastToString>::template evaluate<TYPE_FLOAT,
                                                                                                 TYPE_VARCHAR>(column);
            } else {
                return VectorizedStringStrictUnaryFunction<DoubleCastToString>::template evaluate<TYPE_DOUBLE,
                                                                                                  TYPE_VARCHAR>(column);
            }
        }
        if (type().len < 0) {
            return ColumnHelper::create_const_null_column(column->size());
        }

        // type.length > 0
        ColumnViewer<FloatType> viewer(column);
        ColumnBuilder<TYPE_VARCHAR> builder(viewer.size());

        char value[MAX_DOUBLE_STR_LENGTH];
        size_t len = 0;
        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            if (std::isnan(viewer.value(row))) {
                builder.append("nan");
                continue;
            }

            if constexpr (FloatType == TYPE_FLOAT) {
                len = f2s_buffered_n(viewer.value(row), value);
            } else {
                len = d2s_buffered_n(viewer.value(row), value);
            }

            builder.append(Slice(value, len));
        }

        return builder.build(column->is_constant());
    }

    // cast(string as string) is trivial operation, just return the input column.
    // This behavior is not compatible with MySQL
    // 1. cast(string as varchar(n)) supported in SR, but not supported in MySQL
    // 2. cast(string as char(n)) supported in both SR and MySQL, but in SR, in some queries, length
    //    of char is neglected. in MySQL, the input string shall be truncated if its length is larger than
    //    length of char.
    // In SR, behaviors of both cast(string as varchar(n)) and cast(string as char(n)) keep the same: neglect
    // of the length of char/varchar and return input column directly.
    ColumnPtr _evaluate_string(ExprContext* context, ColumnPtr&& column) { return Column::mutate(std::move(column)); }

    ColumnPtr _evaluate_time(ExprContext* context, const ColumnPtr& column) {
        ColumnViewer<TYPE_TIME> viewer(column);
        ColumnBuilder<TYPE_VARCHAR> builder(viewer.size());

        for (int row = 0; row < viewer.size(); ++row) {
            if (viewer.is_null(row)) {
                builder.append_null();
                continue;
            }

            builder.append(starrocks::time_str_from_double(viewer.value(row)));
        }

        return builder.build(column->is_constant());
    }
};

#define CASE_FROM_TYPE(FROM_TYPE, TO_TYPE, ALLOWTHROWEXCEPTION)             \
    case FROM_TYPE: {                                                       \
        if (ALLOWTHROWEXCEPTION) {                                          \
            return new VectorizedCastExpr<FROM_TYPE, TO_TYPE, true>(node);  \
        } else {                                                            \
            return new VectorizedCastExpr<FROM_TYPE, TO_TYPE, false>(node); \
        }                                                                   \
    }

#define SWITCH_ALL_FROM_TYPE(TO_TYPE, ALLOWTHROWEXCEPTION)                          \
    switch (from_type) {                                                            \
        CASE_FROM_TYPE(TYPE_BOOLEAN, TO_TYPE, ALLOWTHROWEXCEPTION);                 \
        CASE_FROM_TYPE(TYPE_TINYINT, TO_TYPE, ALLOWTHROWEXCEPTION);                 \
        CASE_FROM_TYPE(TYPE_SMALLINT, TO_TYPE, ALLOWTHROWEXCEPTION);                \
        CASE_FROM_TYPE(TYPE_INT, TO_TYPE, ALLOWTHROWEXCEPTION);                     \
        CASE_FROM_TYPE(TYPE_BIGINT, TO_TYPE, ALLOWTHROWEXCEPTION);                  \
        CASE_FROM_TYPE(TYPE_LARGEINT, TO_TYPE, ALLOWTHROWEXCEPTION);                \
        CASE_FROM_TYPE(TYPE_FLOAT, TO_TYPE, ALLOWTHROWEXCEPTION);                   \
        CASE_FROM_TYPE(TYPE_DOUBLE, TO_TYPE, ALLOWTHROWEXCEPTION);                  \
        CASE_FROM_TYPE(TYPE_DECIMALV2, TO_TYPE, ALLOWTHROWEXCEPTION);               \
        CASE_FROM_TYPE(TYPE_TIME, TO_TYPE, ALLOWTHROWEXCEPTION);                    \
        CASE_FROM_TYPE(TYPE_DATE, TO_TYPE, ALLOWTHROWEXCEPTION);                    \
        CASE_FROM_TYPE(TYPE_DATETIME, TO_TYPE, ALLOWTHROWEXCEPTION);                \
        CASE_FROM_TYPE(TYPE_VARCHAR, TO_TYPE, ALLOWTHROWEXCEPTION);                 \
        CASE_FROM_TYPE(TYPE_DECIMAL32, TO_TYPE, ALLOWTHROWEXCEPTION);               \
        CASE_FROM_TYPE(TYPE_DECIMAL64, TO_TYPE, ALLOWTHROWEXCEPTION);               \
        CASE_FROM_TYPE(TYPE_DECIMAL128, TO_TYPE, ALLOWTHROWEXCEPTION);              \
        CASE_FROM_TYPE(TYPE_DECIMAL256, TO_TYPE, ALLOWTHROWEXCEPTION);              \
    default:                                                                        \
        LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type) \
                     << " to type: " << type_to_string(to_type);                    \
        return nullptr;                                                             \
    }

#define CASE_TO_TYPE(TO_TYPE, ALLOWTHROWEXCEPTION)          \
    case TO_TYPE: {                                         \
        SWITCH_ALL_FROM_TYPE(TO_TYPE, ALLOWTHROWEXCEPTION); \
        break;                                              \
    }

#define CASE_FROM_JSON_TO(TO_TYPE, ALLOWTHROWEXCEPTION)                     \
    case TO_TYPE: {                                                         \
        if (ALLOWTHROWEXCEPTION) {                                          \
            return new VectorizedCastExpr<TYPE_JSON, TO_TYPE, true>(node);  \
        } else {                                                            \
            return new VectorizedCastExpr<TYPE_JSON, TO_TYPE, false>(node); \
        }                                                                   \
    }

#define CASE_TO_JSON(FROM_TYPE, ALLOWTHROWEXCEPTION)                          \
    case FROM_TYPE: {                                                         \
        if (ALLOWTHROWEXCEPTION) {                                            \
            return new VectorizedCastExpr<FROM_TYPE, TYPE_JSON, true>(node);  \
        } else {                                                              \
            return new VectorizedCastExpr<FROM_TYPE, TYPE_JSON, false>(node); \
        }                                                                     \
    }

#define CASE_TO_VARIANT(FROM_TYPE, ALLOWTHROWEXCEPTION)                          \
    case FROM_TYPE: {                                                            \
        if (ALLOWTHROWEXCEPTION) {                                               \
            return new VectorizedCastExpr<FROM_TYPE, TYPE_VARIANT, true>(node);  \
        } else {                                                                 \
            return new VectorizedCastExpr<FROM_TYPE, TYPE_VARIANT, false>(node); \
        }                                                                        \
    }

#define CASE_FROM_VARIANT_TO(TO_TYPE, ALLOWTHROWEXCEPTION)                     \
    case TO_TYPE: {                                                            \
        if (ALLOWTHROWEXCEPTION) {                                             \
            return new VectorizedCastExpr<TYPE_VARIANT, TO_TYPE, true>(node);  \
        } else {                                                               \
            return new VectorizedCastExpr<TYPE_VARIANT, TO_TYPE, false>(node); \
        }                                                                      \
    }

#define CASE_TO_STRING_FROM(FROM_TYPE, ALLOWTHROWEXCEPTION)                \
    case FROM_TYPE: {                                                      \
        if (ALLOWTHROWEXCEPTION) {                                         \
            return new VectorizedCastToStringExpr<FROM_TYPE, true>(node);  \
        } else {                                                           \
            return new VectorizedCastToStringExpr<FROM_TYPE, false>(node); \
        }                                                                  \
    }

// Split-out primitive to_type cartesian (see create_primitive_cast); the heavy
// VectorizedCastExpr<From,To> instantiations are distributed across cast_expr_g{1,2,3}.cpp.
Expr* create_primitive_cast_group1(const TExprNode& node, LogicalType from_type, LogicalType to_type,
                                   bool allow_throw_exception);
Expr* create_primitive_cast_group2(const TExprNode& node, LogicalType from_type, LogicalType to_type,
                                   bool allow_throw_exception);
Expr* create_primitive_cast_group3(const TExprNode& node, LogicalType from_type, LogicalType to_type,
                                   bool allow_throw_exception);

} // namespace starrocks
