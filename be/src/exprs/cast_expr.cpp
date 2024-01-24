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

#include "exprs/cast_expr.h"

#include <llvm/ADT/APInt.h>
#include <ryu/ryu.h>

#include <stdexcept>
#include <utility>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/binary_function.h"
#include "exprs/column_ref.h"
#include "exprs/decimal_cast_expr.h"
#include "exprs/jit/ir_helper.h"
#include "exprs/unary_function.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Value.h"
#include "runtime/datetime_value.h"
#include "runtime/large_int_value.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "types/hll.h"
#include "types/logical_type.h"
#include "util/date_func.h"
#include "util/json.h"
#include "util/mysql_global.h"

namespace starrocks {

#define THROW_RUNTIME_ERROR_WITH_TYPE(TYPE)              \
    std::stringstream ss;                                \
    ss << "not supported type " << type_to_string(TYPE); \
    throw std::runtime_error(ss.str())

#define THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FROMTYPE, TOTYPE, VALUE) \
    std::stringstream ss;                                                 \
    ss << "cast from " << type_to_string(FROMTYPE) << "(" << VALUE << ")" \
       << " to " << type_to_string(TOTYPE) << " failed";                  \
    throw std::runtime_error(ss.str())

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException = false>
struct CastFn {
    static ColumnPtr cast_fn(ColumnPtr& column);
};

// All cast implements
#define SELF_CAST(FROM_TYPE)                                                    \
    template <bool AllowThrowException>                                         \
    struct CastFn<FROM_TYPE, FROM_TYPE, AllowThrowException> {                  \
        static ColumnPtr cast_fn(ColumnPtr& column) { return column->clone(); } \
    };

#define UNARY_FN_CAST(FROM_TYPE, TO_TYPE, UNARY_IMPL)                                                        \
    template <bool AllowThrowException>                                                                      \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                                                 \
        static ColumnPtr cast_fn(ColumnPtr& column) {                                                        \
            return VectorizedStrictUnaryFunction<UNARY_IMPL>::template evaluate<FROM_TYPE, TO_TYPE>(column); \
        }                                                                                                    \
    };

#define UNARY_FN_CAST_VALID(FROM_TYPE, TO_TYPE, UNARY_IMPL)                                                            \
    template <bool AllowThrowException>                                                                                \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                                                           \
        static ColumnPtr cast_fn(ColumnPtr& column) {                                                                  \
            if constexpr (std::numeric_limits<RunTimeCppType<TO_TYPE>>::max() <                                        \
                          std::numeric_limits<RunTimeCppType<FROM_TYPE>>::max()) {                                     \
                if constexpr (!AllowThrowException) {                                                                  \
                    return VectorizedInputCheckUnaryFunction<UNARY_IMPL, NumberCheck>::template evaluate<FROM_TYPE,    \
                                                                                                         TO_TYPE>(     \
                            column);                                                                                   \
                } else {                                                                                               \
                    return VectorizedInputCheckUnaryFunction<                                                          \
                            UNARY_IMPL, NumberCheckWithThrowException>::template evaluate<FROM_TYPE, TO_TYPE>(column); \
                }                                                                                                      \
            }                                                                                                          \
            return VectorizedStrictUnaryFunction<UNARY_IMPL>::template evaluate<FROM_TYPE, TO_TYPE>(column);           \
        }                                                                                                              \
    };

#define UNARY_FN_CAST_TIME_VALID(FROM_TYPE, TO_TYPE, UNARY_IMPL)                                                    \
    template <bool AllowThrowException>                                                                             \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                                                        \
        static ColumnPtr cast_fn(ColumnPtr& column) {                                                               \
            return VectorizedInputCheckUnaryFunction<UNARY_IMPL, TimeCheck>::template evaluate<FROM_TYPE, TO_TYPE>( \
                    column);                                                                                        \
        }                                                                                                           \
    };

#define CUSTOMIZE_FN_CAST(FROM_TYPE, TO_TYPE, CUSTOMIZE_IMPL)                       \
    template <bool AllowThrowException>                                             \
    struct CastFn<FROM_TYPE, TO_TYPE, AllowThrowException> {                        \
        static ColumnPtr cast_fn(ColumnPtr& column) {                               \
            return CUSTOMIZE_IMPL<FROM_TYPE, TO_TYPE, AllowThrowException>(column); \
        }                                                                           \
    };

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
        } else {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPE(FromType);
            }
            DCHECK(false) << "not supported type " << FromType;
        }
        if (overflow || value.is_null()) {
            if constexpr (AllowThrowException) {
                if constexpr (FromType == TYPE_LARGEINT) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType,
                                                             LargeIntValue::to_string(viewer.value(row)));
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
    return {};
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
        if constexpr (lt_is_arithmetic<ToType>) {
            [[maybe_unused]] constexpr auto min = RunTimeTypeLimits<ToType>::min_value();
            [[maybe_unused]] constexpr auto max = RunTimeTypeLimits<ToType>::max_value();
            RunTimeCppType<ToType> cpp_value{};
            bool ok = true;
            if constexpr (lt_is_integer<ToType>) {
                auto res = json->get_int();
                ok = res.ok() && min <= res.value() && res.value() <= max;
                cpp_value = ok ? res.value() : cpp_value;
            } else if constexpr (lt_is_float<ToType>) {
                auto res = json->get_double();
                ok = res.ok() && min <= res.value() && res.value() <= max;
                cpp_value = ok ? res.value() : cpp_value;
            } else if constexpr (lt_is_boolean<ToType>) {
                auto res = json->get_bool();
                ok = res.ok();
                cpp_value = ok ? res.value() : cpp_value;
            } else {
                if constexpr (AllowThrowException) {
                    THROW_RUNTIME_ERROR_WITH_TYPE(ToType);
                }
                DCHECK(false) << "unreachable type " << ToType;
                __builtin_unreachable();
            }
            if (ok) {
                builder.append(cpp_value);
            } else {
                if constexpr (AllowThrowException) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, json->to_string().value_or(""));
                }
                builder.append_null();
            }
        } else if constexpr (lt_is_string<ToType>) {
            // if the json already a string value, get the string directly
            // else cast it to string representation
            if (json->get_type() == JsonType::JSON_STRING) {
                auto res = json->get_string();
                if (res.ok()) {
                    builder.append(res.value());
                } else {
                    if constexpr (AllowThrowException) {
                        THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, json->to_string().value_or(""));
                    }
                    builder.append_null();
                }
            } else {
                auto res = json->to_string();
                if (res.ok()) {
                    builder.append(res.value());
                } else {
                    if constexpr (AllowThrowException) {
                        THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(FromType, ToType, json->to_string().value_or(""));
                    }
                    builder.append_null();
                }
            }
        } else {
            if constexpr (AllowThrowException) {
                THROW_RUNTIME_ERROR_WITH_TYPE(ToType);
            }
            DCHECK(false) << "not supported type " << ToType;
            builder.append_null();
        }
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
    // std::numeric_limits<T>::lowest() is a finite value x such that there is no other
    // finite value y where y < x.
    // This is different from std::numeric_limits<T>::min() for floating-point types.
    // So we use lowest instead of min for lower bound of all types.
    return (value < (Type)std::numeric_limits<ResultType>::lowest()) |
           (value > (Type)std::numeric_limits<ResultType>::max());
}

DEFINE_UNARY_FN_WITH_IMPL(NumberCheckWithThrowException, value) {
    // std::numeric_limits<T>::lowest() is a finite value x such that there is no other
    // finite value y where y < x.
    // This is different from std::numeric_limits<T>::min() for floating-point types.
    // So we use lowest instead of min for lower bound of all types.
    auto result = (value < (Type)std::numeric_limits<ResultType>::lowest()) |
                  (value > (Type)std::numeric_limits<ResultType>::max());
    if (result) {
        std::stringstream ss;
        if constexpr (std::is_same_v<Type, __int128_t>) {
            ss << LargeIntValue::to_string(value) << " conflict with range of "
               << "(" << LargeIntValue::to_string((Type)std::numeric_limits<ResultType>::lowest()) << ", "
               << LargeIntValue::to_string((Type)std::numeric_limits<ResultType>::max()) << ")";
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
        auto* input_column = down_cast<NullableColumn*>(column.get());
        NullColumnPtr null_column = ColumnHelper::as_column<NullColumn>(input_column->null_column()->clone());
        auto* data_column = down_cast<BinaryColumn*>(input_column->data_column().get());
        auto& null_data = down_cast<NullColumn*>(null_column.get())->get_data();
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
        NullColumnPtr null_column = NullColumn::create(sz);
        auto& null_data = null_column->get_data();
        auto* data_column = down_cast<BinaryColumn*>(column.get());

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

        bool ret = tv.from_timestamp_literal_with_check((int64_t)value);
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
    ColumnViewer<TYPE_VARCHAR> viewer(column);
    ColumnBuilder<TYPE_DATETIME> builder(viewer.size());

    if (!column->has_null()) {
        for (int row = 0; row < viewer.size(); ++row) {
            auto value = viewer.value(row);
            TimestampValue v;

            bool right = v.from_string(value.data, value.size);
            if constexpr (AllowThrowException) {
                if (!right) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_DATETIME, value.to_string());
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
            TimestampValue v;

            bool right = v.from_string(value.data, value.size);
            if constexpr (AllowThrowException) {
                if (!right) {
                    THROW_RUNTIME_ERROR_WITH_TYPES_AND_VALUE(TYPE_VARCHAR, TYPE_DATETIME, value.to_string());
                }
            }
            builder.append(v, !right);
        }
    }

    return builder.build(column->is_constant());
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

#define DEFINE_CAST_CONSTRUCT(CLASS)             \
    CLASS(const TExprNode& node) : Expr(node) {} \
    virtual ~CLASS(){};                          \
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new CLASS(*this)); }

template <LogicalType FromType, LogicalType ToType, bool AllowThrowException>
class VectorizedCastExpr final : public Expr {
public:
    DEFINE_CAST_CONSTRUCT(VectorizedCastExpr);
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        ASSIGN_OR_RETURN(ColumnPtr column, _children[0]->evaluate_checked(context, ptr));
        if (ColumnHelper::count_nulls(column) == column->size() && column->size() != 0) {
            return ColumnHelper::create_const_null_column(column->size());
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
                result_column = CastFn<TYPE_DOUBLE, TYPE_JSON, AllowThrowException>::cast_fn(double_column);
            } else {
                result_column = CastFn<FromType, ToType, AllowThrowException>::cast_fn(column);
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
            result_column = column->clone();
        } else {
            result_column = CastFn<FromType, ToType, AllowThrowException>::cast_fn(column);
        }
        DCHECK(result_column.get() != nullptr);
        if (result_column->is_constant()) {
            result_column->resize(column->size());
        }
        return result_column;
    };

    bool is_compilable() const override {
        return !AllowThrowException && FromType != TYPE_LARGEINT && ToType != TYPE_LARGEINT &&
               IRHelper::support_jit(FromType) && IRHelper::support_jit(ToType);
    }

    StatusOr<LLVMDatum> generate_ir_impl(ExprContext* context, const llvm::Module& module, llvm::IRBuilder<>& b,
                                         const std::vector<LLVMDatum>& datums) const override {
        auto* l = datums[0].value;

        if constexpr (FromType == TYPE_JSON || ToType == TYPE_JSON) {
            return Status::NotSupported("JIT casting does not support JSON");
        } else if constexpr (lt_is_decimal<FromType> || lt_is_decimal<ToType>) {
            return Status::NotSupported("JIT casting does not support decimal");
        } else {
            LLVMDatum datum(b);
            ASSIGN_OR_RETURN(datum.value, IRHelper::cast_to_type(b, l, FromType, ToType));

            if constexpr ((lt_is_integer<FromType> || lt_is_float<FromType>)&&(lt_is_integer<ToType> ||
                                                                               lt_is_float<ToType>)) {
                typedef RunTimeCppType<FromType> FromCppType;
                typedef RunTimeCppType<ToType> ToCppType;
                if constexpr (std::numeric_limits<ToCppType>::max() < std::numeric_limits<FromCppType>::max()) {
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

    std::string debug_string() const override {
        std::stringstream out;
        auto expr_debug_string = Expr::debug_string();
        out << "VectorizedCastExpr ("
            << "from=" << _children[0]->type().debug_string() << ", to=" << this->type().debug_string()
            << ", expr=" << expr_debug_string << ")";
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
            offset[i + 1] = bytes.size();                                                                   \
        }                                                                                                   \
        return result;                                                                                      \
    }

DEFINE_INT_CAST_TO_STRING(TYPE_BOOLEAN, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_TINYINT, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_SMALLINT, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_INT, TYPE_VARCHAR);
DEFINE_INT_CAST_TO_STRING(TYPE_BIGINT, TYPE_VARCHAR);

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
            return column->clone();
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

        return _evaluate_string(context, column);
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
    ColumnPtr _evaluate_string(ExprContext* context, const ColumnPtr& column) { return column->clone(); }

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

#define CASE_TO_STRING_FROM(FROM_TYPE, ALLOWTHROWEXCEPTION)                \
    case FROM_TYPE: {                                                      \
        if (ALLOWTHROWEXCEPTION) {                                         \
            return new VectorizedCastToStringExpr<FROM_TYPE, true>(node);  \
        } else {                                                           \
            return new VectorizedCastToStringExpr<FROM_TYPE, false>(node); \
        }                                                                  \
    }

template <template <bool> class T, typename... Args>
Expr* dispatch_throw_exception(bool throw_exception, Args&&... args) {
    if (throw_exception) {
        return new T<true>(std::forward<Args>(args)...);
    } else {
        return new T<false>(std::forward<Args>(args)...);
    }
}

template <bool throw_exception>
using CastVarcharToHll = VectorizedCastExpr<TYPE_VARCHAR, TYPE_HLL, throw_exception>;

template <bool throw_exception>
using CastVarcharToBitmap = VectorizedCastExpr<TYPE_VARCHAR, TYPE_OBJECT, throw_exception>;

// Create a SlotRef as the child of children cast for the complex type
// For example: to cast STRUCT<int, int> as STRUCT<double, double>
// Inorder to reuse the primary casting expression, we will generate a new CAST(INT as DOUBLE) for
// each field.
// And for the new generated CAST expressions, we will generate a ColumnRef as its child expression
static std::unique_ptr<Expr> create_slot_ref(const TypeDescriptor& type) {
    TExprNode ref_node;
    ref_node.type = type.to_thrift();
    ref_node.__isset.slot_ref = true;
    ref_node.slot_ref.slot_id = 0;
    ref_node.slot_ref.tuple_id = 0;
    return std::make_unique<ColumnRef>(ref_node);
}

StatusOr<ColumnPtr> MustNullExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    // only null
    auto column = ColumnHelper::create_column(_type, true);
    column->append_nulls(1);
    auto only_null = ConstColumn::create(column, 1);
    if (ptr != nullptr) {
        only_null->resize(ptr->num_rows());
    }
    return only_null;
}

Expr* VectorizedCastExprFactory::create_primitive_cast(ObjectPool* pool, const TExprNode& node, LogicalType from_type,
                                                       LogicalType to_type, bool allow_throw_exception) {
    if (to_type == TYPE_CHAR) {
        to_type = TYPE_VARCHAR;
    }
    if (from_type == TYPE_CHAR) {
        from_type = TYPE_VARCHAR;
    }
    if (from_type == TYPE_NULL) {
        // NULL TO OTHER TYPE, direct return
        from_type = to_type;
    }
    if (from_type == TYPE_VARCHAR && to_type == TYPE_HLL) {
        return dispatch_throw_exception<CastVarcharToHll>(allow_throw_exception, node);
    }
    // Cast string to array<ANY>
    if ((from_type == TYPE_VARCHAR || from_type == TYPE_JSON) && to_type == TYPE_ARRAY) {
        TypeDescriptor cast_to = TypeDescriptor::from_thrift(node.type);
        TExprNode cast;
        cast.type = cast_to.children[0].to_thrift();
        cast.child_type = to_thrift(from_type);
        cast.slot_ref.slot_id = 0;
        cast.slot_ref.tuple_id = 0;

        Expr* cast_element_expr = VectorizedCastExprFactory::from_thrift(pool, cast, allow_throw_exception);
        if (cast_element_expr == nullptr) {
            return nullptr;
        }
        auto* child = new ColumnRef(cast);
        cast_element_expr->add_child(child);
        if (pool) {
            pool->add(cast_element_expr);
            pool->add(child);
        }

        if (from_type == TYPE_VARCHAR) {
            return new CastStringToArray(node, cast_element_expr, cast_to, allow_throw_exception);
        } else {
            return new CastJsonToArray(node, cast_element_expr, cast_to);
        }
    }

    if (from_type == TYPE_VARCHAR && to_type == TYPE_OBJECT) {
        return dispatch_throw_exception<CastVarcharToBitmap>(allow_throw_exception, node);
    }

    if (to_type == TYPE_VARCHAR) {
        switch (from_type) {
            CASE_TO_STRING_FROM(TYPE_BOOLEAN, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_TINYINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_SMALLINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_INT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_BIGINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_LARGEINT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_FLOAT, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DOUBLE, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMALV2, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_TIME, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DATE, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DATETIME, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_VARCHAR, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMAL32, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMAL64, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_DECIMAL128, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_JSON, allow_throw_exception);
            CASE_TO_STRING_FROM(TYPE_VARBINARY, allow_throw_exception);
        default:
            LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type)
                         << ", to type: " << type_to_string(to_type);
            return nullptr;
        }
    } else if (from_type == TYPE_JSON || to_type == TYPE_JSON) {
        // TODO(mofei) simplify type enumeration
        if (from_type == TYPE_JSON) {
            switch (to_type) {
                CASE_FROM_JSON_TO(TYPE_BOOLEAN, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_TINYINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_SMALLINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_INT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_BIGINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_LARGEINT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_FLOAT, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_DOUBLE, allow_throw_exception);
                CASE_FROM_JSON_TO(TYPE_JSON, allow_throw_exception);
            default:
                LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type)
                             << ", to type: " << type_to_string(to_type);
                return nullptr;
            }
        } else {
            switch (from_type) {
                CASE_TO_JSON(TYPE_BOOLEAN, allow_throw_exception);
                CASE_TO_JSON(TYPE_TINYINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_SMALLINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_INT, allow_throw_exception);
                CASE_TO_JSON(TYPE_BIGINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_LARGEINT, allow_throw_exception);
                CASE_TO_JSON(TYPE_FLOAT, allow_throw_exception);
                CASE_TO_JSON(TYPE_DOUBLE, allow_throw_exception);
                CASE_TO_JSON(TYPE_JSON, allow_throw_exception);
                CASE_TO_JSON(TYPE_CHAR, allow_throw_exception);
                CASE_TO_JSON(TYPE_VARCHAR, allow_throw_exception);
                CASE_TO_JSON(TYPE_DECIMAL32, allow_throw_exception);
                CASE_TO_JSON(TYPE_DECIMAL64, allow_throw_exception);
                CASE_TO_JSON(TYPE_DECIMAL128, allow_throw_exception);
                CASE_TO_JSON(TYPE_DATE, allow_throw_exception);
                CASE_TO_JSON(TYPE_TIME, allow_throw_exception);
                CASE_TO_JSON(TYPE_DATETIME, allow_throw_exception);
            default:
                LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type)
                             << ", to type: " << type_to_string(to_type);
                return nullptr;
            }
        }
    } else if (is_binary_type(to_type)) {
        if (is_string_type(from_type)) {
            if (allow_throw_exception) {
                return new VectorizedCastExpr<TYPE_VARCHAR, TYPE_VARBINARY, true>(node);
            } else {
                return new VectorizedCastExpr<TYPE_VARCHAR, TYPE_VARBINARY, false>(node);
            }
        } else {
            LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type)
                         << ", to type: " << type_to_string(to_type);
            return nullptr;
        }
    } else {
        switch (to_type) {
            CASE_TO_TYPE(TYPE_BOOLEAN, allow_throw_exception);
            CASE_TO_TYPE(TYPE_TINYINT, allow_throw_exception);
            CASE_TO_TYPE(TYPE_SMALLINT, allow_throw_exception);
            CASE_TO_TYPE(TYPE_INT, allow_throw_exception);
            CASE_TO_TYPE(TYPE_BIGINT, allow_throw_exception);
            CASE_TO_TYPE(TYPE_LARGEINT, allow_throw_exception);
            CASE_TO_TYPE(TYPE_FLOAT, allow_throw_exception);
            CASE_TO_TYPE(TYPE_DOUBLE, allow_throw_exception);
            CASE_TO_TYPE(TYPE_DECIMALV2, allow_throw_exception);
            CASE_TO_TYPE(TYPE_TIME, allow_throw_exception);
            CASE_TO_TYPE(TYPE_DATE, allow_throw_exception);
            CASE_TO_TYPE(TYPE_DATETIME, allow_throw_exception);
            CASE_TO_TYPE(TYPE_DECIMAL32, allow_throw_exception);
            CASE_TO_TYPE(TYPE_DECIMAL64, allow_throw_exception);
            CASE_TO_TYPE(TYPE_DECIMAL128, allow_throw_exception);
        default:
            LOG(WARNING) << "Not support cast from type: " << type_to_string(from_type)
                         << ", to type: " << type_to_string(to_type);
            return nullptr;
        }
    }

    return nullptr;
}

// NOTE: should return error status to avoid null in ASSIGN_OR_RETURN, otherwise causing crash
StatusOr<std::unique_ptr<Expr>> VectorizedCastExprFactory::create_cast_expr(ObjectPool* pool, const TExprNode& node,
                                                                            const TypeDescriptor& from_type,
                                                                            const TypeDescriptor& to_type,
                                                                            bool allow_throw_exception) {
    if (from_type.is_array_type() && to_type.is_array_type()) {
        ASSIGN_OR_RETURN(auto child_cast,
                         create_cast_expr(pool, from_type.children[0], to_type.children[0], allow_throw_exception));
        auto child_input = create_slot_ref(from_type.children[0]);
        child_cast->add_child(child_input.get());
        pool->add(child_input.release());
        return std::make_unique<CastArrayExpr>(node, std::move(child_cast));
    }
    if (from_type.is_map_type() && to_type.is_map_type()) {
        ASSIGN_OR_RETURN(auto key_cast,
                         create_cast_expr(pool, from_type.children[0], to_type.children[0], allow_throw_exception));
        auto key_input = create_slot_ref(from_type.children[0]);
        key_cast->add_child(key_input.get());
        pool->add(key_input.release());

        ASSIGN_OR_RETURN(auto value_cast,
                         create_cast_expr(pool, from_type.children[1], to_type.children[1], allow_throw_exception));
        auto value_input = create_slot_ref(from_type.children[1]);
        value_cast->add_child(value_input.get());
        pool->add(value_input.release());

        return std::make_unique<CastMapExpr>(node, std::move(key_cast), std::move(value_cast));
    }
    if (from_type.is_struct_type() && to_type.is_struct_type()) {
        if (from_type.children.size() != to_type.children.size()) {
            return Status::NotSupported("Not support cast struct with different number of children.");
        }
        if (to_type.field_names.empty() || from_type.field_names.size() != to_type.field_names.size()) {
            return Status::NotSupported("Not support cast struct with different field of children.");
        }
        std::vector<std::unique_ptr<Expr>> field_casts{from_type.children.size()};
        for (int i = 0; i < from_type.children.size(); ++i) {
            ASSIGN_OR_RETURN(field_casts[i],
                             create_cast_expr(pool, from_type.children[i], to_type.children[i], allow_throw_exception));
            auto cast_input = create_slot_ref(from_type.children[i]);
            field_casts[i]->add_child(cast_input.get());
            pool->add(cast_input.release());
        }
        return std::make_unique<CastStructExpr>(node, std::move(field_casts));
    }
    if ((from_type.type == TYPE_NULL || from_type.type == TYPE_BOOLEAN) && to_type.is_complex_type()) {
        return std::make_unique<MustNullExpr>(node);
    }
    auto res = create_primitive_cast(pool, node, from_type.type, to_type.type, allow_throw_exception);
    if (res == nullptr) {
        return Status::NotSupported(
                fmt::format("Not support cast {} to {}.", from_type.debug_string(), to_type.debug_string()));
    }
    std::unique_ptr<Expr> result(res);
    return std::move(result);
}

Expr* VectorizedCastExprFactory::from_thrift(ObjectPool* pool, const TExprNode& node, bool allow_throw_exception) {
    TypeDescriptor to_type = TypeDescriptor::from_thrift(node.type);
    TypeDescriptor from_type(thrift_to_type(node.child_type));
    if (node.__isset.child_type_desc) {
        from_type = TypeDescriptor::from_thrift(node.child_type_desc);
    }
    auto ret = create_cast_expr(pool, node, from_type, to_type, allow_throw_exception);
    if (!ret.ok()) {
        LOG(WARNING) << "Don't support to cast type: " << from_type << " to type: " << to_type;
        return nullptr;
    }
    return std::move(ret).value().release();
}

StatusOr<std::unique_ptr<Expr>> VectorizedCastExprFactory::create_cast_expr(ObjectPool* pool,
                                                                            const TypeDescriptor& from_type,
                                                                            const TypeDescriptor& to_type,
                                                                            bool allow_throw_exception) {
    TExprNode cast_node;
    cast_node.node_type = TExprNodeType::CAST_EXPR;
    cast_node.type = to_type.to_thrift();
    cast_node.__set_child_type(to_thrift(from_type.type));
    cast_node.__set_child_type_desc(from_type.to_thrift());
    return create_cast_expr(pool, cast_node, from_type, to_type, allow_throw_exception);
}

Expr* VectorizedCastExprFactory::from_type(const TypeDescriptor& from, const TypeDescriptor& to, Expr* child,
                                           ObjectPool* pool, bool allow_throw_exception) {
    auto ret = create_cast_expr(pool, from, to, allow_throw_exception);
    if (!ret.ok()) {
        LOG(WARNING) << "Don't support to cast type: " << from << " to type: " << to;
        return nullptr;
    }
    auto cast_expr = std::move(ret).value().release();
    pool->add(cast_expr);
    cast_expr->add_child(child);
    return cast_expr;
}

} // namespace starrocks
