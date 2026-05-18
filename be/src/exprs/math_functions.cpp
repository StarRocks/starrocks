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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include <base/decimal_types.h>
#include <types/decimalv3.h>
#include <types/logical_type.h>

#include <cmath>
#include <random>

#include "base/hash/murmur_hash3.h"
#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "exprs/function_helper.h"
#include "exprs/math_functions.h"
#include "runtime/runtime_state.h"
#include "types/datetime_value.h"

namespace starrocks {

static std::uniform_real_distribution<double> distribution(0.0, 1.0);
static thread_local std::mt19937_64 generator{std::random_device{}()};

namespace {

int64_t iceberg_datetime_to_epoch_microseconds(const TimestampValue& timestamp) {
    auto ts = timestamp.timestamp();
    int64_t value = timestamp::to_julian(ts);
    value *= SECS_PER_DAY;
    value -= timestamp::UNIX_EPOCH_SECONDS;
    value *= 1000000L;
    value += timestamp::to_time(ts);
    return value;
}

bool iceberg_timestamptz_to_epoch_microseconds(FunctionContext* context, const TimestampValue& timestamp,
                                               int64_t* value) {
    cctz::time_zone timezone = cctz::utc_time_zone();
    if (context != nullptr && context->state() != nullptr) {
        timezone = context->state()->timezone_obj();
    }

    int year, month, day, hour, minute, second, usec;
    timestamp.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
    DateTimeValue datetime(TIME_DATETIME, year, month, day, hour, minute, second, usec);

    int64_t unix_second;
    if (!datetime.unix_timestamp(&unix_second, timezone)) {
        return false;
    }

    *value = unix_second * 1000000L + usec;
    return true;
}

} // namespace

// ==== basic check rules =========
DEFINE_UNARY_FN_WITH_IMPL(NegativeCheck, value) {
    return value < 0;
}

DEFINE_UNARY_FN_WITH_IMPL(NonPositiveCheck, value) {
    return value <= 0;
}

DEFINE_UNARY_FN_WITH_IMPL(NanCheck, value) {
    return std::isnan(value);
}

DEFINE_UNARY_FN_WITH_IMPL(InfNanCheck, value) {
    return std::isinf(value) || std::isnan(value);
}

DEFINE_UNARY_FN_WITH_IMPL(ZeroCheck, value) {
    return value == 0;
}

// ====== evaluation + check rules ========

#define DEFINE_MATH_UNARY_FN(NAME, TYPE, RESULT_TYPE)                                                          \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {                \
        using VectorizedUnaryFunction = VectorizedStrictUnaryFunction<NAME##Impl>;                             \
        if constexpr (lt_is_decimal<TYPE>) {                                                                   \
            const auto& type = context->get_return_type();                                                     \
            return VectorizedUnaryFunction::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0), type.precision, \
                                                                        type.scale);                           \
        } else {                                                                                               \
            return VectorizedUnaryFunction::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0));                \
        }                                                                                                      \
    }

#define DEFINE_MATH_UNARY_WITH_ZERO_CHECK_FN(NAME, TYPE, RESULT_TYPE)                             \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {   \
        using VectorizedUnaryFunction = VectorizedInputCheckUnaryFunction<NAME##Impl, ZeroCheck>; \
        return VectorizedUnaryFunction::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0));       \
    }

#define DEFINE_MATH_UNARY_WITH_NEGATIVE_CHECK_FN(NAME, TYPE, RESULT_TYPE)                             \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {       \
        using VectorizedUnaryFunction = VectorizedInputCheckUnaryFunction<NAME##Impl, NegativeCheck>; \
        return VectorizedUnaryFunction::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0));           \
    }

#define DEFINE_MATH_UNARY_WITH_NON_POSITIVE_CHECK_FN(NAME, TYPE, RESULT_TYPE)                            \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {          \
        using VectorizedUnaryFunction = VectorizedInputCheckUnaryFunction<NAME##Impl, NonPositiveCheck>; \
        return VectorizedUnaryFunction::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0));              \
    }

#define DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN(NAME, TYPE, RESULT_TYPE)                       \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {   \
        using VectorizedUnaryFunction = VectorizedOutputCheckUnaryFunction<NAME##Impl, NanCheck>; \
        return VectorizedUnaryFunction::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0));       \
    }

#define DEFINE_MATH_UNARY_WITH_OUTPUT_INF_NAN_CHECK_FN(NAME, TYPE, RESULT_TYPE)                      \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {      \
        using VectorizedUnaryFunction = VectorizedOutputCheckUnaryFunction<NAME##Impl, InfNanCheck>; \
        return VectorizedUnaryFunction::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0));          \
    }

#define DEFINE_MATH_BINARY_WITH_OUTPUT_NAN_CHECK_FN(NAME, LTYPE, RTYPE, RESULT_TYPE)                 \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {      \
        using VectorizedBinaryFunction = VectorizedOuputCheckBinaryFunction<NAME##Impl, NanCheck>;   \
        return VectorizedBinaryFunction::evaluate<LTYPE, RTYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0),  \
                                                                             VECTORIZED_FN_ARGS(1)); \
    }

#define DEFINE_MATH_BINARY_WITH_OUTPUT_INF_NAN_CHECK_FN(NAME, LTYPE, RTYPE, RESULT_TYPE)              \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {       \
        using VectorizedBinaryFunction = VectorizedOuputCheckBinaryFunction<NAME##Impl, InfNanCheck>; \
        return VectorizedBinaryFunction::evaluate<LTYPE, RTYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0),   \
                                                                             VECTORIZED_FN_ARGS(1));  \
    }

// ============ math function macro ==========

#define DEFINE_MATH_UNARY_FN_WITH_IMPL(NAME, TYPE, RESULT_TYPE, FN) \
    DEFINE_UNARY_FN(NAME##Impl, FN);                                \
    DEFINE_MATH_UNARY_FN(NAME, TYPE, RESULT_TYPE);

#define DEFINE_MATH_UNARY_FN_CAST_WITH_IMPL(NAME, TYPE, RESULT_TYPE, FN) \
    DEFINE_UNARY_FN_CAST(NAME##Impl, FN);                                \
    DEFINE_MATH_UNARY_FN(NAME, TYPE, RESULT_TYPE);

#define DEFINE_MATH_BINARY_FN(NAME, LTYPE, RTYPE, RESULT_TYPE)                                                         \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {                        \
        return VectorizedStrictBinaryFunction<NAME##Impl>::evaluate<LTYPE, RTYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0),  \
                                                                                               VECTORIZED_FN_ARGS(1)); \
    }

#define DEFINE_MATH_BINARY_FN_WITH_NAN_CHECK(NAME, LTYPE, RTYPE, RESULT_TYPE)                                 \
    StatusOr<ColumnPtr> MathFunctions::NAME(FunctionContext* context, const Columns& columns) {               \
        return VectorizedOuputCheckBinaryFunction<NAME##Impl, NanCheck>::evaluate<LTYPE, RTYPE, RESULT_TYPE>( \
                VECTORIZED_FN_ARGS(0), VECTORIZED_FN_ARGS(1));                                                \
    }

#define DEFINE_MATH_BINARY_FN_WITH_IMPL(NAME, LTYPE, RTYPE, RESULT_TYPE, FN) \
    DEFINE_BINARY_FUNCTION(NAME##Impl, FN);                                  \
    DEFINE_MATH_BINARY_FN(NAME, LTYPE, RTYPE, RESULT_TYPE);

#define DEFINE_MATH_UNARY_WITH_NEGATIVE_CHECK_FN_WITH_IMPL(NAME, TYPE, RESULT_TYPE, FN) \
    DEFINE_UNARY_FN(NAME##Impl, FN);                                                    \
    DEFINE_MATH_UNARY_WITH_NEGATIVE_CHECK_FN(NAME, TYPE, RESULT_TYPE);

#define DEFINE_MATH_UNARY_WITH_NON_POSITIVE_CHECK_FN_WITH_IMPL(NAME, TYPE, RESULT_TYPE, FN) \
    DEFINE_UNARY_FN(NAME##Impl, FN);                                                        \
    DEFINE_MATH_UNARY_WITH_NON_POSITIVE_CHECK_FN(NAME, TYPE, RESULT_TYPE);

#define DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(NAME, TYPE, RESULT_TYPE, FN) \
    DEFINE_UNARY_FN(NAME##Impl, FN);                                                      \
    DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN(NAME, TYPE, RESULT_TYPE);

#define DEFINE_MATH_UNARY_WITH_OUTPUT_INF_NAN_CHECK_FN_WITH_IMPL(NAME, TYPE, RESULT_TYPE, FN) \
    DEFINE_UNARY_FN(NAME##Impl, FN);                                                          \
    DEFINE_MATH_UNARY_WITH_OUTPUT_INF_NAN_CHECK_FN(NAME, TYPE, RESULT_TYPE);

#define DEFINE_MATH_BINARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(NAME, LTYPE, RTYPE, RESULT_TYPE, FN) \
    DEFINE_BINARY_FUNCTION(NAME##Impl, FN);                                                        \
    DEFINE_MATH_BINARY_WITH_OUTPUT_NAN_CHECK_FN(NAME, LTYPE, RTYPE, RESULT_TYPE);

// ============ math function impl ==========
StatusOr<ColumnPtr> MathFunctions::pi(FunctionContext* context, const Columns& columns) {
    return ColumnHelper::create_const_column<TYPE_DOUBLE>(M_PI, 1);
}

StatusOr<ColumnPtr> MathFunctions::e(FunctionContext* context, const Columns& columns) {
    return ColumnHelper::create_const_column<TYPE_DOUBLE>(M_E, 1);
}

// sign
DEFINE_UNARY_FN_WITH_IMPL(signImpl, v) {
    return v > 0 ? 1.0f : (v < 0 ? -1.0f : 0.0f);
}

DEFINE_MATH_UNARY_FN(sign, TYPE_DOUBLE, TYPE_FLOAT);

// round
DEFINE_UNARY_FN_WITH_IMPL(roundImpl, v) {
    return static_cast<int64_t>(v + ((v < 0) ? -0.5 : 0.5));
}

DEFINE_MATH_UNARY_FN(round, TYPE_DOUBLE, TYPE_BIGINT);

// log
DEFINE_BINARY_FUNCTION_WITH_IMPL(logProduceNullImpl, base, v) {
    return std::isnan(v) || base <= 0 || std::fabs(base - 1.0) < MathFunctions::EPSILON || v <= 0.0;
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(logImpl, base, v) {
    return (double)(std::log(v) / std::log(base));
}

StatusOr<ColumnPtr> MathFunctions::log(FunctionContext* context, const Columns& columns) {
    const auto& l = VECTORIZED_FN_ARGS(0);
    const auto& r = VECTORIZED_FN_ARGS(1);
    return VectorizedUnstrictBinaryFunction<logProduceNullImpl, logImpl>::evaluate<TYPE_DOUBLE>(l, r);
}

// log2
DEFINE_UNARY_FN_WITH_IMPL(log2Impl, v) {
    return (double)(std::log(v) / std::log(2.0));
}

DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN(log2, TYPE_DOUBLE, TYPE_DOUBLE);

// square
DEFINE_UNARY_FN_WITH_IMPL(squareImpl, v) {
    return v * v;
}

DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN(square, TYPE_DOUBLE, TYPE_DOUBLE);

// radians
DEFINE_UNARY_FN_WITH_IMPL(radiansImpl, v) {
    return (double)(v * M_PI / 180.0);
}

DEFINE_MATH_UNARY_FN(radians, TYPE_DOUBLE, TYPE_DOUBLE);

// degrees
DEFINE_UNARY_FN_WITH_IMPL(degreesImpl, v) {
    return (double)(v * 180.0 / M_PI);
}

DEFINE_MATH_UNARY_FN(degrees, TYPE_DOUBLE, TYPE_DOUBLE);

// bin
DEFINE_STRING_UNARY_FN_WITH_IMPL(binImpl, v) {
    auto n = static_cast<uint64_t>(v);
    const size_t max_bits = sizeof(uint64_t) * 8;
    char result[max_bits];
    uint32_t index = max_bits;
    do {
        result[--index] = '0' + (n & 1);
    } while (n >>= 1);
    return {result + index, max_bits - index};
}

StatusOr<ColumnPtr> MathFunctions::bin(FunctionContext* context, const Columns& columns) {
    return VectorizedStringStrictUnaryFunction<binImpl>::evaluate<TYPE_BIGINT, TYPE_VARCHAR>(columns[0]);
}

// unary math
// float double abs
DEFINE_MATH_UNARY_FN_WITH_IMPL(abs_double, TYPE_DOUBLE, TYPE_DOUBLE, std::fabs);
DEFINE_MATH_UNARY_FN_WITH_IMPL(abs_float, TYPE_FLOAT, TYPE_FLOAT, std::fabs);

// integer abs
// std::abs(TYPE_MIN) is still TYPE_MIN, so integers except largeint need to cast to ResultType
// before std::abs. largeint uses starrocks::abs so int128 remains portable across libstdc++ and libc++.
DEFINE_MATH_UNARY_FN_WITH_IMPL(abs_largeint, TYPE_LARGEINT, TYPE_LARGEINT, starrocks::abs);
DEFINE_MATH_UNARY_FN_CAST_WITH_IMPL(abs_bigint, TYPE_BIGINT, TYPE_LARGEINT, starrocks::abs);
DEFINE_MATH_UNARY_FN_CAST_WITH_IMPL(abs_int, TYPE_INT, TYPE_BIGINT, std::abs);
DEFINE_MATH_UNARY_FN_CAST_WITH_IMPL(abs_smallint, TYPE_SMALLINT, TYPE_INT, std::abs);
DEFINE_MATH_UNARY_FN_CAST_WITH_IMPL(abs_tinyint, TYPE_TINYINT, TYPE_SMALLINT, std::abs);

// decimal abs
DEFINE_MATH_UNARY_FN_WITH_IMPL(abs_decimal32, TYPE_DECIMAL32, TYPE_DECIMAL32, std::abs);
DEFINE_MATH_UNARY_FN_WITH_IMPL(abs_decimal64, TYPE_DECIMAL64, TYPE_DECIMAL64, std::abs);
DEFINE_MATH_UNARY_FN_WITH_IMPL(abs_decimal128, TYPE_DECIMAL128, TYPE_DECIMAL128, starrocks::abs);
DEFINE_MATH_UNARY_FN_WITH_IMPL(abs_decimal256, TYPE_DECIMAL256, TYPE_DECIMAL256, std::abs);

// degrees
DEFINE_UNARY_FN_WITH_IMPL(abs_decimalv2valImpl, v) {
    DecimalV2Value value = v;
    value.to_abs_value();
    return value;
}

DEFINE_MATH_UNARY_FN(abs_decimalv2val, TYPE_DECIMALV2, TYPE_DECIMALV2);

DEFINE_UNARY_FN_WITH_IMPL(cotImpl, v) {
    return 1.0 / std::tan(v);
}

DEFINE_MATH_UNARY_WITH_ZERO_CHECK_FN(cot, TYPE_DOUBLE, TYPE_DOUBLE);

DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(sin, TYPE_DOUBLE, TYPE_DOUBLE, std::sin);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(asin, TYPE_DOUBLE, TYPE_DOUBLE, std::asin);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(sinh, TYPE_DOUBLE, TYPE_DOUBLE, std::sinh);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(cos, TYPE_DOUBLE, TYPE_DOUBLE, std::cos);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(acos, TYPE_DOUBLE, TYPE_DOUBLE, std::acos);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(cosh, TYPE_DOUBLE, TYPE_DOUBLE, std::cosh);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(tan, TYPE_DOUBLE, TYPE_DOUBLE, std::tan);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(atan, TYPE_DOUBLE, TYPE_DOUBLE, std::atan);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(tanh, TYPE_DOUBLE, TYPE_DOUBLE, std::tanh);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(ceil, TYPE_DOUBLE, TYPE_BIGINT, std::ceil);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(floor, TYPE_DOUBLE, TYPE_BIGINT, std::floor);

DEFINE_MATH_UNARY_WITH_OUTPUT_INF_NAN_CHECK_FN_WITH_IMPL(exp, TYPE_DOUBLE, TYPE_DOUBLE, std::exp);

DEFINE_MATH_UNARY_WITH_NON_POSITIVE_CHECK_FN_WITH_IMPL(ln, TYPE_DOUBLE, TYPE_DOUBLE, std::log);
DEFINE_MATH_UNARY_WITH_NON_POSITIVE_CHECK_FN_WITH_IMPL(log10, TYPE_DOUBLE, TYPE_DOUBLE, std::log10);
DEFINE_MATH_UNARY_WITH_NEGATIVE_CHECK_FN_WITH_IMPL(sqrt, TYPE_DOUBLE, TYPE_DOUBLE, std::sqrt);
DEFINE_MATH_UNARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(cbrt, TYPE_DOUBLE, TYPE_DOUBLE, std::cbrt);

DEFINE_BINARY_FUNCTION_WITH_IMPL(truncateImpl, l, r) {
    return MathFunctions::double_round(l, r, false, true);
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(round_up_toImpl, l, r) {
    return MathFunctions::double_round(l, r, false, false);
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(powImpl, l, r) {
    // fast path
    if (r == 1.0) {
        return l;
    } else if (r == 2.0) {
        return l * l;
    } else if (r == -1.0) {
        return 1.0 / l;
    } else if (r == 0) {
        return 1.0;
    }
    return std::pow(l, r);
}
DEFINE_MATH_BINARY_WITH_OUTPUT_INF_NAN_CHECK_FN(pow, TYPE_DOUBLE, TYPE_DOUBLE, TYPE_DOUBLE);

// binary math
DEFINE_MATH_BINARY_FN_WITH_NAN_CHECK(truncate, TYPE_DOUBLE, TYPE_INT, TYPE_DOUBLE);
DEFINE_MATH_BINARY_FN(round_up_to, TYPE_DOUBLE, TYPE_INT, TYPE_DOUBLE);
DEFINE_MATH_BINARY_WITH_OUTPUT_NAN_CHECK_FN_WITH_IMPL(atan2, TYPE_DOUBLE, TYPE_DOUBLE, TYPE_DOUBLE, std::atan2);

template <LogicalType Type>
StatusOr<ColumnPtr> MathFunctions::iceberg_truncate_decimal(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<Type> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

    const int32_t original_scale = viewer.column()->scale();
    const int32_t original_precision = viewer.column()->precision();
    RunTimeCppType<Type> max_val = 1;
    for (int32_t p = original_precision; p > 0; p--) {
        max_val *= 10;
    }

#define ABS(x) ((x) < 0 ? -(x) : (x))
    ColumnBuilder<Type> builder(size, original_precision, original_scale);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            RunTimeCppType<Type> val = viewer.value(i);
            RunTimeCppType<Type> res = val - ((val % width) + width) % width;
            if (ABS(res) >= max_val) {
                std::stringstream error;
                error << "Truncate to decimal(" << original_precision << ", " << original_scale
                      << ") failed, because the result is overflow.";
                context->set_error(error.str().c_str());
                return Status::RuntimeError(error.str());
            }
            builder.append(res);
        }
    }
#undef ABS
    return builder.build(ColumnHelper::is_all_const(columns));
}

template StatusOr<ColumnPtr> MathFunctions::iceberg_truncate_decimal<TYPE_DECIMAL32>(FunctionContext*, const Columns&);
template StatusOr<ColumnPtr> MathFunctions::iceberg_truncate_decimal<TYPE_DECIMAL64>(FunctionContext*, const Columns&);
template StatusOr<ColumnPtr> MathFunctions::iceberg_truncate_decimal<TYPE_DECIMAL128>(FunctionContext*, const Columns&);

template <LogicalType Type>
StatusOr<ColumnPtr> MathFunctions::iceberg_truncate_int(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<Type> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

#define haveDifferentSigns(x, y) (((x) ^ (y)) < 0)
    ColumnBuilder<Type> builder(size);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            RunTimeCppType<Type> val = viewer.value(i);
            RunTimeCppType<Type> res = val - ((val % width) + width) % width;
            if (haveDifferentSigns(res, val)) {
                std::stringstream error;
                error << "Truncate to integer failed, because the result is overflow.";
                context->set_error(error.str().c_str());
                return Status::RuntimeError(error.str());
            }
            builder.append(res);
        }
    }
#undef haveDifferentSigns
    return builder.build(ColumnHelper::is_all_const(columns));
}
template StatusOr<ColumnPtr> MathFunctions::iceberg_truncate_int<TYPE_INT>(FunctionContext*, const Columns&);
template StatusOr<ColumnPtr> MathFunctions::iceberg_truncate_int<TYPE_BIGINT>(FunctionContext*, const Columns&);

template <LogicalType Type>
StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_int(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<Type> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

    ColumnBuilder<TYPE_INT> builder(size);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            int64_t val = viewer.value(i);
            int32_t hash;
            murmur_hash3_x86_32(&val, sizeof(val), 0, &hash);
            builder.append(static_cast<int32_t>((hash & INT_MAX) % width));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

template StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_int<TYPE_INT>(FunctionContext*, const Columns&);
template StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_int<TYPE_BIGINT>(FunctionContext*, const Columns&);

StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_string(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

    ColumnBuilder<TYPE_INT> builder(size);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            auto val = viewer.value(i);
            int32_t hash;
            murmur_hash3_x86_32(val.data, val.size, 0, &hash);
            builder.append(static_cast<int32_t>((hash & INT_MAX) % width));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_date(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<TYPE_DATE> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

    ColumnBuilder<TYPE_INT> builder(size);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            int64_t val = viewer.value(i).julian() - date::UNIX_EPOCH_JULIAN;
            int32_t hash;
            murmur_hash3_x86_32(&val, sizeof(int64_t), 0, &hash);
            builder.append(static_cast<int32_t>((hash & INT_MAX) % width));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_datetime(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<TYPE_DATETIME> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

    ColumnBuilder<TYPE_INT> builder(size);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            int64_t val = iceberg_datetime_to_epoch_microseconds(viewer.value(i));
            int32_t hash;
            murmur_hash3_x86_32(&val, sizeof(int64_t), 0, &hash);
            builder.append(static_cast<int32_t>((hash & INT_MAX) % width));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_timestamptz_datetime(FunctionContext* context,
                                                                       const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<TYPE_DATETIME> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

    ColumnBuilder<TYPE_INT> builder(size);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            int64_t val;
            if (!iceberg_timestamptz_to_epoch_microseconds(context, viewer.value(i), &val)) {
                builder.append_null();
                continue;
            }
            int32_t hash;
            murmur_hash3_x86_32(&val, sizeof(int64_t), 0, &hash);
            builder.append(static_cast<int32_t>((hash & INT_MAX) % width));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

template <typename T>
vector<uint8_t> MathFunctions::int_to_byte_array(T value) {
    std::vector<uint8_t> byteArray(sizeof(value));
    memcpy(byteArray.data(), &value, sizeof(value));
    if (value < 0) {
        value = ~value;
    }
    int bitLength = 0;
    while (value > 0) {
        value >>= 1;
        bitLength++;
    }
    // Convert the integer to its byte representation (Big Endian)
    byteArray.resize(bitLength / 8 + 1);
    std::reverse(byteArray.begin(), byteArray.end());
    return byteArray;
}

template vector<uint8_t> MathFunctions::int_to_byte_array<int32_t>(int32_t value);
template vector<uint8_t> MathFunctions::int_to_byte_array<int64_t>(int64_t value);
template vector<uint8_t> MathFunctions::int_to_byte_array<int128_t>(int128_t value);

template <LogicalType Type>
StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_decimal(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const int size = columns[0]->size();
    ColumnViewer<Type> viewer(columns[0]);
    int64_t width = ColumnViewer<TYPE_INT>(columns[1]).value(0);

    ColumnBuilder<TYPE_INT> builder(size);
    for (int i = 0; i < size; i++) {
        if (viewer.is_null(i)) {
            builder.append_null();
        } else {
            auto val = viewer.value(i);
            auto byte_array = int_to_byte_array(val);
            int32_t hash;
            murmur_hash3_x86_32(byte_array.data(), byte_array.size(), 0, &hash);
            builder.append(static_cast<int32_t>((hash & INT_MAX) % width));
        }
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

template StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_decimal<TYPE_DECIMAL32>(FunctionContext*, const Columns&);
template StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_decimal<TYPE_DECIMAL64>(FunctionContext*, const Columns&);
template StatusOr<ColumnPtr> MathFunctions::iceberg_bucket_decimal<TYPE_DECIMAL128>(FunctionContext*, const Columns&);

#undef DEFINE_MATH_UNARY_FN
#undef DEFINE_MATH_UNARY_FN_WITH_IMPL
#undef DEFINE_MATH_BINARY_FN
#undef DEFINE_MATH_BINARY_FN_WITH_IMPL

const double log_10[] = {
        1e000, 1e001, 1e002, 1e003, 1e004, 1e005, 1e006, 1e007, 1e008, 1e009, 1e010, 1e011, 1e012, 1e013, 1e014, 1e015,
        1e016, 1e017, 1e018, 1e019, 1e020, 1e021, 1e022, 1e023, 1e024, 1e025, 1e026, 1e027, 1e028, 1e029, 1e030, 1e031,
        1e032, 1e033, 1e034, 1e035, 1e036, 1e037, 1e038, 1e039, 1e040, 1e041, 1e042, 1e043, 1e044, 1e045, 1e046, 1e047,
        1e048, 1e049, 1e050, 1e051, 1e052, 1e053, 1e054, 1e055, 1e056, 1e057, 1e058, 1e059, 1e060, 1e061, 1e062, 1e063,
        1e064, 1e065, 1e066, 1e067, 1e068, 1e069, 1e070, 1e071, 1e072, 1e073, 1e074, 1e075, 1e076, 1e077, 1e078, 1e079,
        1e080, 1e081, 1e082, 1e083, 1e084, 1e085, 1e086, 1e087, 1e088, 1e089, 1e090, 1e091, 1e092, 1e093, 1e094, 1e095,
        1e096, 1e097, 1e098, 1e099, 1e100, 1e101, 1e102, 1e103, 1e104, 1e105, 1e106, 1e107, 1e108, 1e109, 1e110, 1e111,
        1e112, 1e113, 1e114, 1e115, 1e116, 1e117, 1e118, 1e119, 1e120, 1e121, 1e122, 1e123, 1e124, 1e125, 1e126, 1e127,
        1e128, 1e129, 1e130, 1e131, 1e132, 1e133, 1e134, 1e135, 1e136, 1e137, 1e138, 1e139, 1e140, 1e141, 1e142, 1e143,
        1e144, 1e145, 1e146, 1e147, 1e148, 1e149, 1e150, 1e151, 1e152, 1e153, 1e154, 1e155, 1e156, 1e157, 1e158, 1e159,
        1e160, 1e161, 1e162, 1e163, 1e164, 1e165, 1e166, 1e167, 1e168, 1e169, 1e170, 1e171, 1e172, 1e173, 1e174, 1e175,
        1e176, 1e177, 1e178, 1e179, 1e180, 1e181, 1e182, 1e183, 1e184, 1e185, 1e186, 1e187, 1e188, 1e189, 1e190, 1e191,
        1e192, 1e193, 1e194, 1e195, 1e196, 1e197, 1e198, 1e199, 1e200, 1e201, 1e202, 1e203, 1e204, 1e205, 1e206, 1e207,
        1e208, 1e209, 1e210, 1e211, 1e212, 1e213, 1e214, 1e215, 1e216, 1e217, 1e218, 1e219, 1e220, 1e221, 1e222, 1e223,
        1e224, 1e225, 1e226, 1e227, 1e228, 1e229, 1e230, 1e231, 1e232, 1e233, 1e234, 1e235, 1e236, 1e237, 1e238, 1e239,
        1e240, 1e241, 1e242, 1e243, 1e244, 1e245, 1e246, 1e247, 1e248, 1e249, 1e250, 1e251, 1e252, 1e253, 1e254, 1e255,
        1e256, 1e257, 1e258, 1e259, 1e260, 1e261, 1e262, 1e263, 1e264, 1e265, 1e266, 1e267, 1e268, 1e269, 1e270, 1e271,
        1e272, 1e273, 1e274, 1e275, 1e276, 1e277, 1e278, 1e279, 1e280, 1e281, 1e282, 1e283, 1e284, 1e285, 1e286, 1e287,
        1e288, 1e289, 1e290, 1e291, 1e292, 1e293, 1e294, 1e295, 1e296, 1e297, 1e298, 1e299, 1e300, 1e301, 1e302, 1e303,
        1e304, 1e305, 1e306, 1e307, 1e308};

#define ARRAY_ELEMENTS_NUM(A) ((uint64_t)(sizeof(A) / sizeof(A[0])))

double MathFunctions::double_round(double value, int64_t dec, bool dec_unsigned, bool truncate) {
    bool dec_negative = (dec < 0) && !dec_unsigned;
    uint64_t abs_dec = dec_negative ? -dec : dec;
    /*
       tmp2 is here to avoid return the value with 80 bit precision
       This will fix that the test round(0.1,1) = round(0.1,1) is true
       Tagging with volatile is no guarantee, it may still be optimized away...
       */
    volatile double tmp2 = 0.0;

    double tmp = (abs_dec < ARRAY_ELEMENTS_NUM(log_10) ? log_10[abs_dec] : std::pow(10.0, (double)abs_dec));

    // Pre-compute these, to avoid optimizing away e.g. 'floor(v/tmp) * tmp'.
    volatile double value_div_tmp = value / tmp;
    volatile double value_mul_tmp = value * tmp;

    if (dec_negative && std::isinf(tmp)) {
        tmp2 = 0.0;
    } else if (!dec_negative && std::isinf(value_mul_tmp)) {
        tmp2 = value;
    } else if (truncate) {
        if (value >= 0.0) {
            tmp2 = dec < 0 ? std::floor(value_div_tmp) * tmp : std::floor(value_mul_tmp) / tmp;
        } else {
            tmp2 = dec < 0 ? std::ceil(value_div_tmp) * tmp : std::ceil(value_mul_tmp) / tmp;
        }
    } else {
        // Because std::rint(+2.5) = 2, std::rint(+3.5) = 4,
        // so It's not expected result, we should use std::round instead of std::rint.
        tmp2 = dec < 0 ? std::round(value_div_tmp) * tmp : std::round(value_mul_tmp) / tmp;
    }

    return tmp2;
}

bool MathFunctions::decimal_in_base_to_decimal(int64_t src_num, int8_t src_base, int64_t* result) {
    uint64_t temp_num = std::abs(src_num);
    int64_t place = 1;
    *result = 0;
    do {
        int64_t digit = temp_num % 10;
        // Reset result if digit is not representable in src_base.
        if (digit >= src_base) {
            *result = 0;
            place = 1;
        } else {
            *result += digit * place;
            place *= src_base;
            // Overflow.
            if (UNLIKELY(*result < digit)) {
                return false;
            }
        }
        temp_num /= 10;
    } while (temp_num > 0);
    *result = (src_num < 0) ? -(*result) : *result;
    return true;
}

bool MathFunctions::handle_parse_result(int8_t dest_base, int64_t* num, StringParser::ParseResult parse_res) {
    // On overflow set special value depending on dest_base.
    // This is consistent with Hive and MySQL's behavior.
    if (parse_res == StringParser::PARSE_OVERFLOW) {
        if (dest_base < 0) {
            *num = -1;
        } else {
            *num = std::numeric_limits<uint64_t>::max();
        }
    } else if (parse_res == StringParser::PARSE_FAILURE) {
        // Some other error condition.
        return false;
    }
    return true;
}

const char* MathFunctions::_s_alphanumeric_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
std::string MathFunctions::decimal_to_base(int64_t src_num, int8_t dest_base) {
    // Max number of digits of any base (base 2 gives max digits), plus sign.
    const size_t max_digits = sizeof(uint64_t) * 8 + 1;
    char buf[max_digits];
    size_t result_len = 0;
    int32_t buf_index = max_digits - 1;
    uint64_t temp_num;
    if (dest_base < 0) {
        // Dest base is negative, treat src_num as signed.
        temp_num = std::abs(src_num);
    } else {
        // Dest base is positive. We must interpret src_num in 2's complement.
        // Convert to an unsigned int to properly deal with 2's complement conversion.
        temp_num = static_cast<uint64_t>(src_num);
    }
    int abs_base = std::abs(dest_base);
    do {
        buf[buf_index] = _s_alphanumeric_chars[temp_num % abs_base];
        temp_num /= abs_base;
        --buf_index;
        ++result_len;
    } while (temp_num > 0);
    // Add optional sign.
    if (src_num < 0 && dest_base < 0) {
        buf[buf_index] = '-';
        ++result_len;
    }
    return {buf + max_digits - result_len, result_len};
}

template <DecimalRoundRule rule, bool keep_scale>
void MathFunctions::decimal_round(const int128_t& lv, const int32_t& original_scale, const int32_t& rv, int128_t* res,
                                  bool* is_over_flow) {
    *is_over_flow = false;
    int32_t target_scale = rv;
    int32_t max_precision = decimal_precision_limit<int128_t>;
    if (target_scale > max_precision) {
        target_scale = max_precision;
    } else if (target_scale < -max_precision) {
        target_scale = -max_precision;
    }
    int32_t scale_diff = target_scale - original_scale;
    if (std::abs(scale_diff) > max_precision) {
        (*is_over_flow) = true;
        return;
    }
    if (scale_diff > 0) {
        if (keep_scale) {
            // Up scale and down scale can offset when keep scale is set
            // E.g. 1.2345 --(scale up by 2)--> 1.234500 --(scale down by 2)--> 1.2345
            *res = lv;
        } else {
            (*is_over_flow) |= DecimalV3Cast::round<int128_t, int128_t, int128_t, rule, true, true>(
                    lv, get_scale_factor<int128_t>(scale_diff), res);
        }
    } else if (scale_diff < 0) {
        // Up scale and down scale cannot offset when keep scale is set
        // E.g. 1.2345 --(scale down by 2)--> 1.23 --(scale up by 2)--> 1.2300
        (*is_over_flow) |= DecimalV3Cast::round<int128_t, int128_t, int128_t, rule, false, true>(
                lv, get_scale_factor<int128_t>(-scale_diff), res);
        if (keep_scale) {
            int128_t new_res;
            (*is_over_flow) |= DecimalV3Cast::round<int128_t, int128_t, int128_t, rule, true, true>(
                    *res, get_scale_factor<int128_t>(-scale_diff), &new_res);
            *res = new_res;
        } else if (target_scale < 0) {
            // E.g. round(13.14, -1), 13.14 --(scale down by 3)--> 1e1 --(scale up by 1)--> 10
            int128_t new_res;
            (*is_over_flow) |= DecimalV3Cast::round<int128_t, int128_t, int128_t, rule, true, true>(
                    *res, get_scale_factor<int128_t>(-target_scale), &new_res);
            *res = new_res;
        }
    } else {
        *res = lv;
    }
}

template <DecimalRoundRule rule>
StatusOr<ColumnPtr> MathFunctions::decimal_round(FunctionContext* context, const Columns& columns) {
    const auto& type = context->get_return_type();

    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnPtr c0 = columns[0];
    ColumnPtr c1 = columns[1];

    NullColumn::MutablePtr null_flags;
    bool has_null = false;
    if (c0->has_null() || c1->has_null()) {
        has_null = true;
        null_flags = FunctionHelper::union_nullable_column(c0, c1);
    } else {
        null_flags = NullColumn::create();
        null_flags->reserve(c0->size());
        null_flags->append_default(c0->size());
    }

    const bool c0_is_const = c0->is_constant();
    const bool c1_is_const = c1->is_constant();

    const int size = c0->size();
    // Unpack const
    c0 = FunctionHelper::get_data_column_of_const(c0);
    c1 = FunctionHelper::get_data_column_of_const(c1);

    // Unpack nullable
    c0 = FunctionHelper::get_data_column_of_nullable(c0);
    c1 = FunctionHelper::get_data_column_of_nullable(c1);

    MutableColumnPtr res = RunTimeColumnType<TYPE_DECIMAL128>::create(type.precision, type.scale);
    res->resize_uninitialized(size);

    const int32_t original_scale = ColumnHelper::cast_to_raw<TYPE_DECIMAL128>(c0)->scale();

    auto* res_col = ColumnHelper::cast_to_raw<TYPE_DECIMAL128>(res.get());
    auto* c0_col = ColumnHelper::cast_to_raw<TYPE_DECIMAL128>(c0);
    auto* c1_col = ColumnHelper::cast_to_raw<TYPE_INT>(c1);

    const int128_t* raw_c0 = c0_col->get_data().data();
    const int32_t* raw_c1 = c1_col->get_data().data();
    int128_t* raw_res = res_col->get_data().data();
    auto& null_data = null_flags->get_data();
    uint8_t* raw_null_flags = null_data.data();

    // If c2 is not const, than we need to keep the original scale
    // TODO(hcf) For truncate(v, d), we also to keep the scale if d is constant
    if (c0_is_const && c1_is_const) {
        bool is_over_flow;
        MathFunctions::decimal_round<rule, false>(raw_c0[0], original_scale, raw_c1[0], &raw_res[0], &is_over_flow);
        if (is_over_flow) {
            DCHECK(!has_null);
            res = ColumnHelper::create_const_null_column(size);
        } else {
            res->resize(1);
            res = ConstColumn::create(std::move(res), size);
        }
    } else if (c0_is_const) {
        for (auto i = 0; i < size; i++) {
            bool is_over_flow;
            MathFunctions::decimal_round<rule, true>(raw_c0[0], original_scale, raw_c1[i], &raw_res[i], &is_over_flow);
            if (is_over_flow) {
                has_null = true;
                raw_null_flags[i] = 1;
            }
        }
    } else if (c1_is_const) {
        for (auto i = 0; i < size; i++) {
            bool is_over_flow;
            MathFunctions::decimal_round<rule, false>(raw_c0[i], original_scale, raw_c1[0], &raw_res[i], &is_over_flow);
            if (is_over_flow) {
                has_null = true;
                raw_null_flags[i] = 1;
            }
        }
    } else {
        for (auto i = 0; i < size; i++) {
            bool is_over_flow;
            MathFunctions::decimal_round<rule, true>(raw_c0[i], original_scale, raw_c1[i], &raw_res[i], &is_over_flow);
            if (is_over_flow) {
                has_null = true;
                raw_null_flags[i] = 1;
            }
        }
    }

    if (has_null) {
        return NullableColumn::create(std::move(res), std::move(null_flags));
    } else {
        return res;
    }
}

StatusOr<ColumnPtr> MathFunctions::truncate_decimal128(FunctionContext* context, const Columns& columns) {
    return decimal_round<DecimalRoundRule::ROUND_TRUNCATE>(context, columns);
}

StatusOr<ColumnPtr> MathFunctions::round_decimal128(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    Columns new_columns;
    new_columns.push_back(columns[0]);
    new_columns.push_back(ColumnHelper::create_const_column<LogicalType::TYPE_INT>(0, columns[0]->size()));
    return decimal_round<DecimalRoundRule::ROUND_HALF_UP>(context, new_columns);
}

StatusOr<ColumnPtr> MathFunctions::round_up_to_decimal128(FunctionContext* context, const Columns& columns) {
    return decimal_round<DecimalRoundRule::ROUND_HALF_UP>(context, columns);
}

StatusOr<ColumnPtr> MathFunctions::conv_int(FunctionContext* context, const Columns& columns) {
    auto bigint = ColumnViewer<TYPE_BIGINT>(columns[0]);
    auto src_base = ColumnViewer<TYPE_TINYINT>(columns[1]);
    auto dest_base = ColumnViewer<TYPE_TINYINT>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (bigint.is_null(row) || src_base.is_null(row) || dest_base.is_null(row)) {
            result.append_null();
            continue;
        }

        int64_t binint_value = bigint.value(row);
        int8_t src_base_value = src_base.value(row);
        int8_t dest_base_value = dest_base.value(row);
        if (std::abs(src_base_value) < MIN_BASE || std::abs(src_base_value) > MAX_BASE ||
            std::abs(dest_base_value) < MIN_BASE || std::abs(dest_base_value) > MAX_BASE) {
            result.append_null();
            continue;
        }

        int64_t decimal_num = binint_value;
        if (src_base_value != 10) {
            if (!decimal_in_base_to_decimal(binint_value, std::abs(src_base_value), &decimal_num)) {
                handle_parse_result(dest_base_value, &decimal_num, StringParser::PARSE_OVERFLOW);
            }
        }

        result.append(Slice(decimal_to_base(decimal_num, dest_base_value)));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> MathFunctions::conv_string(FunctionContext* context, const Columns& columns) {
    auto string_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto src_base = ColumnViewer<TYPE_TINYINT>(columns[1]);
    auto dest_base = ColumnViewer<TYPE_TINYINT>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (string_viewer.is_null(row) || src_base.is_null(row) || dest_base.is_null(row)) {
            result.append_null();
            continue;
        }

        auto string_value = string_viewer.value(row);
        int8_t src_base_value = src_base.value(row);
        int8_t dest_base_value = dest_base.value(row);
        if (std::abs(src_base_value) < MIN_BASE || std::abs(src_base_value) > MAX_BASE ||
            std::abs(dest_base_value) < MIN_BASE || std::abs(dest_base_value) > MAX_BASE) {
            result.append_null();
            continue;
        }
        bool is_signed = src_base_value < 0;
        char* data_ptr = reinterpret_cast<char*>(string_value.data);
        int digit_start_offset = StringParser::skip_leading_whitespace(data_ptr, string_value.size);
        if (digit_start_offset == string_value.size) {
            result.append(Slice("0", 1));
            continue;
        }
        bool negative = data_ptr[digit_start_offset] == '-';
        digit_start_offset += negative;
        StringParser::ParseResult parse_res;
        auto decimal64_num = StringParser::string_to_int<uint64_t>(data_ptr + digit_start_offset,
                                                                   string_value.size - digit_start_offset,
                                                                   std::abs(src_base_value), &parse_res);
        if (parse_res == StringParser::PARSE_SUCCESS) {
            if (is_signed) {
                if (negative && decimal64_num > 0ull - std::numeric_limits<int64_t>::min()) {
                    decimal64_num = 0ull - std::numeric_limits<int64_t>::min();
                }
                if (!negative && decimal64_num > std::numeric_limits<int64_t>::max()) {
                    decimal64_num = std::numeric_limits<int64_t>::max();
                }
            }
        } else if (parse_res == StringParser::PARSE_FAILURE) {
            result.append(Slice("0", 1));
            continue;
        } else if (parse_res == StringParser::PARSE_OVERFLOW) {
            if (is_signed) {
                decimal64_num =
                        negative ? (0ull - std::numeric_limits<int64_t>::min()) : std::numeric_limits<int64_t>::max();
            } else {
                decimal64_num = negative ? 0 : std::numeric_limits<uint64_t>::max();
            }
        } else {
            CHECK(false) << "unreachable path, parse_res: " << parse_res;
        }
        if (negative) {
            decimal64_num = (~decimal64_num + 1);
        }

        result.append(Slice(decimal_to_base(decimal64_num, dest_base_value)));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

Status MathFunctions::rand_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        if (context->get_num_args() == 1) {
            // This is a call to RandSeed, initialize the seed
            // TODO: should we support non-constant seed?
            if (!context->is_constant_column(0)) {
                std::stringstream error;
                error << "Seed argument to rand() must be constant";
                context->set_error(error.str().c_str());
                return Status::InvalidArgument(error.str());
            }

            auto seed_column = context->get_constant_column(0);
            if (seed_column->only_null()) {
                return Status::OK();
            }

            int64_t seed_value = ColumnHelper::get_const_value<TYPE_BIGINT>(seed_column);
            generator.seed(seed_value);
        }
    }
    return Status::OK();
}

Status MathFunctions::rand_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    return Status::OK();
}

StatusOr<ColumnPtr> MathFunctions::rand(FunctionContext* context, const Columns& columns) {
    int32_t num_rows = ColumnHelper::get_const_value<TYPE_INT>(columns[columns.size() - 1]);
    ColumnBuilder<TYPE_DOUBLE> result(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        result.append(distribution(generator));
    }

    return result.build(false);
}

StatusOr<ColumnPtr> MathFunctions::rand_seed(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);

    if (columns[0]->only_null()) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }

    return rand(context, columns);
}

#ifdef __AVX2__
static float sum_m256(__m256 v) {
    __m256 hadd = _mm256_hadd_ps(v, v);
    __m256 hadd2 = _mm256_hadd_ps(hadd, hadd);
    __m128 vlow = _mm256_castps256_ps128(hadd2);
    __m128 vhigh = _mm256_extractf128_ps(hadd2, 1);
    __m128 result = _mm_add_ss(vlow, vhigh);
    return _mm_cvtss_f32(result);
}

static inline float fast_rsqrt_nr(float x) {
    // Guard against denormal/very-small inputs where _mm_rsqrt_ss produces infinity.
    // Fall back to standard sqrt for such cases.
    if (x < 1e-30f) {
        return 1.0f / std::sqrt(x);
    }
    __m128 vx = _mm_set_ss(x);
    __m128 y = _mm_rsqrt_ss(vx);
    // One Newton-Raphson refinement step for better accuracy.
    const __m128 half = _mm_set_ss(0.5f);
    const __m128 three_halves = _mm_set_ss(1.5f);
    y = _mm_mul_ss(y, _mm_sub_ss(three_halves, _mm_mul_ss(half, _mm_mul_ss(vx, _mm_mul_ss(y, y)))));
    return _mm_cvtss_f32(y);
}
#endif

#ifndef __AVX2__
static inline bool offsets_equal_dim_scalar(const uint32_t* offsets, size_t num_rows, uint32_t dim) {
    if (dim == 0) {
        return false;
    }
    for (size_t i = 0; i < num_rows; ++i) {
        if (offsets[i + 1] - offsets[i] != dim) {
            return false;
        }
    }
    return true;
}

static inline bool offsets_equal_dim_two_scalar(const uint32_t* base_offsets, const uint32_t* target_offsets,
                                                size_t num_rows, uint32_t dim) {
    if (dim == 0) {
        return false;
    }
    for (size_t i = 0; i < num_rows; ++i) {
        if (base_offsets[i + 1] - base_offsets[i] != dim || target_offsets[i + 1] - target_offsets[i] != dim) {
            return false;
        }
    }
    return true;
}

static inline bool offsets_equal_nonzero_scalar(const uint32_t* base_offsets, const uint32_t* target_offsets,
                                                size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        uint32_t b_dim = base_offsets[i + 1] - base_offsets[i];
        uint32_t t_dim = target_offsets[i + 1] - target_offsets[i];
        if (b_dim == 0 || b_dim != t_dim) {
            return false;
        }
    }
    return true;
}
#else // __AVX2__
static inline bool offsets_equal_dim_avx2(const uint32_t* offsets, size_t num_rows, uint32_t dim) {
    if (dim == 0) {
        return false;
    }
    __m256i dim_vec = _mm256_set1_epi32(static_cast<int>(dim));
    size_t i = 0;
    for (; i + 8 <= num_rows; i += 8) {
        __m256i o0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + i));
        __m256i o1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(offsets + i + 1));
        __m256i diff = _mm256_sub_epi32(o1, o0);
        __m256i eq = _mm256_cmpeq_epi32(diff, dim_vec);
        if (_mm256_movemask_epi8(eq) != -1) {
            return false;
        }
    }
    for (; i < num_rows; ++i) {
        if (offsets[i + 1] - offsets[i] != dim) {
            return false;
        }
    }
    return true;
}

static inline bool offsets_equal_dim_two_avx2(const uint32_t* base_offsets, const uint32_t* target_offsets,
                                              size_t num_rows, uint32_t dim) {
    if (dim == 0) {
        return false;
    }
    __m256i dim_vec = _mm256_set1_epi32(static_cast<int>(dim));
    size_t i = 0;
    for (; i + 8 <= num_rows; i += 8) {
        __m256i b0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base_offsets + i));
        __m256i b1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base_offsets + i + 1));
        __m256i t0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(target_offsets + i));
        __m256i t1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(target_offsets + i + 1));
        __m256i bdiff = _mm256_sub_epi32(b1, b0);
        __m256i tdiff = _mm256_sub_epi32(t1, t0);
        __m256i b_eq = _mm256_cmpeq_epi32(bdiff, dim_vec);
        __m256i t_eq = _mm256_cmpeq_epi32(tdiff, dim_vec);
        __m256i ok = _mm256_and_si256(b_eq, t_eq);
        if (_mm256_movemask_epi8(ok) != -1) {
            return false;
        }
    }
    for (; i < num_rows; ++i) {
        if (base_offsets[i + 1] - base_offsets[i] != dim || target_offsets[i + 1] - target_offsets[i] != dim) {
            return false;
        }
    }
    return true;
}

static inline bool offsets_equal_nonzero_avx2(const uint32_t* base_offsets, const uint32_t* target_offsets,
                                              size_t num_rows) {
    __m256i zero = _mm256_setzero_si256();
    size_t i = 0;
    for (; i + 8 <= num_rows; i += 8) {
        __m256i b0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base_offsets + i));
        __m256i b1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(base_offsets + i + 1));
        __m256i t0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(target_offsets + i));
        __m256i t1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(target_offsets + i + 1));
        __m256i bdiff = _mm256_sub_epi32(b1, b0);
        __m256i tdiff = _mm256_sub_epi32(t1, t0);
        __m256i eq = _mm256_cmpeq_epi32(bdiff, tdiff);
        __m256i gt0 = _mm256_cmpgt_epi32(bdiff, zero);
        __m256i ok = _mm256_and_si256(eq, gt0);
        if (_mm256_movemask_epi8(ok) != -1) {
            return false;
        }
    }
    for (; i < num_rows; ++i) {
        uint32_t b_dim = base_offsets[i + 1] - base_offsets[i];
        uint32_t t_dim = target_offsets[i + 1] - target_offsets[i];
        if (b_dim == 0 || b_dim != t_dim) {
            return false;
        }
    }
    return true;
}
#endif

static inline bool offsets_equal_dim(const uint32_t* offsets, size_t num_rows, uint32_t dim) {
#ifdef __AVX2__
    return offsets_equal_dim_avx2(offsets, num_rows, dim);
#else
    return offsets_equal_dim_scalar(offsets, num_rows, dim);
#endif
}

static inline bool offsets_equal_dim_two(const uint32_t* base_offsets, const uint32_t* target_offsets, size_t num_rows,
                                         uint32_t dim) {
#ifdef __AVX2__
    return offsets_equal_dim_two_avx2(base_offsets, target_offsets, num_rows, dim);
#else
    return offsets_equal_dim_two_scalar(base_offsets, target_offsets, num_rows, dim);
#endif
}

static inline bool offsets_equal_nonzero(const uint32_t* base_offsets, const uint32_t* target_offsets,
                                         size_t num_rows) {
#ifdef __AVX2__
    return offsets_equal_nonzero_avx2(base_offsets, target_offsets, num_rows);
#else
    return offsets_equal_nonzero_scalar(base_offsets, target_offsets, num_rows);
#endif
}

static inline float sum_squares_float(const float* data, size_t dim) {
    float sum = 0.0f;
#ifdef __AVX2__
    __m256 sum_vec = _mm256_setzero_ps();
    size_t j = 0;
    for (; j + 7 < dim; j += 8) {
        __m256 v = _mm256_loadu_ps(data + j);
        sum_vec = _mm256_add_ps(sum_vec, _mm256_mul_ps(v, v));
    }
    sum += sum_m256(sum_vec);
    for (; j < dim; ++j) {
        sum += data[j] * data[j];
    }
#else
    for (size_t j = 0; j < dim; ++j) {
        sum += data[j] * data[j];
    }
#endif
    return sum;
}

template <bool isNorm>
static inline void vector_cosine_similarity(const float* base_vec, size_t dim, const float* column_data,
                                            size_t num_rows, float* out) {
    float base_sum = 0.0f;
    float base_inv_norm = 0.0f;
    if constexpr (!isNorm) {
        base_sum = sum_squares_float(base_vec, dim);
        if (base_sum == 0.0f) {
            for (size_t i = 0; i < num_rows; ++i) {
                out[i] = 0.0f;
            }
            return;
        }
#ifdef __AVX2__
        base_inv_norm = fast_rsqrt_nr(base_sum);
#else
        base_inv_norm = 1.0f / std::sqrt(base_sum);
#endif
    }

    for (size_t i = 0; i < num_rows; ++i) {
        const float* target = column_data + i * dim;
        float sum = 0.0f;
        float target_sum = 0.0f;
        size_t j = 0;
#ifdef __AVX2__
        __m256 sum_vec = _mm256_setzero_ps();
        __m256 target_sum_vec = _mm256_setzero_ps();
        for (; j + 7 < dim; j += 8) {
            __m256 base_vec_data = _mm256_loadu_ps(base_vec + j);
            __m256 target_vec_data = _mm256_loadu_ps(target + j);
            __m256 mul_vec = _mm256_mul_ps(base_vec_data, target_vec_data);
            sum_vec = _mm256_add_ps(sum_vec, mul_vec);
            if constexpr (!isNorm) {
                __m256 target_mul_vec = _mm256_mul_ps(target_vec_data, target_vec_data);
                target_sum_vec = _mm256_add_ps(target_sum_vec, target_mul_vec);
            }
        }
        sum += sum_m256(sum_vec);
        if constexpr (!isNorm) {
            target_sum += sum_m256(target_sum_vec);
        }
#endif
        for (; j < dim; ++j) {
            sum += base_vec[j] * target[j];
            if constexpr (!isNorm) {
                target_sum += target[j] * target[j];
            }
        }
        if constexpr (isNorm) {
            out[i] = sum;
        } else {
            if (target_sum == 0.0f) {
                out[i] = 0.0f;
            } else {
#ifdef __AVX2__
                float target_inv_norm = fast_rsqrt_nr(target_sum);
                out[i] = sum * base_inv_norm * target_inv_norm;
#else
                out[i] = sum * base_inv_norm / std::sqrt(target_sum);
#endif
            }
        }
    }
}

template <bool isNorm>
static inline void cosine_similarity_fixed_dim_float(const float* base_data, const float* target_data, size_t num_rows,
                                                     size_t dim, float* out) {
    for (size_t i = 0; i < num_rows; ++i) {
        const float* base = base_data + i * dim;
        const float* target = target_data + i * dim;
        float sum = 0.0f;
        float base_sum = 0.0f;
        float target_sum = 0.0f;
        size_t j = 0;
#ifdef __AVX2__
        __m256 sum_vec = _mm256_setzero_ps();
        __m256 base_sum_vec = _mm256_setzero_ps();
        __m256 target_sum_vec = _mm256_setzero_ps();
        for (; j + 7 < dim; j += 8) {
            __m256 base_data_vec = _mm256_loadu_ps(base + j);
            __m256 target_data_vec = _mm256_loadu_ps(target + j);

            __m256 mul_vec = _mm256_mul_ps(base_data_vec, target_data_vec);
            sum_vec = _mm256_add_ps(sum_vec, mul_vec);

            if constexpr (!isNorm) {
                __m256 base_mul_vec = _mm256_mul_ps(base_data_vec, base_data_vec);
                base_sum_vec = _mm256_add_ps(base_sum_vec, base_mul_vec);
                __m256 target_mul_vec = _mm256_mul_ps(target_data_vec, target_data_vec);
                target_sum_vec = _mm256_add_ps(target_sum_vec, target_mul_vec);
            }
        }
        sum += sum_m256(sum_vec);
        if constexpr (!isNorm) {
            base_sum += sum_m256(base_sum_vec);
            target_sum += sum_m256(target_sum_vec);
        }
#endif
        for (; j < dim; ++j) {
            sum += base[j] * target[j];
            if constexpr (!isNorm) {
                base_sum += base[j] * base[j];
                target_sum += target[j] * target[j];
            }
        }
        if constexpr (isNorm) {
            out[i] = sum;
        } else {
            if (base_sum == 0.0f || target_sum == 0.0f) {
                out[i] = 0.0f;
            } else {
                out[i] = sum / (std::sqrt(base_sum) * std::sqrt(target_sum));
            }
        }
    }
}

template <LogicalType TYPE, bool isNorm>
StatusOr<ColumnPtr> MathFunctions::cosine_similarity(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);

    const Column* base = columns[0].get();
    const Column* target = columns[1].get();
    size_t target_size = target->size();
    if (base->size() != target_size) {
        return Status::InvalidArgument(
                fmt::format("cosine_similarity requires equal length arrays. base array size is {} and target "
                            "array size is {}.",
                            base->size(), target->size()));
    }
    if (base->has_null() || target->has_null()) {
        return Status::InvalidArgument(
                fmt::format("cosine_similarity does not support null values. {} array has null value.",
                            base->has_null() ? "base" : "target"));
    }

    bool base_is_const = base->is_constant();
    bool target_is_const = target->is_constant();

    // If both are const, expand one side to N rows to reuse the single-const fast path.
    // This avoids dereferencing size-1 offsets with target_size rows.
    if (base_is_const && target_is_const) {
        const auto* const_column = down_cast<const ConstColumn*>(target);
        const_column->data_column()->as_mutable_raw_ptr()->assign(target_size, 0);
        target = const_column->data_column().get();
        target_is_const = false;
    }

    // Helper: strip ConstColumn/NullableColumn wrappers to reach the underlying ArrayColumn.
    // For const columns we intentionally do NOT call assign() to avoid copying dim*N floats.
    // Instead we keep the original size-1 data_column and read its first-row pointer directly
    // inside the const fast-paths below.
    auto unwrap_to_array = [](const Column* col) -> const Column* {
        if (col->is_constant()) {
            col = down_cast<const ConstColumn*>(col)->data_column().get();
        }
        if (col->is_nullable()) {
            col = down_cast<const NullableColumn*>(col)->data_column().get();
        }
        return col;
    };

    // For non-const columns we still need the full N-row view.
    if (!base_is_const && base->is_nullable()) {
        base = down_cast<const NullableColumn*>(base)->data_column().get();
    }
    if (!target_is_const && target->is_nullable()) {
        target = down_cast<const NullableColumn*>(target)->data_column().get();
    }

    // check dimension equality.
    // For const columns use the unwrapped size-1 ArrayColumn; for non-const use the N-row one.
    const Column* base_arr_for_meta = base_is_const ? unwrap_to_array(columns[0].get()) : base;
    const Column* target_arr_for_meta = target_is_const ? unwrap_to_array(columns[1].get()) : target;

    const Column* base_flat_meta = down_cast<const ArrayColumn*>(base_arr_for_meta)->elements_column().get();
    const uint32_t* base_offset_meta =
            down_cast<const ArrayColumn*>(base_arr_for_meta)->offsets().immutable_data().data();

    const Column* target_flat_meta = down_cast<const ArrayColumn*>(target_arr_for_meta)->elements_column().get();
    const uint32_t* target_offset_meta =
            down_cast<const ArrayColumn*>(target_arr_for_meta)->offsets().immutable_data().data();

    if (base_flat_meta->has_null() || target_flat_meta->has_null()) {
        return Status::InvalidArgument("cosine_similarity does not support null values");
    }
    if (base_flat_meta->is_nullable()) {
        base_flat_meta = down_cast<const NullableColumn*>(base_flat_meta)->data_column().get();
    }
    if (target_flat_meta->is_nullable()) {
        target_flat_meta = down_cast<const NullableColumn*>(target_flat_meta)->data_column().get();
    }

    const Column* base_flat = base_flat_meta;
    const uint32_t* base_offset = base_offset_meta;
    const Column* target_flat = target_flat_meta;
    const uint32_t* target_offset = target_offset_meta;

    using CppType = RunTimeCppType<TYPE>;
    using ColumnType = RunTimeColumnType<TYPE>;

    const CppType* base_data_head = down_cast<const ColumnType*>(base_flat)->immutable_data().data();
    const CppType* target_data_head = down_cast<const ColumnType*>(target_flat)->immutable_data().data();

    // prepare result with nullable value.
    MutableColumnPtr result = ColumnHelper::create_column(TypeDescriptor{TYPE}, false, false, target_size);
    ColumnType* data_result = down_cast<ColumnType*>(result.get());
    CppType* result_data = data_result->get_data().data();

    if constexpr (std::is_same_v<CppType, float>) {
        if (target_size == 0) {
            return result;
        }
        // base is const (size-1), target has N rows.
        // base_data_head points to the single base vector (dim floats, no copy needed).
        if (base_is_const && !target_is_const) {
            uint32_t dim = base_offset_meta[1] - base_offset_meta[0];
            if (!offsets_equal_dim(target_offset, target_size, dim)) {
                return Status::InvalidArgument(fmt::format(
                        "cosine_similarity requires equal length arrays in each row. base array dimension size "
                        "is {}, target array dimension size is {}.",
                        dim, target_offset[1] - target_offset[0]));
            }
            const float* base_vec = reinterpret_cast<const float*>(base_data_head);
            const float* target_data = reinterpret_cast<const float*>(target_data_head);
            vector_cosine_similarity<isNorm>(base_vec, dim, target_data, target_size, result_data);
            return result;
        }
        // target is const (size-1), base has N rows.
        // target_data_head points to the single query vector (dim floats, no copy needed).
        if (!base_is_const && target_is_const) {
            uint32_t dim = target_offset_meta[1] - target_offset_meta[0];
            if (!offsets_equal_dim(base_offset, target_size, dim)) {
                return Status::InvalidArgument(fmt::format(
                        "cosine_similarity requires equal length arrays in each row. base array dimension size "
                        "is {}, target array dimension size is {}.",
                        base_offset[1] - base_offset[0], dim));
            }
            const float* target_vec = reinterpret_cast<const float*>(target_data_head);
            const float* base_data = reinterpret_cast<const float*>(base_data_head);
            vector_cosine_similarity<isNorm>(target_vec, dim, base_data, target_size, result_data);
            return result;
        }
        uint32_t dim = target_offset[1] - target_offset[0];
        if (offsets_equal_dim_two(base_offset, target_offset, target_size, dim)) {
            if (dim == 0) {
                return Status::InvalidArgument("cosine_similarity requires non-empty arrays in each row");
            }
            cosine_similarity_fixed_dim_float<isNorm>(reinterpret_cast<const float*>(base_data_head),
                                                      reinterpret_cast<const float*>(target_data_head), target_size,
                                                      dim, result_data);
            return result;
        }
        if (!offsets_equal_nonzero(base_offset, target_offset, target_size)) {
            return Status::InvalidArgument(
                    "cosine_similarity requires equal length arrays in each row. base array dimension size is "
                    "inconsistent with target array dimension size");
        }
    }

    for (size_t i = 0; i < target_size; i++) {
        size_t t_dim_size = target_offset[i + 1] - target_offset[i];
        size_t b_dim_size = base_offset[i + 1] - base_offset[i];
        if (t_dim_size != b_dim_size) {
            return Status::InvalidArgument(
                    fmt::format("cosine_similarity requires equal length arrays in each row. base array dimension size "
                                "is {}, target array dimension size is {}.",
                                b_dim_size, t_dim_size));
        }
        if (t_dim_size == 0) {
            return Status::InvalidArgument("cosine_similarity requires non-empty arrays in each row");
        }
    }

    const CppType* target_data = target_data_head;
    const CppType* base_data = base_data_head;
    for (size_t i = 0; i < target_size; i++) {
        CppType sum = 0;
        CppType base_sum = 0;
        CppType target_sum = 0;
        size_t dim_size = target_offset[i + 1] - target_offset[i];
        CppType result_value = 0;
        for (size_t j = 0; j < dim_size; j++) {
            sum += base_data[j] * target_data[j];
            if constexpr (!isNorm) {
                base_sum += base_data[j] * base_data[j];
                target_sum += target_data[j] * target_data[j];
            }
        }
        if constexpr (!isNorm) {
            if (base_sum == 0 || target_sum == 0) {
                result_value = 0;
            } else {
                result_value = sum / (std::sqrt(base_sum) * std::sqrt(target_sum));
            }
        } else {
            result_value = sum;
        }
        result_data[i] = result_value;
        target_data += dim_size;
        base_data += dim_size;
    }
    return result;
}

// explicitly instantiate template function.
template StatusOr<ColumnPtr> MathFunctions::cosine_similarity<TYPE_FLOAT, true>(FunctionContext* context,
                                                                                const Columns& columns);
template StatusOr<ColumnPtr> MathFunctions::cosine_similarity<TYPE_FLOAT, false>(FunctionContext* context,
                                                                                 const Columns& columns);

template <LogicalType TYPE>
StatusOr<ColumnPtr> MathFunctions::l2_distance(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);

    const Column* base = columns[0].get();
    const Column* target = columns[1].get();
    size_t target_size = target->size();
    if (base->size() != target_size) {
        return Status::InvalidArgument(fmt::format(
                "l2_distance requires equal length arrays. base array size is {} and target array size is {}.",
                base->size(), target->size()));
    }
    if (base->has_null() || target->has_null()) {
        return Status::InvalidArgument(fmt::format("l2_distance does not support null values. {} array has null value.",
                                                   base->has_null() ? "base" : "target"));
    }
    if (base->is_constant()) {
        auto* const_column = down_cast<const ConstColumn*>(base);
        const_column->data_column()->as_mutable_raw_ptr()->assign(base->size(), 0);
        base = const_column->data_column().get();
    }
    if (target->is_constant()) {
        auto* const_column = down_cast<const ConstColumn*>(target);
        const_column->data_column()->as_mutable_raw_ptr()->assign(target->size(), 0);
        target = const_column->data_column().get();
    }
    if (base->is_nullable()) {
        base = down_cast<const NullableColumn*>(base)->data_column().get();
    }
    if (target->is_nullable()) {
        target = down_cast<const NullableColumn*>(target)->data_column().get();
    }

    // check dimension equality.
    const Column* base_flat = down_cast<const ArrayColumn*>(base)->elements_column().get();
    const uint32_t* base_offset = down_cast<const ArrayColumn*>(base)->offsets().immutable_data().data();
    size_t base_flat_size = base_flat->size();

    const Column* target_flat = down_cast<const ArrayColumn*>(target)->elements_column().get();
    size_t target_flat_size = target_flat->size();
    const uint32_t* target_offset = down_cast<const ArrayColumn*>(target)->offsets().immutable_data().data();

    if (base_flat_size != target_flat_size) {
        return Status::InvalidArgument("l2_distance requires equal length arrays");
    }

    if (base_flat->has_null() || target_flat->has_null()) {
        return Status::InvalidArgument("l2_distance does not support null values");
    }
    if (base_flat->is_nullable()) {
        base_flat = down_cast<const NullableColumn*>(base_flat)->data_column().get();
    }
    if (target_flat->is_nullable()) {
        target_flat = down_cast<const NullableColumn*>(target_flat)->data_column().get();
    }

    using CppType = RunTimeCppType<TYPE>;
    using ColumnType = RunTimeColumnType<TYPE>;

    const CppType* base_data_head = down_cast<const ColumnType*>(base_flat)->immutable_data().data();
    const CppType* target_data_head = down_cast<const ColumnType*>(target_flat)->immutable_data().data();

    // prepare result with nullable value.
    MutableColumnPtr result = ColumnHelper::create_column(TypeDescriptor{TYPE}, false, false, target_size);
    ColumnType* data_result = down_cast<ColumnType*>(result.get());
    CppType* result_data = data_result->get_data().data();

    for (size_t i = 0; i < target_size; i++) {
        size_t t_dim_size = target_offset[i + 1] - target_offset[i];
        size_t b_dim_size = base_offset[i + 1] - base_offset[i];
        if (t_dim_size != b_dim_size) {
            return Status::InvalidArgument(
                    fmt::format("l2_distance requires equal length arrays in each row. base array dimension size "
                                "is {}, target array dimension size is {}.",
                                b_dim_size, t_dim_size));
        }
        if (t_dim_size == 0) {
            return Status::InvalidArgument("l2_distance requires non-empty arrays in each row");
        }
    }

    const CppType* target_data = target_data_head;
    const CppType* base_data = base_data_head;

    for (size_t i = 0; i < target_size; i++) {
        CppType sum = 0;
        size_t dim_size = target_offset[i + 1] - target_offset[i];
        for (size_t j = 0; j < dim_size; j++) {
            CppType distance;
            distance = (base_data[j] - target_data[j]) * (base_data[j] - target_data[j]);
            sum += distance;
        }
        result_data[i] = sum;
        target_data += dim_size;
        base_data += dim_size;
    }

    return result;
}

// explicitly instantiate template function.
template StatusOr<ColumnPtr> MathFunctions::l2_distance<TYPE_FLOAT>(FunctionContext* context, const Columns& columns);

} // namespace starrocks

#include "gen_cpp/opcode/MathFunctions.inc"
