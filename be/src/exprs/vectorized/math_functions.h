// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cmath>

#include "column/column.h"
#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/vectorized/binary_function.h"
#include "exprs/vectorized/decimal_binary_function.h"
#include "exprs/vectorized/function_helper.h"
#include "exprs/vectorized/unary_function.h"
#include "udf/udf.h"
#include "util/string_parser.hpp"

namespace starrocks {

class Expr;

namespace vectorized {

struct RValueCheckZeroImpl;
struct RValueCheckZeroDecimalv2Impl;
struct pmodImpl;
struct pmodFloatImpl;
struct fmodImpl;
struct modImpl;
struct modDecimalv2Impl;
struct negativeImpl;

class MathFunctions {
public:
    /**
     * @param columns: []
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(pi);

    /**
     * @param columns: []
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(e);

    /**
     * @param columns: [DoubleColumn]
     * @return FloatColumn
     */
    DEFINE_VECTORIZED_FN(sign);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_double);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_float);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_largeint);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_bigint);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_int);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_smallint);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_tinyint);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(abs_decimalv2val);

    DEFINE_VECTORIZED_FN(abs_decimal32);
    DEFINE_VECTORIZED_FN(abs_decimal64);
    DEFINE_VECTORIZED_FN(abs_decimal128);
    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(sin);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(asin);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(cos);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(acos);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(tan);

    /**
     * @param columns: [DoubleColumn]
     * @return DoubleColumn
     */
    DEFINE_VECTORIZED_FN(atan);

    template <PrimitiveType TYPE, bool isNorm>
    DEFINE_VECTORIZED_FN(cosine_similarity);

    /**
    * @param columns: [DoubleColumn]
    * @return BigIntColumn
    */
    DEFINE_VECTORIZED_FN(ceil);

    /**
    * @param columns: [DoubleColumn]
    * @return BigIntColumn
    */
    DEFINE_VECTORIZED_FN(floor);

    /**
    * @param columns: [DoubleColumn]
    * @return BigIntColumn
    */
    DEFINE_VECTORIZED_FN(round);

    /**
    * @param columns: [DecimalV3Column<int128_t>]
    * @return BigIntColumn
    */
    DEFINE_VECTORIZED_FN(round_decimal128);

    /**
    * @param: [DoubleColumn, IntColumn]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(round_up_to);

    /**
    * @param: [DoubleColumn, IntColumn]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(round_up_to_decimal128);

    /**
     * @param: [DoubleColumn, IntColumn]
     * @return: DoubleColumn
     */
    DEFINE_VECTORIZED_FN(truncate);

    /**
     * @param: [DecimalV3Column<int128_t>, IntColumn]
     * @return: DecimalV3Column<int128_t>
     */
    DEFINE_VECTORIZED_FN(truncate_decimal128);

    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(ln);

    /**
    * @param: [DoubleColumn base, DoubleColumn value]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(log);
    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    */

    DEFINE_VECTORIZED_FN(log2);
    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    *
    */

    DEFINE_VECTORIZED_FN(log10);
    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(exp);
    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(radians);
    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(degrees);
    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(sqrt);
    /**
    * @param: [DoubleColumn base, DoubleColumn exp]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(pow);
    /**
    * @param: [DoubleColumn y, DoubleColumn x]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(atan2);
    /**
    * @param: [DoubleColumn value]
    * @return: DoubleColumn
    */
    DEFINE_VECTORIZED_FN(cot);
    /**
    * @param: [DoubleColumn]
    * @return: DoubleColumn
    *
    */
    DEFINE_VECTORIZED_FN(square);

    // @todo: these functions belong to math function?
    // =====================================

    // rand's auxiliary method
    static Status rand_prepare(starrocks_udf::FunctionContext* context,
                               starrocks_udf::FunctionContext::FunctionStateScope scope);
    static Status rand_close(starrocks_udf::FunctionContext* context,
                             starrocks_udf::FunctionContext::FunctionStateScope scope);
    /**
     * @param: []
     * @return: DoubleColumn
     * Get the pseudo-random number that normalize to [0,1].
     */
    DEFINE_VECTORIZED_FN(rand);

    /**
     * @param: [Int64Column]
     * @return: DoubleColumn
     * Get the pseudo-random number that normalize to [0,1] with a seed
     */
    DEFINE_VECTORIZED_FN(rand_seed);

    //
    /**
     * @param: [BigIntColumn]
     * @return: StringColumn
     * Binary representation of BIGINT as string.
     */
    DEFINE_VECTORIZED_FN(bin);
    /**
     * @param: [BigIntColumn, TinyInt1Column, TinyInt2Column]
     * @return: StringColumn
     * Get the string based on TinyInt2 from BigInt based on TinyInt1.
     */
    DEFINE_VECTORIZED_FN(conv_int);
    /**
     * @param: [StringColumn, TinyInt1Column, TinyInt2Column]
     * @return: StringColumn
     * Get the string based on TinyInt2 from String based on TinyInt1.
     */
    DEFINE_VECTORIZED_FN(conv_string);
    //    /**
    //     * @param: [BigIntColumn, BigIntColumn]
    //     * @return: BigIntColumn
    //     */

    /**
     * @tparam : TYPE_DOUBLE, TYPE_BIGINT
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <PrimitiveType Type>
    DEFINE_VECTORIZED_FN(pmod) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);

        if constexpr (Type == TYPE_FLOAT || Type == TYPE_DOUBLE) {
            return VectorizedUnstrictBinaryFunction<RValueCheckZeroImpl, pmodFloatImpl>::evaluate<Type>(l, r);
        } else {
            return VectorizedUnstrictBinaryFunction<RValueCheckZeroImpl, pmodImpl>::evaluate<Type>(l, r);
        }
    }

    /**
     * @tparam : TYPE_FLOAT, TYPE_DOUBLE
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <PrimitiveType Type>
    DEFINE_VECTORIZED_FN(fmod) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);

        return VectorizedUnstrictBinaryFunction<RValueCheckZeroImpl, fmodImpl>::evaluate<Type>(l, r);
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
     *           TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE
     * @todo: TYPE_DECIMALV2
     * @param: [TypeColumn, TypeColumn]
     * @return: TypeColumn
     */
    template <PrimitiveType Type>
    DEFINE_VECTORIZED_FN(mod) {
        auto l = VECTORIZED_FN_ARGS(0);
        auto r = VECTORIZED_FN_ARGS(1);

        if constexpr (pt_is_decimalv2<Type>) {
            return VectorizedUnstrictBinaryFunction<RValueCheckZeroDecimalv2Impl, modDecimalv2Impl>::evaluate<Type>(l,
                                                                                                                    r);
        } else if constexpr (pt_is_decimal<Type>) {
            // TODO(by satanson):
            //  FunctionContext carry decimal_overflow_check flag to control overflow checking.
            if (context != nullptr && context->error_if_overflow()) {
                using VectorizedDiv = VectorizedUnstrictDecimalBinaryFunction<Type, ModOp, OverflowMode::REPORT_ERROR>;
                return VectorizedDiv::template evaluate<Type>(l, r);
            } else {
                using VectorizedDiv = VectorizedUnstrictDecimalBinaryFunction<Type, ModOp, OverflowMode::OUTPUT_NULL>;
                return VectorizedDiv::template evaluate<Type>(l, r);
            }
        } else {
            return VectorizedUnstrictBinaryFunction<RValueCheckZeroImpl, modImpl>::evaluate<Type>(l, r);
        }
    }

    /**
     * @tparam : TYPE_BIGINT, TYPE_DOUBLE
     * @todo: TYPE_DECIMALV2
     * @param: [TypeColumn]
     * @return: TypeColumn
     */
    template <PrimitiveType Type>
    DEFINE_VECTORIZED_FN(positive) {
        return VECTORIZED_FN_ARGS(0);
    }

    /**
     * @tparam : TYPE_BIGINT, TYPE_DOUBLE
     * @todo: TYPE_DECIMALV2
     * @param: [TypeColumn]
     * @return: TypeColumn
     */
    template <PrimitiveType Type>
    DEFINE_VECTORIZED_FN(negative) {
        if constexpr (pt_is_decimal<Type>) {
            const auto& type = context->get_return_type();
            return VectorizedStrictUnaryFunction<negativeImpl>::evaluate<Type>(VECTORIZED_FN_ARGS(0), type.precision,
                                                                               type.scale);
        } else {
            return VectorizedStrictUnaryFunction<negativeImpl>::evaluate<Type>(VECTORIZED_FN_ARGS(0));
        }
    }

    /**
    * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT,
    *           TYPE_FLOAT, TYPE_DOUBLE, TYPE_STRING, TYPE_DATETIME, TYPE_DECIMALV2
    * @param: [TypeColumn, ...]
    * @return: TypeColumn
    */
    template <PrimitiveType Type>
    static StatusOr<ColumnPtr> least(FunctionContext* context, const Columns& columns) {
        if (columns.size() == 1) {
            return columns[0];
        }

        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        const auto& type = context->get_return_type();

        std::vector<ColumnViewer<Type>> list;
        list.reserve(columns.size());
        for (const ColumnPtr& col : columns) {
            list.emplace_back(ColumnViewer<Type>(col));
        }

        auto size = columns[0]->size();
        ColumnBuilder<Type> result(size, type.precision, type.scale);
        for (int row = 0; row < size; row++) {
            auto value = list[0].value(row);
            bool is_null = false;
            for (auto& view : list) {
                is_null = is_null || view.is_null(row);
                // TODO (by satanson):
                //  compare two decimal with different precision and scale and prevent casting into
                //  the same precision and scale, because casting can generate overflow values.
                value = value > view.value(row) ? view.value(row) : value;
            }

            result.append(value, is_null);
        }

        return result.build(ColumnHelper::is_all_const(columns));
    }

    /**
     * @tparam : TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT,
     *           TYPE_FLOAT, TYPE_DOUBLE, TYPE_DECIMALV2, TYPE_STRING, TYPE_DATETIME
     * @param: [TypeColumn, ...]
     * @return: TypeColumn
     */
    template <PrimitiveType Type>
    static StatusOr<ColumnPtr> greatest(FunctionContext* context, const Columns& columns) {
        if (columns.size() == 1) {
            return columns[0];
        }

        RETURN_IF_COLUMNS_ONLY_NULL(columns);

        const auto& type = context->get_return_type();

        std::vector<ColumnViewer<Type>> list;
        list.reserve(columns.size());
        for (const ColumnPtr& col : columns) {
            list.emplace_back(ColumnViewer<Type>(col));
        }

        auto size = columns[0]->size();
        ColumnBuilder<Type> result(size, type.precision, type.scale);
        for (int row = 0; row < size; row++) {
            auto value = list[0].value(row);
            bool is_null = false;
            for (auto& view : list) {
                is_null = is_null || view.is_null(row);
                // TODO (by satanson):
                //  compare two decimal with different precision and scale and prevent casting into
                //  the same precision and scale, because casting can generate overflow values.
                value = value < view.value(row) ? view.value(row) : value;
            }

            result.append(value, is_null);
        }

        return result.build(ColumnHelper::is_all_const(columns));
    }

    template <DecimalRoundRule rule>
    static StatusOr<ColumnPtr> decimal_round(FunctionContext* context, const Columns& columns);

    // Specifically, keep_scale means whether to keep the original scale of lv
    // Given an example
    //      col1 - decimal(38,4)      col2 - (int)
    //      1.2345                   3
    //      1.2300                   1
    // The return type is decimal(38, 4), and the result of truncate by col2 are list as follows
    //      without_compensate
    //      1.23 decimal(38,2) <==> 0.1234 decimal(38,4)
    //      1.2  decimal(38,1) <==> 0.0012 decimal(38,4)
    // So we need to compensate
    //      with_compensate = without_compensate * 10^(4 - col)
    //      1.2340          = 0.1234             * 10
    //      1.2000          = 0.0012             * 1000
    template <DecimalRoundRule rule, bool keep_scale>
    static void decimal_round(const int128_t& lv, const int32_t& l_scale, const int32_t& rv, int128_t* res,
                              bool* is_over_flow);
    static double double_round(double value, int64_t dec, bool dec_unsigned, bool truncate);
    static bool decimal_in_base_to_decimal(int64_t src_num, int8_t src_base, int64_t* result);
    static bool handle_parse_result(int8_t dest_base, int64_t* num, StringParser::ParseResult parse_res);
    static std::string decimal_to_base(int64_t src_num, int8_t dest_base);

private:
    static const int32_t MIN_BASE = 2;
    static const int32_t MAX_BASE = 36;
    static const char* _s_alphanumeric_chars;

public:
    constexpr static double EPSILON = 1e-9;
};

DEFINE_BINARY_FUNCTION_WITH_IMPL(RValueCheckZeroImpl, a, b) {
    return b == 0;
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(RValueCheckZeroDecimalv2Impl, a, b) {
    return b == DecimalV2Value::ZERO;
}

// pmod
DEFINE_BINARY_FUNCTION_WITH_IMPL(pmodImpl, a, b) {
    return ((a % (b + (b == 0))) + b) % (b + (b == 0));
}

DEFINE_BINARY_FUNCTION_WITH_IMPL(pmodFloatImpl, a, b) {
    return ::fmod(::fmod(a, (b + (b == 0))) + b, (b + (b == 0)));
}

// fmod
DEFINE_BINARY_FUNCTION(fmodImpl, fmod)

// mod
DEFINE_BINARY_FUNCTION_WITH_IMPL(modImpl, a, b) {
    return (a % (b + (b == 0)));
}

// mod for DecimalV2Value
DEFINE_BINARY_FUNCTION_WITH_IMPL(modDecimalv2Impl, a, b) {
    return a % (b + ((b == DecimalV2Value::ZERO) ? DecimalV2Value::ONE : DecimalV2Value::ZERO));
}

// negative
DEFINE_UNARY_FN_WITH_IMPL(negativeImpl, v) {
    return -v;
}

} // namespace vectorized
} // namespace starrocks
