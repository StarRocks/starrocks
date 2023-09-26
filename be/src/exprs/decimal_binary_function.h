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

#include "column/column_builder.h"
#include "exprs/arithmetic_operation.h"
#include "exprs/binary_function.h"
#include "exprs/overflow.h"
#include "gutil/strings/substitute.h"
#include "types/logical_type.h"

namespace starrocks {
template <OverflowMode overflow_mode, typename Op>
struct DecimalBinaryFunction {
    // Adjust the scale of lhs operand, then evaluate binary operation, the rules about operand
    // scaling is defined in function: compute_result_type.  each operations are depicted as
    // following:
    //
    // 1. AddOp
    // case S(lhs) < S(rhs): then just scale up lhs by S(rhs)-S(lhs);
    // case S(lhs) == S(rhs): no scale needed.
    // case S(lhs) > (rhs): swap two operands because AddOp satisfies commutative law, then adjust
    // left operands.
    //
    // 2. SubOp/ModOp
    // case S(lhs) < S(rhs): then just scale up lhs by S(rhs)-S(lhs);
    // case S(lhs) == S(rhs): no scale needed.
    // case S(lhs) > (rhs): SubOp/ModOp not satisfies commutative law, thus use
    // ReverseSubOp/ReverseModOp instead of SubOp/ModOp;
    // Invariant:  b ReverseSubOp/ReverseModOp a == a SubOp/ModOp b, so now can swap two operands.
    //
    // 3. MulOp: in any cases, S(result) = S(lhs)+ S(rhs), no need to scale lhs operand.
    //
    // 4. DivOp: always scale lhs by max(S(lhs),7) + S(rhs) - S(lhs)
    template <bool lhs_is_const, bool rhs_is_const, bool adjust_left, typename BinaryOperator, typename LhsCppType,
              typename RhsCppType, typename ResultCppType>
    static inline bool adjust_evaluate(size_t num_rows, const LhsCppType* lhs_data, const RhsCppType* rhs_data,
                                       ResultCppType* result_data, NullColumn::ValueType* nulls, bool* has_null,
                                       int adjust_scale) {
        [[maybe_unused]] LhsCppType lhs_datum;
        [[maybe_unused]] RhsCppType rhs_datum;
        [[maybe_unused]] const auto scale_factor = get_scale_factor<LhsCppType>(adjust_scale);
        [[maybe_unused]] auto overflow = false;

        // if lhs is a const column and needs to adjust, adjust lhs outside of loop.
        if constexpr (lhs_is_const) {
            lhs_datum = lhs_data[0];
            // adjust left operand
            if constexpr (adjust_left) {
                overflow = DecimalV3Cast::scale_up<LhsCppType, LhsCppType, check_overflow<overflow_mode>>(
                        lhs_datum, scale_factor, &lhs_datum);
                // adjusting operand generates decimal overflow
                if constexpr (check_overflow<overflow_mode>) {
                    if (overflow) {
                        if constexpr (error_if_overflow<overflow_mode>) {
                            throw std::overflow_error(strings::Substitute(
                                    "The '$0' operation involving decimal values overflows", get_op_name<Op>()));
                        } else {
                            return true;
                        }
                    }
                }
            }
        }

        if constexpr (rhs_is_const) {
            rhs_datum = rhs_data[0];
        }

        for (auto i = 0; i < num_rows; ++i) {
            if constexpr (lhs_is_const && rhs_is_const) {
                overflow = BinaryOperator::template apply<check_overflow<overflow_mode>, false, LhsCppType, RhsCppType,
                                                          ResultCppType>(lhs_datum, rhs_datum, &result_data[i],
                                                                         scale_factor);
            } else if constexpr (lhs_is_const) {
                overflow = BinaryOperator::template apply<check_overflow<overflow_mode>, false, LhsCppType, RhsCppType,
                                                          ResultCppType>(lhs_datum, rhs_data[i], &result_data[i],
                                                                         scale_factor);
            } else if constexpr (rhs_is_const) {
                overflow = BinaryOperator::template apply<check_overflow<overflow_mode>, adjust_left, LhsCppType,
                                                          RhsCppType, ResultCppType>(lhs_data[i], rhs_datum,
                                                                                     &result_data[i], scale_factor);
            } else {
                overflow = BinaryOperator::template apply<check_overflow<overflow_mode>, adjust_left, LhsCppType,
                                                          RhsCppType, ResultCppType>(lhs_data[i], rhs_data[i],
                                                                                     &result_data[i], scale_factor);
            }
            if constexpr (check_overflow<overflow_mode>) {
                if (overflow) {
                    if constexpr (error_if_overflow<overflow_mode>) {
                        throw std::overflow_error(strings::Substitute(
                                "The '$0' operation involving decimal values overflows", get_op_name<Op>()));
                    } else {
                        static_assert(null_if_overflow<overflow_mode>);
                        *has_null = true;
                        nulls[i] = DATUM_NULL;
                    }
                }
            }
        }
        return false;
    }

    template <bool lhs_is_const, bool rhs_is_const, LogicalType LhsType, LogicalType RhsType, LogicalType ResultType>
    static inline ColumnPtr evaluate(const ColumnPtr& lhs, const ColumnPtr& rhs) {
        using ResultCppType = RunTimeCppType<ResultType>;
        using ResultColumnType = RunTimeColumnType<ResultType>;

        auto lhs_column = ColumnHelper::cast_to_raw<LhsType>(lhs);
        auto rhs_column = ColumnHelper::cast_to_raw<RhsType>(rhs);
        auto lhs_data = &lhs_column->get_data().front();
        auto rhs_data = &rhs_column->get_data().front();

        const auto num_rows = std::max(lhs->size(), rhs->size());
        const auto lhs_scale = lhs_column->scale();
        const auto rhs_scale = rhs_column->scale();
        auto [precision, scale, adjust_scale] = compute_decimal_result_type<ResultCppType, Op>(lhs_scale, rhs_scale);

        auto result_column = ResultColumnType::create(precision, scale, num_rows);
        auto result_data = &ColumnHelper::cast_to_raw<ResultType>(result_column)->get_data().front();
        NullColumnPtr null_column;
        NullColumn::ValueType* nulls = nullptr;
        bool has_null = false;
        [[maybe_unused]] bool all_null = false;

        if constexpr (check_overflow<overflow_mode>) {
            null_column = NullColumn::create();
            null_column->resize(num_rows);
            nulls = &null_column->get_data().front();
        }

        using BinaryOperator = ArithmeticBinaryOperator<Op, ResultType>;

        if constexpr (is_add_op<Op> || is_sub_op<Op> || is_mod_op<Op>) {
            // add/sub operation
            if (adjust_scale == 0) {
                // S(lhs)==S(rhs) no need to adjust
                all_null = adjust_evaluate<lhs_is_const, rhs_is_const, false, BinaryOperator>(
                        num_rows, lhs_data, rhs_data, result_data, nulls, &has_null, adjust_scale);
            } else if (lhs_scale < rhs_scale) {
                // S(lhs) < S(rhs), scale lhs up by S(rhs)-S(lhs)
                all_null = adjust_evaluate<lhs_is_const, rhs_is_const, true, BinaryOperator>(
                        num_rows, lhs_data, rhs_data, result_data, nulls, &has_null, adjust_scale);
            } else {
                // S(lhs) > S(rhs), scale rhs up by S(lhs)-S(rhs)
                if constexpr (is_add_op<Op>) {
                    // add: just swap two operands
                    all_null = adjust_evaluate<rhs_is_const, lhs_is_const, true, BinaryOperator>(
                            num_rows, rhs_data, lhs_data, result_data, nulls, &has_null, adjust_scale);
                } else if constexpr (is_sub_op<Op>) {
                    // sub: just swap two operand and then rhs - lhs
                    using ReverseSubOperator = ArithmeticBinaryOperator<ReverseSubOp, ResultType>;
                    all_null = adjust_evaluate<rhs_is_const, lhs_is_const, true, ReverseSubOperator>(
                            num_rows, rhs_data, lhs_data, result_data, nulls, &has_null, adjust_scale);
                } else if constexpr (is_mod_op<Op>) {
                    // mod: just swap two operand and then rhs - lhs
                    using ReverseModOperator = ArithmeticBinaryOperator<ReverseModOp, ResultType>;
                    all_null = adjust_evaluate<rhs_is_const, lhs_is_const, true, ReverseModOperator>(
                            num_rows, rhs_data, lhs_data, result_data, nulls, &has_null, adjust_scale);
                } else {
                    static_assert(is_add_op<Op> || is_sub_op<Op>, "Invalid Op");
                }
            }
        } else if constexpr (is_mul_op<Op>) {
            // mul operation, no need to adjust scale
            all_null = adjust_evaluate<lhs_is_const, rhs_is_const, false, BinaryOperator>(
                    num_rows, lhs_data, rhs_data, result_data, nulls, &has_null, adjust_scale);
        } else if constexpr (is_div_op<Op>) {
            // div operation, scale lhs up by S(rhs)
            if (adjust_scale == 0) {
                all_null = adjust_evaluate<lhs_is_const, rhs_is_const, false, BinaryOperator>(
                        num_rows, lhs_data, rhs_data, result_data, nulls, &has_null, adjust_scale);
            } else {
                all_null = adjust_evaluate<lhs_is_const, rhs_is_const, true, BinaryOperator>(
                        num_rows, lhs_data, rhs_data, result_data, nulls, &has_null, adjust_scale);
            }
        } else if constexpr (is_decimal_fast_mul_op<Op>) {
            all_null = adjust_evaluate<lhs_is_const, rhs_is_const, false, BinaryOperator>(
                    num_rows, lhs_data, rhs_data, result_data, nulls, &has_null, adjust_scale);
        }

        if constexpr (check_overflow<overflow_mode>) {
            if (all_null) {
                // all the elements of the result are overflow, return const null column
                return ColumnHelper::create_const_null_column(num_rows);
            }
            ColumnBuilder<ResultType> builder(result_column, null_column, has_null);
            return builder.build(lhs_is_const && rhs_is_const);
        } else if constexpr (lhs_is_const && rhs_is_const) {
            return ConstColumn::create(result_column, 1);
        } else {
            return result_column;
        }
    }
    template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType>
    static inline ColumnPtr const_const(const ColumnPtr& lhs, const ColumnPtr& rhs) {
        return evaluate<true, true, LhsType, RhsType, ResultType>(lhs, rhs);
    }
    template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType>
    static inline ColumnPtr const_vector(const ColumnPtr& lhs, const ColumnPtr& rhs) {
        return evaluate<true, false, LhsType, RhsType, ResultType>(lhs, rhs);
    }
    template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType>
    static inline ColumnPtr vector_const(const ColumnPtr& lhs, const ColumnPtr& rhs) {
        return evaluate<false, true, LhsType, RhsType, ResultType>(lhs, rhs);
    }
    template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType>
    static inline ColumnPtr vector_vector(const ColumnPtr& lhs, const ColumnPtr& rhs) {
        return evaluate<false, false, LhsType, RhsType, ResultType>(lhs, rhs);
    }
};

template <typename OP, OverflowMode overflow_mode>
class UnpackConstColumnDecimalBinaryFunction {
public:
    template <LogicalType LType, LogicalType RType, LogicalType ResultType>
    static ColumnPtr evaluate(const ColumnPtr& v1, const ColumnPtr& v2) {
        using Function = DecimalBinaryFunction<overflow_mode, OP>;
        if (!v1->is_constant() && !v2->is_constant()) {
            return Function::template vector_vector<LType, RType, ResultType>(v1, v2);
        } else if (!v1->is_constant() && v2->is_constant()) {
            const ColumnPtr& data2 = ColumnHelper::as_raw_column<ConstColumn>(v2)->data_column();
            return Function::template vector_const<LType, RType, ResultType>(v1, data2);
        } else if (v1->is_constant() && !v2->is_constant()) {
            const ColumnPtr& data1 = ColumnHelper::as_raw_column<ConstColumn>(v1)->data_column();
            return Function::template const_vector<LType, RType, ResultType>(data1, v2);
        } else {
            const ColumnPtr& data1 = ColumnHelper::as_raw_column<ConstColumn>(v1)->data_column();
            const ColumnPtr& data2 = ColumnHelper::as_raw_column<ConstColumn>(v2)->data_column();
            return Function::template const_const<LType, RType, ResultType>(data1, data2);
        }
    }
};

template <typename OP, OverflowMode overflow_mode = OverflowMode::IGNORE>
using VectorizedStrictDecimalBinaryFunction =
        UnionNullableColumnBinaryFunction<UnpackConstColumnDecimalBinaryFunction<OP, overflow_mode>>;

template <LogicalType Type, typename OP, OverflowMode overflow_mode = OverflowMode::IGNORE>
using VectorizedUnstrictDecimalBinaryFunction =
        ProduceNullableColumnBinaryFunction<UnpackConstColumnBinaryFunction<ArithmeticRightZeroCheck<Type>>,
                                            UnpackConstColumnDecimalBinaryFunction<OP, overflow_mode>>;

} //namespace starrocks
