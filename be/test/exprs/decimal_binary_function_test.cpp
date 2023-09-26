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

#include <column/column_helper.h>
#include <column/decimalv3_column.h>
#include <column/type_traits.h>
#include <exprs/decimal_binary_function.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "exprs/decimal_cast_expr_test_helper.h"
#include "exprs/overflow.h"

namespace starrocks {

class DecimalBinaryFunctionTest : public ::testing::Test {};
using DecimalTestCase = std::tuple<std::string, std::string, std::string>;
using DecimalTestCaseArray = std::vector<DecimalTestCase>;

template <LogicalType LhsType, LogicalType RhsType>
Columns prepare_vector_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                              int rhs_precision, int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    using LhsCppType = RunTimeCppType<LhsType>;
    using LhsColumnType = RunTimeColumnType<LhsType>;

    using RhsCppType = RunTimeCppType<RhsType>;
    using RhsColumnType = RunTimeColumnType<RhsType>;

    Columns columns;
    columns.reserve(2);
    auto lhs_column = LhsColumnType::create(lhs_precision, lhs_scale);
    auto rhs_column = RhsColumnType::create(rhs_precision, rhs_scale);
    auto num_rows = test_cases.size() + front_fill_size + rear_fill_size;
    lhs_column->resize(num_rows);
    rhs_column->resize(num_rows);

    auto lhs_data = &ColumnHelper::cast_to_raw<LhsType>(lhs_column)->get_data().front();
    auto rhs_data = &ColumnHelper::cast_to_raw<RhsType>(rhs_column)->get_data().front();

    // front filling
    for (auto i = 0; i < front_fill_size; ++i) {
        lhs_data[i] = LhsCppType(0);
        rhs_data[i] = RhsCppType(0);
    }

    for (auto i = front_fill_size; i < front_fill_size + test_cases.size(); ++i) {
        auto& tc = test_cases[i - front_fill_size];
        auto& lhs_datum = std::get<0>(tc);
        auto& rhs_datum = std::get<1>(tc);
        DecimalV3Cast::from_string<LhsCppType>(&lhs_data[i], lhs_precision, lhs_scale, lhs_datum.c_str(),
                                               lhs_datum.size());
        DecimalV3Cast::from_string<RhsCppType>(&rhs_data[i], rhs_precision, rhs_scale, rhs_datum.c_str(),
                                               rhs_datum.size());
    }
    // rear filling
    for (auto i = front_fill_size + test_cases.size(); i < num_rows; ++i) {
        lhs_data[i] = LhsCppType(0);
        rhs_data[i] = RhsCppType(0);
    }
    // std::cout << "lhs_column=" << lhs_column->debug_string() << std::endl;
    // std::cout << "rhs_column=" << rhs_column->debug_string() << std::endl;
    columns.push_back(lhs_column);
    columns.push_back(rhs_column);
    return columns;
}

template <LogicalType Type>
Columns prepare_vector_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                              int rhs_precision, int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    return prepare_vector_vector<Type, Type>(test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                             front_fill_size, rear_fill_size);
}

template <LogicalType LhsType, LogicalType RhsType>
Columns prepare_const_vector(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale, int rhs_precision,
                             int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    auto& lhs_datum = std::get<0>(test_case);
    auto& rhs_datum = std::get<1>(test_case);
    using LhsCppType = RunTimeCppType<LhsType>;
    using LhsColumnType = RunTimeColumnType<LhsType>;
    using RhsCppType = RunTimeCppType<RhsType>;
    using RhsColumnType = RunTimeColumnType<RhsType>;

    Columns columns;
    columns.reserve(2);

    auto lhs_column = LhsColumnType::create(lhs_precision, lhs_scale);
    lhs_column->resize(1);

    auto rhs_column = RhsColumnType::create(rhs_precision, rhs_scale);
    auto num_rows = 1 + front_fill_size + rear_fill_size;
    lhs_column->resize(num_rows);
    rhs_column->resize(num_rows);

    auto lhs_data = &ColumnHelper::cast_to_raw<LhsType>(lhs_column)->get_data().front();
    auto rhs_data = &ColumnHelper::cast_to_raw<RhsType>(rhs_column)->get_data().front();
    DecimalV3Cast::from_string<LhsCppType>(&lhs_data[0], lhs_precision, lhs_scale, lhs_datum.c_str(), lhs_datum.size());

    // front filling
    for (auto i = 0; i < front_fill_size; ++i) {
        rhs_data[i] = RhsCppType(0);
    }

    DecimalV3Cast::from_string<RhsCppType>(&rhs_data[front_fill_size], rhs_precision, rhs_scale, rhs_datum.c_str(),
                                           rhs_datum.size());
    // rear filling
    for (auto i = front_fill_size + 1; i < num_rows; ++i) {
        rhs_data[i] = RhsCppType(0);
    }
    lhs_column->resize(1);
    columns.push_back(ConstColumn::create(lhs_column, num_rows));
    columns.push_back(rhs_column);
    return columns;
}

template <LogicalType Type>
Columns prepare_const_vector(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale, int rhs_precision,
                             int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    return prepare_const_vector<Type, Type>(test_case, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                            front_fill_size, rear_fill_size);
}

template <LogicalType LhsType, LogicalType RhsType>
Columns prepare_vector_const(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale, int rhs_precision,
                             int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    DecimalTestCase const_vector_test_case = {std::get<1>(test_case), std::get<0>(test_case), std::get<2>(test_case)};
    auto const_vector_columns =
            prepare_const_vector<RhsType, LhsType>(const_vector_test_case, rhs_precision, rhs_scale, lhs_precision,
                                                   lhs_scale, front_fill_size, rear_fill_size);
    return Columns{const_vector_columns[1], const_vector_columns[0]};
}

template <LogicalType Type>
Columns prepare_vector_const(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale, int rhs_precision,
                             int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    return prepare_vector_const<Type, Type>(test_case, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                            front_fill_size, rear_fill_size);
}

ColumnPtr add_null_column(ColumnPtr&& column) {
    auto null_column = NullColumn::create(column->size());
    auto* nulls = &null_column->get_data().front();
    for (int i = 0; i < column->size(); ++i) {
        nulls[i] = i % 2 == 0;
    }
    return NullableColumn::create(std::move(column), std::move(null_column));
}

template <LogicalType LhsType, LogicalType RhsType>
Columns prepare_nullable_vector_const(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale,
                                      int rhs_precision, int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    auto columns = prepare_vector_const<LhsType, RhsType>(test_case, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                                          front_fill_size, rear_fill_size);
    return Columns{add_null_column(std::move(columns[0])), columns[1]};
}

template <LogicalType Type>
Columns prepare_nullable_vector_const(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale,
                                      int rhs_precision, int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    return prepare_nullable_vector_const<Type, Type>(test_case, lhs_precision, rhs_scale, rhs_precision, rhs_scale,
                                                     front_fill_size, rear_fill_size);
}

template <LogicalType LhsType, LogicalType RhsType>
Columns prepare_const_nullable_vector(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale,
                                      int rhs_precision, int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    auto columns = prepare_const_vector<LhsType, RhsType>(test_case, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                                          front_fill_size, rear_fill_size);
    return Columns{columns[0], add_null_column(std::move(columns[1]))};
}

template <LogicalType Type>
Columns prepare_const_nullable_vector(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale,
                                      int rhs_precision, int rhs_scale, size_t front_fill_size, size_t rear_fill_size) {
    return prepare_const_nullable_vector<Type, Type>(test_case, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                                     front_fill_size, rear_fill_size);
}

template <LogicalType LhsType, LogicalType RhsType>
Columns prepare_nullable_vector_nullable_vector(const DecimalTestCaseArray& test_case, int lhs_precision, int lhs_scale,
                                                int rhs_precision, int rhs_scale, size_t front_fill_size,
                                                size_t rear_fill_size) {
    auto columns = prepare_vector_vector<LhsType, RhsType>(test_case, lhs_precision, lhs_scale, rhs_precision,
                                                           rhs_scale, front_fill_size, rear_fill_size);
    return Columns{add_null_column(std::move(columns[0])), add_null_column(std::move(columns[1]))};
}

template <LogicalType Type>
Columns prepare_nullable_vector_nullable_vector(const DecimalTestCaseArray& test_case, int lhs_precision, int lhs_scale,
                                                int rhs_precision, int rhs_scale, size_t front_fill_size,
                                                size_t rear_fill_size) {
    return prepare_nullable_vector_nullable_vector<Type, Type>(test_case, lhs_precision, lhs_scale, rhs_precision,
                                                               rhs_scale, front_fill_size, rear_fill_size);
}

template <LogicalType LhsType, LogicalType RhsType>
Columns prepare_const_const(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale, int rhs_precision,
                            int rhs_scale) {
    using LhsCppType = RunTimeCppType<LhsType>;
    using LhsColumnType = RunTimeColumnType<LhsType>;
    using RhsCppType = RunTimeCppType<RhsType>;
    using RhsColumnType = RunTimeColumnType<RhsType>;

    auto lhs_datum = std::get<0>(test_case);
    auto rhs_datum = std::get<1>(test_case);

    auto lhs_column = LhsColumnType::create(lhs_precision, lhs_scale);
    auto rhs_column = RhsColumnType::create(rhs_precision, rhs_scale);
    lhs_column->resize(1);
    rhs_column->resize(1);
    auto lhs_data = &ColumnHelper::cast_to_raw<LhsType>(lhs_column)->get_data().front();
    auto rhs_data = &ColumnHelper::cast_to_raw<RhsType>(rhs_column)->get_data().front();
    DecimalV3Cast::from_string<LhsCppType>(&lhs_data[0], lhs_precision, lhs_scale, lhs_datum.c_str(), lhs_datum.size());
    DecimalV3Cast::from_string<RhsCppType>(&rhs_data[0], rhs_precision, rhs_scale, rhs_datum.c_str(), rhs_datum.size());
    return Columns{ConstColumn::create(lhs_column, 1), ConstColumn::create(rhs_column, 1)};
}

template <LogicalType Type>
Columns prepare_const_const(const DecimalTestCase& test_case, int lhs_precision, int lhs_scale, int rhs_precision,
                            int rhs_scale) {
    return prepare_const_const<Type, Type>(test_case, lhs_precision, lhs_scale, rhs_precision, rhs_scale);
}

using Func = std::function<ColumnPtr(ColumnPtr const&, ColumnPtr const&)>;

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode,
          bool assert_overflow = false>
void test_decimal_binary_functions(DecimalTestCaseArray const& test_cases, Columns columns, int result_precision,
                                   int result_scale, size_t off, [[maybe_unused]] const std::vector<bool>& overflows) {
    using ColumnWiseOp = UnpackConstColumnDecimalBinaryFunction<Op, overflow_mode>;
    using CppType = RunTimeCppType<ResultType>;
    using ColumnType = RunTimeColumnType<ResultType>;
    ASSERT_TRUE(columns.size() == 2);
    auto result = ColumnWiseOp::template evaluate<LhsType, RhsType, ResultType>(columns[0], columns[1]);

    ASSERT_TRUE(result.get() != nullptr);
    // lhs is const and rhs is const -> result is const
    ASSERT_TRUE(!(columns[0]->is_constant() && columns[1]->is_constant()) || result->is_constant());
    if (result->is_constant()) {
        ASSERT_TRUE(test_cases.size() == 1);
        if (result->only_null()) {
            ASSERT_TRUE(null_if_overflow<overflow_mode>);
        } else {
            auto* decimal_column = (ColumnType*)(ColumnHelper::get_data_column(result.get()));
            ASSERT_EQ(result_precision, decimal_column->precision());
            ASSERT_EQ(result_scale, decimal_column->scale());
            CppType value = decimal_column->get_data()[0];
            auto actual =
                    DecimalV3Cast::to_string<CppType>(value, decimal_column->precision(), decimal_column->scale());
            auto& tc = test_cases[0];
            auto& rhs_datum = std::get<1>(tc);
            auto& expect = std::get<2>(tc);
            //std::cout << "test#" << 0 << ": lhs=" << std::get<0>(tc) << ", rhs=" << std::get<1>(tc)
            //          << ", expect=" << expect << ", actual=" << actual << std::endl;

            if constexpr (is_div_op<Op> || is_mod_op<Op>) {
                if (rhs_datum != "0") {
                    compare_decimal_string(expect, actual, result_scale);
                }
            } else {
                compare_decimal_string(expect, actual, result_scale);
            }
        }
        return;
    }

    if constexpr (!check_overflow<overflow_mode>) {
        ASSERT_TRUE(!columns[0]->is_constant() || !columns[1]->is_constant());
    }

    ColumnType* decimal_column;
    if (result->is_nullable()) {
        auto nullable_column = down_cast<NullableColumn*>(result.get());
        decimal_column = (ColumnType*)ColumnHelper::get_data_column(nullable_column);
    } else {
        decimal_column = ColumnHelper::cast_to_raw<ResultType>(result);
    }
    ASSERT_EQ(result_precision, decimal_column->precision());
    ASSERT_EQ(result_scale, decimal_column->scale());
    CppType* data = &decimal_column->get_data().front();

    for (auto i = 0; i < test_cases.size(); i++) {
        auto& tc = test_cases[i];
        // auto& lhs_datum = std::get<0>(tc);
        auto& rhs_datum = std::get<1>(tc);
        auto& expect = std::get<2>(tc);
        auto row_idx = i + off;

        CppType& value = data[row_idx];
        auto actual = DecimalV3Cast::to_string<CppType>(value, decimal_column->precision(), decimal_column->scale());
        //std::cout << "test#" << i << ": lhs=" << lhs_datum << ", rhs=" << rhs_datum << ", expect=" << expect
        //          << ", actual=" << actual << std::endl;
        if constexpr (check_overflow<overflow_mode>) {
            if constexpr (assert_overflow) {
                const auto& expect_overflow = overflows[i];
                ASSERT_EQ(expect_overflow, result->is_null(row_idx));
            }
            if (result->is_nullable() && result->is_null(row_idx)) {
                continue;
            }
        }

        if constexpr (is_div_op<Op> || is_mod_op<Op>) {
            if (rhs_datum != "0") {
                compare_decimal_string(expect, actual, result_scale);
            }
        } else {
            compare_decimal_string(expect, actual, result_scale);
        }
    }
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode,
          bool assert_overflow = false>
void test_decimal_binary_functions_with_nullable_columns(DecimalTestCaseArray const& test_cases, Columns columns,
                                                         int result_precision, int result_scale, size_t off,
                                                         [[maybe_unused]] const std::vector<bool>& overflows) {
    using CppType = RunTimeCppType<ResultType>;
    using ColumnType = RunTimeColumnType<ResultType>;

    ASSERT_TRUE(columns.size() == 2);
    ColumnPtr result = nullptr;
    if constexpr (is_add_op<Op> || is_sub_op<Op> || is_mul_op<Op>) {
        using ColumnWiseOp = VectorizedStrictDecimalBinaryFunction<Op, overflow_mode>;
        result = ColumnWiseOp::template evaluate<LhsType, RhsType, ResultType>(columns[0], columns[1]);
    } else {
        using ColumnWiseOp = VectorizedUnstrictDecimalBinaryFunction<ResultType, Op, overflow_mode>;
        result = ColumnWiseOp::template evaluate<LhsType, RhsType, ResultType>(columns[0], columns[1]);
    }

    ASSERT_TRUE(result.get() != nullptr);
    ColumnType* decimal_column;
    if (result->is_nullable()) {
        auto nullable_column = down_cast<NullableColumn*>(result.get());
        decimal_column = (ColumnType*)ColumnHelper::get_data_column(nullable_column);
    } else {
        decimal_column = ColumnHelper::cast_to_raw<ResultType>(result);
    }
    ASSERT_EQ(result_precision, decimal_column->precision());
    ASSERT_EQ(result_scale, decimal_column->scale());
    CppType* data = &decimal_column->get_data().front();

    for (auto i = 0; i < test_cases.size(); i++) {
        auto& tc = test_cases[i];
        // auto& lhs_datum = std::get<0>(tc);
        auto& rhs_datum = std::get<1>(tc);
        auto& expect = std::get<2>(tc);
        auto row_idx = i + off;

        CppType& value = data[row_idx];
        auto actual = DecimalV3Cast::to_string<CppType>(value, decimal_column->precision(), decimal_column->scale());
        if constexpr (check_overflow<overflow_mode>) {
            if constexpr (assert_overflow) {
                const auto& expect_overflow = overflows[i];
                ASSERT_EQ(expect_overflow, result->is_null(row_idx));
            }
            if (result->is_nullable() && result->is_null(row_idx)) {
                continue;
            }
        }

        if constexpr (is_div_op<Op> || is_mod_op<Op>) {
            if (rhs_datum != "0") {
                compare_decimal_string(expect, actual, result_scale);
            }
        } else {
            compare_decimal_string(expect, actual, result_scale);
        }
    }
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_vector_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                        int rhs_scale, int result_precision, int result_scale) {
    Columns columns = prepare_vector_vector<LhsType, RhsType>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                              rhs_scale, 0, 0);
    test_decimal_binary_functions<LhsType, RhsType, ResultType, Op, overflow_mode>(
            test_cases, columns, result_precision, result_scale, 0, std::vector<bool>());
}

template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_vector_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                        int rhs_scale, int result_precision, int result_scale) {
    test_vector_vector<Type, Type, Type, Op, overflow_mode>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                            rhs_scale, result_precision, result_scale);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_vector_vector_assert_overflow(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                        int rhs_precision, int rhs_scale, int result_precision, int result_scale,
                                        const std::vector<bool>& overflows) {
    Columns columns = prepare_vector_vector<LhsType, RhsType>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                              rhs_scale, 0, 0);
    test_decimal_binary_functions<LhsType, RhsType, ResultType, Op, overflow_mode, true>(
            test_cases, columns, result_precision, result_scale, 0, overflows);
}

template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_vector_vector_assert_overflow(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                        int rhs_precision, int rhs_scale, int result_precision, int result_scale,
                                        const std::vector<bool>& overflows) {
    test_vector_vector_assert_overflow<Type, Type, Type, Op, overflow_mode>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale, overflows);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_const_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                       int rhs_scale, int result_precision, int result_scale) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> rand_int(1, 50);
    int front_fill_size = rand_int(gen);
    int rear_fill_size = rand_int(gen);
    for (auto& tc : test_cases) {
        Columns columns = prepare_const_vector<LhsType, RhsType>(tc, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                                                 front_fill_size, rear_fill_size);
        test_decimal_binary_functions<LhsType, RhsType, ResultType, Op, overflow_mode>(
                DecimalTestCaseArray{tc}, columns, result_precision, result_scale, front_fill_size,
                std::vector<bool>());
    }
}

template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_const_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                       int rhs_scale, int result_precision, int result_scale) {
    test_const_vector<Type, Type, Type, Op, overflow_mode>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                           rhs_scale, result_precision, result_scale);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_vector_const(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                       int rhs_scale, int result_precision, int result_scale) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> rand_int(1, 50);
    int front_fill_size = rand_int(gen);
    int rear_fill_size = rand_int(gen);
    for (auto& tc : test_cases) {
        Columns columns = prepare_vector_const<LhsType, RhsType>(tc, lhs_precision, lhs_scale, rhs_precision, rhs_scale,
                                                                 front_fill_size, rear_fill_size);
        test_decimal_binary_functions<LhsType, RhsType, ResultType, Op, overflow_mode>(
                DecimalTestCaseArray{tc}, columns, result_precision, result_scale, front_fill_size,
                std::vector<bool>());
    }
}

template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_vector_const(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                       int rhs_scale, int result_precision, int result_scale) {
    return test_vector_const<Type, Type, Type, Op, overflow_mode>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                                  rhs_scale, result_precision, result_scale);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_const_const(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                      int rhs_scale, int result_precision, int result_scale) {
    for (auto& tc : test_cases) {
        Columns columns = prepare_const_const<LhsType, RhsType>(tc, lhs_precision, lhs_scale, rhs_precision, rhs_scale);
        test_decimal_binary_functions<LhsType, RhsType, ResultType, Op, overflow_mode>(
                DecimalTestCaseArray{tc}, columns, result_precision, result_scale, 0, std::vector<bool>());
    }
}

template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_const_const(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale, int rhs_precision,
                      int rhs_scale, int result_precision, int result_scale) {
    test_const_const<Type, Type, Type, Op, overflow_mode>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                          rhs_scale, result_precision, result_scale);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_nullable_vector_const(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                int rhs_precision, int rhs_scale, int result_precision, int result_scale) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> rand_int(1, 50);
    int front_fill_size = rand_int(gen);
    int rear_fill_size = rand_int(gen);
    for (auto& tc : test_cases) {
        Columns columns = prepare_nullable_vector_const<LhsType, RhsType>(tc, lhs_precision, lhs_scale, rhs_precision,
                                                                          rhs_scale, front_fill_size, rear_fill_size);
        test_decimal_binary_functions_with_nullable_columns<LhsType, RhsType, ResultType, Op, overflow_mode>(
                DecimalTestCaseArray{tc}, columns, result_precision, result_scale, front_fill_size,
                std::vector<bool>());
    }
}
template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_nullable_vector_const(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                int rhs_precision, int rhs_scale, int result_precision, int result_scale) {
    test_nullable_vector_const<Type, Type, Type, Op, overflow_mode>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                                    rhs_scale, result_precision, result_scale);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_const_nullable_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                int rhs_precision, int rhs_scale, int result_precision, int result_scale) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> rand_int(1, 50);
    int front_fill_size = rand_int(gen);
    int rear_fill_size = rand_int(gen);
    for (auto& tc : test_cases) {
        Columns columns = prepare_const_nullable_vector<LhsType, RhsType>(tc, lhs_precision, lhs_scale, rhs_precision,
                                                                          rhs_scale, front_fill_size, rear_fill_size);
        test_decimal_binary_functions_with_nullable_columns<LhsType, RhsType, ResultType, Op, overflow_mode>(
                DecimalTestCaseArray{tc}, columns, result_precision, result_scale, front_fill_size,
                std::vector<bool>());
    }
}
template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_const_nullable_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                int rhs_precision, int rhs_scale, int result_precision, int result_scale) {
    test_const_nullable_vector<Type, Type, Type, Op, overflow_mode>(test_cases, lhs_precision, lhs_scale, rhs_precision,
                                                                    rhs_scale, result_precision, result_scale);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op, OverflowMode overflow_mode>
void test_nullable_vector_nullable_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                          int rhs_precision, int rhs_scale, int result_precision, int result_scale) {
    Columns columns = prepare_nullable_vector_nullable_vector<LhsType, RhsType>(test_cases, lhs_precision, lhs_scale,
                                                                                rhs_precision, rhs_scale, 0, 0);
    test_decimal_binary_functions_with_nullable_columns<LhsType, RhsType, ResultType, Op, overflow_mode>(
            test_cases, columns, result_precision, result_scale, 0, std::vector<bool>());
}

template <LogicalType Type, typename Op, OverflowMode overflow_mode>
void test_nullable_vector_nullable_vector(DecimalTestCaseArray const& test_cases, int lhs_precision, int lhs_scale,
                                          int rhs_precision, int rhs_scale, int result_precision, int result_scale) {
    return test_nullable_vector_nullable_vector<Type, Type, Type, Op, overflow_mode>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op>
void test_overflow_report_error(const std::string& lv, const std::string& rv, int lhs_precision, int lhs_scale,
                                int rhs_precision, int rhs_scale) {
    using LhsCppType = RunTimeCppType<LhsType>;
    using LhsColumnType = RunTimeColumnType<LhsType>;

    using RhsCppType = RunTimeCppType<RhsType>;
    using RhsColumnType = RunTimeColumnType<RhsType>;

    auto lhs_column = LhsColumnType::create(lhs_precision, lhs_scale);
    auto rhs_column = RhsColumnType::create(rhs_precision, rhs_scale);
    lhs_column->resize(1);
    rhs_column->resize(1);
    auto lhs_data = &ColumnHelper::cast_to_raw<LhsType>(lhs_column)->get_data().front();
    auto rhs_data = &ColumnHelper::cast_to_raw<RhsType>(rhs_column)->get_data().front();

    DecimalV3Cast::from_string<LhsCppType>(&lhs_data[0], lhs_precision, lhs_scale, lv.c_str(), lv.size());
    DecimalV3Cast::from_string<RhsCppType>(&rhs_data[0], rhs_precision, rhs_scale, rv.c_str(), rv.size());

    using ColumnWiseOp = VectorizedStrictDecimalBinaryFunction<Op, OverflowMode::REPORT_ERROR>;
    ColumnWiseOp::template evaluate<LhsType, RhsType, ResultType>(lhs_column, rhs_column);
}

TEST_F(DecimalBinaryFunctionTest, test_decimal128p30s20_add_decimal128p38s28_eq_decimal128p38s28) {
    DecimalTestCaseArray test_cases = {
            {"9999999999.99999999999999999999", "9999999999.9999999999999999999999999999",
             "-14028236692.0938463463374607431868211457"},
            {"9999999999.99999999999999999999", "-9999999999.9999999999999999999999999999",
             "-0.0000000000000000000099999999"},
            {"9999999999.99999999999999999999", "0", "9999999999.9999999999999999999900000000"},
            {"9999999999.99999999999999999999", "0.0000000000000000000000000001",
             "9999999999.9999999999999999999900000001"},
            {"9999999999.99999999999999999999", "-0.0000000000000000000000000001",
             "9999999999.9999999999999999999899999999"},
            {"-9999999999.99999999999999999999", "9999999999.9999999999999999999999999999",
             "0.0000000000000000000099999999"},
            {"-9999999999.99999999999999999999", "-9999999999.9999999999999999999999999999",
             "14028236692.0938463463374607431868211457"},
            {"-9999999999.99999999999999999999", "0", "-9999999999.9999999999999999999900000000"},
            {"-9999999999.99999999999999999999", "0.0000000000000000000000000001",
             "-9999999999.9999999999999999999899999999"},
            {"-9999999999.99999999999999999999", "-0.0000000000000000000000000001",
             "-9999999999.9999999999999999999900000001"},
            {"0", "9999999999.9999999999999999999999999999", "9999999999.9999999999999999999999999999"},
            {"0", "-9999999999.9999999999999999999999999999", "-9999999999.9999999999999999999999999999"},
            {"0", "0", "0"},
            {"0", "0.0000000000000000000000000001", "0.0000000000000000000000000001"},
            {"0", "-0.0000000000000000000000000001", "-0.0000000000000000000000000001"},
            {"0.00000000000000000001", "9999999999.9999999999999999999999999999",
             "10000000000.0000000000000000000099999999"},
            {"0.00000000000000000001", "-9999999999.9999999999999999999999999999",
             "-9999999999.9999999999999999999899999999"},
            {"0.00000000000000000001", "0", "0.0000000000000000000100000000"},
            {"0.00000000000000000001", "0.0000000000000000000000000001", "0.0000000000000000000100000001"},
            {"0.00000000000000000001", "-0.0000000000000000000000000001", "0.0000000000000000000099999999"},
            {"-0.00000000000000000001", "9999999999.9999999999999999999999999999",
             "9999999999.9999999999999999999899999999"},
            {"-0.00000000000000000001", "-9999999999.9999999999999999999999999999",
             "-10000000000.0000000000000000000099999999"},
            {"-0.00000000000000000001", "0", "-0.0000000000000000000100000000"},
            {"-0.00000000000000000001", "0.0000000000000000000000000001", "-0.0000000000000000000099999999"},
            {"-0.00000000000000000001", "-0.0000000000000000000000000001", "-0.0000000000000000000100000001"},
            {"-6307872495.35327887763405355720", "4071316734.4403414663028866270002207242",
             "-2236555760.9129374113311669301997792758"},
            {"-4779900788.74712379563150796108", "-6330020241.8073791065972266737160427417",
             "-11109921030.5545029022287346347960427417"},
            {"4971460943.89762127065208353698", "280102708.5110875069730789807927347481",
             "5251563652.4087087776251625177727347481"},
            {"-5958969025.28535073210443516226", "-9174948430.5057774221976584272874558508",
             "-15133917455.7911281543020935895474558508"},
            {"-148463095.43350668478300571793", "5094945587.9406246784411528172792173141",
             "4946482492.5071179936581470993492173141"},
            {"-8858183883.33389364185396188933", "-4001558163.8687703877447110360176388678",
             "-12859742047.2026640295986729253476388678"},
            {"1416104161.56447681503417707296", "7204287352.7787206768516389814337739432",
             "8620391514.3431974918858160543937739432"},
            {"5468050490.50889946773756500852", "-1430336142.6496963635355292628455086733",
             "4037714347.8592031042020357456744913267"},
            {"-5122224017.99864376608470560793", "5578068370.2909239775743662544468898592",
             "455844352.2922802114896606465168898592"},
            {"6368423354.36929345705158962036", "9726958961.1310771790494835659146234714",
             "16095382315.5003706361010731862746234714"},
            {"-8672210781.26901521433655725777", "-7954489095.5763926953263059624736073108",
             "-16626699876.8454079096628632202436073108"},
            {"7270419736.27754537393031292927", "-8245216117.9754630562948058767748618089",
             "-974796381.6979176823644929475048618089"},
            {"-8961043716.54691395046726031885", "-167455470.0519347085597683218413809908",
             "-9128499186.5988486590270286406913809908"},
            {"3120178750.94416151920001019332", "1684276937.9725219618565561345678211581",
             "4804455688.9166834810565663278878211581"},
            {"-3514260091.56189939612617247507", "-875229810.1885099929865607290285324744",
             "-4389489901.7504093891127332040985324744"},
            {"-7573787545.37958082072628598142", "9857785578.7383082393218965309588691738",
             "2283998033.3587274185956105495388691738"},
            {"962103786.28845275389090625183", "-6859321158.1329648875545600439181519799",
             "-5897217371.8445121336636537920881519799"},
            {"12497156.75034176267941674610", "6800330570.8921956026842743937285579377",
             "6812827727.6425373653636911398285579377"},
            {"5693214213.85514326409600980101", "-5256542090.3195252671769562328156911347",
             "436672123.5356179969190535681943088653"},
            {"4975741476.85157908539479032939", "7829802341.2748292194789550064799926301",
             "12805543818.1264083048737453358699926301"}};
    test_vector_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 30, 20, 38, 28, 38, 28);
    test_vector_vector<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 30, 20, 38, 28, 38, 28);
    test_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 30, 20, 38, 28, 38, 28);
    test_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 30, 20, 38, 28, 38, 28);
    test_const_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 30, 20, 38, 28, 38, 28);
    test_const_vector<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 30, 20, 38, 28, 38, 28);
    test_const_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 30, 20, 38, 28, 38, 28);
    test_const_const<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 30, 20, 38, 28, 38, 28);
    test_const_nullable_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 30, 20, 38, 28, 38, 28);
    test_nullable_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 30, 20, 38, 28, 38, 28);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 30, 20, 38, 28,
                                                                                            38, 28);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p6s2_add_decimal32p4s3_eq_decimal32p9s3) {
    DecimalTestCaseArray test_cases = {{"9999.99", "9.999", "10009.989"},
                                       {"9999.99", "-9.999", "9989.991"},
                                       {"9999.99", "0", "9999.990"},
                                       {"9999.99", "0.001", "9999.991"},
                                       {"9999.99", "-0.001", "9999.989"},
                                       {"-9999.99", "9.999", "-9989.991"},
                                       {"-9999.99", "-9.999", "-10009.989"},
                                       {"-9999.99", "0", "-9999.990"},
                                       {"-9999.99", "0.001", "-9999.989"},
                                       {"-9999.99", "-0.001", "-9999.991"},
                                       {"0", "9.999", "9.999"},
                                       {"0", "-9.999", "-9.999"},
                                       {"0", "0", "0"},
                                       {"0", "0.001", "0.001"},
                                       {"0", "-0.001", "-0.001"},
                                       {"0.01", "9.999", "10.009"},
                                       {"0.01", "-9.999", "-9.989"},
                                       {"0.01", "0", "0.010"},
                                       {"0.01", "0.001", "0.011"},
                                       {"0.01", "-0.001", "0.009"},
                                       {"-0.01", "9.999", "9.989"},
                                       {"-0.01", "-9.999", "-10.009"},
                                       {"-0.01", "0", "-0.010"},
                                       {"-0.01", "0.001", "-0.009"},
                                       {"-0.01", "-0.001", "-0.011"},
                                       {"-7371.44", "-2.244", "-7373.684"},
                                       {"-3749.79", "-6.783", "-3756.573"},
                                       {"3231.48", "-1.157", "3230.323"},
                                       {"3424.68", "2.531", "3427.211"},
                                       {"-93.05", "2.115", "-90.935"},
                                       {"-8694.24", "-4.253", "-8698.493"},
                                       {"-8394.16", "-2.487", "-8396.647"},
                                       {"4735.97", "8.622", "4744.592"},
                                       {"1866.91", "0.432", "1867.342"},
                                       {"-8188.31", "6.706", "-8181.604"},
                                       {"-6246.49", "3.365", "-6243.125"},
                                       {"-3073.16", "8.726", "-3064.434"},
                                       {"-1434.56", "-8.997", "-1443.557"},
                                       {"6456.44", "-7.473", "6448.967"},
                                       {"9600.64", "-5.958", "9594.682"},
                                       {"9217.06", "8.303", "9225.363"},
                                       {"-6123.82", "2.436", "-6121.384"},
                                       {"3130.89", "6.757", "3137.647"},
                                       {"-6381.35", "-4.877", "-6386.227"},
                                       {"5306.82", "-3.635", "5303.185"}};
    test_vector_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 2, 4, 3, 9, 3);
    test_vector_vector<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 6, 2, 4, 3, 9, 3);
    test_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 2, 4, 3, 9, 3);
    test_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 6, 2, 4, 3, 9, 3);
    test_const_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 2, 4, 3, 9, 3);
    test_const_vector<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 6, 2, 4, 3, 9, 3);
    test_const_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 2, 4, 3, 9, 3);
    test_const_const<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 6, 2, 4, 3, 9, 3);
    test_const_nullable_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 2, 4, 3, 9, 3);
    test_nullable_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 2, 4, 3, 9, 3);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 2, 4, 3, 9,
                                                                                           3);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p9s2_add_decimal64p18s9_eq_decimal64p18s9) {
    DecimalTestCaseArray test_cases = {{"9999999.99", "999999999.999999999", "1009999999.989999999"},
                                       {"9999999.99", "-999999999.999999999", "-990000000.009999999"},
                                       {"9999999.99", "0", "9999999.990000000"},
                                       {"9999999.99", "0.000000001", "9999999.990000001"},
                                       {"9999999.99", "-0.000000001", "9999999.989999999"},
                                       {"-9999999.99", "999999999.999999999", "990000000.009999999"},
                                       {"-9999999.99", "-999999999.999999999", "-1009999999.989999999"},
                                       {"-9999999.99", "0", "-9999999.990000000"},
                                       {"-9999999.99", "0.000000001", "-9999999.989999999"},
                                       {"-9999999.99", "-0.000000001", "-9999999.990000001"},
                                       {"0", "999999999.999999999", "999999999.999999999"},
                                       {"0", "-999999999.999999999", "-999999999.999999999"},
                                       {"0", "0", "0"},
                                       {"0", "0.000000001", "0.000000001"},
                                       {"0", "-0.000000001", "-0.000000001"},
                                       {"0.01", "999999999.999999999", "1000000000.009999999"},
                                       {"0.01", "-999999999.999999999", "-999999999.989999999"},
                                       {"0.01", "0", "0.010000000"},
                                       {"0.01", "0.000000001", "0.010000001"},
                                       {"0.01", "-0.000000001", "0.009999999"},
                                       {"-0.01", "999999999.999999999", "999999999.989999999"},
                                       {"-0.01", "-999999999.999999999", "-1000000000.009999999"},
                                       {"-0.01", "0", "-0.010000000"},
                                       {"-0.01", "0.000000001", "-0.009999999"},
                                       {"-0.01", "-0.000000001", "-0.010000001"},
                                       {"-3843399.61", "3224340.555324926", "-619059.054675074"},
                                       {"-3154427.67", "83210287.773296124", "80055860.103296124"},
                                       {"9158855.76", "-199367203.928852448", "-190208348.168852448"},
                                       {"-6682420.82", "-800474251.404448930", "-807156672.224448930"},
                                       {"187881.35", "379575236.600261606", "379763117.950261606"},
                                       {"6739027.82", "168011607.420839664", "174750635.240839664"},
                                       {"3131236.70", "40301544.199712199", "43432780.899712199"},
                                       {"-7956690.37", "-128378649.100179364", "-136335339.470179364"},
                                       {"-7064783.33", "-730997028.095411883", "-738061811.425411883"},
                                       {"9113452.40", "-386332997.764783131", "-377219545.364783131"},
                                       {"-1154385.04", "955289595.046271867", "954135210.006271867"},
                                       {"-5645189.80", "-364608023.054988048", "-370253212.854988048"},
                                       {"-6634367.75", "656891748.518103863", "650257380.768103863"},
                                       {"-3675200.64", "-157873185.927086088", "-161548386.567086088"},
                                       {"3022415.37", "-228704804.819227798", "-225682389.449227798"},
                                       {"773346.65", "-813297227.011717479", "-812523880.361717479"},
                                       {"-3353463.74", "-117590118.167100920", "-120943581.907100920"},
                                       {"-7395895.68", "-14389308.852363634", "-21785204.532363634"},
                                       {"-3100297.81", "933112676.101379889", "930012378.291379889"},
                                       {"-9657790.77", "-625096544.888136982", "-634754335.658136982"}};
    test_vector_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 18, 9, 18, 9);
    test_vector_vector<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 18, 9, 18, 9);
    test_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 18, 9, 18, 9);
    test_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 18, 9, 18, 9);
    test_const_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 18, 9, 18, 9);
    test_const_vector<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 18, 9, 18, 9);
    test_const_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 18, 9, 18, 9);
    test_const_const<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 18, 9, 18, 9);
    test_const_nullable_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 18, 9, 18, 9);
    test_nullable_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 18, 9, 18, 9);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 18, 9, 18,
                                                                                           9);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p38s29_add_decimal128p28s23_eq_decimal128p38s29) {
    DecimalTestCaseArray test_cases = {
            {"999999999.99999999999999999999999999999", "99999.99999999999999999999999",
             "1000099999.99999999999999999999998999999"},
            {"999999999.99999999999999999999999999999", "-99999.99999999999999999999999",
             "999900000.00000000000000000000000999999"},
            {"999999999.99999999999999999999999999999", "0", "999999999.99999999999999999999999999999"},
            {"999999999.99999999999999999999999999999", "0.00000000000000000000001",
             "1000000000.00000000000000000000000999999"},
            {"999999999.99999999999999999999999999999", "-0.00000000000000000000001",
             "999999999.99999999999999999999998999999"},
            {"-999999999.99999999999999999999999999999", "99999.99999999999999999999999",
             "-999900000.00000000000000000000000999999"},
            {"-999999999.99999999999999999999999999999", "-99999.99999999999999999999999",
             "-1000099999.99999999999999999999998999999"},
            {"-999999999.99999999999999999999999999999", "0", "-999999999.99999999999999999999999999999"},
            {"-999999999.99999999999999999999999999999", "0.00000000000000000000001",
             "-999999999.99999999999999999999998999999"},
            {"-999999999.99999999999999999999999999999", "-0.00000000000000000000001",
             "-1000000000.00000000000000000000000999999"},
            {"0", "99999.99999999999999999999999", "99999.99999999999999999999999000000"},
            {"0", "-99999.99999999999999999999999", "-99999.99999999999999999999999000000"},
            {"0", "0", "0"},
            {"0", "0.00000000000000000000001", "0.00000000000000000000001000000"},
            {"0", "-0.00000000000000000000001", "-0.00000000000000000000001000000"},
            {"0.00000000000000000000000000001", "99999.99999999999999999999999", "99999.99999999999999999999999000001"},
            {"0.00000000000000000000000000001", "-99999.99999999999999999999999",
             "-99999.99999999999999999999998999999"},
            {"0.00000000000000000000000000001", "0", "0.00000000000000000000000000001"},
            {"0.00000000000000000000000000001", "0.00000000000000000000001", "0.00000000000000000000001000001"},
            {"0.00000000000000000000000000001", "-0.00000000000000000000001", "-0.00000000000000000000000999999"},
            {"-0.00000000000000000000000000001", "99999.99999999999999999999999",
             "99999.99999999999999999999998999999"},
            {"-0.00000000000000000000000000001", "-99999.99999999999999999999999",
             "-99999.99999999999999999999999000001"},
            {"-0.00000000000000000000000000001", "0", "-0.00000000000000000000000000001"},
            {"-0.00000000000000000000000000001", "0.00000000000000000000001", "0.00000000000000000000000999999"},
            {"-0.00000000000000000000000000001", "-0.00000000000000000000001", "-0.00000000000000000000001000001"},
            {"189953567.09171402031687465804337747133", "-63563.38288542950506604779646",
             "189890003.70882859081180861024691747133"},
            {"-903630441.87505060929498351761968085714", "-31528.64143809605502995872869",
             "-903661970.51648870535001347634837085714"},
            {"-260133100.24082958914020239121121640202", "-11241.65144854699363740064740",
             "-260144341.89227813613383979185861640202"},
            {"-11550877.26465009504728579691173920614", "10881.59780938710278210660222",
             "-11539995.66684070794450369030951920614"},
            {"-776704843.02198315049635862552102571153", "471.16484157198773812301640",
             "-776704371.85714157850862050250462571153"},
            {"-919269040.35226201868639009785051774291", "-14468.27104840476872272484304",
             "-919283508.62331042345511282269355774291"},
            {"-434894309.34888492775786411863901867067", "72161.03813103794254472105352",
             "-434822148.31075388981531939758549867067"},
            {"285860051.12850949202711071155575830895", "-99802.35278090823178209364396",
             "285760248.77572858379532861791179830895"},
            {"-173546513.26244955850645415439542362508", "790.28107684737695905452770",
             "-173545722.98137271112949509986772362508"},
            {"285063678.36930819231531872704145921293", "-70784.86535122360699787926560",
             "284992893.50395696870832084777585921293"},
            {"317484344.64212115262331050241579063231", "79426.01482741353868914763379",
             "317563770.65694856616199965004958063231"},
            {"955765203.13741162854892795047267960538", "-64056.52611040616484061607290",
             "955701146.61130122238408733439977960538"},
            {"-141906526.10966898676331140023630033665", "-85463.30955393076170963427120",
             "-141991989.41922291752502103450750033665"},
            {"-786196084.89415636609399839294865428952", "69006.57251680800915681787086",
             "-786127078.32163955808484157507779428952"},
            {"-230171167.16504805341790688909087212855", "-27321.93581505639740717923679",
             "-230198489.10086310981531406832766212855"},
            {"-795728574.07784809770904224884776227247", "-60196.71417516181030333787704",
             "-795788770.79202325951934558672480227247"},
            {"-982213241.73102374048880929442852451540", "56547.64388152002707920023929",
             "-982156694.08714222046173009418923451540"},
            {"-996484357.38061455453476942339789934393", "-46970.87585092900159542661536",
             "-996531328.25646548353636485001325934393"},
            {"346453963.79369368706610783908459720220", "-40987.15176192467595927678529",
             "346412976.64193176239014856229930720220"},
            {"814713310.91412625460492314804893469799", "14944.19745575522970573657688",
             "814728255.11158200983462888462581469799"}};
    test_vector_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 29, 28, 23, 38, 29);
    test_vector_vector<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 29, 28, 23, 38, 29);
    test_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 29, 28, 23, 38, 29);
    test_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 29, 28, 23, 38, 29);
    test_const_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 29, 28, 23, 38, 29);
    test_const_vector<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 29, 28, 23, 38, 29);
    test_const_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 29, 28, 23, 38, 29);
    test_const_const<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 29, 28, 23, 38, 29);
    test_const_nullable_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 29, 28, 23, 38, 29);
    test_nullable_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 29, 28, 23, 38, 29);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 29, 28, 23,
                                                                                            38, 29);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p5s5_add_decimal32p6s2_eq_decimal32p9s5) {
    DecimalTestCaseArray test_cases = {{"0.99999", "9999.99", "10000.98999"},
                                       {"0.99999", "-9999.99", "-9998.99001"},
                                       {"0.99999", "0", "0.99999"},
                                       {"0.99999", "0.01", "1.00999"},
                                       {"0.99999", "-0.01", "0.98999"},
                                       {"-0.99999", "9999.99", "9998.99001"},
                                       {"-0.99999", "-9999.99", "-10000.98999"},
                                       {"-0.99999", "0", "-0.99999"},
                                       {"-0.99999", "0.01", "-0.98999"},
                                       {"-0.99999", "-0.01", "-1.00999"},
                                       {"0", "9999.99", "9999.99000"},
                                       {"0", "-9999.99", "-9999.99000"},
                                       {"0", "0", "0"},
                                       {"0", "0.01", "0.01000"},
                                       {"0", "-0.01", "-0.01000"},
                                       {"0.00001", "9999.99", "9999.99001"},
                                       {"0.00001", "-9999.99", "-9999.98999"},
                                       {"0.00001", "0", "0.00001"},
                                       {"0.00001", "0.01", "0.01001"},
                                       {"0.00001", "-0.01", "-0.00999"},
                                       {"-0.00001", "9999.99", "9999.98999"},
                                       {"-0.00001", "-9999.99", "-9999.99001"},
                                       {"-0.00001", "0", "-0.00001"},
                                       {"-0.00001", "0.01", "0.00999"},
                                       {"-0.00001", "-0.01", "-0.01001"},
                                       {"0.71815", "-1774.93", "-1774.21185"},
                                       {"0.66747", "7539.27", "7539.93747"},
                                       {"-0.27025", "-2028.11", "-2028.38025"},
                                       {"0.17291", "-7568.05", "-7567.87709"},
                                       {"0.98020", "-5839.83", "-5838.84980"},
                                       {"-0.53184", "489.25", "488.71816"},
                                       {"0.52161", "-4798.46", "-4797.93839"},
                                       {"0.77158", "2982.55", "2983.32158"},
                                       {"-0.64722", "3173.01", "3172.36278"},
                                       {"-0.26691", "8689.36", "8689.09309"},
                                       {"0.61338", "622.62", "623.23338"},
                                       {"-0.25987", "-8021.14", "-8021.39987"},
                                       {"0.10560", "-1470.87", "-1470.76440"},
                                       {"-0.93065", "-5782.24", "-5783.17065"},
                                       {"0.24708", "4205.81", "4206.05708"},
                                       {"0.68201", "695.40", "696.08201"},
                                       {"-0.78452", "-2608.62", "-2609.40452"},
                                       {"0.86931", "-2649.62", "-2648.75069"},
                                       {"0.46404", "-3843.14", "-3842.67596"},
                                       {"0.93959", "2124.67", "2125.60959"}};
    test_vector_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 5, 6, 2, 9, 5);
    test_vector_vector<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 5, 5, 6, 2, 9, 5);
    test_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 5, 6, 2, 9, 5);
    test_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 5, 5, 6, 2, 9, 5);
    test_const_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 5, 6, 2, 9, 5);
    test_const_vector<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 5, 5, 6, 2, 9, 5);
    test_const_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 5, 6, 2, 9, 5);
    test_const_const<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 5, 5, 6, 2, 9, 5);
    test_const_nullable_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 5, 6, 2, 9, 5);
    test_nullable_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 5, 6, 2, 9, 5);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 5, 6, 2, 9,
                                                                                           5);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p18s11_add_decimal64p12s5_eq_decimal64p18s11) {
    DecimalTestCaseArray test_cases = {{"9999999.99999999999", "9999999.99999", "19999999.99998999999"},
                                       {"9999999.99999999999", "-9999999.99999", "0.00000999999"},
                                       {"9999999.99999999999", "0", "9999999.99999999999"},
                                       {"9999999.99999999999", "0.00001", "10000000.00000999999"},
                                       {"9999999.99999999999", "-0.00001", "9999999.99998999999"},
                                       {"-9999999.99999999999", "9999999.99999", "-0.00000999999"},
                                       {"-9999999.99999999999", "-9999999.99999", "-19999999.99998999999"},
                                       {"-9999999.99999999999", "0", "-9999999.99999999999"},
                                       {"-9999999.99999999999", "0.00001", "-9999999.99998999999"},
                                       {"-9999999.99999999999", "-0.00001", "-10000000.00000999999"},
                                       {"0", "9999999.99999", "9999999.99999000000"},
                                       {"0", "-9999999.99999", "-9999999.99999000000"},
                                       {"0", "0", "0"},
                                       {"0", "0.00001", "0.00001000000"},
                                       {"0", "-0.00001", "-0.00001000000"},
                                       {"0.00000000001", "9999999.99999", "9999999.99999000001"},
                                       {"0.00000000001", "-9999999.99999", "-9999999.99998999999"},
                                       {"0.00000000001", "0", "0.00000000001"},
                                       {"0.00000000001", "0.00001", "0.00001000001"},
                                       {"0.00000000001", "-0.00001", "-0.00000999999"},
                                       {"-0.00000000001", "9999999.99999", "9999999.99998999999"},
                                       {"-0.00000000001", "-9999999.99999", "-9999999.99999000001"},
                                       {"-0.00000000001", "0", "-0.00000000001"},
                                       {"-0.00000000001", "0.00001", "0.00000999999"},
                                       {"-0.00000000001", "-0.00001", "-0.00001000001"},
                                       {"8331768.35936399237", "-2753430.33758", "5578338.02178399237"},
                                       {"2290128.51682627490", "8627873.28360", "10918001.80042627490"},
                                       {"-7012755.19227595713", "-3575681.06818", "-10588436.26045595713"},
                                       {"-5179033.32398805192", "-6333449.94380", "-11512483.26778805192"},
                                       {"8071295.81193199664", "-9063507.03093", "-992211.21899800336"},
                                       {"9490848.06505502851", "7727067.59546", "17217915.66051502851"},
                                       {"-4023547.35228228365", "-4360433.25256", "-8383980.60484228365"},
                                       {"-2528914.82134877357", "8097044.71654", "5568129.89519122643"},
                                       {"8380998.04956321445", "-4466762.69833", "3914235.35123321445"},
                                       {"9289186.95164668956", "-7022428.44381", "2266758.50783668956"},
                                       {"-1688138.49806088022", "-9964228.48037", "-11652366.97843088022"},
                                       {"-314981.63314097782", "-9820111.29414", "-10135092.92728097782"},
                                       {"-7258821.70710069859", "6540023.15856", "-718798.54854069859"},
                                       {"-6877698.95639009983", "-6172553.83161", "-13050252.78800009983"},
                                       {"8220452.32696288085", "8013049.81447", "16233502.14143288085"},
                                       {"8669136.78059011672", "-8771540.07336", "-102403.29276988328"},
                                       {"-2097173.68881382025", "-9612234.98733", "-11709408.67614382025"},
                                       {"310474.86147925182", "6780992.77370", "7091467.63517925182"},
                                       {"7066952.79949447594", "2017203.20571", "9084156.00520447594"},
                                       {"-6160737.94855374226", "-807330.86052", "-6968068.80907374226"}};
    test_vector_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 11, 12, 5, 18, 11);
    test_vector_vector<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 18, 11, 12, 5, 18, 11);
    test_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 11, 12, 5, 18, 11);
    test_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 18, 11, 12, 5, 18, 11);
    test_const_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 11, 12, 5, 18, 11);
    test_const_vector<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 18, 11, 12, 5, 18, 11);
    test_const_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 11, 12, 5, 18, 11);
    test_const_const<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 18, 11, 12, 5, 18, 11);
    test_const_nullable_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 11, 12, 5, 18, 11);
    test_nullable_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 11, 12, 5, 18, 11);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 11, 12, 5,
                                                                                           18, 11);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p38s38_add_decimal128p38s38_eq_decimal128p38s38) {
    DecimalTestCaseArray test_cases = {
            {"0.99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999",
             "-1.40282366920938463463374607431768211458"},
            {"0.99999999999999999999999999999999999999", "-0.99999999999999999999999999999999999999", "0"},
            {"0.99999999999999999999999999999999999999", "0", "0.99999999999999999999999999999999999999"},
            {"0.99999999999999999999999999999999999999", "0.00000000000000000000000000000000000001", "1"},
            {"0.99999999999999999999999999999999999999", "-0.00000000000000000000000000000000000001",
             "0.99999999999999999999999999999999999998"},
            {"-0.99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999", "0"},
            {"-0.99999999999999999999999999999999999999", "-0.99999999999999999999999999999999999999",
             "1.40282366920938463463374607431768211458"},
            {"-0.99999999999999999999999999999999999999", "0", "-0.99999999999999999999999999999999999999"},
            {"-0.99999999999999999999999999999999999999", "0.00000000000000000000000000000000000001",
             "-0.99999999999999999999999999999999999998"},
            {"-0.99999999999999999999999999999999999999", "-0.00000000000000000000000000000000000001", "-1"},
            {"0", "0.99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999"},
            {"0", "-0.99999999999999999999999999999999999999", "-0.99999999999999999999999999999999999999"},
            {"0", "0", "0"},
            {"0", "0.00000000000000000000000000000000000001", "0.00000000000000000000000000000000000001"},
            {"0", "-0.00000000000000000000000000000000000001", "-0.00000000000000000000000000000000000001"},
            {"0.00000000000000000000000000000000000001", "0.99999999999999999999999999999999999999", "1"},
            {"0.00000000000000000000000000000000000001", "-0.99999999999999999999999999999999999999",
             "-0.99999999999999999999999999999999999998"},
            {"0.00000000000000000000000000000000000001", "0", "0.00000000000000000000000000000000000001"},
            {"0.00000000000000000000000000000000000001", "0.00000000000000000000000000000000000001",
             "0.00000000000000000000000000000000000002"},
            {"0.00000000000000000000000000000000000001", "-0.00000000000000000000000000000000000001", "0"},
            {"-0.00000000000000000000000000000000000001", "0.99999999999999999999999999999999999999",
             "0.99999999999999999999999999999999999998"},
            {"-0.00000000000000000000000000000000000001", "-0.99999999999999999999999999999999999999", "-1"},
            {"-0.00000000000000000000000000000000000001", "0", "-0.00000000000000000000000000000000000001"},
            {"-0.00000000000000000000000000000000000001", "0.00000000000000000000000000000000000001", "0"},
            {"-0.00000000000000000000000000000000000001", "-0.00000000000000000000000000000000000001",
             "-0.00000000000000000000000000000000000002"},
            {"-0.32176311295796576188688861018347190651", "0.56950768232702353328760497807242344803",
             "0.24774456936905777140071636788895154152"},
            {"0.23877182473724167008504210709889537849", "-0.98133648049686565203161575889367420016",
             "-0.74256465575962398194657365179477882167"},
            {"-0.10008521542727674225922247140647738895", "0.00869367895555241336287260965437574467",
             "-0.09139153647172432889634986175210164428"},
            {"0.02745402202020021456946749049095226540", "-0.39040854441992439682729882433303856176",
             "-0.36295452239972418225783133384208629636"},
            {"-0.42577632553385537593147537592684422911", "-0.60560644500451679325688755773475689788",
             "-1.03138277053837216918836293366160112699"},
            {"0.36573858370222473221241848665974527126", "0.18443177150679414511726011866772367041",
             "0.55017035520901887732967860532746894167"},
            {"-0.72101736186299768783310394999366199748", "0.93370398968790869168906659295601756499",
             "0.21268662782491100385596264296235556751"},
            {"-0.39470207932687589144121564612262757321", "-0.83574302914611594112681374675761136529",
             "-1.23044510847299183256802939288023893850"},
            {"-0.96932011371936911338385082575611108142", "0.44521294240213885918635523735686284877",
             "-0.52410717131723025419749558839924823265"},
            {"-0.67557872820821974799592532549619623302", "0.07946215706622075333623066944449874671",
             "-0.59611657114199899465969465605169748631"},
            {"0.50309303943681835107264573803376051209", "0.44651760747384316036571310120488968157",
             "0.94961064691066151143835883923865019366"},
            {"0.29124274407973054218292550333619789946", "-0.49204582928444898863297115986783172433",
             "-0.20080308520471844645004565653163382487"},
            {"0.68609635954616663679424471878551885736", "0.55720653365001358738989331978504607161",
             "1.24330289319618022418413803857056492897"},
            {"0.63494045229041316360521792237780970607", "-0.04894977661775477758331399522730009498",
             "0.58599067567265838602190392715050961109"},
            {"-0.14488048540617138190677156196998658118", "-0.81767151711218010511437250157169506816",
             "-0.96255200251835148702114406354168164934"},
            {"-0.58684474421889977587865108504513040239", "0.54059725987227537812260057427682782795",
             "-0.04624748434662439775605051076830257444"},
            {"-0.44548962849443527838526810348112100583", "-0.35323495080099712559811289005056625866",
             "-0.79872457929543240398338099353168726449"},
            {"0.65735978061364764608100720173950722677", "0.13047438963071846217574251044239995376",
             "0.78783417024436610825674971218190718053"},
            {"-0.50940605034221672664887752140245285886", "0.17191817868652538209825324156370009803",
             "-0.33748787165569134455062427983875276083"},
            {"-0.67467574067481850167560732049745640318", "-0.82039350481122346181845032396528195764",
             "-1.49506924548604196349405764446273836082"}};
    test_vector_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 38, 38, 38, 38, 38);
    test_vector_vector<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 38, 38, 38, 38, 38);
    test_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 38, 38, 38, 38, 38);
    test_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 38, 38, 38, 38, 38);
    test_const_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 38, 38, 38, 38, 38);
    test_const_vector<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 38, 38, 38, 38, 38);
    test_const_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 38, 38, 38, 38, 38);
    test_const_const<TYPE_DECIMAL128, AddOp, OverflowMode::IGNORE>(test_cases, 38, 38, 38, 38, 38, 38);
    test_const_nullable_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 38, 38, 38, 38, 38);
    test_nullable_vector_const<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 38, 38, 38, 38, 38);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 38, 38, 38,
                                                                                            38, 38);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p9s2_add_decimal32p9s2_eq_decimal32p9s2) {
    DecimalTestCaseArray test_cases = {{"9999999.99", "9999999.99", "19999999.98"},
                                       {"9999999.99", "-9999999.99", "0"},
                                       {"9999999.99", "0", "9999999.99"},
                                       {"9999999.99", "0.01", "10000000"},
                                       {"9999999.99", "-0.01", "9999999.98"},
                                       {"-9999999.99", "9999999.99", "0"},
                                       {"-9999999.99", "-9999999.99", "-19999999.98"},
                                       {"-9999999.99", "0", "-9999999.99"},
                                       {"-9999999.99", "0.01", "-9999999.98"},
                                       {"-9999999.99", "-0.01", "-10000000"},
                                       {"0", "9999999.99", "9999999.99"},
                                       {"0", "-9999999.99", "-9999999.99"},
                                       {"0", "0", "0"},
                                       {"0", "0.01", "0.01"},
                                       {"0", "-0.01", "-0.01"},
                                       {"0.01", "9999999.99", "10000000"},
                                       {"0.01", "-9999999.99", "-9999999.98"},
                                       {"0.01", "0", "0.01"},
                                       {"0.01", "0.01", "0.02"},
                                       {"0.01", "-0.01", "0"},
                                       {"-0.01", "9999999.99", "9999999.98"},
                                       {"-0.01", "-9999999.99", "-10000000"},
                                       {"-0.01", "0", "-0.01"},
                                       {"-0.01", "0.01", "0"},
                                       {"-0.01", "-0.01", "-0.02"},
                                       {"6963445.60", "-5714534.94", "1248910.66"},
                                       {"-9838210.13", "-746488.74", "-10584698.87"},
                                       {"6468342.34", "-9740468.96", "-3272126.62"},
                                       {"1631277.34", "-8299828.06", "-6668550.72"},
                                       {"5173500.18", "6749078.17", "11922578.35"},
                                       {"9677827.40", "-9518893.23", "158934.17"},
                                       {"-6470643.07", "8271524.11", "1800881.04"},
                                       {"5742909.95", "52469.37", "5795379.32"},
                                       {"6562303.69", "-7902780.82", "-1340477.13"},
                                       {"2600627.29", "1319645.02", "3920272.31"},
                                       {"-149448.15", "-9101426.30", "-9250874.45"},
                                       {"-9584363.84", "8697713.36", "-886650.48"},
                                       {"356147.35", "4445393.38", "4801540.73"},
                                       {"6553412.83", "6950650.13", "13504062.96"},
                                       {"-3663931.61", "6421697.17", "2757765.56"},
                                       {"98922.83", "7332479.47", "7431402.30"},
                                       {"-2572496.54", "-8525123.36", "-11097619.90"},
                                       {"4367854.80", "2928178.33", "7296033.13"},
                                       {"4977479.77", "-7748164.05", "-2770684.28"},
                                       {"8140199.01", "6887181.94", "15027380.95"}};
    test_vector_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_vector_vector<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
    test_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_vector_const<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_vector<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_vector<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_const<TYPE_DECIMAL32, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_const<TYPE_DECIMAL32, AddOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p17s9_add_decimal64p17s9_eq_decimal64p18s9) {
    DecimalTestCaseArray test_cases = {{"99999999.999999999", "99999999.999999999", "199999999.999999998"},
                                       {"99999999.999999999", "-99999999.999999999", "0"},
                                       {"99999999.999999999", "0", "99999999.999999999"},
                                       {"99999999.999999999", "0.000000001", "100000000"},
                                       {"99999999.999999999", "-0.000000001", "99999999.999999998"},
                                       {"-99999999.999999999", "99999999.999999999", "0"},
                                       {"-99999999.999999999", "-99999999.999999999", "-199999999.999999998"},
                                       {"-99999999.999999999", "0", "-99999999.999999999"},
                                       {"-99999999.999999999", "0.000000001", "-99999999.999999998"},
                                       {"-99999999.999999999", "-0.000000001", "-100000000"},
                                       {"0", "99999999.999999999", "99999999.999999999"},
                                       {"0", "-99999999.999999999", "-99999999.999999999"},
                                       {"0", "0", "0"},
                                       {"0", "0.000000001", "0.000000001"},
                                       {"0", "-0.000000001", "-0.000000001"},
                                       {"0.000000001", "99999999.999999999", "100000000"},
                                       {"0.000000001", "-99999999.999999999", "-99999999.999999998"},
                                       {"0.000000001", "0", "0.000000001"},
                                       {"0.000000001", "0.000000001", "0.000000002"},
                                       {"0.000000001", "-0.000000001", "0"},
                                       {"-0.000000001", "99999999.999999999", "99999999.999999998"},
                                       {"-0.000000001", "-99999999.999999999", "-100000000"},
                                       {"-0.000000001", "0", "-0.000000001"},
                                       {"-0.000000001", "0.000000001", "0"},
                                       {"-0.000000001", "-0.000000001", "-0.000000002"},
                                       {"44214269.838756303", "-62829080.130996975", "-18614810.292240672"},
                                       {"-85243679.624413398", "-3903913.097049479", "-89147592.721462877"},
                                       {"-581774.775606364", "-28797775.619420334", "-29379550.395026698"},
                                       {"97954988.719719643", "-94258631.150852454", "3696357.568867189"},
                                       {"-30981422.214296774", "-38266279.495150610", "-69247701.709447384"},
                                       {"-33180115.228515146", "-76336982.503769274", "-109517097.732284420"},
                                       {"2608694.857456313", "-49587668.690981323", "-46978973.833525010"},
                                       {"45747230.754111140", "85777350.313016694", "131524581.067127834"},
                                       {"-20063398.713734175", "-46794299.767530819", "-66857698.481264994"},
                                       {"46548425.539034766", "-88534331.386802791", "-41985905.847768025"},
                                       {"95672008.730328499", "46883976.691291513", "142555985.421620012"},
                                       {"86086565.602936254", "-84656542.912699457", "1430022.690236797"},
                                       {"99403351.422746636", "26150812.173564833", "125554163.596311469"},
                                       {"38630858.396072899", "-89204346.581863164", "-50573488.185790265"},
                                       {"93147246.646517483", "14239178.576960367", "107386425.223477850"},
                                       {"-14700523.387408522", "-92445117.694771784", "-107145641.082180306"},
                                       {"-15838323.551334471", "38237765.398306349", "22399441.846971878"},
                                       {"-62876211.621087932", "86876581.959110312", "24000370.338022380"},
                                       {"76564882.553342952", "-91266268.954760214", "-14701386.401417262"},
                                       {"-50140855.530337280", "-35274287.273107290", "-85415142.803444570"}};
    test_vector_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 9, 17, 9, 18, 9);
    test_vector_vector<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 17, 9, 17, 9, 18, 9);
    test_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 9, 17, 9, 18, 9);
    test_vector_const<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 17, 9, 17, 9, 18, 9);
    test_const_vector<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 9, 17, 9, 18, 9);
    test_const_vector<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 17, 9, 17, 9, 18, 9);
    test_const_const<TYPE_DECIMAL64, AddOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 9, 17, 9, 18, 9);
    test_const_const<TYPE_DECIMAL64, AddOp, OverflowMode::IGNORE>(test_cases, 17, 9, 17, 9, 18, 9);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p15s11_div_decimal128p25s22_eq_decimal128p38s12) {
    DecimalTestCaseArray test_cases = {{"9999.99999999999", "999.9999999999999999999999", "10"},
                                       {"9999.99999999999", "-999.9999999999999999999999", "-10"},
                                       {"9999.99999999999", "0", "9999.999999999990"},
                                       {"9999.99999999999", "0.0000000000000000000001", "99999999999999900000000000"},
                                       {"9999.99999999999", "-0.0000000000000000000001", "-99999999999999900000000000"},
                                       {"-9999.99999999999", "999.9999999999999999999999", "-10"},
                                       {"-9999.99999999999", "-999.9999999999999999999999", "10"},
                                       {"-9999.99999999999", "0", "-9999.999999999990"},
                                       {"-9999.99999999999", "0.0000000000000000000001", "-99999999999999900000000000"},
                                       {"-9999.99999999999", "-0.0000000000000000000001", "99999999999999900000000000"},
                                       {"0", "999.9999999999999999999999", "0"},
                                       {"0", "-999.9999999999999999999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.0000000000000000000001", "0"},
                                       {"0", "-0.0000000000000000000001", "0"},
                                       {"0.00000000001", "999.9999999999999999999999", "0"},
                                       {"0.00000000001", "-999.9999999999999999999999", "0"},
                                       {"0.00000000001", "0", "0.000000000010"},
                                       {"0.00000000001", "0.0000000000000000000001", "100000000000"},
                                       {"0.00000000001", "-0.0000000000000000000001", "-100000000000"},
                                       {"-0.00000000001", "999.9999999999999999999999", "0"},
                                       {"-0.00000000001", "-999.9999999999999999999999", "0"},
                                       {"-0.00000000001", "0", "-0.000000000010"},
                                       {"-0.00000000001", "0.0000000000000000000001", "-100000000000"},
                                       {"-0.00000000001", "-0.0000000000000000000001", "100000000000"},
                                       {"6258.06248505645", "-367.5941187568188722325541", "-17.024381418889"},
                                       {"6550.07868101061", "-674.3899861338920868304718", "-9.712597778269"},
                                       {"6548.87540540820", "63.2778575480086237819056", "103.493949687528"},
                                       {"9052.01055508047", "362.9281820994242855014611", "24.941602778592"},
                                       {"-5975.05243958129", "-678.0149684121701623599137", "8.812567152572"},
                                       {"4748.33421197768", "157.3764033191742357621608", "30.171830794402"},
                                       {"-3133.07712151936", "-90.0366076672142248888306", "34.797813941409"},
                                       {"4571.27543040118", "321.7115103219302167961873", "14.209238040090"},
                                       {"9718.15781911719", "663.1921867006211546433585", "14.653607225780"},
                                       {"-1649.11194331968", "-370.1353060996941129379103", "4.455429990447"},
                                       {"-3422.97879613732", "123.6564649534938594421255", "-27.681357359073"},
                                       {"-6791.70437429918", "-703.6984193306766784638127", "9.651441850273"},
                                       {"7486.45819426021", "-319.3954011560426248924278", "-23.439467716702"},
                                       {"-8258.74594727585", "999.8247976815067701559444", "-8.260193152267"},
                                       {"9544.41379735440", "493.9782146129743429365175", "19.321527782014"},
                                       {"-433.67787881056", "512.1532286410112334532296", "-0.846773689119"},
                                       {"-3484.47809048088", "-404.4521155455302110024445", "8.615304399585"},
                                       {"1352.82423985800", "-547.3441345315043848260712", "-2.471615487423"},
                                       {"729.55662250914", "380.6496092864635723207224", "1.916609408523"},
                                       {"-1585.50133094284", "347.3142457187999537378553", "-4.565033972797"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 11, 25, 22, 38, 12);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 11, 25, 22, 38, 12);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 11, 25, 22, 38, 12);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 11, 25, 22, 38, 12);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 11, 25, 22, 38, 12);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 11, 25, 22, 38, 12);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 11, 25, 22, 38, 12);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 11, 25, 22, 38, 12);
    test_const_nullable_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 11, 25, 22, 38, 12);
    test_nullable_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 11, 25, 22, 38, 12);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 11, 25, 22,
                                                                                            38, 12);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p7s2_div_decimal32p3s2_eq_decimal128p38s8) {
    DecimalTestCaseArray test_cases = {{"99999.99", "9.99", "10010.00900901"},
                                       {"99999.99", "-9.99", "-10010.00900901"},
                                       {"99999.99", "0", "99999.99000000"},
                                       {"99999.99", "0.01", "9999999"},
                                       {"99999.99", "-0.01", "-9999999"},
                                       {"-99999.99", "9.99", "-10010.00900901"},
                                       {"-99999.99", "-9.99", "10010.00900901"},
                                       {"-99999.99", "0", "-99999.99000000"},
                                       {"-99999.99", "0.01", "-9999999"},
                                       {"-99999.99", "-0.01", "9999999"},
                                       {"0", "9.99", "0"},
                                       {"0", "-9.99", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.01", "0"},
                                       {"0", "-0.01", "0"},
                                       {"0.01", "9.99", "0.00100100"},
                                       {"0.01", "-9.99", "-0.00100100"},
                                       {"0.01", "0", "0.01000000"},
                                       {"0.01", "0.01", "1"},
                                       {"0.01", "-0.01", "-1"},
                                       {"-0.01", "9.99", "-0.00100100"},
                                       {"-0.01", "-9.99", "0.00100100"},
                                       {"-0.01", "0", "-0.01000000"},
                                       {"-0.01", "0.01", "-1"},
                                       {"-0.01", "-0.01", "1"},
                                       {"40382.95", "-8.05", "-5016.51552795"},
                                       {"-84157.39", "0.56", "-150281.05357143"},
                                       {"-50702.04", "2.59", "-19576.07722008"},
                                       {"70078.34", "0.47", "149102.85106383"},
                                       {"-82878.94", "1.36", "-60940.39705882"},
                                       {"-28338.46", "-6.21", "4563.35909823"},
                                       {"-6979.22", "2.62", "-2663.82442748"},
                                       {"11170.59", "-0.34", "-32854.67647059"},
                                       {"80257.07", "0.84", "95544.13095238"},
                                       {"61410.07", "-4.12", "-14905.35679612"},
                                       {"-96429.55", "-9.60", "10044.74479167"},
                                       {"22432.51", "-0.78", "-28759.62820513"},
                                       {"14512.84", "6.13", "2367.51060359"},
                                       {"61292.14", "-3.03", "-20228.42904290"},
                                       {"30371.64", "2.18", "13931.94495413"},
                                       {"-85737.11", "4.30", "-19938.86279070"},
                                       {"92530.67", "2.22", "41680.48198198"},
                                       {"-97770.51", "-5.82", "16799.05670103"},
                                       {"-27003.53", "4.11", "-6570.20194647"},
                                       {"-34864.13", "-2.06", "16924.33495146"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 38, 8);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 38, 8);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 38, 8);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 38, 8);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 38, 8);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 38, 8);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 38, 8);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 38, 8);
    test_const_nullable_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 38, 8);
    test_nullable_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 38, 8);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 38,
                                                                                            8);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p12s8_div_decimal64p9s6_eq_decimal128p38s12) {
    DecimalTestCaseArray test_cases = {{"9999.99999999", "999.999999", "10.000000009990"},
                                       {"9999.99999999", "-999.999999", "-10.000000009990"},
                                       {"9999.99999999", "0", "9999.999999990000"},
                                       {"9999.99999999", "0.000001", "9999999999.990000000000"},
                                       {"9999.99999999", "-0.000001", "-9999999999.990000000000"},
                                       {"-9999.99999999", "999.999999", "-10.000000009990"},
                                       {"-9999.99999999", "-999.999999", "10.000000009990"},
                                       {"-9999.99999999", "0", "-9999.999999990000"},
                                       {"-9999.99999999", "0.000001", "-9999999999.990000000000"},
                                       {"-9999.99999999", "-0.000001", "9999999999.990000000000"},
                                       {"0", "999.999999", "0"},
                                       {"0", "-999.999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.000001", "0"},
                                       {"0", "-0.000001", "0"},
                                       {"0.00000001", "999.999999", "0.000000000010"},
                                       {"0.00000001", "-999.999999", "-0.000000000010"},
                                       {"0.00000001", "0", "0.000000010000"},
                                       {"0.00000001", "0.000001", "0.010000000000"},
                                       {"0.00000001", "-0.000001", "-0.010000000000"},
                                       {"-0.00000001", "999.999999", "-0.000000000010"},
                                       {"-0.00000001", "-999.999999", "0.000000000010"},
                                       {"-0.00000001", "0", "-0.000000010000"},
                                       {"-0.00000001", "0.000001", "-0.010000000000"},
                                       {"-0.00000001", "-0.000001", "0.010000000000"},
                                       {"5334.47236455", "-547.997703", "-9.734479424542"},
                                       {"-1438.02689886", "273.327283", "-5.261190478596"},
                                       {"6861.65608733", "-933.428819", "-7.351022325067"},
                                       {"8002.45222750", "297.190028", "26.927054993581"},
                                       {"1352.30364680", "466.684211", "2.897684590405"},
                                       {"-7265.77602121", "653.230478", "-11.122836833112"},
                                       {"534.62666616", "457.712518", "1.168040298518"},
                                       {"9257.97639380", "940.491766", "9.843761241180"},
                                       {"484.63092202", "45.311260", "10.695595797159"},
                                       {"-8004.54132914", "-438.955225", "18.235439227634"},
                                       {"-2545.69163012", "315.093570", "-8.079160835050"},
                                       {"-8206.82663204", "-185.911460", "44.143737196405"},
                                       {"686.22238750", "-943.775483", "-0.727103426462"},
                                       {"5491.33605500", "-498.485281", "-11.016044533921"},
                                       {"-7404.83109997", "842.149014", "-8.792780110017"},
                                       {"9830.27240605", "-756.492938", "-12.994532945726"},
                                       {"5676.05909421", "323.660568", "17.537073265626"},
                                       {"6398.69670583", "565.243257", "11.320253053156"},
                                       {"-9450.30961930", "24.258915", "-389.560275853228"},
                                       {"1558.23869752", "-496.994912", "-3.135321227433"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 12, 8, 9, 6, 38, 12);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 12, 8, 9, 6, 38, 12);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 12, 8, 9, 6, 38, 12);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 12, 8, 9, 6, 38, 12);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 12, 8, 9, 6, 38, 12);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 12, 8, 9, 6, 38, 12);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 12, 8, 9, 6, 38, 12);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 12, 8, 9, 6, 38, 12);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p25s17_div_decimal128p9s0_eq_decimal128p38s17) {
    DecimalTestCaseArray test_cases = {{"99999999.99999999999999999", "999999999", "0.10000000010000000"},
                                       {"99999999.99999999999999999", "-999999999", "-0.10000000010000000"},
                                       {"99999999.99999999999999999", "0", "99999999.99999999999999999"},
                                       {"99999999.99999999999999999", "1", "99999999.99999999999999999"},
                                       {"99999999.99999999999999999", "-1", "-99999999.99999999999999999"},
                                       {"-99999999.99999999999999999", "999999999", "-0.10000000010000000"},
                                       {"-99999999.99999999999999999", "-999999999", "0.10000000010000000"},
                                       {"-99999999.99999999999999999", "0", "-99999999.99999999999999999"},
                                       {"-99999999.99999999999999999", "1", "-99999999.99999999999999999"},
                                       {"-99999999.99999999999999999", "-1", "99999999.99999999999999999"},
                                       {"0", "999999999", "0"},
                                       {"0", "-999999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "1", "0"},
                                       {"0", "-1", "0"},
                                       {"0.00000000000000001", "999999999", "0"},
                                       {"0.00000000000000001", "-999999999", "0"},
                                       {"0.00000000000000001", "0", "0.00000000000000001"},
                                       {"0.00000000000000001", "1", "0.00000000000000001"},
                                       {"0.00000000000000001", "-1", "-0.00000000000000001"},
                                       {"-0.00000000000000001", "999999999", "0"},
                                       {"-0.00000000000000001", "-999999999", "0"},
                                       {"-0.00000000000000001", "0", "-0.00000000000000001"},
                                       {"-0.00000000000000001", "1", "-0.00000000000000001"},
                                       {"-0.00000000000000001", "-1", "0.00000000000000001"},
                                       {"74116040.94213202374095506", "-254389603", "-0.29134854596291038"},
                                       {"-67670889.35063141291044881", "-380544995", "0.17782624982528390"},
                                       {"-228800.81754861206077225", "828780339", "-0.00027606931147122"},
                                       {"-52338251.53320430752059601", "644347387", "-0.08122676150963317"},
                                       {"87755021.64505223314785883", "-1718112", "-51.07642670853368881"},
                                       {"54561866.00464503859655988", "-799517399", "-0.06824350048277691"},
                                       {"15810603.07643939703179946", "657853512", "0.02403362266528327"},
                                       {"-81026549.63792208107874479", "81098435", "-0.99911360358460778"},
                                       {"-45450870.40275402937072143", "607491095", "-0.07481734428181870"},
                                       {"-65133723.45035919520313708", "-286839633", "0.22707365355734922"},
                                       {"33899523.21242733853458464", "-808428564", "-0.04193261436075054"},
                                       {"3908415.58371051046935541", "-772422127", "-0.00505994772429727"},
                                       {"-3650953.62598575046824675", "-697885755", "0.00523144884363738"},
                                       {"-80781367.71827760396839862", "-864811945", "0.09340917200013652"},
                                       {"83081022.61511391421873181", "298662880", "0.27817659367348870"},
                                       {"-95282218.58450054653702770", "992044226", "-0.09604634157157077"},
                                       {"55257233.91405628895854559", "-694654269", "-0.07954638210688991"},
                                       {"68225778.23064308437573415", "-769973761", "-0.08860792625197404"},
                                       {"47367143.19011734346518047", "610301254", "0.07761272466616518"},
                                       {"85848629.92129111690055903", "364576713", "0.23547480368361080"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 25, 17, 9, 0, 38, 17);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 25, 17, 9, 0, 38, 17);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 25, 17, 9, 0, 38, 17);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 25, 17, 9, 0, 38, 17);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 25, 17, 9, 0, 38, 17);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 25, 17, 9, 0, 38, 17);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 25, 17, 9, 0, 38, 17);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 25, 17, 9, 0, 38, 17);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p9s3_div_decimal32p3s0_eq_decimal128p38s9) {
    DecimalTestCaseArray test_cases = {{"999999.999", "999", "1001.001000000"},
                                       {"999999.999", "-999", "-1001.001000000"},
                                       {"999999.999", "0", "999999.999000000"},
                                       {"999999.999", "1", "999999.999000000"},
                                       {"999999.999", "-1", "-999999.999000000"},
                                       {"-999999.999", "999", "-1001.001000000"},
                                       {"-999999.999", "-999", "1001.001000000"},
                                       {"-999999.999", "0", "-999999.999000000"},
                                       {"-999999.999", "1", "-999999.999000000"},
                                       {"-999999.999", "-1", "999999.999000000"},
                                       {"0", "999", "0"},
                                       {"0", "-999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "1", "0"},
                                       {"0", "-1", "0"},
                                       {"0.001", "999", "0.000001001"},
                                       {"0.001", "-999", "-0.000001001"},
                                       {"0.001", "0", "0.001000000"},
                                       {"0.001", "1", "0.001000000"},
                                       {"0.001", "-1", "-0.001000000"},
                                       {"-0.001", "999", "-0.000001001"},
                                       {"-0.001", "-999", "0.000001001"},
                                       {"-0.001", "0", "-0.001000000"},
                                       {"-0.001", "1", "-0.001000000"},
                                       {"-0.001", "-1", "0.001000000"},
                                       {"2597.393", "748", "3.472450535"},
                                       {"-37228.493", "-543", "68.560760589"},
                                       {"550317.492", "948", "580.503683544"},
                                       {"870721.522", "488", "1784.265413934"},
                                       {"-673346.275", "460", "-1463.796250000"},
                                       {"-694104.206", "-963", "720.772799585"},
                                       {"-28283.734", "335", "-84.429056716"},
                                       {"-471733.395", "-901", "523.566476138"},
                                       {"-28188.078", "-665", "42.388087218"},
                                       {"-761665.496", "-735", "1036.279586395"},
                                       {"-350231.914", "954", "-367.119406709"},
                                       {"-648879.407", "-48", "13518.320979167"},
                                       {"808899.711", "-325", "-2488.922187692"},
                                       {"-681342.514", "666", "-1023.036807808"},
                                       {"-269673.472", "138", "-1954.155594203"},
                                       {"498332.321", "-669", "-744.891361734"},
                                       {"910048.962", "-37", "-24595.917891892"},
                                       {"242121.137", "973", "248.839811922"},
                                       {"-193814.768", "-446", "434.562260090"},
                                       {"-240048.747", "-929", "258.394776103"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 38, 9);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 38, 9);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 38, 9);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 38, 9);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 38, 9);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 38, 9);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 38, 9);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 38, 9);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p17s2_div_decimal64p15s0_eq_decimal128p38s8) {
    DecimalTestCaseArray test_cases = {{"999999999999999.99", "999999999999999", "1"},
                                       {"999999999999999.99", "-999999999999999", "-1"},
                                       {"999999999999999.99", "0", "999999999999999.99000000"},
                                       {"999999999999999.99", "1", "999999999999999.99000000"},
                                       {"999999999999999.99", "-1", "-999999999999999.99000000"},
                                       {"-999999999999999.99", "999999999999999", "-1"},
                                       {"-999999999999999.99", "-999999999999999", "1"},
                                       {"-999999999999999.99", "0", "-999999999999999.99000000"},
                                       {"-999999999999999.99", "1", "-999999999999999.99000000"},
                                       {"-999999999999999.99", "-1", "999999999999999.99000000"},
                                       {"0", "999999999999999", "0"},
                                       {"0", "-999999999999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "1", "0"},
                                       {"0", "-1", "0"},
                                       {"0.01", "999999999999999", "0"},
                                       {"0.01", "-999999999999999", "0"},
                                       {"0.01", "0", "0.01000000"},
                                       {"0.01", "1", "0.01000000"},
                                       {"0.01", "-1", "-0.01000000"},
                                       {"-0.01", "999999999999999", "0"},
                                       {"-0.01", "-999999999999999", "0"},
                                       {"-0.01", "0", "-0.01000000"},
                                       {"-0.01", "1", "-0.01000000"},
                                       {"-0.01", "-1", "0.01000000"},
                                       {"-889345531665736.87", "-88021135339541", "10.10377256"},
                                       {"332739133280022.89", "810709534368599", "0.41042953"},
                                       {"-356130216698695.29", "-619903509408927", "0.57449298"},
                                       {"809806937200199.04", "945647083174780", "0.85635218"},
                                       {"-890437891410508.65", "-328444872175058", "2.71107259"},
                                       {"773604897679066.13", "380555083658044", "2.03283291"},
                                       {"496912651333899.57", "626436257130617", "0.79323737"},
                                       {"204297554975669.23", "-463086989005312", "-0.44116453"},
                                       {"-479038253127693.49", "483398008212434", "-0.99098102"},
                                       {"-694013024771979.79", "659455166829211", "-1.05240365"},
                                       {"-759243193547346.44", "997509252428282", "-0.76113900"},
                                       {"668241719519673.06", "718066652859816", "0.93061238"},
                                       {"136195510090638.98", "445749047653236", "0.30554302"},
                                       {"-642563564455113.34", "185019519472549", "-3.47295013"},
                                       {"-457407924628847.85", "324254838792830", "-1.41064333"},
                                       {"220552487045408.17", "-134966993286477", "-1.63412166"},
                                       {"-135122115808294.13", "-843844260737689", "0.16012684"},
                                       {"372085889948693.63", "936797518158384", "0.39718924"},
                                       {"904482717533081.91", "-425466412804779", "-2.12586162"},
                                       {"446252633031284.02", "-512319100731698", "-0.87104430"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 2, 15, 0, 38, 8);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 17, 2, 15, 0, 38, 8);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 2, 15, 0, 38, 8);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 17, 2, 15, 0, 38, 8);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 2, 15, 0, 38, 8);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 17, 2, 15, 0, 38, 8);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 2, 15, 0, 38, 8);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 17, 2, 15, 0, 38, 8);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p7s0_div_decimal32p3s1_eq_decimal128p38s6) {
    DecimalTestCaseArray test_cases = {{"9999999", "99.9", "100100.090090"},
                                       {"9999999", "-99.9", "-100100.090090"},
                                       {"9999999", "0", "9999999"},
                                       {"9999999", "0.1", "99999990"},
                                       {"9999999", "-0.1", "-99999990"},
                                       {"-9999999", "99.9", "-100100.090090"},
                                       {"-9999999", "-99.9", "100100.090090"},
                                       {"-9999999", "0", "-9999999"},
                                       {"-9999999", "0.1", "-99999990"},
                                       {"-9999999", "-0.1", "99999990"},
                                       {"0", "99.9", "0"},
                                       {"0", "-99.9", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.1", "0"},
                                       {"0", "-0.1", "0"},
                                       {"1", "99.9", "0.010010"},
                                       {"1", "-99.9", "-0.010010"},
                                       {"1", "0", "1"},
                                       {"1", "0.1", "10"},
                                       {"1", "-0.1", "-10"},
                                       {"-1", "99.9", "-0.010010"},
                                       {"-1", "-99.9", "0.010010"},
                                       {"-1", "0", "-1"},
                                       {"-1", "0.1", "-10"},
                                       {"-1", "-0.1", "10"},
                                       {"-6650627", "55.7", "-119400.843806"},
                                       {"6976950", "53", "131640.566038"},
                                       {"-6644747", "-83.9", "79198.414779"},
                                       {"2691942", "69.2", "38900.895954"},
                                       {"-2527543", "-60.8", "41571.430921"},
                                       {"9965268", "-23.6", "-422257.118644"},
                                       {"-5841655", "8.3", "-703813.855422"},
                                       {"-7138853", "-23.4", "305079.188034"},
                                       {"-7664189", "60.7", "-126263.410214"},
                                       {"-6959548", "-80", "86994.350000"},
                                       {"4492039", "5.9", "761362.542373"},
                                       {"2901651", "-64.8", "-44778.564815"},
                                       {"-8478776", "-77.2", "109828.704663"},
                                       {"-177042", "-37.7", "4696.074271"},
                                       {"-6210988", "-2.3", "2700429.565217"},
                                       {"5987033", "-89.4", "-66969.049217"},
                                       {"-3356758", "67.2", "-49951.755952"},
                                       {"5811623", "-66.7", "-87130.779610"},
                                       {"2207779", "99.4", "22211.056338"},
                                       {"-542981", "87.4", "-6212.597254"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 38, 6);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 38, 6);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 38, 6);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 38, 6);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 38, 6);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 38, 6);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 38, 6);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 38, 6);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p19s0_mod_decimal128p25s10_eq_decimal128p38s10) {
    DecimalTestCaseArray test_cases = {
            {"9999999999999999999", "999999999999999.9999999999", "999999999999999.0000009999"},
            {"9999999999999999999", "-999999999999999.9999999999", "999999999999999.0000009999"},
            {"9999999999999999999", "0", "0"},
            {"9999999999999999999", "0.0000000001", "0"},
            {"9999999999999999999", "-0.0000000001", "0"},
            {"-9999999999999999999", "999999999999999.9999999999", "-999999999999999.0000009999"},
            {"-9999999999999999999", "-999999999999999.9999999999", "-999999999999999.0000009999"},
            {"-9999999999999999999", "0", "0"},
            {"-9999999999999999999", "0.0000000001", "0"},
            {"-9999999999999999999", "-0.0000000001", "0"},
            {"0", "999999999999999.9999999999", "0"},
            {"0", "-999999999999999.9999999999", "0"},
            {"0", "0", "0"},
            {"0", "0.0000000001", "0"},
            {"0", "-0.0000000001", "0"},
            {"1", "999999999999999.9999999999", "1"},
            {"1", "-999999999999999.9999999999", "1"},
            {"1", "0", "0"},
            {"1", "0.0000000001", "0"},
            {"1", "-0.0000000001", "0"},
            {"-1", "999999999999999.9999999999", "-1"},
            {"-1", "-999999999999999.9999999999", "-1"},
            {"-1", "0", "0"},
            {"-1", "0.0000000001", "0"},
            {"-1", "-0.0000000001", "0"},
            {"3556863202848959609", "544674436317701.0562764865", "139133694371711.5145431550"},
            {"5107869304349842988", "877541046580438.6555944474", "580413251690012.4403161320"},
            {"-4866786761235065952", "71799515005337.7124732888", "-235628259787.4230652696"},
            {"-9539708030122414248", "925276491928397.2776412316", "-107398340638315.5189022040"},
            {"-2719410416447603335", "461892591629812.5154129591", "-248729522897056.7639097783"},
            {"315479561767459285", "78949287847897.1643472195", "77156815110113.4328580975"},
            {"9156091093041939160", "549306121580058.6138949201", "256658545522183.5994717732"},
            {"-1535146531224369994", "-881219377497574.6731852588", "-62375623594913.3112791704"},
            {"-5477438214725614234", "532630499652247.9994027074", "-398786801548056.1419598058"},
            {"-8942959745485586445", "540037966776467.4160751395", "-471053634062502.2117650195"},
            {"-9214051371113546040", "717388039073606.5787931883", "-636785291216748.5590826631"},
            {"-7348172967715031442", "-318296623210213.3222791957", "-295420907256897.1847672655"},
            {"7989731849866799101", "950265232689936.9959003986", "852038642498776.4653489698"},
            {"7854761024021182062", "181714509213463.6134685932", "151363269217369.8200589300"},
            {"8626982361442758196", "911540315048382.7143012722", "164819824864187.8527598992"},
            {"8724005683691188300", "-233749488044123.4924046431", "7290908411316.4739102218"},
            {"-1222590658313015482", "-628155176748869.6288728069", "-200684359715184.2135177726"},
            {"-7341118520804255717", "626477587531744.1469953720", "-54150107277802.5082309040"},
            {"-1203448675473009682", "40966023177328.8636775864", "-30778615796982.6072219136"},
            {"-4287335955395136634", "354431798245120.1851337606", "-128923822162874.6220317824"}};
    test_vector_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 19, 0, 25, 10, 38, 10);
    test_vector_vector<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 19, 0, 25, 10, 38, 10);
    test_vector_const<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 19, 0, 25, 10, 38, 10);
    test_vector_const<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 19, 0, 25, 10, 38, 10);
    test_const_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 19, 0, 25, 10, 38, 10);
    test_const_vector<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 19, 0, 25, 10, 38, 10);
    test_const_const<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 19, 0, 25, 10, 38, 10);
    test_const_const<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 19, 0, 25, 10, 38, 10);
    test_const_nullable_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 19, 0, 25, 10, 38, 10);
    test_nullable_vector_const<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 19, 0, 25, 10, 38, 10);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 19, 0, 25, 10,
                                                                                            38, 10);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p7s0_mod_decimal32p3s1_eq_decimal32p9s1) {
    DecimalTestCaseArray test_cases = {{"9999999", "99.9", "9"},
                                       {"9999999", "-99.9", "9"},
                                       {"9999999", "0", "0"},
                                       {"9999999", "0.1", "0"},
                                       {"9999999", "-0.1", "0"},
                                       {"-9999999", "99.9", "-9"},
                                       {"-9999999", "-99.9", "-9"},
                                       {"-9999999", "0", "0"},
                                       {"-9999999", "0.1", "0"},
                                       {"-9999999", "-0.1", "0"},
                                       {"0", "99.9", "0"},
                                       {"0", "-99.9", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.1", "0"},
                                       {"0", "-0.1", "0"},
                                       {"1", "99.9", "1"},
                                       {"1", "-99.9", "1"},
                                       {"1", "0", "0"},
                                       {"1", "0.1", "0"},
                                       {"1", "-0.1", "0"},
                                       {"-1", "99.9", "-1"},
                                       {"-1", "-99.9", "-1"},
                                       {"-1", "0", "0"},
                                       {"-1", "0.1", "0"},
                                       {"-1", "-0.1", "0"},
                                       {"6860102", "-10.3", "3.3"},
                                       {"-6375273", "66.6", "-54.6"},
                                       {"-1150629", "-64.3", "-44.8"},
                                       {"-1452459", "-68.1", "-22.2"},
                                       {"-883757", "-28.4", "-5.8"},
                                       {"1445610", "4.8", "3.6"},
                                       {"8407873", "25.4", "15.8"},
                                       {"6943812", "-96.8", "57.6"},
                                       {"-5827595", "35.3", "-23.9"},
                                       {"-4876597", "-24.1", "-10.2"},
                                       {"-97646", "-94.7", "-10.3"},
                                       {"9715051", "12.2", "8"},
                                       {"5140406", "31.8", "31.4"},
                                       {"3384700", "-63", "25"},
                                       {"5032547", "69.3", "50.3"},
                                       {"5759639", "-83.9", "71.8"},
                                       {"1231582", "-98.3", "79.6"},
                                       {"8204400", "-79.7", "2.3"},
                                       {"3174301", "93.5", "69.5"},
                                       {"-280267", "93.8", "-86.4"}};
    test_vector_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 9, 1);
    test_vector_vector<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 9, 1);
    test_vector_const<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 9, 1);
    test_vector_const<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 9, 1);
    test_const_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 9, 1);
    test_const_vector<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 9, 1);
    test_const_const<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 9, 1);
    test_const_const<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 0, 3, 1, 9, 1);
    test_const_nullable_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 9, 1);
    test_nullable_vector_const<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 9, 1);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 0, 3, 1, 9,
                                                                                           1);
}

TEST_F(DecimalBinaryFunctionTest, test_decimal64p10s0_mod_decimal64p9s5_eq_decimal64p18s5) {
    DecimalTestCaseArray test_cases = {{"9999999999", "9999.99999", "9"},
                                       {"9999999999", "-9999.99999", "9"},
                                       {"9999999999", "0", "0"},
                                       {"9999999999", "0.00001", "0"},
                                       {"9999999999", "-0.00001", "0"},
                                       {"-9999999999", "9999.99999", "-9"},
                                       {"-9999999999", "-9999.99999", "-9"},
                                       {"-9999999999", "0", "0"},
                                       {"-9999999999", "0.00001", "0"},
                                       {"-9999999999", "-0.00001", "0"},
                                       {"0", "9999.99999", "0"},
                                       {"0", "-9999.99999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.00001", "0"},
                                       {"0", "-0.00001", "0"},
                                       {"1", "9999.99999", "1"},
                                       {"1", "-9999.99999", "1"},
                                       {"1", "0", "0"},
                                       {"1", "0.00001", "0"},
                                       {"1", "-0.00001", "0"},
                                       {"-1", "9999.99999", "-1"},
                                       {"-1", "-9999.99999", "-1"},
                                       {"-1", "0", "0"},
                                       {"-1", "0.00001", "0"},
                                       {"-1", "-0.00001", "0"},
                                       {"632741824", "-5794.67188", "4217.40716"},
                                       {"4046630533", "-6007.03631", "2536.84112"},
                                       {"-4947933864", "-8380.54152", "-3486.80136"},
                                       {"-5965712923", "3985.22005", "-1902.17205"},
                                       {"1999849352", "7344.84203", "3108.91363"},
                                       {"-2862285376", "-8360.37655", "-1779.21235"},
                                       {"-196763842", "4489.84845", "-723.52720"},
                                       {"-7427963413", "-5703.16557", "-885.99604"},
                                       {"-371452271", "-5273.72670", "-2604.61220"},
                                       {"-7181856007", "-8208.14167", "-2914.42511"},
                                       {"2532356100", "-6334.85294", "4972.08794"},
                                       {"1005926329", "9995.74593", "4437.33445"},
                                       {"2125395856", "-3569.62037", "1052.25756"},
                                       {"5697757320", "7030.87658", "5248.33380"},
                                       {"-4762721272", "6429.07724", "-2990.91284"},
                                       {"7503800008", "-505.09989", "180.06781"},
                                       {"-3280847867", "879.69138", "-158.86928"},
                                       {"6165666620", "3528.43279", "2008.79983"},
                                       {"-847919062", "-6873.70436", "-6386.96784"},
                                       {"3073937658", "-9682.00813", "6578.81443"}};
    test_vector_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 10, 0, 9, 5, 18, 5);
    test_vector_vector<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 10, 0, 9, 5, 18, 5);
    test_vector_const<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 10, 0, 9, 5, 18, 5);
    test_vector_const<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 10, 0, 9, 5, 18, 5);
    test_const_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 10, 0, 9, 5, 18, 5);
    test_const_vector<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 10, 0, 9, 5, 18, 5);
    test_const_const<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 10, 0, 9, 5, 18, 5);
    test_const_const<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 10, 0, 9, 5, 18, 5);
    test_const_nullable_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 10, 0, 9, 5, 18, 5);
    test_nullable_vector_const<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 10, 0, 9, 5, 18, 5);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 10, 0, 9, 5, 18,
                                                                                           5);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p15s0_div_decimal128p25s17_eq_decimal128p38s6) {
    DecimalTestCaseArray test_cases = {{"999999999999999", "99999999.99999999999999999", "10000000"},
                                       {"999999999999999", "-99999999.99999999999999999", "-10000000"},
                                       {"999999999999999", "0", "999999999999999"},
                                       {"999999999999999", "0.00000000000000001", "99999999999999900000000000000000"},
                                       {"999999999999999", "-0.00000000000000001", "-99999999999999900000000000000000"},
                                       {"-999999999999999", "99999999.99999999999999999", "-10000000"},
                                       {"-999999999999999", "-99999999.99999999999999999", "10000000"},
                                       {"-999999999999999", "0", "-999999999999999"},
                                       {"-999999999999999", "0.00000000000000001", "-99999999999999900000000000000000"},
                                       {"-999999999999999", "-0.00000000000000001", "99999999999999900000000000000000"},
                                       {"0", "99999999.99999999999999999", "0"},
                                       {"0", "-99999999.99999999999999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.00000000000000001", "0"},
                                       {"0", "-0.00000000000000001", "0"},
                                       {"1", "99999999.99999999999999999", "0"},
                                       {"1", "-99999999.99999999999999999", "0"},
                                       {"1", "0", "1"},
                                       {"1", "0.00000000000000001", "100000000000000000"},
                                       {"1", "-0.00000000000000001", "-100000000000000000"},
                                       {"-1", "99999999.99999999999999999", "0"},
                                       {"-1", "-99999999.99999999999999999", "0"},
                                       {"-1", "0", "-1"},
                                       {"-1", "0.00000000000000001", "-100000000000000000"},
                                       {"-1", "-0.00000000000000001", "100000000000000000"},
                                       {"-494071391073767", "-77451260.24720219828194471", "6379126.556454"},
                                       {"-783375334902532", "-29553859.70109609053092613", "26506701.419899"},
                                       {"-421502349707416", "90039655.39508066317417023", "-4681296.789264"},
                                       {"-450029303045983", "-34944915.84498205199094622", "12878248.299190"},
                                       {"-70716438406705", "-35084956.51417478259181390", "2015577.199822"},
                                       {"486208542875384", "32991104.32880192218386505", "14737564.951741"},
                                       {"-946224336200430", "-93742218.95341695461629396", "10093897.357717"},
                                       {"635741916396347", "-70883511.62590745188613294", "-8968826.484663"},
                                       {"-947024476124778", "49702799.68746870847089682", "-19053745.102483"},
                                       {"497285875508908", "-81574397.96605109992585748", "-6096102.305479"},
                                       {"-703863501155130", "4663372.48179420090439260", "-150934437.234643"},
                                       {"445750953510240", "-33647504.10199604115762516", "-13247667.706913"},
                                       {"-820918571987103", "-22842755.82547003278570862", "35937807.953617"},
                                       {"-421426484464574", "28006691.18510396523159473", "-15047349.995015"},
                                       {"-561753912840535", "23247021.21007609864308117", "-24164554.579451"},
                                       {"947790337085651", "89856333.15727829339597053", "10547841.245944"},
                                       {"255613069386921", "-66446705.21363964725764021", "-3846888.548726"},
                                       {"-192687054134394", "20305515.96744102909828465", "-9489394.627714"},
                                       {"-574633718564171", "26995620.02571797212079092", "-21286183.388888"},
                                       {"-562422579463537", "-40941330.91455957957681706", "13737281.297407"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 0, 25, 17, 38, 6);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 0, 25, 17, 38, 6);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 0, 25, 17, 38, 6);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 0, 25, 17, 38, 6);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 0, 25, 17, 38, 6);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 0, 25, 17, 38, 6);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 15, 0, 25, 17, 38, 6);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 15, 0, 25, 17, 38, 6);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p13s0_div_decimal64p9s3_eq_decimal128p38s6) {
    DecimalTestCaseArray test_cases = {{"9999999999999", "999999.999", "10000000.009999"},
                                       {"9999999999999", "-999999.999", "-10000000.009999"},
                                       {"9999999999999", "0", "9999999999999"},
                                       {"9999999999999", "0.001", "9999999999999000"},
                                       {"9999999999999", "-0.001", "-9999999999999000"},
                                       {"-9999999999999", "999999.999", "-10000000.009999"},
                                       {"-9999999999999", "-999999.999", "10000000.009999"},
                                       {"-9999999999999", "0", "-9999999999999"},
                                       {"-9999999999999", "0.001", "-9999999999999000"},
                                       {"-9999999999999", "-0.001", "9999999999999000"},
                                       {"0", "999999.999", "0"},
                                       {"0", "-999999.999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.001", "0"},
                                       {"0", "-0.001", "0"},
                                       {"1", "999999.999", "0.000001"},
                                       {"1", "-999999.999", "-0.000001"},
                                       {"1", "0", "1"},
                                       {"1", "0.001", "1000"},
                                       {"1", "-0.001", "-1000"},
                                       {"-1", "999999.999", "-0.000001"},
                                       {"-1", "-999999.999", "0.000001"},
                                       {"-1", "0", "-1"},
                                       {"-1", "0.001", "-1000"},
                                       {"-1", "-0.001", "1000"},
                                       {"-4174757411586", "716672.317", "-5825196.972951"},
                                       {"-9422967910205", "-998300.264", "9439011.738261"},
                                       {"-3712222123784", "363865.993", "-10202168.367474"},
                                       {"-6393744789450", "-538917.823", "11864044.046378"},
                                       {"7247709651495", "401480.903", "18052439.349762"},
                                       {"-9998247961458", "825886.559", "-12106079.040158"},
                                       {"-64981699952", "543631.347", "-119532.658134"},
                                       {"-9890687820854", "-453930.610", "21788986.252445"},
                                       {"4135370832609", "-671145.930", "-6161656.724357"},
                                       {"-1517095251255", "-87830.988", "17272892.925388"},
                                       {"4125421418350", "561776.561", "7343527.132934"},
                                       {"-4213664602363", "489195.177", "-8613463.093000"},
                                       {"3037804057161", "350187.415", "8674795.058415"},
                                       {"-6354622838107", "552059.198", "-11510763.449153"},
                                       {"270714300618", "-872070.186", "-310427.193779"},
                                       {"765388511297", "-611512.599", "-1251631.630401"},
                                       {"-5648272450357", "34437.329", "-164015985.396457"},
                                       {"9732313140496", "-564089.673", "-17253131.206492"},
                                       {"-8313546164646", "-961638.869", "8645185.248482"},
                                       {"211575831776", "-156421.503", "-1352600.682887"}};
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 13, 0, 9, 3, 38, 6);
    test_vector_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 13, 0, 9, 3, 38, 6);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 13, 0, 9, 3, 38, 6);
    test_vector_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 13, 0, 9, 3, 38, 6);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 13, 0, 9, 3, 38, 6);
    test_const_vector<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 13, 0, 9, 3, 38, 6);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_cases, 13, 0, 9, 3, 38, 6);
    test_const_const<TYPE_DECIMAL128, DivOp, OverflowMode::IGNORE>(test_cases, 13, 0, 9, 3, 38, 6);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p14s11_mod_decimal128p25s24_eq_decimal128p38s24) {
    DecimalTestCaseArray test_cases = {
            {"999.99999999999", "9.999999999999999999999999", "9.999999999990000000000099"},
            {"999.99999999999", "-9.999999999999999999999999", "9.999999999990000000000099"},
            {"999.99999999999", "0", "0"},
            {"999.99999999999", "0.000000000000000000000001", "0"},
            {"999.99999999999", "-0.000000000000000000000001", "0"},
            {"-999.99999999999", "9.999999999999999999999999", "-9.999999999990000000000099"},
            {"-999.99999999999", "-9.999999999999999999999999", "-9.999999999990000000000099"},
            {"-999.99999999999", "0", "0"},
            {"-999.99999999999", "0.000000000000000000000001", "0"},
            {"-999.99999999999", "-0.000000000000000000000001", "0"},
            {"0", "9.999999999999999999999999", "0"},
            {"0", "-9.999999999999999999999999", "0"},
            {"0", "0", "0"},
            {"0", "0.000000000000000000000001", "0"},
            {"0", "-0.000000000000000000000001", "0"},
            {"0.00000000001", "9.999999999999999999999999", "0.000000000010000000000000"},
            {"0.00000000001", "-9.999999999999999999999999", "0.000000000010000000000000"},
            {"0.00000000001", "0", "0"},
            {"0.00000000001", "0.000000000000000000000001", "0"},
            {"0.00000000001", "-0.000000000000000000000001", "0"},
            {"-0.00000000001", "9.999999999999999999999999", "-0.000000000010000000000000"},
            {"-0.00000000001", "-9.999999999999999999999999", "-0.000000000010000000000000"},
            {"-0.00000000001", "0", "0"},
            {"-0.00000000001", "0.000000000000000000000001", "0"},
            {"-0.00000000001", "-0.000000000000000000000001", "0"},
            {"-321.25066595334", "-8.534576256022672208097191", "-5.471344480501128300403933"},
            {"-662.91674600696", "-5.983225947667299562417968", "-4.761891763557048134023520"},
            {"-674.02681881430", "-7.927665110594174877059523", "-0.175284413795135449940545"},
            {"177.05641910453", "-7.235228273422680196195122", "3.410940542385675291317072"},
            {"248.88986922921", "8.487104279555924650365180", "2.763845122088185139409780"},
            {"782.70538038569", "-0.392646387834887744103252", "0.161129430758726002218764"},
            {"-652.95590990773", "8.734770318239852023654293", "-6.582906357980950249582318"},
            {"364.81895077727", "8.129666569052257288312350", "7.113621738970679314256600"},
            {"-754.25164297197", "7.997770888110219117695815", "-2.461179489609402936593390"},
            {"-368.07054885874", "-0.627957033698112502261129", "-0.087727111646073674978406"},
            {"235.31901818326", "1.313718880765889683304956", "0.163338526165746688412876"},
            {"-822.84186365002", "7.196184807151376381418289", "-2.476795634763092518315054"},
            {"773.16950957994", "-4.596141593943376696229715", "1.017721797452715033407880"},
            {"-926.89319714551", "8.293547925062912358845385", "-6.309377463526728168162265"},
            {"166.30755375232", "5.948312058587479697862306", "5.703128170458048157717738"},
            {"-883.91794154023", "8.584548090105493496686825", "-8.294036349469663337943850"},
            {"545.48006341727", "8.309770582491595057786385", "5.344975555316321243884975"},
            {"-89.98857039894", "-5.219267312564343871218155", "-1.261026085346154189291365"},
            {"-417.25741255056", "-9.615706041593941633216225", "-3.782052762020509771702325"},
            {"-550.40864136515", "8.919012908470573845421394", "-6.348853948444995429294966"}};
    test_vector_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 11, 25, 24, 38, 24);
    test_vector_vector<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 14, 11, 25, 24, 38, 24);
    test_vector_const<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 11, 25, 24, 38, 24);
    test_vector_const<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 14, 11, 25, 24, 38, 24);
    test_const_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 11, 25, 24, 38, 24);
    test_const_vector<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 14, 11, 25, 24, 38, 24);
    test_const_const<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 11, 25, 24, 38, 24);
    test_const_const<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 14, 11, 25, 24, 38, 24);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p7s2_mod_decimal32p3s2_eq_decimal32p9s2) {
    DecimalTestCaseArray test_cases = {{"99999.99", "9.99", "0.09"},
                                       {"99999.99", "-9.99", "0.09"},
                                       {"99999.99", "0", "0"},
                                       {"99999.99", "0.01", "0"},
                                       {"99999.99", "-0.01", "0"},
                                       {"-99999.99", "9.99", "-0.09"},
                                       {"-99999.99", "-9.99", "-0.09"},
                                       {"-99999.99", "0", "0"},
                                       {"-99999.99", "0.01", "0"},
                                       {"-99999.99", "-0.01", "0"},
                                       {"0", "9.99", "0"},
                                       {"0", "-9.99", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.01", "0"},
                                       {"0", "-0.01", "0"},
                                       {"0.01", "9.99", "0.01"},
                                       {"0.01", "-9.99", "0.01"},
                                       {"0.01", "0", "0"},
                                       {"0.01", "0.01", "0"},
                                       {"0.01", "-0.01", "0"},
                                       {"-0.01", "9.99", "-0.01"},
                                       {"-0.01", "-9.99", "-0.01"},
                                       {"-0.01", "0", "0"},
                                       {"-0.01", "0.01", "0"},
                                       {"-0.01", "-0.01", "0"},
                                       {"51659.92", "-2.18", "0.46"},
                                       {"84310.17", "8.66", "5.07"},
                                       {"-39838.40", "-2.94", "-1.40"},
                                       {"-56316.48", "-9.55", "-0.13"},
                                       {"-52111.74", "9.42", "-0.30"},
                                       {"-37779.44", "1.24", "-0.36"},
                                       {"78029.95", "-4.66", "2.91"},
                                       {"18087.56", "1.69", "1.18"},
                                       {"73924.07", "-7.31", "5.35"},
                                       {"87170.69", "9.71", "4.02"},
                                       {"-70463.43", "-6.76", "-3.95"},
                                       {"-73763.31", "4", "-3.31"},
                                       {"-28328.85", "-5.27", "-2.60"},
                                       {"-90204.25", "-9.53", "-2.80"},
                                       {"48184.27", "4.11", "2.74"},
                                       {"-54560.47", "-7.09", "-2.92"},
                                       {"13466.80", "9.22", "5.60"},
                                       {"-88487.59", "1.71", "-0.22"},
                                       {"-62357.07", "-4.26", "-3.45"},
                                       {"-34972.69", "-0.71", "-0.22"}};
    test_vector_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 9, 2);
    test_vector_vector<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 9, 2);
    test_vector_const<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 9, 2);
    test_vector_const<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 9, 2);
    test_const_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 9, 2);
    test_const_vector<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 9, 2);
    test_const_const<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 7, 2, 3, 2, 9, 2);
    test_const_const<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 7, 2, 3, 2, 9, 2);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p11s8_mod_decimal64p9s7_eq_decimal64p18s8) {
    DecimalTestCaseArray test_cases = {{"999.99999999", "99.9999999", "0.00000099"},
                                       {"999.99999999", "-99.9999999", "0.00000099"},
                                       {"999.99999999", "0", "0"},
                                       {"999.99999999", "0.0000001", "0.00000009"},
                                       {"999.99999999", "-0.0000001", "0.00000009"},
                                       {"-999.99999999", "99.9999999", "-0.00000099"},
                                       {"-999.99999999", "-99.9999999", "-0.00000099"},
                                       {"-999.99999999", "0", "0"},
                                       {"-999.99999999", "0.0000001", "-0.00000009"},
                                       {"-999.99999999", "-0.0000001", "-0.00000009"},
                                       {"0", "99.9999999", "0"},
                                       {"0", "-99.9999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.0000001", "0"},
                                       {"0", "-0.0000001", "0"},
                                       {"0.00000001", "99.9999999", "0.00000001"},
                                       {"0.00000001", "-99.9999999", "0.00000001"},
                                       {"0.00000001", "0", "0"},
                                       {"0.00000001", "0.0000001", "0.00000001"},
                                       {"0.00000001", "-0.0000001", "0.00000001"},
                                       {"-0.00000001", "99.9999999", "-0.00000001"},
                                       {"-0.00000001", "-99.9999999", "-0.00000001"},
                                       {"-0.00000001", "0", "0"},
                                       {"-0.00000001", "0.0000001", "-0.00000001"},
                                       {"-0.00000001", "-0.0000001", "-0.00000001"},
                                       {"-974.85658644", "1.5525002", "-1.43896104"},
                                       {"-329.27557774", "62.2307175", "-18.12199024"},
                                       {"786.48213820", "-66.7088564", "52.68471780"},
                                       {"905.41417923", "-2.6483455", "2.32836373"},
                                       {"228.40340027", "51.9634237", "20.54970547"},
                                       {"249.72550657", "-1.4376515", "1.01179707"},
                                       {"-322.36371695", "-67.0996161", "-53.96525255"},
                                       {"-478.82974461", "92.2087705", "-17.78589211"},
                                       {"-224.84221621", "-61.2145564", "-41.19854701"},
                                       {"356.24767655", "78.1719466", "43.55989015"},
                                       {"465.03872240", "70.7989313", "40.24513460"},
                                       {"-76.70662335", "-17.5710564", "-6.42239775"},
                                       {"853.48233916", "28.2478729", "6.04615216"},
                                       {"-12.89575963", "12.5062872", "-0.38947243"},
                                       {"834.36831997", "-8.1835727", "7.82747727"},
                                       {"311.18649347", "97.2092606", "19.55871167"},
                                       {"-820.80217804", "-98.8697330", "-29.84431404"},
                                       {"519.39882254", "95.9184850", "39.80639754"},
                                       {"-144.80040473", "-70.5479193", "-3.70456613"},
                                       {"-621.54729545", "-76.2382137", "-11.64158585"}};
    test_vector_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 8, 9, 7, 18, 8);
    test_vector_vector<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 11, 8, 9, 7, 18, 8);
    test_vector_const<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 8, 9, 7, 18, 8);
    test_vector_const<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 11, 8, 9, 7, 18, 8);
    test_const_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 8, 9, 7, 18, 8);
    test_const_vector<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 11, 8, 9, 7, 18, 8);
    test_const_const<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 8, 9, 7, 18, 8);
    test_const_const<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 11, 8, 9, 7, 18, 8);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p37s10_mod_decimal128p19s0_eq_decimal128p38s10) {
    DecimalTestCaseArray test_cases = {
            {"999999999999999999999999999.9999999999", "9999999999999999999", "99999999.9999999999"},
            {"999999999999999999999999999.9999999999", "-9999999999999999999", "99999999.9999999999"},
            {"999999999999999999999999999.9999999999", "0", "0"},
            {"999999999999999999999999999.9999999999", "1", "0.9999999999"},
            {"999999999999999999999999999.9999999999", "-1", "0.9999999999"},
            {"-999999999999999999999999999.9999999999", "9999999999999999999", "-99999999.9999999999"},
            {"-999999999999999999999999999.9999999999", "-9999999999999999999", "-99999999.9999999999"},
            {"-999999999999999999999999999.9999999999", "0", "0"},
            {"-999999999999999999999999999.9999999999", "1", "-0.9999999999"},
            {"-999999999999999999999999999.9999999999", "-1", "-0.9999999999"},
            {"0", "9999999999999999999", "0"},
            {"0", "-9999999999999999999", "0"},
            {"0", "0", "0"},
            {"0", "1", "0"},
            {"0", "-1", "0"},
            {"0.0000000001", "9999999999999999999", "0.0000000001"},
            {"0.0000000001", "-9999999999999999999", "0.0000000001"},
            {"0.0000000001", "0", "0"},
            {"0.0000000001", "1", "0.0000000001"},
            {"0.0000000001", "-1", "0.0000000001"},
            {"-0.0000000001", "9999999999999999999", "-0.0000000001"},
            {"-0.0000000001", "-9999999999999999999", "-0.0000000001"},
            {"-0.0000000001", "0", "0"},
            {"-0.0000000001", "1", "-0.0000000001"},
            {"-0.0000000001", "-1", "-0.0000000001"},
            {"-875615534974729688677938742.5210121960", "4997490071265754974", "-2248781217983126302.5210121960"},
            {"880445859769860488930618951.6527431568", "-9325790596905871512", "2538327284962688567.6527431568"},
            {"-265114755584793180382157613.0130101710", "-3717254800619808988", "-1948905979805256069.0130101710"},
            {"551636683718265308048222615.6327012149", "-8697064308125457335", "1941757066491344085.6327012149"},
            {"139775972631884515717977943.8002971933", "511536957226280401", "430772855277704878.8002971933"},
            {"593058886808149460386880886.4852826908", "7912834019583759933", "654042511341102948.4852826908"},
            {"-169011300946106414152286268.7103474302", "2531956269987239357", "-1808039033291313521.7103474302"},
            {"-350887977196964822469624377.3501216538", "554539156987406273", "-41859049753455196.3501216538"},
            {"-632470249745796639557053675.0793107477", "-4315982569887831184", "-731324058030762923.0793107477"},
            {"927046263593346229674230069.2286967321", "-1683624500409390027", "1131830719146446699.2286967321"},
            {"211183346616547322085354329.2821196498", "4203272407885750254", "3929561910602233421.2821196498"},
            {"-97601821741373740683894254.1472458475", "-6484210811026281846", "-2213741232345641366.1472458475"},
            {"-54637204957608219721633118.4937781481", "9333655055615532285", "-4331526906680216678.4937781481"},
            {"662111092365481097162841592.0007435055", "2211753976557554179", "1709956132202107582.0007435055"},
            {"715707066380710156349771371.5542660191", "-6808997459218053116", "4616277985160957127.5542660191"},
            {"-590350285989584077632146305.4030805169", "3993599143938909816", "-2910462256383674569.4030805169"},
            {"-854628397307273573869220881.0298065948", "-3696501993317824849", "-1241237290094678856.0298065948"},
            {"703705951470571265241650745.6103595237", "-440292678426069664", "179896338935711609.6103595237"},
            {"-448396464273958359356356812.7270470068", "-1103614251073279439", "-433593374512676914.7270470068"},
            {"-516945573350517038940528510.3300588707", "703292555006229313", "-428755557624421724.3300588707"}};
    test_vector_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 37, 10, 19, 0, 38, 10);
    test_vector_vector<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 37, 10, 19, 0, 38, 10);
    test_vector_const<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 37, 10, 19, 0, 38, 10);
    test_vector_const<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 37, 10, 19, 0, 38, 10);
    test_const_vector<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 37, 10, 19, 0, 38, 10);
    test_const_vector<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 37, 10, 19, 0, 38, 10);
    test_const_const<TYPE_DECIMAL128, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 37, 10, 19, 0, 38, 10);
    test_const_const<TYPE_DECIMAL128, ModOp, OverflowMode::IGNORE>(test_cases, 37, 10, 19, 0, 38, 10);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p9s3_mod_decimal32p3s0_eq_decimal32p9s3) {
    DecimalTestCaseArray test_cases = {{"999999.999", "999", "0.999"},
                                       {"999999.999", "-999", "0.999"},
                                       {"999999.999", "0", "0"},
                                       {"999999.999", "1", "0.999"},
                                       {"999999.999", "-1", "0.999"},
                                       {"-999999.999", "999", "-0.999"},
                                       {"-999999.999", "-999", "-0.999"},
                                       {"-999999.999", "0", "0"},
                                       {"-999999.999", "1", "-0.999"},
                                       {"-999999.999", "-1", "-0.999"},
                                       {"0", "999", "0"},
                                       {"0", "-999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "1", "0"},
                                       {"0", "-1", "0"},
                                       {"0.001", "999", "0.001"},
                                       {"0.001", "-999", "0.001"},
                                       {"0.001", "0", "0"},
                                       {"0.001", "1", "0.001"},
                                       {"0.001", "-1", "0.001"},
                                       {"-0.001", "999", "-0.001"},
                                       {"-0.001", "-999", "-0.001"},
                                       {"-0.001", "0", "0"},
                                       {"-0.001", "1", "-0.001"},
                                       {"-0.001", "-1", "-0.001"},
                                       {"-32977.938", "-759", "-340.938"},
                                       {"207067.857", "-396", "355.857"},
                                       {"620851.584", "444", "139.584"},
                                       {"81347.829", "12", "11.829"},
                                       {"-199332.035", "-119", "-7.035"},
                                       {"950990.893", "-618", "506.893"},
                                       {"-734245.927", "330", "-325.927"},
                                       {"938771.769", "-276", "95.769"},
                                       {"-704006.093", "171", "-170.093"},
                                       {"-670143.712", "-844", "-7.712"},
                                       {"75319.886", "51", "43.886"},
                                       {"-728547.261", "254", "-75.261"},
                                       {"1364.384", "-521", "322.384"},
                                       {"-5297.578", "868", "-89.578"},
                                       {"-407194.174", "-240", "-154.174"},
                                       {"-54290.864", "473", "-368.864"},
                                       {"-768442.299", "921", "-328.299"},
                                       {"712934.753", "624", "326.753"},
                                       {"802450.008", "-288", "82.008"},
                                       {"-408672.100", "554", "-374.100"}};
    test_vector_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 9, 3);
    test_vector_vector<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 9, 3);
    test_vector_const<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 9, 3);
    test_vector_const<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 9, 3);
    test_const_vector<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 9, 3);
    test_const_vector<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 9, 3);
    test_const_const<TYPE_DECIMAL32, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 3, 3, 0, 9, 3);
    test_const_const<TYPE_DECIMAL32, ModOp, OverflowMode::IGNORE>(test_cases, 9, 3, 3, 0, 9, 3);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p17s5_mod_decimal64p11s0_eq_decimal64p18s5) {
    DecimalTestCaseArray test_cases = {{"999999999999.99999", "99999999999", "9.99999"},
                                       {"999999999999.99999", "-99999999999", "9.99999"},
                                       {"999999999999.99999", "0", "0"},
                                       {"999999999999.99999", "1", "0.99999"},
                                       {"999999999999.99999", "-1", "0.99999"},
                                       {"-999999999999.99999", "99999999999", "-9.99999"},
                                       {"-999999999999.99999", "-99999999999", "-9.99999"},
                                       {"-999999999999.99999", "0", "0"},
                                       {"-999999999999.99999", "1", "-0.99999"},
                                       {"-999999999999.99999", "-1", "-0.99999"},
                                       {"0", "99999999999", "0"},
                                       {"0", "-99999999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "1", "0"},
                                       {"0", "-1", "0"},
                                       {"0.00001", "99999999999", "0.00001"},
                                       {"0.00001", "-99999999999", "0.00001"},
                                       {"0.00001", "0", "0"},
                                       {"0.00001", "1", "0.00001"},
                                       {"0.00001", "-1", "0.00001"},
                                       {"-0.00001", "99999999999", "-0.00001"},
                                       {"-0.00001", "-99999999999", "-0.00001"},
                                       {"-0.00001", "0", "0"},
                                       {"-0.00001", "1", "-0.00001"},
                                       {"-0.00001", "-1", "-0.00001"},
                                       {"273816572986.28723", "-67131144761", "5291993942.28723"},
                                       {"616230231154.08421", "-85482357744", "17853726946.08421"},
                                       {"390447198198.97269", "-17411199769", "7400803280.97269"},
                                       {"245904000913.47972", "57269475521", "16826098829.47972"},
                                       {"-646775677543.43313", "-71172667592", "-6221669215.43313"},
                                       {"219850980715.75653", "6317025322", "5072119767.75653"},
                                       {"-387278726856.99573", "-15405433223", "-2142896281.99573"},
                                       {"-816355878162.49528", "86241348072", "-40183745514.49528"},
                                       {"-107840070282.97203", "-85338421892", "-22501648390.97203"},
                                       {"-562751573974.23879", "16688786244", "-12021627922.23879"},
                                       {"-594084527630.81600", "-86641350766", "-74236423034.81600"},
                                       {"451892037927.84077", "55472688367", "8110530991.84077"},
                                       {"234707358623.33501", "90714602556", "53278153511.33501"},
                                       {"500818875548.99361", "40092219346", "19712243396.99361"},
                                       {"-319533104283.93730", "50526193623", "-16375942545.93730"},
                                       {"53685597398.03118", "-42598051722", "11087545676.03118"},
                                       {"391075473605.41712", "50604061995", "36847039640.41712"},
                                       {"901467078994.55401", "-38003812277", "27379396623.55401"},
                                       {"780876971551.84693", "-71583534641", "65041625141.84693"},
                                       {"966612870052.51187", "-5842683459", "2570099317.51187"}};
    test_vector_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 5, 11, 0, 18, 5);
    test_vector_vector<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 17, 5, 11, 0, 18, 5);
    test_vector_const<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 5, 11, 0, 18, 5);
    test_vector_const<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 17, 5, 11, 0, 18, 5);
    test_const_vector<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 5, 11, 0, 18, 5);
    test_const_vector<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 17, 5, 11, 0, 18, 5);
    test_const_const<TYPE_DECIMAL64, ModOp, OverflowMode::OUTPUT_NULL>(test_cases, 17, 5, 11, 0, 18, 5);
    test_const_const<TYPE_DECIMAL64, ModOp, OverflowMode::IGNORE>(test_cases, 17, 5, 11, 0, 18, 5);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p14s7_mul_decimal128p15s13_eq_decimal128p38s20) {
    DecimalTestCaseArray test_cases = {{"9999999.9999999", "99.9999999999999", "999999999.99998900000000000001"},
                                       {"9999999.9999999", "-99.9999999999999", "-999999999.99998900000000000001"},
                                       {"9999999.9999999", "0", "0"},
                                       {"9999999.9999999", "0.0000000000001", "0.00000099999999999999"},
                                       {"9999999.9999999", "-0.0000000000001", "-0.00000099999999999999"},
                                       {"-9999999.9999999", "99.9999999999999", "-999999999.99998900000000000001"},
                                       {"-9999999.9999999", "-99.9999999999999", "999999999.99998900000000000001"},
                                       {"-9999999.9999999", "0", "0"},
                                       {"-9999999.9999999", "0.0000000000001", "-0.00000099999999999999"},
                                       {"-9999999.9999999", "-0.0000000000001", "0.00000099999999999999"},
                                       {"0", "99.9999999999999", "0"},
                                       {"0", "-99.9999999999999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.0000000000001", "0"},
                                       {"0", "-0.0000000000001", "0"},
                                       {"0.0000001", "99.9999999999999", "0.00000999999999999999"},
                                       {"0.0000001", "-99.9999999999999", "-0.00000999999999999999"},
                                       {"0.0000001", "0", "0"},
                                       {"0.0000001", "0.0000000000001", "0.00000000000000000001"},
                                       {"0.0000001", "-0.0000000000001", "-0.00000000000000000001"},
                                       {"-0.0000001", "99.9999999999999", "-0.00000999999999999999"},
                                       {"-0.0000001", "-99.9999999999999", "0.00000999999999999999"},
                                       {"-0.0000001", "0", "0"},
                                       {"-0.0000001", "0.0000000000001", "-0.00000000000000000001"},
                                       {"-0.0000001", "-0.0000000000001", "0.00000000000000000001"},
                                       {"2678031.0224646", "99.9949562312059", "267789594.87715926133444806114"},
                                       {"-4608176.0170627", "42.7982975932170", "-197222088.54017485453578370590"},
                                       {"9434596.9745012", "-72.7906926470257", "-686750848.61947541447492108084"},
                                       {"-9049063.1977138", "-72.5508926109920", "656517612.28741377200715008960"},
                                       {"-2902077.9735825", "3.5588144112629", "-10327956.91499403459666233925"},
                                       {"8964075.7347425", "40.3733018843296", "361909335.75275261886704112800"},
                                       {"7134896.2383359", "37.9055644088873", "270452268.91296916962092264407"},
                                       {"-3975033.1731631", "60.9529650850964", "-242290058.21591038650825642284"},
                                       {"6969015.2198327", "94.2899411198016", "657108034.74102648663100919232"},
                                       {"8311646.0654193", "-62.9147804900805", "-522925387.71709652681772325365"},
                                       {"2790423.8333721", "-34.5188362791990", "-96322183.45374639072925694790"},
                                       {"-9826601.2663643", "-47.0742926237650", "462580303.50989277548932758950"},
                                       {"-7946214.4574575", "24.3302296794666", "-193333222.83223905304699216950"},
                                       {"3062671.8494665", "-54.4081017840837", "-166634161.71702120373203634605"},
                                       {"-6136215.6622071", "82.5319154393578", "-506433632.15093930326935460038"},
                                       {"2065561.9424535", "38.6907501356945", "79918141.00526815012525645575"},
                                       {"8730053.6846735", "-28.4705162871039", "-248549135.61678829660513207665"},
                                       {"2135270.8932478", "-59.4856253344513", "-127017924.34329778897388793214"},
                                       {"-5117146.3353753", "90.7328324270471", "-464293180.95228525464724927663"},
                                       {"-114625.2118252", "-64.7847660599647", "7425967.53266948134273457044"}};
    test_vector_vector<TYPE_DECIMAL128, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 7, 15, 13, 38, 20);
    test_vector_vector<TYPE_DECIMAL128, MulOp, OverflowMode::IGNORE>(test_cases, 14, 7, 15, 13, 38, 20);
    test_vector_const<TYPE_DECIMAL128, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 7, 15, 13, 38, 20);
    test_vector_const<TYPE_DECIMAL128, MulOp, OverflowMode::IGNORE>(test_cases, 14, 7, 15, 13, 38, 20);
    test_const_vector<TYPE_DECIMAL128, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 7, 15, 13, 38, 20);
    test_const_vector<TYPE_DECIMAL128, MulOp, OverflowMode::IGNORE>(test_cases, 14, 7, 15, 13, 38, 20);
    test_const_const<TYPE_DECIMAL128, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 7, 15, 13, 38, 20);
    test_const_const<TYPE_DECIMAL128, MulOp, OverflowMode::IGNORE>(test_cases, 14, 7, 15, 13, 38, 20);
    test_const_nullable_vector<TYPE_DECIMAL128, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 7, 15, 13, 38, 20);
    test_nullable_vector_const<TYPE_DECIMAL128, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 7, 15, 13, 38, 20);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 14, 7, 15, 13,
                                                                                            38, 20);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p5s2_mul_decimal32p5s1_eq_decimal32p9s3) {
    DecimalTestCaseArray test_cases = {{"999.99", "9999.9", "1409865.409"},
                                       {"999.99", "-9999.9", "-1409865.409"},
                                       {"999.99", "0", "0"},
                                       {"999.99", "0.1", "99.999"},
                                       {"999.99", "-0.1", "-99.999"},
                                       {"-999.99", "9999.9", "-1409865.409"},
                                       {"-999.99", "-9999.9", "1409865.409"},
                                       {"-999.99", "0", "0"},
                                       {"-999.99", "0.1", "-99.999"},
                                       {"-999.99", "-0.1", "99.999"},
                                       {"0", "9999.9", "0"},
                                       {"0", "-9999.9", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.1", "0"},
                                       {"0", "-0.1", "0"},
                                       {"0.01", "9999.9", "99.999"},
                                       {"0.01", "-9999.9", "-99.999"},
                                       {"0.01", "0", "0"},
                                       {"0.01", "0.1", "0.001"},
                                       {"0.01", "-0.1", "-0.001"},
                                       {"-0.01", "9999.9", "-99.999"},
                                       {"-0.01", "-9999.9", "99.999"},
                                       {"-0.01", "0", "0"},
                                       {"-0.01", "0.1", "-0.001"},
                                       {"-0.01", "-0.1", "0.001"},
                                       {"919.29", "2746.5", "-1770137.311"},
                                       {"-59.99", "8841.7", "-530413.583"},
                                       {"830.53", "-8985.5", "1127207.277"},
                                       {"933.38", "2408.3", "-2047108.242"},
                                       {"289.96", "6167", "1788183.320"},
                                       {"-673.49", "-9675.8", "-2073380.050"},
                                       {"364.13", "-1612", "-586977.560"},
                                       {"-469.75", "6735.3", "1131060.121"},
                                       {"939.26", "7138.9", "-1884651.378"},
                                       {"326.21", "4655.8", "1518768.518"},
                                       {"513.97", "-335.9", "-172642.523"},
                                       {"-92.79", "4963", "-460516.770"},
                                       {"-438.62", "-8700.3", "-478841.710"},
                                       {"396.62", "223.1", "88485.922"},
                                       {"548.40", "6463.1", "-750603.256"},
                                       {"694.65", "-7967.5", "-1239656.579"},
                                       {"-270.30", "-431.6", "116661.480"},
                                       {"704.65", "-7497.9", "-988427.939"},
                                       {"662.39", "4582.8", "-1259366.404"},
                                       {"-546.47", "4504.4", "1833447.828"}};
    test_vector_vector<TYPE_DECIMAL32, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 2, 5, 1, 9, 3);
    test_vector_vector<TYPE_DECIMAL32, MulOp, OverflowMode::IGNORE>(test_cases, 5, 2, 5, 1, 9, 3);
    test_vector_const<TYPE_DECIMAL32, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 2, 5, 1, 9, 3);
    test_vector_const<TYPE_DECIMAL32, MulOp, OverflowMode::IGNORE>(test_cases, 5, 2, 5, 1, 9, 3);
    test_const_vector<TYPE_DECIMAL32, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 2, 5, 1, 9, 3);
    test_const_vector<TYPE_DECIMAL32, MulOp, OverflowMode::IGNORE>(test_cases, 5, 2, 5, 1, 9, 3);
    test_const_const<TYPE_DECIMAL32, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 2, 5, 1, 9, 3);
    test_const_const<TYPE_DECIMAL32, MulOp, OverflowMode::IGNORE>(test_cases, 5, 2, 5, 1, 9, 3);
    test_const_nullable_vector<TYPE_DECIMAL32, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 2, 5, 1, 9, 3);
    test_nullable_vector_const<TYPE_DECIMAL32, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 2, 5, 1, 9, 3);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL32, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 5, 2, 5, 1, 9,
                                                                                           3);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p16s4_mul_decimal64p4s3_eq_decimal64p18s7) {
    DecimalTestCaseArray test_cases = {{"999999999999.9999", "9.999", "775627963145.2231921"},
                                       {"999999999999.9999", "-9.999", "-775627963145.2231921"},
                                       {"999999999999.9999", "0", "0"},
                                       {"999999999999.9999", "0.001", "999999999.9999999"},
                                       {"999999999999.9999", "-0.001", "-999999999.9999999"},
                                       {"-999999999999.9999", "9.999", "-775627963145.2231921"},
                                       {"-999999999999.9999", "-9.999", "775627963145.2231921"},
                                       {"-999999999999.9999", "0", "0"},
                                       {"-999999999999.9999", "0.001", "-999999999.9999999"},
                                       {"-999999999999.9999", "-0.001", "999999999.9999999"},
                                       {"0", "9.999", "0"},
                                       {"0", "-9.999", "0"},
                                       {"0", "0", "0"},
                                       {"0", "0.001", "0"},
                                       {"0", "-0.001", "0"},
                                       {"0.0001", "9.999", "0.0009999"},
                                       {"0.0001", "-9.999", "-0.0009999"},
                                       {"0.0001", "0", "0"},
                                       {"0.0001", "0.001", "0.0000001"},
                                       {"0.0001", "-0.001", "-0.0000001"},
                                       {"-0.0001", "9.999", "-0.0009999"},
                                       {"-0.0001", "-9.999", "0.0009999"},
                                       {"-0.0001", "0", "0"},
                                       {"-0.0001", "0.001", "-0.0000001"},
                                       {"-0.0001", "-0.001", "0.0000001"},
                                       {"-799482941617.1783", "-2.692", "307533671462.4888220"},
                                       {"613000911568.8626", "8.432", "-365199535764.2160416"},
                                       {"-26417648359.1194", "-7.115", "187961568075.1345310"},
                                       {"801115434246.7598", "5.900", "-807442160056.9826648"},
                                       {"209714368454.8203", "6.225", "-539202463739.6987941"},
                                       {"39037506175.8679", "8.873", "346379792298.4758767"},
                                       {"427192426295.8897", "6.452", "911571127090.1251828"},
                                       {"272765628320.9553", "6.185", "-157618996205.8466311"},
                                       {"-71758035209.5468", "-9.802", "703372261123.9777336"},
                                       {"308972100363.1980", "-4.365", "496011189285.5958916"},
                                       {"-16235660626.8011", "-4.404", "71501849400.4320444"},
                                       {"415454489339.0295", "7.346", "-637420136057.3996162"},
                                       {"-778659477819.1396", "-1.578", "-615949751372.3528728"},
                                       {"849975365452.3492", "-4.924", "-495929884745.4571376"},
                                       {"840955195299.5452", "1.793", "-336841742198.8706180"},
                                       {"375676172980.4831", "5.886", "366555546792.1683650"},
                                       {"-461656510520.2279", "-0.640", "295460166732.9458560"},
                                       {"670097572887.8554", "3.989", "828344810878.7000290"},
                                       {"736082785867.9490", "6.370", "-845175876134.0303548"},
                                       {"-381480307337.5364", "7.793", "716472779660.4891580"}};
    test_vector_vector<TYPE_DECIMAL64, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 16, 4, 4, 3, 18, 7);
    test_vector_vector<TYPE_DECIMAL64, MulOp, OverflowMode::IGNORE>(test_cases, 16, 4, 4, 3, 18, 7);
    test_vector_const<TYPE_DECIMAL64, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 16, 4, 4, 3, 18, 7);
    test_vector_const<TYPE_DECIMAL64, MulOp, OverflowMode::IGNORE>(test_cases, 16, 4, 4, 3, 18, 7);
    test_const_vector<TYPE_DECIMAL64, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 16, 4, 4, 3, 18, 7);
    test_const_vector<TYPE_DECIMAL64, MulOp, OverflowMode::IGNORE>(test_cases, 16, 4, 4, 3, 18, 7);
    test_const_const<TYPE_DECIMAL64, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 16, 4, 4, 3, 18, 7);
    test_const_const<TYPE_DECIMAL64, MulOp, OverflowMode::IGNORE>(test_cases, 16, 4, 4, 3, 18, 7);
    test_const_nullable_vector<TYPE_DECIMAL64, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 16, 4, 4, 3, 18, 7);
    test_nullable_vector_const<TYPE_DECIMAL64, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 16, 4, 4, 3, 18, 7);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL64, MulOp, OverflowMode::OUTPUT_NULL>(test_cases, 16, 4, 4, 3, 18,
                                                                                           7);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p31s6_sub_decimal128p34s8_eq_decimal128p38s8) {
    DecimalTestCaseArray test_cases = {
            {"9999999999999999999999999.999999", "99999999999999999999999999.99999999",
             "-90000000000000000000000000.00000099"},
            {"9999999999999999999999999.999999", "-99999999999999999999999999.99999999",
             "109999999999999999999999999.99999899"},
            {"9999999999999999999999999.999999", "0", "9999999999999999999999999.99999900"},
            {"9999999999999999999999999.999999", "0.00000001", "9999999999999999999999999.99999899"},
            {"9999999999999999999999999.999999", "-0.00000001", "9999999999999999999999999.99999901"},
            {"-9999999999999999999999999.999999", "99999999999999999999999999.99999999",
             "-109999999999999999999999999.99999899"},
            {"-9999999999999999999999999.999999", "-99999999999999999999999999.99999999",
             "90000000000000000000000000.00000099"},
            {"-9999999999999999999999999.999999", "0", "-9999999999999999999999999.99999900"},
            {"-9999999999999999999999999.999999", "0.00000001", "-9999999999999999999999999.99999901"},
            {"-9999999999999999999999999.999999", "-0.00000001", "-9999999999999999999999999.99999899"},
            {"0", "99999999999999999999999999.99999999", "-99999999999999999999999999.99999999"},
            {"0", "-99999999999999999999999999.99999999", "99999999999999999999999999.99999999"},
            {"0", "0", "0"},
            {"0", "0.00000001", "-0.00000001"},
            {"0", "-0.00000001", "0.00000001"},
            {"0.000001", "99999999999999999999999999.99999999", "-99999999999999999999999999.99999899"},
            {"0.000001", "-99999999999999999999999999.99999999", "100000000000000000000000000.00000099"},
            {"0.000001", "0", "0.00000100"},
            {"0.000001", "0.00000001", "0.00000099"},
            {"0.000001", "-0.00000001", "0.00000101"},
            {"-0.000001", "99999999999999999999999999.99999999", "-100000000000000000000000000.00000099"},
            {"-0.000001", "-99999999999999999999999999.99999999", "99999999999999999999999999.99999899"},
            {"-0.000001", "0", "-0.00000100"},
            {"-0.000001", "0.00000001", "-0.00000101"},
            {"-0.000001", "-0.00000001", "-0.00000099"},
            {"7139467396451956034010135.040140", "-58459729309984018987092546.41215784",
             "65599196706435975021102681.45229784"},
            {"6686686500611623626317066.265686", "-45701848047257745998799199.22472185",
             "52388534547869369625116265.49040785"},
            {"-9055454316221018160092654.024851", "83078576048580611776333725.20048937",
             "-92134030364801629936426379.22534037"},
            {"7232323371240268593688639.684691", "-3375778578783073477522862.10390984",
             "10608101950023342071211501.78860084"},
            {"-1513166140639047288042073.213407", "50754818644140624609466542.91086328",
             "-52267984784779671897508616.12427028"},
            {"-3665854745313780052819802.716006", "-74466118770137498894919221.36670892",
             "70800264024823718842099418.65070292"},
            {"-4006622070555456486871967.173320", "65171477163811556286413146.57361465",
             "-69178099234367012773285113.74693465"},
            {"-9421245340847632547027551.858587", "27042027380155843136283592.02058923",
             "-36463272721003475683311143.87917623"},
            {"-5633146396984735752532852.838369", "-55538330241013390547439041.43608384",
             "49905183844028654794906188.59771484"},
            {"1508224596287441608979316.541403", "-25199849329226903628156596.99625076",
             "26708073925514345237135913.53765376"},
            {"-3365297719323644404897910.364064", "-37960212521618177276445419.86278538",
             "34594914802294532871547509.49872138"},
            {"4141635726938548299337217.666668", "97308607036452914123839830.08074655",
             "-93166971309514365824502612.41407855"},
            {"6993278592058103632738751.875049", "42735944759631547698599171.21742798",
             "-35742666167573444065860419.34237898"},
            {"6356839399202770358789021.902333", "-14383667564416554862142608.12712082",
             "20740506963619325220931630.02945382"},
            {"7356806106301133211882703.666973", "94547603660254581146865987.94908880",
             "-87190797553953447934983284.28211580"},
            {"-3908165106013817737655722.244799", "46894628419365269206936678.78863787",
             "-50802793525379086944592401.03343687"},
            {"3734639676854311419441442.553056", "-40262854893531592184984058.07334717",
             "43997494570385903604425500.62640317"},
            {"-364252295651433463690950.867905", "-8726701571111902069968920.71573203",
             "8362449275460468606277969.84782703"},
            {"1695410368961961846893813.081899", "-55301395592272879068627261.92775009",
             "56996805961234840915521075.00964909"},
            {"5808943082534482268879950.393443", "-38136483080721731398635132.11663572",
             "43945426163256213667515082.51007872"}};
    test_vector_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 31, 6, 34, 8, 38, 8);
    test_vector_vector<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 31, 6, 34, 8, 38, 8);
    test_vector_const<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 31, 6, 34, 8, 38, 8);
    test_vector_const<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 31, 6, 34, 8, 38, 8);
    test_const_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 31, 6, 34, 8, 38, 8);
    test_const_vector<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 31, 6, 34, 8, 38, 8);
    test_const_const<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 31, 6, 34, 8, 38, 8);
    test_const_const<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 31, 6, 34, 8, 38, 8);
    test_const_nullable_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 31, 6, 34, 8, 38, 8);
    test_nullable_vector_const<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 31, 6, 34, 8, 38, 8);
    test_nullable_vector_nullable_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 31, 6, 34, 8,
                                                                                            38, 8);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p4s2_sub_decimal32p6s4_eq_decimal32p9s4) {
    DecimalTestCaseArray test_cases = {{"99.99", "99.9999", "-0.0099"},
                                       {"99.99", "-99.9999", "199.9899"},
                                       {"99.99", "0", "99.9900"},
                                       {"99.99", "0.0001", "99.9899"},
                                       {"99.99", "-0.0001", "99.9901"},
                                       {"-99.99", "99.9999", "-199.9899"},
                                       {"-99.99", "-99.9999", "0.0099"},
                                       {"-99.99", "0", "-99.9900"},
                                       {"-99.99", "0.0001", "-99.9901"},
                                       {"-99.99", "-0.0001", "-99.9899"},
                                       {"0", "99.9999", "-99.9999"},
                                       {"0", "-99.9999", "99.9999"},
                                       {"0", "0", "0"},
                                       {"0", "0.0001", "-0.0001"},
                                       {"0", "-0.0001", "0.0001"},
                                       {"0.01", "99.9999", "-99.9899"},
                                       {"0.01", "-99.9999", "100.0099"},
                                       {"0.01", "0", "0.0100"},
                                       {"0.01", "0.0001", "0.0099"},
                                       {"0.01", "-0.0001", "0.0101"},
                                       {"-0.01", "99.9999", "-100.0099"},
                                       {"-0.01", "-99.9999", "99.9899"},
                                       {"-0.01", "0", "-0.0100"},
                                       {"-0.01", "0.0001", "-0.0101"},
                                       {"-0.01", "-0.0001", "-0.0099"},
                                       {"42.46", "-85.5135", "127.9735"},
                                       {"-12.37", "9.4487", "-21.8187"},
                                       {"-0.76", "-44.9380", "44.1780"},
                                       {"54.37", "-42.9424", "97.3124"},
                                       {"-57.51", "-68.8592", "11.3492"},
                                       {"-0.25", "-97.6520", "97.4020"},
                                       {"2.19", "50.2494", "-48.0594"},
                                       {"78.03", "-28.6147", "106.6447"},
                                       {"-88.62", "38.7760", "-127.3960"},
                                       {"-58.67", "-90.6153", "31.9453"},
                                       {"-75.13", "-87.1785", "12.0485"},
                                       {"-37.92", "25.0426", "-62.9626"},
                                       {"-12.84", "-55.2489", "42.4089"},
                                       {"-86.04", "-5.7304", "-80.3096"},
                                       {"21.87", "-41.5443", "63.4143"},
                                       {"-55.03", "-36.6801", "-18.3499"},
                                       {"62.90", "-33.5025", "96.4025"},
                                       {"-2.59", "-16.5820", "13.9920"},
                                       {"30.01", "-55.3397", "85.3497"},
                                       {"-34.59", "-74.7302", "40.1402"}};
    test_vector_vector<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 4, 2, 6, 4, 9, 4);
    test_vector_vector<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 4, 2, 6, 4, 9, 4);
    test_vector_const<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 4, 2, 6, 4, 9, 4);
    test_vector_const<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 4, 2, 6, 4, 9, 4);
    test_const_vector<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 4, 2, 6, 4, 9, 4);
    test_const_vector<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 4, 2, 6, 4, 9, 4);
    test_const_const<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 4, 2, 6, 4, 9, 4);
    test_const_const<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 4, 2, 6, 4, 9, 4);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p11s5_sub_decimal64p18s12_eq_decimal64p18s12) {
    DecimalTestCaseArray test_cases = {{"999999.99999", "999999.999999999999", "-0.000009999999"},
                                       {"999999.99999", "-999999.999999999999", "1999999.999989999999"},
                                       {"999999.99999", "0", "999999.999990000000"},
                                       {"999999.99999", "0.000000000001", "999999.999989999999"},
                                       {"999999.99999", "-0.000000000001", "999999.999990000001"},
                                       {"-999999.99999", "999999.999999999999", "-1999999.999989999999"},
                                       {"-999999.99999", "-999999.999999999999", "0.000009999999"},
                                       {"-999999.99999", "0", "-999999.999990000000"},
                                       {"-999999.99999", "0.000000000001", "-999999.999990000001"},
                                       {"-999999.99999", "-0.000000000001", "-999999.999989999999"},
                                       {"0", "999999.999999999999", "-999999.999999999999"},
                                       {"0", "-999999.999999999999", "999999.999999999999"},
                                       {"0", "0", "0"},
                                       {"0", "0.000000000001", "-0.000000000001"},
                                       {"0", "-0.000000000001", "0.000000000001"},
                                       {"0.00001", "999999.999999999999", "-999999.999989999999"},
                                       {"0.00001", "-999999.999999999999", "1000000.000009999999"},
                                       {"0.00001", "0", "0.000010000000"},
                                       {"0.00001", "0.000000000001", "0.000009999999"},
                                       {"0.00001", "-0.000000000001", "0.000010000001"},
                                       {"-0.00001", "999999.999999999999", "-1000000.000009999999"},
                                       {"-0.00001", "-999999.999999999999", "999999.999989999999"},
                                       {"-0.00001", "0", "-0.000010000000"},
                                       {"-0.00001", "0.000000000001", "-0.000010000001"},
                                       {"-0.00001", "-0.000000000001", "-0.000009999999"},
                                       {"-591630.14719", "-904719.417370699967", "313089.270180699967"},
                                       {"-700075.13711", "-619719.763837302433", "-80355.373272697567"},
                                       {"-507412.66007", "352839.211665703564", "-860251.871735703564"},
                                       {"-426969.79054", "-198046.534995434971", "-228923.255544565029"},
                                       {"488174.36261", "-910026.288103729739", "1398200.650713729739"},
                                       {"-772896.04458", "451490.569298775229", "-1224386.613878775229"},
                                       {"585459.67685", "974443.915626268728", "-388984.238776268728"},
                                       {"392460.32241", "452823.091841251850", "-60362.769431251850"},
                                       {"-421552.30871", "-121250.587908461260", "-300301.720801538740"},
                                       {"678861.53146", "571460.155510248303", "107401.375949751697"},
                                       {"-196927.83533", "252615.278046287301", "-449543.113376287301"},
                                       {"-285511.61458", "762991.167607809015", "-1048502.782187809015"},
                                       {"468353.60203", "-793625.875075034297", "1261979.477105034297"},
                                       {"-874012.71105", "549679.085118765067", "-1423691.796168765067"},
                                       {"-36625.51638", "-626954.096453750359", "590328.580073750359"},
                                       {"-324996.33776", "970900.009903800839", "-1295896.347663800839"},
                                       {"583163.91064", "4746.705050964415", "578417.205589035585"},
                                       {"-179251.29605", "319444.891069099630", "-498696.187119099630"},
                                       {"113717.68336", "-377670.640222284138", "491388.323582284138"},
                                       {"468626.59505", "121406.960236864241", "347219.634813135759"}};
    test_vector_vector<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 5, 18, 12, 18, 12);
    test_vector_vector<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 11, 5, 18, 12, 18, 12);
    test_vector_const<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 5, 18, 12, 18, 12);
    test_vector_const<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 11, 5, 18, 12, 18, 12);
    test_const_vector<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 5, 18, 12, 18, 12);
    test_const_vector<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 11, 5, 18, 12, 18, 12);
    test_const_const<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 11, 5, 18, 12, 18, 12);
    test_const_const<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 11, 5, 18, 12, 18, 12);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p34s7_sub_decimal128p29s5_eq_decimal128p38s7) {
    DecimalTestCaseArray test_cases = {
            {"999999999999999999999999999.9999999", "999999999999999999999999.99999",
             "999000000000000000000000000.0000099"},
            {"999999999999999999999999999.9999999", "-999999999999999999999999.99999",
             "1000999999999999999999999999.9999899"},
            {"999999999999999999999999999.9999999", "0", "999999999999999999999999999.9999999"},
            {"999999999999999999999999999.9999999", "0.00001", "999999999999999999999999999.9999899"},
            {"999999999999999999999999999.9999999", "-0.00001", "1000000000000000000000000000.0000099"},
            {"-999999999999999999999999999.9999999", "999999999999999999999999.99999",
             "-1000999999999999999999999999.9999899"},
            {"-999999999999999999999999999.9999999", "-999999999999999999999999.99999",
             "-999000000000000000000000000.0000099"},
            {"-999999999999999999999999999.9999999", "0", "-999999999999999999999999999.9999999"},
            {"-999999999999999999999999999.9999999", "0.00001", "-1000000000000000000000000000.0000099"},
            {"-999999999999999999999999999.9999999", "-0.00001", "-999999999999999999999999999.9999899"},
            {"0", "999999999999999999999999.99999", "-999999999999999999999999.9999900"},
            {"0", "-999999999999999999999999.99999", "999999999999999999999999.9999900"},
            {"0", "0", "0"},
            {"0", "0.00001", "-0.0000100"},
            {"0", "-0.00001", "0.0000100"},
            {"0.0000001", "999999999999999999999999.99999", "-999999999999999999999999.9999899"},
            {"0.0000001", "-999999999999999999999999.99999", "999999999999999999999999.9999901"},
            {"0.0000001", "0", "0.0000001"},
            {"0.0000001", "0.00001", "-0.0000099"},
            {"0.0000001", "-0.00001", "0.0000101"},
            {"-0.0000001", "999999999999999999999999.99999", "-999999999999999999999999.9999901"},
            {"-0.0000001", "-999999999999999999999999.99999", "999999999999999999999999.9999899"},
            {"-0.0000001", "0", "-0.0000001"},
            {"-0.0000001", "0.00001", "-0.0000101"},
            {"-0.0000001", "-0.00001", "0.0000099"},
            {"-796494053119004539452151942.0361306", "-40346563284668021823452.56933",
             "-796453706555719871430328489.4668006"},
            {"622748280044523902926941527.7784803", "-338146189668950155675479.56371",
             "623086426234192853082617007.3421903"},
            {"-212890228018179088961615395.8469059", "343433525201230190862214.00556",
             "-213233661543380319152477609.8524659"},
            {"-214406695840823890751202394.6082475", "-562809848140712056631045.76763",
             "-213843885992683178694571348.8406175"},
            {"83073879205974926739272059.6915239", "44585358991732271837133.94998",
             "83029293846983194467434925.7415439"},
            {"390133516828008326302855351.4599815", "-240128150771445892330552.95872",
             "390373644978779772195185904.4187015"},
            {"-596221831087827164006644753.4068653", "-985286851165179783655594.83946",
             "-595236544236661984222989158.5674053"},
            {"-594817999807080312994984837.9366957", "792374427948139644948505.18432",
             "-595610374235028452639933343.1210157"},
            {"-542977504427071506113067711.6282937", "647796334798408330860995.40116",
             "-543625300761869914443928707.0294537"},
            {"339407510964028454546243750.7968518", "930910842915544458740509.97880",
             "338476600121112910087503240.8180518"},
            {"-604867410527246231792505231.6069731", "-695579750023393676021161.56192",
             "-604171830777222838116484070.0450531"},
            {"962310996423351610261498079.4862286", "-944100665783729470652300.20303",
             "963255097089135339732150379.6892586"},
            {"206524899927954537978154573.2210453", "-307384340392288086037568.99510",
             "206832284268346826064192142.2161453"},
            {"-244079119858172136887514205.9148876", "588401844936251672436761.64446",
             "-244667521703108388559950967.5593476"},
            {"379252770713167607051356754.3023250", "-295687088974978832506168.65530",
             "379548457802142585883862922.9576250"},
            {"262749665340562403581607389.1905626", "-743473776504122654284669.68852",
             "263493139117066526235892058.8790826"},
            {"230036323748039402524236742.4355709", "516526724182684918748018.55060",
             "229519797023856717605488723.8849709"},
            {"778974466958982318333405445.8787773", "-285020142884610872163002.43152",
             "779259487101866929205568448.3102973"},
            {"804064472963540954764653100.4666677", "187019207024477150023855.52828",
             "803877453756516477614629244.9383877"},
            {"733803306326475847099651045.2642731", "335471485030510034013807.16863",
             "733467834841445337065637238.0956431"}};
    test_vector_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 34, 7, 29, 5, 38, 7);
    test_vector_vector<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 34, 7, 29, 5, 38, 7);
    test_vector_const<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 34, 7, 29, 5, 38, 7);
    test_vector_const<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 34, 7, 29, 5, 38, 7);
    test_const_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 34, 7, 29, 5, 38, 7);
    test_const_vector<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 34, 7, 29, 5, 38, 7);
    test_const_const<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 34, 7, 29, 5, 38, 7);
    test_const_const<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 34, 7, 29, 5, 38, 7);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p6s5_sub_decimal32p4s2_eq_decimal32p9s5) {
    DecimalTestCaseArray test_cases = {{"9.99999", "99.99", "-89.99001"},
                                       {"9.99999", "-99.99", "109.98999"},
                                       {"9.99999", "0", "9.99999"},
                                       {"9.99999", "0.01", "9.98999"},
                                       {"9.99999", "-0.01", "10.00999"},
                                       {"-9.99999", "99.99", "-109.98999"},
                                       {"-9.99999", "-99.99", "89.99001"},
                                       {"-9.99999", "0", "-9.99999"},
                                       {"-9.99999", "0.01", "-10.00999"},
                                       {"-9.99999", "-0.01", "-9.98999"},
                                       {"0", "99.99", "-99.99000"},
                                       {"0", "-99.99", "99.99000"},
                                       {"0", "0", "0"},
                                       {"0", "0.01", "-0.01000"},
                                       {"0", "-0.01", "0.01000"},
                                       {"0.00001", "99.99", "-99.98999"},
                                       {"0.00001", "-99.99", "99.99001"},
                                       {"0.00001", "0", "0.00001"},
                                       {"0.00001", "0.01", "-0.00999"},
                                       {"0.00001", "-0.01", "0.01001"},
                                       {"-0.00001", "99.99", "-99.99001"},
                                       {"-0.00001", "-99.99", "99.98999"},
                                       {"-0.00001", "0", "-0.00001"},
                                       {"-0.00001", "0.01", "-0.01001"},
                                       {"-0.00001", "-0.01", "0.00999"},
                                       {"-6.55073", "98.10", "-104.65073"},
                                       {"6.29768", "98.03", "-91.73232"},
                                       {"-7.45340", "-74.05", "66.59660"},
                                       {"-3.61408", "-6.95", "3.33592"},
                                       {"4.22035", "28.55", "-24.32965"},
                                       {"-4.59416", "-54.27", "49.67584"},
                                       {"2.27335", "42.18", "-39.90665"},
                                       {"-3.00690", "-76.83", "73.82310"},
                                       {"8.61513", "31.93", "-23.31487"},
                                       {"1.37724", "95.08", "-93.70276"},
                                       {"-1.15981", "78.27", "-79.42981"},
                                       {"6.67180", "72.85", "-66.17820"},
                                       {"-9.51199", "-52.71", "43.19801"},
                                       {"9.67446", "18.67", "-8.99554"},
                                       {"6.08260", "-79.99", "86.07260"},
                                       {"7.52712", "-17.78", "25.30712"},
                                       {"9.85584", "-32.20", "42.05584"},
                                       {"6.20602", "-92.21", "98.41602"},
                                       {"1.32342", "84.60", "-83.27658"},
                                       {"-8.14264", "-76.72", "68.57736"}};
    test_vector_vector<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 5, 4, 2, 9, 5);
    test_vector_vector<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 6, 5, 4, 2, 9, 5);
    test_vector_const<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 5, 4, 2, 9, 5);
    test_vector_const<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 6, 5, 4, 2, 9, 5);
    test_const_vector<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 5, 4, 2, 9, 5);
    test_const_vector<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 6, 5, 4, 2, 9, 5);
    test_const_const<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 6, 5, 4, 2, 9, 5);
    test_const_const<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 6, 5, 4, 2, 9, 5);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p18s13_sub_decimal64p11s6_eq_decimal64p18s13) {
    DecimalTestCaseArray test_cases = {{"99999.9999999999999", "99999.999999", "0.0000009999999"},
                                       {"99999.9999999999999", "-99999.999999", "199999.9999989999999"},
                                       {"99999.9999999999999", "0", "99999.9999999999999"},
                                       {"99999.9999999999999", "0.000001", "99999.9999989999999"},
                                       {"99999.9999999999999", "-0.000001", "100000.0000009999999"},
                                       {"-99999.9999999999999", "99999.999999", "-199999.9999989999999"},
                                       {"-99999.9999999999999", "-99999.999999", "-0.0000009999999"},
                                       {"-99999.9999999999999", "0", "-99999.9999999999999"},
                                       {"-99999.9999999999999", "0.000001", "-100000.0000009999999"},
                                       {"-99999.9999999999999", "-0.000001", "-99999.9999989999999"},
                                       {"0", "99999.999999", "-99999.9999990000000"},
                                       {"0", "-99999.999999", "99999.9999990000000"},
                                       {"0", "0", "0"},
                                       {"0", "0.000001", "-0.0000010000000"},
                                       {"0", "-0.000001", "0.0000010000000"},
                                       {"0.0000000000001", "99999.999999", "-99999.9999989999999"},
                                       {"0.0000000000001", "-99999.999999", "99999.9999990000001"},
                                       {"0.0000000000001", "0", "0.0000000000001"},
                                       {"0.0000000000001", "0.000001", "-0.0000009999999"},
                                       {"0.0000000000001", "-0.000001", "0.0000010000001"},
                                       {"-0.0000000000001", "99999.999999", "-99999.9999990000001"},
                                       {"-0.0000000000001", "-99999.999999", "99999.9999989999999"},
                                       {"-0.0000000000001", "0", "-0.0000000000001"},
                                       {"-0.0000000000001", "0.000001", "-0.0000010000001"},
                                       {"-0.0000000000001", "-0.000001", "0.0000009999999"},
                                       {"10275.2991947632424", "-97869.532253", "108144.8314477632424"},
                                       {"5999.7039892487337", "17001.402050", "-11001.6980607512663"},
                                       {"85682.7928077334264", "-50418.605691", "136101.3984987334264"},
                                       {"18435.0005636390002", "-90612.160446", "109047.1610096390002"},
                                       {"24079.1121520454314", "-6472.330959", "30551.4431110454314"},
                                       {"-85391.8328808396934", "-21969.532130", "-63422.3007508396934"},
                                       {"-19781.4431952080011", "-30834.167295", "11052.7240997919989"},
                                       {"-93420.7044140282483", "-83106.192769", "-10314.5116450282483"},
                                       {"-13810.5968040250537", "88925.463769", "-102736.0605730250537"},
                                       {"-79101.2890336607874", "-38343.555551", "-40757.7334826607874"},
                                       {"39233.1772882184898", "-62486.850204", "101720.0274922184898"},
                                       {"-87958.2608495742309", "-84211.894018", "-3746.3668315742309"},
                                       {"-90809.2446535459542", "-96611.812139", "5802.5674854540458"},
                                       {"-69723.5795110538280", "50033.984427", "-119757.5639380538280"},
                                       {"-1380.3129998765630", "-8409.376104", "7029.0631041234370"},
                                       {"-70962.1421816178207", "-97750.115950", "26787.9737683821793"},
                                       {"25092.1837736443604", "-69706.644325", "94798.8280986443604"},
                                       {"74719.8269072728377", "75169.943624", "-450.1167167271623"},
                                       {"-15781.6385731459534", "-12923.439599", "-2858.1989741459534"},
                                       {"-91298.4606984177119", "14193.444740", "-105491.9054384177119"}};
    test_vector_vector<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 13, 11, 6, 18, 13);
    test_vector_vector<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 13, 11, 6, 18, 13);
    test_vector_const<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 13, 11, 6, 18, 13);
    test_vector_const<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 13, 11, 6, 18, 13);
    test_const_vector<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 13, 11, 6, 18, 13);
    test_const_vector<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 13, 11, 6, 18, 13);
    test_const_const<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 13, 11, 6, 18, 13);
    test_const_const<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 13, 11, 6, 18, 13);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal128p38s37_sub_decimal128p38s37_eq_decimal128p38s37) {
    DecimalTestCaseArray test_cases = {
            {"9.9999999999999999999999999999999999999", "9.9999999999999999999999999999999999999", "0"},
            {"9.9999999999999999999999999999999999999", "-9.9999999999999999999999999999999999999",
             "-14.0282366920938463463374607431768211458"},
            {"9.9999999999999999999999999999999999999", "0", "9.9999999999999999999999999999999999999"},
            {"9.9999999999999999999999999999999999999", "0.0000000000000000000000000000000000001",
             "9.9999999999999999999999999999999999998"},
            {"9.9999999999999999999999999999999999999", "-0.0000000000000000000000000000000000001", "10"},
            {"-9.9999999999999999999999999999999999999", "9.9999999999999999999999999999999999999",
             "14.0282366920938463463374607431768211458"},
            {"-9.9999999999999999999999999999999999999", "-9.9999999999999999999999999999999999999", "0"},
            {"-9.9999999999999999999999999999999999999", "0", "-9.9999999999999999999999999999999999999"},
            {"-9.9999999999999999999999999999999999999", "0.0000000000000000000000000000000000001", "-10"},
            {"-9.9999999999999999999999999999999999999", "-0.0000000000000000000000000000000000001",
             "-9.9999999999999999999999999999999999998"},
            {"0", "9.9999999999999999999999999999999999999", "-9.9999999999999999999999999999999999999"},
            {"0", "-9.9999999999999999999999999999999999999", "9.9999999999999999999999999999999999999"},
            {"0", "0", "0"},
            {"0", "0.0000000000000000000000000000000000001", "-0.0000000000000000000000000000000000001"},
            {"0", "-0.0000000000000000000000000000000000001", "0.0000000000000000000000000000000000001"},
            {"0.0000000000000000000000000000000000001", "9.9999999999999999999999999999999999999",
             "-9.9999999999999999999999999999999999998"},
            {"0.0000000000000000000000000000000000001", "-9.9999999999999999999999999999999999999", "10"},
            {"0.0000000000000000000000000000000000001", "0", "0.0000000000000000000000000000000000001"},
            {"0.0000000000000000000000000000000000001", "0.0000000000000000000000000000000000001", "0"},
            {"0.0000000000000000000000000000000000001", "-0.0000000000000000000000000000000000001",
             "0.0000000000000000000000000000000000002"},
            {"-0.0000000000000000000000000000000000001", "9.9999999999999999999999999999999999999", "-10"},
            {"-0.0000000000000000000000000000000000001", "-9.9999999999999999999999999999999999999",
             "9.9999999999999999999999999999999999998"},
            {"-0.0000000000000000000000000000000000001", "0", "-0.0000000000000000000000000000000000001"},
            {"-0.0000000000000000000000000000000000001", "0.0000000000000000000000000000000000001",
             "-0.0000000000000000000000000000000000002"},
            {"-0.0000000000000000000000000000000000001", "-0.0000000000000000000000000000000000001", "0"},
            {"9.6949632299437336710470329905199639154", "-6.3085074727396427143710143481887980426",
             "16.0034707026833763854180473387087619580"},
            {"-8.6976800505312898074458720869997192640", "0.0807629086833223240883796804593719122",
             "-8.7784429592146121315342517674590911762"},
            {"-8.4019312949397494962373496877368249511", "-8.2004281235272049460483716508362596025",
             "-0.2015031714125445501889780369005653486"},
            {"-8.9959720596057972560369978589526280893", "2.7141623593765966160441590830656646575",
             "-11.7101344189823938720811569420182927468"},
            {"-5.5477747187944059948117848293355154832", "5.0896676080157671839713296473134819997",
             "-10.6374423268101731787831144766489974829"},
            {"0.1975870193324490938977223889286783931", "-0.1329582714798106671324729797987464495",
             "0.3305452908122597610301953687274248426"},
            {"0.8716323140259831528083113427250164735", "-5.9591179369558432935894170538688834812",
             "6.8307502509818264463977283965938999547"},
            {"-3.1250236744478698650233373332485585681", "1.7758973608315978829743220864199961056",
             "-4.9009210352794677479976594196685546737"},
            {"-9.8027453673728134084028417714954963174", "-7.8092140221015574601674351171609306699",
             "-1.9935313452712559482354066543345656475"},
            {"-4.6772195351772428953462509163688579830", "7.0720165109861677041633114918466984287",
             "-11.7492360461634105995095624082155564117"},
            {"4.8309504156132513185369372792495315559", "2.6684393290857831362380374131118890276",
             "2.1625110865274681822988998661376425283"},
            {"0.3483414624390007914773537184468074115", "8.2429137428377190382141534122614687625",
             "-7.8945722803987182467367996938146613510"},
            {"9.6796643792756486813351290317609698757", "-5.3911190734049934902203761413705116630",
             "15.0707834526806421715555051731314815387"},
            {"-1.0500076890961647726768309208297220985", "-5.5140262753890205159329153981933199004",
             "4.4640185862928557432560844773635978019"},
            {"-9.5050438377199093567807599263801232505", "-7.9936496153477918568260920701718459406",
             "-1.5113942223721174999546678562082773099"},
            {"5.3215929980575719918168220549119584123", "4.1292727423114054692394853563228711759",
             "1.1923202557461665225773366985890872364"},
            {"-5.3990989003043742159373971046162687767", "-0.4106549057024978940256596887480388450",
             "-4.9884439946018763219117374158682299317"},
            {"-9.2564880009774214321051662229102396093", "2.8311230252893970940571690795403821362",
             "-12.0876110262668185261623353024506217455"},
            {"-4.9847682002862134526401831931999877973", "-7.7476186472652163397189438490795350010",
             "2.7628504469790028870787606558795472037"},
            {"-8.7242732380520100061423696468014466798", "-4.7009748792012557916712889114003709319",
             "-4.0232983588507542144710807354010757479"}};
    test_vector_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 37, 38, 37, 38, 37);
    test_vector_vector<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 38, 37, 38, 37, 38, 37);
    test_vector_const<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 37, 38, 37, 38, 37);
    test_vector_const<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 38, 37, 38, 37, 38, 37);
    test_const_vector<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 37, 38, 37, 38, 37);
    test_const_vector<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 38, 37, 38, 37, 38, 37);
    test_const_const<TYPE_DECIMAL128, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 38, 37, 38, 37, 38, 37);
    test_const_const<TYPE_DECIMAL128, SubOp, OverflowMode::IGNORE>(test_cases, 38, 37, 38, 37, 38, 37);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal32p9s2_sub_decimal32p9s2_eq_decimal32p9s2) {
    DecimalTestCaseArray test_cases = {{"9999999.99", "9999999.99", "0"},
                                       {"9999999.99", "-9999999.99", "19999999.98"},
                                       {"9999999.99", "0", "9999999.99"},
                                       {"9999999.99", "0.01", "9999999.98"},
                                       {"9999999.99", "-0.01", "10000000"},
                                       {"-9999999.99", "9999999.99", "-19999999.98"},
                                       {"-9999999.99", "-9999999.99", "0"},
                                       {"-9999999.99", "0", "-9999999.99"},
                                       {"-9999999.99", "0.01", "-10000000"},
                                       {"-9999999.99", "-0.01", "-9999999.98"},
                                       {"0", "9999999.99", "-9999999.99"},
                                       {"0", "-9999999.99", "9999999.99"},
                                       {"0", "0", "0"},
                                       {"0", "0.01", "-0.01"},
                                       {"0", "-0.01", "0.01"},
                                       {"0.01", "9999999.99", "-9999999.98"},
                                       {"0.01", "-9999999.99", "10000000"},
                                       {"0.01", "0", "0.01"},
                                       {"0.01", "0.01", "0"},
                                       {"0.01", "-0.01", "0.02"},
                                       {"-0.01", "9999999.99", "-10000000"},
                                       {"-0.01", "-9999999.99", "9999999.98"},
                                       {"-0.01", "0", "-0.01"},
                                       {"-0.01", "0.01", "-0.02"},
                                       {"-0.01", "-0.01", "0"},
                                       {"-7758165.43", "4655605.16", "-12413770.59"},
                                       {"9142831.37", "7287898.34", "1854933.03"},
                                       {"-5922634.90", "-2998529.20", "-2924105.70"},
                                       {"6593870.19", "3640165.34", "2953704.85"},
                                       {"-9396186.21", "-519251.39", "-8876934.82"},
                                       {"2345063.07", "5077910.42", "-2732847.35"},
                                       {"5271951.98", "6044375.03", "-772423.05"},
                                       {"-2095462.39", "-1334182.52", "-761279.87"},
                                       {"-7858286", "-3125854.96", "-4732431.04"},
                                       {"8406842.52", "-2015187.03", "10422029.55"},
                                       {"-5853836.49", "-4333831.06", "-1520005.43"},
                                       {"2400425.48", "3640860.45", "-1240434.97"},
                                       {"1983493.22", "128418.91", "1855074.31"},
                                       {"-2925084.05", "3428083.86", "-6353167.91"},
                                       {"-8442970.95", "-1249710.70", "-7193260.25"},
                                       {"6042964.77", "-573287.89", "6616252.66"},
                                       {"6471816.75", "-2978793.25", "9450610"},
                                       {"206849.83", "-9297503.07", "9504352.90"},
                                       {"8765765.42", "-6104097.70", "14869863.12"},
                                       {"2314600.13", "-6732808.59", "9047408.72"}};
    test_vector_vector<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_vector_vector<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
    test_vector_const<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_vector_const<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_vector<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_vector<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_const<TYPE_DECIMAL32, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 9, 2, 9, 2, 9, 2);
    test_const_const<TYPE_DECIMAL32, SubOp, OverflowMode::IGNORE>(test_cases, 9, 2, 9, 2, 9, 2);
}
TEST_F(DecimalBinaryFunctionTest, test_decimal64p18s15_sub_decimal64p18s15_eq_decimal64p18s15) {
    DecimalTestCaseArray test_cases = {{"999.999999999999999", "999.999999999999999", "0"},
                                       {"999.999999999999999", "-999.999999999999999", "1999.999999999999998"},
                                       {"999.999999999999999", "0", "999.999999999999999"},
                                       {"999.999999999999999", "0.000000000000001", "999.999999999999998"},
                                       {"999.999999999999999", "-0.000000000000001", "1000"},
                                       {"-999.999999999999999", "999.999999999999999", "-1999.999999999999998"},
                                       {"-999.999999999999999", "-999.999999999999999", "0"},
                                       {"-999.999999999999999", "0", "-999.999999999999999"},
                                       {"-999.999999999999999", "0.000000000000001", "-1000"},
                                       {"-999.999999999999999", "-0.000000000000001", "-999.999999999999998"},
                                       {"0", "999.999999999999999", "-999.999999999999999"},
                                       {"0", "-999.999999999999999", "999.999999999999999"},
                                       {"0", "0", "0"},
                                       {"0", "0.000000000000001", "-0.000000000000001"},
                                       {"0", "-0.000000000000001", "0.000000000000001"},
                                       {"0.000000000000001", "999.999999999999999", "-999.999999999999998"},
                                       {"0.000000000000001", "-999.999999999999999", "1000"},
                                       {"0.000000000000001", "0", "0.000000000000001"},
                                       {"0.000000000000001", "0.000000000000001", "0"},
                                       {"0.000000000000001", "-0.000000000000001", "0.000000000000002"},
                                       {"-0.000000000000001", "999.999999999999999", "-1000"},
                                       {"-0.000000000000001", "-999.999999999999999", "999.999999999999998"},
                                       {"-0.000000000000001", "0", "-0.000000000000001"},
                                       {"-0.000000000000001", "0.000000000000001", "-0.000000000000002"},
                                       {"-0.000000000000001", "-0.000000000000001", "0"},
                                       {"-116.558256503628798", "-828.226308179227363", "711.668051675598565"},
                                       {"-191.893929690433500", "340.334248641039860", "-532.228178331473360"},
                                       {"774.278547748258098", "-960.454578050025307", "1734.733125798283405"},
                                       {"-997.354409659651668", "261.309220212978652", "-1258.663629872630320"},
                                       {"-56.928833797061919", "749.934162777451252", "-806.862996574513171"},
                                       {"641.502093121205296", "-412.017326977685463", "1053.519420098890759"},
                                       {"404.215249012454849", "238.668760193764485", "165.546488818690364"},
                                       {"320.758547794771016", "-239.816651901041860", "560.575199695812876"},
                                       {"-294.572327215313287", "658.991433288758980", "-953.563760504072267"},
                                       {"-704.818699818709817", "-287.636894483567608", "-417.181805335142209"},
                                       {"428.960972361136239", "790.839053953247098", "-361.878081592110859"},
                                       {"-63.616798456581674", "881.114623766748431", "-944.731422223330105"},
                                       {"446.625912965602439", "-782.081593116408141", "1228.707506082010580"},
                                       {"47.095551882918691", "681.173372237510465", "-634.077820354591774"},
                                       {"939.168526740252470", "-178.506604787052902", "1117.675131527305372"},
                                       {"-615.007470482750903", "433.173687448288435", "-1048.181157931039338"},
                                       {"-53.210430099341528", "144.177084753035032", "-197.387514852376560"},
                                       {"87.222692156793347", "209.659799435793034", "-122.437107278999687"},
                                       {"259.513532937364050", "-571.904356334394336", "831.417889271758386"},
                                       {"-57.241918690550156", "-668.401908164319819", "611.159989473769663"}};
    test_vector_vector<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 15, 18, 15, 18, 15);
    test_vector_vector<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 15, 18, 15, 18, 15);
    test_vector_const<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 15, 18, 15, 18, 15);
    test_vector_const<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 15, 18, 15, 18, 15);
    test_const_vector<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 15, 18, 15, 18, 15);
    test_const_vector<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 15, 18, 15, 18, 15);
    test_const_const<TYPE_DECIMAL64, SubOp, OverflowMode::OUTPUT_NULL>(test_cases, 18, 15, 18, 15, 18, 15);
    test_const_const<TYPE_DECIMAL64, SubOp, OverflowMode::IGNORE>(test_cases, 18, 15, 18, 15, 18, 15);
}

using DecimalOverflowTestCase = std::tuple<std::string, std::string, std::string, bool>;
using DecimalOverflowTestCaseArray = std::vector<DecimalOverflowTestCase>;
TEST_F(DecimalBinaryFunctionTest, test_decimal128p38s16_div_decimal128p38s16_eq_decimal128p38s16) {
    DecimalOverflowTestCaseArray test_cases = {{"1384931237.28", "1382967695.28", "0", true},
                                               {"384931237.28", "1382967695.28", "0", true},
                                               {"84931237.28", "1382967695.28", "0", true},
                                               {"4931237.28", "1382967695.28", "0", true},
                                               {"931237.28", "1382967695.28", "0.0006733615565846", false},
                                               {"-931237.28", "1382967695.28", "-0.0006733615565846", false}};
    DecimalTestCaseArray test_case_array;
    std::vector<bool> overflows;
    test_case_array.reserve(test_cases.size());
    overflows.reserve(test_cases.size());
    for (auto& tc : test_cases) {
        test_case_array.emplace_back(std::get<0>(tc), std::get<1>(tc), std::get<2>(tc));
        overflows.emplace_back(std::get<3>(tc));
    }
    test_vector_vector_assert_overflow<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_case_array, 38, 16, 38,
                                                                                          16, 38, 16, overflows);
}

TEST_F(DecimalBinaryFunctionTest, test_decimal128p38s15_div_decimal128p38s15_eq_decimal128p38s15) {
    DecimalOverflowTestCaseArray test_cases = {{"1384931237.28", "1382967695.28", "0", true},
                                               {"384931237.28", "1382967695.28", "0", true},
                                               {"84931237.28", "1382967695.28", "0.061412307438464", false},
                                               {"4931237.28", "1382967695.28", "0.003565692312865", false},
                                               {"931237.28", "1382967695.28", "0.000673361556585", false},
                                               {"-931237.28", "1382967695.28", "-0.000673361556585", false}};
    DecimalTestCaseArray test_case_array;
    std::vector<bool> overflows;
    test_case_array.reserve(test_cases.size());
    overflows.reserve(test_cases.size());
    for (auto& tc : test_cases) {
        test_case_array.emplace_back(std::get<0>(tc), std::get<1>(tc), std::get<2>(tc));
        overflows.emplace_back(std::get<3>(tc));
    }
    test_vector_vector_assert_overflow<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_case_array, 38, 15, 38,
                                                                                          15, 38, 15, overflows);
}

TEST_F(DecimalBinaryFunctionTest, test_decimal128p38s14_div_decimal128p38s14_eq_decimal128p38s14) {
    DecimalOverflowTestCaseArray test_cases = {{"1384931237.28", "1382967695.28", "1.00141980322946", false},
                                               {"384931237.28", "1382967695.28", "0.27833711415946", false},
                                               {"84931237.28", "1382967695.28", "0.06141230743846", false},
                                               {"4931237.28", "1382967695.28", "0.00356569231286", false},
                                               {"931237.28", "1382967695.28", "0.00067336155658", false},
                                               {"-931237.28", "1382967695.28", "-0.00067336155658", false}};
    DecimalTestCaseArray test_case_array;
    std::vector<bool> overflows;
    test_case_array.reserve(test_cases.size());
    overflows.reserve(test_cases.size());
    for (auto& tc : test_cases) {
        test_case_array.emplace_back(std::get<0>(tc), std::get<1>(tc), std::get<2>(tc));
        overflows.emplace_back(std::get<3>(tc));
    }
    test_vector_vector_assert_overflow<TYPE_DECIMAL128, DivOp, OverflowMode::OUTPUT_NULL>(test_case_array, 38, 14, 38,
                                                                                          14, 38, 14, overflows);
}

template <LogicalType LhsType, LogicalType RhsType, LogicalType ResultType, typename Op>
void test_decimal_fast_mul_help(const DecimalTestCaseArray& test_cases, int lhs_precision, int lhs_scale,
                                int rhs_precision, int rhs_scale, int result_precision, int result_scale) {
#if defined(__x86_64__) && defined(__GNUC__)
    test_vector_vector<LhsType, RhsType, ResultType, Op, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
    test_vector_const<LhsType, RhsType, ResultType, Op, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
    test_const_vector<LhsType, RhsType, ResultType, Op, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
    test_const_const<LhsType, RhsType, ResultType, Op, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
    test_nullable_vector_const<LhsType, RhsType, ResultType, Op, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
    test_const_nullable_vector<LhsType, RhsType, ResultType, Op, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
    test_nullable_vector_nullable_vector<LhsType, RhsType, ResultType, Op, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
    test_vector_vector<ResultType, ResultType, ResultType, MulOp, OverflowMode::IGNORE>(
            test_cases, lhs_precision, lhs_scale, rhs_precision, rhs_scale, result_precision, result_scale);
#endif
}
TEST_F(DecimalBinaryFunctionTest, test_decimal_fast_mul_32x32) {
    DecimalTestCaseArray test_cases = {{"0.14", "3348947.24", "468852.6136"},
                                       {"-9786689.10", "7798141.97", "-76317991018051.5270"},
                                       {"-4791887.62", "3743357.14", "-17937746736404.6068"},
                                       {"32559.38", "1932061.44", "62906722608.3072"},
                                       {"9967778.87", "-4436693.25", "-44223977230021.6275"},
                                       {"66.04", "1745489.71", "115272140.4484"},
                                       {"8730215.73", "9461462.88", "82600612063787.1024"},
                                       {"-1640031.37", "-3649585.51", "5985434723897.4487"},
                                       {"0.40", "-5992669.43", "-2397067.7720"},
                                       {"-4915774.24", "2601554.88", "-12788656463050.2912"},
                                       {"2321876.63", "6786.86", "15758251625.0818"},
                                       {"5795924.12", "-7510017.32", "-43527490526605.7584"},
                                       {"-1477602.43", "7417972.70", "-10960814487193.6610"},
                                       {"4982776.64", "9458572.68", "47129954997646.1952"},
                                       {"-4157714.37", "-319363.42", "1327821880586.3454"},
                                       {"-1338202.52", "9712047.22", "-12996686064162.9944"},
                                       {"5568896.04", "-4178259.11", "-23268290611772.9244"},
                                       {"1216062.54", "-8740084.85", "-10628489782506.5190"},
                                       {"-1011533.50", "1754253.55", "-1774486233318.9250"},
                                       {"-7485146.41", "4185814.42", "-31331433778789.2322"},
                                       {"569696.96", "735990.44", "419291516257.0624"},
                                       {"-1022446.87", "-8669363.57", "8863963647038.5259"},
                                       {"7038776.17", "-9960876.32", "-70112378873533.2944"},
                                       {"-4530254.27", "-1483099.01", "6718815622885.2727"},
                                       {"8033925.05", "4639565.36", "37273920366816.2680"},
                                       {"-4060954.23", "-7233186.83", "29373640653668.7909"}};
    test_decimal_fast_mul_help<TYPE_DECIMAL32, TYPE_DECIMAL32, TYPE_DECIMAL64, MulOp32x32_64>(test_cases, 9, 2, 9, 2,
                                                                                              18, 4);
    test_decimal_fast_mul_help<TYPE_DECIMAL32, TYPE_DECIMAL32, TYPE_DECIMAL128, MulOp32x32_128>(test_cases, 9, 2, 9, 2,
                                                                                                38, 4);
    test_decimal_fast_mul_help<TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128, MulOp32x64_128>(test_cases, 9, 2, 9, 2,
                                                                                                38, 4);
}

TEST_F(DecimalBinaryFunctionTest, test_decimal_fast_mul_32x64) {
    DecimalTestCaseArray test_cases = {{"9032611.72", "-99999987017825.7359", "-903261054737060591007.964748"},
                                       {"2736895.22", "-92663866986328.7512", "-253611294621598964507.849264"},
                                       {"-3828865.35", "80937842651332.4369", "-309900101231438898977.471415"},
                                       {"-7148341.89", "-89822041958723.6528", "642078665178881938244.055792"},
                                       {"-6484113.59", "-37888289565618.6030", "245671973274282780469.114770"},
                                       {"-1714464.49", "-33647025220542.7704", "57686629934754998377.023096"},
                                       {"3058437.45", "-99999999957006.6629", "-305843744868507567712.885605"},
                                       {"-8195353.44", "-99999999989236.8008", "819535343911791778410.874752"},
                                       {"-7133537.38", "-2003684724783.3757", "14293359881977222958.533666"},
                                       {"5696891.54", "-38236526225684.3015", "-217829342774089027926.159310"},
                                       {"-5730753.29", "4.0217", "-23047370.506393"},
                                       {"3.04", "55199582163813.2378", "167806729777992.242912"},
                                       {"-6562428.11", "-99999999493883.1965", "656242807678644861768.253615"},
                                       {"0.00", "0.7476", "0.000000"},
                                       {"823750.91", "23145425564409.9206", "19066065371019935707.277746"},
                                       {"1230710.88", "0.7183", "884019.625104"},
                                       {"72.10", "5140290622005.2846", "370614953846581.019660"},
                                       {"4746532.04", "-40180624201993.8931", "-190718620161963445483.484924"},
                                       {"-170865.42", "46959038948537.3316", "-8023675912738189549.513272"},
                                       {"2569778.63", "-79738078104668.9157", "-204909209110649082791.131491"},
                                       {"-3481236.21", "-99999999999894.5607", "348123620999632940890.882947"},
                                       {"-6347458.75", "-84505412562356.0775", "536394620391287004743.053125"},
                                       {"2059495.57", "0.0486", "100091.484702"},
                                       {"533.04", "3067942701.0884", "1635336177388.160736"},
                                       {"2742930.04", "-99999834076370.4796", "-274292548883092242664.047184"},
                                       {"2269330.25", "0.0361", "81922.822025"}};
    test_decimal_fast_mul_help<TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128, MulOp32x64_128>(test_cases, 9, 2, 18, 4,
                                                                                                38, 6);
    test_decimal_fast_mul_help<TYPE_DECIMAL64, TYPE_DECIMAL64, TYPE_DECIMAL128, MulOp64x64_128>(test_cases, 9, 2, 18, 4,
                                                                                                38, 6);
}

TEST_F(DecimalBinaryFunctionTest, test_decimal_fast_mul_64x64) {
    DecimalTestCaseArray test_cases = {
            {"0.0006", "-82780021658097.3483", "-49668012994.85840898"},
            {"0.0074", "-99827652105771.3123", "-738724625582.70771102"},
            {"-99973172481064.8851", "41055058.0121", "-4104404395863798146564.72090971"},
            {"10418.0297", "-78182709607827.6816", "-814509790720824139.39094352"},
            {"84231788195839.5714", "77738262261.3072", "6548032841507056635908796.85173408"},
            {"-95593465131926.9776", "-2201368772639.3279", "210436469009810477989501604.74235504"},
            {"-5829918858943.4969", "41160.0999", "-239960042643008340.85934031"},
            {"22286515224419.0423", "-45199673982539.5695", "-1007343222350645401189049609.04428985"},
            {"0.3026", "-67285817488933.7073", "-20360688372151.33982898"},
            {"93643928070236.3651", "-85952437612924.2777", "-8048923885286159719807127647.19098827"},
            {"-68455776580233.7557", "5711626623.7667", "-390993836066288058523463.22159519"},
            {"96190766579211.5819", "-99999999999998.3166", "-9619076657920996262463540555.22302954"},
            {"-99999999911670.1121", "-7286176685749.2421", "728617667931337040128175917.79703941"},
            {"5300.1993", "-14319371956134.8862", "-75895525218345754.54281966"},
            {"52771758641568.0008", "-25416153656949.7962", "-1341255126381560552930609374.00143696"},
            {"3.2917", "-99999999999999.9213", "-329169999999999.74094321"},
            {"-78809072751576.0845", "-14758895507471.3475", "1163134869781218860114475849.98886375"},
            {"-63650347027092.4971", "83952744775493.0151", "-5343621338837057001558324362.66700621"},
            {"197070.9764", "20234892726480.4061", "3987710066956751765.59551604"},
            {"0.0114", "-75288876496539.0058", "-858293192060.54466612"},
            {"-72638466358742.9662", "3.1225", "-226813611205174.91195950"},
            {"60915079068521.5269", "-99999121308263.0016", "-6091454381275516575941632856.40914304"}};
    test_decimal_fast_mul_help<TYPE_DECIMAL64, TYPE_DECIMAL64, TYPE_DECIMAL128, MulOp64x64_128>(test_cases, 18, 4, 18,
                                                                                                4, 38, 8);
}

TEST_F(DecimalBinaryFunctionTest, test_overflow_report_error) {
    ASSERT_THROW((test_overflow_report_error<TYPE_DECIMAL32, TYPE_DECIMAL32, TYPE_DECIMAL32, MulOp>(
                         "274.97790", "1.0000", 9, 5, 9, 4)),
                 std::overflow_error);
    ASSERT_THROW((test_overflow_report_error<TYPE_DECIMAL64, TYPE_DECIMAL64, TYPE_DECIMAL64, MulOp>(
                         "274.97790000000", "1.000000", 18, 11, 18, 6)),
                 std::overflow_error);
    ASSERT_THROW((test_overflow_report_error<TYPE_DECIMAL128, TYPE_DECIMAL128, TYPE_DECIMAL128, MulOp>(
                         "274.97790000000000000000", "1.0000000000000000", 38, 20, 38, 16)),
                 std::overflow_error);
}

} // namespace starrocks
