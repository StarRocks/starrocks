// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/math_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "exprs/vectorized/mock_vectorized_expr.h"
#include "math.h"

#define PI acos(-1)

namespace starrocks {
namespace vectorized {
class VecMathFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(VecMathFunctionsTest, truncateTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        double dous[] = {2341.2341111, 4999.90134, 2144.2855, 934.12439};
        int ints[] = {2, 3, 1, 4};

        double res[] = {2341.23, 4999.901, 2144.2, 934.1243};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            tc1->append(dous[i]);
            tc2->append(ints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::truncate(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, truncateNanTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        tc1->append(0);
        tc2->append(1591994755);

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::truncate(ctx.get(), columns);

        ASSERT_EQ(true, result->is_null(0));
    }
}

TEST_F(VecMathFunctionsTest, Round_up_toTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        double dous[] = {2341.2341111, 4999.90134, 2144.2855, 934.12439};
        int ints[] = {2, 3, 1, 4};

        double res[] = {2341.23, 4999.901, 2144.3, 934.1244};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            tc1->append(dous[i]);
            tc2->append(ints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::round_up_to(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, RoundUpToHalfwayCasesWithPositiveTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        double dous[] = {7.845, 7.855};
        int ints[] = {2, 2};

        double res[] = {7.85, 7.86};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            tc1->append(dous[i]);
            tc2->append(ints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::round_up_to(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, RoundUpToHalfwayCasesWithNegativeTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        auto tc2 = Int32Column::create();

        double dous[] = {45.0, 44.0};
        int ints[] = {-1, -1};

        double res[] = {50, 40};

        for (int i = 0; i < sizeof(dous) / sizeof(dous[0]); ++i) {
            tc1->append(dous[i]);
            tc2->append(ints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::round_up_to(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, BinTest) {
    {
        Columns columns;

        auto tc1 = Int64Column::create();

        int64_t ints[] = {1, 2, 256};

        std::string res[] = {"1", "10", "100000000"};

        for (int i = 0; i < sizeof(ints) / sizeof(ints[0]); ++i) {
            tc1->append(ints[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::bin(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i].to_string());
        }
    }
}

TEST_F(VecMathFunctionsTest, LeastDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.2222222222", "-740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc1->append(dec_values[i]);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"2342.111", "9866.9011"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc2->append(dec_values[i]);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template least<TYPE_DECIMALV2>(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"2342.111", "-740740740.716049"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, GreatestDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.2222222222", "-740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc1->append(dec_values[i]);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"2342.111", "9866.9011"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc2->append(dec_values[i]);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template greatest<TYPE_DECIMALV2>(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"3333333333.2222222222", "9866.9011"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, PositiveDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"-3333333333.2222222222", "-740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc1->append(dec_values[i]);
        }
    }

    columns.emplace_back(tc1);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template positive<TYPE_DECIMALV2>(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"-3333333333.2222222222", "-740740740.716049"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, NegativeDecimalTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.2222222222", "740740740.716049"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc1->append(dec_values[i]);
        }
    }

    columns.emplace_back(tc1);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template negative<TYPE_DECIMALV2>(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

    std::string result_str[] = {"-3333333333.2222222222", "-740740740.716049"};
    DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
    for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
        results[i] = DecimalV2Value(result_str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, ModDecimalGeneralTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {"3333333333.3222222222", "2342414342.132", "32413241.12342", "999234812.222"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc1->append(dec_values[i]);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"4", "3", "0", "1"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc2->append(dec_values[i]);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template mod<TYPE_DECIMALV2>(ctx.get(), columns);

    //auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);
    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    std::string str[] = {"1.3222222222", "2.132", "0.12342", "0.222"};
    DecimalV2Value results[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        results[i] = DecimalV2Value(str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, ModDecimalBigTest) {
    Columns columns;

    auto tc1 = DecimalColumn::create();
    {
        std::string str[] = {
                "333333099873333.322222222", "23112133142414342.132", "413241.12342", "999234812.222", "68482.48227",
                "2413424287348.24221"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc1->append(dec_values[i]);
        }
    }

    auto tc2 = DecimalColumn::create();
    {
        std::string str[] = {"4535.3452", "7.34535", "2.91", "71.234", "34241.24114", "777982341.234234"};
        DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
        for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
            dec_values[i] = DecimalV2Value(str[i]);
        }
        for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
            tc2->append(dec_values[i]);
        }
    }

    columns.emplace_back(tc1);
    columns.emplace_back(tc2);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = MathFunctions::template mod<TYPE_DECIMALV2>(ctx.get(), columns);

    //auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);
    auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    std::string str[] = {"163.573422222", "4.4786", "0.75342", "19.69", "34241.24113", "123064839.648342"};
    DecimalV2Value results[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        results[i] = DecimalV2Value(str[i]);
    }

    for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
        ASSERT_EQ(results[i], v->get_data()[i]);
    }
}

TEST_F(VecMathFunctionsTest, Conv_intTest) {
    {
        Columns columns;

        auto tc1 = Int64Column::create();
        auto tc2 = Int8Column::create();
        auto tc3 = Int8Column::create();

        int64_t bigints[] = {1,           2,           35,          10,         10,         10,         10,
                             3,           3,           99999999,    99999999,   99999999,   99999999,   -99999999,
                             -99999999,   -99999999,   -99999999,   4294967296, 4294967296, 4294967296, 4294967296,
                             -4294967296, -4294967296, -4294967296, -4294967296};
        int8_t baseints[] = {10,  10,  10, 2,  2,   -2,  -2, -2, -2,  -16, -16, 16, 16,
                             -16, -16, 16, 16, -16, -16, 16, 16, -16, -16, -16, -16};
        int8_t destints[] = {10, 16,  36, 10,  -10, 10,  -10, 10,  10, 10,  -10, 10, -10,
                             10, -10, 10, -10, 10,  -10, 10,  -10, 10, -10, 10,  -10};

        std::string results[] = {"1",
                                 "2",
                                 "Z",
                                 "2",
                                 "2",
                                 "2",
                                 "2",
                                 "0",
                                 "0",
                                 "2576980377",
                                 "2576980377",
                                 "2576980377",
                                 "2576980377",
                                 "18446744071132571239",
                                 "-2576980377",
                                 "18446744071132571239",
                                 "-2576980377",
                                 "285960729238",
                                 "285960729238",
                                 "285960729238",
                                 "285960729238",
                                 "18446743787748822378",
                                 "-285960729238",
                                 "18446743787748822378",
                                 "-285960729238"};

        for (int i = 0; i < sizeof(bigints) / sizeof(bigints[0]); ++i) {
            tc1->append(bigints[i]);
            tc2->append(baseints[i]);
            tc3->append(destints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);
        columns.emplace_back(tc3);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::conv_int(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

        for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i].to_string());
        }
    }
}

TEST_F(VecMathFunctionsTest, Conv_stringTest) {
    {
        Columns columns;

        auto tc1 = BinaryColumn::create();
        auto tc2 = Int8Column::create();
        auto tc3 = Int8Column::create();

        std::string bigints[] = {"1",
                                 "2",
                                 "35",
                                 "1000",
                                 "103",
                                 "114",
                                 "18446744073709551617",
                                 "10",
                                 "10",
                                 "10",
                                 "10",
                                 "-9223372036854775808",
                                 "-9223372036854775808",
                                 "-9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775808",
                                 "9223372036854775808",
                                 "9223372036854775808",
                                 "9223372036854775808",
                                 "-9223372036854775809",
                                 "-9223372036854775809",
                                 "-9223372036854775809",
                                 "-9223372036854775809",
                                 "18446744073709551616",
                                 "18446744073709551616",
                                 "18446744073709551616",
                                 "18446744073709551616",
                                 "-18446744073709551616",
                                 "-18446744073709551616",
                                 "-18446744073709551616",
                                 "-18446744073709551616",
                                 "de0b6b3a7640000",
                                 "8ac7230489e80000",
                                 "a"};
        int8_t baseints[] = {10, 10, 10,  8,   35, 35, 10,  -2,  -2, 2,  2,   -10, -10, 10, 10, -10, -10,
                             10, 10, -10, -10, 10, 10, -10, -10, 10, 10, -10, -10, 10,  10, 16, 16,  10};
        int8_t destints[] = {10, 16,  36, 10,  10, 10,  10, 10,  -10, 10,  -10, 10,  -10, 10,  -10, 10, -10,
                             10, -10, 10, -10, 10, -10, 10, -10, 10,  -10, 10,  -10, 10,  -10, 10,  10, 10};

        std::string results[] = {"1",
                                 "2",
                                 "Z",
                                 "512",
                                 "1228",
                                 "1264",
                                 "18446744073709551615",
                                 "2",
                                 "2",
                                 "2",
                                 "2",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "9223372036854775807",
                                 "18446744073709551615",
                                 "-1",
                                 "9223372036854775808",
                                 "-9223372036854775808",
                                 "0",
                                 "0",
                                 "1000000000000000000",
                                 "10000000000000000000",
                                 "0"};

        for (int i = 0; i < sizeof(bigints) / sizeof(bigints[0]); ++i) {
            tc1->append(bigints[i]);
            tc2->append(baseints[i]);
            tc3->append(destints[i]);
        }

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);
        columns.emplace_back(tc3);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::conv_string(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

        for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i].to_string());
        }
    }
}

TEST_F(VecMathFunctionsTest, LnTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        tc1->append(0);
        tc1->append(2.0);
        tc1->append(-1);
        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result_log10 = MathFunctions::log10(ctx.get(), columns);
        ColumnPtr result_ln = MathFunctions::ln(ctx.get(), columns);

        ASSERT_EQ(true, result_log10->is_null(0));
        ASSERT_EQ(std::log10(2), result_log10->get(1).get_double());
        ASSERT_EQ(true, result_log10->is_null(2));

        ASSERT_EQ(true, result_ln->is_null(0));
        ASSERT_EQ(std::log(2), result_ln->get(1).get_double());
        ASSERT_EQ(true, result_ln->is_null(2));
    }
}

TEST_F(VecMathFunctionsTest, ExpTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        tc1->append(0);
        tc1->append(2.0);
        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result_exp = MathFunctions::exp(ctx.get(), columns);

        ASSERT_EQ(false, result_exp->is_null(0));
        ASSERT_EQ(std::exp(0), result_exp->get(0).get_double());
        ASSERT_EQ(std::exp(2), result_exp->get(1).get_double());
    }
}

TEST_F(VecMathFunctionsTest, ExpOverflowTest) {
    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        tc1->append(2.47498282E8);
        tc1->append(2.47498282E3);
        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result_exp = MathFunctions::exp(ctx.get(), columns);

        ASSERT_EQ(true, result_exp->is_null(0));
        ASSERT_EQ(true, result_exp->is_null(1));
    }
}

TEST_F(VecMathFunctionsTest, AbsTest) {
    {
        Columns columns;

        auto tc1 = Int8Column::create();
        int8_t inputs[] = {10, 10, 10, 8, -35, 35, -128};
        int16_t results[] = {10, 10, 10, 8, 35, 35, 128};

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            tc1->append(inputs[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_tinyint(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_SMALLINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int16Column::create();
        int16_t inputs[] = {1000, 1000, 1000, -500, -35, 35, -32768};
        int32_t results[] = {1000, 1000, 1000, 500, 35, 35, 32768};

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            tc1->append(inputs[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_smallint(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int32Column::create();
        int32_t inputs[] = {23424242, -1111188324, 909877, -4353525, -39879711, 35, -2147483648};
        int64_t results[] = {23424242, 1111188324, 909877, 4353525, 39879711, 35, 2147483648};

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            tc1->append(inputs[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_int(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int64Column::create();
        int64_t inputs[] = {2342423422442, -11111883200004,         90987700000, -435352241435, -398797110000,
                            3523411,       -9223372036854775807 - 1};
        int128_t results[] = {2342423422442, 11111883200004, 90987700000,         435352241435,
                              398797110000,  3523411,        9223372036854775808U};

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            tc1->append(inputs[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_bigint(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_LARGEINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = Int128Column::create();
        int128_t inputs[] = {2342423422442, -1111188320000004,   -90987700000241,
                             -435352241435, -390241238797110000, -3523411};
        int128_t results[] = {2342423422442, 1111188320000004,   90987700000241,
                              435352241435,  390241238797110000, 3523411};

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            tc1->append(inputs[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_largeint(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_LARGEINT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = DoubleColumn::create();
        double inputs[] = {23424222.124, -34123412.24311, -23412487.1111, -97241.8761, -2349723, -3523411};
        double results[] = {23424222.124, 34123412.24311, 23412487.1111, 97241.8761, 2349723, 3523411};

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            tc1->append(inputs[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_double(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = FloatColumn::create();
        float inputs[] = {23424222.124, -34123412.24311, -23412487.1111, -97241.8761, -2349723, -3523411};
        float results[] = {23424222.124, 34123412.24311, 23412487.1111, 97241.8761, 2349723, 3523411};

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            tc1->append(inputs[i]);
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_float(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_FLOAT>(result);

        for (int i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }

    {
        Columns columns;

        auto tc1 = DecimalColumn::create();
        {
            std::string str[] = {"3333333333.2222222222", "-740740740.716049"};
            DecimalV2Value dec_values[sizeof(str) / sizeof(str[0])];
            for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
                dec_values[i] = DecimalV2Value(str[i]);
            }
            for (int i = 0; i < sizeof(dec_values) / sizeof(dec_values[0]); ++i) {
                tc1->append(dec_values[i]);
            }
        }

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::abs_decimalv2val(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_DECIMALV2>(result);

        std::string result_str[] = {"3333333333.2222222222", "740740740.716049"};
        DecimalV2Value results[sizeof(result_str) / sizeof(result_str[0])];
        for (int i = 0; i < sizeof(result_str) / sizeof(result_str[0]); ++i) {
            results[i] = DecimalV2Value(result_str[i]);
        }

        for (int i = 0; i < sizeof(results) / sizeof(results[0]); ++i) {
            ASSERT_EQ(results[i], v->get_data()[i]);
        }
    }
}

TEST_F(VecMathFunctionsTest, CotTest) {
    {
        Columns columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(0);
        tc1->append(0.1);
        tc1->append(0.2);
        tc1->append(0.3);
        columns.emplace_back(tc1);

        Columns columns2;
        auto tc2 = DoubleColumn::create();
        tc2->append(0);
        tc2->append(0.1);
        tc2->append(0.2);
        tc2->append(0.3);
        columns2.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::cot(ctx.get(), columns);
        ColumnPtr result2 = MathFunctions::tan(ctx.get(), columns2);

        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(nullable->data_column());
        auto v2 = ColumnHelper::cast_to<TYPE_DOUBLE>(result2);

        ASSERT_TRUE(nullable->is_null(0));
        for (int i = 1; i < 4; ++i) {
            LOG(INFO) << "v->get_data()[i]: " << v->get_data()[i] << " v2->get_data()[i]: " << v2->get_data()[i];
            ASSERT_TRUE((1 - v->get_data()[i] * v2->get_data()[i]) < 0.1);
        }
    }
}

TEST_F(VecMathFunctionsTest, Atan2Test) {
    {
        Columns columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(0.1);
        tc1->append(0.2);
        tc1->append(0.3);
        columns.emplace_back(tc1);

        auto tc2 = DoubleColumn::create();
        tc2->append(0.1);
        tc2->append(0.2);
        tc2->append(0.3);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::atan2(ctx.get(), columns);

        auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE((v->get_data()[i] - (PI / 4)) < 0.1);
        }
    }
}

TEST_F(VecMathFunctionsTest, OutputNanTest) {
    Columns columns;
    auto tc1 = DoubleColumn::create();
    tc1->append(-10);
    tc1->append(1.0);
    tc1->append(std::nan("not a double number"));
    columns.emplace_back(tc1);

    {
        std::vector<bool> null_expect = {true, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::acos(ctx.get(), columns);
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {false, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::sin(ctx.get(), columns);
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {true, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::asin(ctx.get(), columns);
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        std::vector<bool> null_expect = {true, false, true};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::log2(ctx.get(), columns);
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        Columns binary_columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(-0.9);
        tc1->append(0.2);
        tc1->append(0.3);
        binary_columns.emplace_back(tc1);

        auto tc2 = DoubleColumn::create();
        tc2->append(0.8);
        tc2->append(0.2);
        tc2->append(0.3);
        binary_columns.emplace_back(tc2);

        std::vector<bool> null_expect = {true, false, false};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::pow(ctx.get(), binary_columns);
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }

    {
        Columns binary_columns;
        auto tc1 = DoubleColumn::create();
        tc1->append(std::nan("not a double number"));
        tc1->append(0.2);
        tc1->append(0.3);
        binary_columns.emplace_back(tc1);

        auto tc2 = DoubleColumn::create();
        tc2->append(0.8);
        tc2->append(0.2);
        tc2->append(0.3);
        binary_columns.emplace_back(tc2);

        std::vector<bool> null_expect = {true, false, false};
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = MathFunctions::atan2(ctx.get(), binary_columns);
        auto nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_EQ(nullable->size(), null_expect.size());
        for (size_t i = 0; i < nullable->size(); i++) {
            ASSERT_EQ(nullable->is_null(i), null_expect[i]);
        }
    }
}

} // namespace vectorized
} // namespace starrocks
