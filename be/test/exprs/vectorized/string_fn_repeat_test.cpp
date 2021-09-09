// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include <gtest/gtest.h>

#include "exprs/vectorized/string_functions.h"

namespace starrocks {
namespace vectorized {

class StringFunctionRepeatTest : public ::testing::Test {};
TEST_F(StringFunctionRepeatTest, repeatTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto times = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j));
        times->append(j);
    }

    columns.emplace_back(str);
    columns.emplace_back(times);

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns);
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 20; ++k) {
        std::string s;
        for (int i = 0; i < k; ++i) {
            s.append(std::to_string(k));
        }

        ASSERT_EQ(s, v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionRepeatTest, repeatLargeTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto times = Int32Column::create();

    str->append(std::to_string(1));
    times->append(OLAP_STRING_MAX_LENGTH + 100);

    columns.emplace_back(str);
    columns.emplace_back(times);

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns);
    ASSERT_EQ(1, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    std::string s;
    s.resize(OLAP_STRING_MAX_LENGTH, '1');
    ASSERT_EQ(s, v->get_data()[0].to_string());
}

TEST_F(StringFunctionRepeatTest, repeatConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto times = Int32Column::create();

    for (int i = 0; i < 100; ++i) {
        str->append(std::string(i, 'x'));
        str->append(std::string(1, 'x'));
    }
    int32_t repeat_times = OLAP_STRING_MAX_LENGTH / 100 + 10;
    times->append(repeat_times);

    columns.emplace_back(str);
    columns.emplace_back(ConstColumn::create(times, 1));

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns);
    const auto num_rows = str->size();
    ASSERT_EQ(num_rows, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
    for (int i = 0; i < num_rows; ++i) {
        auto si = str->get_slice(i);
        auto so = v->get_slice(i);
        if (si.size * repeat_times < OLAP_STRING_MAX_LENGTH) {
            ASSERT_EQ(so.size, si.size * repeat_times);
        } else {
            auto real_repeat_times = std::max((int32_t)(OLAP_STRING_MAX_LENGTH / si.size), 1);
            ASSERT_EQ(so.size, si.size * real_repeat_times);
        }
    }
}

} // namespace vectorized
} // namespace starrocks
