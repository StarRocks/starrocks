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

#include <gtest/gtest.h>

#include "exprs/string_functions.h"

namespace starrocks {

class StringFunctionSpaceTest : public ::testing::Test {};

TEST_F(StringFunctionSpaceTest, spaceTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto times = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        times->append(j);
    }

    columns.emplace_back(std::move(times));

    ColumnPtr result = StringFunctions::space(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_TRUE(result->is_binary());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 20; ++k) {
        std::string s;
        s.resize(k, ' ');
        ASSERT_EQ(s, v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionSpaceTest, spaceConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    std::vector<std::tuple<ColumnPtr, bool, std::string>> times_cases = {
            {ColumnHelper::create_const_column<TYPE_INT>(-1, 1), true, ""},
            {ColumnHelper::create_const_column<TYPE_INT>(0, 1), false, ""},
            {ColumnHelper::create_const_column<TYPE_INT>(1, 1), false, std::string(1, ' ')},
            {ColumnHelper::create_const_column<TYPE_INT>(get_olap_string_max_length() - 1, 1), false,
             std::string(get_olap_string_max_length() - 1, ' ')},
            {ColumnHelper::create_const_column<TYPE_INT>(get_olap_string_max_length() + 1, 1), true, ""},
            {ColumnHelper::create_const_column<TYPE_INT>(INT32_MAX, 1), true, ""},
            {ColumnHelper::create_const_column<TYPE_INT>(INT32_MIN, 1), true, ""},
            {ColumnHelper::create_const_null_column(1), true, ""}};
    for (auto& c : times_cases) {
        auto [column, is_null, expect] = c;
        Columns columns{column};
        ColumnPtr result = StringFunctions::space(ctx.get(), columns).value();
        ASSERT_EQ(result->size(), 1);
        if (is_null) {
            ASSERT_TRUE(result->is_null(0));
        } else {
            ASSERT_FALSE(result->is_null(0));
            auto nullable_datum = result->get(0);
            ASSERT_FALSE(nullable_datum.is_null());
            ASSERT_EQ(nullable_datum.get_slice().to_string(), expect);
        }
    }
}

TEST_F(StringFunctionSpaceTest, spaceNullableColumnTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto times_col = Int32Column::create();
    auto null_col = NullColumn::create();

    for (int i = 0; i < 100; ++i) {
        times_col->append(INT_MIN);
        times_col->append(INT_MAX);
        times_col->append(get_olap_string_max_length() + i);
        times_col->append(get_olap_string_max_length() - i);
        times_col->append(i);
        times_col->append(-i);
    }

    for (int i = 0; i < times_col->size(); ++i) {
        if (i % 3 == 1) {
            null_col->append(1);
        } else {
            null_col->append(0);
        }
    }
    auto start_row = times_col->size();
    std::vector<std::tuple<int32_t, bool, bool, std::string>> times_cases = {
            {1, false, false, std::string(1, ' ')},
            {10, false, false, std::string(10, ' ')},
            {0, true, true, ""},
            {0, false, false, ""},
            {INT32_MAX, false, true, ""},
            {INT32_MIN, false, true, ""},
            {get_olap_string_max_length(), false, false, std::string(get_olap_string_max_length(), ' ')},
            {get_olap_string_max_length() - 1, false, false, std::string(get_olap_string_max_length() - 1, ' ')},
            {get_olap_string_max_length() + 1, true, true, ""},
            {-10, false, true, ""},
            {0, true, true, ""},
            {100, true, true, ""},
            {INT32_MAX, true, true, ""},
            {10000, false, false, std::string(10000, ' ')},
    };

    for (auto& c : times_cases) {
        int32_t times = std::get<0>(c);
        bool is_null = std::get<1>(c);
        times_col->append(times);
        null_col->append(is_null);
    }
    const auto num_rows = times_col->size();
    Columns columns{NullableColumn::create(std::move(times_col), std::move(null_col))};
    auto result = StringFunctions::space(ctx.get(), columns).value();
    ASSERT_EQ(result->size(), num_rows);
    ASSERT_TRUE(num_rows - start_row > 0);
    for (int i = start_row; i < num_rows; ++i) {
        bool expect_is_null = std::get<2>(times_cases[i - start_row]);
        std::string expect = std::get<3>(times_cases[i - start_row]);
        auto nullable_datum = result->get(i);
        if (expect_is_null) {
            ASSERT_TRUE(nullable_datum.is_null());
        } else {
            ASSERT_FALSE(nullable_datum.is_null());
            ASSERT_EQ(nullable_datum.get_slice().to_string(), expect);
        }
    }
}

} // namespace starrocks
