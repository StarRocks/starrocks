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

class StringFunctionFormatBytesTest : public ::testing::Test {};

TEST_F(StringFunctionFormatBytesTest, formatBytesBasicTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto bytes_col = Int64Column::create();

    // Test basic examples from the spec
    std::vector<std::tuple<int64_t, std::string>> test_cases = {{0, "0 B"},
                                                                {123, "123 B"},
                                                                {1023, "1023 B"},
                                                                {1024, "1.00 KB"},
                                                                {4096, "4.00 KB"},
                                                                {123456789, "117.74 MB"},
                                                                {10737418240, "10.00 GB"},
                                                                {1048576, "1.00 MB"},
                                                                {1073741824, "1.00 GB"},
                                                                {1099511627776, "1.00 TB"},
                                                                {1125899906842624, "1.00 PB"},
                                                                {1152921504606846976, "1.00 EB"}};

    for (auto& test_case : test_cases) {
        bytes_col->append(std::get<0>(test_case));
    }

    columns.emplace_back(std::move(bytes_col));

    ColumnPtr result = StringFunctions::format_bytes(ctx.get(), columns).value();
    ASSERT_EQ(test_cases.size(), result->size());
    ASSERT_TRUE(result->is_binary());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int i = 0; i < test_cases.size(); ++i) {
        ASSERT_EQ(std::get<1>(test_cases[i]), v->get_data()[i].to_string())
                << "Failed for input: " << std::get<0>(test_cases[i]);
    }
}

TEST_F(StringFunctionFormatBytesTest, formatBytesEdgeCasesTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    std::vector<std::tuple<ColumnPtr, bool, std::string>> test_cases = {
            // Test null input
            {ColumnHelper::create_const_null_column(1), true, ""},

            // Test zero
            {ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1), false, "0 B"},

            // Test negative numbers (should return null)
            {ColumnHelper::create_const_column<TYPE_BIGINT>(-1, 1), true, ""},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(-1024, 1), true, ""},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(INT64_MIN, 1), true, ""},

            // Test small values (bytes)
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1, 1), false, "1 B"},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(512, 1), false, "512 B"},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1023, 1), false, "1023 B"},

            // Test KB values
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1024, 1), false, "1.00 KB"},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1536, 1), false, "1.50 KB"},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(2048, 1), false, "2.00 KB"},

            // Test MB values
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1048576, 1), false, "1.00 MB"},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1572864, 1), false, "1.50 MB"},

            // Test GB values
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1073741824, 1), false, "1.00 GB"},
            {ColumnHelper::create_const_column<TYPE_BIGINT>(2147483648, 1), false, "2.00 GB"},

            // Test TB values
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1099511627776, 1), false, "1.00 TB"},

            // Test PB values
            {ColumnHelper::create_const_column<TYPE_BIGINT>(1125899906842624, 1), false, "1.00 PB"},

            // Test very large values
            {ColumnHelper::create_const_column<TYPE_BIGINT>(INT64_MAX, 1), false, "8.00 EB"},
    };

    for (auto& test_case : test_cases) {
        auto [column, is_null, expected] = test_case;
        Columns columns{column};
        ColumnPtr result = StringFunctions::format_bytes(ctx.get(), columns).value();
        ASSERT_EQ(result->size(), 1);

        if (is_null) {
            ASSERT_TRUE(result->is_null(0));
        } else {
            ASSERT_FALSE(result->is_null(0));
            auto nullable_datum = result->get(0);
            ASSERT_FALSE(nullable_datum.is_null());
            ASSERT_EQ(nullable_datum.get_slice().to_string(), expected);
        }
    }
}

TEST_F(StringFunctionFormatBytesTest, formatBytesNullableColumnTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto bytes_col = Int64Column::create();
    auto null_col = NullColumn::create();

    std::vector<std::tuple<int64_t, bool, bool, std::string>> test_cases = {
            {0, false, false, "0 B"},
            {1024, false, false, "1.00 KB"},
            {-1, false, true, ""},
            {1048576, false, false, "1.00 MB"},
            {0, true, true, ""}, // null input
            {1073741824, false, false, "1.00 GB"},
            {-512, false, true, ""},
            {123, false, false, "123 B"},
            {4096, true, true, ""}, // null input
            {1099511627776, false, false, "1.00 TB"},
    };

    for (auto& test_case : test_cases) {
        int64_t bytes = std::get<0>(test_case);
        bool is_null = std::get<1>(test_case);
        bytes_col->append(bytes);
        null_col->append(is_null);
    }

    const auto num_rows = bytes_col->size();
    Columns columns{NullableColumn::create(std::move(bytes_col), std::move(null_col))};
    auto result = StringFunctions::format_bytes(ctx.get(), columns).value();
    ASSERT_EQ(result->size(), num_rows);

    for (int i = 0; i < num_rows; ++i) {
        bool expect_is_null = std::get<2>(test_cases[i]);
        std::string expected = std::get<3>(test_cases[i]);
        auto nullable_datum = result->get(i);

        if (expect_is_null) {
            ASSERT_TRUE(nullable_datum.is_null()) << "Row " << i << " should be null";
        } else {
            ASSERT_FALSE(nullable_datum.is_null()) << "Row " << i << " should not be null";
            ASSERT_EQ(nullable_datum.get_slice().to_string(), expected)
                    << "Row " << i << " input: " << std::get<0>(test_cases[i]);
        }
    }
}

TEST_F(StringFunctionFormatBytesTest, formatBytesPrecisionTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto bytes_col = Int64Column::create();

    // Test precision and rounding (2 decimal places)
    std::vector<std::tuple<int64_t, std::string>> precision_cases = {
            {1024 + 512, "1.50 KB"},             // 1.5 KB
            {1024 + 256, "1.25 KB"},             // 1.25 KB
            {1024 + 1, "1.00 KB"},               // 1.0009765625 KB -> 1.00 KB
            {1024 + 102, "1.10 KB"},             // ~1.0996 KB -> 1.10 KB
            {1024 + 103, "1.10 KB"},             // ~1.1005 KB -> 1.10 KB
            {1048576 + 524288, "1.50 MB"},       // 1.5 MB
            {1048576 + 52429, "1.05 MB"},        // ~1.05 MB
            {1073741824 + 536870912, "1.50 GB"}, // 1.5 GB
    };

    for (auto& test_case : precision_cases) {
        bytes_col->append(std::get<0>(test_case));
    }

    columns.emplace_back(std::move(bytes_col));

    ColumnPtr result = StringFunctions::format_bytes(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int i = 0; i < precision_cases.size(); ++i) {
        ASSERT_EQ(std::get<1>(precision_cases[i]), v->get_data()[i].to_string())
                << "Failed precision test for input: " << std::get<0>(precision_cases[i]);
    }
}

} // namespace starrocks