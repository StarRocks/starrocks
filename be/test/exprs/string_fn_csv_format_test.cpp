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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "butil/time.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/string_functions.h"

namespace starrocks {

class StringFunctionCsvFormatTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StringFunctionCsvFormatTest, csv_format) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto sep_col = BinaryColumn::create();    // Delimiter column
    auto quote_col = BinaryColumn::create();  // Quotation mark column
    auto str1 = BinaryColumn::create();       // First data column
    auto str2 = BinaryColumn::create();       // Second data column
    auto str3 = BinaryColumn::create();       // The third data column

    auto null = NullColumn::create();         // The NULL indicator for the fourth data column

    for (int j = 0; j < 20; ++j) {
        sep_col->append(",");                     // Delimiter: comma
        quote_col->append("\"");                  // Quotation marks: Double quotation marks
        str1->append("a");                        // First data column: Fixed value "a"
        str2->append(std::to_string(j));      // Second data column: Numbers 0-19
        str3->append("b");                        // Third data column: Fixed value "b"
        null->append(j % 2);                    // NULL marker: Odd lines are marked as NULL
    }

    // Create a Nullable column: The odd line of the 3th column str3 is NULL
    columns.emplace_back(sep_col);                             // Delimiter column
    columns.emplace_back(quote_col);                           // Quotation mark column
    columns.emplace_back(str1);                                // First data column
    columns.emplace_back(str2);                                // Second data column
    columns.emplace_back(NullableColumn::create(str3, null));  // The third data column (with NULL)

    ColumnPtr result = StringFunctions::csv_format(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < 20; ++j) {
        if (j % 2) {
            // Odd rows: The third data column is NULL and should be displayed as ""
            // Expected format: "a","j","
            std::string expected = "\"a\",\"" + std::to_string(j) + "\",\"\"";
            ASSERT_EQ(expected, v->get_data()[j].to_string());
        } else {
            // Even rows: All columns have values
            // Expected formats: "a","j","b"
            std::string expected = "\"a\",\"" + std::to_string(j) + "\",\"b\"";
            ASSERT_EQ(expected, v->get_data()[j].to_string());
        }
    }
}


} // namespace starrocks
