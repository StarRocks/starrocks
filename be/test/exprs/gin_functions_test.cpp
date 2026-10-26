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

#include "exprs/gin_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

class GinFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

namespace {

// The tokens of one row of a tokenize() result, whatever kind of column it came back as.
std::vector<std::string> tokens_at(const ColumnPtr& result, size_t row) {
    std::vector<std::string> tokens;
    if (result->is_null(row)) {
        return tokens;
    }
    // Column::get() returns by value and a range-for does not extend that temporary's lifetime,
    // so the Datum has to outlive the loop.
    const Datum row_datum = result->get(row);
    for (const auto& datum : row_datum.get_array()) {
        tokens.emplace_back(datum.get_slice().to_string());
    }
    return tokens;
}

ColumnPtr tokenize_or_die(const ColumnPtr& method, const ColumnPtr& value, bool value_is_constant) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns{method, value};
    // A constant argument is the only one the planner records here; a column argument stays unset.
    ctx->set_constant_columns(Columns{method, value_is_constant ? value : nullptr});
    CHECK(GinFunctions::tokenize_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    auto result = GinFunctions::tokenize(ctx.get(), columns).value();
    CHECK(GinFunctions::tokenize_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    return result;
}

ColumnPtr const_varchar(const std::string& value, size_t num_rows) {
    auto data = BinaryColumn::create();
    data->append(Slice(value));
    return ColumnPtr(ConstColumn::create(std::move(data), num_rows));
}

} // namespace

TEST_F(GinFunctionsTest, tokenizeTest) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto tokenizer = BinaryColumn::create();
        tokenizer->append("error_tokenizer");
        columns.emplace_back(ConstColumn::create(tokenizer));

        ctx->set_constant_columns(columns);

        ASSERT_FALSE(GinFunctions::tokenize_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    }
    {
        auto result = tokenize_or_die(const_varchar("english", 1), const_varchar("hello world", 1), true);
        ASSERT_EQ((std::vector<std::string>{"hello", "world"}), tokens_at(result, 0));
    }
}

// A column handed to a chunk has to be either constant or as long as the chunk. Tokenizing a
// constant argument used to produce an ordinary one-row column, which a multi-row chunk rejected:
//   chunk.cpp] Check failed: num_rows() == c->size() (4 vs. 1)
// reached from ProjectOperator::push_chunk by
//   SELECT id, tokenize('chinese', 'english') FROM t ORDER BY id
// on a table with more than one row.
TEST_F(GinFunctionsTest, tokenizeConstantValueFitsAMultiRowChunk) {
    for (size_t num_rows : {1u, 2u, 4u, 4096u}) {
        auto result = tokenize_or_die(const_varchar("english", num_rows), const_varchar("hello world", num_rows), true);
        ASSERT_TRUE(result->is_constant() || result->size() == num_rows)
                << "num_rows=" << num_rows << " produced a column of size " << result->size();
        ASSERT_EQ((std::vector<std::string>{"hello", "world"}), tokens_at(result, 0));
        // Every row of the chunk reads the same tokens.
        ASSERT_EQ((std::vector<std::string>{"hello", "world"}), tokens_at(result, num_rows - 1));
    }
}

// A constant that tokenizes to nothing is null for every row, not just the first.
TEST_F(GinFunctionsTest, tokenizeConstantEmptyValueIsNullForEveryRow) {
    auto result = tokenize_or_die(const_varchar("english", 4), const_varchar("", 4), true);
    ASSERT_TRUE(result->is_constant() || result->size() == 4);
    ASSERT_TRUE(result->only_null());
}

// The column case must keep producing one row of tokens per input row.
TEST_F(GinFunctionsTest, tokenizeColumnValueKeepsOneRowPerInputRow) {
    auto values = BinaryColumn::create();
    values->append("hello world");
    values->append("");
    values->append("Today is saturday");
    ColumnPtr value_column = std::move(values);

    auto result = tokenize_or_die(const_varchar("english", 3), value_column, false);
    ASSERT_FALSE(result->is_constant());
    ASSERT_EQ(3u, result->size());
    ASSERT_EQ((std::vector<std::string>{"hello", "world"}), tokens_at(result, 0));
    ASSERT_TRUE(result->is_null(1));
    ASSERT_EQ((std::vector<std::string>{"today", "is", "saturday"}), tokens_at(result, 2));
}

} // namespace starrocks