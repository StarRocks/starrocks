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

#include "column/array_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {

class GinFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(GinFunctionsTest, tokenizeTest) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto tokenizer = BinaryColumn::create();
        tokenizer->append("error_tokenizer");
        columns.emplace_back(ConstColumn::create(tokenizer));

        ctx->set_constant_columns(columns);

        ASSERT_FALSE(GinFunctions::tokenize_prepare(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    }
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto tokenizer = BinaryColumn::create();
        auto content = BinaryColumn::create();

        tokenizer->append("english");
        content->append("hello world");
        columns.emplace_back(ConstColumn::create(tokenizer));
        columns.emplace_back(ConstColumn::create(content));
        ctx->set_constant_columns(columns);
        ASSERT_TRUE(GinFunctions::tokenize_prepare(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
        ColumnPtr result = GinFunctions::tokenize(ctx.get(), columns).value();
        ASSERT_TRUE(GinFunctions::tokenize_close(ctx.get(), FunctionContext::THREAD_LOCAL).ok());

        columns.clear();
        auto nullable_result = ColumnHelper::as_column<NullableColumn>(result);
        auto v = ColumnHelper::as_column<ArrayColumn>(nullable_result->data_column());

        auto res_array = v->get(0).get_array();

        ASSERT_EQ("hello", res_array[0].get_slice().to_string());
        ASSERT_EQ("world", res_array[1].get_slice().to_string());
    }
}

} // namespace starrocks
