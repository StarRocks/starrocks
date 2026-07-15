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

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "column/array_column.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace starrocks {

class GinFunctionsTest : public testing::Test {
protected:
    static StatusOr<std::vector<std::string>> tokenize(const std::string& tokenizer, const std::string& text,
                                                       bool use_tantivy) {
        TQueryOptions query_options;
        query_options.__set_use_tantivy_tokenize(use_tantivy);
        RuntimeState runtime_state(TUniqueId(), query_options, TQueryGlobals(), nullptr);
        std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
        context->set_runtime_state(&runtime_state);

        Columns columns;
        auto tokenizer_column = BinaryColumn::create();
        tokenizer_column->append(tokenizer);
        columns.emplace_back(ConstColumn::create(tokenizer_column));
        auto text_column = BinaryColumn::create();
        text_column->append(text);
        columns.emplace_back(ConstColumn::create(text_column));
        context->set_constant_columns(columns);

        RETURN_IF_ERROR(GinFunctions::tokenize_prepare(context.get(), FunctionContext::THREAD_LOCAL));
        DeferOp close([&]() { (void)GinFunctions::tokenize_close(context.get(), FunctionContext::THREAD_LOCAL); });
        ASSIGN_OR_RETURN(auto result, GinFunctions::tokenize(context.get(), columns));

        std::vector<std::string> tokens;
        auto row = result->get(0);
        for (const auto& value : row.get_array()) {
            tokens.emplace_back(value.get_slice().to_string());
        }
        return tokens;
    }
};

TEST_F(GinFunctionsTest, TantivyTokenizers) {
    ASSERT_EQ((std::vector<std::string>{"quick", "brown", "foxes", "running"}),
              tokenize("english", "The Quick Brown Foxes Running", true).value());
    ASSERT_EQ((std::vector<std::string>{"中华", "华人", "人民"}), tokenize("chinese", "中华人民", true).value());
    ASSERT_EQ((std::vector<std::string>{"中华", "华人", "人民"}), tokenize("cjk", "中华人民", true).value());

    auto jieba = tokenize("jieba", "中华人民共和国成立了", true).value();
    ASSERT_NE(std::find(jieba.begin(), jieba.end(), "人民"), jieba.end());
    ASSERT_NE(std::find(jieba.begin(), jieba.end(), "共和国"), jieba.end());
}

TEST_F(GinFunctionsTest, CLuceneTokenizers) {
    ASSERT_EQ((std::vector<std::string>{"hello", "world"}), tokenize("english", "Hello World", false).value());
    ASSERT_EQ((std::vector<std::string>{"hello", "world"}), tokenize("standard", "hello, world", false).value());
    ASSERT_EQ((std::vector<std::string>{"中华", "华人", "人民"}), tokenize("chinese", "中华人民", false).value());
}

TEST_F(GinFunctionsTest, RejectsEngineSpecificUnsupportedTokenizer) {
    ASSERT_TRUE(tokenize("standard", "hello", true).status().is_not_supported());
    ASSERT_TRUE(tokenize("jieba", "中华人民共和国", false).status().is_not_supported());
}

} // namespace starrocks
