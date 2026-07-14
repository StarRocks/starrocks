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

#include "exprs/embedding_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "exprs/function_context.h"
#include "types/json_value.h"

namespace starrocks {

class EmbeddingFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}

protected:
    // Parse a JSON string into a JsonValue for use as the embedding() config argument.
    static JsonValue parseConfig(const std::string& json_str) {
        JsonValue json;
        EXPECT_TRUE(JsonValue::parse(json_str, &json).ok()) << "invalid test config JSON: " << json_str;
        return json;
    }

    // A well-formed config that points at a local port with nothing listening, so call_embedding()
    // fails fast (connection refused) and the row degrades to NULL without depending on the network.
    static JsonValue unreachableConfig() {
        return parseConfig(R"({
            "endpoint": "http://127.0.0.1:1/v1/embeddings",
            "model": "test-embedding-model",
            "dimensions": 4,
            "timeout_ms": 1000
        })");
    }

    // Build a (text VARCHAR, config JSON) column pair. Both columns are plain (non-const, non-nullable)
    // with one config replicated per text row.
    static void buildColumns(Columns& columns, const std::vector<std::string>& texts, const JsonValue& config) {
        auto text_col = BinaryColumn::create();
        for (const auto& t : texts) {
            text_col->append(t);
        }
        columns.emplace_back(std::move(text_col));

        auto json_col = JsonColumn::create();
        for (size_t i = 0; i < texts.size(); ++i) {
            json_col->append(&config);
        }
        columns.emplace_back(std::move(json_col));
    }

    // The result of embedding() must be a NullableColumn wrapping an ARRAY<FLOAT> column.
    static const NullableColumn* asNullableArray(const ColumnPtr& result) {
        const auto* nullable = down_cast<const NullableColumn*>(result.get());
        EXPECT_NE(nullptr, nullable);
        if (nullable != nullptr) {
            EXPECT_NE(nullptr, dynamic_cast<const ArrayColumn*>(nullable->data_column().get()));
        }
        return nullable;
    }
};

// embedding() requires exactly two arguments (text, config).
TEST_F(EmbeddingFunctionsTest, WrongArgumentCount) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns one_arg;
    auto text_col = BinaryColumn::create();
    text_col->append("hello");
    one_arg.emplace_back(std::move(text_col));

    auto result = EmbeddingFunctions::embedding(ctx.get(), one_arg);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().message().find("takes exactly two arguments") != std::string::npos)
            << result.status().message();
}

// A config that is not a JSON object (here, a bare number) is rejected up front.
TEST_F(EmbeddingFunctionsTest, ConfigNotJsonObject) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    buildColumns(columns, {"hello"}, parseConfig("123"));

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().message().find("not a JSON object") != std::string::npos) << result.status().message();
}

// 'endpoint' is required.
TEST_F(EmbeddingFunctionsTest, ConfigMissingEndpoint) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    buildColumns(columns, {"hello"}, parseConfig(R"({"model": "m"})"));

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().message().find("'endpoint'") != std::string::npos) << result.status().message();
}

// 'model' is required.
TEST_F(EmbeddingFunctionsTest, ConfigMissingModel) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    buildColumns(columns, {"hello"}, parseConfig(R"({"endpoint": "http://127.0.0.1:1/v1/embeddings"})"));

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().message().find("'model'") != std::string::npos) << result.status().message();
}

// A null text or null config yields a NULL array for that row; the query itself still succeeds.
TEST_F(EmbeddingFunctionsTest, NullInputsProduceNullRows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;

    // Row 0: text not null, config null -> NULL. Row 1: text null, config not null -> NULL.
    auto text_data = BinaryColumn::create();
    auto text_null = NullColumn::create();
    text_data->append("hello");
    text_data->append("world");
    text_null->append(0);
    text_null->append(1);
    columns.emplace_back(NullableColumn::create(std::move(text_data), std::move(text_null)));

    auto config = unreachableConfig();
    auto json_data = JsonColumn::create();
    auto json_null = NullColumn::create();
    json_data->append(&config);
    json_data->append(&config);
    json_null->append(1);
    json_null->append(0);
    columns.emplace_back(NullableColumn::create(std::move(json_data), std::move(json_null)));

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_TRUE(result.ok()) << result.status().message();

    auto column = std::move(result.value());
    const auto* nullable = asNullableArray(column);
    ASSERT_NE(nullptr, nullable);
    ASSERT_EQ(2, nullable->size());
    ASSERT_TRUE(nullable->is_null(0)) << "row 0 (null config) should be NULL";
    ASSERT_TRUE(nullable->is_null(1)) << "row 1 (null text) should be NULL";
}

// A well-formed config whose provider is unreachable degrades every row to NULL, and the whole
// query still returns OK rather than failing. Exercises the ARRAY<FLOAT> column-building path.
TEST_F(EmbeddingFunctionsTest, ProviderUnreachableYieldsNullRows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    buildColumns(columns, {"hello", "world"}, unreachableConfig());

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_TRUE(result.ok()) << result.status().message();

    auto column = std::move(result.value());
    const auto* nullable = asNullableArray(column);
    ASSERT_NE(nullptr, nullable);
    ASSERT_EQ(2, nullable->size());
    ASSERT_TRUE(nullable->is_null(0));
    ASSERT_TRUE(nullable->is_null(1));
}

// A constant config column is parsed once. An invalid constant config surfaces the parse error.
TEST_F(EmbeddingFunctionsTest, ConstConfigInvalidReportsError) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto config = parseConfig(R"({"model": "m"})"); // missing endpoint
    auto json_one = JsonColumn::create();
    json_one->append(&config);
    ColumnPtr const_config = ConstColumn::create(std::move(json_one), 2);
    ctx->set_constant_columns({nullptr, const_config});

    Columns columns;
    auto text_col = BinaryColumn::create();
    text_col->append("hello");
    text_col->append("world");
    columns.emplace_back(std::move(text_col));
    columns.emplace_back(const_config);

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().message().find("'endpoint'") != std::string::npos) << result.status().message();
}

// A valid constant config with an unreachable provider still returns OK with all-NULL rows, and the
// const-config fast path is taken (is_notnull_constant_column true).
TEST_F(EmbeddingFunctionsTest, ConstConfigUnreachableYieldsNullRows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto config = unreachableConfig();
    auto json_one = JsonColumn::create();
    json_one->append(&config);
    ColumnPtr const_config = ConstColumn::create(std::move(json_one), 2);
    ctx->set_constant_columns({nullptr, const_config});
    ASSERT_TRUE(ctx->is_notnull_constant_column(1));

    Columns columns;
    auto text_col = BinaryColumn::create();
    text_col->append("hello");
    text_col->append("world");
    columns.emplace_back(std::move(text_col));
    columns.emplace_back(const_config);

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_TRUE(result.ok()) << result.status().message();

    auto column = std::move(result.value());
    const auto* nullable = asNullableArray(column);
    ASSERT_NE(nullptr, nullable);
    ASSERT_EQ(2, nullable->size());
    ASSERT_TRUE(nullable->is_null(0));
    ASSERT_TRUE(nullable->is_null(1));
}

// Live end-to-end call against a real OpenAI-compatible embeddings endpoint. Disabled by default
// because it needs network access and a valid api_key; enable it manually to validate the happy path:
//   ./run-be-ut.sh --gtest_filter='EmbeddingFunctionsTest.DISABLED_*' --gtest_also_run_disabled_tests
TEST_F(EmbeddingFunctionsTest, DISABLED_SingleEmbeddingCall) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto config = parseConfig(R"({
        "endpoint": "https://api.openai.com/v1/embeddings",
        "model": "text-embedding-3-small",
        "api_key": "sk-REPLACE-ME",
        "dimensions": 1536
    })");

    Columns columns;
    buildColumns(columns, {"StarRocks is a fast analytical database."}, config);

    auto result = EmbeddingFunctions::embedding(ctx.get(), columns);
    ASSERT_TRUE(result.ok()) << result.status().message();

    auto column = std::move(result.value());
    const auto* nullable = asNullableArray(column);
    ASSERT_NE(nullptr, nullable);
    ASSERT_EQ(1, nullable->size());
    ASSERT_FALSE(nullable->is_null(0));

    const auto* array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
    ASSERT_EQ(1536, array_col->get(0).get_array().size());
}

} // namespace starrocks
