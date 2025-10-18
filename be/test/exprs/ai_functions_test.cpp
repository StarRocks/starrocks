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

#include "exprs/ai_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "exprs/mock_vectorized_expr.h"
#include "util/json.h"

namespace starrocks {

class AiFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}

protected:
    // Helper method to create test configuration JSON
    JsonValue createTestConfig(const std::string& endpoint = "https://api.openai.com/v1/chat/completions",
                               const std::string& model = "gpt-3.5-turbo", const std::string& api_key = "test-api-key",
                               double temperature = 0.7, int max_tokens = 1000, double top_p = 0.9) {
        std::string json_str = R"({
            "endpoint": ")" + endpoint +
                               R"(",
            "model": ")" + model +
                               R"(",
            "api_key": ")" + api_key +
                               R"(",
            "temperature": )" + std::to_string(temperature) +
                               R"(,
            "max_tokens": )" + std::to_string(max_tokens) +
                               R"(,
            "top_p": )" + std::to_string(top_p) +
                               R"(
        })";

        JsonValue json;
        EXPECT_TRUE(JsonValue::parse(json_str, &json).ok());
        return json;
    }

    // Helper method to create minimal test configuration JSON
    JsonValue createMinimalTestConfig(const std::string& model = "gpt-3.5-turbo",
                                      const std::string& api_key = "test-api-key") {
        std::string json_str = R"({
            "model": ")" + model +
                               R"(",
            "api_key": ")" + api_key +
                               R"("
        })";

        JsonValue json;
        EXPECT_TRUE(JsonValue::parse(json_str, &json).ok());
        return json;
    }

    // Helper method to create columns for ai_query tests
    void createAiQueryColumns(Columns& columns, const std::vector<std::string>& prompts, const JsonValue& config) {
        // Create prompt column
        auto prompt_col = BinaryColumn::create();
        for (const auto& prompt : prompts) {
            prompt_col->append(prompt);
        }
        columns.emplace_back(prompt_col);

        // Create JSON config column
        auto json_col = JsonColumn::create();
        for (size_t i = 0; i < prompts.size(); ++i) {
            json_col->append(&config);
        }
        columns.emplace_back(json_col);
    }

    // Helper method to validate sentiment analysis responses
    void validateSentimentResponses(const ColumnPtr& result_column, size_t expected_size) {
        ASSERT_EQ(result_column->size(), expected_size);
        const auto* binary_column = down_cast<const BinaryColumn*>(result_column.get());

        for (size_t i = 0; i < expected_size; ++i) {
            std::string response = binary_column->get_data()[i].to_string();
            ASSERT_TRUE(response == "positive" || response == "negative")
                    << "Response at index " << i << " is neither 'positive' nor 'negative': " << response;
        }
    }

    // Test constants
    static constexpr const char* SENTIMENT_ANALYSIS_PROMPT_PREFIX =
            "You are an expert in sentiment analysis. The user will provide you with a piece of text, "
            "and you need to analyze whether the expressed sentiment is positive or negative. "
            "Please answer directly with either \"positive\" or \"negative\". The text content is as follows: \n";

    static constexpr const char* TEST_API_KEY = "sk-67cb94xxxxxxxx";
    static constexpr const char* DEEPSEEK_ENDPOINT = "https://api.deepseek.com/chat/completions";
    static constexpr const char* DEEPSEEK_MODEL = "deepseek-chat";
};

TEST_F(AiFunctionsTest, ValidConfigParsing) {
    auto config = createTestConfig();
    auto config_result = AiFunctions::parse_model_config(config);
    ASSERT_TRUE(config_result.ok());

    ModelConfig parsed_config = config_result.value();
    ASSERT_EQ("https://api.openai.com/v1/chat/completions", parsed_config.endpoint);
    ASSERT_EQ("gpt-3.5-turbo", parsed_config.model);
    ASSERT_EQ("test-api-key", parsed_config.api_key);
    ASSERT_EQ(0.7, parsed_config.temperature);
    ASSERT_EQ(1000, parsed_config.max_tokens);
    ASSERT_EQ(0.9, parsed_config.top_p);
}

TEST_F(AiFunctionsTest, ConfigParsingWithDefaults) {
    auto config = createMinimalTestConfig();
    auto config_result = AiFunctions::parse_model_config(config);
    ASSERT_TRUE(config_result.ok());

    ModelConfig parsed_config = config_result.value();
    ASSERT_EQ("https://api.openai.com/v1/chat/completions", parsed_config.endpoint);
    ASSERT_EQ("gpt-3.5-turbo", parsed_config.model);
    ASSERT_EQ("test-api-key", parsed_config.api_key);
    ASSERT_EQ(kDefaultTemperature, parsed_config.temperature);
    ASSERT_EQ(kDefaultMaxTokens, parsed_config.max_tokens);
    ASSERT_EQ(kDefaultTopP, parsed_config.top_p);
}

TEST_F(AiFunctionsTest, InvalidTemperatureConfig) {
    auto config = createTestConfig("https://api.openai.com/v1/chat/completions", "gpt-3.5-turbo", TEST_API_KEY, 3.0);
    auto config_result = AiFunctions::parse_model_config(config);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("temperature must be between 0 and 2") != std::string::npos);
}

TEST_F(AiFunctionsTest, InvalidMaxTokensConfig) {
    auto config =
            createTestConfig("https://api.openai.com/v1/chat/completions", "gpt-3.5-turbo", TEST_API_KEY, 0.7, -10);
    auto config_result = AiFunctions::parse_model_config(config);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("max_tokens must be positive") != std::string::npos);
}

TEST_F(AiFunctionsTest, InvalidTopPConfig) {
    auto config = createTestConfig("https://api.openai.com/v1/chat/completions", "gpt-3.5-turbo", TEST_API_KEY, 0.7,
                                   1000, 1.5);
    auto config_result = AiFunctions::parse_model_config(config);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("top_p must be between 0 and 1") != std::string::npos);
}

TEST_F(AiFunctionsTest, MissingRequiredFieldsConfig) {
    std::string json_str = R"({
        "api_key": ")" + std::string(TEST_API_KEY) +
                           R"("
    })";

    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());

    auto config_result = AiFunctions::parse_model_config(json);
    ASSERT_FALSE(config_result.ok());
}

TEST_F(AiFunctionsTest, EnvironmentVariableApiKey) {
    // Set up test environment variable
    const char* env_var = "TEST_AI_KEY";
    setenv(env_var, "env-test-key", 1);

    std::string json_str = R"({
        "model": "gpt-3.5-turbo",
        "api_key": "env.TEST_AI_KEY"
    })";
    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());

    auto config_result = AiFunctions::parse_model_config(json);
    ASSERT_TRUE(config_result.ok());
    ASSERT_EQ("env-test-key", config_result->api_key);

    // Clean up
    unsetenv(env_var);
}

TEST_F(AiFunctionsTest, EnvironmentVariableNotFound) {
    std::string json_str = R"({
        "model": "gpt-3.5-turbo",
        "api_key": "env.NON_EXISTENT_VAR"
    })";
    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());

    auto config_result = AiFunctions::parse_model_config(json);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("Environment variable not found") != std::string::npos);
}

TEST_F(AiFunctionsTest, AiQueryWrongArgumentCount) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto prompt_col = BinaryColumn::create();
    prompt_col->append("Hello, how are you?");
    columns.emplace_back(prompt_col);

    auto result = AiFunctions::ai_query(ctx.get(), columns);
    ASSERT_FALSE(result.ok());
    ASSERT_TRUE(result.status().message().find("Ai_query function only call by ai_query(prompt, config)") !=
                std::string::npos);
}

TEST_F(AiFunctionsTest, AiQueryWithNullValues) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    // Create nullable prompt column
    auto prompt_data = BinaryColumn::create();
    auto prompt_null = NullColumn::create();
    prompt_data->append("Hello");
    prompt_data->append("World");
    prompt_null->append(0); // not null
    prompt_null->append(1); // null
    auto prompt_col = NullableColumn::create(prompt_data, prompt_null);
    columns.emplace_back(prompt_col);

    // Create nullable JSON config column
    auto json_data = JsonColumn::create();
    auto json_null = NullColumn::create();
    auto config = createMinimalTestConfig(DEEPSEEK_MODEL, TEST_API_KEY);

    json_data->append(&config);
    json_data->append(&config);
    json_null->append(1); // null
    json_null->append(0); // not null
    auto json_col = NullableColumn::create(json_data, json_null);
    columns.emplace_back(json_col);

    // Test ai_query with null values
    auto result = AiFunctions::ai_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok()) << "ai_query should handle null values gracefully: " << result.status().message();

    auto result_column = std::move(result.value());
    ASSERT_EQ(result_column->size(), 2) << "Result should contain 2 entries";

    // Check if result column is nullable
    auto* nullable_result = dynamic_cast<NullableColumn*>(result_column.get());
    if (nullable_result != nullptr) {
        // If result is nullable, verify null handling
        const auto& null_data = nullable_result->null_column_data();

        // Row 0: prompt is not null, config is null -> result should be null
        ASSERT_TRUE(null_data[0]) << "Row 0 should be null (config is null)";

        // Row 1: prompt is null, config is not null -> result should be null
        ASSERT_TRUE(null_data[1]) << "Row 1 should be null (prompt is null)";
    } else {
        // If result is not nullable, it might return empty strings or error messages
        const auto* binary_result = down_cast<const BinaryColumn*>(result_column.get());
        ASSERT_NE(binary_result, nullptr) << "Result should be either NullableColumn or BinaryColumn";

        // Verify that null inputs produce some predictable output (empty string or error message)
        for (size_t i = 0; i < 2; ++i) {
            std::string response = binary_result->get_data()[i].to_string();
            EXPECT_TRUE(response.empty() || response.find("null") != std::string::npos ||
                        response.find("error") != std::string::npos)
                    << "Row " << i << " should handle null input appropriately, got: " << response;
        }
    }
}

TEST_F(AiFunctionsTest, DISABLED_SingleSentimentAnalysisCall) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    std::vector<std::string> prompts = {std::string(SENTIMENT_ANALYSIS_PROMPT_PREFIX) + "I am very happy today!"};

    auto config = createTestConfig(DEEPSEEK_ENDPOINT, DEEPSEEK_MODEL, TEST_API_KEY);
    createAiQueryColumns(columns, prompts, config);

    auto result = AiFunctions::ai_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());

    auto result_column = std::move(result.value());
    validateSentimentResponses(result_column, 1);

    // For this specific positive test case, we expect "positive"
    const auto* binary_column = down_cast<const BinaryColumn*>(result_column.get());
    std::string response = binary_column->get_data()[0].to_string();
    ASSERT_EQ(response, "positive");
}

TEST_F(AiFunctionsTest, DISABLED_MultipleSameSentimentAnalysisCalls) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    const int num_calls = 5;
    std::vector<std::string> prompts;
    std::string base_prompt = std::string(SENTIMENT_ANALYSIS_PROMPT_PREFIX) + "I am very happy today!";

    for (int i = 0; i < num_calls; ++i) {
        prompts.push_back(base_prompt);
    }

    auto config = createTestConfig(DEEPSEEK_ENDPOINT, DEEPSEEK_MODEL, TEST_API_KEY);
    createAiQueryColumns(columns, prompts, config);

    auto result = AiFunctions::ai_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());

    auto result_column = std::move(result.value());
    validateSentimentResponses(result_column, num_calls);

    // For these specific positive test cases, we expect all "positive"
    const auto* binary_column = down_cast<const BinaryColumn*>(result_column.get());
    for (int i = 0; i < num_calls; ++i) {
        std::string response = binary_column->get_data()[i].to_string();
        ASSERT_EQ(response, "positive") << "Response at index " << i << " should be positive";
    }
}

TEST_F(AiFunctionsTest, DISABLED_DiverseSentimentAnalysisCalls) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    std::vector<std::string> text_samples = {
            "I am very happy today!", // positive
            "I am not happy!",        // negative
            "It is a bad day",        // negative
            "I am angry!!!",          // negative
            "It is a good day!"       // positive
    };

    std::vector<std::string> prompts;
    for (const auto& sample : text_samples) {
        prompts.push_back(std::string(SENTIMENT_ANALYSIS_PROMPT_PREFIX) + sample);
    }

    auto config = createTestConfig(DEEPSEEK_ENDPOINT, DEEPSEEK_MODEL, TEST_API_KEY);
    createAiQueryColumns(columns, prompts, config);

    auto result = AiFunctions::ai_query(ctx.get(), columns);
    ASSERT_TRUE(result.ok());

    auto result_column = std::move(result.value());
    validateSentimentResponses(result_column, text_samples.size());

    // Note: We don't assert specific sentiments here as the actual LLM response
    // may vary and this test is more about ensuring the function works with
    // different inputs rather than validating LLM accuracy
}

} // namespace starrocks
