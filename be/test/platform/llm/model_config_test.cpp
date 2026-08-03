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

#include "platform/llm/model_config.h"

#include <gtest/gtest.h>

#include <cstdlib>

#include "types/json_value.h"

namespace starrocks {

namespace {

JsonValue create_test_config(const std::string& endpoint = kDefaultEndpoint, const std::string& model = "gpt-3.5-turbo",
                             const std::string& api_key = "test-api-key", double temperature = 0.7,
                             int max_tokens = 1000, double top_p = 0.9) {
    std::string json_str = R"({
            "endpoint": ")" +
                           endpoint +
                           R"(",
            "model": ")" + model +
                           R"(",
            "api_key": ")" +
                           api_key +
                           R"(",
            "temperature": )" +
                           std::to_string(temperature) +
                           R"(,
            "max_tokens": )" +
                           std::to_string(max_tokens) +
                           R"(,
            "top_p": )" + std::to_string(top_p) +
                           R"(
        })";

    JsonValue json;
    EXPECT_TRUE(JsonValue::parse(json_str, &json).ok());
    return json;
}

JsonValue create_minimal_test_config(const std::string& model = "gpt-3.5-turbo",
                                     const std::string& api_key = "test-api-key") {
    std::string json_str = R"({
            "model": ")" + model +
                           R"(",
            "api_key": ")" +
                           api_key +
                           R"("
        })";

    JsonValue json;
    EXPECT_TRUE(JsonValue::parse(json_str, &json).ok());
    return json;
}

} // namespace

TEST(ModelConfigTest, ValidConfigParsing) {
    auto config = create_test_config();
    auto config_result = parse_model_config(config);
    ASSERT_TRUE(config_result.ok());

    ModelConfig parsed_config = config_result.value();
    ASSERT_EQ(kDefaultEndpoint, parsed_config.endpoint);
    ASSERT_EQ("gpt-3.5-turbo", parsed_config.model);
    ASSERT_EQ("test-api-key", parsed_config.api_key);
    ASSERT_EQ(0.7, parsed_config.temperature);
    ASSERT_EQ(1000, parsed_config.max_tokens);
    ASSERT_EQ(0.9, parsed_config.top_p);
}

TEST(ModelConfigTest, ConfigParsingWithDefaults) {
    auto config = create_minimal_test_config();
    auto config_result = parse_model_config(config);
    ASSERT_TRUE(config_result.ok());

    ModelConfig parsed_config = config_result.value();
    ASSERT_EQ(kDefaultEndpoint, parsed_config.endpoint);
    ASSERT_EQ("gpt-3.5-turbo", parsed_config.model);
    ASSERT_EQ("test-api-key", parsed_config.api_key);
    ASSERT_EQ(kDefaultTemperature, parsed_config.temperature);
    ASSERT_EQ(kDefaultMaxTokens, parsed_config.max_tokens);
    ASSERT_EQ(kDefaultTopP, parsed_config.top_p);
}

TEST(ModelConfigTest, InvalidTemperatureConfig) {
    auto config = create_test_config(kDefaultEndpoint, "gpt-3.5-turbo", "test-api-key", 3.0);
    auto config_result = parse_model_config(config);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("temperature must be between 0 and 2") != std::string::npos);
}

TEST(ModelConfigTest, InvalidMaxTokensConfig) {
    auto config = create_test_config(kDefaultEndpoint, "gpt-3.5-turbo", "test-api-key", 0.7, -10);
    auto config_result = parse_model_config(config);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("max_tokens must be positive") != std::string::npos);
}

TEST(ModelConfigTest, InvalidTopPConfig) {
    auto config = create_test_config(kDefaultEndpoint, "gpt-3.5-turbo", "test-api-key", 0.7, 1000, 1.5);
    auto config_result = parse_model_config(config);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("top_p must be between 0 and 1") != std::string::npos);
}

TEST(ModelConfigTest, MissingRequiredFieldsConfig) {
    std::string json_str = R"({
        "api_key": "test-api-key"
    })";

    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());

    auto config_result = parse_model_config(json);
    ASSERT_FALSE(config_result.ok());
}

TEST(ModelConfigTest, EnvironmentVariableApiKey) {
    const char* env_var = "TEST_AI_KEY";
    setenv(env_var, "env-test-key", 1);

    std::string json_str = R"({
        "model": "gpt-3.5-turbo",
        "api_key": "env.TEST_AI_KEY"
    })";
    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());

    auto config_result = parse_model_config(json);
    ASSERT_TRUE(config_result.ok());
    ASSERT_EQ("env-test-key", config_result->api_key);

    unsetenv(env_var);
}

TEST(ModelConfigTest, EnvironmentVariableNotFound) {
    std::string json_str = R"({
        "model": "gpt-3.5-turbo",
        "api_key": "env.NON_EXISTENT_VAR"
    })";
    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(json_str, &json).ok());

    auto config_result = parse_model_config(json);
    ASSERT_FALSE(config_result.ok());
    ASSERT_TRUE(config_result.status().message().find("Environment variable not found") != std::string::npos);
}

} // namespace starrocks
