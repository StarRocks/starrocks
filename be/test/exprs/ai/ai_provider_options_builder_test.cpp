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

#include "exprs/ai/ai_provider_options_builder.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <future>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "platform/llm/openai_compatible_provider.h"
#include "types/json_value.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

JsonColumn::MutablePtr make_json(std::string_view text) {
    auto column = JsonColumn::create();
    auto value = JsonValue::parse(Slice(text.data(), text.size()));
    CHECK(value.ok()) << value.status();
    column->append(std::move(value).value());
    return column;
}

std::string expected_body(std::string_view option_members) {
    std::string body =
            R"({"model":"test-model","messages":[{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":"test-prompt"}],"stream":false)";
    if (!option_members.empty()) {
        body.push_back(',');
        body.append(option_members);
    }
    body.push_back('}');
    return body;
}

std::string request_body(const AIProviderOptions& options) {
    OpenAICompatibleProvider provider;
    auto request = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/chat",
            .model = "test-model",
            .api_key = "test-api-key",
            .prompt = "test-prompt",
            .options = &options,
    });
    CHECK(request.ok()) << request.status().message();
    return std::move(request).value().body;
}

void expect_redacted(const Status& status, std::initializer_list<std::string> sentinels) {
    ASSERT_FALSE(status.ok());
    const std::string message(status.message());
    for (const auto& sentinel : sentinels) {
        EXPECT_EQ(std::string::npos, message.find(sentinel)) << message;
    }
}

TEST(AIProviderOptionsBuilderTest, EmptyObjectAndSqlNullProduceEmptyOptions) {
    auto json = make_json("{}");
    auto empty = build_ai_provider_options(*json, TypeDescriptor(TYPE_JSON), 0);
    ASSERT_TRUE(empty.ok()) << empty.status();
    EXPECT_TRUE(empty->members().empty());
    EXPECT_EQ(expected_body(""), request_body(*empty));

    auto nulls = NullColumn::create();
    nulls->append(1);
    auto nullable = NullableColumn::create(std::move(json), std::move(nulls));
    auto constant = ConstColumn::create(std::move(nullable), 3);
    auto sql_null = build_ai_provider_options(*constant, TypeDescriptor(TYPE_JSON), 2);
    ASSERT_TRUE(sql_null.ok()) << sql_null.status();
    EXPECT_TRUE(sql_null->members().empty());
}

TEST(AIProviderOptionsBuilderTest, ConstAndNullableColumnsRespectTheSelectedRow) {
    auto json = make_json(R"({"dimensions":512})");
    json->append(JsonValue::parse(R"({"dimensions":1024})").value());
    auto nulls = NullColumn::create();
    nulls->append(1);
    nulls->append(0);
    auto nullable = NullableColumn::create(std::move(json), std::move(nulls));
    auto options = build_ai_provider_options(*nullable, TypeDescriptor(TYPE_JSON), 1);
    ASSERT_TRUE(options.ok()) << options.status();
    EXPECT_EQ(expected_body(R"("dimensions":1024)"), request_body(*options));
    EXPECT_FALSE(build_ai_provider_options(*nullable, TypeDescriptor(TYPE_JSON), 2).ok());

    auto constant = ConstColumn::create(make_json(R"({"dimensions":512})"), 4);
    auto repeated = build_ai_provider_options(*constant, TypeDescriptor(TYPE_JSON), 3);
    ASSERT_TRUE(repeated.ok()) << repeated.status();
    EXPECT_EQ(expected_body(R"("dimensions":512)"), request_body(*repeated));
}

TEST(AIProviderOptionsBuilderTest, JsonTypesSurviveTheFinalRequest) {
    auto json = make_json(R"({"guardrails":true,"disabled":false,"dimensions":512,"temperature":0.7,
                             "text":"123","payload":{"input":[1,true,null,{"model":"nested"}]},
                             "nullable":null,"empty_array":[],"empty_object":{}})");
    auto options = build_ai_provider_options(*json, TypeDescriptor(TYPE_JSON), 0);
    ASSERT_TRUE(options.ok()) << options.status();
    rapidjson::Document body;
    body.Parse(request_body(*options).c_str());
    ASSERT_FALSE(body.HasParseError());
    EXPECT_TRUE(body["guardrails"].GetBool());
    EXPECT_FALSE(body["disabled"].GetBool());
    EXPECT_EQ(512, body["dimensions"].GetInt());
    EXPECT_DOUBLE_EQ(0.7, body["temperature"].GetDouble());
    EXPECT_STREQ("123", body["text"].GetString());
    EXPECT_TRUE(body["payload"]["input"][2].IsNull());
    EXPECT_STREQ("nested", body["payload"]["input"][3]["model"].GetString());
    EXPECT_TRUE(body["nullable"].IsNull());
    EXPECT_TRUE(body["empty_array"].IsArray());
    EXPECT_TRUE(body["empty_array"].Empty());
    EXPECT_TRUE(body["empty_object"].IsObject());
    EXPECT_EQ(0, body["empty_object"].MemberCount());
}

TEST(AIProviderOptionsBuilderTest, NonObjectJsonIsRejectedWithoutEchoingValues) {
    for (const char* text : {"null", "[]", "true", "42", R"("secret-value")"}) {
        auto json = make_json(text);
        auto result = build_ai_provider_options(*json, TypeDescriptor(TYPE_JSON), 0);
        ASSERT_FALSE(result.ok());
        expect_redacted(result.status(), {"secret-value"});
    }
}

TEST(AIProviderOptionsBuilderTest, OnlyJsonTypedColumnsAreAccepted) {
    auto json = make_json("{}");
    EXPECT_FALSE(build_ai_provider_options(*json, TypeDescriptor(TYPE_VARCHAR), 0).ok());
    EXPECT_FALSE(build_ai_provider_options(*json, TypeDescriptor(TYPE_MAP), 0).ok());
    auto string = BinaryColumn::create();
    string->append("secret-value");
    auto result = build_ai_provider_options(*string, TypeDescriptor(TYPE_JSON), 0);
    ASSERT_FALSE(result.ok());
    expect_redacted(result.status(), {"secret-value"});
}

TEST(AIProviderOptionsBuilderTest, ReservedFieldsAreCapabilitySpecificAndRedacted) {
    for (AICapability capability : {AICapability::CHAT, AICapability::EMBEDDING}) {
        for (const std::string key : {"model", "endpoint", "key", "api_key", "protocol", "provider", "messages",
                                      "stream", "input", "encoding_format"}) {
            auto json = make_json("{\"" + key + "\":\"secret-value\"}");
            auto result = build_ai_provider_options(*json, TypeDescriptor(TYPE_JSON), 0, capability);
            const bool allowed = capability == AICapability::CHAT ? key == "input" || key == "encoding_format"
                                                                  : key == "messages" || key == "stream";
            EXPECT_EQ(allowed, result.ok()) << key;
            if (!allowed) {
                expect_redacted(result.status(), {"secret-value"});
            }
        }
    }
    auto nested = make_json(R"({"payload":{"model":"nested"},"Model":"case-sensitive"})");
    EXPECT_TRUE(build_ai_provider_options(*nested, TypeDescriptor(TYPE_JSON), 0).ok());
    auto empty_key = make_json(R"({"":"secret-value"})");
    auto result = build_ai_provider_options(*empty_key, TypeDescriptor(TYPE_JSON), 0);
    ASSERT_FALSE(result.ok());
    expect_redacted(result.status(), {"secret-value"});
}

TEST(AIProviderOptionsBuilderTest, PreparedOptionsOwnTheirStateAndSupportConcurrentReadOnlyRequestBuilding) {
    auto prepared_result = [] {
        auto json = make_json(R"({"payload":{"immutable":[1,2,3]}})");
        return build_ai_provider_options(*json, TypeDescriptor(TYPE_JSON), 0);
    }();
    ASSERT_TRUE(prepared_result.ok()) << prepared_result.status();
    auto prepared = std::move(prepared_result).value();
    auto copied = prepared;
    const auto expected = expected_body(R"("payload":{"immutable":[1,2,3]})");
    std::vector<std::future<bool>> reads;
    for (int i = 0; i < 8; ++i) {
        reads.emplace_back(std::async(std::launch::async, [&prepared, &copied, &expected, i] {
            return request_body(i % 2 == 0 ? prepared : copied) == expected;
        }));
    }
    for (auto& read : reads) {
        EXPECT_TRUE(read.get());
    }
}

} // namespace
} // namespace starrocks
