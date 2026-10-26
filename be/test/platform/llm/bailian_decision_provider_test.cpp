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

#include "platform/llm/bailian_decision_provider.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <future>
#include <memory>
#include <string>
#include <utility>

#include "platform/llm/ai_decision_json.h"
#include "platform/llm/dashscope_multimodal_provider.h"
#include "platform/llm/openai_compatible_provider.h"

namespace starrocks {
namespace {
const std::string kQuestions =
        R"({"route":{"type":"choice","criteria":{"billing":"Billing","support":"Support"}},"urgent":{"type":"noul","instructions":"Needs action?"},"severity":{"type":"score","criteria":["Low","High"]}})";
const std::string kResponse =
        R"({"model":"decision-model-preview","request_id":"request-1","answers":{"route":{"type":"choice","choice":"billing","confidence":0.7,"probabilities":{"billing":0.8,"support":0.2}},"urgent":{"type":"noul","noul":0.9},"severity":{"type":"score","score":0.25,"confidence":0.6,"probabilities":{"0":0.75,"1":0.25},"legend":{"0":"Low","1":"High"}}},"usage":{"input_tokens":42},"latency_ms":8.5})";

StatusOr<AIProviderHttpRequest> build(BailianDecisionProvider& provider, std::string state = "ticket",
                                      bool state_is_json = false, std::string_view questions = kQuestions,
                                      std::string_view model = "decision-model") {
    ASSIGN_OR_RETURN(auto parsed, parse_ai_decision_questions(questions));
    AIDecisionInput input{std::move(state), state_is_json,
                          std::make_shared<const AIDecisionQuestions>(std::move(parsed))};
    return provider.build_request({.endpoint = "https://example.com/compatible-mode/v1/systemone",
                                   .model = model,
                                   .api_key = "decision-secret",
                                   .capability = AICapability::DECISION,
                                   .decision = &input});
}

AIProviderParseResult parse(BailianDecisionProvider& provider, std::string_view body,
                            std::string_view questions = kQuestions) {
    auto request = build(provider, "state", false, questions);
    EXPECT_TRUE(request.ok()) << request.status();
    return provider.parse_response(body, AICapability::DECISION, request->response_context);
}
} // namespace

TEST(BailianDecisionProviderTest, BuildsTypedRequestAndPreservesFullResponse) {
    BailianDecisionProvider provider;
    auto request = build(provider, "{\"text\":\"退款\\n\"}");
    ASSERT_TRUE(request.ok()) << request.status();
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(request->body.c_str()).HasParseError());
    EXPECT_TRUE(doc["state"].IsString());
    EXPECT_STREQ("{\"text\":\"退款\\n\"}", doc["state"].GetString());
    EXPECT_EQ(3, doc["questions"].MemberCount());
    EXPECT_FALSE(doc.HasMember("messages"));
    EXPECT_FALSE(doc.HasMember("stream"));
    EXPECT_EQ("https://example.com/compatible-mode/v1/systemone", request->url);
    ASSERT_EQ(2, request->headers.size());
    EXPECT_EQ("Bearer decision-secret", request->headers[1].value);
    auto result = provider.parse_response(kResponse, AICapability::DECISION, request->response_context);
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
    const auto& success = std::get<AIProviderSuccess>(result);
    EXPECT_EQ(kResponse, std::get<std::string>(success.value));
    EXPECT_EQ(42, success.usage.prompt_tokens);
    EXPECT_EQ(42, success.usage.total_tokens);
    EXPECT_EQ(0, success.usage.completion_tokens);
}

TEST(BailianDecisionProviderTest, PreservesJsonStateTypeAndRejectsScalars) {
    BailianDecisionProvider provider;
    for (const auto* state : {R"({"ticket":7,"text":"退款"})", R"(["hello",{"role":"user"}])", R"("hello")"}) {
        auto request = build(provider, state, true);
        ASSERT_TRUE(request.ok()) << request.status();
        rapidjson::Document doc, expected;
        ASSERT_FALSE(doc.Parse(request->body.c_str()).HasParseError());
        ASSERT_FALSE(expected.Parse(state).HasParseError());
        EXPECT_EQ(expected, doc["state"]);
    }
    for (const auto* state : {"null", "true", "1", "{", "{} trailing", R"({"nested":{"a":1,"a":2}})"}) {
        EXPECT_FALSE(build(provider, state, true).ok()) << state;
    }
    EXPECT_FALSE(build(provider, "state", false, kQuestions, "").ok());
    const std::string state("hello\0world", 11);
    auto request = build(provider, state);
    ASSERT_TRUE(request.ok());
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(request->body.data(), request->body.size()).HasParseError());
    EXPECT_EQ(state, std::string(doc["state"].GetString(), doc["state"].GetStringLength()));
}

TEST(BailianDecisionProviderTest, RejectsInvalidQuestions) {
    for (const auto* json :
         {"{}", "[]", "null", "{", R"({"":{"type":"noul"}})", R"({"q":{"type":"chat"}})",
          R"({"q":{"type":"noul","instructions":1}})", R"({"q":{"type":"noul","unexpected":true}})",
          R"({"q":{"type":"noul","criteria":{"yes":"yes"}}})", R"({"q":{"type":"choice","criteria":{}}})",
          R"({"q":{"type":"choice","criteria":{"a":1}}})", R"({"q":{"type":"score","criteria":["one"]}})",
          R"({"q":{"type":"score","criteria":["one",2]}})", R"({"q":{"type":"noul"},"q":{"type":"noul"}})",
          R"({"q":{"type":"noul","type":"choice"}})",
          R"({"q":{"type":"choice","criteria":{"a":"first","a":"second"}}})"}) {
        EXPECT_TRUE(parse_ai_decision_questions(json).status().is_invalid_argument()) << json;
    }
}

TEST(BailianDecisionProviderTest, EnforcesQuestionAndCriteriaLimits) {
    for (int count : {1, 16, 17}) {
        std::string json = "{";
        for (int i = 0; i < count; ++i) {
            if (i) json += ',';
            json += "\"q" + std::to_string(i) + "\":{\"type\":\"noul\"}";
        }
        json += '}';
        EXPECT_EQ(count <= 16, parse_ai_decision_questions(json).ok());
    }
    for (int count : {1, 2, 255, 256}) {
        std::string choices = "{", scores = "[";
        for (int i = 0; i < count; ++i) {
            if (i) {
                choices += ',';
                scores += ',';
            }
            choices += "\"c" + std::to_string(i) + "\":\"description\"";
            scores += "\"level\"";
        }
        choices += '}';
        scores += ']';
        EXPECT_EQ(count <= 255,
                  parse_ai_decision_questions("{\"q\":{\"type\":\"choice\",\"criteria\":" + choices + "}}").ok());
        EXPECT_EQ(count >= 2 && count <= 255,
                  parse_ai_decision_questions("{\"q\":{\"type\":\"score\",\"criteria\":" + scores + "}}").ok());
    }
}

TEST(BailianDecisionProviderTest, ValidatesCompleteDocumentUtf8AndDuplicates) {
    BailianDecisionProvider provider;
    for (const auto& suffix : {std::string("\0", 1), std::string("\0hidden", 7), std::string(" trailing")}) {
        EXPECT_FALSE(parse_ai_decision_questions(kQuestions + suffix).ok());
        EXPECT_FALSE(build(provider, "{}" + suffix, true).ok());
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(parse(provider, kResponse + suffix)));
    }
    for (const auto& invalid : {std::string("\xff", 1), std::string("\xc0\xaf", 2), std::string("\xe4\xb8", 2),
                                std::string("\xed\xa0\x80", 3)}) {
        EXPECT_FALSE(build(provider, invalid).ok());
        EXPECT_FALSE(build(provider, "text", false, kQuestions, invalid).ok());
        EXPECT_FALSE(
                parse_ai_decision_questions("{\"q\":{\"type\":\"noul\",\"instructions\":\"" + invalid + "\"}}").ok());
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
                parse(provider, kResponse.substr(0, kResponse.size() - 1) + ",\"extra\":\"" + invalid + "\"}")));
    }
    EXPECT_TRUE(parse_ai_decision_questions(kQuestions + " \t\n").ok());
    EXPECT_TRUE(build(provider, "{} \t\n", true).ok());
    EXPECT_TRUE(std::holds_alternative<AIProviderSuccess>(parse(provider, kResponse + " \t\n")));
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
            parse(provider, kResponse.substr(0, kResponse.size() - 1) + R"(,"extra":{"x":1,"x":2}})")));
}

TEST(BailianDecisionProviderTest, RejectsMissingUnexpectedOrMismatchedAnswers) {
    BailianDecisionProvider provider;
    for (const auto* json :
         {"{}", "[]", "null", "{", R"({"answers":{}})", R"({"answers":{"q":{"type":"choice","choice":"x"}}})",
          R"({"answers":{"q":{"type":"noul","noul":0.5},"extra":{"type":"noul","noul":0.1}}})",
          R"({"answers":{"q":{"type":"noul","noul":0.5},"q":{"type":"noul","noul":0.1}}})",
          R"({"answers":{"q":{"type":"noul","noul":true}}})", R"({"answers":{"q":{"type":"noul","noul":1.01}}})",
          R"({"answers":{"q":{"type":"noul","noul":-0.01}}})"}) {
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(parse(provider, json, R"({"q":{"type":"noul"}})")))
                << json;
    }
    for (const auto& replacement :
         {std::pair{"\"choice\":\"billing\"", "\"choice\":\"other\""},
          std::pair{"\"confidence\":0.7", "\"confidence\":1.7"}, std::pair{"\"billing\":0.8", "\"billing\":0.4"},
          std::pair{"\"billing\":0.8", "\"unknown\":0.8"}, std::pair{"\"score\":0.25", "\"score\":2.0"},
          std::pair{"\"1\":\"High\"", "\"1\":2"}}) {
        std::string json = kResponse;
        json.replace(json.find(replacement.first), std::string(replacement.first).size(), replacement.second);
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(parse(provider, json))) << json;
    }
}

TEST(BailianDecisionProviderTest, PreservesTrustedUsageEvenWhenAnswersOrMetadataAreInvalid) {
    BailianDecisionProvider provider;
    for (const auto* body :
         {R"({"answers":{"q":{"type":"noul","noul":2}},"usage":{"input_tokens":500}})",
          R"({"answers":{},"usage":{"input_tokens":500}})",
          R"({"answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":500},"latency_ms":"8"})",
          R"({"answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":500},"model":42})"}) {
        auto result = parse(provider, body, R"({"q":{"type":"noul"}})");
        ASSERT_TRUE(std::holds_alternative<AIProviderMalformed>(result));
        auto usage = std::get<AIProviderMalformed>(result).usage;
        EXPECT_EQ(500, usage.prompt_tokens);
        EXPECT_EQ(500, usage.total_tokens);
        EXPECT_EQ(0, usage.completion_tokens);
    }
    auto no_usage = parse(provider, R"({"answers":{"q":{"type":"noul","noul":0}}})", R"({"q":{"type":"noul"}})");
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(no_usage));
    EXPECT_EQ(std::nullopt, std::get<AIProviderSuccess>(no_usage).usage.total_tokens);
}

TEST(BailianDecisionProviderTest, RejectsAmbiguousNegativeAndOverflowUsage) {
    BailianDecisionProvider provider;
    for (const auto* body :
         {R"({"usage":{"input_tokens":500})", R"({"usage":{"input_tokens":500},"usage":{"input_tokens":600}})",
          R"({"usage":{"input_tokens":500,"input_tokens":600}})", R"({"usage":{"input_tokens":-500}})",
          R"({"usage":{"input_tokens":1.5}})", R"({"usage":{"input_tokens":9223372036854775808}})",
          R"({"usage":{"input_tokens":"500"}})", R"({"usage":null})"}) {
        auto result = parse(provider, body);
        ASSERT_TRUE(std::holds_alternative<AIProviderMalformed>(result));
        EXPECT_EQ(std::nullopt, std::get<AIProviderMalformed>(result).usage.total_tokens);
    }
}

TEST(BailianDecisionProviderTest, KeepsPerRequestSchemasIsolatedWithoutStateOrCredentials) {
    BailianDecisionProvider provider;
    auto first = build(provider, "private-state", false,
                       R"({"first":{"type":"noul","instructions":"private-instruction"}})");
    auto second = build(provider, "second-state", false,
                        R"({"second":{"type":"choice","criteria":{"a":"private-description"}}})");
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());
    const std::string response1 = R"({"answers":{"first":{"type":"noul","noul":0.5}}})";
    const std::string response2 =
            R"({"answers":{"second":{"type":"choice","choice":"a","confidence":1,"probabilities":{"a":1}}}})";
    auto schema1 = std::move(first->response_context);
    auto schema2 = std::move(second->response_context);
    first = Status::Cancelled("released");
    second = Status::Cancelled("released");
    auto run = [&](const auto& context, const std::string& own, const std::string& other) {
        for (int i = 0; i < 50; ++i) {
            EXPECT_TRUE(std::holds_alternative<AIProviderSuccess>(
                    provider.parse_response(own, AICapability::DECISION, context)));
            EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
                    provider.parse_response(other, AICapability::DECISION, context)));
        }
    };
    auto future = std::async(std::launch::async, [&] { run(schema1, response1, response2); });
    run(schema2, response2, response1);
    future.get();
    ASSERT_NE(nullptr, schema1.decision_schema());
    EXPECT_EQ("first", schema1.decision_schema()->questions[0].id);
    EXPECT_TRUE(schema1.decision_schema()->questions[0].labels.empty());
    EXPECT_GT(schema1.retained_bytes(), 0);
    EXPECT_TRUE(
            std::holds_alternative<AIProviderMalformed>(provider.parse_response(response1, AICapability::DECISION)));
}

TEST(BailianDecisionProviderTest, FactoryRestrictsProtocolCapabilityAndRejectsInvalidCredentials) {
    EXPECT_TRUE(create_ai_provider("bailian_decision", AICapability::DECISION).ok());
    EXPECT_FALSE(create_ai_provider("bailian_decision", AICapability::CHAT).ok());
    EXPECT_FALSE(create_ai_provider("openai_compatible", AICapability::DECISION).ok());
    OpenAICompatibleProvider chat;
    DashScopeMultimodalProvider embedding;
    EXPECT_FALSE(chat.build_request({.model = "model", .capability = AICapability::DECISION}).ok());
    EXPECT_FALSE(embedding.build_request({.model = "model", .capability = AICapability::DECISION}).ok());
    BailianDecisionProvider provider;
    auto questions = parse_ai_decision_questions(R"({"q":{"type":"noul"}})");
    ASSERT_TRUE(questions.ok());
    AIDecisionInput input{"state", false, std::make_shared<const AIDecisionQuestions>(std::move(*questions))};
    for (const auto* key : {"secret\r\nX: injected", "secret\t", "secret\x7f"}) {
        EXPECT_FALSE(provider.build_request({.model = "model",
                                             .api_key = key,
                                             .capability = AICapability::DECISION,
                                             .decision = &input})
                             .ok());
    }
    EXPECT_FALSE(provider.build_request({.model = "model", .capability = AICapability::DECISION}).ok());
}

TEST(BailianDecisionProviderTest, RetainsZeroAndMaximumTrustedUsageWithoutChangingResponseMetadata) {
    BailianDecisionProvider provider;
    for (const auto* tokens : {"0", "9223372036854775807"}) {
        const std::string response =
                std::string(R"({"answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":)") + tokens +
                R"(},"model":"private-provider-model","request_id":"private-id"})";
        auto result = parse(provider, response, R"({"q":{"type":"noul"}})");
        ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
        const auto& success = std::get<AIProviderSuccess>(result);
        EXPECT_EQ(std::stoll(tokens), success.usage.prompt_tokens);
        EXPECT_EQ(std::stoll(tokens), success.usage.total_tokens);
        EXPECT_EQ(0, success.usage.completion_tokens);
        EXPECT_EQ(response, std::get<std::string>(success.value));
    }
    auto malformed =
            parse(provider, R"({"answers":{"q":{"type":"noul","noul":0.5,"noul":0.1}},"usage":{"input_tokens":7}})",
                  R"({"q":{"type":"noul"}})");
    ASSERT_TRUE(std::holds_alternative<AIProviderMalformed>(malformed));
    EXPECT_EQ(7, std::get<AIProviderMalformed>(malformed).usage.total_tokens);
}

TEST(BailianDecisionProviderTest, JsonAllocatorPropagatesRejectedAllocations) {
    struct RejectingAllocator {
        void* Malloc(size_t) { return nullptr; }
        void* Realloc(void*, size_t, size_t) { return nullptr; }
    };
    decision_json::AIJsonAllocator<RejectingAllocator> allocator;
    EXPECT_THROW(allocator.Malloc(64 * 1024), std::bad_alloc);
    EXPECT_THROW(allocator.Realloc(nullptr, 0, 64 * 1024), std::bad_alloc);
    EXPECT_EQ(nullptr, allocator.Malloc(0));
    EXPECT_EQ(nullptr, allocator.Realloc(nullptr, 0, 0));
}

TEST(BailianDecisionProviderTest, DeepStateAndResponseExtensionsAvoidRecursiveTraversal) {
    BailianDecisionProvider provider;
    constexpr size_t depth = 20000;
    const std::string nested = std::string(depth, '[') + "0" + std::string(depth, ']');
    auto request = build(provider, nested, true, R"({"q":{"type":"noul"}})");
    ASSERT_TRUE(request.ok()) << request.status();
    EXPECT_NE(std::string::npos, request->body.find(nested));
    const std::string response = R"({"answers":{"q":{"type":"noul","noul":0.5}},"extension":)" + nested + "}";
    auto result = provider.parse_response(response, AICapability::DECISION, request->response_context);
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
    EXPECT_EQ(response, std::get<std::string>(std::get<AIProviderSuccess>(result).value));
}

} // namespace starrocks
