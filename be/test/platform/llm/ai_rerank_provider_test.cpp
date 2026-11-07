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
#include <rapidjson/document.h>

#include "platform/llm/ai_provider.h"

namespace starrocks {
namespace {

AIChatRequest request_for(const AIRerankInput& input, const AIProviderOptions* options = nullptr) {
    return {.endpoint = "https://rerank.invalid/custom/path",
            .model = "rerank-model",
            .api_key = "secret",
            .options = options,
            .capability = AICapability::RERANK,
            .rerank = &input};
}

std::string envelope(AIProviderProtocol protocol, std::string_view results, std::string_view usage = "") {
    if (protocol == AIProviderProtocol::COHERE) {
        return "{\"results\":" + std::string(results) + std::string(usage) + "}";
    }
    return "{\"output\":{\"results\":" + std::string(results) + "}" + std::string(usage) + "}";
}

class AIRerankProviderTest : public testing::TestWithParam<AIProviderProtocol> {};

TEST_P(AIRerankProviderTest, SerializesWholeCandidateGroupAndClampsTopN) {
    auto provider = create_ai_provider(GetParam(), AICapability::RERANK);
    ASSERT_TRUE(provider.ok()) << provider.status();
    auto options = AIProviderOptions::create({{"top_n", "5.0", AIProviderOptionKind::NUMBER},
                                              {"return_documents", "false", AIProviderOptionKind::FALSE_VALUE}});
    ASSERT_TRUE(options.ok());
    AIRerankInput input{"query\n中文", {"same", "same", ""}};
    auto request = (*provider)->build_request(request_for(input, &*options));
    ASSERT_TRUE(request.ok()) << request.status();
    EXPECT_EQ("https://rerank.invalid/custom/path", request->url);
    ASSERT_NE(nullptr, request->response_context.rerank_shape());
    EXPECT_EQ(3, request->response_context.rerank_shape()->candidate_count);
    EXPECT_EQ(3, request->response_context.rerank_shape()->expected_hits);
    EXPECT_EQ(0, request->response_context.retained_bytes());
    rapidjson::Document body;
    body.Parse(request->body.c_str());
    ASSERT_FALSE(body.HasParseError());
    EXPECT_STREQ("rerank-model", body["model"].GetString());
    const auto& data = GetParam() == AIProviderProtocol::COHERE ? body : body["input"];
    EXPECT_EQ(input.query, data["query"].GetString());
    ASSERT_EQ(3, data["documents"].Size());
    EXPECT_STREQ("same", data["documents"][0].GetString());
    EXPECT_STREQ("same", data["documents"][1].GetString());
    EXPECT_STREQ("", data["documents"][2].GetString());
    const auto& parameters = GetParam() == AIProviderProtocol::COHERE ? body : body["parameters"];
    EXPECT_EQ(3, parameters["top_n"].GetInt());
    EXPECT_FALSE(parameters["return_documents"].GetBool());
    auto output = (*provider)->parse_response(
            envelope(
                    GetParam(),
                    R"([{"index":2,"relevance_score":0.123456789012345},{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":0.1}])"),
            AICapability::RERANK, request->response_context);
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(output));
    const auto& hits = std::get<AIRerankResult>(std::get<AIProviderSuccess>(output).value);
    ASSERT_EQ(3, hits.size());
    EXPECT_EQ(2, hits[0].index);
    EXPECT_DOUBLE_EQ(0.123456789012345, hits[0].relevance_score);
}

TEST_P(AIRerankProviderTest, RejectsForeignInputsReservedOptionsAndInvalidEncoding) {
    auto provider = create_ai_provider(GetParam(), AICapability::RERANK);
    ASSERT_TRUE(provider.ok());
    AIRerankInput input{"query", {"doc"}};
    auto request = request_for(input);
    request.capability = AICapability::CHAT;
    EXPECT_FALSE((*provider)->build_request(request).ok());
    request = request_for(input);
    request.rerank = nullptr;
    EXPECT_FALSE((*provider)->build_request(request).ok());
    request = request_for(input);
    request.api_key = "key\nsecret";
    EXPECT_FALSE((*provider)->build_request(request).ok());
    for (const auto* key : {"model", "input", "query", "documents", "parameters"}) {
        auto options = AIProviderOptions::create({{key, "1", AIProviderOptionKind::NUMBER}});
        ASSERT_TRUE(options.ok());
        EXPECT_FALSE((*provider)->build_request(request_for(input, &*options)).ok());
    }
    input.query = std::string("\xff", 1);
    EXPECT_FALSE((*provider)->build_request(request_for(input)).ok());
    input.query = "query";
    input.documents[0] = std::string("\xff", 1);
    EXPECT_FALSE((*provider)->build_request(request_for(input)).ok());
}

TEST_P(AIRerankProviderTest, RejectsMalformedRankingsWithoutRepair) {
    auto provider = create_ai_provider(GetParam(), AICapability::RERANK);
    ASSERT_TRUE(provider.ok());
    AIProviderResponseContext context(AIRerankResponseShape{3, 2});
    for (const auto* results : {R"([])", R"([{"index":0,"relevance_score":1}])",
                                R"([{"index":0,"relevance_score":1},{"index":0,"relevance_score":0}])",
                                R"([{"index":3,"relevance_score":1},{"index":0,"relevance_score":0}])",
                                R"([{"index":-1,"relevance_score":1},{"index":0,"relevance_score":0}])",
                                R"([{"index":2147483648,"relevance_score":1},{"index":0,"relevance_score":0}])",
                                R"([{"index":0.5,"relevance_score":1},{"index":1,"relevance_score":0}])",
                                R"([{"index":0,"relevance_score":0},{"index":1,"relevance_score":1}])",
                                R"([{"index":0,"relevance_score":"1"},{"index":1,"relevance_score":0}])",
                                R"([{"index":0,"index":1,"relevance_score":1},{"index":2,"relevance_score":0}])"}) {
        auto parsed = (*provider)->parse_response(envelope(GetParam(), results), AICapability::RERANK, context);
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(parsed)) << results;
    }
    const auto valid = envelope(GetParam(), R"([{"index":0,"relevance_score":1},{"index":1,"relevance_score":0}])");
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>((*provider)->parse_response(valid, AICapability::RERANK)));
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
            (*provider)->parse_response(valid, AICapability::CHAT, context)));
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
            (*provider)->parse_response("{\"bad\":\"\xff\"}", AICapability::RERANK, context)));
}

TEST_P(AIRerankProviderTest, PreservesOnlyActualUsageOnSuccessAndMalformed) {
    auto provider = create_ai_provider(GetParam(), AICapability::RERANK);
    ASSERT_TRUE(provider.ok());
    const std::string usage = GetParam() == AIProviderProtocol::COHERE
                                      ? R"(,"meta":{"billed_units":{"input_tokens":192,"search_units":1}})"
                                      : R"(,"usage":{"total_tokens":192})";
    for (const auto* results : {R"([{"index":0,"relevance_score":1}])", R"([])"}) {
        auto result = (*provider)->parse_response(envelope(GetParam(), results, usage), AICapability::RERANK,
                                                  AIProviderResponseContext(AIRerankResponseShape{1, 1}));
        auto actual = std::visit([](const auto& value) { return value.usage; }, result);
        EXPECT_FALSE(actual.completion_tokens.has_value());
        if (GetParam() == AIProviderProtocol::COHERE) {
            EXPECT_EQ(192, actual.prompt_tokens);
            EXPECT_FALSE(actual.total_tokens.has_value());
        } else {
            EXPECT_EQ(192, actual.total_tokens);
            EXPECT_FALSE(actual.prompt_tokens.has_value());
        }
    }
}

INSTANTIATE_TEST_SUITE_P(Protocols, AIRerankProviderTest,
                         testing::Values(AIProviderProtocol::COHERE, AIProviderProtocol::DASHSCOPE_RERANK));

TEST(AIRerankProviderCompatibilityTest, ExistingProvidersRejectRerankPayloads) {
    AIRerankInput input{"query", {"doc"}};
    for (const auto& pair : {std::pair{AIProviderProtocol::OPENAI, AICapability::CHAT},
                             std::pair{AIProviderProtocol::OPENAI, AICapability::EMBEDDING},
                             std::pair{AIProviderProtocol::DASHSCOPE_MULTIMODAL, AICapability::EMBEDDING}}) {
        auto provider = create_ai_provider(pair.first, pair.second);
        ASSERT_TRUE(provider.ok());
        auto request = request_for(input);
        request.capability = pair.second;
        EXPECT_FALSE((*provider)->build_request(request).ok());
    }
}

TEST(AIRerankProviderUsageTest, CohereSearchUnitsAreNotTokensAndRealSourcesAreNotAdded) {
    auto provider = create_ai_provider(AIProviderProtocol::COHERE, AICapability::RERANK);
    ASSERT_TRUE(provider.ok());
    auto result = (*provider)->parse_response(R"({"message":"bad request","meta":{"billed_units":{"search_units":1}}})",
                                              AICapability::RERANK);
    auto usage = std::visit([](const auto& value) { return value.usage; }, result);
    EXPECT_FALSE(usage.prompt_tokens);
    EXPECT_FALSE(usage.completion_tokens);
    EXPECT_FALSE(usage.total_tokens);
    result = (*provider)->parse_response(
            R"({"error":{"code":"rate_limit_exceeded"},"meta":{"billed_units":{"input_tokens":12,"output_tokens":0},"tokens":{"input_tokens":99,"output_tokens":99}}})",
            AICapability::RERANK);
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result));
    usage = std::get<AIProviderStructuredError>(result).usage;
    EXPECT_EQ(12, usage.prompt_tokens);
    EXPECT_EQ(0, usage.completion_tokens);
    EXPECT_FALSE(usage.total_tokens);
}

TEST(AIRerankProviderUsageTest, CohereFallsBackPerTokenFieldWithoutCountingSearchUnits) {
    auto provider = create_ai_provider(AIProviderProtocol::COHERE, AICapability::RERANK);
    ASSERT_TRUE(provider.ok());
    auto result = (*provider)->parse_response(
            R"({"message":"bad request","meta":{"billed_units":{"search_units":1,"input_tokens":12},"tokens":{"input_tokens":99,"output_tokens":3}}})",
            AICapability::RERANK);
    auto usage = std::visit([](const auto& value) { return value.usage; }, result);
    EXPECT_EQ(12, usage.prompt_tokens);
    EXPECT_EQ(3, usage.completion_tokens);
    EXPECT_FALSE(usage.total_tokens);
    result = (*provider)->parse_response(
            R"({"results":[],"meta":{"billed_units":{"search_units":1},"tokens":{"input_tokens":99,"output_tokens":0}}})",
            AICapability::RERANK);
    usage = std::visit([](const auto& value) { return value.usage; }, result);
    EXPECT_EQ(99, usage.prompt_tokens);
    EXPECT_EQ(0, usage.completion_tokens);
    EXPECT_FALSE(usage.total_tokens);
}

TEST(AIRerankProviderUsageTest, DashScopeUsageSurvivesStructuredErrorsAndRejectsInvalidCounts) {
    auto provider = create_ai_provider(AIProviderProtocol::DASHSCOPE_RERANK, AICapability::RERANK);
    ASSERT_TRUE(provider.ok());
    auto result = (*provider)->parse_response(
            R"({"code":"Throttling.RateLimit","message":"secret","usage":{"prompt_tokens":5,"input_tokens":7,"output_tokens":-1,"total_tokens":9223372036854775808}})",
            AICapability::RERANK);
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result));
    const auto& error = std::get<AIProviderStructuredError>(result);
    EXPECT_EQ(AIProviderErrorCode::THROTTLING, error.code);
    EXPECT_EQ(5, error.usage.prompt_tokens);
    EXPECT_FALSE(error.usage.completion_tokens);
    EXPECT_FALSE(error.usage.total_tokens);
}

} // namespace
} // namespace starrocks
