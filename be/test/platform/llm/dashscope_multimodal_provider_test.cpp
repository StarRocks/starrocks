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

#include "platform/llm/dashscope_multimodal_provider.h"

#include <gtest/gtest.h>

#include <limits>

namespace starrocks {
namespace {

TEST(DashScopeMultimodalProviderTest, BuildsTextAndImageContentsWithParameters) {
    DashScopeMultimodalProvider provider;
    auto options = AIProviderOptions::create(
            {{.key = "dimension", .serialized_json = "512", .kind = AIProviderOptionKind::NUMBER},
             {.key = "metadata", .serialized_json = R"({"enabled":true})", .kind = AIProviderOptionKind::OBJECT}});
    ASSERT_TRUE(options.ok());
    AIChatRequest request{.endpoint = "https://provider.example/multimodal-embedding",
                          .model = "model",
                          .api_key = "key",
                          .prompt = "hello\nworld",
                          .options = &*options,
                          .capability = AICapability::TEXT_EMBEDDING};
    auto text = provider.build_request(request);
    ASSERT_TRUE(text.ok()) << text.status();
    EXPECT_EQ(request.endpoint, text->url);
    EXPECT_EQ(3, text->headers.size());
    EXPECT_EQ("Bearer key", text->headers.back().value);
    EXPECT_EQ(
            R"({"model":"model","input":{"contents":[{"text":"hello\nworld"}]},"parameters":{"dimension":512,"metadata":{"enabled":true}}})",
            text->body);
    AIMediaInput media{"image/jpeg", std::string("\xff\xd8\xff\xe0\0\x02", 6)};
    request.prompt = "";
    request.media = &media;
    request.options = nullptr;
    auto image = provider.build_request(request);
    ASSERT_TRUE(image.ok()) << image.status();
    EXPECT_EQ(R"({"model":"model","input":{"contents":[{"image":"data:image/jpeg;base64,/9j/4AAC"}]}})", image->body);
}

TEST(DashScopeMultimodalProviderTest, RejectsVideoChatAndImplicitFusion) {
    DashScopeMultimodalProvider provider;
    AIMediaInput image{"image/png", std::string("\x89PNG\r\n\x1a\n", 8)};
    AIChatRequest request{
            .model = "model", .prompt = "unexpected text", .capability = AICapability::TEXT_EMBEDDING, .media = &image};
    EXPECT_FALSE(provider.build_request(request).ok());
    request.prompt = "";
    request.capability = AICapability::CHAT;
    EXPECT_FALSE(provider.build_request(request).ok());
    request.capability = AICapability::TEXT_EMBEDDING;
    AIMediaInput video{"video/mp4", std::string("\0\0\0\x18"
                                                "ftypisom\0\0\0\0isommp42",
                                                24)};
    request.media = &video;
    EXPECT_TRUE(provider.build_request(request).status().is_not_supported());
}

TEST(DashScopeMultimodalProviderTest, RejectsReservedOptionsAndHeaderInjection) {
    DashScopeMultimodalProvider provider;
    for (const auto* key : {"model", "input", "contents", "parameters", "encoding_format", "messages", "stream"}) {
        auto options = AIProviderOptions::create(
                {{.key = key, .serialized_json = R"("secret-override")", .kind = AIProviderOptionKind::STRING}});
        ASSERT_TRUE(options.ok());
        auto result = provider.build_request(
                {.model = "model", .prompt = "text", .options = &*options, .capability = AICapability::TEXT_EMBEDDING});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(std::string::npos, result.status().message().find("secret-override"));
    }
    EXPECT_FALSE(provider.build_request({.model = "model",
                                         .api_key = "secret\r\nX-Key: bad",
                                         .prompt = "text",
                                         .capability = AICapability::TEXT_EMBEDDING})
                         .ok());
}

TEST(DashScopeMultimodalProviderTest, ParsesOneIndexedFiniteVectorAndOptionalUsage) {
    DashScopeMultimodalProvider provider;
    auto result = provider.parse_response(
            R"({"output":{"embeddings":[{"index":0,"embedding":[0,0.25,-2,3e-5],"type":"image"}]},"usage":{"input_tokens":0,"total_tokens":7}})",
            AICapability::TEXT_EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
    const auto& success = std::get<AIProviderSuccess>(result);
    EXPECT_EQ((std::vector<float>{0, 0.25f, -2, 3e-5f}), std::get<std::vector<float>>(success.value));
    EXPECT_EQ(0, success.usage.prompt_tokens);
    EXPECT_FALSE(success.usage.completion_tokens.has_value());
    EXPECT_EQ(7, success.usage.total_tokens);
    auto no_usage = provider.parse_response(R"({"output":{"embeddings":[{"index":0,"embedding":[1]}]}})",
                                            AICapability::TEXT_EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(no_usage));
    EXPECT_FALSE(std::get<AIProviderSuccess>(no_usage).usage.prompt_tokens.has_value());
}

TEST(DashScopeMultimodalProviderTest, NormalizesSplitAndInclusiveInputUsage) {
    DashScopeMultimodalProvider provider;
    const struct {
        const char* usage;
        int64_t prompt_tokens;
        std::optional<int64_t> total_tokens;
    } cases[] = {
            {R"({"input_tokens":43,"image_tokens":1247,"total_tokens":1290})", 1290, 1290},
            {R"({"input_tokens":9,"image_tokens":3})", 12, std::nullopt},
            {R"({"input_tokens":0,"image_tokens":0})", 0, std::nullopt},
            {R"({"input_tokens":9,"input_tokens_details":{"image_tokens":7,"text_tokens":2}})", 9, std::nullopt},
            {R"({"input_tokens":9223372036854775807,"image_tokens":0})", std::numeric_limits<int64_t>::max(),
             std::nullopt},
            {R"({"input_tokens":9223372036854775806,"image_tokens":1})", std::numeric_limits<int64_t>::max(),
             std::nullopt},
    };
    for (const auto& test : cases) {
        SCOPED_TRACE(test.usage);
        const std::string body =
                std::string(R"({"output":{"embeddings":[{"index":0,"embedding":[1]}]},"usage":)") + test.usage + "}";
        auto result = provider.parse_response(body, AICapability::TEXT_EMBEDDING);
        ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
        const auto& usage = std::get<AIProviderSuccess>(result).usage;
        EXPECT_EQ(test.prompt_tokens, usage.prompt_tokens);
        EXPECT_FALSE(usage.completion_tokens.has_value());
        EXPECT_EQ(test.total_tokens, usage.total_tokens);
    }
}

TEST(DashScopeMultimodalProviderTest, InvalidOrOverflowingImageUsageDoesNotReportPartialInputCount) {
    DashScopeMultimodalProvider provider;
    for (const auto* input_usage :
         {R"("input_tokens":9,"image_tokens":null)", R"("input_tokens":9,"image_tokens":-1)",
          R"("input_tokens":9,"image_tokens":0.5)", R"("input_tokens":9,"image_tokens":"3")",
          R"("input_tokens":9,"image_tokens":{})", R"("input_tokens":9,"image_tokens":9223372036854775808)",
          R"("input_tokens":9223372036854775807,"image_tokens":1)", R"("image_tokens":3)",
          R"("input_tokens":null,"image_tokens":3)", R"("input_tokens":-1,"image_tokens":3)"}) {
        SCOPED_TRACE(input_usage);
        const std::string body = std::string(R"({"output":{"embeddings":[{"index":0,"embedding":[1]}]},"usage":{)") +
                                 input_usage + R"(,"output_tokens":2,"total_tokens":19}})";
        auto result = provider.parse_response(body, AICapability::TEXT_EMBEDDING);
        ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
        const auto& usage = std::get<AIProviderSuccess>(result).usage;
        EXPECT_FALSE(usage.prompt_tokens.has_value());
        EXPECT_EQ(2, usage.completion_tokens);
        EXPECT_EQ(19, usage.total_tokens);
    }
}

TEST(DashScopeMultimodalProviderTest, PreservesSplitInputUsageOnErrorsAndMalformedOutput) {
    DashScopeMultimodalProvider provider;
    for (const auto* payload : {R"("code":"Throttling.RateQuota")", R"("error":{"code":"InternalError.Algo"})",
                                R"("output":{"embeddings":[]})"}) {
        SCOPED_TRACE(payload);
        const std::string body = std::string("{") + payload + R"(,"usage":{"input_tokens":43,"image_tokens":1247}})";
        auto result = provider.parse_response(body, AICapability::TEXT_EMBEDDING);
        ASSERT_FALSE(std::holds_alternative<AIProviderSuccess>(result));
        const auto& usage =
                std::visit([](const auto& parsed) -> const AIProviderUsage& { return parsed.usage; }, result);
        EXPECT_EQ(1290, usage.prompt_tokens);
        EXPECT_FALSE(usage.completion_tokens.has_value());
        EXPECT_FALSE(usage.total_tokens.has_value());
    }
}

TEST(DashScopeMultimodalProviderTest, RejectsInvalidEmbeddingShapeCardinalityIndexesAndValues) {
    DashScopeMultimodalProvider provider;
    for (const auto* entries : {"[]", "{}", "[null]", R"([{"index":0,"embedding":[1]},{"index":1,"embedding":[2]}])",
                                R"([{"embedding":[1]}])", R"([{"text_index":0,"embedding":[1]}])",
                                R"([{"index":1,"embedding":[1]}])", R"([{"index":-1,"embedding":[1]}])",
                                R"([{"index":"0","embedding":[1]}])", R"([{"index":0.5,"embedding":[1]}])",
                                R"([{"index":0,"embedding":[]}])", R"([{"index":0,"embedding":[null]}])",
                                R"([{"index":0,"embedding":[true]}])", R"([{"index":0,"embedding":["1"]}])",
                                R"([{"index":0,"embedding":[3.5e38]}])", R"([{"index":0,"embedding":[1e400]}])",
                                R"([{"index":0,"embedding":[NaN]}])", R"([{"index":0,"embedding":[Infinity]}])"}) {
        const std::string body = std::string(R"({"output":{"embeddings":)") + entries + "}}";
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
                provider.parse_response(body, AICapability::TEXT_EMBEDDING)))
                << body;
    }
    EXPECT_TRUE(
            std::holds_alternative<AIProviderMalformed>(provider.parse_response("{}", AICapability::TEXT_EMBEDDING)));
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
            provider.parse_response(R"({"output":{"embeddings":[{"index":0,"embedding":[1]}]}})", AICapability::CHAT)));
}

TEST(DashScopeMultimodalProviderTest, ErrorsPreserveClassificationAndUsageWithoutSensitiveMessages) {
    DashScopeMultimodalProvider provider;
    auto result = provider.parse_response(
            R"({"code":"Throttling.RateQuota","message":"private-secret","usage":{"input_tokens":9,"output_tokens":0}})",
            AICapability::TEXT_EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result));
    const auto& error = std::get<AIProviderStructuredError>(result);
    EXPECT_EQ(AIProviderErrorCode::THROTTLING, error.code);
    EXPECT_EQ(AIProviderErrorAction::THROTTLED, ai_provider_error_action(error.code));
    EXPECT_EQ(9, error.usage.prompt_tokens);
    EXPECT_EQ(0, error.usage.completion_tokens);
    EXPECT_FALSE(error.usage.total_tokens.has_value());
    auto retry = provider.parse_response(R"({"code":"InternalError.Algo"})", AICapability::TEXT_EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(retry));
    EXPECT_EQ(AIProviderErrorAction::RETRYABLE,
              ai_provider_error_action(std::get<AIProviderStructuredError>(retry).code));
}

TEST(AIProviderFactoryTest, ValidatesExplicitProtocolAndCapability) {
    EXPECT_TRUE(create_ai_provider("openai_compatible", AICapability::CHAT).ok());
    EXPECT_TRUE(create_ai_provider("openai_compatible", AICapability::TEXT_EMBEDDING).ok());
    EXPECT_TRUE(create_ai_provider("qwen_compatible", AICapability::CHAT).ok());
    EXPECT_FALSE(create_ai_provider("qwen_compatible", AICapability::TEXT_EMBEDDING).ok());
    EXPECT_TRUE(create_ai_provider("dashscope_multimodal", AICapability::TEXT_EMBEDDING).ok());
    EXPECT_FALSE(create_ai_provider("dashscope_multimodal", AICapability::CHAT).ok());
    EXPECT_FALSE(create_ai_provider("private-unknown-protocol", AICapability::CHAT).ok());
}

} // namespace
} // namespace starrocks
