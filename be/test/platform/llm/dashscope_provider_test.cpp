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

#include "platform/llm/dashscope_provider.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/pointer.h>

#include <limits>
#include <memory>
#include <string>
#include <utility>

namespace starrocks {
namespace {

using EndpointType = DashScopeProvider::EndpointType;

TEST(DashScopeProviderTest, BuildsTextAndImageContentsWithParameters) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING, 256);
    auto options = AIProviderOptions::create(
            {{.key = "dimensions", .serialized_json = "512", .kind = AIProviderOptionKind::NUMBER},
             {.key = "metadata", .serialized_json = R"({"enabled":true})", .kind = AIProviderOptionKind::OBJECT}});
    ASSERT_TRUE(options.ok());
    AIChatRequest request{.endpoint = "https://provider.example/multimodal-embedding",
                          .model = "model",
                          .api_key = "key",
                          .prompt = "hello\nworld",
                          .options = std::make_shared<const AIProviderOptions>(std::move(options).value()),
                          .capability = AICapability::EMBEDDING};
    auto text = provider.build_request(request);
    ASSERT_TRUE(text.ok()) << text.status();
    EXPECT_EQ(request.endpoint, text->url);
    EXPECT_EQ(3, text->headers.size());
    EXPECT_EQ("Bearer key", text->headers.back().value);
    EXPECT_EQ(
            R"({"model":"model","input":{"contents":[{"text":"hello\nworld"}]},"parameters":{"dimensions":512,"metadata":{"enabled":true},"dimension":256}})",
            text->body);
    auto media = std::make_shared<const AIMediaInput>(
            AIMediaInput{.content_type = "image/jpeg", .inline_bytes = std::string("\xff\xd8\xff\xe0\0\x02", 6)});
    request.prompt = "";
    request.media = media;
    request.options = nullptr;
    auto image = provider.build_request(request);
    ASSERT_TRUE(image.ok()) << image.status();
    EXPECT_EQ(
            R"({"model":"model","input":{"contents":[{"image":"data:image/jpeg;base64,/9j/4AAC"}]},"parameters":{"dimension":256}})",
            image->body);
}

TEST(DashScopeProviderTest, MultimodalEmbeddingRejectsInlineVideoChatAndImplicitFusion) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    auto image = std::make_shared<const AIMediaInput>(
            AIMediaInput{.content_type = "image/png", .inline_bytes = std::string("\x89PNG\r\n\x1a\n", 8)});
    AIChatRequest request{
            .model = "model", .prompt = "unexpected text", .capability = AICapability::EMBEDDING, .media = image};
    EXPECT_FALSE(provider.build_request(request).ok());
    request.prompt = "";
    request.capability = AICapability::CHAT;
    EXPECT_FALSE(provider.build_request(request).ok());
    request.capability = AICapability::EMBEDDING;
    auto video =
            std::make_shared<const AIMediaInput>(AIMediaInput{.content_type = "VIDEO/MP4",
                                                              .inline_bytes = std::string("\0\0\0\x18"
                                                                                          "ftypisom\0\0\0\0isommp42",
                                                                                          24)});
    request.media = video;
    EXPECT_TRUE(provider.build_request(request).status().is_not_supported());
    auto webm = std::make_shared<const AIMediaInput>(
            AIMediaInput{.content_type = "video/webm", .inline_bytes = "\x1a\x45\xdf\xa3"});
    request.media = webm;
    EXPECT_TRUE(provider.build_request(request).status().is_not_supported());
}

TEST(DashScopeProviderTest, TextAndMultimodalChatKeepTheirFormatForTextOnlyRows) {
    for (const auto endpoint_type : {EndpointType::TEXT_CHAT, EndpointType::MULTIMODAL_CHAT}) {
        DashScopeProvider provider(endpoint_type);
        const bool text_chat = endpoint_type == EndpointType::TEXT_CHAT;
        auto options = AIProviderOptions::create(
                {{.key = "max_tokens", .serialized_json = "128", .kind = AIProviderOptionKind::NUMBER}});
        ASSERT_TRUE(options.ok());
        auto request = provider.build_request(
                {.model = "row-model",
                 .prompt = "hello",
                 .options = std::make_shared<const AIProviderOptions>(std::move(options).value())});
        ASSERT_TRUE(request.ok()) << request.status();
        const auto content = text_chat ? R"("hello")" : R"([{"text":"hello"}])";
        EXPECT_EQ(std::string(R"({"model":"row-model","input":{"messages":[{"role":"user","content":)") + content +
                          R"(}]},"parameters":{"result_format":"message","max_tokens":128}})",
                  request->body);
        const std::pair<const char*, bool> replies[] = {{R"("answer")", true},
                                                        {R"([{"text":"an"},{"text":"swer"}])", !text_chat},
                                                        {"null", false},
                                                        {"[]", false},
                                                        {R"([{"text":1}])", false}};
        for (const auto& [reply, valid] : replies) {
            const auto body = std::string(R"({"output":{"choices":[{"message":{"content":)") + reply + "}}]}}";
            auto result = provider.parse_response(body);
            if (valid) {
                ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
                EXPECT_EQ("answer", std::get<std::string>(std::get<AIProviderSuccess>(result).value));
            } else {
                EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(result));
            }
        }
    }
}

TEST(DashScopeProviderTest, NativeChatResultFormatCannotChangeTheResponseSchema) {
    DashScopeProvider provider(EndpointType::TEXT_CHAT);
    for (const auto* value : {R"("message")", R"("text")"}) {
        auto options = AIProviderOptions::create(
                {{.key = "result_format", .serialized_json = value, .kind = AIProviderOptionKind::STRING}});
        ASSERT_TRUE(options.ok());
        auto request = provider.build_request(
                {.model = "m",
                 .prompt = "hello",
                 .options = std::make_shared<const AIProviderOptions>(std::move(options).value())});
        EXPECT_EQ(std::string_view(value) == R"("message")", request.ok());
    }
}

TEST(DashScopeProviderTest, SendsSignedMediaUrlsVerbatimAndInlineChatMediaAsDataUris) {
    for (const auto* content_type : {"image/jpeg", "video/mp4"}) {
        auto media = std::make_shared<const AIMediaInput>(
                AIMediaInput{.content_type = content_type,
                             .referenced_url = "https://bucket.example/object?x-signature=a%2Fb&expires=1"});
        const char* key = media->is_video() ? "video" : "image";
        const auto part = std::string(R"({")") + key + R"(":")" + *media->referenced_url + R"("})";
        for (auto endpoint_type : {EndpointType::TEXT_CHAT, EndpointType::MULTIMODAL_CHAT, EndpointType::TEXT_EMBEDDING,
                                   EndpointType::MULTIMODAL_EMBEDDING}) {
            const bool chat =
                    endpoint_type == EndpointType::TEXT_CHAT || endpoint_type == EndpointType::MULTIMODAL_CHAT;
            DashScopeProvider provider(endpoint_type);
            auto result = provider.build_request({.model = "m",
                                                  .prompt = chat ? "describe" : "",
                                                  .capability = chat ? AICapability::CHAT : AICapability::EMBEDDING,
                                                  .media = media});
            if (endpoint_type == EndpointType::TEXT_CHAT || endpoint_type == EndpointType::TEXT_EMBEDDING) {
                ASSERT_TRUE(result.status().is_not_supported());
                EXPECT_EQ(std::string::npos, result.status().message().find("signature"));
            } else {
                ASSERT_TRUE(result.ok()) << result.status();
                auto expected =
                        chat ? R"("content":[)" + part + R"(,{"text":"describe"}])" : R"("contents":[)" + part + "]";
                EXPECT_NE(std::string::npos, result->body.find(expected));
            }
        }
        auto inline_media =
                std::make_shared<const AIMediaInput>(AIMediaInput{.content_type = content_type, .inline_bytes = "abc"});
        auto result = DashScopeProvider(EndpointType::MULTIMODAL_CHAT)
                              .build_request({.model = "m", .prompt = "describe", .media = inline_media});
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_NE(std::string::npos,
                  result->body.find(std::string(R"(")") + key + R"(":"data:)" + content_type + ";base64,YWJj"));
    }
}

TEST(DashScopeProviderTest, TextEmbeddingUsesTextsAndAcceptsNativeIndexNames) {
    DashScopeProvider provider(EndpointType::TEXT_EMBEDDING, 1024);
    auto options = AIProviderOptions::create(
            {{.key = "dimension", .serialized_json = "256", .kind = AIProviderOptionKind::NUMBER}});
    ASSERT_TRUE(options.ok());
    auto request =
            provider.build_request({.model = "m",
                                    .prompt = "hello",
                                    .options = std::make_shared<const AIProviderOptions>(std::move(options).value()),
                                    .capability = AICapability::EMBEDDING});
    ASSERT_TRUE(request.ok()) << request.status();
    EXPECT_EQ(R"({"model":"m","input":{"texts":["hello"]},"parameters":{"dimension":256}})", request->body);
    for (const auto* key : {"text_index", "index"}) {
        auto body = std::string(R"({"output":{"embeddings":[{")") + key +
                    R"(":0,"embedding":[1,2]}]},"usage":{"prompt_tokens":5,"total_tokens":5}})";
        auto parsed = provider.parse_response(body, AICapability::EMBEDDING);
        ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(parsed));
        const auto& success = std::get<AIProviderSuccess>(parsed);
        EXPECT_EQ((std::vector<float>{1, 2}), std::get<std::vector<float>>(success.value));
        EXPECT_EQ(5, success.usage.prompt_tokens);
    }
    auto conflict = provider.parse_response(R"({"output":{"embeddings":[{"index":0,"text_index":1,"embedding":[1]}]}})",
                                            AICapability::EMBEDDING);
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(conflict));
}

TEST(DashScopeProviderTest, ForwardsDimensionAndDimensionsAsSeparateOptions) {
    auto options = AIProviderOptions::create(
            {{.key = "dimension", .serialized_json = "256", .kind = AIProviderOptionKind::NUMBER},
             {.key = "dimensions", .serialized_json = "512", .kind = AIProviderOptionKind::NUMBER}});
    ASSERT_TRUE(options.ok());
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    auto result =
            provider.build_request({.model = "m",
                                    .prompt = "hello",
                                    .options = std::make_shared<const AIProviderOptions>(std::move(options).value()),
                                    .capability = AICapability::EMBEDDING});
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(
            R"({"model":"m","input":{"contents":[{"text":"hello"}]},"parameters":{"dimension":256,"dimensions":512}})",
            result->body);
}

TEST(DashScopeProviderTest, PlacesVideoFpsInChatContentOrEmbeddingParameters) {
    auto options =
            AIProviderOptions::create({{.key = "fps", .serialized_json = "0.5", .kind = AIProviderOptionKind::NUMBER}});
    ASSERT_TRUE(options.ok());
    AIChatRequest request{.model = "m",
                          .options = std::make_shared<const AIProviderOptions>(std::move(options).value()),
                          .media = std::make_shared<const AIMediaInput>(AIMediaInput{
                                  .content_type = "video/mp4", .referenced_url = "https://example.com/clip.mp4"})};
    for (auto endpoint : {EndpointType::MULTIMODAL_CHAT, EndpointType::MULTIMODAL_EMBEDDING}) {
        const bool chat = endpoint == EndpointType::MULTIMODAL_CHAT;
        SCOPED_TRACE(chat ? "chat" : "embedding");
        request.capability = chat ? AICapability::CHAT : AICapability::EMBEDDING;
        request.prompt = chat ? "Describe the video" : "";
        auto result = DashScopeProvider(endpoint).build_request(request);
        ASSERT_TRUE(result.ok()) << result.status();
        rapidjson::Document body;
        body.Parse(result->body.c_str());
        ASSERT_TRUE(body.IsObject());
        const auto* media_fps =
                rapidjson::Pointer(chat ? "/input/messages/0/content/0/fps" : "/input/contents/0/fps").Get(body);
        const auto* parameter_fps = rapidjson::Pointer("/parameters/fps").Get(body);
        const auto* fps = chat ? media_fps : parameter_fps;
        ASSERT_NE(nullptr, fps);
        ASSERT_TRUE(fps->IsNumber());
        EXPECT_DOUBLE_EQ(0.5, fps->GetDouble());
        EXPECT_EQ(nullptr, chat ? parameter_fps : media_fps);
    }
}

TEST(DashScopeProviderTest, ChatUsageIncludesImageAndVideoEvenOnErrors) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_CHAT);
    for (const auto* payload : {R"("output":{"choices":[{"message":{"content":[{"text":"ok"}]}}]})",
                                R"("code":"Throttling.RateQuota")", R"("output":{"choices":[]})"}) {
        auto body = std::string("{") + payload +
                    R"(,"usage":{"input_tokens":30,"image_tokens":7,"video_tokens":19,"output_tokens":1}})";
        auto parsed = provider.parse_response(body);
        auto usage = std::visit([](const auto& result) { return result.usage; }, parsed);
        EXPECT_EQ(30, usage.prompt_tokens);
        EXPECT_EQ(1, usage.completion_tokens);
    }
}

TEST(DashScopeProviderTest, RejectsReservedOptions) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    for (const auto* key : {"model", "input", "contents", "parameters", "encoding_format", "messages", "stream"}) {
        auto options = AIProviderOptions::create(
                {{.key = key, .serialized_json = R"("secret-override")", .kind = AIProviderOptionKind::STRING}});
        ASSERT_TRUE(options.ok());
        auto result = provider.build_request(
                {.model = "model",
                 .prompt = "text",
                 .options = std::make_shared<const AIProviderOptions>(std::move(options).value()),
                 .capability = AICapability::EMBEDDING});
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(std::string::npos, result.status().message().find("secret-override"));
    }
}

TEST(DashScopeProviderTest, ParsesOneIndexedFiniteVectorAndOptionalUsage) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    auto result = provider.parse_response(
            R"({"output":{"embeddings":[{"index":0,"embedding":[0,0.25,-2,3e-5],"type":"image"}]},"usage":{"input_tokens":0,"total_tokens":7}})",
            AICapability::EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
    const auto& success = std::get<AIProviderSuccess>(result);
    EXPECT_EQ((std::vector<float>{0, 0.25f, -2, 3e-5f}), std::get<std::vector<float>>(success.value));
    EXPECT_EQ(0, success.usage.prompt_tokens);
    EXPECT_FALSE(success.usage.completion_tokens.has_value());
    EXPECT_EQ(7, success.usage.total_tokens);
    auto no_usage = provider.parse_response(R"({"output":{"embeddings":[{"index":0,"embedding":[1]}]}})",
                                            AICapability::EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(no_usage));
    EXPECT_FALSE(std::get<AIProviderSuccess>(no_usage).usage.prompt_tokens.has_value());
}

TEST(DashScopeProviderTest, NormalizesSplitAndInclusiveInputUsage) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    const struct {
        const char* usage;
        int64_t prompt_tokens;
        std::optional<int64_t> total_tokens;
    } cases[] = {
            {R"({"input_tokens":43,"image_tokens":1247,"total_tokens":1290})", 1290, 1290},
            {R"({"input_tokens":9,"image_tokens":3})", 12, std::nullopt},
            {R"({"input_tokens":2,"video_tokens":8})", 10, std::nullopt},
            {R"({"input_tokens":10,"video_tokens":8,"input_tokens_details":{"video_tokens":8}})", 10, std::nullopt},
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
        auto result = provider.parse_response(body, AICapability::EMBEDDING);
        ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
        const auto& usage = std::get<AIProviderSuccess>(result).usage;
        EXPECT_EQ(test.prompt_tokens, usage.prompt_tokens);
        EXPECT_FALSE(usage.completion_tokens.has_value());
        EXPECT_EQ(test.total_tokens, usage.total_tokens);
    }
}

TEST(DashScopeProviderTest, InvalidOrOverflowingImageUsageDoesNotReportPartialInputCount) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    for (const auto* input_usage :
         {R"("input_tokens":9,"image_tokens":null)", R"("input_tokens":9,"image_tokens":-1)",
          R"("input_tokens":9,"image_tokens":0.5)", R"("input_tokens":9,"image_tokens":"3")",
          R"("input_tokens":9,"image_tokens":{})", R"("input_tokens":9,"image_tokens":9223372036854775808)",
          R"("input_tokens":9223372036854775807,"image_tokens":1)", R"("image_tokens":3)",
          R"("input_tokens":null,"image_tokens":3)", R"("input_tokens":-1,"image_tokens":3)"}) {
        SCOPED_TRACE(input_usage);
        const std::string body = std::string(R"({"output":{"embeddings":[{"index":0,"embedding":[1]}]},"usage":{)") +
                                 input_usage + R"(,"output_tokens":2,"total_tokens":19}})";
        auto result = provider.parse_response(body, AICapability::EMBEDDING);
        ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(result));
        const auto& usage = std::get<AIProviderSuccess>(result).usage;
        EXPECT_FALSE(usage.prompt_tokens.has_value());
        EXPECT_EQ(2, usage.completion_tokens);
        EXPECT_EQ(19, usage.total_tokens);
    }
}

TEST(DashScopeProviderTest, PreservesSplitInputUsageOnErrorsAndMalformedOutput) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    for (const auto* payload : {R"("code":"Throttling.RateQuota")", R"("error":{"code":"InternalError.Algo"})",
                                R"("output":{"embeddings":[]})"}) {
        SCOPED_TRACE(payload);
        const std::string body = std::string("{") + payload + R"(,"usage":{"input_tokens":43,"image_tokens":1247}})";
        auto result = provider.parse_response(body, AICapability::EMBEDDING);
        ASSERT_FALSE(std::holds_alternative<AIProviderSuccess>(result));
        const auto& usage =
                std::visit([](const auto& parsed) -> const AIProviderUsage& { return parsed.usage; }, result);
        EXPECT_EQ(1290, usage.prompt_tokens);
        EXPECT_FALSE(usage.completion_tokens.has_value());
        EXPECT_FALSE(usage.total_tokens.has_value());
    }
}

TEST(DashScopeProviderTest, RejectsInvalidEmbeddingShapeCardinalityIndexesAndValues) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    for (const auto* entries : {"[]", "{}", "[null]", R"([{"index":0,"embedding":[1]},{"index":1,"embedding":[2]}])",
                                R"([{"embedding":[1]}])", R"([{"text_index":0,"embedding":[1]}])",
                                R"([{"index":1,"embedding":[1]}])", R"([{"index":-1,"embedding":[1]}])",
                                R"([{"index":"0","embedding":[1]}])", R"([{"index":0.5,"embedding":[1]}])",
                                R"([{"index":0,"embedding":[]}])", R"([{"index":0,"embedding":[null]}])",
                                R"([{"index":0,"embedding":[true]}])", R"([{"index":0,"embedding":["1"]}])",
                                R"([{"index":0,"embedding":[3.5e38]}])", R"([{"index":0,"embedding":[1e400]}])",
                                R"([{"index":0,"embedding":[NaN]}])", R"([{"index":0,"embedding":[Infinity]}])"}) {
        const std::string body = std::string(R"({"output":{"embeddings":)") + entries + "}}";
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(provider.parse_response(body, AICapability::EMBEDDING)))
                << body;
    }
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(provider.parse_response("{}", AICapability::EMBEDDING)));
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(
            provider.parse_response(R"({"output":{"embeddings":[{"index":0,"embedding":[1]}]}})", AICapability::CHAT)));
}

TEST(DashScopeProviderTest, ErrorsPreserveClassificationAndUsageWithoutSensitiveMessages) {
    DashScopeProvider provider(EndpointType::MULTIMODAL_EMBEDDING);
    auto result = provider.parse_response(
            R"({"code":"Throttling.RateQuota","message":"private-secret","usage":{"input_tokens":9,"output_tokens":0}})",
            AICapability::EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result));
    const auto& error = std::get<AIProviderStructuredError>(result);
    EXPECT_EQ(AIProviderErrorCode::THROTTLING, error.code);
    EXPECT_EQ(AIProviderErrorAction::THROTTLED, ai_provider_error_action(error.code));
    EXPECT_EQ(9, error.usage.prompt_tokens);
    EXPECT_EQ(0, error.usage.completion_tokens);
    EXPECT_FALSE(error.usage.total_tokens.has_value());
    auto retry = provider.parse_response(R"({"code":"InternalError.Algo"})", AICapability::EMBEDDING);
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(retry));
    EXPECT_EQ(AIProviderErrorAction::RETRYABLE,
              ai_provider_error_action(std::get<AIProviderStructuredError>(retry).code));
}

TEST(AIProviderFactoryTest, ValidatesExplicitProtocolAndCapability) {
    const auto create = [](AIProviderProtocol protocol, AICapability capability) {
        AIProviderConfig config{.endpoint = "https://unit.test.invalid/v1", .model = "test-model"};
        config.protocol = protocol;
        config.capability = capability;
        return create_ai_provider(config);
    };
    EXPECT_TRUE(create(AIProviderProtocol::OPENAI, AICapability::CHAT).ok());
    EXPECT_TRUE(create(AIProviderProtocol::OPENAI, AICapability::EMBEDDING).ok());
    EXPECT_TRUE(create(AIProviderProtocol::DASHSCOPE, AICapability::CHAT).status().is_invalid_argument());
    EXPECT_TRUE(create(static_cast<AIProviderProtocol>(99), AICapability::CHAT).status().is_not_supported());
    EXPECT_TRUE(create(AIProviderProtocol::OPENAI, static_cast<AICapability>(99)).status().is_not_supported());
}

} // namespace
} // namespace starrocks
