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

#include "platform/llm/openai_compatible_provider.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace starrocks {
namespace {

AIProviderOptions make_json_options(std::initializer_list<AIProviderOption> members) {
    auto options = AIProviderOptions::create(AIProviderOptions::Members(members));
    EXPECT_TRUE(options.ok()) << options.status();
    return options.ok() ? std::move(options).value() : AIProviderOptions{};
}

void expect_status_redacted(const Status& status, std::initializer_list<std::string> sentinels) {
    ASSERT_FALSE(status.ok());
    const std::string message(status.message());
    for (const auto& sentinel : sentinels) {
        EXPECT_EQ(std::string::npos, message.find(sentinel)) << message;
    }
}

std::string structured_envelope(bool nested, std::string_view members) {
    if (nested) {
        return R"({"error":{)" + std::string(members) + "}}";
    }
    return "{" + std::string(members) + "}";
}

TEST(OpenAICompatibleProviderTest, BuildsExactCanonicalRequestAndOnlyApprovedHeaders) {
    OpenAICompatibleProvider provider;

    auto result = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/v1/chat/completions",
            .model = "model-a",
            .api_key = "api-key-a",
            .prompt = "hello",
            .options = nullptr,
    });

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ("https://provider.example/v1/chat/completions", result->url);
    ASSERT_EQ(3, result->headers.size());
    for (const auto& header : result->headers) {
        EXPECT_TRUE(header.name == "Content-Type" || header.name == "Accept" || header.name == "Authorization")
                << header.name;
    }
    const auto header_value = [&](std::string_view name) -> const std::string& {
        auto it = std::find_if(result->headers.begin(), result->headers.end(),
                               [name](const AIHttpHeader& header) { return header.name == name; });
        CHECK(it != result->headers.end());
        return it->value;
    };
    EXPECT_EQ(1, std::count_if(result->headers.begin(), result->headers.end(),
                               [](const AIHttpHeader& header) { return header.name == "Content-Type"; }));
    EXPECT_EQ(1, std::count_if(result->headers.begin(), result->headers.end(),
                               [](const AIHttpHeader& header) { return header.name == "Accept"; }));
    EXPECT_EQ(1, std::count_if(result->headers.begin(), result->headers.end(),
                               [](const AIHttpHeader& header) { return header.name == "Authorization"; }));
    EXPECT_EQ("application/json", header_value("Content-Type"));
    EXPECT_EQ("application/json", header_value("Accept"));
    EXPECT_EQ("Bearer api-key-a", header_value("Authorization"));
    EXPECT_EQ(
            R"({"model":"model-a","messages":[{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":"hello"}],"stream":false})",
            result->body);
}

TEST(OpenAICompatibleProviderTest, MergesPreparedOptionsOnlyAtTheTopLevel) {
    OpenAICompatibleProvider provider;
    auto options = make_json_options({
            {.key = "temperature", .serialized_json = "0.25", .kind = AIProviderOptionKind::NUMBER},
            {.key = "response_format",
             .serialized_json = R"({"type":"json_object"})",
             .kind = AIProviderOptionKind::OBJECT},
    });

    auto result = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/chat",
            .model = "model-a",
            .api_key = "api-key-a",
            .prompt = "hello",
            .options = &options,
    });

    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(
            R"({"model":"model-a","messages":[{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":"hello"}],"stream":false,"temperature":0.25,"response_format":{"type":"json_object"}})",
            result->body);
}

TEST(AIProviderOptionsTest, RejectsInvalidOrAmbiguousPreparedMembers) {
    const std::vector<AIProviderOptions::Members> invalid_options = {
            {{.key = "temperature", .serialized_json = "0.25", .kind = AIProviderOptionKind::NUMBER},
             {.key = "temperature", .serialized_json = "0.5", .kind = AIProviderOptionKind::NUMBER}},
            {{.key = "temperature", .serialized_json = "not-json", .kind = AIProviderOptionKind::NUMBER}},
            {{.key = "temperature", .serialized_json = R"("0.25")", .kind = AIProviderOptionKind::NUMBER}},
    };

    for (const auto& members : invalid_options) {
        auto options = AIProviderOptions::create(members);
        EXPECT_TRUE(options.status().is_invalid_argument()) << options.status();
    }
}

TEST(OpenAICompatibleProviderTest, RejectsReservedOptionsAtTheProviderMergeBoundary) {
    OpenAICompatibleProvider provider;
    for (const std::string_view key : {"model", "messages", "stream"}) {
        auto options = make_json_options(
                {{.key = std::string(key), .serialized_json = R"("override")", .kind = AIProviderOptionKind::STRING}});
        auto request = provider.build_request(AIChatRequest{
                .endpoint = "https://provider.example/chat",
                .model = "model-a",
                .api_key = "api-key-a",
                .prompt = "hello",
                .options = &options,
        });
        EXPECT_TRUE(request.status().is_invalid_argument()) << request.status();
    }
}

TEST(OpenAICompatibleProviderTest, EmptyPromptIsValidAndPromptBytesAreJsonEscaped) {
    OpenAICompatibleProvider provider;

    auto empty = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/chat",
            .model = "model-a",
            .api_key = "api-key-a",
            .prompt = "",
            .options = nullptr,
    });
    ASSERT_TRUE(empty.ok()) << empty.status().message();
    EXPECT_NE(std::string::npos, empty->body.find(R"("role":"user","content":""})"));

    const std::string prompt("a\0b\"\\\n", 6);
    auto escaped = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/chat",
            .model = "model-a",
            .api_key = "api-key-a",
            .prompt = prompt,
            .options = nullptr,
    });
    ASSERT_TRUE(escaped.ok()) << escaped.status().message();
    EXPECT_NE(std::string::npos, escaped->body.find(R"("content":"a\u0000b\"\\\n"})"));
}

TEST(OpenAICompatibleProviderTest, InvalidUtf8InJsonInputIsRejectedWithoutEchoingBytes) {
    OpenAICompatibleProvider provider;
    const std::string invalid_utf8("\xC3\x28", 2);

    auto request = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/chat",
            .model = "model-a",
            .api_key = "api-key-a",
            .prompt = invalid_utf8,
            .options = nullptr,
    });
    ASSERT_FALSE(request.ok());
    expect_status_redacted(request.status(), {invalid_utf8});

    const std::string response = R"({"choices":[{"message":{"content":")" + invalid_utf8 + R"("}}]})";
    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(provider.parse_response(response)));
}

TEST(OpenAICompatibleProviderTest, RejectsEmptyModelWithASecretFreeFixedError) {
    OpenAICompatibleProvider provider;
    auto options = make_json_options(
            {{.key = "user_option", .serialized_json = R"("option-secret")", .kind = AIProviderOptionKind::STRING}});

    auto result = provider.build_request(AIChatRequest{
            .endpoint = "https://endpoint-secret.example/chat",
            .model = "",
            .api_key = "api-key-secret",
            .prompt = "prompt-secret",
            .options = &options,
    });

    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
    EXPECT_EQ("AI provider model is empty", result.status().message());
    expect_status_redacted(result.status(),
                           {"endpoint-secret", "api-key-secret", "prompt-secret", "user_option", "option-secret"});
}

TEST(OpenAICompatibleProviderTest, RejectsEveryC0AndDelApiKeyWithTheSameSecretFreeError) {
    OpenAICompatibleProvider provider;
    auto options = make_json_options(
            {{.key = "user_option", .serialized_json = R"("option-secret")", .kind = AIProviderOptionKind::STRING}});
    std::vector<std::string> invalid_keys{""};
    for (int byte = 0; byte <= 0x1f; ++byte) {
        std::string key = "api-key-secret";
        key.push_back(static_cast<char>(byte));
        key.append("tail");
        invalid_keys.emplace_back(std::move(key));
    }
    invalid_keys.emplace_back(
            "api-key-secret\x7f"
            "tail");

    for (const auto& key : invalid_keys) {
        auto result = provider.build_request(AIChatRequest{
                .endpoint = "https://endpoint-secret.example/chat",
                .model = "model-secret",
                .api_key = key,
                .prompt = "prompt-secret",
                .options = &options,
        });
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_invalid_argument());
        EXPECT_EQ("AI provider API key is invalid", result.status().message());
        expect_status_redacted(result.status(), {"endpoint-secret", "model-secret", "api-key-secret", "prompt-secret",
                                                 "user_option", "option-secret"});
    }
}

TEST(OpenAICompatibleProviderTest, SpaceInApiKeyIsNotAControlCharacter) {
    OpenAICompatibleProvider provider;
    auto result = provider.build_request(AIChatRequest{
            .endpoint = "https://provider.example/chat",
            .model = "model-a",
            .api_key = "api key with spaces",
            .prompt = "hello",
            .options = nullptr,
    });

    ASSERT_TRUE(result.ok()) << result.status().message();
}

TEST(OpenAICompatibleProviderTest, ParsesTheFirstChoiceAndPreservesEmptyOrNulContent) {
    OpenAICompatibleProvider provider;

    auto first = provider.parse_response(
            R"({"choices":[{"message":{"content":"first"}},{"message":{"content":"second"}}]})");
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(first));
    EXPECT_EQ("first", std::get<AIProviderSuccess>(first).content);

    auto empty = provider.parse_response(R"({"choices":[{"message":{"content":""}}]})");
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(empty));
    EXPECT_TRUE(std::get<AIProviderSuccess>(empty).content.empty());

    auto with_nul = provider.parse_response(R"({"choices":[{"message":{"content":"a\u0000b"}}]})");
    ASSERT_TRUE(std::holds_alternative<AIProviderSuccess>(with_nul));
    EXPECT_EQ(std::string("a\0b", 3), std::get<AIProviderSuccess>(with_nul).content);
}

TEST(OpenAICompatibleProviderTest, MalformedSuccessShapesReturnOnlyTheMalformedKind) {
    OpenAICompatibleProvider provider;
    const std::array<std::string_view, 8> malformed = {
            "not-json",
            R"({})",
            R"({"choices":[]})",
            R"({"choices":{}})",
            R"({"choices":[{}]})",
            R"({"choices":[{"message":{}}]})",
            R"({"choices":[{"message":{"content":null}}]})",
            R"({"choices":[{"message":{"content":7}}]})",
    };

    for (std::string_view body : malformed) {
        auto result = provider.parse_response(body);
        EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(result)) << body;
    }
}

TEST(OpenAICompatibleProviderTest, EmbeddedNulInRawResponseBodyIsMalformedInsteadOfPrefixParsed) {
    OpenAICompatibleProvider provider;
    std::string body = R"({"choices":[{"message":{"content":"prefix"}}]})";
    body.push_back('\0');
    body.append(R"({"error":{"code":"rate_limit_exceeded"}})");

    auto result = provider.parse_response(body);

    EXPECT_TRUE(std::holds_alternative<AIProviderMalformed>(result));
}

TEST(OpenAICompatibleProviderTest, ParsesNestedAndTopLevelStructuredErrorEnvelopes) {
    OpenAICompatibleProvider provider;

    auto nested = provider.parse_response(
            R"({"error":{"message":"raw-message-secret","type":"server_error","code":"rate_limit_exceeded"}})");
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(nested));
    const auto nested_code = std::get<AIProviderStructuredError>(nested).code;
    EXPECT_EQ(AIProviderErrorCode::RATE_LIMIT_EXCEEDED, nested_code);
    EXPECT_EQ(AIProviderErrorAction::THROTTLED, ai_provider_error_action(nested_code));

    auto top_level = provider.parse_response(
            R"({"code":"InternalError.ProviderBusy","type":"invalid_request_error","message":"body-secret"})");
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(top_level));
    const auto top_level_code = std::get<AIProviderStructuredError>(top_level).code;
    EXPECT_EQ(AIProviderErrorCode::INTERNAL_ERROR, top_level_code);
    EXPECT_EQ(AIProviderErrorAction::RETRYABLE, ai_provider_error_action(top_level_code));
}

TEST(OpenAICompatibleProviderTest, NestedAndTopLevelStructuredErrorsTakePrecedenceOverChoices) {
    OpenAICompatibleProvider provider;
    const std::array<std::pair<std::string_view, AIProviderErrorCode>, 2> cases = {{
            {R"({"choices":[{"message":{"content":"must-not-win"}}],"error":{"code":"server_error"}})",
             AIProviderErrorCode::SERVER_ERROR},
            {R"({"choices":[{"message":{"content":"must-not-win"}}],"code":"rate_limit_exceeded","type":"server_error"})",
             AIProviderErrorCode::RATE_LIMIT_EXCEEDED},
    }};

    for (const auto& [body, expected_code] : cases) {
        auto result = provider.parse_response(body);
        ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result)) << body;
        EXPECT_EQ(expected_code, std::get<AIProviderStructuredError>(result).code) << body;
    }
}

TEST(OpenAICompatibleProviderTest, CodeHasPriorityAndOnlyUnusableCodeFallsBackToType) {
    OpenAICompatibleProvider provider;
    const std::string overlong(129, 'a');
    const std::vector<std::string> unusable_code_members = {
            R"("type":"rate_limit_exceeded","message":"secret")",
            R"("code":null,"type":"rate_limit_exceeded","message":"secret")",
            R"("code":7,"type":"rate_limit_exceeded","message":"secret")",
            R"("code":"","type":"rate_limit_exceeded","message":"secret")",
            R"("code":"bad code","type":"rate_limit_exceeded","message":"secret")",
            R"("code":"bad\ncode","type":"rate_limit_exceeded","message":"secret")",
            R"("code":")" + overlong + R"(","type":"rate_limit_exceeded","message":"secret")",
    };

    for (bool nested : {false, true}) {
        auto known_code = provider.parse_response(structured_envelope(
                nested, R"("code":"server_error","type":"rate_limit_exceeded","message":"secret")"));
        ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(known_code));
        EXPECT_EQ(AIProviderErrorCode::SERVER_ERROR, std::get<AIProviderStructuredError>(known_code).code);

        auto usable_unknown_code = provider.parse_response(structured_envelope(
                nested, R"("code":"unknown_but_safe","type":"rate_limit_exceeded","message":"secret")"));
        ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(usable_unknown_code));
        EXPECT_EQ(AIProviderErrorCode::UNKNOWN, std::get<AIProviderStructuredError>(usable_unknown_code).code);

        for (const auto& members : unusable_code_members) {
            const std::string body = structured_envelope(nested, members);
            auto fallback = provider.parse_response(body);
            ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(fallback)) << body;
            EXPECT_EQ(AIProviderErrorCode::RATE_LIMIT_EXCEEDED, std::get<AIProviderStructuredError>(fallback).code)
                    << body;
        }
    }
}

TEST(OpenAICompatibleProviderTest, AllowlistMatchingIsAsciiCaseInsensitiveWithStrictPrefixBoundaries) {
    struct Case {
        std::string_view identifier;
        AIProviderErrorCode code;
        AIProviderErrorAction action;
    };
    const std::array<Case, 11> cases = {{
            {"RaTe_LiMiT_ExCeEdEd", AIProviderErrorCode::RATE_LIMIT_EXCEEDED, AIProviderErrorAction::THROTTLED},
            {"TOO_MANY_REQUESTS", AIProviderErrorCode::TOO_MANY_REQUESTS, AIProviderErrorAction::THROTTLED},
            {"thROTTLING.RateQuota", AIProviderErrorCode::THROTTLING, AIProviderErrorAction::THROTTLED},
            {"RateLimit.User", AIProviderErrorCode::RATE_LIMIT, AIProviderErrorAction::THROTTLED},
            {"server_ERROR", AIProviderErrorCode::SERVER_ERROR, AIProviderErrorAction::RETRYABLE},
            {"internal_error", AIProviderErrorCode::INTERNAL_ERROR, AIProviderErrorAction::RETRYABLE},
            {"InternalError.Provider", AIProviderErrorCode::INTERNAL_ERROR, AIProviderErrorAction::RETRYABLE},
            {"SERVICE_UNAVAILABLE", AIProviderErrorCode::SERVICE_UNAVAILABLE, AIProviderErrorAction::RETRYABLE},
            {"ServiceUnavailable.Region", AIProviderErrorCode::SERVICE_UNAVAILABLE, AIProviderErrorAction::RETRYABLE},
            {"TimeOut", AIProviderErrorCode::TIMEOUT, AIProviderErrorAction::RETRYABLE},
            {"API_CONNECTION_ERROR", AIProviderErrorCode::API_CONNECTION_ERROR, AIProviderErrorAction::RETRYABLE},
    }};
    OpenAICompatibleProvider provider;

    for (const auto& test_case : cases) {
        const std::string body = R"({"error":{"code":")" + std::string(test_case.identifier) + R"("}})";
        auto result = provider.parse_response(body);
        ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result)) << test_case.identifier;
        const auto code = std::get<AIProviderStructuredError>(result).code;
        EXPECT_EQ(test_case.code, code) << test_case.identifier;
        EXPECT_EQ(test_case.action, ai_provider_error_action(code)) << test_case.identifier;
    }
}

TEST(OpenAICompatibleProviderTest, SubstringsAndMalformedPrefixLookalikesRemainUnknown) {
    OpenAICompatibleProvider provider;
    for (std::string_view identifier :
         {"xrate_limit_exceeded", "rate_limit_exceeded_suffix", "ThrottlingEvil", "Throttling", "RateLimit",
          "InternalErrorx", "ServiceUnavailableEvil", "service_unavailable.extra"}) {
        const std::string body = R"({"error":{"code":")" + std::string(identifier) + R"("}})";
        auto result = provider.parse_response(body);
        ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result)) << identifier;
        const auto code = std::get<AIProviderStructuredError>(result).code;
        EXPECT_EQ(AIProviderErrorCode::UNKNOWN, code) << identifier;
        EXPECT_EQ(AIProviderErrorAction::TERMINAL, ai_provider_error_action(code)) << identifier;
    }
}

TEST(OpenAICompatibleProviderTest, UnknownIdentifiersAndRawMessagesAreNeverRetainedOrUsed) {
    OpenAICompatibleProvider provider;
    const std::string secret = "raw-provider-message-and-body-secret";

    auto unknown = provider.parse_response(R"({"error":{"code":"safe_unknown","message":")" + secret +
                                           R"( rate_limit_exceeded"},"body":"another-secret"})");
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(unknown));
    const auto unknown_code = std::get<AIProviderStructuredError>(unknown).code;
    EXPECT_EQ(AIProviderErrorCode::UNKNOWN, unknown_code);
    EXPECT_EQ(AIProviderErrorAction::TERMINAL, ai_provider_error_action(unknown_code));

    auto message_only = provider.parse_response(R"({"error":{"message":"rate_limit_exceeded )" + secret + R"("}})");
    ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(message_only));
    EXPECT_EQ(AIProviderErrorCode::UNKNOWN, std::get<AIProviderStructuredError>(message_only).code);
}

TEST(OpenAICompatibleProviderTest, IdentifierSafetyIsBoundedToAsciiLettersDigitsDotUnderscoreAndDash) {
    OpenAICompatibleProvider provider;
    const std::array<std::string, 4> unsafe = {"server error", "server/error", "server:error", "s\xC3\xA9rver_error"};

    for (const auto& identifier : unsafe) {
        const std::string body = R"({"error":{"code":")" + identifier + R"(","type":"timeout"}})";
        auto result = provider.parse_response(body);
        ASSERT_TRUE(std::holds_alternative<AIProviderStructuredError>(result)) << identifier;
        EXPECT_EQ(AIProviderErrorCode::TIMEOUT, std::get<AIProviderStructuredError>(result).code) << identifier;
    }
}

} // namespace
} // namespace starrocks
