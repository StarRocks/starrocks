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

#include "exec/pipeline/ai/ai_provider_config_parser.h"

#include <gtest/gtest.h>

namespace starrocks::pipeline {
namespace {

TAIProvider rerank_provider() {
    TAIProvider provider;
    provider.__set_id("provider:rerank");
    provider.__set_type(TAIProviderType::RERANK);
    provider.__set_protocol(TAIProviderProtocol::COHERE);
    provider.__set_params({{"endpoint", "https://127.0.0.1/v2/rerank"},
                           {"model", "reranker"},
                           {"api_key", "secret-sentinel"},
                           {"max_documents", "+10"},
                           {"deadline_ms", "2147483647"}});
    return provider;
}

TEST(AIProviderConfigParserTest, RerankAcceptsBothNativeProtocolsAndPositiveIntBounds) {
    for (const auto protocol : {TAIProviderProtocol::COHERE, TAIProviderProtocol::DASHSCOPE_RERANK}) {
        auto provider = rerank_provider();
        provider.__set_protocol(protocol);
        auto result = AIProviderConfigParser::parse(provider.id, provider);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(AICapability::RERANK, result->capability);
        EXPECT_EQ(protocol == TAIProviderProtocol::COHERE ? AIProviderProtocol::COHERE
                                                          : AIProviderProtocol::DASHSCOPE_RERANK,
                  result->protocol);
        EXPECT_EQ(10, result->max_documents.value());
        EXPECT_EQ(2147483647, result->deadline_ms.value());
        provider.params.erase("max_documents");
        provider.params.erase("deadline_ms");
        result = AIProviderConfigParser::parse(provider.id, provider);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_FALSE(result->max_documents.has_value());
        EXPECT_FALSE(result->deadline_ms.has_value());
    }
}

TEST(AIProviderConfigParserTest, RerankRejectsInvalidLimitsWithoutExposingCredentials) {
    for (const auto key : {"max_documents", "deadline_ms"}) {
        for (const auto value : {"0", "-1", "2147483648", "1.5", "1x", " 1", "+", "secret-sentinel"}) {
            auto provider = rerank_provider();
            provider.params[key] = value;
            auto result = AIProviderConfigParser::parse(provider.id, provider);
            ASSERT_FALSE(result.ok()) << key << ": " << value;
            EXPECT_TRUE(result.status().is_invalid_argument());
            EXPECT_EQ(std::string::npos, result.status().to_string().find("secret-sentinel"));
        }
    }
}

TEST(AIProviderConfigParserTest, ProtocolCapabilityMismatchIsRejectedAndEmbeddingDefaultsRemainValid) {
    auto provider = rerank_provider();
    provider.__set_protocol(TAIProviderProtocol::OPENAI);
    EXPECT_FALSE(AIProviderConfigParser::parse(provider.id, provider).ok());
    provider.__set_type(TAIProviderType::CHAT);
    provider.__set_protocol(TAIProviderProtocol::COHERE);
    EXPECT_FALSE(AIProviderConfigParser::parse(provider.id, provider).ok());
    provider.__set_type(TAIProviderType::EMBEDDING);
    provider.__set_protocol(TAIProviderProtocol::OPENAI);
    provider.params = {{"endpoint", "https://127.0.0.1/v1/embeddings"},
                       {"model", "embed"},
                       {"timeout_ms", "75"},
                       {"dimensions", "3"}};
    auto result = AIProviderConfigParser::parse(provider.id, provider);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(AICapability::EMBEDDING, result->capability);
    EXPECT_EQ(75, result->timeout_ms.value());
    EXPECT_EQ(3, result->dimensions.value());
}

} // namespace
} // namespace starrocks::pipeline
