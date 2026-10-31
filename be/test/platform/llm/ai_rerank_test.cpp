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

#include "platform/llm/ai_rerank.h"

#include <gtest/gtest.h>

#include <limits>

namespace starrocks {

TEST(AIRerankTest, ParsesExactPositiveNumericTopN) {
    auto absent = parse_ai_rerank_top_n(AIProviderOptions{});
    ASSERT_TRUE(absent.ok());
    EXPECT_FALSE(absent->has_value());
    for (const auto* json : {"1", "5.0", "50e-1", "2147483647", "2147483647.0000"}) {
        auto options = AIProviderOptions::create({{"top_n", json, AIProviderOptionKind::NUMBER}});
        ASSERT_TRUE(options.ok());
        auto result = parse_ai_rerank_top_n(*options);
        ASSERT_TRUE(result.ok()) << json;
        EXPECT_GT(**result, 0);
    }
    for (const auto* json :
         {"0", "-1", "1.5", "2147483648", "5.0000000000000000001", "0.999999999999999999999", "1e-999"}) {
        auto options = AIProviderOptions::create({{"top_n", json, AIProviderOptionKind::NUMBER}});
        ASSERT_TRUE(options.ok()) << json;
        EXPECT_FALSE(parse_ai_rerank_top_n(*options).ok()) << json;
    }
    auto wrong_type = AIProviderOptions::create({{"top_n", "\"5\"", AIProviderOptionKind::STRING}});
    ASSERT_TRUE(wrong_type.ok());
    EXPECT_FALSE(parse_ai_rerank_top_n(*wrong_type).ok());
}

TEST(AIRerankTest, RejectsOnlyRequestOwnedOptions) {
    for (const auto* key : {"model", "query", "documents", "input", "parameters"}) {
        auto options = AIProviderOptions::create({{key, "1", AIProviderOptionKind::NUMBER}});
        ASSERT_TRUE(options.ok());
        EXPECT_FALSE(validate_ai_rerank_options(*options).ok()) << key;
    }
    auto options = AIProviderOptions::create({{"return_documents", "false", AIProviderOptionKind::FALSE_VALUE}});
    ASSERT_TRUE(options.ok());
    EXPECT_TRUE(validate_ai_rerank_options(*options).ok());
}

TEST(AIRerankTest, ValidatesExactCountIndicesAndOrderedFiniteScores) {
    const AIRerankResponseShape shape{3, 2};
    EXPECT_TRUE(validate_ai_rerank_hits({{2, 3.5}, {0, 3.5}}, shape).ok());
    EXPECT_TRUE(validate_ai_rerank_hits({{0, -2}, {1, -3}}, shape).ok());
    EXPECT_TRUE(validate_ai_rerank_hits({}, {0, 0}).ok());
    for (const auto& hits : std::vector<AIRerankResult>{{{0, 1}},
                                                        {{0, 1}, {1, 0}, {2, 0}},
                                                        {{0, 1}, {0, 0}},
                                                        {{-1, 1}, {0, 0}},
                                                        {{3, 1}, {0, 0}},
                                                        {{0, 0}, {1, 1}},
                                                        {{0, std::numeric_limits<double>::infinity()}, {1, 0}},
                                                        {{0, 1}, {1, std::numeric_limits<double>::quiet_NaN()}}}) {
        EXPECT_FALSE(validate_ai_rerank_hits(hits, shape).ok());
    }
    EXPECT_FALSE(validate_ai_rerank_hits({}, {-1, 0}).ok());
    EXPECT_FALSE(validate_ai_rerank_hits({}, {1, 2}).ok());
}

} // namespace starrocks
