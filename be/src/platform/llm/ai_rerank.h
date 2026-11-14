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

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "base/statusor.h"
#include "platform/llm/ai_provider_options.h"

namespace starrocks {

struct AIRerankHit {
    int32_t index;
    double relevance_score;
};
using AIRerankResult = std::vector<AIRerankHit>;

struct AIRerankInput {
    std::string query;
    std::vector<std::string> documents;
};

struct AIRerankResponseShape {
    int32_t candidate_count;
    int32_t expected_hits;
};

StatusOr<std::optional<int32_t>> parse_ai_rerank_top_n(const AIProviderOptions& options);
Status validate_ai_rerank_options(const AIProviderOptions& options);
Status validate_ai_rerank_hits(const AIRerankResult& hits, const AIRerankResponseShape& shape);

} // namespace starrocks
