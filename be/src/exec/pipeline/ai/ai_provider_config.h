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
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "base/statusor.h"
#include "platform/llm/ai_provider.h"
#include "platform/llm/ai_rate_limiter.h"

namespace starrocks {

class TAIProvider;

namespace pipeline {

// Query-plan snapshot. Provider credentials belong to this query. Never print this object.
struct AIProviderConfig {
    std::string endpoint;
    std::string model;
    std::string api_key;
    AICapability capability = AICapability::CHAT;
    std::optional<int64_t> timeout_ms;
    std::optional<int32_t> dimensions;
    AIProviderProtocol protocol = AIProviderProtocol::OPENAI;
};

// Map keys are Provider registry IDs carried by the AI expressions.
using AIProviderConfigs = std::map<std::string, AIProviderConfig, std::less<>>;

// Converts wire metadata once at node initialization. Errors never include credentials.
class AIProviderConfigParser {
public:
    static StatusOr<AIProviderConfig> parse(std::string_view id, const TAIProvider& provider);
};

} // namespace pipeline
} // namespace starrocks
