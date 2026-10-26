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

#include <algorithm>
#include <cctype>
#include <charconv>

namespace starrocks::pipeline {
namespace {

Status invalid_provider() {
    return Status::InvalidArgument("Invalid AI project provider configuration");
}

bool contains_control_character(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) { return c <= 0x1f || c == 0x7f; });
}

bool is_non_blank(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) { return std::isspace(c) == 0; });
}

StatusOr<std::optional<int32_t>> parse_positive_int(std::string_view value) {
    if (value.empty()) return std::optional<int32_t>{};
    // Match FE Integer.parseInt, including its optional leading plus sign.
    if (value.front() == '+') value.remove_prefix(1);
    if (value.empty()) return invalid_provider();
    int32_t parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size() || parsed <= 0) {
        return invalid_provider();
    }
    return std::optional<int32_t>{parsed};
}

} // namespace

StatusOr<AIProjectModelConfig> AIProviderConfigParser::parse(std::string_view id, const TAIProvider& provider) {
    if (!provider.__isset.id || provider.id != id || !is_non_blank(id) || contains_control_character(id) ||
        !provider.__isset.type || !provider.__isset.protocol || !provider.__isset.params) {
        return invalid_provider();
    }
    AIProjectModelConfig config;
    switch (provider.type) {
    case TAIProviderType::CHAT:
        config.capability = AICapability::CHAT;
        break;
    case TAIProviderType::EMBEDDING:
        config.capability = AICapability::EMBEDDING;
        break;
    default:
        return invalid_provider();
    }
    const auto get = [&](const char* key) -> std::string {
        auto it = provider.params.find(key);
        return it == provider.params.end() ? std::string{} : it->second;
    };
    config.endpoint = get("endpoint");
    config.model = get("model");
    config.api_key = get("api_key");
    if (provider.protocol != TAIProviderProtocol::OPENAI) {
        return invalid_provider();
    }
    config.protocol = AIProviderProtocol::OPENAI;
    if (!is_non_blank(config.endpoint) || contains_control_character(config.endpoint) || !is_non_blank(config.model) ||
        contains_control_character(config.model) || contains_control_character(config.api_key)) {
        return invalid_provider();
    }
    ASSIGN_OR_RETURN(auto timeout, parse_positive_int(get("timeout_ms")));
    if (timeout.has_value()) config.timeout_ms = *timeout;
    if (config.capability == AICapability::EMBEDDING) {
        ASSIGN_OR_RETURN(config.dimensions, parse_positive_int(get("dimensions")));
    }
    return config;
}

} // namespace starrocks::pipeline
