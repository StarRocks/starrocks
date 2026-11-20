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

#include "platform/llm/ai_provider.h"

namespace starrocks {

class DashScopeProvider final : public AIProvider {
public:
    enum class EndpointType { TEXT_CHAT, MULTIMODAL_CHAT, TEXT_EMBEDDING, MULTIMODAL_EMBEDDING };

    explicit DashScopeProvider(EndpointType endpoint_type,
                               std::optional<int32_t> default_embedding_dimensions = std::nullopt)
            : _endpoint_type(endpoint_type), _default_embedding_dimensions(default_embedding_dimensions) {}
    static StatusOr<std::unique_ptr<AIProvider>> create(const AIProviderConfig& config);

    StatusOr<AIProviderHttpRequest> build_request(const AIChatRequest& request) const override;
    AIProviderParseResult parse_response(std::string_view body,
                                         AICapability capability = AICapability::CHAT) const override;

private:
    const EndpointType _endpoint_type;
    // Default embedding vector size from the provider configuration.
    // Used when `dimensions` is not specified in JSON options.
    const std::optional<int32_t> _default_embedding_dimensions;
};

} // namespace starrocks
