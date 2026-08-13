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

#include "platform/llm/ai_provider.h"

namespace starrocks {

AIProviderErrorAction ai_provider_error_action(AIProviderErrorCode code) {
    switch (code) {
    case AIProviderErrorCode::RATE_LIMIT_EXCEEDED:
    case AIProviderErrorCode::TOO_MANY_REQUESTS:
    case AIProviderErrorCode::THROTTLING:
    case AIProviderErrorCode::RATE_LIMIT:
        return AIProviderErrorAction::THROTTLED;
    case AIProviderErrorCode::SERVER_ERROR:
    case AIProviderErrorCode::INTERNAL_ERROR:
    case AIProviderErrorCode::SERVICE_UNAVAILABLE:
    case AIProviderErrorCode::TIMEOUT:
    case AIProviderErrorCode::API_CONNECTION_ERROR:
        return AIProviderErrorAction::RETRYABLE;
    case AIProviderErrorCode::UNKNOWN:
        return AIProviderErrorAction::TERMINAL;
    }
    return AIProviderErrorAction::TERMINAL;
}

} // namespace starrocks
