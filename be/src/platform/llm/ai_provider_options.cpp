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

#include "platform/llm/ai_provider_options.h"

#include <rapidjson/document.h>

#include <new>
#include <unordered_set>

#include "base/string/utf8_check.h"

namespace starrocks {
namespace {

bool type_matches(AIProviderOptionKind kind, const rapidjson::Value& value) {
    switch (kind) {
    case AIProviderOptionKind::NULL_VALUE:
        return value.IsNull();
    case AIProviderOptionKind::FALSE_VALUE:
        return value.IsFalse();
    case AIProviderOptionKind::TRUE_VALUE:
        return value.IsTrue();
    case AIProviderOptionKind::OBJECT:
        return value.IsObject();
    case AIProviderOptionKind::ARRAY:
        return value.IsArray();
    case AIProviderOptionKind::STRING:
        return value.IsString();
    case AIProviderOptionKind::NUMBER:
        return value.IsNumber();
    }
    return false;
}

} // namespace

StatusOr<AIProviderOptions> AIProviderOptions::create(Members members) {
    try {
        std::unordered_set<std::string> keys;
        keys.reserve(members.size());
        for (const AIProviderOption& member : members) {
            if (member.key.empty() || !validate_utf8(member.key.data(), member.key.size()) ||
                !keys.emplace(member.key).second || member.serialized_json.empty() ||
                member.serialized_json.find('\0') != std::string::npos) {
                return Status::InvalidArgument("AI provider options are invalid");
            }
            rapidjson::Document value;
            value.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(
                    member.serialized_json.data(), member.serialized_json.size());
            if (value.HasParseError() || !type_matches(member.kind, value)) {
                return Status::InvalidArgument("AI provider options are invalid");
            }
        }
        return AIProviderOptions(std::make_shared<const Members>(std::move(members)));
    } catch (const std::bad_alloc&) {
        return Status::MemoryLimitExceeded("Failed to allocate AI provider options");
    }
}

const AIProviderOptions::Members& AIProviderOptions::members() const noexcept {
    static const Members kEmpty;
    return _members == nullptr ? kEmpty : *_members;
}

} // namespace starrocks
