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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/statusor.h"

namespace starrocks {

// Describes the syntactic JSON category of an already validated serialized value. Keeping this enum independent of
// the JSON implementation prevents the platform provider contract from exposing parser-specific types.
enum class AIProviderOptionKind : uint8_t { NULL_VALUE, FALSE_VALUE, TRUE_VALUE, OBJECT, ARRAY, STRING, NUMBER };

struct AIProviderOption {
    std::string key;
    std::string serialized_json;
    AIProviderOptionKind kind = AIProviderOptionKind::NULL_VALUE;
};

// Immutable, shareable options prepared at the expression boundary and consumed by platform providers.
class AIProviderOptions {
public:
    using Members = std::vector<AIProviderOption>;

    AIProviderOptions() = default;

    // Validates the provider-independent structural contract and then takes an
    // immutable copy. Callers cannot mutate the published options through a
    // retained container alias.
    static StatusOr<AIProviderOptions> create(Members members);

    const Members& members() const noexcept;

private:
    explicit AIProviderOptions(std::shared_ptr<const Members> members) : _members(std::move(members)) {}

    std::shared_ptr<const Members> _members;
};

} // namespace starrocks
