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

#include <rapidjson/rapidjson.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/statusor.h"

namespace starrocks {

class Column;
class OpenAICompatibleProvider;
struct TypeDescriptor;

class PreparedAIProviderOptions {
public:
    PreparedAIProviderOptions() = default;
    PreparedAIProviderOptions(const PreparedAIProviderOptions&) = default;
    PreparedAIProviderOptions& operator=(const PreparedAIProviderOptions&) = default;
    PreparedAIProviderOptions(PreparedAIProviderOptions&&) noexcept = default;
    PreparedAIProviderOptions& operator=(PreparedAIProviderOptions&&) noexcept = default;

private:
    struct SerializedMember {
        std::string key;
        std::string value;
        rapidjson::Type value_type = rapidjson::kNullType;
    };
    using SerializedMembers = std::vector<SerializedMember>;

    explicit PreparedAIProviderOptions(std::shared_ptr<const SerializedMembers> members)
            : _members(std::move(members)) {}

    const SerializedMembers& _serialized_members() const;

    std::shared_ptr<const SerializedMembers> _members;

    friend class OpenAICompatibleProvider;
    friend StatusOr<PreparedAIProviderOptions> prepare_ai_provider_options(const Column&, const TypeDescriptor&,
                                                                           size_t);
};

StatusOr<PreparedAIProviderOptions> prepare_ai_provider_options(const Column& column, const TypeDescriptor& type,
                                                                size_t row);

} // namespace starrocks
