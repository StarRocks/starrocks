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

#include "exprs/ai/ai_provider_options_builder.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <string>
#include <utility>

#include "column/const_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "types/type_descriptor.h"

namespace starrocks {
namespace {

using JsonBuffer = rapidjson::StringBuffer;
using JsonWriter = rapidjson::Writer<JsonBuffer, rapidjson::UTF8<>, rapidjson::UTF8<>, rapidjson::CrtAllocator,
                                     rapidjson::kWriteValidateEncodingFlag>;

Status invalid_options() {
    return Status::InvalidArgument("AI provider options are invalid");
}

Status require_json_write(bool success) {
    return success ? Status::OK() : invalid_options();
}

struct UnwrappedValue {
    const Column* column = nullptr;
    size_t row = 0;
    bool is_null = false;
};

StatusOr<UnwrappedValue> unwrap_value(const Column& input, size_t row) {
    const Column* column = &input;
    size_t current_row = row;
    if (current_row >= column->size()) {
        return invalid_options();
    }

    while (column->is_constant() || column->is_nullable()) {
        if (column->is_constant()) {
            const auto* constant = dynamic_cast<const ConstColumn*>(column);
            if (constant == nullptr || constant->data_column()->empty()) {
                return invalid_options();
            }
            column = constant->data_column_raw_ptr();
            current_row = 0;
            continue;
        }

        const auto* nullable = dynamic_cast<const NullableColumn*>(column);
        if (nullable == nullptr || current_row >= nullable->size()) {
            return invalid_options();
        }
        if (nullable->is_null(current_row)) {
            return UnwrappedValue{nullable->data_column_raw_ptr(), current_row, true};
        }
        column = nullable->data_column_raw_ptr();
    }

    if (current_row >= column->size()) {
        return invalid_options();
    }
    return UnwrappedValue{column, current_row, false};
}

template <typename ColumnType>
const ColumnType* checked_column(const UnwrappedValue& value) {
    return dynamic_cast<const ColumnType*>(value.column);
}

AIProviderOptionKind provider_option_kind(rapidjson::Type type) {
    switch (type) {
    case rapidjson::kNullType:
        return AIProviderOptionKind::NULL_VALUE;
    case rapidjson::kFalseType:
        return AIProviderOptionKind::FALSE_VALUE;
    case rapidjson::kTrueType:
        return AIProviderOptionKind::TRUE_VALUE;
    case rapidjson::kObjectType:
        return AIProviderOptionKind::OBJECT;
    case rapidjson::kArrayType:
        return AIProviderOptionKind::ARRAY;
    case rapidjson::kStringType:
        return AIProviderOptionKind::STRING;
    case rapidjson::kNumberType:
        return AIProviderOptionKind::NUMBER;
    }
    return AIProviderOptionKind::NULL_VALUE;
}

} // namespace

StatusOr<AIProviderOptions> build_ai_provider_options(const Column& column, const TypeDescriptor& type, size_t row,
                                                      AICapability capability) {
    if (type.type != TYPE_JSON || (capability != AICapability::CHAT && capability != AICapability::EMBEDDING)) {
        return invalid_options();
    }
    auto unwrapped = unwrap_value(column, row);
    if (!unwrapped.ok()) {
        return unwrapped.status();
    }

    AIProviderOptions::Members members;
    if (unwrapped->is_null) {
        return AIProviderOptions::create(std::move(members));
    }

    const auto* json_column = checked_column<JsonColumn>(*unwrapped);
    if (json_column == nullptr || json_column->is_flat_json()) {
        return invalid_options();
    }
    auto json = json_column->get_object(unwrapped->row)->to_string();
    if (!json.ok() || json->find('\0') != std::string::npos) {
        return invalid_options();
    }
    rapidjson::Document document;
    document.Parse<rapidjson::kParseFullPrecisionFlag | rapidjson::kParseValidateEncodingFlag>(json->data(),
                                                                                               json->size());
    if (document.HasParseError() || !document.IsObject()) {
        return invalid_options();
    }

    members.reserve(document.MemberCount());
    for (const auto& member : document.GetObject()) {
        std::string key(member.name.GetString(), member.name.GetStringLength());
        if (key == "model" || key == "endpoint" || key == "key" || key == "api_key" || key == "protocol" ||
            key == "provider" || (capability == AICapability::CHAT && (key == "messages" || key == "stream")) ||
            (capability == AICapability::EMBEDDING && (key == "input" || key == "encoding_format"))) {
            return invalid_options();
        }
        JsonBuffer buffer;
        JsonWriter writer(buffer);
        RETURN_IF_ERROR(require_json_write(member.value.Accept(writer)));
        if (!writer.IsComplete()) {
            return invalid_options();
        }
        members.emplace_back(AIProviderOption{.key = std::move(key),
                                              .serialized_json = std::string(buffer.GetString(), buffer.GetSize()),
                                              .kind = provider_option_kind(member.value.GetType())});
    }

    return AIProviderOptions::create(std::move(members));
}

} // namespace starrocks
