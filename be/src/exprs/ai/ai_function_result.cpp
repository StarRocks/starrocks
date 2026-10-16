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

#include "exprs/ai/ai_function_result.h"

#include <fast_float/fast_float.h>

#include <cmath>
#include <limits>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "gutil/strings/ascii_ctype.h"
#include "simdjson.h"
#include "types/datum.h"
#include "types/json_value.h"
#include "velocypack/Builder.h"
#include "velocypack/Exception.h"
#include "velocypack/Parser.h"

namespace starrocks {
namespace {

Status malformed_result() {
    return Status::InvalidArgument("Malformed AI function result");
}

std::string_view trim_ascii(std::string_view value) {
    while (!value.empty() && ascii_isspace(static_cast<unsigned char>(value.front()))) {
        value.remove_prefix(1);
    }
    while (!value.empty() && ascii_isspace(static_cast<unsigned char>(value.back()))) {
        value.remove_suffix(1);
    }
    return value;
}

std::string normalized_token(std::string_view value) {
    std::string result(trim_ascii(value));
    for (char& ch : result) {
        ch = ascii_tolower(static_cast<unsigned char>(ch));
    }
    return result;
}

StatusOr<JsonValue> parse_object(std::string_view text) {
    text = trim_ascii(text);
    if (text.empty() || text.front() != '{') {
        return malformed_result();
    }
    if (text.size() > kJSONLengthLimit) {
        return Status::CapacityLimitExceed("AI JSON result exceeds maximum JSON length");
    }
    // VPack's conversion is recursive and has no nesting limit. Validate the
    // provider-controlled JSON with the bounded, non-recursive DOM parser first.
    simdjson::dom::parser parser;
    auto error = parser.allocate(text.size(), 64);
    if (error == simdjson::SUCCESS) {
        error = parser.parse(text.data(), text.size()).error();
    }
    if (error == simdjson::MEMALLOC) {
        return Status::MemoryAllocFailed("AI JSON result validation allocation failed");
    }
    if (error != simdjson::SUCCESS) {
        return malformed_result();
    }
    try {
        auto builder = vpack::Parser::fromJson(text.data(), text.size());
        JsonValue json;
        json.assign(*builder);
        return json;
    } catch (const vpack::Exception& error) {
        switch (error.errorCode()) {
        case vpack::Exception::ParseError:
        case vpack::Exception::UnexpectedControlCharacter:
        case vpack::Exception::NumberOutOfRange:
        case vpack::Exception::InvalidUtf8Sequence:
        case vpack::Exception::DuplicateAttributeName:
            return malformed_result();
        default:
            // Internal parser failures are not malformed rows and must not be
            // hidden by the query's on_error=ignore policy.
            return Status::InternalError("AI JSON result parsing failed");
        }
    }
}

} // namespace

Status append_ai_function_result(AIFunctionResultKind kind, const AIProviderValue& value, Column* output) {
    auto* nullable = dynamic_cast<NullableColumn*>(output);
    if (nullable == nullptr) {
        return Status::InternalError("Invalid AI result column");
    }
    const Column* data = nullable->data_column().get();
    if (kind == AIFunctionResultKind::EMBEDDING) {
        const auto* array = dynamic_cast<const ArrayColumn*>(data);
        const auto* elements = array == nullptr ? nullptr : dynamic_cast<const NullableColumn*>(&array->elements());
        if (elements == nullptr || dynamic_cast<const FloatColumn*>(elements->data_column().get()) == nullptr) {
            return Status::InternalError("Invalid AI embedding result column");
        }
        const auto* embedding = std::get_if<std::vector<float>>(&value);
        if (embedding == nullptr || embedding->empty()) {
            return malformed_result();
        }
        if (array->elements().size() > std::numeric_limits<uint32_t>::max() ||
            embedding->size() > std::numeric_limits<uint32_t>::max() - array->elements().size()) {
            return Status::CapacityLimitExceed("AI embedding result exceeds array offset capacity");
        }
        DatumArray result;
        result.reserve(embedding->size());
        for (const float element : *embedding) {
            if (!std::isfinite(element)) {
                return malformed_result();
            }
            result.emplace_back(element);
        }
        output->append_datum(Datum(result));
        return Status::OK();
    }
    const auto* content = std::get_if<std::string>(&value);
    if (content == nullptr) {
        return malformed_result();
    }
    switch (kind) {
    case AIFunctionResultKind::STRING:
    case AIFunctionResultKind::SENTIMENT: {
        if (dynamic_cast<const BinaryColumn*>(data) == nullptr) {
            return Status::InternalError("Invalid AI string result column");
        }
        if (kind == AIFunctionResultKind::STRING) {
            output->append_datum(Datum(Slice(*content)));
            return Status::OK();
        }
        const std::string token = normalized_token(*content);
        if (token != "positive" && token != "negative" && token != "neutral" && token != "mixed" &&
            token != "unknown") {
            return malformed_result();
        }
        output->append_datum(Datum(Slice(token)));
        return Status::OK();
    }
    case AIFunctionResultKind::BOOLEAN: {
        if (dynamic_cast<const BooleanColumn*>(data) == nullptr) {
            return Status::InternalError("Invalid AI boolean result column");
        }
        const std::string token = normalized_token(*content);
        if (token != "true" && token != "false") {
            return malformed_result();
        }
        output->append_datum(Datum(uint8_t(token == "true")));
        return Status::OK();
    }
    case AIFunctionResultKind::SIMILARITY: {
        if (dynamic_cast<const FloatColumn*>(data) == nullptr) {
            return Status::InternalError("Invalid AI similarity result column");
        }
        const auto text = trim_ascii(*content);
        if (text.empty()) {
            return malformed_result();
        }
        double score;
        const auto parsed = fast_float::from_chars(text.data(), text.data() + text.size(), score);
        if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || !std::isfinite(score) || score < 0 ||
            score > 1) {
            return malformed_result();
        }
        output->append_datum(Datum(static_cast<float>(score)));
        return Status::OK();
    }
    case AIFunctionResultKind::JSON: {
        if (dynamic_cast<const JsonColumn*>(data) == nullptr) {
            return Status::InternalError("Invalid AI JSON result column");
        }
        ASSIGN_OR_RETURN(auto json, parse_object(*content));
        output->append_datum(Datum(&json));
        return Status::OK();
    }
    case AIFunctionResultKind::EMBEDDING:
        break;
    }
    return Status::InternalError("Invalid AI function result kind");
}

} // namespace starrocks
