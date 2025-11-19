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

#include "variant_path_parser.h"

#include <cctype>

namespace starrocks {

bool VariantPathParser::ParserState::is_at_end() const {
    return pos >= input.get_size();
}

char VariantPathParser::ParserState::peek() const {
    if (is_at_end()) return '\0';
    return input[pos];
}

char VariantPathParser::ParserState::advance() {
    if (is_at_end()) return '\0';
    return input[pos++];
}

bool VariantPathParser::ParserState::match(char expected) {
    if (is_at_end() || peek() != expected) {
        return false;
    }

    advance();
    return true;
}

bool VariantPathParser::is_digit(char c) {
    return c >= '0' && c <= '9';
}

bool VariantPathParser::is_valid_key_char(char c) {
    // Valid unquoted key characters: letters, digits, underscore
    // Exclude dots and brackets which are delimiters
    return std::isalnum(c) || c == '_';
}

bool VariantPathParser::parse_root(ParserState& state) {
    return state.match('$');
}

std::string VariantPathParser::parse_number(ParserState& state) {
    std::string number;
    while (!state.is_at_end() && is_digit(state.peek())) {
        number += state.advance();
    }

    return number;
}

std::string VariantPathParser::parse_unquoted_key(ParserState& state) {
    std::string key;
    while (!state.is_at_end() && is_valid_key_char(state.peek())) {
        key += state.advance();
    }

    return key;
}

std::string VariantPathParser::parse_quoted_string(ParserState& state, char quote) {
    std::string str;
    while (!state.is_at_end() && state.peek() != quote) {
        char c = state.advance();
        // Handle escape sequences if needed
        if (c == '\\' && !state.is_at_end()) {
            char escaped = state.advance();
            switch (escaped) {
            case '"':
            case '\'':
            case '\\':
                str += escaped;
                break;
            case 'n':
                str += '\n';
                break;
            case 't':
                str += '\t';
                break;
            case 'r':
                str += '\r';
                break;
            default:
                str += escaped;
                break;
            }
        } else {
            str += c;
        }
    }

    return str;
}

StatusOr<ArrayExtraction> VariantPathParser::parse_array_index(ParserState& state) {
    if (!state.match('[')) {
        return Status::VariantError(fmt::format("Expected '[' at position {}", static_cast<int>(state.pos)));
    }

    std::string indexStr = parse_number(state);
    if (indexStr.empty()) {
        return Status::VariantError(
                fmt::format("Expected array index after '[' at position {}", static_cast<int>(state.pos)));
    }

    if (!state.match(']')) {
        return Status::VariantError(fmt::format("Expected ']' after array index '{}' at position {}", indexStr,
                                                static_cast<int>(state.pos)));
    }

    try {
        int index = std::stoi(indexStr);
        return ArrayExtraction(index);
    } catch (const std::exception&) {
        return Status::VariantError(
                fmt::format("Invalid array index '{}' at position {}", indexStr, static_cast<int>(state.pos)));
    }
}

StatusOr<ObjectExtraction> VariantPathParser::parse_quoted_key(ParserState& state) {
    if (!state.match('[')) {
        return Status::VariantError(fmt::format("Expected '[' at position {}", static_cast<int>(state.pos)));
    }

    char quote = state.peek();
    if (quote != '\'' && quote != '"') {
        return Status::VariantError(
                fmt::format("Expected quote (\" or ') at position {}, found '{}'", static_cast<int>(state.pos), quote));
    }

    state.advance(); // consume quote

    std::string key = parse_quoted_string(state, quote);

    if (!state.match(quote)) {
        return Status::VariantError(fmt::format("Expected closing quote '{}' at position {}, found '{}'", quote,
                                                static_cast<int>(state.pos), state.peek()));
    }

    if (!state.match(']')) {
        return Status::VariantError(
                fmt::format("Expected ']' after quoted key '{}' at position {}", key, static_cast<int>(state.pos)));
    }

    return ObjectExtraction(key);
}

StatusOr<ObjectExtraction> VariantPathParser::parse_object_key(ParserState& state) {
    if (!state.match('.')) {
        return Status::VariantError(fmt::format("Expected '.' at position {}", static_cast<int>(state.pos)));
    }

    std::string key = parse_unquoted_key(state);
    if (key.empty()) {
        return Status::VariantError(fmt::format("Expected key after '.' at position {}", static_cast<int>(state.pos)));
    }

    return ObjectExtraction(key);
}

StatusOr<VariantPathExtraction> VariantPathParser::parse_segment(ParserState& state) {
    if (state.is_at_end()) {
        return Status::VariantError(fmt::format("Unexpected end of input at position {}", static_cast<int>(state.pos)));
    }

    if (char c = state.peek(); c == '.') {
        // Dot notation: .field
        auto result = parse_object_key(state);
        if (result.ok()) {
            return VariantPathExtraction(std::move(result.value()));
        }
        return result.status();
    } else if (c == '[') {
        // Bracket notation: could be [index] or ['key'] or ["key"]
        size_t saved_pos = state.pos;

        // Try parsing as array index first
        auto arraySegment = parse_array_index(state);
        if (arraySegment.ok()) {
            return VariantPathExtraction(std::move(arraySegment.value()));
        }

        // Reset and try parsing as quoted key
        state.pos = saved_pos;
        auto objectSegment = parse_quoted_key(state);
        if (objectSegment.ok()) {
            return VariantPathExtraction(std::move(objectSegment.value()));
        }

        // Reset position if both failed
        state.pos = saved_pos;
        return Status::VariantError(fmt::format("Failed to parse segment at position {}", static_cast<int>(state.pos)));
    }

    return Status::VariantError(
            fmt::format("Unexpected character '{}' at position {}", state.peek(), static_cast<int>(state.pos)));
}

StatusOr<VariantPath> VariantPathParser::parse(Slice input) {
    ParserState state(input);
    VariantPath variant_path;

    // Must start with '$'
    if (!parse_root(state)) {
        return Status::InvalidArgument("Path must start with '$'");
    }

    // Parse segments until end of input
    while (!state.is_at_end()) {
        auto segment_result = parse_segment(state);
        if (!segment_result.ok()) {
            return segment_result.status();
        }

        variant_path.segments.push_back(std::move(segment_result.value()));
    }

    return variant_path;
}

StatusOr<VariantPath> VariantPathParser::parse(const std::string& input) {
    return parse(Slice(input));
}

void VariantPath::reset(const VariantPath& rhs) {
    segments = rhs.segments;
}

void VariantPath::reset(VariantPath&& rhs) {
    segments = std::move(rhs.segments);
}

StatusOr<VariantValue> VariantPath::seek(const VariantValue* value, const VariantPath* variant_path) {
    if (value == nullptr || variant_path == nullptr) {
        return Status::InvalidArgument("Variant value and path must not be null");
    }

    const std::string& metadata = value->get_metadata();
    if (metadata.empty()) {
        return Status::InvalidArgument("Can not find variant value with empty metadata");
    }

    const std::string& val = value->get_value();
    if (val.empty()) {
        return Status::InvalidArgument("Variant value is empty");
    }

    Variant current{metadata, val};
    for (size_t seg_idx = 0; seg_idx < variant_path->segments.size(); ++seg_idx) {
        const auto& segment = variant_path->segments[seg_idx];

        StatusOr<Variant> sub;
        std::visit(
                [&]<typename T0>(const T0& seg) {
                    if constexpr (std::is_same_v<std::decay_t<T0>, ObjectExtraction>) {
                        sub = current.get_object_by_key(seg.get_key());
                    } else if constexpr (std::is_same_v<std::decay_t<T0>, ArrayExtraction>) {
                        sub = current.get_element_at_index(seg.get_index());
                    }
                },
                segment);

        if (!sub.ok()) {
            return sub.status();
        }

        current = Variant{sub->metadata(), sub->value()};
    }

    return VariantValue::of_variant(current);
}

} // namespace starrocks
