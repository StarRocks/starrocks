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

#include "column/variant_path_parser.h"

#include <cctype>

namespace starrocks {

// ---------------------------------------------------------------------------
// File-local helpers
// ---------------------------------------------------------------------------

static bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

// A "simple key" can be expressed as a dotted segment: [a-zA-Z0-9_]+
static bool is_valid_key_char(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static bool is_simple_key(const std::string& key) {
    if (key.empty()) return false;
    for (char c : key) {
        if (!is_valid_key_char(c)) return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// ParserState
// ---------------------------------------------------------------------------

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
    if (is_at_end() || peek() != expected) return false;
    advance();
    return true;
}

// ---------------------------------------------------------------------------
// Leaf parsers
// ---------------------------------------------------------------------------

bool VariantPathParser::parse_root(ParserState& state) {
    return state.match('$');
}

std::string VariantPathParser::parse_number(ParserState& state) {
    const size_t start = state.pos;
    while (!state.is_at_end() && is_digit(state.peek())) state.advance();
    return std::string(state.input.data + start, state.pos - start);
}

std::string VariantPathParser::parse_unquoted_key(ParserState& state) {
    const size_t start = state.pos;
    while (!state.is_at_end() && is_valid_key_char(state.peek())) state.advance();
    return std::string(state.input.data + start, state.pos - start);
}

std::string VariantPathParser::parse_quoted_string(ParserState& state, char quote) {
    std::string str;
    while (!state.is_at_end() && state.peek() != quote) {
        char c = state.advance();
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

StatusOr<VariantSegment> VariantPathParser::parse_array_index(ParserState& state) {
    if (!state.match('[')) {
        return Status::VariantError("Expected '[' for array index");
    }
    std::string index_str = parse_number(state);
    if (index_str.empty()) {
        return Status::VariantError("Expected array index after '['");
    }
    if (!state.match(']')) {
        return Status::VariantError("Expected ']' after array index");
    }
    try {
        return VariantSegment::make_array(std::stoi(index_str));
    } catch (const std::exception&) {
        return Status::VariantError("Invalid array index");
    }
}

StatusOr<VariantSegment> VariantPathParser::parse_quoted_key(ParserState& state) {
    if (!state.match('[')) {
        return Status::VariantError("Expected '[' for quoted key");
    }
    char quote = state.peek();
    if (quote != '\'' && quote != '"') {
        return Status::VariantError("Expected quote after '['");
    }
    state.advance(); // consume quote
    std::string key = parse_quoted_string(state, quote);
    if (!state.match(quote)) {
        return Status::VariantError("Expected closing quote");
    }
    if (!state.match(']')) {
        return Status::VariantError("Expected ']' after quoted key");
    }
    return VariantSegment::make_object(std::move(key));
}

StatusOr<VariantSegment> VariantPathParser::parse_object_key(ParserState& state) {
    if (!state.match('.')) {
        return Status::VariantError("Expected '.' for object key");
    }
    std::string key = parse_unquoted_key(state);
    if (key.empty()) {
        return Status::VariantError("Expected key after '.'");
    }
    return VariantSegment::make_object(std::move(key));
}

StatusOr<VariantSegment> VariantPathParser::parse_segment(ParserState& state) {
    if (state.is_at_end()) {
        return Status::VariantError("Unexpected end of path");
    }
    if (char c = state.peek(); c == '.') {
        return parse_object_key(state);
    } else if (c == '[') {
        size_t saved_pos = state.pos;
        auto array_seg = parse_array_index(state);
        if (array_seg.ok()) return array_seg;
        state.pos = saved_pos;
        auto object_seg = parse_quoted_key(state);
        if (object_seg.ok()) return object_seg;
        state.pos = saved_pos;
        return Status::VariantError("Failed to parse bracket segment");
    }
    return Status::VariantError("Unexpected character in path");
}

// ---------------------------------------------------------------------------
// Public parse entry points
// ---------------------------------------------------------------------------

StatusOr<VariantPath> VariantPathParser::parse(Slice input) {
    ParserState state(input);
    VariantPath path;
    if (!parse_root(state)) {
        return Status::InvalidArgument("Path must start with '$'");
    }
    while (!state.is_at_end()) {
        auto seg = parse_segment(state);
        if (!seg.ok()) return seg.status();
        path.segments.push_back(std::move(seg).value());
    }
    return path;
}

StatusOr<VariantPath> VariantPathParser::parse(const std::string& input) {
    return parse(Slice(input));
}

StatusOr<VariantPath> VariantPathParser::parse_shredded_path(Slice input) {
    // A shredded-path canonical string is a full JSONPath with the leading "$" (and
    // the leading "." before the first dotted key) stripped.  We parse it directly
    // without constructing any intermediate string.
    //
    // Grammar (same as JSONPath segments, but no leading '$'):
    //   shredded_path ::= ""                    (root / empty)
    //                   | unquoted_key rest     (first segment is a bare key)
    //                   | bracket_segment rest  (first segment is ['...'])
    //   rest          ::= ("." unquoted_key | bracket_segment)*
    if (input.get_size() == 0) {
        return VariantPath{};
    }

    ParserState state(input);
    VariantPath path;

    // Parse the first segment: no leading '.' required for unquoted keys.
    if (state.peek() == '[') {
        // Bracket segment — same logic as parse_segment handles '['.
        auto seg = parse_segment(state);
        if (!seg.ok()) return seg.status();
        path.segments.push_back(std::move(seg).value());
    } else {
        // Bare unquoted key (no leading '.').
        std::string key = parse_unquoted_key(state);
        if (key.empty()) {
            return Status::VariantError("Expected key at start of shredded path");
        }
        path.segments.push_back(VariantSegment::make_object(std::move(key)));
    }

    // Remaining segments: '.' key  or  ['...'] — identical to full JSONPath.
    while (!state.is_at_end()) {
        auto seg = parse_segment(state);
        if (!seg.ok()) return seg.status();
        path.segments.push_back(std::move(seg).value());
    }
    return path;
}

StatusOr<VariantPath> VariantPathParser::parse_shredded_path(std::string_view input) {
    return parse_shredded_path(Slice(input.data(), input.size()));
}

// ---------------------------------------------------------------------------
// VariantPath methods
// ---------------------------------------------------------------------------

bool VariantPath::is_strict_prefix_of(const VariantPath& other) const {
    if (other.segments.size() <= segments.size()) return false;
    for (size_t i = 0; i < segments.size(); ++i) {
        if (segments[i] != other.segments[i]) return false;
    }
    return true;
}

bool VariantPath::is_ancestor_or_same(const VariantPath& other) const {
    if (other.segments.size() < segments.size()) return false;
    for (size_t i = 0; i < segments.size(); ++i) {
        if (segments[i] != other.segments[i]) return false;
    }
    return true;
}

std::optional<std::string> VariantPath::to_shredded_path() const {
    std::string result;
    for (const auto& seg : segments) {
        // Array segments are not permitted in shredded paths.
        if (!seg.is_object()) return std::nullopt;

        const std::string& k = seg.get_key();
        if (is_simple_key(k)) {
            // Simple key: use dotted notation. Add '.' separator before non-first segments.
            if (!result.empty()) result += '.';
            result += k;
        } else {
            // Key contains special characters (e.g. '.'): use bracket notation.
            // Escape single-quote and backslash inside the quoted string.
            // No leading '.' before bracket segments.
            result += "['";
            for (char c : k) {
                if (c == '\'') {
                    result += "\\'";
                } else if (c == '\\') {
                    result += "\\\\";
                } else {
                    result += c;
                }
            }
            result += "']";
        }
    }
    return result;
}

static StatusOr<VariantValue> seek_variant_value(const VariantMetadata& metadata, VariantValue current,
                                                 const VariantPath* variant_path, size_t seg_offset) {
    if (variant_path == nullptr) {
        return Status::InvalidArgument("Variant value and path must not be null");
    }
    for (size_t i = seg_offset; i < variant_path->segments.size(); ++i) {
        const auto& seg = variant_path->segments[i];
        if (seg.is_object()) {
            ASSIGN_OR_RETURN(current, current.get_object_by_key(metadata, seg.get_key()));
        } else {
            ASSIGN_OR_RETURN(current, current.get_element_at_index(metadata, seg.get_index()));
        }
        if (current.is_null()) break;
    }
    return current;
}

StatusOr<VariantRowRef> VariantPath::seek_view(const VariantRowRef& value, const VariantPath& variant_path,
                                               size_t seg_offset) {
    ASSIGN_OR_RETURN(auto current,
                     seek_variant_value(value.get_metadata(), value.get_value(), &variant_path, seg_offset));
    return VariantRowRef::from_variant(value.get_metadata(), current);
}

} // namespace starrocks
