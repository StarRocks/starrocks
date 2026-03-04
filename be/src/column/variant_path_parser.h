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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "base/statusor.h"
#include "base/string/slice.h"
#include "types/variant_value.h"

namespace starrocks {

// A single segment of a parsed JSONPath: either an object-key extraction ("field")
// or an array-index extraction ([123]).
struct VariantSegment {
    enum class Kind : uint8_t { kObject, kArray };

    Kind kind;
    std::string key; // valid when kind == kObject
    int index;       // valid when kind == kArray

    static VariantSegment make_object(std::string k) { return {Kind::kObject, std::move(k), 0}; }
    static VariantSegment make_array(int i) { return {Kind::kArray, {}, i}; }

    bool is_object() const { return kind == Kind::kObject; }
    bool is_array() const { return kind == Kind::kArray; }

    const std::string& get_key() const { return key; }
    int get_index() const { return index; }
};

// A parsed variant path: a sequence of VariantSegments.
//
// Canonical string format (shredded_path):
//   - Only object-key segments are allowed in shredded paths (no array segments).
//   - Simple keys (matching [a-zA-Z0-9_]+) use dotted notation: "a.b.c"
//   - Keys with special characters (e.g. containing '.') use bracket notation: "['k.ey']"
//   - Bracket segments are appended without a leading '.'; dotted segments
//     (non-first) are preceded by '.'.
//
// Examples:
//   ["a", "b"]          -> "a.b"
//   ["key.a", "b"]      -> "['key.a'].b"
//   ["a", "key.a"]      -> "a['key.a']"
//   ["key.a", "k2.b"]   -> "['key.a']['k2.b']"
//   ["a", "key.a", "b"] -> "a['key.a'].b"
//
// Round-trip: parse_shredded_path(path.to_shredded_path().value()) == path
struct VariantPath {
    std::vector<VariantSegment> segments;

    explicit VariantPath(std::vector<VariantSegment> segs) : segments(std::move(segs)) {}

    VariantPath() = default;
    VariantPath(VariantPath&&) = default;
    VariantPath(const VariantPath&) = default;
    VariantPath& operator=(VariantPath&&) = default;
    VariantPath& operator=(const VariantPath&) = default;
    ~VariantPath() = default;

    bool empty() const { return segments.empty(); }

    // Returns the canonical shredded-path string (no '$' prefix).
    // Returns nullopt if any segment is an array index (shredded paths do not
    // support array indexing).
    //
    // Simple keys ([a-zA-Z0-9_]+) use dotted notation; keys with other characters
    // use bracket notation ['...']. Within bracket notation, single-quote (')
    // and backslash (\) are escaped as \' and \\ respectively.
    //
    // The returned string can always be re-parsed by VariantPathParser::parse_shredded_path().
    std::optional<std::string> to_shredded_path() const;

    // Seek into a variant using the parsed segments, starting at seg_offset.
    // Returns a non-owning row ref. Call to_owned() when retained storage is required.
    static StatusOr<VariantRowRef> seek_view(const VariantRowRef& value, const VariantPath& path,
                                             size_t seg_offset = 0);
};

// Parser for variant path expressions (JSONPath subset: "$", "$.a.b", "$[0]", "$['key']").
//
// Also parses shredded-path canonical strings via parse_shredded_path() (no '$' prefix).
class VariantPathParser {
public:
    // Parse a full JSONPath expression starting with '$'.
    // Examples: "$", "$.a.b", "$[0]", "$['key.a'].b"
    static StatusOr<VariantPath> parse(Slice input);
    static StatusOr<VariantPath> parse(const std::string& input);

    // Parse a shredded-path canonical string (no '$' prefix).
    // This is the inverse of VariantPath::to_shredded_path().
    // Examples: "a.b", "['key.a'].b", "a['key.a']['k2.b']"
    static StatusOr<VariantPath> parse_shredded_path(Slice input);
    static StatusOr<VariantPath> parse_shredded_path(std::string_view input);

private:
    struct ParserState {
        Slice input;
        size_t pos = 0;

        explicit ParserState(Slice inp) : input(inp) {}

        bool is_at_end() const;
        char peek() const;
        char advance();
        bool match(char expected);
    };

    static bool parse_root(ParserState& state);
    static StatusOr<VariantSegment> parse_segment(ParserState& state);
    static StatusOr<VariantSegment> parse_array_index(ParserState& state);
    static StatusOr<VariantSegment> parse_object_key(ParserState& state);
    static StatusOr<VariantSegment> parse_quoted_key(ParserState& state);
    static std::string parse_number(ParserState& state);
    static std::string parse_unquoted_key(ParserState& state);
    static std::string parse_quoted_string(ParserState& state, char quote);
};

} // namespace starrocks
