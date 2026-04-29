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

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include "column/variant_encoder.h"

namespace starrocks {

struct PathTestCase {
    std::string path;
    std::vector<std::string> expected_segments;
    std::vector<std::string> expected_types; // "object" or "array"
    bool should_succeed;

    PathTestCase(const std::string& p, const std::vector<std::string>& segs, const std::vector<std::string>& types,
                 bool succeed = true)
            : path(p), expected_segments(segs), expected_types(types), should_succeed(succeed) {}
};

class VariantPathParserTest : public ::testing::TestWithParam<PathTestCase> {
public:
    static std::vector<PathTestCase> get_test_cases() {
        return {
                // Root only
                {"$", {}, {}, true},

                // Simple field access
                {"$.name", {"name"}, {"object"}, true},
                {"$.field1", {"field1"}, {"object"}, true},

                // Nested field access
                {"$.field1.field2", {"field1", "field2"}, {"object", "object"}, true},
                {"$.user.profile.name", {"user", "profile", "name"}, {"object", "object", "object"}, true},

                // Array index access
                {"$[0]", {"0"}, {"array"}, true},
                {"$[123]", {"123"}, {"array"}, true},
                {"$[999]", {"999"}, {"array"}, true},

                // Mixed access patterns
                {"$.field[0]", {"field", "0"}, {"object", "array"}, true},
                {"$.users[0].name", {"users", "0", "name"}, {"object", "array", "object"}, true},
                {"$.data[5].items[2]", {"data", "5", "items", "2"}, {"object", "array", "object", "array"}, true},

                // Quoted keys
                {"$['quoted_key']", {"quoted_key"}, {"object"}, true},
                {"$[\"double_quoted\"]", {"double_quoted"}, {"object"}, true},
                {"$.field['special-key']", {"field", "special-key"}, {"object", "object"}, true},
                {"$['key with spaces']", {"key with spaces"}, {"object"}, true},

                // Complex paths
                {"$.arr[0].field['key']", {"arr", "0", "field", "key"}, {"object", "array", "object", "object"}, true},
                {"$.users[1].profile['personal-info'].address",
                 {"users", "1", "profile", "personal-info", "address"},
                 {"object", "array", "object", "object", "object"},
                 true},

                // Invalid cases
                {"", {}, {}, false},            // Empty string
                {"invalid", {}, {}, false},     // No $ prefix
                {"$.", {}, {}, false},          // Incomplete dot notation
                {"$[", {}, {}, false},          // Incomplete bracket
                {"$[]", {}, {}, false},         // Empty brackets
                {"$[abc]", {}, {}, false},      // Invalid array index
                {"$['unclosed", {}, {}, false}, // Unclosed quote
                {"$.field[", {}, {}, false},    // Incomplete bracket after field
        };
    }
};

TEST_P(VariantPathParserTest, ParseAndVerifySegments) {
    PathTestCase test_case = GetParam();

    auto result = VariantPathParser::parse(test_case.path);

    if (!test_case.should_succeed) {
        EXPECT_FALSE(result.ok()) << "Should fail to parse invalid path: " << test_case.path;
        return;
    }

    ASSERT_TRUE(result.ok()) << "Failed to parse valid path: " << test_case.path;

    const auto& segments = result.value().segments;
    EXPECT_EQ(segments.size(), test_case.expected_segments.size())
            << "Unexpected number of segments for path: " << test_case.path;

    for (size_t i = 0; i < segments.size() && i < test_case.expected_segments.size(); ++i) {
        const auto& segment = segments[i];
        const std::string& expected_value = test_case.expected_segments[i];
        const std::string& expected_type = test_case.expected_types[i];

        if (expected_type == "object") {
            EXPECT_TRUE(segment.is_object())
                    << "Expected object extraction at index " << i << " for path: " << test_case.path;
            if (segment.is_object()) {
                EXPECT_EQ(segment.get_key(), expected_value)
                        << "Unexpected object key at index " << i << " for path: " << test_case.path;
            }
        } else if (expected_type == "array") {
            EXPECT_TRUE(segment.is_array())
                    << "Expected array extraction at index " << i << " for path: " << test_case.path;
            if (segment.is_array()) {
                EXPECT_EQ(std::to_string(segment.get_index()), expected_value)
                        << "Unexpected array index at index " << i << " for path: " << test_case.path;
            }
        } else {
            FAIL() << "Unknown expected type: " << expected_type << " at index " << i;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(VariantPathParsingTests, VariantPathParserTest,
                         ::testing::ValuesIn(VariantPathParserTest::get_test_cases()),
                         [](const ::testing::TestParamInfo<PathTestCase>& info) {
                             std::string name = info.param.path;

                             // Handle special cases first before general replacement
                             if (name.empty()) {
                                 return std::string("empty_string");
                             }
                             if (name == "$") {
                                 return std::string("root_only");
                             }
                             if (name == "$.") {
                                 return std::string("incomplete_dot");
                             }
                             if (name == "$[") {
                                 return std::string("incomplete_bracket");
                             }
                             if (name == "$[]") {
                                 return std::string("empty_brackets");
                             }
                             if (name == "invalid") {
                                 return std::string("no_dollar_prefix");
                             }

                             // Replace special characters for test name
                             std::replace(name.begin(), name.end(), '$', '_');
                             std::replace(name.begin(), name.end(), '.', '_');
                             std::replace(name.begin(), name.end(), '[', '_');
                             std::replace(name.begin(), name.end(), ']', '_');
                             std::replace(name.begin(), name.end(), '\'', '_');
                             std::replace(name.begin(), name.end(), '"', '_');
                             std::replace(name.begin(), name.end(), ' ', '_');
                             std::replace(name.begin(), name.end(), '-', '_');

                             // Remove leading underscore if present
                             if (!name.empty() && name[0] == '_') {
                                 name = name.substr(1);
                             }

                             // Ensure we don't have empty names after processing
                             if (name.empty() || name == "_") {
                                 return std::string("unnamed_case");
                             }

                             return name;
                         });

class VariantPathTest : public ::testing::Test {};

TEST_F(VariantPathTest, SpecificPathTests) {
    // Test specific parsing scenarios with exact expectations
    struct {
        std::string path;
        std::function<void(const std::vector<VariantSegment>&)> verifier;
    } specific_tests[] = {
            {"$.users[0].profile['personal-data'].address", [](const std::vector<VariantSegment>& segments) {
                 EXPECT_EQ(segments.size(), 5);

                 // $.users
                 EXPECT_TRUE(segments[0].is_object());
                 EXPECT_EQ(segments[0].get_key(), "users");

                 // [0]
                 EXPECT_TRUE(segments[1].is_array());
                 EXPECT_EQ(segments[1].get_index(), 0);

                 // .profile
                 EXPECT_TRUE(segments[2].is_object());
                 EXPECT_EQ(segments[2].get_key(), "profile");

                 // ['personal-data']
                 EXPECT_TRUE(segments[3].is_object());
                 EXPECT_EQ(segments[3].get_key(), "personal-data");

                 // .address
                 EXPECT_TRUE(segments[4].is_object());
                 EXPECT_EQ(segments[4].get_key(), "address");
             }}};

    for (const auto& test : specific_tests) {
        auto result = VariantPathParser::parse(test.path);
        ASSERT_TRUE(result.ok()) << "Failed to parse: " << test.path;
        test.verifier(result.value().segments);
    }
}

// Verifies "$" can map to root shredded path "".
TEST_F(VariantPathTest, ToShreddedPathRoot) {
    auto result = VariantPathParser::parse(std::string("$"));
    ASSERT_TRUE(result.ok());
    auto shredded = result->to_shredded_path();
    ASSERT_TRUE(shredded.has_value());
    ASSERT_EQ("", shredded.value());
}

// Verifies pure object path maps to dotted shredded path.
TEST_F(VariantPathTest, ToShreddedPathObject) {
    auto result = VariantPathParser::parse(std::string("$.a.b"));
    ASSERT_TRUE(result.ok());
    auto shredded = result->to_shredded_path();
    ASSERT_TRUE(shredded.has_value());
    ASSERT_EQ("a.b", shredded.value());
}

// Verifies that a key containing a literal dot uses bracket notation in shredded path.
TEST_F(VariantPathTest, ToShreddedPathQuotedDottedKey) {
    auto result = VariantPathParser::parse(std::string("$['a.b']"));
    ASSERT_TRUE(result.ok());
    auto shredded = result->to_shredded_path();
    ASSERT_TRUE(shredded.has_value());
    ASSERT_EQ("['a.b']", shredded.value());
}

// Verifies that an empty key uses bracket notation in shredded path.
TEST_F(VariantPathTest, ToShreddedPathEmptyKey) {
    auto result = VariantPathParser::parse(std::string("$['']"));
    ASSERT_TRUE(result.ok());
    auto shredded = result->to_shredded_path();
    ASSERT_TRUE(shredded.has_value());
    ASSERT_EQ("['']", shredded.value());
}

// Verifies paths with array segments cannot map to shredded typed path.
TEST_F(VariantPathTest, ToShreddedPathArrayReturnsNullopt) {
    auto result = VariantPathParser::parse(std::string("$.a[1].b"));
    ASSERT_TRUE(result.ok());
    auto shredded = result->to_shredded_path();
    ASSERT_FALSE(shredded.has_value());
}

// ----- parse_shredded_path round-trip tests -----

// parse_shredded_path("") == root (empty segments).
TEST_F(VariantPathTest, ParseShreddedPathRoot) {
    auto result = VariantPathParser::parse_shredded_path(std::string_view(""));
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result->segments.empty());
}

// Simple dotted path: "a.b.c" -> ["a", "b", "c"].
TEST_F(VariantPathTest, ParseShreddedPathDotted) {
    auto result = VariantPathParser::parse_shredded_path(std::string_view("a.b.c"));
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result->segments.size(), 3u);
    EXPECT_EQ(result->segments[0].get_key(), "a");
    EXPECT_EQ(result->segments[1].get_key(), "b");
    EXPECT_EQ(result->segments[2].get_key(), "c");
}

// Bracket notation first segment: "['key.a'].b" -> ["key.a", "b"].
TEST_F(VariantPathTest, ParseShreddedPathBracketFirst) {
    auto result = VariantPathParser::parse_shredded_path(std::string_view("['key.a'].b"));
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result->segments.size(), 2u);
    EXPECT_EQ(result->segments[0].get_key(), "key.a");
    EXPECT_EQ(result->segments[1].get_key(), "b");
}

// Bracket notation middle: "a['key.a'].b" -> ["a", "key.a", "b"].
TEST_F(VariantPathTest, ParseShreddedPathBracketMiddle) {
    auto result = VariantPathParser::parse_shredded_path(std::string_view("a['key.a'].b"));
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result->segments.size(), 3u);
    EXPECT_EQ(result->segments[0].get_key(), "a");
    EXPECT_EQ(result->segments[1].get_key(), "key.a");
    EXPECT_EQ(result->segments[2].get_key(), "b");
}

// Round-trip: to_shredded_path -> parse_shredded_path -> segments match.
TEST_F(VariantPathTest, RoundTripSimple) {
    auto parsed = VariantPathParser::parse(std::string("$.a.b"));
    ASSERT_TRUE(parsed.ok());
    auto shredded = parsed->to_shredded_path();
    ASSERT_TRUE(shredded.has_value());
    auto back = VariantPathParser::parse_shredded_path(std::string_view(shredded.value()));
    ASSERT_TRUE(back.ok());
    ASSERT_EQ(back->segments.size(), 2u);
    EXPECT_EQ(back->segments[0].get_key(), "a");
    EXPECT_EQ(back->segments[1].get_key(), "b");
}

// Round-trip with bracket key: "$['key.a'].b"
TEST_F(VariantPathTest, RoundTripBracketKey) {
    auto parsed = VariantPathParser::parse(std::string("$['key.a'].b"));
    ASSERT_TRUE(parsed.ok());
    auto shredded = parsed->to_shredded_path();
    ASSERT_TRUE(shredded.has_value());
    auto back = VariantPathParser::parse_shredded_path(std::string_view(shredded.value()));
    ASSERT_TRUE(back.ok());
    ASSERT_EQ(back->segments.size(), 2u);
    EXPECT_EQ(back->segments[0].get_key(), "key.a");
    EXPECT_EQ(back->segments[1].get_key(), "b");
}

// parse_shredded_path of a single key with no dots: "name" -> ["name"].
TEST_F(VariantPathTest, ParseShreddedPathSingleKey) {
    auto result = VariantPathParser::parse_shredded_path(std::string_view("name"));
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result->segments.size(), 1u);
    EXPECT_EQ(result->segments[0].get_key(), "name");
}

TEST_F(VariantPathTest, SeekViewReturnsExpectedNestedValue) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(R"({"a":{"b":[10,20]},"x":1})");
    ASSERT_TRUE(encoded.ok());
    VariantRowValue row = std::move(encoded).value();

    auto path = VariantPathParser::parse(std::string("$.a.b[1]"));
    ASSERT_TRUE(path.ok());

    VariantRowRef row_ref = row.as_ref();
    auto view_seek = VariantPath::seek_view(row_ref, path.value());
    ASSERT_TRUE(view_seek.ok());
    auto json = view_seek->to_owned().to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ("20", json.value());
}

TEST_F(VariantPathTest, SeekViewRootReturnsWholeValue) {
    auto encoded = VariantEncoder::encode_json_text_to_variant(R"({"k":"v","n":123})");
    ASSERT_TRUE(encoded.ok());
    VariantRowValue row = std::move(encoded).value();

    auto root_path = VariantPathParser::parse(std::string("$"));
    ASSERT_TRUE(root_path.ok());

    VariantRowRef row_ref = row.as_ref();
    auto view_seek = VariantPath::seek_view(row_ref, root_path.value());
    ASSERT_TRUE(view_seek.ok());
    auto json = view_seek->to_owned().to_json();
    ASSERT_TRUE(json.ok());
    EXPECT_EQ(R"({"k":"v","n":123})", json.value());
}

TEST_F(VariantPathTest, SegmentEquality) {
    auto p1 = VariantPathParser::parse_shredded_path(std::string_view("foo.bar"));
    auto p2 = VariantPathParser::parse_shredded_path(std::string_view("foo.bar"));
    auto p3 = VariantPathParser::parse_shredded_path(std::string_view("foo.baz"));
    ASSERT_TRUE(p1.ok());
    ASSERT_TRUE(p2.ok());
    ASSERT_TRUE(p3.ok());

    // Same key segments are equal.
    EXPECT_EQ(p1->segments[0], p2->segments[0]);
    EXPECT_EQ(p1->segments[1], p2->segments[1]);
    // Different key segments are not equal.
    EXPECT_NE(p1->segments[1], p3->segments[1]);
    // Segment from different positions with same key are equal.
    EXPECT_EQ(p1->segments[0], p3->segments[0]);
}

TEST_F(VariantPathTest, IsStrictPrefixOf) {
    auto a = VariantPathParser::parse_shredded_path(std::string_view("a"));
    auto ab = VariantPathParser::parse_shredded_path(std::string_view("a.b"));
    auto abc = VariantPathParser::parse_shredded_path(std::string_view("a.b.c"));
    auto xy = VariantPathParser::parse_shredded_path(std::string_view("x.y"));
    ASSERT_TRUE(a.ok() && ab.ok() && abc.ok() && xy.ok());

    // Strict prefix: "a" < "a.b" < "a.b.c"
    EXPECT_TRUE(a->is_strict_prefix_of(*ab));
    EXPECT_TRUE(a->is_strict_prefix_of(*abc));
    EXPECT_TRUE(ab->is_strict_prefix_of(*abc));

    // Equal paths are not strict prefixes.
    EXPECT_FALSE(ab->is_strict_prefix_of(*ab));
    EXPECT_FALSE(abc->is_strict_prefix_of(*abc));

    // Longer path cannot be a prefix of a shorter one.
    EXPECT_FALSE(abc->is_strict_prefix_of(*ab));
    EXPECT_FALSE(ab->is_strict_prefix_of(*a));

    // Unrelated paths are not prefixes of each other.
    EXPECT_FALSE(ab->is_strict_prefix_of(*xy));
    EXPECT_FALSE(xy->is_strict_prefix_of(*ab));
}

TEST_F(VariantPathTest, IsAncestorOrSame) {
    auto a = VariantPathParser::parse_shredded_path(std::string_view("a"));
    auto ab = VariantPathParser::parse_shredded_path(std::string_view("a.b"));
    auto abc = VariantPathParser::parse_shredded_path(std::string_view("a.b.c"));
    auto xy = VariantPathParser::parse_shredded_path(std::string_view("x.y"));
    ASSERT_TRUE(a.ok() && ab.ok() && abc.ok() && xy.ok());

    // Ancestor-or-same: equal paths.
    EXPECT_TRUE(ab->is_ancestor_or_same(*ab));
    EXPECT_TRUE(abc->is_ancestor_or_same(*abc));

    // Ancestor-or-same: strict prefix.
    EXPECT_TRUE(a->is_ancestor_or_same(*ab));
    EXPECT_TRUE(a->is_ancestor_or_same(*abc));
    EXPECT_TRUE(ab->is_ancestor_or_same(*abc));

    // Not an ancestor: longer path is not an ancestor of a shorter one.
    EXPECT_FALSE(abc->is_ancestor_or_same(*ab));
    EXPECT_FALSE(ab->is_ancestor_or_same(*a));

    // Unrelated paths.
    EXPECT_FALSE(ab->is_ancestor_or_same(*xy));
    EXPECT_FALSE(xy->is_ancestor_or_same(*ab));
}

} // namespace starrocks
