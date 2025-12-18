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

#include "exprs/variant_path_parser.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

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
            EXPECT_TRUE(std::holds_alternative<ObjectExtraction>(segment))
                    << "Expected object extraction at index " << i << " for path: " << test_case.path;

            if (std::holds_alternative<ObjectExtraction>(segment)) {
                const auto& obj_extraction = std::get<ObjectExtraction>(segment);
                EXPECT_EQ(obj_extraction.get_key(), expected_value)
                        << "Unexpected object key at index " << i << " for path: " << test_case.path;
            }
        } else if (expected_type == "array") {
            EXPECT_TRUE(std::holds_alternative<ArrayExtraction>(segment))
                    << "Expected array extraction at index " << i << " for path: " << test_case.path;

            if (std::holds_alternative<ArrayExtraction>(segment)) {
                const auto& arr_extraction = std::get<ArrayExtraction>(segment);
                EXPECT_EQ(std::to_string(arr_extraction.get_index()), expected_value)
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

class VariantPathParserBasicTest : public ::testing::Test {};

TEST_F(VariantPathParserBasicTest, SpecificPathTests) {
    // Test specific parsing scenarios with exact expectations
    struct {
        std::string path;
        std::function<void(const std::vector<VariantPathExtraction>&)> verifier;
    } specific_tests[] = {
            {"$.users[0].profile['personal-data'].address", [](const std::vector<VariantPathExtraction>& segments) {
                 EXPECT_EQ(segments.size(), 5);

                 // $.users
                 EXPECT_TRUE(std::holds_alternative<ObjectExtraction>(segments[0]));
                 EXPECT_EQ(std::get<ObjectExtraction>(segments[0]).get_key(), "users");

                 // [0]
                 EXPECT_TRUE(std::holds_alternative<ArrayExtraction>(segments[1]));
                 EXPECT_EQ(std::get<ArrayExtraction>(segments[1]).get_index(), 0);

                 // .profile
                 EXPECT_TRUE(std::holds_alternative<ObjectExtraction>(segments[2]));
                 EXPECT_EQ(std::get<ObjectExtraction>(segments[2]).get_key(), "profile");

                 // ['personal-data']
                 EXPECT_TRUE(std::holds_alternative<ObjectExtraction>(segments[3]));
                 EXPECT_EQ(std::get<ObjectExtraction>(segments[3]).get_key(), "personal-data");

                 // .address
                 EXPECT_TRUE(std::holds_alternative<ObjectExtraction>(segments[4]));
                 EXPECT_EQ(std::get<ObjectExtraction>(segments[4]).get_key(), "address");
             }}};

    for (const auto& test : specific_tests) {
        auto result = VariantPathParser::parse(test.path);
        ASSERT_TRUE(result.ok()) << "Failed to parse: " << test.path;
        test.verifier(result.value().segments);
    }
}

} // namespace starrocks