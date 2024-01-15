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

#include "exec/json_parser.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "exprs/json_functions.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks {

class JsonParserTest : public ::testing::Test {
public:
    JsonParserTest() = default;
    ~JsonParserTest() override = default;

    void SetUp() override {}
};

PARALLEL_TEST(JsonParserTest, test_json_document_stream_parser) {
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key1": 1} {"key2": 2}    {"key3": 3}
    {"key4": 4})";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));

    auto st = parser->parse(input.data(), size, padded_size);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    // double get.
    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key2").get_int64();
    ASSERT_EQ(val, 2);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key3").get_int64();
    ASSERT_EQ(val, 3);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key4").get_int64();
    ASSERT_EQ(val, 4);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

PARALLEL_TEST(JsonParserTest, test_json_array_parser) {
    // json array with ' ', '/t', '\n'
    std::string input = R"( [  {"key1": 1}, {"key2": 2},    {"key3": 3},
    {"key4": 4}])";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new JsonArrayParser(&simdjson_parser));

    auto st = parser->parse(input.data(), size, padded_size);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    // double get.
    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key2").get_int64();
    ASSERT_EQ(val, 2);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key3").get_int64();
    ASSERT_EQ(val, 3);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key4").get_int64();
    ASSERT_EQ(val, 4);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

PARALLEL_TEST(JsonParserTest, test_json_document_stream_parser_with_jsonroot) {
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key0": {"key1": 1}} {"key0": {"key2": 2}}    {"key0": {"key3": 3}}
    {"key0": {"key4": 4}})";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.key0", &jsonroot));

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));

    auto st = parser->parse(input.data(), size, padded_size);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    // double get.
    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key2").get_int64();
    ASSERT_EQ(val, 2);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key3").get_int64();
    ASSERT_EQ(val, 3);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key4").get_int64();
    ASSERT_EQ(val, 4);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

PARALLEL_TEST(JsonParserTest, test_json_array_parser_with_jsonroot) {
    // json array with ' ', '/t', '\n'
    std::string input = R"([   {"key0": {"key1": 1}}, {"key0": {"key2": 2}}  ,  {"key0": {"key3": 3}},
    {"key0": {"key4": 4}}])";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.key0", &jsonroot));

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new JsonArrayParserWithRoot(&simdjson_parser, jsonroot));

    auto st = parser->parse(input.data(), size, padded_size);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    // double get.
    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key2").get_int64();
    ASSERT_EQ(val, 2);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key3").get_int64();
    ASSERT_EQ(val, 3);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key4").get_int64();
    ASSERT_EQ(val, 4);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

PARALLEL_TEST(JsonParserTest, test_expanded_json_document_stream_parser_with_jsonroot) {
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key0": [{"key1": 1},  {"key2": 2}]}    {"key0": [{"key3": 3},
    {"key4": 4}]})";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.key0", &jsonroot));

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));

    auto st = parser->parse(input.data(), size, padded_size);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    // double get.
    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key2").get_int64();
    ASSERT_EQ(val, 2);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());

    val = row.find_field("key3").get_int64();
    ASSERT_EQ(val, 3);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key4").get_int64();
    ASSERT_EQ(val, 4);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

PARALLEL_TEST(JsonParserTest, test_expanded_json_array_parser_with_jsonroot) {
    // json array with ' ', '/t', '\n'
    std::string input = R"([   {"key0": [{"key1": 1},  {"key2": 2}]},    {"key0": [{"key3": 3},
    {"key4": 4}]} ])";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.key0", &jsonroot));

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));

    auto st = parser->parse(input.data(), size, padded_size);
    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    // double get.
    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key2").get_int64();
    ASSERT_EQ(val, 2);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key3").get_int64();
    ASSERT_EQ(val, 3);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    val = row.find_field("key4").get_int64();
    ASSERT_EQ(val, 4);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

PARALLEL_TEST(JsonParserTest, test_illegal_document_stream) {
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key1": 1} "key2": 2}    {"key3": 3}
    {"key4": 4})";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));

    auto st = parser->parse(input.data(), size, padded_size);

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_STREQ(parser->left_bytes_string(64).data(), R"("key2": 2}    {"key3": 3}
    {"key4": 4})");
}

PARALLEL_TEST(JsonParserTest, test_illegal_json_array) {
    // json array with ' ', '/t', '\n'
    std::string input = R"( [  {"key1": 1}, "key2": 2},    {"key3": 3},
    {"key4": 4}])";
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;

    std::unique_ptr<JsonParser> parser(new JsonArrayParser(&simdjson_parser));

    auto st = parser->parse(input.data(), size, padded_size);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key1").get_int64();
    ASSERT_EQ(val, 1);

    st = parser->advance();
    ASSERT_TRUE(st.ok());

    st = parser->get_current(&row);
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_STREQ(parser->left_bytes_string(64).data(), R"("key2": 2},    {"key3": 3},
    {"key4": 4}])");
}

PARALLEL_TEST(JsonParserTest, test_big_value) {
    simdjson::ondemand::parser simdjson_parser;
    // The padded_string would allocate memory with simdjson::SIMDJSON_PADDING bytes padding.
    simdjson::padded_string input =
            simdjson::padded_string::load("./be/test/exec/test_data/json_scanner/big_value.json");

    std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));

    auto st = parser->parse(input.data(), input.size(), input.size() + simdjson::SIMDJSON_PADDING);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key2").get_int64();
    ASSERT_EQ(val, 12345);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

PARALLEL_TEST(JsonParserTest, test_big_json) {
    simdjson::ondemand::parser simdjson_parser;
    // The padded_string would allocate memory with simdjson::SIMDJSON_PADDING bytes padding.
    simdjson::padded_string input = simdjson::padded_string::load("./be/test/exec/test_data/json_scanner/big.json");

    std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));

    auto st = parser->parse(input.data(), input.size(), input.size() + simdjson::SIMDJSON_PADDING);

    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok());
    int64_t val = row.find_field("key10086").get_int64();
    ASSERT_EQ(val, 10086);

    st = parser->advance();
    ASSERT_TRUE(st.is_end_of_file());
}

} // namespace starrocks
