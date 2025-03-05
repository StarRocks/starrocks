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

    void test_parse_error_msg_base(JsonParser* parser, std::string& data, const std::string& operation,
                                   const std::string& custom_msg);
};

TEST_F(JsonParserTest, test_json_document_stream_parser_with_dynamic_batch_size_1) {
    config::json_parse_many_batch_size = 41;
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

TEST_F(JsonParserTest, test_json_document_stream_parser_with_dynamic_batch_size_2) {
    config::json_parse_many_batch_size = 40;
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key0": [{"key1": 1},  {"key2": 2}]}    {"key0": [{"key3": 3},
    {"key4": 4}]} {xxxxxxxxxx})";
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
    ASSERT_FALSE(st.ok());
}

TEST_F(JsonParserTest, test_json_document_stream_parser_with_dynamic_batch_size_3) {
    config::json_parse_many_batch_size = 40;
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key0": [{"key1": 1},  {"key2": 2}]}  {xxxxxxxxxxx}  {"key0": [{"key3": 3},
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
    ASSERT_FALSE(st.ok());
}

TEST_F(JsonParserTest, test_json_document_stream_parser_with_dynamic_batch_size_4) {
    config::json_parse_many_batch_size = 40;
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key0": [{"key1": 1},  {"key2": 2}]}    {"key0": [{"key3": 3},
    {"key4": 4}]} asdasd )";
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
    ASSERT_FALSE(st.ok());
}

TEST_F(JsonParserTest, test_json_document_stream_parser_with_dynamic_batch_size_5) {
    config::json_parse_many_batch_size = 40;
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key0": [{"key1": 1},  {"key2": 2}]} adasdas   {"key0": [{"key3": 3},
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
    ASSERT_FALSE(st.ok());
}

TEST_F(JsonParserTest, test_json_document_stream_parser_with_dynamic_batch_size_6) {
    config::json_parse_many_batch_size = 1;
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key1": 1} 
    {"keyxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx2": 2}    
    {"key3": 3}
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
    val = row.find_field("keyxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx2").get_int64();
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

PARALLEL_TEST(JsonParserTest, test_big_illegal_document_stream) {
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key1": 1} "key2": 012345678901234567890123456789012345678901234567890"} {"key3": 3})";
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
    ASSERT_STREQ(parser->left_bytes_string(10).data(), R"("key2": 01)");
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

PARALLEL_TEST(JsonParserTest, test_big_illegal_json_array) {
    // json array with ' ', '/t', '\n'
    std::string input = R"( [  {"key1": 1}, "key2": "123456789012345678901234567890"},    {"key3": 3},
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
    ASSERT_STREQ(parser->left_bytes_string(10).data(), R"("key2": "1)");
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
    config::json_parse_many_batch_size = 1;
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

PARALLEL_TEST(JsonParserTest, test_document_stream_parser_invalid_type_array) {
    std::string input = R"(   [{"key":1},{"key":2}]   )";
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
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_TRUE(st.message().find("The value is array type in json document stream, you can set strip_outer_array=true "
                                  "to parse each element of the array as individual rows") != std::string::npos);
}

PARALLEL_TEST(JsonParserTest, test_document_stream_parser_invalid_type_not_object) {
    std::string input = R"(   1   )";
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
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_TRUE(st.message().find("The value should be object type in json document stream") != std::string::npos);
}

PARALLEL_TEST(JsonParserTest, test_document_stream_parser_with_jsonroot_invalid_type_array) {
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key0": [{"key1":1},{"key1":2}]}    {"key0":[{"key1":3},{"key1":4}]}  )";
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
    ASSERT_TRUE(
            st.message().find("The value is array type in json document stream with json root, you can set "
                              "strip_outer_array=true to parse each element of the array as individual rows, value: "
                              "[{\"key1\":1},{\"key1\":2}]") != std::string::npos);
}

PARALLEL_TEST(JsonParserTest, test_array_parser_with_jsonroot_invalid_type_array) {
    // json array with ' ', '/t', '\n'
    std::string input = R"([   {"key0": [{"key1":1},{"key1":2}]},    {"key0": [{"key1":3},{"key1":4}]} ])";
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
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_TRUE(st.message().find(
                        "The value is array type in json array with json root, you can set strip_outer_array=true to "
                        "parse each element of the array as individual rows, value: [{\"key1\":1},{\"key1\":2}]") !=
                std::string::npos);
}

void JsonParserTest::test_parse_error_msg_base(JsonParser* parser, std::string& data, const std::string& operation,
                                               const std::string& custom_msg) {
    auto size = data.size();
    data.resize(data.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = data.size();
    Status st;
    if (operation == "parse") {
        st = parser->parse(data.data(), size, padded_size);
    } else if (operation == "get_current") {
        ASSERT_OK(parser->parse(data.data(), size, padded_size));
        simdjson::ondemand::object object;
        st = parser->get_current(&object);
    } else if (operation == "advance") {
        ASSERT_OK(parser->parse(data.data(), size, padded_size));
        simdjson::ondemand::object object;
        ASSERT_OK(parser->get_current(&object));
        st = parser->advance();
    }
    ASSERT_TRUE(st.is_data_quality_error());
    ASSERT_TRUE(st.message().find("parse error") != std::string::npos);
    ASSERT_TRUE(st.message().find(custom_msg) != std::string::npos);
}

TEST_F(JsonParserTest, test_json_document_stream_parser_error) {
    simdjson::ondemand::parser simdjson_parser;

    // get_current() error because doc is an array
    {
        std::string data = R"([{"key1": 1}])";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "get_current", "The value is array type in json document stream");
    }

    // get_current() error because doc is a string
    {
        std::string data = R"("invalid")";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "The value should be object type in json document stream");
    }

    // get_current() error because json is invalid
    {
        std::string data = R"(:)";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "get_current", "Failed to iterate document stream as object");
    }
}

TEST_F(JsonParserTest, test_json_array_parser_error) {
    simdjson::ondemand::parser simdjson_parser;

    // parse() error because json is invalid
    {
        std::string data = R"(:)";
        std::unique_ptr<JsonParser> parser(new JsonArrayParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "parse", "Failed to parse json as array");
    }

    // parse() error because doc is not an array
    {
        std::string data = R"({"key1": 1})";
        std::unique_ptr<JsonParser> parser(new JsonArrayParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "parse", "the value should be array type");
    }

    // get_current() error because the element is not a valid json
    {
        std::string data = R"(["key1": 1}])";
        std::unique_ptr<JsonParser> parser(new JsonArrayParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "get_current", "Failed to iterate json array as object");
    }
}

TEST_F(JsonParserTest, test_json_document_stream_parser_with_root_error) {
    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.root", &jsonroot));
    simdjson::ondemand::parser simdjson_parser;

    // get_current() error because doc is an array
    {
        std::string data = R"({"root": []})";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "The value is array type in json document stream with json root");
    }

    // get_current() error because the root is an integer
    {
        std::string data = R"({"root": 123})";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "The value should be object type in json document stream with json root");
    }

    // get_current() error because the root is invalid
    {
        std::string data = R"({"root": :})";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "Failed to iterate document stream as object with json root");
    }
}

TEST_F(JsonParserTest, test_json_array_parser_with_root_error) {
    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.root", &jsonroot));
    simdjson::ondemand::parser simdjson_parser;

    // get_current() error because the root is an array
    {
        std::string data = R"([{"root": []}])";
        std::unique_ptr<JsonParser> parser(new JsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "The value is array type in json array with json root");
    }

    // get_current() error because the root is not an object
    {
        std::string data = R"([{"root": 123}])";
        std::unique_ptr<JsonParser> parser(new JsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "The value should be object type in json array with json root");
    }

    // get_current() error because it's invalid
    {
        std::string data = R"([{"root": :}])";
        std::unique_ptr<JsonParser> parser(new JsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "Failed to iterate json array as object with json root");
    }
}

TEST_F(JsonParserTest, test_expanded_json_document_stream_parser_with_root_error) {
    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.root", &jsonroot));
    simdjson::ondemand::parser simdjson_parser;

    // parse() error because the root is not an array
    {
        std::string data = R"({"root": 1234})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "parse", "the value should be array type");
    }

    // parse() error because the root is an invalid json
    {
        std::string data = R"({"root": :})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "parse",
                                  "Failed to parse json as expanded document stream with json root");
    }

    // get_current() error because the root is an invalid array
    {
        std::string data = R"({"root": [:]})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "Failed to iterate expanded document stream as object with json root");
    }

    // get_current() error because the root array element is not object
    {
        std::string data = R"({"root": [123]})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "the value should be object type in expanded json document stream with json root");
    }

    // advance() error because the second document is not an array
    {
        std::string data = R"({"root": [{"k1":1}]}{"root":123})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "advance", "the value under json root should be array type ");
    }

    // advance() error because the second root is an invalid json
    {
        std::string data = R"({"root": [{"k1":1}]}{"root": :})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "advance", "Failed to iterate document stream sub-array");
    }
}

TEST_F(JsonParserTest, test_expanded_json_array_parser_with_root_error) {
    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(JsonFunctions::parse_json_paths("$.root", &jsonroot));
    simdjson::ondemand::parser simdjson_parser;

    // parse() error because the root is not an array
    {
        std::string data = R"([{"root": 1234}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "parse", "the value under json root should be array type");
    }

    // parse() error because the root is not an array
    {
        std::string data = R"([{"root": :}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "parse",
                                  "Failed to parse json as expanded json array with json root");
    }

    // get_current() error because the root is an invalid array
    {
        std::string data = R"([{"root": [:]}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "Failed to iterate json array as object with json root");
    }

    // get_current() error because the array element is not object
    {
        std::string data = R"([{"root": [123]}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  "the value should be object type in expanded json array with json root");
    }

    // advance() error because the second document is not an array
    {
        std::string data = R"([{"root": [{"k1":1}]},{"root":123}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "advance", "the value under json root should be array type");
    }

    // advance() error because the second document is an invalid json
    {
        std::string data = R"([{"root": [{"k1":1}]},{"root": :}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "advance", "Failed to iterate json array sub-array");
    }
}

} // namespace starrocks
