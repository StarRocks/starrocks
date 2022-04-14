// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/json_parser.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "exprs/vectorized/json_functions.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"

namespace starrocks::vectorized {

class JsonParserTest : public ::testing::Test {
public:
    JsonParserTest() {}
    virtual ~JsonParserTest() {}

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

    std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser);

    auto st = parser->parse(reinterpret_cast<uint8_t*>(input.data()), size, padded_size);

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

    std::unique_ptr<JsonParser> parser(new JsonArrayParser);

    auto st = parser->parse(reinterpret_cast<uint8_t*>(input.data()), size, padded_size);

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
    JsonFunctions::parse_json_paths("$.key0", &jsonroot);

    std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParserWithRoot(jsonroot));

    auto st = parser->parse(reinterpret_cast<uint8_t*>(input.data()), size, padded_size);

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
    JsonFunctions::parse_json_paths("$.key0", &jsonroot);

    std::unique_ptr<JsonParser> parser(new JsonArrayParserWithRoot(jsonroot));

    auto st = parser->parse(reinterpret_cast<uint8_t*>(input.data()), size, padded_size);

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
    JsonFunctions::parse_json_paths("$.key0", &jsonroot);

    std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(jsonroot));

    auto st = parser->parse(reinterpret_cast<uint8_t*>(input.data()), size, padded_size);

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
    JsonFunctions::parse_json_paths("$.key0", &jsonroot);

    std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(jsonroot));

    auto st = parser->parse(reinterpret_cast<uint8_t*>(input.data()), size, padded_size);
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

} // namespace starrocks::vectorized
