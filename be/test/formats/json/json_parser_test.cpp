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

#include "formats/json/json_parser.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/parallel_test.h"
#include "base/utility/defer_op.h"
#include "common/config_exec_flow_fwd.h"
#include "common/simdjson_util.h"
#include "types/simple_json_path.h"

namespace starrocks {

namespace {

constexpr int32_t kDefaultJsonParseManyBatchSize = 1000000;
constexpr bool kDefaultEnableDynamicBatchSizeForJsonParseMany = true;

} // namespace

class JsonParserTest : public ::testing::Test {
public:
    JsonParserTest() = default;
    ~JsonParserTest() override = default;

    void SetUp() override {
        _old_json_parse_many_batch_size = config::json_parse_many_batch_size;
        _old_enable_dynamic_batch_size_for_json_parse_many = config::enable_dynamic_batch_size_for_json_parse_many;
        config::json_parse_many_batch_size = kDefaultJsonParseManyBatchSize;
        config::enable_dynamic_batch_size_for_json_parse_many = kDefaultEnableDynamicBatchSizeForJsonParseMany;
    }

    void TearDown() override {
        config::json_parse_many_batch_size = _old_json_parse_many_batch_size;
        config::enable_dynamic_batch_size_for_json_parse_many = _old_enable_dynamic_batch_size_for_json_parse_many;
    }

    void test_parse_error_msg_base(JsonParser* parser, std::string& data, const std::string& operation,
                                   std::initializer_list<const char*> custom_msgs);
    void test_parse_error_msg_base(JsonParser* parser, std::string& data, const std::string& operation,
                                   const std::string& custom_msg);

private:
    int32_t _old_json_parse_many_batch_size = 0;
    bool _old_enable_dynamic_batch_size_for_json_parse_many = true;
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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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

class JsonDocumentStreamParserTest : public ::testing::Test {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    void _check_row(simdjson::ondemand::object& row, const std::string& key, int64_t value);

    simdjson::ondemand::parser _simdjson_parser;
    std::unique_ptr<JsonDocumentStreamParser> _parser;
    faststring _buf;

private:
    int32_t _old_json_parse_many_batch_size = 0;
    bool _old_enable_dynamic_batch_size_for_json_parse_many = true;
};

void JsonDocumentStreamParserTest::SetUp() {
    _old_json_parse_many_batch_size = config::json_parse_many_batch_size;
    _old_enable_dynamic_batch_size_for_json_parse_many = config::enable_dynamic_batch_size_for_json_parse_many;
    config::json_parse_many_batch_size = kDefaultJsonParseManyBatchSize;
    config::enable_dynamic_batch_size_for_json_parse_many = kDefaultEnableDynamicBatchSizeForJsonParseMany;
    _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
}

void JsonDocumentStreamParserTest::TearDown() {
    _parser.reset();
    config::json_parse_many_batch_size = _old_json_parse_many_batch_size;
    config::enable_dynamic_batch_size_for_json_parse_many = _old_enable_dynamic_batch_size_for_json_parse_many;
}

void JsonDocumentStreamParserTest::_check_row(simdjson::ondemand::object& row, const std::string& key, int64_t value) {
    ASSERT_EQ(1, row.count_fields().value());
    auto field = *row.begin();
    simdjson::ondemand::value simd_value = field.value().value();
    ASSERT_EQ(key, field.key().value());
    ASSERT_EQ(value, simd_value.get_int64().value());
}

TEST_F(JsonDocumentStreamParserTest, test_truncated_bytes) {
    std::string input = R"({"key1": 1} {"key2": 2} {"key3": 3} {"key4":)";
    size_t size = input.size();
    size_t padded_size = size + simdjson::SIMDJSON_PADDING;
    input.resize(padded_size);

    ASSERT_OK(_parser->parse(input.data(), size, padded_size));
    ASSERT_EQ(8, _parser->truncated_bytes());
}

// simdjson iterate_many stops before an incomplete document at the end of the input and only reports EOF, so
// truncated_tail() is the only trace of it. It is only known once the stream is exhausted.
TEST_F(JsonDocumentStreamParserTest, truncated_tail) {
    EXPECT_TRUE(_parser->truncated_tail().empty());

    std::string input = R"({"key1": 1} {"key2": 2} {"key3":)";
    size_t size = input.size();
    input.resize(size + simdjson::SIMDJSON_PADDING);
    ASSERT_OK(_parser->parse(input.data(), size, input.size()));

    simdjson::ondemand::object row;
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key1", 1);
    EXPECT_TRUE(_parser->truncated_tail().empty());

    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key2", 2);
    EXPECT_TRUE(_parser->truncated_tail().empty());

    EXPECT_STATUS(Status::EndOfFile(""), _parser->advance());
    EXPECT_EQ(R"({"key3":)", std::string(_parser->truncated_tail()));
}

TEST_F(JsonDocumentStreamParserTest, truncated_tail_without_complete_document) {
    std::string input = " \n{\"key1\": 1, \"key2\": [1, 2";
    size_t size = input.size();
    input.resize(size + simdjson::SIMDJSON_PADDING);
    ASSERT_OK(_parser->parse(input.data(), size, input.size()));

    simdjson::ondemand::object row;
    EXPECT_STATUS(Status::EndOfFile(""), _parser->get_current(&row));
    EXPECT_EQ(R"({"key1": 1, "key2": [1, 2)", std::string(_parser->truncated_tail()));
}

TEST_F(JsonDocumentStreamParserTest, no_truncated_tail) {
    for (const std::string data : {R"({"key1": 1} {"key2": 2})", "{\"key1\": 1}\n{\"key2\": 2}\n \t\r\n", " \n\t\r "}) {
        std::string input = data;
        size_t size = input.size();
        input.resize(size + simdjson::SIMDJSON_PADDING);
        _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
        ASSERT_OK(_parser->parse(input.data(), size, input.size()));

        simdjson::ondemand::object row;
        Status st = _parser->get_current(&row);
        while (st.ok()) {
            st = _parser->advance();
        }
        EXPECT_TRUE(st.is_end_of_file()) << st;
        EXPECT_TRUE(_parser->truncated_tail().empty()) << data;
    }
}

// With an input larger than the batch, truncated_bytes() is computed from a partial batch until the last batch is
// parsed, and it underflows there. Only the value of the last batch may be used.
TEST_F(JsonDocumentStreamParserTest, truncated_tail_multi_batch) {
    // The constructor copies the batch size, and only honours a size above simdjson::dom::MINIMAL_BATCH_SIZE.
    config::json_parse_many_batch_size = 64;
    for (const std::string tail : {"", "\n \n", R"({"key": 100, "val)"}) {
        std::string input;
        for (int i = 0; i < 100; i++) {
            input += "{\"key\": " + std::to_string(i) + "}\n";
        }
        input += tail;
        size_t size = input.size();
        input.resize(size + simdjson::SIMDJSON_PADDING);
        _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
        ASSERT_OK(_parser->parse(input.data(), size, input.size()));

        simdjson::ondemand::object row;
        for (int i = 0; i < 100; i++) {
            ASSERT_OK(_parser->get_current(&row));
            _check_row(row, "key", i);
            EXPECT_TRUE(_parser->truncated_tail().empty());
            Status st = _parser->advance();
            ASSERT_TRUE(i < 99 ? st.ok() : st.is_end_of_file()) << i << ": " << st;
        }
        EXPECT_EQ(tail.find('{') == std::string::npos ? "" : tail, std::string(_parser->truncated_tail()));
    }
}

// An incomplete document larger than the batch: the batch size grows until the rest of the input is one batch, which
// has no document left.
TEST_F(JsonDocumentStreamParserTest, truncated_tail_larger_than_batch) {
    config::json_parse_many_batch_size = 64;
    const std::string tail = "{\"key\": 1, \"val\": \"" + std::string(200, 'x');
    for (const std::string& head : {std::string(), std::string("{\"key\": 0}\n")}) {
        std::string input = head + tail;
        size_t size = input.size();
        input.resize(size + simdjson::SIMDJSON_PADDING);
        _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
        ASSERT_OK(_parser->parse(input.data(), size, input.size()));

        simdjson::ondemand::object row;
        if (!head.empty()) {
            ASSERT_OK(_parser->get_current(&row));
            _check_row(row, "key", 0);
            // The next batch fails with CAPACITY, which only get_current() sees.
            ASSERT_OK(_parser->advance());
        }
        EXPECT_STATUS(Status::EndOfFile(""), _parser->get_current(&row));
        EXPECT_EQ(tail, std::string(_parser->truncated_tail()));
    }
}

// A document larger than the batch makes the parser iterate the rest of the input again with a larger batch size.
TEST_F(JsonDocumentStreamParserTest, truncated_tail_after_batch_size_grows) {
    config::json_parse_many_batch_size = 64;
    const std::string tail = R"({"key": 3, "val)";
    std::string input = "{\"key\": 0}\n{\"key\": 1, \"big\": \"" + std::string(200, 'x') + "\"}\n{\"key\": 2}\n" + tail;
    size_t size = input.size();
    input.resize(size + simdjson::SIMDJSON_PADDING);
    _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
    ASSERT_OK(_parser->parse(input.data(), size, input.size()));

    simdjson::ondemand::object row;
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key", 0);
    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    EXPECT_EQ(1, row.find_field("key").get_int64().value());
    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key", 2);
    EXPECT_STATUS(Status::EndOfFile(""), _parser->advance());
    EXPECT_EQ(tail, std::string(_parser->truncated_tail()));
}

// A document larger than the batch makes the parser iterate the rest of the input again, from the last parsed
// document and with a larger batch size. The offsets of that document stream start at that document, and they used
// to be taken as offsets in the whole input: at the end of the input, the parser iterated again from a wrong place,
// which returned documents twice, or failed and hid the incomplete document at the end.
TEST_F(JsonDocumentStreamParserTest, batch_size_grows_far_from_the_start) {
    config::json_parse_many_batch_size = 64;
    for (bool aligned : {true, false}) {
        for (const std::string tail : {"", R"({"key": 101, "val)"}) {
            std::string input;
            for (int key = 0; key < 101; key++) {
                std::string doc = "{\"key\": " + std::to_string(key);
                if (key == 60) {
                    doc += ", \"big\": \"" + std::string(200, 'x') + "\"";
                }
                doc += "}";
                if (aligned) {
                    // Documents at multiples of 16 bytes: a wrong offset lands on another document.
                    doc.resize((doc.size() + 16) / 16 * 16 - 1, ' ');
                }
                input += doc + "\n";
            }
            input += tail;
            size_t size = input.size();
            input.resize(size + simdjson::SIMDJSON_PADDING);
            _parser = std::make_unique<JsonDocumentStreamParser>(&_simdjson_parser);
            ASSERT_OK(_parser->parse(input.data(), size, input.size()));

            std::vector<int64_t> keys;
            simdjson::ondemand::object row;
            Status st = _parser->get_current(&row);
            while (st.ok() && keys.size() <= 101) {
                keys.push_back(row.find_field("key").get_int64().value());
                st = _parser->advance();
                if (st.ok()) {
                    st = _parser->get_current(&row);
                }
            }
            std::vector<int64_t> expected(101);
            std::iota(expected.begin(), expected.end(), 0);
            EXPECT_EQ(expected, keys) << "aligned: " << aligned << ", tail: " << tail;
            EXPECT_TRUE(st.is_end_of_file()) << st;
            EXPECT_EQ(tail, std::string(_parser->truncated_tail())) << "aligned: " << aligned;
        }
    }
}

TEST_F(JsonDocumentStreamParserTest, with_dynamic_batch_size_6) {
    config::json_parse_many_batch_size = 1;
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key1": 1}
    {"keyxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx2": 2}
    {"key3": 3}
    {"key4": 4})";
    size_t size = input.size();
    size_t padded_size = size + simdjson::SIMDJSON_PADDING;
    input.resize(padded_size);

    ASSERT_OK(_parser->parse(input.data(), size, padded_size));

    simdjson::ondemand::object row;
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key1", 1);

    // double get.
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key1", 1);

    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "keyxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx2", 2);

    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key3", 3);

    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key4", 4);

    EXPECT_STATUS(Status::EndOfFile(""), _parser->advance());
}

TEST_F(JsonDocumentStreamParserTest, stream_processing) {
    // ndjson with ' ', '/t', '\n'
    std::string input = R"(   {"key1": 1} {"key2": 2}    {"key3": 3}
    {"key4": 4})";
    size_t size = input.size();
    size_t padded_size = size + simdjson::SIMDJSON_PADDING;
    input.resize(padded_size);

    ASSERT_OK(_parser->parse(input.data(), size, padded_size));
    simdjson::ondemand::object row;

    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key1", 1);

    // double get.
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key1", 1);

    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key2", 2);

    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key3", 3);

    ASSERT_OK(_parser->advance());
    ASSERT_OK(_parser->get_current(&row));
    _check_row(row, "key4", 4);

    EXPECT_STATUS(Status::EndOfFile(""), _parser->advance());
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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    const auto old_json_parse_many_batch_size = config::json_parse_many_batch_size;
    DeferOp restore_config(
            [old_json_parse_many_batch_size] { config::json_parse_many_batch_size = old_json_parse_many_batch_size; });
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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    ASSERT_OK(parse_simple_json_paths("$.key0", &jsonroot));

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
    test_parse_error_msg_base(parser, data, operation, {custom_msg.c_str()});
}

void JsonParserTest::test_parse_error_msg_base(JsonParser* parser, std::string& data, const std::string& operation,
                                               std::initializer_list<const char*> custom_msgs) {
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
    bool found_custom_msg = false;
    std::string expected_msgs;
    for (const auto* custom_msg : custom_msgs) {
        if (st.message().find(custom_msg) != std::string::npos) {
            found_custom_msg = true;
            break;
        }
        if (!expected_msgs.empty()) {
            expected_msgs.append(" or ");
        }
        expected_msgs.append(custom_msg);
    }
    ASSERT_TRUE(found_custom_msg) << "actual message: " << st.message() << ", expected substring: " << expected_msgs;
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

    // simdjson may expose the invalid token either as a non-object value or as a structural error.
    {
        std::string data = R"(:)";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  {"The value should be object type in json document stream",
                                   "Failed to iterate document stream as object"});
    }
}

TEST_F(JsonParserTest, test_json_array_parser_error) {
    simdjson::ondemand::parser simdjson_parser;

    // simdjson may expose the invalid token either as a non-array value or as a structural error.
    {
        std::string data = R"(:)";
        std::unique_ptr<JsonParser> parser(new JsonArrayParser(&simdjson_parser));
        test_parse_error_msg_base(parser.get(), data, "parse",
                                  {"the value should be array type", "Failed to parse json as array"});
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
    ASSERT_OK(parse_simple_json_paths("$.root", &jsonroot));
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

    // simdjson may expose the invalid root either as a non-object value or as a structural error.
    {
        std::string data = R"({"root": :})";
        std::unique_ptr<JsonParser> parser(new JsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  {"The value should be object type in json document stream with json root",
                                   "Failed to iterate document stream as object with json root"});
    }
}

TEST_F(JsonParserTest, test_json_array_parser_with_root_error) {
    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(parse_simple_json_paths("$.root", &jsonroot));
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

    // simdjson may expose the invalid root either as a non-object value or as a structural error.
    {
        std::string data = R"([{"root": :}])";
        std::unique_ptr<JsonParser> parser(new JsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  {"The value should be object type in json array with json root",
                                   "Failed to iterate json array as object with json root"});
    }
}

TEST_F(JsonParserTest, test_expanded_json_document_stream_parser_with_root_error) {
    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(parse_simple_json_paths("$.root", &jsonroot));
    simdjson::ondemand::parser simdjson_parser;

    // parse() error because the root is not an array
    {
        std::string data = R"({"root": 1234})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "parse", "the value should be array type");
    }

    // simdjson may expose the invalid root either as a non-array value or as a structural error.
    {
        std::string data = R"({"root": :})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(
                parser.get(), data, "parse",
                {"the value should be array type", "Failed to parse json as expanded document stream with json root"});
    }

    // simdjson may expose the invalid array element either as a non-object value or as a structural error.
    {
        std::string data = R"({"root": [:]})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  {"the value should be object type in expanded json document stream with json root",
                                   "Failed to iterate expanded document stream as object with json root"});
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

    // simdjson may expose the invalid second root either as a non-array value or as a structural error.
    {
        std::string data = R"({"root": [{"k1":1}]}{"root": :})";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonDocumentStreamParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(
                parser.get(), data, "advance",
                {"the value under json root should be array type", "Failed to iterate document stream sub-array"});
    }
}

TEST_F(JsonParserTest, test_expanded_json_array_parser_with_root_error) {
    std::vector<SimpleJsonPath> jsonroot;
    ASSERT_OK(parse_simple_json_paths("$.root", &jsonroot));
    simdjson::ondemand::parser simdjson_parser;

    // parse() error because the root is not an array
    {
        std::string data = R"([{"root": 1234}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "parse", "the value under json root should be array type");
    }

    // simdjson may expose the invalid root either as a non-array value or as a structural error.
    {
        std::string data = R"([{"root": :}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "parse",
                                  {"the value under json root should be array type",
                                   "Failed to parse json as expanded json array with json root"});
    }

    // simdjson may expose the invalid array element either as a non-object value or as a structural error.
    {
        std::string data = R"([{"root": [:]}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(parser.get(), data, "get_current",
                                  {"the value should be object type in expanded json array with json root",
                                   "Failed to iterate json array as object with json root"});
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

    // simdjson may expose the invalid second root either as a non-array value or as a structural error.
    {
        std::string data = R"([{"root": [{"k1":1}]},{"root": :}])";
        std::unique_ptr<JsonParser> parser(new ExpandedJsonArrayParserWithRoot(&simdjson_parser, jsonroot));
        test_parse_error_msg_base(
                parser.get(), data, "advance",
                {"the value under json root should be array type", "Failed to iterate json array sub-array"});
    }
}

TEST_F(JsonParserTest, test_retrieve_value_from_simdjson_object_repeatedly) {
    std::string input =
            R"({"key0": "a long long string value. ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ"})";
    std::string input_value(
            "a long long string value. ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ "
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    // Reserved for simdjson padding.
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;
    std::unique_ptr<JsonParser> parser = std::make_unique<JsonDocumentStreamParser>(&simdjson_parser);
    auto st = parser->parse(input.data(), size, padded_size);
    ASSERT_TRUE(st.ok()) << st;

    simdjson::ondemand::object row;
    st = parser->get_current(&row);
    ASSERT_TRUE(st.ok()) << st;

    std::string key("key0");
    for (int i = 0; i < 100000; ++i) {
        simdjson::ondemand::value value;
        row.find_field_unordered(key).get(value);

        faststring buffer;
        auto str_val = value_get_string_safe(&value, &buffer);

        ASSERT_FALSE(str_val.error()) << str_val.error();
        ASSERT_EQ(input_value, str_val.value());
    }
}

TEST_F(JsonParserTest, test_debezium_json_document_stream_parser) {
    // 1. Standard envelope format (upsert)
    // 2. Standard envelope format (delete)
    // 3. Flat format (upsert)
    // 4. Flat format (delete)
    std::string input = R"(
        {"payload": {"before": null, "after": {"id": 1, "name": "a"}, "op": "c"}}
        {"payload": {"before": {"id": 2, "name": "b"}, "after": null, "op": "d"}}
        {"before": null, "after": {"id": 3, "name": "c"}, "op": "u"}
        {"before": {"id": 4, "name": "d"}, "after": null, "op": "d"}
    )";
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;
    std::unique_ptr<DebeziumJsonDocumentStreamParser> parser =
            std::make_unique<DebeziumJsonDocumentStreamParser>(&simdjson_parser);

    auto st = parser->parse(input.data(), size, padded_size);
    ASSERT_TRUE(st.ok());

    simdjson::ondemand::object row;

    // Row 1: upsert, id=1, name=a
    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(0, parser->current_op());
    ASSERT_EQ(1, row.find_field_unordered("id").get_int64().value());
    ASSERT_EQ("a", row.find_field_unordered("name").get_string().value());

    // Row 2: delete, id=2, name=b
    ASSERT_OK(parser->advance());
    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(1, parser->current_op());
    ASSERT_EQ(2, row.find_field_unordered("id").get_int64().value());
    ASSERT_EQ("b", row.find_field_unordered("name").get_string().value());

    // Row 3: upsert, id=3, name=c
    ASSERT_OK(parser->advance());
    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(0, parser->current_op());
    ASSERT_EQ(3, row.find_field_unordered("id").get_int64().value());
    ASSERT_EQ("c", row.find_field_unordered("name").get_string().value());

    // Row 4: delete, id=4, name=d
    ASSERT_OK(parser->advance());
    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(1, parser->current_op());
    ASSERT_EQ(4, row.find_field_unordered("id").get_int64().value());
    ASSERT_EQ("d", row.find_field_unordered("name").get_string().value());

    ASSERT_TRUE(parser->advance().is_end_of_file());
}

TEST_F(JsonParserTest, test_debezium_tombstone_skipped) {
    // Tombstone records (payload: null) must be silently skipped so that mixed
    // streams containing tombstones and valid events are fully ingested.
    std::string input = R"({"payload": null} )"
                        R"({"payload": {"before": null, "after": {"id": 1, "name": "a"}, "op": "c"}} )"
                        R"({"payload": null} )"
                        R"({"payload": {"before": {"id": 2, "name": "b"}, "after": null, "op": "d"}} )"
                        R"({"payload": null})";
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;
    std::unique_ptr<DebeziumJsonDocumentStreamParser> parser =
            std::make_unique<DebeziumJsonDocumentStreamParser>(&simdjson_parser);

    ASSERT_OK(parser->parse(input.data(), size, padded_size));

    simdjson::ondemand::object row;

    // Leading tombstone skipped; get_current returns row 1 directly.
    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(0, parser->current_op());
    ASSERT_EQ(1, row.find_field_unordered("id").get_int64().value());
    ASSERT_EQ("a", row.find_field_unordered("name").get_string().value());

    // Tombstone between rows skipped.
    ASSERT_OK(parser->advance());
    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(1, parser->current_op());
    ASSERT_EQ(2, row.find_field_unordered("id").get_int64().value());
    ASSERT_EQ("b", row.find_field_unordered("name").get_string().value());

    // Trailing tombstone: advance to it, then get_current skips it and returns EOF.
    ASSERT_OK(parser->advance());
    ASSERT_TRUE(parser->get_current(&row).is_end_of_file());
}

TEST_F(JsonParserTest, test_debezium_json_document_stream_parser_get_current_twice) {
    std::string input = R"({"payload": {"before": null, "after": {"id": 1, "name": "a"}, "op": "c"}})";
    auto size = input.size();
    input.resize(input.size() + simdjson::SIMDJSON_PADDING);
    auto padded_size = input.size();

    simdjson::ondemand::parser simdjson_parser;
    auto parser = std::make_unique<DebeziumJsonDocumentStreamParser>(&simdjson_parser);

    ASSERT_OK(parser->parse(input.data(), size, padded_size));

    simdjson::ondemand::object row;
    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(0, parser->current_op());
    ASSERT_EQ(1, row.find_field_unordered("id").get_int64().value());

    ASSERT_OK(parser->get_current(&row));
    ASSERT_EQ(0, parser->current_op());
    ASSERT_EQ("a", row.find_field_unordered("name").get_string().value());
}

TEST_F(JsonParserTest, test_debezium_json_document_stream_parser_error_cases) {
    simdjson::ondemand::parser simdjson_parser;

    {
        std::string input = R"({"payload": {"before": null, "after": {"id": 1}, "op": 1}})";
        auto size = input.size();
        input.resize(input.size() + simdjson::SIMDJSON_PADDING);
        auto padded_size = input.size();

        auto parser = std::make_unique<DebeziumJsonDocumentStreamParser>(&simdjson_parser);
        ASSERT_OK(parser->parse(input.data(), size, padded_size));
        simdjson::ondemand::object row;
        auto st = parser->get_current(&row);
        ASSERT_TRUE(st.is_data_quality_error());
        std::string message(st.message());
        ASSERT_NE(nullptr, std::strstr(message.c_str(), "'op' field in Debezium JSON message is not a string"));
    }

    {
        std::string input = R"({"payload": {"after": null, "op": "d"}})";
        auto size = input.size();
        input.resize(input.size() + simdjson::SIMDJSON_PADDING);
        auto padded_size = input.size();

        auto parser = std::make_unique<DebeziumJsonDocumentStreamParser>(&simdjson_parser);
        ASSERT_OK(parser->parse(input.data(), size, padded_size));
        simdjson::ondemand::object row;
        auto st = parser->get_current(&row);
        ASSERT_TRUE(st.is_data_quality_error());
        std::string message(st.message());
        ASSERT_NE(nullptr, std::strstr(message.c_str(), "Missing 'before' field in Debezium delete message"));
    }

    {
        std::string input = R"({"payload": {"before": null, "op": "u"}})";
        auto size = input.size();
        input.resize(input.size() + simdjson::SIMDJSON_PADDING);
        auto padded_size = input.size();

        auto parser = std::make_unique<DebeziumJsonDocumentStreamParser>(&simdjson_parser);
        ASSERT_OK(parser->parse(input.data(), size, padded_size));
        simdjson::ondemand::object row;
        auto st = parser->get_current(&row);
        ASSERT_TRUE(st.is_data_quality_error());
        std::string message(st.message());
        ASSERT_NE(nullptr, std::strstr(message.c_str(), "Missing 'after' field in Debezium message"));
    }

    {
        std::string input = R"({"payload": "not_an_object"})";
        auto size = input.size();
        input.resize(input.size() + simdjson::SIMDJSON_PADDING);
        auto padded_size = input.size();

        auto parser = std::make_unique<DebeziumJsonDocumentStreamParser>(&simdjson_parser);
        ASSERT_OK(parser->parse(input.data(), size, padded_size));
        simdjson::ondemand::object row;
        auto st = parser->get_current(&row);
        ASSERT_TRUE(st.is_data_quality_error());
        std::string message(st.message());
        ASSERT_NE(nullptr, std::strstr(message.c_str(), "Failed to parse Debezium JSON envelope"));
    }
}

} // namespace starrocks
