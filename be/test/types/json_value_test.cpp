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

#include "types/json_value.h"

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <gutil/strings/substitute.h>

#include <cstring>
#include <memory>
#include <tuple>
#include <vector>

#include "velocypack/vpack.h"

namespace starrocks {

TEST(JsonValueTest, Parse) {
    const std::string json_str = "{\"a\": 1}";

    JsonValue json_value;
    Status st = JsonValue::parse(json_str, &json_value);
    ASSERT_TRUE(st.ok());

    auto json = json_value.to_string();
    ASSERT_TRUE(json.ok());
    ASSERT_EQ(json_str, json.value());

    auto parsed = JsonValue::parse(json_str);
    ASSERT_TRUE(parsed.ok());
    ASSERT_EQ(json_str, parsed.value().to_string().value());

    Slice oversized;
    oversized.data = const_cast<char*>(json_str.data());
    oversized.size = kJSONLengthLimit + 1;
    auto oversized_json = JsonValue::parse_json_or_string(oversized);
    ASSERT_FALSE(oversized_json.ok());
}

TEST(JsonValueTest, ToString) {
    auto maybe_json = JsonValue::parse(R"( {"a": "a"} )");
    ASSERT_TRUE(maybe_json.ok());
    EXPECT_EQ(R"({"a": "a"})", maybe_json.value().to_string().value());

    maybe_json = JsonValue::parse(R"( {"a": "\"a\""} )");
    ASSERT_TRUE(maybe_json.ok());
    EXPECT_EQ(R"({"a": "\"a\""})", maybe_json.value().to_string().value());

    auto str_json = JsonValue::from_string("a");
    EXPECT_EQ(R"("a")", str_json.to_string().value());

    auto quoted = JsonValue::from_string("\"a\"");
    EXPECT_EQ(R"("\"a\"")", quoted.to_string().value());
}

TEST(JsonValueTest, Build) {
    JsonValue null_json = JsonValue::from_null();
    ASSERT_EQ(0, null_json.compare(JsonValue::from_null()));
    ASSERT_EQ(JsonType::JSON_NULL, null_json.get_type());
    ASSERT_TRUE(null_json.is_null());
    ASSERT_EQ("null", null_json.to_string().value());

    JsonValue int_json = JsonValue::from_int(1024);
    ASSERT_EQ(JsonType::JSON_NUMBER, int_json.get_type());
    ASSERT_EQ(1024, int_json.get_int().value());
    ASSERT_EQ("1024", int_json.to_string().value());

    JsonValue uint_json = JsonValue::from_uint((uint64_t)1024);
    ASSERT_EQ(JsonType::JSON_NUMBER, uint_json.get_type());
    ASSERT_EQ((uint64_t)1024, uint_json.get_uint().value());
    ASSERT_EQ("1024", uint_json.to_string().value());

    JsonValue double_json = JsonValue::from_double(1.23);
    ASSERT_EQ(JsonType::JSON_NUMBER, double_json.get_type());
    ASSERT_DOUBLE_EQ(1.23, double_json.get_double().value());
    ASSERT_EQ("1.23", double_json.to_string().value());

    JsonValue bool_json = JsonValue::from_bool(true);
    ASSERT_EQ(JsonType::JSON_BOOL, bool_json.get_type());
    ASSERT_EQ(true, bool_json.get_bool().value());
    ASSERT_EQ("true", bool_json.to_string().value());

    JsonValue str_json = JsonValue::from_string("hehe");
    ASSERT_EQ(JsonType::JSON_STRING, str_json.get_type());
    ASSERT_EQ("hehe", str_json.get_string().value());
    ASSERT_EQ("\"hehe\"", str_json.to_string().value());
}

TEST(JsonValueTest, Accessor) {
    JsonValue json = JsonValue::parse("{\"a\": 1}").value();

    Slice slice = json.get_slice();
    JsonValue from_slice(slice);
    ASSERT_EQ(0, json.compare(from_slice));

    JsonValue::VSlice vslice = json.to_vslice();
    JsonValue from_vslice(vslice);
    ASSERT_EQ(0, json.compare(from_vslice));
}

TEST(JsonValueTest, Compare) {
    std::vector<JsonValue> values;
    values.push_back(JsonValue::parse(R"({"a": false})").value());
    values.push_back(JsonValue::parse(R"({"a": true})").value());
    values.push_back(JsonValue::parse(R"({"a": {"b": 1}})").value());
    values.push_back(JsonValue::parse(R"({"a": {"b": 2}})").value());
    values.push_back(JsonValue::parse(R"({"a": "a"})").value());
    values.push_back(JsonValue::parse(R"({"a": "b"})").value());
    values.push_back(JsonValue::parse(R"({"a": 1.0})").value());
    values.push_back(JsonValue::parse(R"({"a": 2.0})").value());
    values.push_back(JsonValue::parse(R"({"a": 3})").value());
    values.push_back(JsonValue::parse(R"({"a": 4})").value());

    EXPECT_LT(values[0], values[1]);
    EXPECT_LT(values[2], values[3]);
    EXPECT_LT(values[4], values[5]);
    EXPECT_LT(values[6], values[7]);
    EXPECT_LT(values[8], values[9]);
}

TEST(JsonValueTest, CompareLargeIntegerArrays) {
    auto small = JsonValue::parse("[1]").value();
    auto large = JsonValue::parse("[9223372036854775807]").value();
    EXPECT_LT(small.compare(large), 0);

    auto neg_small = JsonValue::parse("[-1]").value();
    auto neg_large = JsonValue::parse("[-9223372036854775808]").value();
    EXPECT_GT(neg_small.compare(neg_large), 0);
}

TEST(JsonValueTest, Hash) {
    JsonValue x = JsonValue::parse(R"({"a": 1, "b": 2})").value();
    JsonValue y = JsonValue::parse(R"({"b": 2, "a": 1})").value();
    ASSERT_EQ(x.hash(), y.hash());
}

TEST(JsonValueTest, FmtFormat) {
    JsonValue json = JsonValue::parse("1").value();
    std::string str = fmt::format("{}", json);
    ASSERT_EQ("\"1\"", str);
}

class JsonConvertTestFixture : public ::testing::TestWithParam<std::tuple<std::string>> {};

TEST_P(JsonConvertTestFixture, ConvertFromSimdjson) {
    using namespace simdjson;

    const std::string input = std::get<0>(GetParam());
    ondemand::parser parser;
    padded_string json_str(input);
    ondemand::document doc = parser.iterate(json_str);
    ondemand::object obj = doc.get_object();

    auto maybe_json = JsonValue::from_simdjson(&obj);
    ASSERT_TRUE(maybe_json.ok()) << maybe_json.status().code_as_string() << ": " << maybe_json.status().message();
    ASSERT_EQ(input, maybe_json.value().to_string_uncheck());
}

INSTANTIATE_TEST_SUITE_P(JsonConvertTest, JsonConvertTestFixture,
                         ::testing::Values(std::make_tuple(R"({"a": 1})"), std::make_tuple(R"({"a": null})"),
                                           std::make_tuple(R"({"a": ""})"), std::make_tuple(R"({"a": "x"})"),
                                           std::make_tuple(R"({"a": [1, 2, 3]})"),
                                           std::make_tuple(R"({"a": {"b": 1}})"),
                                           std::make_tuple(R"({"a": 18446744073709551615})"),
                                           std::make_tuple(R"({"a": {"": ""}})"), std::make_tuple(R"({"a": []})")));

TEST(JsonValueTest, ConvertFromSimdjsonBigInteger) {
    using namespace simdjson;
    ondemand::parser parser;

    auto big_integer_str = R"({"a": 10000000000000000000000000000000000000000})"_padded;
    ondemand::document big_integer_doc = parser.iterate(big_integer_str);
    ondemand::object big_integer_obj = big_integer_doc.get_object();
    auto big_integer_json = JsonValue::from_simdjson(&big_integer_obj);
    ASSERT_TRUE(big_integer_json.ok());

    auto double_str = R"({"a": 10000000000000000000000000000000000000000.0})"_padded;
    ondemand::document double_doc = parser.iterate(double_str);
    ondemand::object double_obj = double_doc.get_object();
    auto double_json = JsonValue::from_simdjson(&double_obj);
    ASSERT_TRUE(double_json.ok());

    ASSERT_EQ(double_json.value().to_string_uncheck(), big_integer_json.value().to_string_uncheck());

    padded_string double_overflow_str = strings::Substitute("{\"a\":$0}", std::string(400, '1'));
    ondemand::document double_overflow_doc = parser.iterate(double_overflow_str);
    ondemand::object double_overflow_obj = double_overflow_doc.get_object();
    auto double_overflow_json = JsonValue::from_simdjson(&double_overflow_obj);
    ASSERT_FALSE(double_overflow_json.ok());
}

namespace {
// Build an EXACT-size buffer (new char[capacity], so ASAN's redzone sits precisely at the end)
// holding `json` at the front, with every byte set to a non-NUL, non-whitespace filler and NO
// trailing NUL. This mirrors the stream-load path, where the JSON lives in a fixed-size buffer
// allocated without simdjson padding, so the bytes after the value are not NUL. A strlen on an
// unbounded raw pointer would read past the end of such a buffer; ASAN turns that read into a test
// failure. std::vector is intentionally avoided: it may over-allocate, leaving an uninitialized
// tail that could hold an early NUL and mask the overflow.
std::unique_ptr<char[]> make_unterminated_buffer(const std::string& json, size_t capacity) {
    auto buf = std::make_unique<char[]>(capacity);
    std::memset(buf.get(), 'A', capacity);
    std::memcpy(buf.get(), json.data(), json.size());
    return buf;
}
} // namespace

// A JSON string whose escape sequence is invalid: it is structurally well-formed (so parsing and
// field lookup succeed) but throws simdjson_error when the value is materialized during conversion,
// which drives the DataQualityError path that constructs the error message from the raw JSON.
TEST(JsonValueTest, ConvertFromSimdjsonErrorValueBounded) {
    using namespace simdjson;
    const std::string json = R"({"a": "\p"})";
    const size_t capacity = json.size() + SIMDJSON_PADDING + 32;
    auto buf = make_unterminated_buffer(json, capacity);

    ondemand::parser parser;
    ondemand::document doc = parser.iterate(padded_string_view(buf.get(), json.size(), capacity));
    ondemand::value val = doc.find_field("a");

    auto maybe_json = JsonValue::from_simdjson(&val);
    ASSERT_FALSE(maybe_json.ok());
    ASSERT_TRUE(maybe_json.status().is_data_quality_error());
}

TEST(JsonValueTest, ConvertFromSimdjsonErrorObjectBounded) {
    using namespace simdjson;
    const std::string json = R"({"a": "\p"})";
    const size_t capacity = json.size() + SIMDJSON_PADDING + 32;
    auto buf = make_unterminated_buffer(json, capacity);

    ondemand::parser parser;
    ondemand::document doc = parser.iterate(padded_string_view(buf.get(), json.size(), capacity));
    ondemand::object obj = doc.get_object();

    auto maybe_json = JsonValue::from_simdjson(&obj);
    ASSERT_FALSE(maybe_json.ok());
    ASSERT_TRUE(maybe_json.status().is_data_quality_error());
}

// A MALFORMED document that simdjson's lazy stage-1 parse still accepts: the ROOT object is closed
// (so doc.get_object() succeeds) but it contains an unclosed nested array. Conversion throws while
// materializing the invalid-escape string, and then object::raw_json() must consume() the broken
// structure, hits the unmatched array, and returns an error. The fix must not let that error throw a
// second exception out of the catch: assert nothing escapes and a DataQualityError is returned. This
// exercises the object overload's raw_json() error-guard branch. (A missing ROOT brace instead would
// make doc.get_object() throw INCOMPLETE_ARRAY_OR_OBJECT before conversion runs, never reaching it.)
TEST(JsonValueTest, ConvertFromSimdjsonErrorMalformedObjectBounded) {
    using namespace simdjson;
    const std::string json = R"({"a":["\p"})"; // JSON bytes: {"a":["\p"} — nested array left unclosed
    const size_t capacity = json.size() + SIMDJSON_PADDING + 32;
    auto buf = make_unterminated_buffer(json, capacity);

    ondemand::parser parser;
    ondemand::document doc = parser.iterate(padded_string_view(buf.get(), json.size(), capacity));
    ondemand::object obj = doc.get_object();

    auto maybe_json = JsonValue::from_simdjson(&obj);
    ASSERT_FALSE(maybe_json.ok());
    ASSERT_TRUE(maybe_json.status().is_data_quality_error());
}

} // namespace starrocks
