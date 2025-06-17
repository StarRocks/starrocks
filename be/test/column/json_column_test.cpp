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

#include "column/json_column.h"

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <gutil/strings/substitute.h>

#include <vector>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "util/json.h"

namespace starrocks {

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, test_parse) {
    std::string json_str = "{\"a\": 1}";
    {
        JsonValue json_value;
        Status s = JsonValue::parse(json_str, &json_value);
        ASSERT_TRUE(s.ok());

        auto json = json_value.to_string();
        ASSERT_TRUE(json.ok());
        ASSERT_EQ(json_str, json.value());
    }
    {
        auto json = JsonValue::parse(json_str);
        ASSERT_TRUE(json.ok());
        ASSERT_TRUE(json.value().to_string().ok());
        ASSERT_EQ(json_str, json.value().to_string().value());
    }
    {
        // Exceed length limitation
        Slice slice;
        slice.data = json_str.data();
        slice.size = kJSONLengthLimit + 1;
        auto maybe_json = JsonValue::parse_json_or_string(slice);
        ASSERT_FALSE(maybe_json.ok());
    }
}

PARALLEL_TEST(JsonColumnTest, test_to_string) {
    {
        // normal json with string
        auto maybe_json = JsonValue::parse(R"( {"a": "a"} )");
        ASSERT_TRUE(maybe_json.ok());
        EXPECT_EQ(R"({"a": "a"})", maybe_json.value().to_string().value());
    }

    {
        // value string with quote
        auto maybe_json = JsonValue::parse(R"( {"a": "\"a\""} )");
        ASSERT_TRUE(maybe_json.ok());
        EXPECT_EQ(R"({"a": "\"a\""})", maybe_json.value().to_string().value());
    }

    {
        // string type
        auto maybe_json = JsonValue::from_string("a");
        EXPECT_EQ(R"("a")", maybe_json.to_string().value());
    }
    {
        // string type with quote
        auto maybe_json = JsonValue::from_string("\"a\"");
        EXPECT_EQ(R"("\"a\"")", maybe_json.to_string().value());
    }
}

PARALLEL_TEST(JsonColumnTest, test_build) {
    // null
    {
        JsonValue json = JsonValue::from_null();
        ASSERT_EQ(0, json.compare(JsonValue::from_null()));
        ASSERT_EQ(JsonType::JSON_NULL, json.get_type());
        ASSERT_TRUE(json.is_null());
        ASSERT_EQ("null", json.to_string().value());
    }
    // int
    {
        JsonValue json = JsonValue::from_int(1024);
        ASSERT_EQ(JsonType::JSON_NUMBER, json.get_type());
        ASSERT_EQ(1024, json.get_int().value());
        ASSERT_EQ("1024", json.to_string().value());
    }
    // uint
    {
        JsonValue json = JsonValue::from_uint((uint64_t)1024);
        ASSERT_EQ(JsonType::JSON_NUMBER, json.get_type());
        ASSERT_EQ((uint64_t)1024, json.get_uint().value());
        ASSERT_EQ("1024", json.to_string().value());
    }

    // double
    {
        JsonValue json = JsonValue::from_double(1.23);
        ASSERT_EQ(JsonType::JSON_NUMBER, json.get_type());
        ASSERT_DOUBLE_EQ(1.23, json.get_double().value());
        ASSERT_EQ("1.23", json.to_string().value());
    }
    // boolean
    {
        JsonValue json = JsonValue::from_bool(true);
        ASSERT_EQ(JsonType::JSON_BOOL, json.get_type());
        ASSERT_EQ(true, json.get_bool().value());
        ASSERT_EQ("true", json.to_string().value());
    }
    // string
    {
        JsonValue json = JsonValue::from_string("hehe");
        ASSERT_EQ(JsonType::JSON_STRING, json.get_type());
        ASSERT_EQ("hehe", json.get_string().value());
        ASSERT_EQ("\"hehe\"", json.to_string().value());
    }
    // object
    {
        JsonValue json = JsonValue::parse("{\"a\": 1}").value();
        ASSERT_EQ(JsonType::JSON_OBJECT, json.get_type());
        ASSERT_EQ("{\"a\": 1}", json.to_string().value());
        ASSERT_EQ("{\"a\": 1}", json.to_string().value());
    }
}

PARALLEL_TEST(JsonColumnTest, test_accessor) {
    JsonValue json = JsonValue::parse("{\"a\": 1}").value();
    Slice slice = json.get_slice();
    JsonValue::VSlice vslice = json.to_vslice();

    // deserialize json from slice
    {
        JsonValue rhs(slice);
        Slice rhs_slice = rhs.get_slice();
        ASSERT_EQ(0, json.compare(rhs));
        ASSERT_STREQ(slice.get_data(), rhs_slice.get_data());
    }

    // deserialize json from vslice
    {
        JsonValue rhs(vslice);
        ASSERT_EQ(0, json.compare(rhs));
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, test_compare) {
    std::vector<JsonValue> column;

    // bool
    column.push_back(JsonValue::parse(R"({"a": false})").value());
    column.push_back(JsonValue::parse(R"({"a": true})").value());
    // object
    column.push_back(JsonValue::parse(R"({"a": {"b": 1}})").value());
    column.push_back(JsonValue::parse(R"({"a": {"b": 2}})").value());
    // string
    column.push_back(JsonValue::parse(R"({"a": "a"})").value());
    column.push_back(JsonValue::parse(R"({"a": "b"})").value());
    // double
    column.push_back(JsonValue::parse(R"({"a": 1.0})").value());
    column.push_back(JsonValue::parse(R"({"a": 2.0})").value());
    // small int
    column.push_back(JsonValue::parse(R"({"a": 3})").value());
    column.push_back(JsonValue::parse(R"({"a": 4})").value());
    // int
    column.push_back(JsonValue::parse(R"({"a": 3046})").value());
    column.push_back(JsonValue::parse(R"({"a": 4048})").value());

    // same type
    std::vector<std::pair<int, int>> same_type_cases = {
            {0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11},
    };
    for (auto p : same_type_cases) {
        int lhs = p.first;
        int rhs = p.second;
        ASSERT_EQ(0, column[lhs].compare(column[lhs]));
        ASSERT_EQ(0, column[rhs].compare(column[rhs]));
        ASSERT_LT(column[lhs].compare(column[rhs]), 0);
        ASSERT_GT(column[rhs].compare(column[lhs]), 0);

        ASSERT_EQ(column[lhs], column[lhs]);
        ASSERT_EQ(column[rhs], column[rhs]);
        ASSERT_LT(column[lhs], column[rhs]);
        ASSERT_GT(column[rhs], column[lhs]);
    }

    // different type
    std::vector<std::pair<int, int>> diff_type_cases = {
            {0, 2},
            {2, 4},
            {6, 4},
    };
    for (auto p : diff_type_cases) {
        int lhs = p.first;
        int rhs = p.second;
        EXPECT_LT(column[lhs].compare(column[rhs]), 0);
        EXPECT_GT(column[rhs].compare(column[lhs]), 0);

        // operators
        EXPECT_LT(column[lhs], column[rhs]);
        EXPECT_GT(column[rhs], column[lhs]);
    }

    // numbers of different types
    for (int i = 6; i <= 11; i++) {
        for (int j = i + 1; j <= 11; j++) {
            EXPECT_LT(column[i], column[j]);
            EXPECT_GT(column[j], column[i]);
            EXPECT_NE(column[i], column[j]);
        }
    }
}

PARALLEL_TEST(JsonColumnTest, test_compare_array) {
    auto array0 = JsonValue::parse("[]").value();
    auto array1 = JsonValue::parse("[1]").value();
    auto array2 = JsonValue::parse("[1, 2]").value();
    EXPECT_EQ(0, array0.compare(array0));
    EXPECT_EQ(0, array1.compare(array1));
    EXPECT_EQ(0, array2.compare(array2));
    EXPECT_LT(array0.compare(array1), 0);
    EXPECT_LT(array0.compare(array2), 0);
    EXPECT_LT(array1.compare(array2), 0);
}

PARALLEL_TEST(JsonColumnTest, test_compare_object) {
    auto obj0 = JsonValue::parse("{}").value();
    auto obj1 = JsonValue::parse(R"( {"a": 1} )").value();
    auto obj2 = JsonValue::parse(R"( {"a": 1, "b": 2} )").value();
    EXPECT_EQ(0, obj0.compare(obj0));
    EXPECT_EQ(0, obj1.compare(obj1));
    EXPECT_EQ(0, obj2.compare(obj2));
    EXPECT_LT(obj0.compare(obj1), 0);
    EXPECT_LT(obj0.compare(obj2), 0);
    EXPECT_LT(obj1.compare(obj2), 0);
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, test_hash) {
    JsonValue x = JsonValue::parse(R"({"a": 1, "b": 2})").value();
    JsonValue y = JsonValue::parse(R"({"b": 2, "a": 1})").value();
    ASSERT_EQ(-3726198756236301983, x.hash());
    ASSERT_EQ(x.hash(), y.hash());
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, test_filter) {
    // TODO(mofei)
    const int N = 100;
    JsonColumn::Ptr json_column = JsonColumn::create();
    for (int i = 0; i < N; i++) {
        std::string json_str = strings::Substitute("{\"a\": $0}", i);
        json_column->append(JsonValue::parse(json_str).value());
    }

    Filter filter(N, 1);
    json_column->filter_range(filter, 0, N);
    ASSERT_EQ(N, json_column->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, put_mysql_buffer) {
    JsonColumn::Ptr json_column = JsonColumn::create();
    json_column->append(JsonValue::parse("{\"a\": 0}").value());

    MysqlRowBuffer rowBuffer;
    json_column->put_mysql_row_buffer(&rowBuffer, 0);

    ASSERT_EQ("\b{\"a\": 0}", rowBuffer.data());
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, test_fmt) {
    JsonValue json = JsonValue::parse("1").value();
    std::cerr << json;

    std::string str = fmt::format("{}", json);
    ASSERT_EQ("\"1\"", str);
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, test_column_builder) {
    // create from type traits
    {
        auto column = RunTimeColumnType<TYPE_JSON>::create();
        auto input = JsonValue::parse("1").value();
        column->append(&input);
        JsonValue* json = column->get_object(0);
        ASSERT_EQ(0, json->compare(input));
        ASSERT_EQ(0, json->compare(*column->get(0).get_json()));
    }
    // create from builder
    {
        ColumnBuilder<TYPE_JSON> builder(1);
        auto json = JsonValue::parse("1").value();
        builder.append(&json);
        auto result = builder.build(false);

        JsonColumn::Ptr json_column_ptr = ColumnHelper::cast_to<TYPE_JSON>(std::move(result));
        JsonColumn* json_column = json_column_ptr.get();
        ASSERT_EQ(1, json_column->size());
        ASSERT_EQ(0, json_column->get_object(0)->compare(json));
    }
    // clone
    {
        JsonColumn::Ptr column = JsonColumn::create();
        column->append(JsonValue::parse("1").value());

        {
            auto copy = column->clone();
            ASSERT_EQ(1, copy->size());
            ASSERT_EQ(0, copy->compare_at(0, 0, *column, 0));
        }

        // clone nullable by helper
        {
            TypeDescriptor desc = TypeDescriptor::create_json_type();
            auto copy = ColumnHelper::clone_column(desc, true, column, column->size());
            ASSERT_EQ(1, copy->size());
            ASSERT_EQ(0, copy->compare_at(0, 0, *column, 0));
            ASSERT_TRUE(copy->is_nullable());

            // unwrap nullable column
            Column* unwrapped = ColumnHelper::get_data_column(copy.get());

            auto* json_column_ptr = down_cast<JsonColumn*>(unwrapped);
            ASSERT_EQ(1, json_column_ptr->size());
            ASSERT_EQ(0, json_column_ptr->compare_at(0, 0, *column, 0));
        }

        // clone json_column by helper
        {
            TypeDescriptor desc = TypeDescriptor::create_json_type();
            ColumnPtr copy = ColumnHelper::clone_column(desc, false, column, column->size());
            ASSERT_EQ(1, copy->size());
            ASSERT_EQ(0, copy->compare_at(0, 0, *column, 0));
            ASSERT_FALSE(copy->is_nullable());

            JsonColumn::Ptr json_column_ptr = ColumnHelper::cast_to<TYPE_JSON>(copy);
            ASSERT_EQ(1, json_column_ptr->size());
            ASSERT_EQ(0, json_column_ptr->compare_at(0, 0, *column, 0));

            JsonColumn* json_column = ColumnHelper::cast_to_raw<TYPE_JSON>(copy.get());
            ASSERT_EQ(1, json_column->size());
            ASSERT_EQ(0, json_column->compare_at(0, 0, *column, 0));
        }
    }
}

PARALLEL_TEST(JsonColumnTest, test_assign) {
    auto column = RunTimeColumnType<TYPE_JSON>::create();
    column->append(JsonValue::parse("1").value());
    column->assign(10, 0);
    ASSERT_EQ(10, column->size());
    for (int i = 0; i < 10; i++) {
        const JsonValue* json = column->get_object(i);
        EXPECT_EQ(JsonValue::parse("1").value(), *json);
    }
    column->assign(20, 0);
    ASSERT_EQ(20, column->size());
    for (int i = 0; i < 20; i++) {
        const JsonValue* json = column->get_object(i);
        EXPECT_EQ(JsonValue::parse("1").value(), *json);
    }
}

PARALLEL_TEST(JsonColumnTest, test_serialize) {
    auto column = RunTimeColumnType<TYPE_JSON>::create();
    JsonValue json = JsonValue::parse("1").value();
    column->append(&json);

    EXPECT_EQ(json.serialize_size(), column->serialize_size(0));
    std::vector<uint8_t> buffer;
    buffer.resize(json.serialize_size());
    column->serialize(0, buffer.data());

    // deserialize
    auto new_column = column->clone_empty();
    new_column->deserialize_and_append(buffer.data());
    EXPECT_EQ(0, column->compare_at(0, 0, *new_column, 1));
}

class JsonConvertTestFixture : public ::testing::TestWithParam<std::tuple<std::string>> {
public:
};

TEST_P(JsonConvertTestFixture, convert_from_simdjson) {
    using namespace simdjson;
    std::string param_0 = std::get<0>(GetParam());
    ondemand::parser parser;
    padded_string json_str(param_0);
    ondemand::document doc = parser.iterate(json_str);
    ondemand::object obj = doc.get_object();
    auto maybe_json = JsonValue::from_simdjson(&obj);
    ASSERT_TRUE(maybe_json.ok());
    ASSERT_EQ(json_str.data(), maybe_json.value().to_string_uncheck());
}

INSTANTIATE_TEST_SUITE_P(JsonConvertTest, JsonConvertTestFixture,
                         ::testing::Values(
                                 // clang-format off
                                    std::make_tuple(R"({"a": 1})"),
                                    std::make_tuple(R"({"a": null})"),
                                    std::make_tuple(R"({"a": ""})"),
                                    std::make_tuple(R"({"a": [1, 2, 3]})"),
                                    std::make_tuple(R"({"a": {"b": 1}})"),
                                    // unsigned integer
                                    std::make_tuple(R"({"a": 18446744073709551615})"),

                                    // empty key
                                    std::make_tuple(R"({"a": {"": ""}})"),
                                    // empty array
                                    std::make_tuple(R"({"a": []})")

                                 // clang-format on
                                 ));

PARALLEL_TEST(JsonConvertTest, convert_from_simdjson_big_integer) {
    using namespace simdjson;
    ondemand::parser parser;

    // a is simdjson::ondemand::number_type::big_integer, and should be converted to double
    auto big_integer_str = R"({"a": 10000000000000000000000000000000000000000})"_padded;
    ondemand::document big_integer_doc = parser.iterate(big_integer_str);
    ondemand::object big_integer_obj = big_integer_doc.get_object();
    auto big_integer_json = JsonValue::from_simdjson(&big_integer_obj);
    ASSERT_TRUE(big_integer_json.ok());

    // a is simdjson::ondemand::number_type::floating_point_number
    auto double_str = R"({"a": 10000000000000000000000000000000000000000.0})"_padded;
    ondemand::document double_doc = parser.iterate(double_str);
    ondemand::object double_obj = double_doc.get_object();
    auto double_json = JsonValue::from_simdjson(&double_obj);
    ASSERT_TRUE(double_json.ok());

    ASSERT_EQ(double_json.value().to_string_uncheck(), big_integer_json.value().to_string_uncheck());

    // a is simdjson::ondemand::number_type::big_integer, but is overflow for double
    padded_string double_overflow_str = strings::Substitute("{\"a\":$0}", std::string(400, '1'));
    ondemand::document double_overflow_doc = parser.iterate(double_overflow_str);
    ondemand::object double_overflow_obj = double_overflow_doc.get_object();
    auto double_overflow_json = JsonValue::from_simdjson(&double_overflow_obj);
    ASSERT_FALSE(double_overflow_json.ok());
}

} // namespace starrocks
