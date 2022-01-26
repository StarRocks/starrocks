// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <gutil/strings/substitute.h>

#include <vector>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "testutil/parallel_test.h"
#include "util/json.h"

namespace starrocks::vectorized {

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
    // double
    column.push_back(JsonValue::parse(R"({"a": 1.0})").value());
    column.push_back(JsonValue::parse(R"({"a": 2.0})").value());
    // int
    column.push_back(JsonValue::parse(R"({"a": 0})").value());
    column.push_back(JsonValue::parse(R"({"a": 1})").value());
    // string
    column.push_back(JsonValue::parse(R"({"a": "a"})").value());
    column.push_back(JsonValue::parse(R"({"a": "b"})").value());

    // same type
    std::vector<std::pair<int, int>> same_type_cases = {
            {0, 1},
            {2, 3},
            {4, 5},
            {6, 7},
    };
    for (auto p : same_type_cases) {
        int lhs = p.first;
        int rhs = p.second;
        ASSERT_EQ(0, column[lhs].compare(column[lhs]));
        ASSERT_EQ(0, column[rhs].compare(column[rhs]));
        ASSERT_LT(column[lhs].compare(column[rhs]), 0);
        ASSERT_GT(column[rhs].compare(column[lhs]), 0);
    }

    // different type
    std::vector<std::pair<int, int>> diff_type_cases = {
            {0, 2},
            {2, 4},
            {4, 6},
    };
    for (auto p : diff_type_cases) {
        int lhs = p.first;
        int rhs = p.second;
        ASSERT_LT(column[lhs].compare(column[rhs]), 0);
        ASSERT_GT(column[rhs].compare(column[lhs]), 0);
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, test_filter) {
    // TODO(mofei)
    const int N = 100;
    auto json_column = JsonColumn::create();
    for (int i = 0; i < N; i++) {
        std::string json_str = strings::Substitute("{\"a\": $0}", i);
        json_column->append(JsonValue::parse(json_str).value());
    }

    Column::Filter filter(N, 1);
    json_column->filter_range(filter, 0, N);
    ASSERT_EQ(N, json_column->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(JsonColumnTest, put_mysql_buffer) {
    auto json_column = JsonColumn::create();
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
    ASSERT_EQ("1", str);
}

} // namespace starrocks::vectorized