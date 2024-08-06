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

#include <glog/logging.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <velocypack/vpack.h>

#include <string>
#include <vector>

#include "butil/time.h"
#include "column/const_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/json_functions.h"
#include "exprs/mock_vectorized_expr.h"
#include "gtest/gtest-param-test.h"
#include "gutil/casts.h"
#include "gutil/strings/strip.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/defer_op.h"
#include "util/json.h"
#include "util/json_flattener.h"

namespace starrocks {

class FlatJsonQueryTestFixture2
        : public ::testing::TestWithParam<std::tuple<std::string, std::vector<std::string>, std::vector<LogicalType>,
                                                     std::string, std::string>> {};

TEST_P(FlatJsonQueryTestFixture2, flat_json_query) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::vector<std::string> param_flat_path = std::get<1>(GetParam());
    std::vector<LogicalType> param_flat_type = std::get<2>(GetParam());
    std::string param_path = std::get<3>(GetParam());
    std::string param_result = std::get<4>(GetParam());

    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(param_json, &json).ok());
    json_col->append(&json);
    if (param_path == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_path);
    }

    auto flat_json = JsonColumn::create();
    auto flat_json_ptr = flat_json.get();

    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns{flat_json, builder.build(true)};

    ctx.get()->set_constant_columns(columns);
    std::ignore =
            JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    ColumnPtr result = JsonFunctions::json_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    StripWhiteSpace(&param_result);
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_FALSE(datum.is_null());
        auto st = datum.get_json()->to_string();
        ASSERT_TRUE(st.ok()) << st->c_str();
        std::string json_result = datum.get_json()->to_string().value();
        StripWhiteSpace(&json_result);
        ASSERT_EQ(param_result, json_result);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(FlatJsonQueryTest, FlatJsonQueryTestFixture2,
    ::testing::Values(
        // empty
        std::make_tuple(R"( {"k1":1} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_BIGINT},  "NULL", R"(NULL)"),

        // various types
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_VARCHAR, TYPE_JSON}, "$.k2", R"( "hehe" )"),
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, std::vector<LogicalType> {TYPE_JSON, TYPE_JSON, TYPE_JSON}, "$.k3", R"( [1] )"),
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k2", "k4"}, std::vector<LogicalType> {TYPE_JSON, TYPE_JSON, TYPE_BIGINT},"$.k4", R"( NULL )"),
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k2", "k5"},  std::vector<LogicalType> {TYPE_BIGINT, TYPE_VARCHAR, TYPE_JSON},"$.k5", R"( NULL )"),

        // simple syntax
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_VARCHAR, TYPE_JSON}, "k2", R"( "hehe" )"),
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, std::vector<LogicalType> {TYPE_JSON, TYPE_JSON, TYPE_JSON}, "k3", R"( [1] )"),
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k2", "k4"}, std::vector<LogicalType> {TYPE_JSON, TYPE_JSON, TYPE_BIGINT},"k4", R"( NULL )"),
        std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k2", "k5"},  std::vector<LogicalType> {TYPE_BIGINT, TYPE_VARCHAR, TYPE_JSON},"k5", R"( NULL )"),

        // nested array
        std::make_tuple(R"( {"k1": [1,2,3]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[0]", R"( 1 )"),
        std::make_tuple(R"( {"k1": [1,2,3]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[3]", R"( NULL )"),
        std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[0][0]", R"( 1 )"),
        std::make_tuple(R"( {"k1": [[[1,2,3]]]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[0][0][0]", R"( 1 )"),
        std::make_tuple(R"( {"k1": [{"k2": [[1, 2], [3, 4]] }] } )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[0].k2[0][0]", R"( 1 )"),

        // nested object
        std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1", R"( {"k2": {"k3": 1}} )"),
        std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1.k2", R"( {"k3": 1} )"),
        std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1.k2.k3.k4", R"( NULL )"),
        std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "k1", R"( {"k2": {"k3": 1}} )"),
        std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "k1.k2.k3", R"( 1 )"),

        // nested object in array
        std::make_tuple(R"( {"k1": [{"k2": 1}]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[0]", R"( {"k2": 1} )"),
        std::make_tuple(R"( {"k1": [{"k2": 1}]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[0].k2", R"( 1 )"),
        std::make_tuple(R"( {"k1": [{"k2": 1}]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[0].k3", R"( NULL )"),

        // array result
        std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[*].k2", R"( [1, 2] )"),
        std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", std::vector<std::string>{"k1"}, std::vector<LogicalType> {TYPE_JSON}, "$.k1[*]", R"( [{"k2": 1}, {"k2": 2}] )")
        ));
// clang-format on

class FlatJsonQueryErrorTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::vector<std::string>, std::string>> {};

TEST_P(FlatJsonQueryErrorTestFixture, json_query) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::vector<std::string> param_flat_path = std::get<1>(GetParam());

    std::string param_path = std::get<2>(GetParam());

    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(param_json, &json).ok());
    json_col->append(&json);
    if (param_path == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_path);
    }

    auto flat_json = JsonColumn::create();
    auto flat_json_ptr = flat_json.get();

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }

    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns{flat_json, builder.build(true)};

    ctx.get()->set_constant_columns(columns);
    std::ignore =
            JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    auto result = JsonFunctions::json_query(ctx.get(), columns);
    ASSERT_FALSE(result.ok());

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

INSTANTIATE_TEST_SUITE_P(
        FlatJsonQueryTest, FlatJsonQueryErrorTestFixture,
        ::testing::Values(
                // clang-format off
                std::make_tuple(R"( {"k1":1} )", std::vector<std::string>{"k1"}, ""),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[2]]]]]"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[[[[[2]")
                // clang-format on
                ));

class FlatJsonExistsTestFixture2
        : public ::testing::TestWithParam<
                  std::tuple<std::string, std::vector<std::string>, std::vector<LogicalType>, std::string, bool>> {};

TEST_P(FlatJsonExistsTestFixture2, flat_json_exists_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::vector<std::string> param_flat_path = std::get<1>(GetParam());
    std::vector<LogicalType> param_flat_type = std::get<2>(GetParam());
    std::string param_path = std::get<3>(GetParam());
    bool param_exist = std::get<4>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.push_back(flat_json);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.push_back(path_column);
    }

    ctx.get()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    if (!st.ok()) {
        ASSERT_FALSE(param_exist);
        return;
    }

    ASSIGN_OR_ABORT(ColumnPtr result, JsonFunctions::json_exists(ctx.get(), columns))
    ASSERT_TRUE(!!result);

    if (param_exist) {
        ASSERT_TRUE((bool)result->get(0).get_uint8());
    } else {
        ASSERT_TRUE(result->get(0).is_null() || !(bool)result->get(0).get_uint8());
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(FlatJsonExistsTest, FlatJsonExistsTestFixture2,
    ::testing::Values(
        std::make_tuple(R"({ "k1":1, "k2":"2"})", std::vector<std::string>{"k1", "k2"}, std::vector<LogicalType>{TYPE_BIGINT, TYPE_BIGINT}, "$.k1", true),
        std::make_tuple(R"({ "k1":1, "k2":"2"})", std::vector<std::string>{"k1", "k2"}, std::vector<LogicalType>{TYPE_BIGINT, TYPE_VARCHAR}, "$.k2", true),
        std::make_tuple(R"({ "k1":1, "k2":"2"})", std::vector<std::string>{"k1", "k2"}, std::vector<LogicalType>{TYPE_BIGINT, TYPE_JSON}, "$.k2", true),
        std::make_tuple(R"({ "k1": [1,2,3]})", std::vector<std::string>({"k1", "k2"}), std::vector<LogicalType>{TYPE_JSON, TYPE_JSON}, "$.k1", true),
        std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1.k2.k3", true),
        std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1[0].k2", true),
        std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1[*].k2", true),
        std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1[0:2].k2", true),
        std::make_tuple(R"({ })", std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_VARCHAR}, "$.k1", false),
        std::make_tuple(R"({"k1": 1})", std::vector<std::string>({"k2"}), std::vector<LogicalType>{TYPE_VARCHAR}, "$.k2", false),
        std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1.k2.k3.k4", false),
        std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1[0].k3", false),
        //  nested array
        std::make_tuple(R"({"k1": [[1]]})",std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1[0][1]", false),
        std::make_tuple(R"({"k1": [[1]]})",std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON}, "$.k1[0][0]", true),
        // special case
        std::make_tuple(R"([{"k1": 1}, {"k2": 2}])",std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_JSON},  "$.k1[1]", false),
        std::make_tuple(R"("k1")",std::vector<std::string>({"k1"}), std::vector<LogicalType>{TYPE_BIGINT}, "$.k1", false)
));
// clang-format on

class FlatJsonLengthTestFixture2
        : public ::testing::TestWithParam<
                  std::tuple<std::string, std::vector<std::string>, std::vector<LogicalType>, std::string, int>> {};

TEST_P(FlatJsonLengthTestFixture2, flat_json_length_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::vector<std::string> param_flat_path = std::get<1>(GetParam());
    std::vector<LogicalType> param_flat_type = std::get<2>(GetParam());
    std::string param_path = std::get<3>(GetParam());
    int expect_length = std::get<4>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.push_back(flat_json);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.push_back(path_column);
    }

    // ctx.get()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    ASSERT_OK(st);

    ASSIGN_OR_ABORT(ColumnPtr result, JsonFunctions::json_length(ctx.get(), columns));
    ASSERT_TRUE(!!result);
    EXPECT_EQ(expect_length, result->get(0).get_int32());

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(FlatJsonLengthTest, FlatJsonLengthTestFixture2,
    ::testing::Values(
        std::make_tuple(R"({ "k1":1, "k2": {} })", std::vector<std::string>({"k1", "k2"}), std::vector<LogicalType> {TYPE_JSON, TYPE_JSON}, "$.k2", 0), 
        std::make_tuple(R"({ "k1":1, "k2": [1,2] })", std::vector<std::string>({"k1", "k2"}), std::vector<LogicalType> {TYPE_JSON, TYPE_JSON}, "$.k2", 2), 
        std::make_tuple(R"({ "k1":1, "k2": [1,2] })", std::vector<std::string>({"k1", "k2", "k3"}), std::vector<LogicalType> {TYPE_JSON, TYPE_JSON, TYPE_JSON}, "$.k3", 0),
        std::make_tuple(R"({ "k1":1, "k2": {"xx": 1} })", std::vector<std::string>{"k1", "k2"}, std::vector<LogicalType> {TYPE_JSON, TYPE_JSON}, "$.k1", 1)
));
// clang-format on

class FlatJsonKeysTestFixture2
        : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::vector<std::string>,
                                                     std::vector<LogicalType>, std::string>> {};

TEST_P(FlatJsonKeysTestFixture2, json_keys) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    std::vector<std::string> param_flat_path = std::get<2>(GetParam());
    std::vector<LogicalType> param_flat_type = std::get<3>(GetParam());
    std::string param_result = std::get<4>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_column->append(&*json);

    if (param_path == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_path);
    }

    auto flat_json = JsonColumn::create();
    auto flat_json_ptr = flat_json.get();

    Columns columns{flat_json, builder.build(true)};

    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_column.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    ASSERT_OK(st);

    ColumnPtr result = JsonFunctions::json_keys(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    if (param_result == "NULL") {
        EXPECT_TRUE(result->is_null(0));
    } else {
        const JsonValue* keys = result->get(0).get_json();
        std::string keys_str = keys->to_string_uncheck();
        EXPECT_EQ(param_result, keys_str);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(JsonKeysTest, FlatJsonKeysTestFixture2,
    ::testing::Values(
        std::make_tuple(R"({ "k1": 1, "k2": 2 })", "NULL", std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_JSON, TYPE_JSON}, R"(NULL)"),
        std::make_tuple(R"({ "k1": "v1" })",  "$.k1", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}, R"(NULL)"),
        std::make_tuple(R"({ "k1": "v1" })",  "$.k3",std::vector<std::string> {"k1", "k3"}, std::vector<LogicalType> {TYPE_JSON, TYPE_JSON}, R"(NULL)"),
        std::make_tuple(R"({ "k1": {"k2": 1} })",  "$.k1",std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}, R"(["k2"])")
));
// clang-format on

// 0: JSON Input
// 1. Path
// 2. Flat Path
// 3. Flat Type
// 4. get_json_bool execpted result
// 5. get_json_int execpted result
// 6. get_json_string expected result
// 7. get_json_double expected result
using GetJsonXXXParam = std::tuple<std::string, std::string, std::vector<std::string>, std::vector<LogicalType>, int,
                                   int, std::string, double>;

class FlatGetJsonXXXTestFixture2 : public ::testing::TestWithParam<GetJsonXXXParam> {
public:
    StatusOr<Columns> setup() {
        _ctx = std::unique_ptr<FunctionContext>(FunctionContext::create_test_context());
        auto ints = JsonColumn::create();
        ColumnBuilder<TYPE_VARCHAR> builder(1);

        std::string param_json = std::get<0>(GetParam());
        std::string param_path = std::get<1>(GetParam());
        std::vector<std::string> flat_path = std::get<2>(GetParam());
        std::vector<LogicalType> flat_type = std::get<3>(GetParam());

        JsonValue json;
        Status st = JsonValue::parse(param_json, &json);
        if (!st.ok()) {
            return st;
        }
        ints->append(&json);
        if (param_path == "NULL") {
            builder.append_null();
        } else {
            builder.append(param_path);
        }

        auto flat_json = JsonColumn::create();
        auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

        JsonFlattener jf(flat_path, flat_type, false);
        jf.flatten(ints.get());
        flat_json_ptr->set_flat_columns(flat_path, flat_type, jf.mutable_result());

        Columns columns{flat_json, builder.build(true)};

        _ctx->set_constant_columns(columns);
        std::ignore = JsonFunctions::native_json_path_prepare(_ctx.get(),
                                                              FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        return columns;
    }

    void tear_down() {
        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            _ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

public:
    std::unique_ptr<FunctionContext> _ctx;
};

TEST_P(FlatGetJsonXXXTestFixture2, get_json_bool) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    int expected = std::get<4>(GetParam());

    ColumnPtr result = JsonFunctions::get_native_json_bool(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    ASSERT_EQ(1, result->size());
    Datum datum = result->get(0);
    if (expected == -1) {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        int64_t value = datum.get_uint8();
        ASSERT_EQ(expected, value);
    }
}

TEST_P(FlatGetJsonXXXTestFixture2, get_json_int) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    int expected = std::get<5>(GetParam());

    ColumnPtr result = JsonFunctions::get_native_json_bigint(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    ASSERT_EQ(1, result->size());
    Datum datum = result->get(0);
    if (expected == -1) {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_FALSE(datum.is_null());
        int64_t value = datum.get_int64();
        ASSERT_EQ(expected, value);
    }
}

TEST_P(FlatGetJsonXXXTestFixture2, get_json_string) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    std::string param_result = std::get<6>(GetParam());
    StripWhiteSpace(&param_result);

    ColumnPtr result = JsonFunctions::get_native_json_string(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    ASSERT_EQ(1, result->size());
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto value = datum.get_slice();
        std::string value_str(value);
        ASSERT_EQ(param_result, value_str);
    }
}

TEST_P(FlatGetJsonXXXTestFixture2, get_json_double) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    double expected = std::get<7>(GetParam());

    ColumnPtr result = JsonFunctions::get_native_json_double(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());

    Datum datum = result->get(0);
    if (expected == -1) {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        double value = datum.get_double();
        ASSERT_EQ(expected, value);
    }
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(GetJsonXXXTest, FlatGetJsonXXXTestFixture2,
    ::testing::Values(
        std::make_tuple(R"( {"k1":1} )", "NULL", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, -1, -1, "NULL", -1),
        std::make_tuple(R"( {"k0": null} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, -1, -1, "NULL", -1),
        std::make_tuple(R"( {"k1": 1} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, 1, 1, "1", 1.0),
        std::make_tuple(R"( {"k1": -10} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_VARCHAR}, 1, -10, R"( -10 )", -10),
        std::make_tuple(R"( {"k1": 1.1} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_JSON}, 1, 1, R"( 1.1 )", 1.1),
        std::make_tuple(R"( {"k1": 3.14} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, 1, 3, R"( 3 )", 3.0),
        std::make_tuple(R"( {"k1": 3.14} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_DOUBLE}, 1, 3, R"( 3.14 )", 3.14),
        std::make_tuple(R"( {"k1": 0.14} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, 0, 0, R"( 0 )", 0.0),
        std::make_tuple(R"( {"k1": 0.14} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_DOUBLE}, 1, 0, R"( 0.14 )", 0.14),
        std::make_tuple(R"( {"k1": null} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_JSON}, -1, -1, R"( NULL )", -1),
        std::make_tuple(R"( {"k1": "value" } )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, -1, -1, R"( NULL )", -1),
        std::make_tuple(R"( {"k1": {"k2": 1}} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_JSON}, -1, -1, R"( {"k2": 1} )", -1),
        std::make_tuple(R"( {"k1": [1,2,3] } )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_JSON}, -1, -1, R"( [1, 2, 3] )", -1),
        std::make_tuple(R"( {"k1": true} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, 1, 1, "1", 1.0),
        std::make_tuple(R"( {"k1": false} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_BIGINT}, 0, 0, "0", 0.0),
        std::make_tuple(R"( {"k1": true} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_JSON}, 1, 1, "true", 1.0),
        std::make_tuple(R"( {"k1": false} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_JSON}, 0, 0, "false", 0.0),
        std::make_tuple(R"( {"k1": true} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_VARCHAR}, 1, -1, "true", -1),
        std::make_tuple(R"( {"k1": false} )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_VARCHAR}, 0, -1, "false", -1),
        std::make_tuple(R"( {"k1": "value" } )", "$.k1", std::vector<std::string> { "k1"}, std::vector<LogicalType> {TYPE_VARCHAR}, -1, -1, R"( value )", -1)
    ));
// clang-format on

class FlatJsonDeriverPaths
        : public ::testing::TestWithParam<
                  std::tuple<std::string, std::string, std::vector<std::string>, std::vector<LogicalType>>> {};

TEST_P(FlatJsonDeriverPaths, flat_json_path_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json1 = std::get<0>(GetParam());
    std::string param_json2 = std::get<1>(GetParam());
    std::vector<std::string> param_flat_path = std::get<2>(GetParam());
    std::vector<LogicalType> param_flat_type = std::get<3>(GetParam());

    auto json = JsonValue::parse(param_json1);
    ASSERT_TRUE(json.ok());
    json_column->append(&*json);

    auto json2 = JsonValue::parse(param_json2);
    ASSERT_TRUE(json2.ok());
    json_column->append(&*json2);

    std::vector<const Column*> columns{json_column.get()};
    JsonPathDeriver jf;
    jf.derived(columns);
    std::vector<std::string> path = jf.flat_paths();
    std::vector<LogicalType> type = jf.flat_types();

    ASSERT_EQ(param_flat_path, path);
    ASSERT_EQ(param_flat_type, type);
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(FlatJsonPathDeriver, FlatJsonDeriverPaths,
    ::testing::Values(
        std::make_tuple(R"({ "k1": 1, "k2": 2 })", R"({ "k1": 3, "k2": 4 })", std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_BIGINT}),
        std::make_tuple(R"({ "k1": "v1" })",  R"({ "k1": "v33" })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_VARCHAR}),
        std::make_tuple(R"({ "k1": {"k2": 1} })",  R"({ "k1": 123 })", std::vector<std::string> {}, std::vector<LogicalType> {}),
        std::make_tuple(R"({ "k1": "v1" })",  R"({ "k1": 1.123 })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}),
        std::make_tuple(R"({ "k1": {"k2": 1} })", R"({ "k1": 1.123 })", std::vector<std::string> {}, std::vector<LogicalType> {}),
        std::make_tuple(R"({ "k1": [1,2,3] })", R"({ "k1": "v33" })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}),

        std::make_tuple(R"({ "k1": "v1", "k2": [3,4,5], "k3": 1, "k4": 1.2344 })",  
                        R"({ "k1": "abc", "k2": [11,123,54], "k3": 23423, "k4": 1.2344 })",
                        std::vector<std::string> {"k1", "k2", "k3", "k4"}, 
                        std::vector<LogicalType> {TYPE_VARCHAR, TYPE_JSON, TYPE_BIGINT, TYPE_DOUBLE}),
        std::make_tuple(R"({ "k1": 1, "k2": "a" })", R"({ "k1": 3, "k2": null })", std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_JSON}),
        std::make_tuple(R"({ "k1": 1, "k2": 2 })", R"({ "k1": 3, "k2": 4 })", std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_BIGINT}),

        std::make_tuple(R"({ "k1": "v1", "k1": "v2", "k1": "v3", "k4": 1.2344 })",  
                        R"({ "k1": "v1", "k2": "v1", "k3": "v1", "k4": 1.2344 })",  
                        std::vector<std::string> {"k4"}, 
                        std::vector<LogicalType> {TYPE_DOUBLE})

));
// clang-format on

} // namespace starrocks
