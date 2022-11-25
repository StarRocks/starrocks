// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/json_functions.h"

#include <glog/logging.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <velocypack/vpack.h>

#include <string>

#include "butil/time.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "gtest/gtest-param-test.h"
#include "gutil/strings/strip.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/json.h"

namespace starrocks::vectorized {

class JsonFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

    Status test_extract_from_object(std::string input, const std::string& jsonpath, std::string* output) {
        // reverse for padding.
        input.reserve(input.size() + simdjson::SIMDJSON_PADDING);

        simdjson::ondemand::parser parser;
        simdjson::ondemand::document doc;
        EXPECT_EQ(simdjson::error_code::SUCCESS, parser.iterate(input).get(doc));

        simdjson::ondemand::object obj;

        EXPECT_EQ(simdjson::error_code::SUCCESS, doc.get_object().get(obj));

        std::vector<SimpleJsonPath> path;
        JsonFunctions::parse_json_paths(jsonpath, &path);

        simdjson::ondemand::value val;
        RETURN_IF_ERROR(JsonFunctions::extract_from_object(obj, path, &val));
        std::string_view sv = simdjson::to_json_string(val);

        output->assign(sv.data(), sv.size());
        return Status::OK();
    }

public:
    TExprNode expr_node;
};

TEST_F(JsonFunctionsTest, get_json_string_casting) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto strings = BinaryColumn::create();
    auto strings2 = BinaryColumn::create();

    std::string values[] = {
            R"({"k1":    1})",          //int, 1 key
            R"({"k1":    1, "k2": 2})", // 2 keys, get the former
            R"({"k0":    0, "k1": 1})", // 2 keys, get  the latter

            R"({"k1":    3.14159})",                 //double, 1 key
            R"({"k0":    2.71828, "k1":  3.14159})", // 2 keys, get  the former
            R"({"k1":    3.14159, "k2":  2.71828})", // 2 keys, get  the latter

            R"({"k1":    "{\"k11\":       \"v11\"}"})",                                        //string, 1 key
            R"({"k0":    "{\"k01\":       \"v01\"}",  "k1":     "{\"k11\":       \"v11\"}"})", // 2 keys, get  the former
            R"({"k1":    "{\"k11\":       \"v11\"}",  "k2":     "{\"k21\": \"v21\"}"})", // 2 keys, get  the latter

            R"({"k1":    {"k11":       "v11"}})",                             //object, 1 key
            R"({"k0":    {"k01":       "v01"},  "k1":     {"k11": "v11"}})",  // 2 keys, get  the former
            R"({"k1":    {"k11":       "v11"},  "k2":     {"k21": "v21"}})"}; // 2 keys, get  the latter

    std::string strs[] = {"$.k1", "$.k1", "$.k1", "$.k1", "$.k1", "$.k1",
                          "$.k1", "$.k1", "$.k1", "$.k1", "$.k1", "$.k1"};
    std::string length_strings[] = {"1",
                                    "1",
                                    "1",
                                    "3.14159",
                                    "3.14159",
                                    "3.14159",
                                    R"({"k11":       "v11"})",
                                    R"({"k11":       "v11"})",
                                    R"({"k11":       "v11"})",
                                    R"({"k11": "v11"})",
                                    R"({"k11": "v11"})",
                                    R"({"k11": "v11"})"};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        strings->append(values[j]);
        strings2->append(strs[j]);
    }

    columns.emplace_back(strings);
    columns.emplace_back(strings2);

    ctx.get()->impl()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_strings[j], v->get_data()[j].to_string());
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_string_array) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto strings = BinaryColumn::create();
    auto strings2 = BinaryColumn::create();

    std::string values[] = {R"([{"key":    1}, {"key": 2 }])", R"([{"key":    1}, {"key": 2 }])"};

    std::string strs[] = {"$[*].key", "$.[*].key"};
    std::string length_strings[] = {"[1, 2]", "[1, 2]"};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        strings->append(values[j]);
        strings2->append(strs[j]);
    }

    columns.emplace_back(strings);
    columns.emplace_back(strings2);

    ctx.get()->impl()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_strings[j], v->get_data()[j].to_string());
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_emptyTest) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto doubles = BinaryColumn::create();
        auto doubles2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            doubles->append(values[j]);
            doubles2->append(strs[j]);
        }

        columns.emplace_back(doubles);
        columns.emplace_back(doubles2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_double(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto str_values = BinaryColumn::create();
        auto str_values2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            str_values->append(values[j]);
            str_values2->append(strs[j]);
        }

        columns.emplace_back(str_values);
        columns.emplace_back(str_values2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto ints = BinaryColumn::create();
        auto ints2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ints->append(values[j]);
            ints2->append(strs[j]);
        }

        columns.emplace_back(ints);
        columns.emplace_back(ints2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_int(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto ints = BinaryColumn::create();
        auto ints2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"$.k3", "$.k4"};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ints->append(values[j]);
            ints2->append(strs[j]);
        }

        columns.emplace_back(ints);
        columns.emplace_back(ints2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_int(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }
}

// Test compatibility of float and double
TEST_F(JsonFunctionsTest, json_float_double) {
    namespace vpack = arangodb::velocypack;
    double x = 1.2;
    JsonValue json = JsonValue::from_double(static_cast<double>(x));
    vpack::Slice vslice = json.to_vslice();
    ASSERT_DOUBLE_EQ(1.2, vslice.getDouble());

    // check to_string
    auto json_str = json.to_string();
    ASSERT_TRUE(json_str.ok());
    ASSERT_EQ("1.2", json_str.value());
}

class JsonQueryTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string>> {};

TEST_P(JsonQueryTestFixture, json_query) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto ints = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    std::string param_result = std::get<2>(GetParam());

    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(param_json, &json).ok());
    ints->append(&json);
    if (param_path == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_path);
    }

    Columns columns{ints, builder.build(true)};

    ctx.get()->impl()->set_constant_columns(columns);
    JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    ColumnPtr result = JsonFunctions::json_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    StripWhiteSpace(&param_result);
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
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

INSTANTIATE_TEST_SUITE_P(
        JsonQueryTest, JsonQueryTestFixture,
        ::testing::Values(
                // clang-format off
                // empty
                std::make_tuple(R"( {"k1":1} )", "NULL", R"(NULL)"),
                std::make_tuple(R"( {"k1":1} )", "$", R"( {"k1": 1} )"),
                std::make_tuple(R"( {"k1":1} )", "", R"(NULL)"),

                // various types
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "$.k2", R"( "hehe" )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "$.k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "$.k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "$.k4", R"( {} )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "$.k5", R"( NULL )"),

                // simple syntax
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "k2", R"( "hehe" )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "k4", R"( {} )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "k5", R"( NULL )"),

                // nested array
                std::make_tuple(R"( {"k1": [1,2,3]} )", "$.k1[0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [1,2,3]} )", "$.k1[3]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [1,2,3]} )", "$.k1[-1]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][1]", R"( 2 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][2]", R"( 3 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][3]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[1][0]", R"( 4 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[1][2]", R"( 6 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[2][0]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[2]]]]]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[[[[[2]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[[1,2,3]]]} )", "$.k1[0][0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": [[1, 2], [3, 4]] }] } )", "$.k1[0].k2[0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": [[1, 2], [3, 4]] }] } )", "$.k1[0].k2[1][0]", R"( 3 )"),

                // nested object
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1", R"( {"k2": {"k3": 1}} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1.k2", R"( {"k3": 1} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1.k2.k3", R"( 1 )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1.k2.k3.k4", R"( NULL )"),

                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1", R"( {"k2": {"k3": 1}} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1.k2", R"( {"k3": 1} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1.k2.k3", R"( 1 )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1.k2.k3.k4", R"( NULL )"),

                // nested object in array
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", "$.k1[0]", R"( {"k2": 1} )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", "$.k1[0].k2", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", "$.k1[0].k3", R"( NULL )"),

                // Top Level Array
                std::make_tuple(R"( [1,2,3] )", "$[1]", R"( 2 )"), 
                std::make_tuple(R"( [1,2,3] )", "$[5]", R"( NULL )"),
                std::make_tuple(R"( [1,2,3] )", "[1]",  R"( 2 )"), 
                std::make_tuple(R"( [1,2,3] )", "[5]",  R"( NULL )"),
                std::make_tuple(R"( [1,2,3] )", "[*]",  R"( [1, 2, 3] )"),
                std::make_tuple(R"( [1,2,3] )", "[*].k1",  R"( [] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k1": 2}] )", "$[0]",      R"( {"k1": 1} )"),
                std::make_tuple(R"( [{"k1": 1}, {"k1": 2}] )", "$[0].k1",   R"( 1 )"),
                std::make_tuple(R"( [{"k1": 1}, {"k1": 2}] )", "$[*].k1",   R"( [1, 2] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k2": 2}] )", "$[*].k1",   R"( [1] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k2": 2}] )", "$[*].k2",   R"( [2] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k2": 2}] )", "$[*]",      R"( [{"k1": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( [[[0, 1, 2]]] )", "$[0][0][0]",      R"( 0 )"),
                std::make_tuple(R"( [[[0, 1, 2]]] )", "$[0][0][1]",      R"( 1 )"),

                // array result
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", "$.k1[*].k2", R"( [1, 2] )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", "$.k1[*]", R"( [{"k2": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}, {"k2": 3}]} )", "$.k1[0:2]",
                                R"( [{"k2": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( {"k1": [1,2,3,4]} )", "$.k1[*]", R"( [1, 2, 3, 4] )"),
                std::make_tuple(R"( {"k1": [1,2,3,4]} )", "$.k1[1:3]", R"( [2, 3] )")
                // clang-format on
                ));

class JsonExistTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, bool>> {};

TEST_P(JsonExistTestFixture, json_exists) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto ints = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::string param_result = std::get<1>(GetParam());
    bool param_exist = std::get<2>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    ints->append(&*json);
    if (param_result == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_result);
    }

    Columns columns{ints, builder.build(true)};

    ctx.get()->impl()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    if (!st.ok()) {
        ASSERT_FALSE(param_exist);
        return;
    }

    ColumnPtr result = JsonFunctions::json_exists(ctx.get(), columns).value();
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

INSTANTIATE_TEST_SUITE_P(JsonExistTest, JsonExistTestFixture,
                         ::testing::Values(std::make_tuple(R"({ "k1":1, "k2":"2"})", "$.k1", true),
                                           std::make_tuple(R"({ "k1": [1,2,3]})", "$.k1", true),
                                           std::make_tuple(R"({ "k1": {}})", "$.k1", true),
                                           std::make_tuple(R"({ "k1": {}})", "$.k1", true),
                                           // nested object
                                           std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", "$.k1.k2.k3", true),
                                           std::make_tuple(R"({"k1": [1]})", "$.k1[0]", true),

                                           // object in array
                                           std::make_tuple(R"({"k1": [{"k2": 1}]})", "$.k1[0].k2", true),

                                           // not exists
                                           std::make_tuple(R"({ })", "$.k1", false),
                                           std::make_tuple(R"({"k1": 1})", "$.k2", false),
                                           std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", "$.k1.k2.k3.k4", false),
                                           std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", "$.k1.k2.k4", false),
                                           std::make_tuple(R"({"k1": [{"k2": 1}]})", "$.k1[0].k3", false),

                                           //  nested array
                                           std::make_tuple(R"({"k1": [[1]]})", "$.k1[0][1]", false),
                                           std::make_tuple(R"({"k1": [[1]]})", "$.k1[0][0]", true),

                                           // special case
                                           std::make_tuple(R"({ "k1": {}})", "$", true),
                                           std::make_tuple(R"({ "k1": {}})", "", false),
                                           std::make_tuple(R"( { "k1": 1} )", "NULL", false),

                                           // error case
                                           std::make_tuple(R"( {"k1": null} )", std::string(10, 0x1), false)));

class JsonParseTestFixture : public ::testing::TestWithParam<std::tuple<std::string, bool, std::string>> {};

TEST_P(JsonParseTestFixture, json_parse) {
    auto [param_json, param_ok, expected] = GetParam();
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto ints = BinaryColumn::create();
    ints->append(param_json);
    Columns columns{ints};
    ctx.get()->impl()->set_constant_columns(columns);

    ColumnPtr result = JsonFunctions::parse_json(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    Datum datum = result->get(0);
    if (param_ok) {
        ASSERT_FALSE(datum.is_null());
        const JsonValue* json_value = datum.get_json();
        ASSERT_TRUE(!!json_value);
        std::string json_str = json_value->to_string().value();
        StripWhiteSpace(&json_str);
        StripWhiteSpace(&param_json);
        if (!expected.empty()) {
            ASSERT_EQ(expected, param_json);
        } else {
            ASSERT_EQ(param_json, json_str);
        }
    } else {
        ASSERT_TRUE(datum.is_null());
    }
}

INSTANTIATE_TEST_SUITE_P(JsonParseTest, JsonParseTestFixture,
                         ::testing::Values(
                                 // clang-format off
                                 
                                 // Parse as json object/array
                                 std::make_tuple(R"( {"k1": 1} )", true, ""), 
                                 std::make_tuple(R"( [1, 2, 3] )", true, ""), 
                                 std::make_tuple(R"( [] )", true, ""),
                                 std::make_tuple(R"( "a" )", true, ""),
                                 std::make_tuple(R"( "1" )", true, ""),

                                 // Parse as string
                                 std::make_tuple(R"( 2.1 )", true, R"(2.1)"), 
                                 std::make_tuple(R"( 1 )", true, R"(1)"), 
                                 std::make_tuple(R"( 1e5 )", true, R"(1e5)"), 
                                 std::make_tuple(R"( a1 )", true, R"(a1)"), 
                                 std::make_tuple(R"( 1a )", true, R"(1a)"), 
                                 std::make_tuple(R"( 1+1 )", true, "1+1"),
                                 std::make_tuple(R"( 2.x )", true, "2.x"),
                                 std::make_tuple(R"( nul )", true, "nul"),

                                 // failure
                                 std::make_tuple(R"( {"k1": 1 )", false, ""),
                                 std::make_tuple(R"( [,,,,,,] )", false, ""),
                                 std::make_tuple(R"( [1, )", false, ""),
                                 std::make_tuple(R"( "1 )", false, "")

                                 // clang-format on
                                 ));

class JsonArrayTestFixture : public ::testing::TestWithParam<std::tuple<std::vector<std::string>, std::string>> {};

TEST_P(JsonArrayTestFixture, json_array) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    std::vector<std::string> param_json = std::get<0>(GetParam());
    std::string param_result = std::get<1>(GetParam());
    for (const auto& json_str : param_json) {
        auto column = JsonColumn::create();
        auto json = JsonValue::parse(json_str);
        ASSERT_TRUE(json.ok());
        column->append(&json.value());
        columns.emplace_back(std::move(column));
    }

    ctx.get()->impl()->set_constant_columns(columns);

    ColumnPtr result;
    if (param_json.empty()) {
        result = JsonFunctions::json_array_empty(ctx.get(), columns).value();
    } else {
        result = JsonFunctions::json_array(ctx.get(), columns).value();
    }
    ASSERT_TRUE(!!result);

    Datum datum = result->get(0);
    ASSERT_FALSE(datum.is_null());
    std::string json_str = datum.get_json()->to_string().value();
    StripWhiteSpace(&json_str);
    StripWhiteSpace(&param_result);
    ASSERT_EQ(param_result, json_str);
}

INSTANTIATE_TEST_SUITE_P(
        JsonArrayTest, JsonArrayTestFixture,
        ::testing::Values(std::make_tuple(std::vector<std::string>{}, "[]"),
                          std::make_tuple(std::vector<std::string>{"1", "2"}, R"(["1", "2"])"),
                          std::make_tuple(std::vector<std::string>{"1", "\"1\""}, R"(["1", "1"])"),
                          std::make_tuple(std::vector<std::string>{"1", R"({"a":1})"}, R"(["1", {"a": 1}])"),
                          std::make_tuple(std::vector<std::string>{"null", R"(1)"}, R"(["null", "1"])"),
                          std::make_tuple(std::vector<std::string>{"null", R"(null)"}, R"(["null", "null"])"),
                          std::make_tuple(std::vector<std::string>{"1", "1", "1"}, R"(["1", "1", "1"])"),
                          std::make_tuple(std::vector<std::string>{"1.1", "1.2"}, R"(["1.1", "1.2"])")));

using JsonObjectTestParam = std::tuple<std::vector<std::string>, std::string>;

class JsonObjectTestFixture : public ::testing::TestWithParam<JsonObjectTestParam> {};

TEST_P(JsonObjectTestFixture, json_object) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    std::vector<std::string> param_json = std::get<0>(GetParam());
    std::string param_result = std::get<1>(GetParam());
    for (const auto& json_str : param_json) {
        auto column = JsonColumn::create();
        auto json = JsonValue::parse(json_str);
        ASSERT_TRUE(json.ok()) << "parse json failed: " << json_str;
        column->append(&json.value());
        columns.emplace_back(std::move(column));
    }

    ctx.get()->impl()->set_constant_columns(columns);

    ColumnPtr result;
    if (param_json.empty()) {
        result = JsonFunctions::json_object_empty(ctx.get(), columns).value();
    } else {
        result = JsonFunctions::json_object(ctx.get(), columns).value();
    }
    ASSERT_TRUE(!!result);

    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
        return;
    }
    ASSERT_FALSE(datum.is_null());
    std::string json_str = datum.get_json()->to_string().value();
    StripWhiteSpace(&json_str);
    StripWhiteSpace(&param_result);
    ASSERT_EQ(param_result, json_str);
}

INSTANTIATE_TEST_SUITE_P(JsonObjectTest, JsonObjectTestFixture,
                         ::testing::Values(
                                 // clang-format off
                          JsonObjectTestParam(std::vector<std::string>{}, "{}"),
                          JsonObjectTestParam({R"("a")", "1", R"("b")", ""}, R"( {"a": "1", "b": ""} )"),
                          JsonObjectTestParam({R"("a")"}, R"({"a": null})"),
                          JsonObjectTestParam({R"("a")", R"("a")", R"("a")"}, R"({"a": "a", "a": null})"),
                          JsonObjectTestParam({R"("a")", R"("a")"}, R"({"a": "a"})"),
                          JsonObjectTestParam({R"("a")", "1"}, R"({"a": "1"})"),
                          JsonObjectTestParam({R"("a")", "1.234"}, R"({"a": "1.234"})"),
                          JsonObjectTestParam({R"("a")", "null"}, R"({"a": "null"})"),
                          JsonObjectTestParam({R"("a")", "true"}, R"({"a": "true"})"),
                          JsonObjectTestParam({R"("a")", "1", R"("b")", "2"}, R"({"a": "1", "b": "2"})"),
                          JsonObjectTestParam({R"("a")", "[1,2]"}, R"({"a": [1, 2]})"),
                          JsonObjectTestParam({R"("a")", R"({"b": 2})"}, R"({"a": {"b": 2}})"),

                          // illegal object
                          JsonObjectTestParam({"1", "1"}, R"({"1": "1"})"), 
                          JsonObjectTestParam({"1"}, R"({"1": null})"),
                          JsonObjectTestParam({R"("a")", "1", "1"}, R"({"1": null, "a": "1"})"),
                          JsonObjectTestParam({R"("")"}, R"(NULL)")

                                 // clang-format on
                                 ));

TEST_F(JsonFunctionsTest, extract_from_object_test) {
    std::string output;

    EXPECT_OK(test_extract_from_object(R"({"data" : 1})", "$.data", &output));
    EXPECT_STREQ(output.data(), "1");

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data" : 1})", "$.dataa", &output));

    EXPECT_OK(test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[1].key", &output));
    EXPECT_STREQ(output.data(), "2");

    EXPECT_STATUS(Status::NotFound(""),
                  test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[2].key", &output));

    EXPECT_STATUS(Status::NotFound(""),
                  test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[3].key", &output));

    EXPECT_STATUS(Status::DataQualityError(""),
                  test_extract_from_object(R"({"data1 " : 1, "data2":})", "$.data", &output));
}

class JsonLengthTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, int>> {};

TEST_P(JsonLengthTestFixture, json_length_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    int expect_length = std::get<2>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_column->append(&*json);

    Columns columns;
    columns.push_back(json_column);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.push_back(path_column);
    }

    // ctx.get()->impl()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    ASSERT_OK(st);

    ColumnPtr result = JsonFunctions::json_length(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    EXPECT_EQ(expect_length, result->get(0).get_int32());

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(JsonLengthTest, JsonLengthTestFixture,
                         ::testing::Values(
                            std::make_tuple(R"({ "k1":1, "k2": 2 })", "", 2), 
                            std::make_tuple(R"({ "k1":1, "k2": {} })", "$.k2", 0), 
                            std::make_tuple(R"({ "k1":1, "k2": [1,2] })", "$.k2", 2), 
                            std::make_tuple(R"({ "k1":1, "k2": [1,2] })", "$.k3", 0), 
                            std::make_tuple(R"({ })", "", 0),
                            std::make_tuple(R"( [] )", "", 0), 
                            std::make_tuple(R"( [1] )", "", 1),
                            std::make_tuple(R"( null )", "", 1), 
                            std::make_tuple(R"( 1 )", "", 1)
                        ));
// clang-format on

class JsonKeysTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string>> {};

TEST_P(JsonKeysTestFixture, json_keys) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    std::string param_result = std::get<2>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_column->append(&*json);

    Columns columns;
    columns.push_back(json_column);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.push_back(path_column);
    }

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
INSTANTIATE_TEST_SUITE_P(JsonKeysTest, JsonKeysTestFixture,
                         ::testing::Values(std::make_tuple(R"({ "k1": 1, "k2": 2 })", "", R"(["k1", "k2"])"),
                                           std::make_tuple(R"({ "k1": "v1" })",  "", R"(["k1"])"),
                                           std::make_tuple(R"({ "k1": {"k2": 1} })",  "", R"(["k1"])"),
                                           std::make_tuple(R"({ })",  "", R"([])"),
                                           std::make_tuple(R"( [] )",  "", "NULL"),
                                           std::make_tuple(R"( 1 )",  "", "NULL"),
                                           std::make_tuple(R"( "hehe")", "", "NULL"),
                                           std::make_tuple(R"({ "k1": "v1" })",  "$.k1", R"(NULL)"),
                                           std::make_tuple(R"({ "k1": "v1" })",  "$.k3", R"(NULL)"),
                                           std::make_tuple(R"({ "k1": {"k2": 1} })",  "$.k1", R"(["k2"])")
                         ));

// clang-format on

// 1: JSON Input
// 2. Path
// 3. get_json_int execpted result
// 4. get_json_string expected result
// 5. get_json_double expected result
using GetJsonXXXParam = std::tuple<std::string, std::string, int, std::string, double>;

class GetJsonXXXTestFixture : public ::testing::TestWithParam<GetJsonXXXParam> {
public:
    StatusOr<Columns> setup() {
        _ctx = std::unique_ptr<FunctionContext>(FunctionContext::create_test_context());
        auto ints = JsonColumn::create();
        ColumnBuilder<TYPE_VARCHAR> builder(1);

        std::string param_json = std::get<0>(GetParam());
        std::string param_path = std::get<1>(GetParam());

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
        Columns columns{ints, builder.build(true)};

        _ctx->impl()->set_constant_columns(columns);
        JsonFunctions::native_json_path_prepare(_ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
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

TEST_P(GetJsonXXXTestFixture, get_json_int) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    int expected = std::get<2>(GetParam());

    ColumnPtr result = JsonFunctions::get_native_json_int(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    ASSERT_EQ(1, result->size());
    Datum datum = result->get(0);
    if (expected == 0) {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        int64_t value = datum.get_int32();
        ASSERT_EQ(expected, value);
    }
}

TEST_P(GetJsonXXXTestFixture, get_json_string) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    std::string param_result = std::get<3>(GetParam());
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

TEST_P(GetJsonXXXTestFixture, get_json_double) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    double expected = std::get<4>(GetParam());

    ColumnPtr result = JsonFunctions::get_native_json_double(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());

    Datum datum = result->get(0);
    if (expected == 0) {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        double value = datum.get_double();
        ASSERT_EQ(expected, value);
    }
}

INSTANTIATE_TEST_SUITE_P(GetJsonXXXTest, GetJsonXXXTestFixture,
                         ::testing::Values(
                                 // clang-format: off

                                 std::make_tuple(R"( {"k1":1} )", "NULL", 0, "NULL", 0.0),
                                 std::make_tuple(R"( {"k1":1} )", "$", 0, R"( {"k1": 1} )", 0.0),
                                 std::make_tuple(R"( {"k1":1} )", "", 0, R"( NULL )", 0.0),
                                 std::make_tuple(R"( {"k0": null} )", "$.k1", 0, "NULL", 0.0),
                                 std::make_tuple(R"( {"k1": 1} )", "$.k1", 1, "1", 1.0),
                                 std::make_tuple(R"( {"k1": -1} )", "$.k1", -1, R"( -1 )", -1),
                                 std::make_tuple(R"( {"k1": 1.1} )", "$.k1", 1, R"( 1.1 )", 1.1),
                                 std::make_tuple(R"( {"k1": 3.14} )", "$.k1", 3, R"( 3.14 )", 3.14),
                                 std::make_tuple(R"( {"k1": null} )", "$.k1", 0, R"( NULL )", 0.0),
                                 std::make_tuple(R"( {"k1": "value" } )", "$.k1", 0, R"( value )", 0.0),
                                 std::make_tuple(R"( {"k1": {"k2": 1}} )", "$.k1", 0, R"( {"k2": 1} )", 0.0),
                                 std::make_tuple(R"( {"k1": [1,2,3] } )", "$.k1", 0, R"( [1, 2, 3] )", 0.0),

                                 // nested path
                                 std::make_tuple(R"( {"k1.k2": [1,2,3] } )", "$.\"k1.k2\"", 0, R"( [1, 2, 3] )", 0.0)

                                 // clang-format: on
                                 ));

} // namespace starrocks::vectorized
