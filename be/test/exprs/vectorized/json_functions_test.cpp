// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exprs/vectorized/json_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "butil/time.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "testutil/assert.h"

namespace starrocks {
namespace vectorized {

class JsonFunctionsTest : public ::testing::Test {
public:
    void SetUp() {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

    Status test_extract_from_object(std::string input, std::string jsonpath, std::string* output) {
        // reverse for padding.
        input.reserve(input.size() + simdjson::SIMDJSON_PADDING);

        simdjson::ondemand::parser parser;
        simdjson::ondemand::document doc;
        EXPECT_EQ(simdjson::error_code::SUCCESS, parser.iterate(input).get(doc));

        simdjson::ondemand::object obj;

        EXPECT_EQ(simdjson::error_code::SUCCESS, doc.get_object().get(obj));

        std::vector<JsonPath> path;
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

TEST_F(JsonFunctionsTest, get_json_intTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto ints = BinaryColumn::create();
    auto ints2 = BinaryColumn::create();

    std::string values[] = {"{\"k1\":1, \"k2\":\"2\"}", "{\"k1\":\"v1\", \"my.key\":[1, 2, 3]}"};
    std::string strs[] = {"$.k1", "$.\"my.key\"[1]"};
    int length_ints[] = {1, 2};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ints->append(values[j]);
        ints2->append(strs[j]);
    }

    columns.emplace_back(ints);
    columns.emplace_back(ints2);

    ctx.get()->impl()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ColumnPtr result = JsonFunctions::get_json_int(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_ints[j], v->get_data()[j]);
    }

    ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_doubleTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto doubles = BinaryColumn::create();
    auto doubles2 = BinaryColumn::create();

    std::string values[] = {"{\"k1\":1.3, \"k2\":\"2\"}", "{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}"};
    std::string strs[] = {"$.k1", "$.\"my.key\"[1]"};
    double length_doubles[] = {1.3, 2.2};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        doubles->append(values[j]);
        doubles2->append(strs[j]);
    }

    columns.emplace_back(doubles);
    columns.emplace_back(doubles2);

    ctx.get()->impl()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ColumnPtr result = JsonFunctions::get_json_double(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_DOUBLE>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_doubles[j], v->get_data()[j]);
    }

    ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_stringTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto strings = BinaryColumn::create();
    auto strings2 = BinaryColumn::create();

    std::string values[] = {"{\"k1\":\"v1\", \"k2\":\"v2\"}", "{\"k1\":\"v1\", \"my.key\":[\"e1\", \"e2\", \"e3\"]}",
                            "{\"k1.key\":{\"k2\":[\"v1\", \"v2\"]}}",
                            "[{\"k1\":\"v1\"}, {\"k1\":\"v2\"}, {\"k1\":\"v3\"}, {\"k1\":\"v4\"}]"};

    std::string strs[] = {"$.k1", "$.\"my.key\"[1]", "$.\"k1.key\".k2[0]", "$.k1"};
    std::string length_strings[] = {"v1", "e2", "v1", "v1", "v2", "v3", "v4"};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        strings->append(values[j]);
        strings2->append(strs[j]);
    }

    columns.emplace_back(strings);
    columns.emplace_back(strings2);

    ctx.get()->impl()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_strings[j], v->get_data()[j].to_string());
    }

    ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

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
                                    R"({"k11":"v11"})",
                                    R"({"k11":"v11"})",
                                    R"({"k11":"v11"})"};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        strings->append(values[j]);
        strings2->append(strs[j]);
    }

    columns.emplace_back(strings);
    columns.emplace_back(strings2);

    ctx.get()->impl()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns);

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_strings[j], v->get_data()[j].to_string());
    }

    ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_emptyTest) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto doubles = BinaryColumn::create();
        auto doubles2 = BinaryColumn::create();

        std::string values[] = {"{\"k1\":1.3, \"k2\":\"2\"}", "{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            doubles->append(values[j]);
            doubles2->append(strs[j]);
        }

        columns.emplace_back(doubles);
        columns.emplace_back(doubles2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        ColumnPtr result = JsonFunctions::get_json_double(ctx.get(), columns);

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                                   FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto str_values = BinaryColumn::create();
        auto str_values2 = BinaryColumn::create();

        std::string values[] = {"{\"k1\":1.3, \"k2\":\"2\"}", "{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            str_values->append(values[j]);
            str_values2->append(strs[j]);
        }

        columns.emplace_back(str_values);
        columns.emplace_back(str_values2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns);

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                                   FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto ints = BinaryColumn::create();
        auto ints2 = BinaryColumn::create();

        std::string values[] = {"{\"k1\":1.3, \"k2\":\"2\"}", "{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ints->append(values[j]);
            ints2->append(strs[j]);
        }

        columns.emplace_back(ints);
        columns.emplace_back(ints2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        ColumnPtr result = JsonFunctions::get_json_int(ctx.get(), columns);

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                                   FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto ints = BinaryColumn::create();
        auto ints2 = BinaryColumn::create();

        std::string values[] = {"{\"k1\":1.3, \"k2\":\"2\"}", "{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}"};
        std::string strs[] = {"$.k3", "$.k4"};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ints->append(values[j]);
            ints2->append(strs[j]);
        }

        columns.emplace_back(ints);
        columns.emplace_back(ints2);

        ctx.get()->impl()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        ColumnPtr result = JsonFunctions::get_json_int(ctx.get(), columns);

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::json_path_close(ctx.get(),
                                                   FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }
}

TEST_F(JsonFunctionsTest, extract_from_object_test) {
    std::string output;
    Status st;

    st = test_extract_from_object(R"({"data" : 1})", "$.data", &output);
    EXPECT_OK(st);
    EXPECT_STREQ(output.data(), "1");

    st = test_extract_from_object(R"({"data" : 1})", "$.dataa", &output);
    EXPECT_STATUS(Status::NotFound(""), st);

    st = test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[1].key", &output);
    EXPECT_OK(st);
    EXPECT_STREQ(output.data(), "2");

    st = test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[2].key", &output);
    EXPECT_STATUS(Status::NotFound(""), st);
}

} // namespace vectorized
} // namespace starrocks
