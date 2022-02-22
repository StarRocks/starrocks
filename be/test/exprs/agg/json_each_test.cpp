// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <gtest/gtest.h>

#include "exprs/table_function/table_function.h"
#include "exprs/table_function/table_function_factory.h"
#include "gen_cpp/Types_types.h"
#include "testutil/assert.h"
#include "testutil/function_utils.h"

namespace starrocks::vectorized {

class JsonEachTest : public testing::Test {
public:
    void test_impl(std::string input, std::vector<std::tuple<std::string, std::string>> expected) {
        const TableFunction* func =
                get_table_function("json_each", {TYPE_JSON}, {TYPE_VARCHAR, TYPE_JSON}, TFunctionBinaryType::BUILTIN);

        // input
        auto json_column = JsonColumn::create();
        json_column->append(JsonValue::parse(input).value());
        Columns input_columns{json_column};
        TableFunctionState* func_state;
        bool eos;

        // execute
        ASSERT_OK(func->init({}, &func_state));
        func_state->set_params(input_columns);
        ASSERT_OK(func->open(func_state));
        auto [result_columns, offset_column] = func->process(func_state, &eos);

        // check
        ASSERT_TRUE(eos);
        ASSERT_EQ(2, offset_column->size());
        ASSERT_EQ(2, result_columns.size());
        ASSERT_EQ(expected.size(), result_columns[0]->size());
        auto result_key = ColumnHelper::cast_to<TYPE_VARCHAR>(result_columns[0]);
        auto result_value = ColumnHelper::cast_to<TYPE_JSON>(result_columns[1]);
        int i = 0;
        for (auto [expect_key, expect_value] : expected) {
            EXPECT_EQ(expect_key, result_key->get(i).get_slice());
            EXPECT_EQ(JsonValue::parse(expect_value).value(), *result_value->get(i).get_json());
            i++;
        }

        // close
        func->close(func_state);
    }
};

TEST_F(JsonEachTest, json_each_object) {
    std::string input = R"( {"k1": 1, "k2": "str", "k3": [1,2,3], "k4": null, "k5": {}} )";
    // clang-format off
    std::vector<std::tuple<std::string, std::string>> expect = {
        {"k1", "1"},
        {"k2", "\"str\""},
        {"k3", "[1,2,3]"},
        {"k4", "null"},
        {"k5", "{}"},
    };
    // clang-format on
    test_impl(input, expect);
}

TEST_F(JsonEachTest, json_each_array) {
    std::string input = R"( [1, 2, true, [1], null, {"a": 1}] )";
    // clang-format off
    std::vector<std::tuple<std::string, std::string>> expect = {
        {"0", "1"},
        {"1", "2"},
        {"2", "true"},
        {"3", "[1]"},
        {"4", "null"},
        {"5", R"({"a": 1})"},
    };
    // clang-format on
    test_impl(input, expect);
}
} // namespace starrocks::vectorized