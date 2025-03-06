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

#include <gtest/gtest.h>

#include "exprs/table_function/table_function.h"
#include "exprs/table_function/table_function_factory.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/function_utils.h"

namespace starrocks {

class JsonEachTest : public testing::Test {
public:
    void test_impl(const std::vector<std::string>& inputs,
                   const std::vector<std::tuple<std::string, std::string>>& expected) {
        const TableFunction* func =
                get_table_function("json_each", {TYPE_JSON}, {TYPE_VARCHAR, TYPE_JSON}, TFunctionBinaryType::BUILTIN);

        auto rt_state = std::make_unique<RuntimeState>();
        rt_state->set_chunk_size(4096);

        // input
        auto json_column = JsonColumn::create();
        for (auto& input : inputs) {
            if (input != "null" && input != "empty") {
                json_column->append(JsonValue::parse(input).value());
            } else {
                json_column->append_nulls(1);
            }
        }
        Columns input_columns;
        if (!inputs.empty()) {
            input_columns.push_back(json_column->clone());
        }
        TableFunctionState* func_state;

        // execute
        ASSERT_OK(func->init({}, &func_state));
        func_state->set_params(input_columns);
        ASSERT_OK(func->open(rt_state.get(), func_state));
        auto [result_columns, offset_column] = func->process(rt_state.get(), func_state);

        // check
        ASSERT_EQ(func_state->input_rows(), func_state->processed_rows());
        ASSERT_EQ(2, result_columns.size());
        ASSERT_EQ(expected.size(), result_columns[0]->size());
        auto result_key = ColumnHelper::cast_to<TYPE_VARCHAR>(result_columns[0]);
        auto result_value = ColumnHelper::cast_to<TYPE_JSON>(result_columns[1]);
        int i = 0;
        for (auto [expect_key, expect_value] : expected) {
            EXPECT_EQ(expect_key, result_key->get(i).get_slice());
            EXPECT_EQ(expect_value, result_value->get(i).get_json()->to_string_uncheck());
            i++;
        }

        // close
        func->close(rt_state.get(), func_state);
    }
};

TEST_F(JsonEachTest, json_each_object) {
    std::string input = R"( {"k1": 1, "k2": "str", "k3": [1,2,3], "k4": null, "k5": {}} )";
    // clang-format off
    std::vector<std::tuple<std::string, std::string>> expect = {
        {"k1", "1"},
        {"k2", "\"str\""},
        {"k3", "[1, 2, 3]"},
        {"k4", "null"},
        {"k5", "{}"},
    };
    // clang-format on
    test_impl(std::vector<std::string>{input}, expect);
}

TEST_F(JsonEachTest, json_each_object_null) {
    std::string input = R"(null)";
    // clang-format off
    std::vector<std::tuple<std::string, std::string>> expect = {
    };
    // clang-format on
    test_impl(std::vector<std::string>{input}, expect);
}

TEST_F(JsonEachTest, json_each_object_empty) {
    // clang-format off
    std::vector<std::tuple<std::string, std::string>> expect = {
    };
    // clang-format on
    test_impl(std::vector<std::string>{}, expect);
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
    test_impl(std::vector<std::string>{input}, expect);
}

TEST_F(JsonEachTest, json_each_hybrid) {
    // clang-format off
    std::vector<std::tuple<std::string, std::string>> expect = {
        {"0", "1"},
    };
    // clang-format on
    test_impl(std::vector<std::string>{R"( 3.14 )", R"( [1] )", R"( [] )", R"( {} )"}, expect);
}

} // namespace starrocks
