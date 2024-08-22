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

#include "util/json_flattener.h"

#include <glog/logging.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <velocypack/vpack.h>

#include <string>
#include <vector>

#include "column/const_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/mock_vectorized_expr.h"
#include "gtest/gtest-param-test.h"
#include "gutil/casts.h"
#include "gutil/strings/strip.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "util/json_flattener.h"

namespace starrocks {

class JsonPathDeriverTest
        : public ::testing::TestWithParam<
                  std::tuple<std::string, std::string, bool, std::vector<std::string>, std::vector<LogicalType>>> {};

TEST_P(JsonPathDeriverTest, json_path_deriver_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json1 = std::get<0>(GetParam());
    std::string param_json2 = std::get<1>(GetParam());
    bool param_has_remain = std::get<2>(GetParam());
    std::vector<std::string> param_flat_path = std::get<3>(GetParam());
    std::vector<LogicalType> param_flat_type = std::get<4>(GetParam());

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

    ASSERT_EQ(param_has_remain, jf.has_remain_json());
    ASSERT_EQ(param_flat_path, path);
    ASSERT_EQ(param_flat_type, type);
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(JsonPathDeriverCases, JsonPathDeriverTest,
    ::testing::Values(
        // NORMAL
        std::make_tuple(R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3, "k2": 4} )", false, std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_BIGINT}),
        std::make_tuple(R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3} )", true, std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_BIGINT}),
        std::make_tuple(R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3, "k3": 4} )", true, std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_BIGINT}),

        // EMPTY
        std::make_tuple(R"( {"k1": 1, "k2": {}} )", R"( {"k1": 3, "k2": {}} )", true, std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_BIGINT}),
        std::make_tuple(R"( {} )", R"( {"k1": 3} )", true, std::vector<std::string> {}, std::vector<LogicalType> {}),
        std::make_tuple(R"( {"": 123} )", R"( {"": 234} )", true, std::vector<std::string> {}, std::vector<LogicalType> {}),

        // DEEP
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": 2}} )", R"( {"k2": {"j1": 3, "j2": 4}} )", false, std::vector<std::string> {"k2.j1", "k2.j2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_BIGINT}),
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": 2}} )", R"( {"k2": {"j1": 3, "j3": 4}} )", true, std::vector<std::string> {"k2.j1"}, std::vector<LogicalType> {TYPE_BIGINT}),
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": 2}} )", R"( {"k2": {"j1": 3, "j2": {"p1": "abc"}}} )", true, std::vector<std::string> {"k2.j1"}, std::vector<LogicalType> {TYPE_BIGINT}),
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": {"p1": [1,2,3,4]}}} )", R"( {"k2": {"j1": 3, "j2": {"p1": "abc"}}} )", false, std::vector<std::string> {"k2.j1", "k2.j2.p1"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_JSON})
));
// clang-format on

class JsonFlattenerTest : public testing::Test {
public:
    JsonFlattenerTest() = default;

    ~JsonFlattenerTest() override = default;

protected:
    void SetUp() override {}

    void TearDown() override {}

    std::vector<ColumnPtr> test_json(const std::vector<std::string>& inputs, const std::vector<std::string>& paths,
                                     const std::vector<LogicalType>& types, bool has_remain) {
        ColumnPtr input = JsonColumn::create();
        JsonColumn* json_input = down_cast<JsonColumn*>(input.get());
        for (const auto& json : inputs) {
            ASSIGN_OR_ABORT(auto json_value, JsonValue::parse(json));
            json_input->append(&json_value);
        }

        JsonFlattener flattener(paths, types, has_remain);
        flattener.flatten(json_input);

        auto result = flattener.mutable_result();
        if (has_remain) {
            for (size_t i = 0; i < result.size() - 1; i++) {
                auto& c = result[i];
                EXPECT_TRUE(c->is_nullable());
                auto* nullable = down_cast<NullableColumn*>(c.get());
                EXPECT_EQ(input->size(), nullable->size());
            }
            EXPECT_FALSE(result.back()->is_nullable());
            EXPECT_EQ(input->size(), result.back()->size());
            EXPECT_EQ(paths.size() + 1, result.size());
        } else {
            for (auto& c : result) {
                EXPECT_TRUE(c->is_nullable());
                auto* nullable = down_cast<NullableColumn*>(c.get());
                EXPECT_EQ(input->size(), nullable->size());
            }
            EXPECT_EQ(paths.size(), result.size());
        }

        return result;
    }

    std::vector<ColumnPtr> test_null_json(const std::vector<std::string>& inputs, const std::vector<std::string>& paths,
                                          const std::vector<LogicalType>& types, bool has_remain) {
        ColumnPtr input = JsonColumn::create();
        NullColumnPtr nulls = NullColumn::create();
        JsonColumn* json_input = down_cast<JsonColumn*>(input.get());
        for (const auto& json : inputs) {
            if (json == "NULL") {
                json_input->append_default();
                nulls->append(1);
            } else {
                ASSIGN_OR_ABORT(auto json_value, JsonValue::parse(json));
                json_input->append(&json_value);
                nulls->append(0);
            }
        }

        auto nullable_input = NullableColumn::create(input, nulls);
        JsonFlattener flattener(paths, types, has_remain);
        flattener.flatten(nullable_input.get());

        auto result = flattener.mutable_result();
        if (has_remain) {
            for (size_t i = 0; i < result.size() - 1; i++) {
                auto& c = result[i];
                EXPECT_TRUE(c->is_nullable());
                auto* nullable = down_cast<NullableColumn*>(c.get());
                EXPECT_EQ(input->size(), nullable->size());
            }
            EXPECT_FALSE(result.back()->is_nullable());
            EXPECT_EQ(input->size(), result.back()->size());
            EXPECT_EQ(paths.size() + 1, result.size());
        } else {
            for (auto& c : result) {
                EXPECT_TRUE(c->is_nullable());
                auto* nullable = down_cast<NullableColumn*>(c.get());
                EXPECT_EQ(input->size(), nullable->size());
            }
            EXPECT_EQ(paths.size(), result.size());
        }
        return result;
    }
};

TEST_F(JsonFlattenerTest, testNormalJson) {
    std::vector<std::string> json = {R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3, "k2": 4} )"};

    std::vector<std::string> paths = {"k1", "k2"};
    std::vector<LogicalType> types = {TYPE_BIGINT, TYPE_BIGINT};
    auto result = test_json(json, paths, types, false);
    EXPECT_EQ("1", result[0]->debug_item(0));
    EXPECT_EQ("4", result[1]->debug_item(1));
}

TEST_F(JsonFlattenerTest, testCastNormalJson) {
    std::vector<std::string> json = {R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3, "k2": [1,2,3,4]} )"};

    std::vector<std::string> paths = {"k1", "k2"};
    std::vector<LogicalType> types = {TYPE_BIGINT, TYPE_JSON};
    auto result = test_json(json, paths, types, false);
    EXPECT_EQ("1", result[0]->debug_item(0));
    EXPECT_EQ("[1, 2, 3, 4]", result[1]->debug_item(1));
}

TEST_F(JsonFlattenerTest, testCastJson) {
    std::vector<std::string> json = {R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3, "k2": [1,2,3,4]} )"};

    std::vector<std::string> paths = {"k1", "k2"};
    std::vector<LogicalType> types = {TYPE_BIGINT, TYPE_BIGINT};
    auto result = test_json(json, paths, types, false);
    EXPECT_EQ("1", result[0]->debug_item(0));
    EXPECT_EQ("NULL", result[1]->debug_item(1));
}

TEST_F(JsonFlattenerTest, testDeepJson) {
    std::vector<std::string> json = {R"( {"k1": 1, "k2": 2} )",
                                     R"( {"k1": {"c1": 123}, "k2": {"j1": "abc", "j2": 123}} )"};

    std::vector<std::string> paths = {"k1", "k2.j1"};
    std::vector<LogicalType> types = {TYPE_BIGINT, TYPE_VARCHAR};
    auto result = test_json(json, paths, types, false);
    EXPECT_EQ("1", result[0]->debug_item(0));
    EXPECT_EQ("NULL", result[1]->debug_item(0));
    EXPECT_EQ("NULL", result[0]->debug_item(1));
    EXPECT_EQ("'abc'", result[1]->debug_item(1));
}

TEST_F(JsonFlattenerTest, testDeepJson2) {
    std::vector<std::string> json = {R"( {"k1": 1, "k2": 2} )",
                                     R"( {"k1": {"c1": 123}, "k2": {"j1": "abc", "j2": 123}} )"};

    std::vector<std::string> paths = {"k1", "k2.j1", "k2.j2"};
    std::vector<LogicalType> types = {TYPE_JSON, TYPE_JSON, TYPE_BIGINT};
    auto result = test_json(json, paths, types, false);
    EXPECT_EQ("1", result[0]->debug_item(0));
    EXPECT_EQ(R"({"c1": 123})", result[0]->debug_item(1));
    EXPECT_EQ("NULL", result[1]->debug_item(0));
    EXPECT_EQ("\"abc\"", result[1]->debug_item(1));
    EXPECT_EQ("NULL", result[2]->debug_item(0));
    EXPECT_EQ("123", result[2]->debug_item(1));
}

TEST_F(JsonFlattenerTest, testDeepJson3) {
    std::vector<std::string> json = {R"( {"k1": 1, "k2": 2} )",
                                     R"( {"k1": {"c1": 123}, "k2": {"j1": "abc", "j2": 123}} )"};

    std::vector<std::string> paths = {"k1", "k2.j1", "k2"};
    std::vector<LogicalType> types = {TYPE_JSON, TYPE_JSON, TYPE_JSON};
    auto result = test_json(json, paths, types, false);
    EXPECT_EQ("1", result[0]->debug_item(0));
    EXPECT_EQ(R"({"c1": 123})", result[0]->debug_item(1));
    EXPECT_EQ("NULL", result[1]->debug_item(0));
    EXPECT_EQ("\"abc\"", result[1]->debug_item(1));
    EXPECT_EQ("NULL", result[2]->debug_item(0));
    EXPECT_EQ("NULL", result[2]->debug_item(1));
}

TEST_F(JsonFlattenerTest, testMiddleJson) {
    std::vector<std::string> json = {R"( {"k1": {"c1": {"d1":  123 }}, "k2": {"j1": "def", "j2": {"g1": [1,2,3]}}} )",
                                     R"( {"k1": {"c1": {"d1": "abc"}}, "k2": {"j1": "abc", "j2": {"g1": 123}}} )"};

    std::vector<std::string> paths = {"k1.c1", "k2.j2"};
    std::vector<LogicalType> types = {TYPE_JSON, TYPE_JSON};
    auto result = test_json(json, paths, types, false);
    EXPECT_EQ(R"({"d1": 123})", result[0]->debug_item(0));
    EXPECT_EQ(R"({"d1": "abc"})", result[0]->debug_item(1));
    EXPECT_EQ(R"({"g1": [1, 2, 3]})", result[1]->debug_item(0));
    EXPECT_EQ(R"({"g1": 123})", result[1]->debug_item(1));
}

} // namespace starrocks
