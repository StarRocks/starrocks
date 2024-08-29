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

TEST_F(JsonFlattenerTest, testSortHitNums) {
    // clang-format off
    std::vector<std::string> jsons = {
    R"({"k1": 10, "k2": 20, "k3": 30, "k4": 40, "k5": 50, "k6": 60, "k7": 70, "k8": 80,  "k9": 90,  "a10": 100, "k11": 110, "h12": 120, "k13": 130, "g14": 140, "k15": 150, "z16": 160, "e17": 170, "c18": 180, "a19": 190, "b20": 200})",
    R"({"k2": 25, "k3": 35, "k4": 45, "k5": 55, "k6": 65, "k7": 75, "k8": 85,  "k9": 95,  "a10": 105, "k11": 115, "h12": 125, "k13": 135, "g14": 145, "k15": 155, "z16": 165, "e17": 175, "c18": 185, "a19": 195, "b20": 205})",
    R"({"k3": 32, "k4": 42, "k5": 52, "k6": 62, "k7": 72, "k8": 82,  "k9": 92,  "a10": 102, "k11": 112, "h12": 122, "k13": 132, "g14": 142, "k15": 152, "z16": 162, "e17": 172, "c18": 182, "a19": 192, "b20": 202})",
    R"({"k4": 48, "k5": 58, "k6": 68, "k7": 78, "k8": 88,  "k9": 98,  "a10": 108, "k11": 118, "h12": 128, "k13": 138, "g14": 148, "k15": 158, "z16": 168, "e17": 178, "c18": 188, "a19": 198, "b20": 208})",
    R"({"k5": 54, "k6": 64, "k7": 74, "k8": 84,  "k9": 94,  "a10": 104, "k11": 114, "h12": 124, "k13": 134, "g14": 144, "k15": 154, "z16": 164, "e17": 174, "c18": 184, "a19": 194, "b20": 204})",
    R"({"k6": 66, "k7": 76, "k8": 86,  "k9": 96,  "a10": 106, "k11": 116, "h12": 126, "k13": 136, "g14": 146, "k15": 156, "z16": 166, "e17": 176, "c18": 186, "a19": 196, "b20": 206})",
    R"({"k7": 79, "k8": 89,  "k9": 99,  "a10": 109, "k11": 119, "h12": 129, "k13": 139, "g14": 149, "k15": 159, "z16": 169, "e17": 179, "c18": 189, "a19": 199, "b20": 209})",
    R"({"k8": 81,  "k9": 91,  "a10": 101, "k11": 111, "h12": 121, "k13": 131, "g14": 141, "k15": 151, "z16": 161, "e17": 171, "c18": 181, "a19": 191, "b20": 201})",
    R"({"k9": 93,  "a10": 103, "k11": 113, "h12": 123, "k13": 133, "g14": 143, "k15": 153, "z16": 163, "e17": 173, "c18": 183, "a19": 193, "b20": 203})",
    R"({"a10": 107, "k11": 117, "h12": 127, "k13": 137, "g14": 147, "k15": 157, "z16": 167, "e17": 177, "c18": 187, "a19": 197, "b20": 207})",
   R"({"k11": 130, "h12": 140, "k13": 150, "g14": 160, "k15": 170, "z16": 180, "e17": 190, "c18": 200, "a19": 210, "b20": 220})",
   R"({"h12": 145, "k13": 155, "g14": 165, "k15": 175, "z16": 185, "e17": 195, "c18": 205, "a19": 215, "b20": 225})",
   R"({"k13": 152, "g14": 162, "k15": 172, "z16": 182, "e17": 192, "c18": 202, "a19": 212, "b20": 222})",
   R"({"g14": 168, "k15": 178, "z16": 188, "e17": 198, "c18": 208, "a19": 218, "b20": 228})",
   R"({"k15": 174, "z16": 184, "e17": 194, "c18": 204, "a19": 214, "b20": 224})",
   R"({"z16": 186, "e17": 196, "c18": 206, "a19": 216, "b20": 226})",
   R"({"e17": 199, "c18": 209, "a19": 219, "b20": 229})",
   R"({"c18": 201, "a19": 211, "b20": 221})",
   R"({"a19": 213, "b20": 223})",
   R"({"b20": 227})"
    };
    // clang-format on

    ColumnPtr input = JsonColumn::create();
    JsonColumn* json_input = down_cast<JsonColumn*>(input.get());
    for (const auto& json : jsons) {
        ASSIGN_OR_ABORT(auto json_value, JsonValue::parse(json));
        json_input->append(&json_value);
    }

    {
        config::json_flat_sparsity_factor = 0.3;
        JsonPathDeriver jf;
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a10", "a19", "b20", "c18", "e17", "g14", "h12", "k11",
                                          "k13", "k15", "k6",  "k7",  "k8",  "k9",  "z16"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
    }

    {
        config::json_flat_sparsity_factor = 0.4;
        JsonPathDeriver jf;
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a10", "a19", "b20", "c18", "e17", "g14", "h12",
                                          "k11", "k13", "k15", "k8",  "k9",  "z16"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
    }

    {
        config::json_flat_sparsity_factor = 0.8;
        JsonPathDeriver jf;
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a19", "b20", "c18", "e17", "z16"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
    }

    {
        config::json_flat_sparsity_factor = 0;
        JsonPathDeriver jf;
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a10", "a19", "b20", "c18", "e17", "g14", "h12", "k1", "k11", "k13",
                                          "k15", "k2",  "k3",  "k4",  "k5",  "k6",  "k7",  "k8", "k9",  "z16"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
    }
}

} // namespace starrocks
