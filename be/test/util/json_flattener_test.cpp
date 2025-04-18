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

#include <cmath>
#include <cstdint>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "column/column.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/mock_vectorized_expr.h"
#include "gtest/gtest-param-test.h"
#include "gutil/casts.h"
#include "gutil/integral_types.h"
#include "gutil/strings/strip.h"
#include "storage/rowset/binary_dict_page.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/binary_prefix_page.h"
#include "storage/rowset/bitshuffle_page.h"
#include "storage/rowset/dict_page.h"
#include "storage/rowset/frame_of_reference_page.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/plain_page.h"
#include "storage/rowset/rle_page.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/compression/block_compression.h"
#include "util/json.h"
#include "util/json_flattener.h"
#include "util/slice.h"

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

    EXPECT_EQ(param_has_remain, jf.has_remain_json());
    EXPECT_EQ(param_flat_path, path);
    EXPECT_EQ(param_flat_type, type);
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(JsonPathDeriverCases, JsonPathDeriverTest,
    ::testing::Values(
        // NORMAL
        std::make_tuple(R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3, "k2": 4} )", false, std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_BIGINT}),
        std::make_tuple(R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3} )", true, std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_BIGINT}),
        std::make_tuple(R"( {"k1": 1, "k2": 2} )", R"( {"k1": 3, "k3": 4} )", true, std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_BIGINT}),

        // EMPTY
        std::make_tuple(R"( {"k1": 1, "k2": {}} )", R"( {"k1": 3, "k2": {}} )", false, std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_JSON}),
        std::make_tuple(R"( {"k1": 1, "k2": {}} )", R"( {"k1": 3, "k2": {"k3": 123}} )", true, std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_JSON}),
        std::make_tuple(R"( {} )", R"( {"k1": 3} )", true, std::vector<std::string> {}, std::vector<LogicalType> {}),
        std::make_tuple(R"( {"": 123} )", R"( {"": 234} )", true, std::vector<std::string> {}, std::vector<LogicalType> {}),

        // DEEP
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": 2}} )", R"( {"k2": {"j1": 3, "j2": 4}} )", false, std::vector<std::string> {"k2.j1", "k2.j2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_BIGINT}),
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": 2}} )", R"( {"k2": {"j1": 3, "j3": 4}} )", true, std::vector<std::string> {"k2.j1"}, std::vector<LogicalType> {TYPE_BIGINT}),
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": 2}} )", R"( {"k2": {"j1": 3, "j2": {"p1": "abc"}}} )", true, std::vector<std::string> {"k2.j1", "k2.j2"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_JSON}),
        std::make_tuple(R"( {"k2": {"j1": 1, "j2": {"p1": [1,2,3,4]}}} )", R"( {"k2": {"j1": 3, "j2": {"p1": "abc"}}} )", false, std::vector<std::string> {"k2.j1", "k2.j2.p1"}, std::vector<LogicalType> {TYPE_BIGINT, TYPE_JSON})
));
// clang-format on

class JsonFlattenerTest : public testing::Test {
public:
    JsonFlattenerTest() = default;

    ~JsonFlattenerTest() override = default;

protected:
    void SetUp() override {}

    void TearDown() override {
        config::json_flat_sparsity_factor = 0.9;
        config::json_flat_null_factor = 0.3;
    }

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
        jf.set_generate_filter(true);
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a10", "a19", "b20", "c18", "e17", "g14", "h12", "k11",
                                          "k13", "k15", "k6",  "k7",  "k8",  "k9",  "z16"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
        ASSERT_TRUE(nullptr != jf.remain_fitler());
        EXPECT_FALSE(jf.remain_fitler()->test_bytes("b20", 3));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("k5", 2));
    }

    {
        config::json_flat_sparsity_factor = 0.4;
        JsonPathDeriver jf;
        jf.set_generate_filter(true);
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a10", "a19", "b20", "c18", "e17", "g14", "h12",
                                          "k11", "k13", "k15", "k8",  "k9",  "z16"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
        ASSERT_TRUE(nullptr != jf.remain_fitler());
        EXPECT_FALSE(jf.remain_fitler()->test_bytes("b20", 3));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("k5", 2));
    }

    {
        config::json_flat_sparsity_factor = 0.8;
        JsonPathDeriver jf;
        jf.set_generate_filter(true);
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a19", "b20", "c18", "e17", "z16"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
        ASSERT_TRUE(nullptr != jf.remain_fitler());
        EXPECT_FALSE(jf.remain_fitler()->test_bytes("b20", 3));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("k9", 2));
    }

    {
        config::json_flat_sparsity_factor = 0;
        JsonPathDeriver jf;
        jf.set_generate_filter(true);
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"a10", "a19", "b20", "c18", "e17", "g14", "h12", "k1", "k11", "k13",
                                          "k15", "k2",  "k3",  "k4",  "k5",  "k6",  "k7",  "k8", "k9",  "z16"};
        EXPECT_EQ(false, jf.has_remain_json());
        EXPECT_EQ(paths, result);
        ASSERT_FALSE(nullptr != jf.remain_fitler());
    }
}

TEST_F(JsonFlattenerTest, testRemainFilter) {
    // clang-format off
    std::vector<std::string> jsons = {
    R"({"K1": 123, "K2": "some", "K3": {"f1": "valu1", "n2": 456},              "K4": [true, "abc", 789],     "K5": {"nf": {"s1": "text", "subfield2": 123, "subfield3": ["a", "b", "c"]}}})",
    R"({"K1": 456, "K2": "anor", "K3": {"f1": "valu3", "n4": 789},              "K4": [false, "def", 101112], "K6": {"nf": {"s1": 789, "subfield2": "text", "subfield3": [1, 2, 3]}}})",
    R"({"K1": 789, "K2": "yete", "K3": {"f1": 1011122, "n6": "nested_value6"},  "K4": [true, "xyz", 131415],  "K7": {"nf": {"s1": "text", "subfield2": ["x", "y", "z"], "subfield3": 456}}})",
    R"({"K1": 101, "K2": "onee", "K3": {"f1": "valu7", "n8": ["a", "b", "c"]},  "K4": [false, "uvw", 789],    "K8": {"nf": {"s1": 101112, "subfield2": "text", "subfield3": 123}}})",
    R"({"K1": 131, "K2": "fine", "K3": {"f1": 7892342, "n1": "nested_value10"}, "K4": [true, "pqr", 456],     "K9": {"nf": {"s1": ["p", "q", "r"], "subfield2": 789, "subfield3": "text"}}})",
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
        jf.set_generate_filter(true);
        jf.derived({json_input});

        auto& result = jf.flat_paths();
        std::vector<std::string> paths = {"K1", "K2", "K3.f1", "K4"};
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ(paths, result);
        ASSERT_TRUE(nullptr != jf.remain_fitler());
        for (const auto& pp : paths) {
            EXPECT_FALSE(jf.remain_fitler()->test_bytes(pp.data(), pp.size()));
        }
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("K5", 2));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("n2", 2));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("K3", 2));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("K9", 2));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("K6", 2));
        EXPECT_TRUE(jf.remain_fitler()->test_bytes("subfield2", 9));
    }
}

TEST_F(JsonFlattenerTest, testPointJson) {
    // clang-format off
    std::vector<std::string> jsons = {
    R"( {"k1.k2.k3.k4": 1, "k2": 2} )",
    R"( {"k1.k2.k3.k4": 3, "k2": 4} )"
    };
    // clang-format on

    ColumnPtr input = JsonColumn::create();
    JsonColumn* json_input = down_cast<JsonColumn*>(input.get());
    for (const auto& json : jsons) {
        ASSIGN_OR_ABORT(auto json_value, JsonValue::parse(json));
        json_input->append(&json_value);
    }
    JsonPathDeriver jf;
    jf.derived({json_input});

    auto& result = jf.flat_paths();
    std::vector<std::string> paths = {"k2"};
    EXPECT_EQ(true, jf.has_remain_json());
    EXPECT_EQ(paths, result);

    std::vector<LogicalType> types = {TYPE_BIGINT};
    auto result_col = test_json(jsons, paths, types, false);
    EXPECT_EQ("2", result_col[0]->debug_item(0));
    EXPECT_EQ("4", result_col[0]->debug_item(1));
}

TEST_P(JsonFlattenerTest, testClean) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();
    for (int k = 0; k < 5; k++) {
        vpack::Builder builder;
        builder.openObject(true);
        for (int j = 0; j < 500; j++) {
            builder.add("fixkey", vpack::Value("fixvalue" + std::to_string(j)));
            builder.add("key" + std::to_string(j), vpack::Value("value" + std::to_string(j)));
        }
        builder.close();

        JsonValue jv;
        jv.assign(builder);
        json_column->append(&jv);
    }

    std::vector<const Column*> columns{json_column.get()};
    {
        config::vector_chunk_size = 1;
        JsonPathDeriver jf;
        jf.derived(columns);
        jf.set_generate_filter(true);
        config::vector_chunk_size = 4096;
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ({"fixkey"}, jf.flat_paths());
        EXPECT_EQ({TYPE_VARCHAR}, jf.flat_types());
        EXPECT_EQ(nullptr, jf.remain_fitler());
    }
}

TEST_P(JsonFlattenerTest, testComplexJsonExtract) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();

    // clang-format off
    std::vector<std::string> jsons = {
        R"({"K1": 123, "K2": "some", "K5": {"nf": {"s1": "text", "subfield2": 123, "subfield3": ["a", "b", "c"]}}})",
        R"({"K1": 456, "K2": "anor", "K6": {"nf": {"s1": 789, "subfield2": "text", "subfield3": [1, 2, 3]}}})",
        R"({"K1": 789, "K2": "yete", "K7": {"nf": {"s1": "text", "subfield2": ["x", "y", "z"], "subfield3": 456}}})",
        R"({"K1": 101, "K2": "onee", "K8": {"nf": {"s1": 101112, "subfield2": "text", "subfield3": 123}}})",
        R"({"K1": 131, "K2": "fine", "K9": {"nf": {"s1": ["p", "q", "r"], "subfield2": 789, "subfield3": "text"}}})",
    };
    // clang-format on

    for (auto& str : jsons) {
        ASSIGN_OR_ABORT(auto json_value, JsonValue::parse(str));
        json_column->append(&json_value);
    }
    std::vector<const Column*> columns{json_column.get()};

    {
        JsonPathDeriver jf;
        jf.derived(columns);
        EXPECT_EQ(true, jf.has_remain_json());
        EXPECT_EQ({"asdf"}, jf.flat_paths());
        EXPECT_EQ({"asdf"}, jf.flat_types());
    }
    {}
}

static std::string string_2_asc(const std::string& input) {
    std::stringstream oss;
    oss << "'";
    for (char c : input) {
        // if (c == '\n') {
        //     oss << "\\n";
        // } else if (c == '\t') {
        //     oss << "\\t";
        // } else if (std::isprint(static_cast<unsigned char>(c))) {
        //     oss << c;
        // } else {
        //     oss << "0x" << std::hex << (static_cast<unsigned int>(c) & 0xFF);
        // }

        oss << "0X" << std::hex << (static_cast<unsigned int>(c) & 0xFF);
    }
    oss << "'";
    return oss.str();
}

static std::string uint8_2_asc(const uint8_t* input, size_t len) {
    std::stringstream oss;
    oss << "'";
    for (size_t i = 0; i < len; i++) {
        // if (c == '\n') {
        //     oss << "\\n";
        // } else if (c == '\t') {
        //     oss << "\\t";
        // } else if (std::isprint(static_cast<unsigned char>(c))) {
        //     oss << c;
        // } else {
        //     oss << "0x" << std::hex << (static_cast<unsigned int>(c) & 0xFF);
        // }

        oss << "0X" << std::hex << (static_cast<unsigned int>(input[i]) & 0xFF);
    }
    oss << "'";
    return oss.str();
}

static StatusOr<size_t> get_compress_size(CompressionTypePB type, const std::vector<Slice>& slices,
                                          int compress_level) {
    const BlockCompressionCodec* _compress_codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(type, &_compress_codec, compress_level));

    faststring compressed_body;
    RETURN_IF_ERROR(PageIO::compress_page_body(_compress_codec, 0.1, slices, &compressed_body));

    size_t x = compressed_body.size();
    compressed_body.clear();
    return x;
}

static Status get_compress_size2(CompressionTypePB type, const std::vector<Slice>& slices, int compress_level,
                                 faststring* compressed_body) {
    const BlockCompressionCodec* _compress_codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(type, &_compress_codec, compress_level));
    RETURN_IF_ERROR(PageIO::compress_page_body(_compress_codec, 0.1, slices, compressed_body));
    return Status::OK();
}

TEST_F(JsonFlattenerTest, testSizeJson) {
    std::string json1 = R"({"Bool": false, "arr": [10, 20, 30]})";
    std::string json2 = R"(false)";
    std::string json3 = R"([10, 20, 30])";

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(json1));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse(json2));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse(json3));

    ColumnPtr col1 = JsonColumn::create();
    ColumnPtr col2 = JsonColumn::create();
    ColumnPtr col3 = JsonColumn::create();

    // std::vector<Slice>

    for (int i = 0; i < 3616768; i++) {
        down_cast<JsonColumn*>(col1.get())->append(&jv1);
        down_cast<JsonColumn*>(col2.get())->append(&jv2);
        down_cast<JsonColumn*>(col3.get())->append(&jv3);
    }

    auto rd1 = col1->raw_data();
    auto rd2 = col2->raw_data();
    auto rd3 = col3->raw_data();

    const auto* sl1 = reinterpret_cast<const Slice*>(rd1);
    const auto* sl2 = reinterpret_cast<const Slice*>(rd2);
    const auto* sl3 = reinterpret_cast<const Slice*>(rd3);

    std::vector<Slice> slices1;
    std::vector<Slice> slices2;
    std::vector<Slice> slices3;

    size_t uncompress1 = 0;
    size_t uncompress2 = 0;
    size_t uncompress3 = 0;

    for (int i = 0; i < 3616768; i++) {
        slices1.push_back(sl1[i]);
        slices2.push_back(sl2[i]);
        slices3.push_back(sl3[i]);

        uncompress1 += sl1[i].size;
        uncompress2 += sl2[i].size;
        uncompress3 += sl3[i].size;
    }

    LOG(INFO) << "sl1 " << sl1[0].size << " sl2 " << sl2[0].size << " sl3 " << sl3[0].size;

    LOG(INFO) << "sl1 " << string_2_asc(sl1[0].to_string()) << " sl2 " << string_2_asc(sl2[0].to_string()) << " sl3 "
              << string_2_asc(sl3[0].to_string());

    LOG(INFO) << "col1 " << col1->byte_size() << " col2 " << col2->byte_size() << " col3 " << col3->byte_size();

    LOG(INFO) << "uncompress1[" << uncompress1 << "], uncompress2[" << uncompress2 << "], uncompress3[" << uncompress3
              << "]";

    std::vector<CompressionTypePB> compress_types{CompressionTypePB::SNAPPY,    CompressionTypePB::LZ4,
                                                  CompressionTypePB::LZ4_FRAME, CompressionTypePB::ZLIB,
                                                  CompressionTypePB::ZSTD,      CompressionTypePB::GZIP};

    for (auto type : compress_types) {
        ASSIGN_OR_ABORT(size_t compress1, get_compress_size(type, slices1, 3));
        ASSIGN_OR_ABORT(size_t compress2, get_compress_size(type, slices2, 3));
        ASSIGN_OR_ABORT(size_t compress3, get_compress_size(type, slices3, 3));
        LOG(INFO) << "Compression[" << type << "], compress1[" << compress1 << "], compress2[" << compress2
                  << "], compress3[" << compress3 << "]";
    }
    /*
    sl1 23 sl2 6 sl3 8
    sl1 '0xb0x170x2DBool0x19Carr0x20x8(\n(0x14(0x1e0x3\t' sl2 'Efalse' sl3 '0x20x8(\n(0x14(0x1e'
    col1 83185664 col2 21700608 col3 28934144
                  uncompress1[83185664], uncompress2[21700608], uncompress3[28934144]
    Compression[3], compress1[3929812],    compress2[1019544],    compress3[1360270]
    Compression[4], compress1[326252],     compress2[85116],      compress3[113485]
    Compression[5], compress1[330383],     compress2[86196],      compress3[114928]
    Compression[6], compress1[201825],     compress2[31641],      compress3[42170]
    Compression[7], compress1[7648],       compress2[2003],       compress3[2665]
    Compression[8], compress1[201837],     compress2[31653],      compress3[42182]
*/
}

static size_t addPage(const uint8_t* rd1, std::vector<Slice>& slices1, ObjectPool& pool) {
    PageBuilderOptions opts;
    opts.data_page_size = config::data_page_size;
    auto pb1 = std::make_unique<BinaryPlainPageBuilder>(opts);

    size_t size = 0;
    size_t remaing = 3616768;
    while (remaing > 0) {
        size_t num_written = pb1->add(rd1, remaing);
        rd1 += sizeof(Slice) * num_written;
        remaing -= num_written;

        if (num_written < remaing) {
            faststring* ev1 = pb1->finish();
            size += ev1->size();
            faststring* co = pool.add(new faststring());
            ev1->swap(*co);
            slices1.emplace_back(*co);
            pb1->reset();
        }
    }

    faststring* ev1 = pb1->finish();
    size += ev1->size();
    faststring* co = pool.add(new faststring());
    ev1->swap(*co);
    slices1.emplace_back(*co);

    return size;
}

TEST_F(JsonFlattenerTest, testSizeJson2) {
    std::string json1 = R"({"Bool": false, "arr": [10, 20, 30]})";
    std::string json2 = R"(false)";
    std::string json3 = R"([10, 20, 30])";

    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(json1));
    ASSIGN_OR_ABORT(auto jv2, JsonValue::parse(json2));
    ASSIGN_OR_ABORT(auto jv3, JsonValue::parse(json3));

    ColumnPtr col1 = JsonColumn::create();
    ColumnPtr col2 = JsonColumn::create();
    ColumnPtr col3 = JsonColumn::create();

    // std::vector<Slice>

    for (int i = 0; i < 3616768; i++) {
        down_cast<JsonColumn*>(col1.get())->append(&jv1);
        down_cast<JsonColumn*>(col2.get())->append(&jv2);
        down_cast<JsonColumn*>(col3.get())->append(&jv3);
    }

    auto rd1 = col1->raw_data();
    auto rd2 = col2->raw_data();
    auto rd3 = col3->raw_data();

    std::vector<Slice> slices1;
    std::vector<Slice> slices2;
    std::vector<Slice> slices3;

    ObjectPool pool;
    size_t uncompress1 = addPage(rd1, slices1, pool);
    size_t uncompress2 = addPage(rd2, slices2, pool);
    size_t uncompress3 = addPage(rd3, slices3, pool);

    LOG(INFO) << "encoded1[" << uncompress1 << "], encoded2[" << uncompress2 << "], encoded3[" << uncompress3 << "]";

    std::vector<CompressionTypePB> compress_types{CompressionTypePB::SNAPPY,    CompressionTypePB::LZ4,
                                                  CompressionTypePB::LZ4_FRAME, CompressionTypePB::ZLIB,
                                                  CompressionTypePB::ZSTD,      CompressionTypePB::GZIP};

    for (auto type : compress_types) {
        ASSIGN_OR_ABORT(size_t compress1, get_compress_size(type, slices1, 3));
        ASSIGN_OR_ABORT(size_t compress2, get_compress_size(type, slices2, 3));
        ASSIGN_OR_ABORT(size_t compress3, get_compress_size(type, slices3, 3));
        LOG(INFO) << "Compression[" << type << "], compress1[" << compress1 << "], compress2[" << compress2
                  << "], compress3[" << compress3 << "]";
    }
    /*
                  uncompress1[83185664], uncompress2[21700608], uncompress3[28934144]
    Compression[3], compress1[3929812],    compress2[1019544],    compress3[1360270]
    Compression[4], compress1[326252],     compress2[85116],      compress3[113485]
    Compression[5], compress1[330383],     compress2[86196],      compress3[114928]
    Compression[6], compress1[201825],     compress2[31641],      compress3[42170]
    Compression[7], compress1[7648],       compress2[2003],       compress3[2665]
    Compression[8], compress1[201837],     compress2[31653],      compress3[42182]

                  uncompress1[83185664], uncompress2[21700608], uncompress3[28934144]
                     encoded1[57871820],    encoded2[57871820],    encoded3[57871820]
    Compression[3], compress1[18430381],   compress2[18188905],   compress3[17995585]
    Compression[4], compress1[14802162],   compress2[14558656],   compress3[14478402]
    Compression[5], compress1[14804377],   compress2[14560871],   compress3[14480623]
    Compression[6], compress1[6946753],    compress2[6032825],    compress3[5190669]
    Compression[7], compress1[4359341],    compress2[3864812],    compress3[2711603]
    Compression[8], compress1[6946765],    compress2[6032837],    compress3[5190681]
*/
}

static void test_json_compress(std::string json_str, CompressionTypePB type) {
    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(json_str));

    ColumnPtr col1 = JsonColumn::create();
    for (int i = 0; i < 3616768; i++) {
        down_cast<JsonColumn*>(col1.get())->append(&jv1);
    }

    auto rd1 = col1->raw_data();

    const auto* sl1 = reinterpret_cast<const Slice*>(rd1);
    size_t uncompress1 = 0;

    for (int i = 0; i < 3616768; i++) {
        uncompress1 += sl1[i].size;
    }

    PageBuilderOptions opts;
    opts.data_page_size = config::data_page_size;
    auto pb1 = std::make_unique<BinaryDictPageBuilder>(opts);

    size_t encode_size = 0;
    size_t compress_size = 0;
    size_t remaing = 3616768;
    while (remaing > 0) {
        size_t num_written = pb1->add(rd1, remaing);
        if (num_written < remaing) {
            faststring* ev1 = pb1->finish();
            encode_size += ev1->size();
            std::vector<Slice> slices1;
            slices1.emplace_back(*ev1);

            ASSIGN_OR_ABORT(size_t compress1, get_compress_size(type, slices1, 3));
            compress_size += compress1;
            pb1->reset();
        }

        rd1 += sizeof(Slice) * num_written;
        remaing -= num_written;
    }

    LOG(INFO) << "uncopmress[" << uncompress1 << "], encoded1[" << encode_size << "], compressType[" << type
              << "], compress[" << compress_size << "]";
}

TEST_F(JsonFlattenerTest, testSizeJson3) {
    std::vector<CompressionTypePB> compress_types{CompressionTypePB::SNAPPY, CompressionTypePB::LZ4,
                                                  CompressionTypePB::ZSTD, CompressionTypePB::GZIP};
    for (auto type : compress_types) {
        test_json_compress(R"({"Bool": false, "arr": [10, 20, 30]})", type);
    }
    /*
data: R"({"Bool": false, "arr": [10, 20, 30]})"
uncopmress[83185664], encoded1[97618840], compressType[3], compress[18411485]
uncopmress[83185664], encoded1[97618840], compressType[4], compress[14873621]
uncopmress[83185664], encoded1[97618840], compressType[5], compress[14895956]
uncopmress[83185664], encoded1[97618840], compressType[6], compress[6015560]
uncopmress[83185664], encoded1[97618840], compressType[7], compress[9172240]
uncopmress[83185664], encoded1[97618840], compressType[8], compress[6033428]

uncopmress[83185664], encoded1[87120], compressType[3], compress[11660]
uncopmress[83185664], encoded1[87120], compressType[4], compress[9680]
uncopmress[83185664], encoded1[87120], compressType[7], compress[9900]
uncopmress[83185664], encoded1[87120], compressType[8], compress[11000]

*/
}
TEST_F(JsonFlattenerTest, testSizeJson31) {
    std::vector<CompressionTypePB> compress_types{CompressionTypePB::SNAPPY,    CompressionTypePB::LZ4,
                                                  CompressionTypePB::LZ4_FRAME, CompressionTypePB::ZLIB,
                                                  CompressionTypePB::ZSTD,      CompressionTypePB::GZIP};
    for (auto type : compress_types) {
        test_json_compress(R"(false)", type);
    }
    /*
data: R"(false)"
uncopmress[21700608], encoded1[36114744], compressType[3], compress[15470978]
uncopmress[21700608], encoded1[36114744], compressType[4], compress[14593235]
uncopmress[21700608], encoded1[36114744], compressType[5], compress[14601500]
uncopmress[21700608], encoded1[36114744], compressType[6], compress[4846045]
uncopmress[21700608], encoded1[36114744], compressType[7], compress[8726187]
uncopmress[21700608], encoded1[36114744], compressType[8], compress[4852657]

uncopmress[21700608], encoded1[87120], compressType[3], compress[11660]
uncopmress[21700608], encoded1[87120], compressType[4], compress[9680]
uncopmress[21700608], encoded1[87120], compressType[5], compress[12980]
uncopmress[21700608], encoded1[87120], compressType[6], compress[8360]
uncopmress[21700608], encoded1[87120], compressType[7], compress[9900]
uncopmress[21700608], encoded1[87120], compressType[8], compress[11000]
*/
}

TEST_F(JsonFlattenerTest, testSizeJson32) {
    std::vector<CompressionTypePB> compress_types{CompressionTypePB::SNAPPY,    CompressionTypePB::LZ4,
                                                  CompressionTypePB::LZ4_FRAME, CompressionTypePB::ZLIB,
                                                  CompressionTypePB::ZSTD,      CompressionTypePB::GZIP};
    for (auto type : compress_types) {
        test_json_compress(R"([10, 20, 30])", type);
    }
    /*
data: R"([10, 20, 30])"
uncopmress[28934144], encoded1[43392776], compressType[3], compress[15832392]
uncopmress[28934144], encoded1[43392776], compressType[4], compress[14634834]
uncopmress[28934144], encoded1[43392776], compressType[5], compress[14644764]
uncopmress[28934144], encoded1[43392776], compressType[6], compress[4082554]
uncopmress[28934144], encoded1[43392776], compressType[7], compress[8114796]
uncopmress[28934144], encoded1[43392776], compressType[8], compress[4090498]
*/
}

static void print_value(std::string str) {
    ASSIGN_OR_ABORT(auto jv1, JsonValue::parse(str));

    size_t length = 10000;

    ColumnPtr col1 = JsonColumn::create();
    for (int i = 0; i < length; i++) {
        down_cast<JsonColumn*>(col1.get())->append(&jv1);
    }

    auto rd1 = col1->raw_data();
    const auto* sl1 = reinterpret_cast<const Slice*>(rd1);
    size_t uncompress1 = 0;

    LOG(INFO) << "column bytes: " << col1->byte_size();
    LOG(INFO) << "decode value: " << string_2_asc(sl1[0].to_string());

    for (int i = 0; i < length; i++) {
        uncompress1 += sl1[i].size;
    }

    PageBuilderOptions opts;
    opts.data_page_size = config::data_page_size;
    auto pb1 = std::make_unique<BinaryPlainPageBuilder>(opts);

    size_t encode_size = 0;
    size_t compress_size = 0;
    size_t remaing = length;
    int i = 0;
    while (remaing > 0) {
        size_t num_written = pb1->add(rd1, remaing);
        if (num_written <= remaing) {
            faststring* ev1 = pb1->finish();
            encode_size += ev1->size();

            LOG(INFO) << "loop[" << i << "], num_written[" << num_written << "], ev1[" << ev1->size() << "]"
                      << ", encode_size: [" << encode_size << "]";

            if (i < 1) {
                LOG(INFO) << "encode value: " << string_2_asc(ev1->ToString());
            }

            std::vector<Slice> slices1;
            slices1.emplace_back(*ev1);

            faststring compress1;
            get_compress_size2(CompressionTypePB::LZ4_FRAME, slices1, 3, &compress1);
            compress_size += compress1.size();

            LOG(INFO) << "loop[" << i << "], compress1[" << compress1.size() << "], compress_size[" << compress_size
                      << "]";

            if (i < 1) {
                LOG(INFO) << "compress value: " << string_2_asc(compress1.ToString());
            }

            pb1->reset();
        }

        rd1 += sizeof(Slice) * num_written;
        remaing -= num_written;
        i++;
    }

    LOG(INFO) << "uncopmress[" << uncompress1 << "], encoded1[" << encode_size << "]";
}

TEST_F(JsonFlattenerTest, testSizeJson33) {
    print_value(R"(false)");
}

TEST_F(JsonFlattenerTest, testSizeJson34) {
    print_value(R"({"Bool": false, "arr": [10, 20, 30]})");
}
} // namespace starrocks
