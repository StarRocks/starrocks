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
#include "util/json_flater.h"

namespace starrocks {

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

    Columns columns{json_column};
    JsonFlater jf;
    config::json_flat_internal_column_min_limit = 0;
    jf.derived_paths(columns);
    config::json_flat_internal_column_min_limit = 5;
    std::vector<std::string> path = jf.get_flat_paths();
    std::vector<LogicalType> type = jf.get_flat_types();

    ASSERT_EQ(param_flat_path, path);
    ASSERT_EQ(param_flat_type, type);
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(FlatJsonPathDeriver, FlatJsonDeriverPaths,
    ::testing::Values(
        std::make_tuple(R"({ "k1": 1, "k2": 2 })", R"({ "k1": 3, "k2": 4 })", std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_SMALLINT, TYPE_SMALLINT}),
        std::make_tuple(R"({ "k1": "v1" })",  R"({ "k1": "v33" })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_VARCHAR}),
        std::make_tuple(R"({ "k1": {"k2": 1} })",  R"({ "k1": 123 })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}),
        std::make_tuple(R"({ "k1": "v1" })",  R"({ "k1": 1.123 })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}),
        std::make_tuple(R"({ "k1": {"k2": 1} })", R"({ "k1": 1.123 })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}),
        std::make_tuple(R"({ "k1": [1,2,3] })", R"({ "k1": "v33" })", std::vector<std::string> {"k1"}, std::vector<LogicalType> {TYPE_JSON}),

        std::make_tuple(R"({ "k1": "v1", "k2": [3,4,5], "k3": 1, "k4": 1.2344 })",  
                        R"({ "k1": "abc", "k2": [11,123,54], "k3": 23423, "k4": 1.2344 })", 
                        std::vector<std::string> {"k3", "k4", "k1", "k2"}, 
                        std::vector<LogicalType> {TYPE_LARGEINT, TYPE_DOUBLE, TYPE_VARCHAR, TYPE_JSON}),
        std::make_tuple(R"({ "k1": 1, "k2": "a" })", R"({ "k1": 3, "k2": null })", std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_SMALLINT, TYPE_JSON}),
        std::make_tuple(R"({ "k1": 1, "k2": 2 })", R"({ "k1": 3, "k2": 4 })", std::vector<std::string> {"k1", "k2"}, std::vector<LogicalType> {TYPE_SMALLINT, TYPE_SMALLINT})

));
// clang-format on

} // namespace starrocks
