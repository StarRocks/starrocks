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

#include "exprs/agg/aggregate_factory.h"
#include "types/logical_type.h"

namespace starrocks {

TEST(AggregateFactoryCoreTest, builtinAggregateResolves) {
    const auto* func = get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, false);
    ASSERT_NE(nullptr, func);
}

TEST(AggregateFactoryCoreTest, builtinWindowResolves) {
    const auto* func = get_window_function("row_number", TYPE_BIGINT, TYPE_BIGINT, false);
    ASSERT_NE(nullptr, func);
}

TEST(AggregateFactoryCoreTest, dataSketchAggregatesResolve) {
    EXPECT_NE(nullptr, get_aggregate_function("ds_hll_count_distinct", TYPE_BIGINT, TYPE_BIGINT, false));
    EXPECT_NE(nullptr, get_aggregate_function("ds_theta_count_distinct", TYPE_DOUBLE, TYPE_BIGINT, false));
}

TEST(AggregateFactoryCoreTest, unsupportedNonBuiltinReturnsNullWithoutProvider) {
    const auto* func =
            get_aggregate_function("python_udaf", TYPE_BIGINT, TYPE_BIGINT, false, TFunctionBinaryType::PYTHON);
    EXPECT_EQ(nullptr, func);
}

} // namespace starrocks
