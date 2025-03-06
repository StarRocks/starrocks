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

#include "exprs/es_functions.h"

#include <cctz/civil_time.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"
#include "runtime/time_types.h"
#include "testutil/function_utils.h"

namespace starrocks {

class EsFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(EsFunctionsTest, matchTest) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // test esquery
    {
        auto var1_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("STRING"), 1);
        auto var2_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("STRING2"), 1);

        Columns columns;
        columns.emplace_back(std::move(var1_col));
        columns.emplace_back(std::move(var2_col));

        ColumnPtr result = ESFunctions::match(ctx, columns).value();

        ASSERT_EQ(1, result->size());
        ASSERT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(result));
    }
}

} // namespace starrocks
