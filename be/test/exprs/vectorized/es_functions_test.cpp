// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/es_functions.h"

#include <cctz/civil_time.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"
#include "runtime/time_types.h"
#include "testutil/function_utils.h"

namespace starrocks::vectorized {

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
        columns.emplace_back(var1_col);
        columns.emplace_back(var2_col);

        ColumnPtr result = ESFunctions::match(ctx, columns).value();

        ASSERT_EQ(1, result->size());
        ASSERT_TRUE(ColumnHelper::get_const_value<TYPE_BOOLEAN>(result));
    }
}

} // namespace starrocks::vectorized
