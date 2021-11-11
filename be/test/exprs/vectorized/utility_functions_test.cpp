// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/utility_functions.h"

#include <cctz/civil_time.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/column_helper.h"

namespace starrocks {
namespace vectorized {

class UtilityFunctionsTest : public ::testing::Test {
public:
    void SetUp() {}
};

TEST_F(UtilityFunctionsTest, versionTest) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // test version
    {
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(2, 1);

        Columns columns;
        columns.emplace_back(var1_col);

        ColumnPtr result = UtilityFunctions::version(ctx, columns);

        ASSERT_TRUE(result->is_constant());

        auto v = ColumnHelper::get_const_value<TYPE_VARCHAR>(result);

        ASSERT_EQ("5.1.0", v);
    }
}

TEST_F(UtilityFunctionsTest, sleepTest) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // test sleep
    {
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(1, 1);

        Columns columns;
        columns.emplace_back(var1_col);

        ColumnPtr result = UtilityFunctions::sleep(ctx, columns);

        ASSERT_EQ(1, result->size());

        auto v = ColumnHelper::get_const_value<TYPE_BOOLEAN>(result);
        ASSERT_TRUE(v);
    }
}

TEST_F(UtilityFunctionsTest, uuidTest) {
    FunctionContext* ctx = FunctionContext::create_test_context();
    auto ptr = std::unique_ptr<FunctionContext>(ctx);

    // test version
    {
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(2, 1);

        Columns columns;
        columns.emplace_back(var1_col);

        ColumnPtr result = UtilityFunctions::uuid(ctx, columns);

        ASSERT_TRUE(result->is_constant());

        auto v = ColumnHelper::get_const_value<TYPE_VARCHAR>(result);

        ASSERT_EQ(36, v.get_size());

        std::set<int> hyphens_position = {8, 13, 18, 23};

        for (int i = 0; i < v.get_size(); i++) {
            if (hyphens_position.count(i)) {
                ASSERT_EQ(v.get_data()[i], '-');
            } else {
                ASSERT_GE(v.get_data()[i], '0');
                ASSERT_LE(v.get_data()[i], 'f');
            }
        }
    }
}

} // namespace vectorized
} // namespace starrocks
