// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/utility_functions.h"

#include <cctz/civil_time.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "util/random.h"
#include "util/time.h"

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

    // test uuid
    {
        int column_size = static_cast<int>(Random(GetCurrentTimeNanos()).Uniform(10) + 1);
        auto var1_col = ColumnHelper::create_const_column<TYPE_INT>(column_size, column_size);

        Columns columns;
        columns.emplace_back(var1_col);

        ColumnPtr result = UtilityFunctions::uuid(ctx, columns);

        ASSERT_FALSE(result->is_constant());

        std::set<int> hyphens_position = {8, 13, 18, 23};
        std::set<std::string> deduplication;
        ColumnViewer<TYPE_VARCHAR> column_viewer(result);

        ASSERT_EQ(column_viewer.size(), column_size);

        for (int column_idx = 0; column_idx < column_viewer.size(); column_idx++) {
            auto& column = column_viewer.value(column_idx);
            ASSERT_EQ(36, column.get_size());
            deduplication.insert(column.to_string());

            for (int i = 0; i < column.get_size(); i++) {
                if (hyphens_position.count(i)) {
                    ASSERT_EQ(column.get_data()[i], '-');
                } else {
                    ASSERT_GE(column.get_data()[i], '0');
                    ASSERT_LE(column.get_data()[i], 'f');
                }
            }
        }

        ASSERT_EQ(deduplication.size(), column_size);
    }
}

} // namespace vectorized
} // namespace starrocks
