// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/hash_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "exprs/vectorized/mock_vectorized_expr.h"

namespace starrocks::vectorized {
class HashFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(HashFunctionsTest, hashTest) {
    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("test1234567");

        columns.emplace_back(tc1);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::murmur_hash3_32(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);

        ASSERT_EQ(-1948194659, v->get_data()[0]);
    }

    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("test1234567");

        auto tc2 = BinaryColumn::create();
        tc2->append("asdf213");

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::murmur_hash3_32(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_INT>(result);

        ASSERT_EQ(-500290079, v->get_data()[0]);
    }

    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("test1234567");

        auto tc2 = BinaryColumn::create();
        tc2->append("asdf213");

        auto tc3 = ColumnHelper::create_const_null_column(1);

        columns.emplace_back(tc1);
        columns.emplace_back(tc2);
        columns.emplace_back(tc3);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::murmur_hash3_32(ctx.get(), columns).value();

        ASSERT_TRUE(result->is_null(0));
    }
}

TEST_F(HashFunctionsTest, emptyTest) {
    uint32_t h3 = 123456;

    BinaryColumn b;
    b.crc32_hash(&h3, 0, 1);
    ASSERT_EQ(123456, h3);
}

} // namespace starrocks::vectorized
