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

#include "exprs/hash_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/array_column.h"
#include "exprs/mock_vectorized_expr.h"

namespace starrocks {
class HashFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {}
};

TEST_F(HashFunctionsTest, hashTest) {
    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("test1234567");

        columns.emplace_back(std::move(tc1));

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

        columns.emplace_back(std::move(tc1));
        columns.emplace_back(std::move(tc2));

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

        columns.emplace_back(std::move(tc1));
        columns.emplace_back(std::move(tc2));
        columns.emplace_back(std::move(tc3));

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::murmur_hash3_32(ctx.get(), columns).value();

        ASSERT_TRUE(result->is_null(0));
    }
}

TEST_F(HashFunctionsTest, test_xx_hash3_64) {
    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("hello");
        tc1->append("starrocks");
        columns.emplace_back(std::move(tc1));

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::xx_hash3_64(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(-7685981735718036227, v->get_data()[0]);
        ASSERT_EQ(6573472450560322992, v->get_data()[1]);
    }

    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("hello");
        tc1->append("hello");

        auto tc2 = BinaryColumn::create();
        tc2->append("world");
        tc2->append("starrocks");

        columns.emplace_back(std::move(tc1));
        columns.emplace_back(std::move(tc2));

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::xx_hash3_64(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(7001965798170371843, v->get_data()[0]);
        ASSERT_EQ(2803320466222626098, v->get_data()[1]);
    }

    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("hello");

        auto tc2 = ColumnHelper::create_const_null_column(1);

        auto tc3 = BinaryColumn::create();
        tc3->append("world");

        columns.emplace_back(std::move(tc1));
        columns.emplace_back(std::move(tc2));
        columns.emplace_back(std::move(tc3));

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::xx_hash3_64(ctx.get(), columns).value();

        ASSERT_TRUE(result->is_null(0));
    }
}

#define INT128_LITERAL(high, low) (((int128_t)high << 64) | (uint64_t)low)

TEST_F(HashFunctionsTest, test_xx_hash3_128) {
    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("hello");
        tc1->append("starrocks");
        columns.emplace_back(std::move(tc1));

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::xx_hash3_128(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_LARGEINT>(result);
        ASSERT_EQ(INT128_LITERAL(-5338522934378283393, -4072996057346066408), v->get_data()[0]);
        ASSERT_EQ(INT128_LITERAL(3846997910503780466, 1697546255957561686), v->get_data()[1]);
    }

    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("hello");
        tc1->append("hello");

        auto tc2 = BinaryColumn::create();
        tc2->append("world");
        tc2->append("starrocks");

        columns.emplace_back(std::move(tc1));
        columns.emplace_back(std::move(tc2));

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::xx_hash3_128(ctx.get(), columns).value();

        auto v = ColumnHelper::cast_to<TYPE_LARGEINT>(result);
        ASSERT_EQ(INT128_LITERAL(-2452210651042717451, 1087493910761260911), v->get_data()[0]);
        ASSERT_EQ(INT128_LITERAL(1559307639436096304, 8859976453967563600), v->get_data()[1]);
    }

    {
        Columns columns;
        auto tc1 = BinaryColumn::create();
        tc1->append("hello");

        auto tc2 = ColumnHelper::create_const_null_column(1);

        auto tc3 = BinaryColumn::create();
        tc3->append("world");

        columns.emplace_back(std::move(tc1));
        columns.emplace_back(std::move(tc2));
        columns.emplace_back(std::move(tc3));

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::xx_hash3_128(ctx.get(), columns).value();

        ASSERT_TRUE(result->is_null(0));
    }
}

TEST_F(HashFunctionsTest, emptyTest) {
    uint32_t h3 = 123456;

    BinaryColumn b;
    b.crc32_hash(&h3, 0, 1);
    ASSERT_EQ(123456, h3);
}

TEST_F(HashFunctionsTest, test_crc32_hash_array) {
    {
        Columns columns;
        auto data_column = Int32Column::create();
        auto offsets = UInt32Column::create();
        NullableColumn::Ptr elements = NullableColumn::create(data_column, NullColumn::create());
        ArrayColumn::Ptr arr = ArrayColumn::create(elements, offsets);

        data_column->append(1);
        data_column->append(2);
        data_column->append(3);
        offsets->append(3);

        data_column->append(4);
        data_column->append(5);
        data_column->append(6);
        offsets->append(6);

        columns.emplace_back(arr);

        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::crc32_hash(ctx.get(), columns).value();
        auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
        ASSERT_EQ(2, result->size());
        ASSERT_EQ(1799959227, v->get_data()[0]);
        ASSERT_EQ(3471948928, v->get_data()[1]);
    }

    {
        Columns columns;
        ColumnPtr only_null_column = ColumnHelper::create_const_null_column(5);
        columns.emplace_back(only_null_column);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = HashFunctions::crc32_hash(ctx.get(), columns).value();
        ASSERT_EQ(5, result->size());
        ASSERT_TRUE(result->only_null());
    }
}

} // namespace starrocks
