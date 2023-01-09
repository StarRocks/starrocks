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
#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "exprs/math_functions.h"
#include "exprs/mock_vectorized_expr.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class VectorizedBinaryNullableTest : public ::testing::Test {
public:
    FunctionContext* function_context = nullptr;
};

TEST_F(VectorizedBinaryNullableTest, arg1Null) {
    Columns columns;

    columns.emplace_back(ColumnTestHelper::create_nullable_column<TYPE_DOUBLE>());
    columns.emplace_back(ColumnTestHelper::create_nullable_column<TYPE_DOUBLE>());

    auto* arg1 = ColumnHelper::as_raw_column<NullableColumn>(columns[0]);
    auto* arg2 = ColumnHelper::as_raw_column<NullableColumn>(columns[1]);

    arg1->append_nulls(1);
    arg2->append_datum((double)1);

    auto result = MathFunctions::pow(function_context, columns).value();
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(VectorizedBinaryNullableTest, arg2Null) {
    Columns columns;

    columns.emplace_back(ColumnTestHelper::create_nullable_column<TYPE_DOUBLE>());
    columns.emplace_back(ColumnTestHelper::create_nullable_column<TYPE_DOUBLE>());

    auto* arg1 = ColumnHelper::as_raw_column<NullableColumn>(columns[0]);
    auto* arg2 = ColumnHelper::as_raw_column<NullableColumn>(columns[1]);

    arg1->append_datum((double)1);
    arg2->append_nulls(1);

    auto result = MathFunctions::pow(function_context, columns).value();
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(VectorizedBinaryNullableTest, allArgsNull) {
    Columns columns;

    columns.emplace_back(ColumnTestHelper::create_nullable_column<TYPE_DOUBLE>());
    columns.emplace_back(ColumnTestHelper::create_nullable_column<TYPE_DOUBLE>());

    auto* arg1 = ColumnHelper::as_raw_column<NullableColumn>(columns[0]);
    auto* arg2 = ColumnHelper::as_raw_column<NullableColumn>(columns[1]);

    arg1->append_nulls(1);
    arg2->append_nulls(1);

    auto result = MathFunctions::pow(function_context, columns).value();
    ASSERT_TRUE(result->is_null(0));
}
} // namespace starrocks
