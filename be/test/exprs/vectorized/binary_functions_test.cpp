// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "exprs/vectorized/math_functions.h"
#include "exprs/vectorized/mock_vectorized_expr.h"

namespace starrocks::vectorized {

template <PrimitiveType TYPE>
ColumnPtr create_nullable_column() {
    return NullableColumn::create(RunTimeColumnType<TYPE>::create(), RunTimeColumnType<TYPE_NULL>::create());
}

class VectorizedBinaryNullableTest : public ::testing::Test {
public:
    FunctionContext* function_context = nullptr;
};

TEST_F(VectorizedBinaryNullableTest, arg1Null) {
    Columns columns;

    columns.emplace_back(create_nullable_column<TYPE_DOUBLE>());
    columns.emplace_back(create_nullable_column<TYPE_DOUBLE>());

    auto* arg1 = ColumnHelper::as_raw_column<NullableColumn>(columns[0]);
    auto* arg2 = ColumnHelper::as_raw_column<NullableColumn>(columns[1]);

    arg1->append_nulls(1);
    arg2->append_datum((double)1);

    auto result = MathFunctions::pow(function_context, columns).value();
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(VectorizedBinaryNullableTest, arg2Null) {
    Columns columns;

    columns.emplace_back(create_nullable_column<TYPE_DOUBLE>());
    columns.emplace_back(create_nullable_column<TYPE_DOUBLE>());

    auto* arg1 = ColumnHelper::as_raw_column<NullableColumn>(columns[0]);
    auto* arg2 = ColumnHelper::as_raw_column<NullableColumn>(columns[1]);

    arg1->append_datum((double)1);
    arg2->append_nulls(1);

    auto result = MathFunctions::pow(function_context, columns).value();
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(VectorizedBinaryNullableTest, allArgsNull) {
    Columns columns;

    columns.emplace_back(create_nullable_column<TYPE_DOUBLE>());
    columns.emplace_back(create_nullable_column<TYPE_DOUBLE>());

    auto* arg1 = ColumnHelper::as_raw_column<NullableColumn>(columns[0]);
    auto* arg2 = ColumnHelper::as_raw_column<NullableColumn>(columns[1]);

    arg1->append_nulls(1);
    arg2->append_nulls(1);

    auto result = MathFunctions::pow(function_context, columns).value();
    ASSERT_TRUE(result->is_null(0));
}
} // namespace starrocks::vectorized
