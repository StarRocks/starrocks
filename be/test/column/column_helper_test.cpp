// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/column_helper.h"
#include "column/column_builder.h"

#include "gtest/gtest.h"

namespace starrocks::vectorized {

class ColumnHelperTest : public testing::Test {
public:
<<<<<<< HEAD
    virtual void SetUp() {}
    virtual void TearDown() {}
=======
    void SetUp() override {}
    void TearDown() override {}

protected:
    ColumnPtr create_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"));
        return builder.build(false);
    }

    ColumnPtr create_nullable_column() {
        ColumnBuilder<TYPE_VARCHAR> builder(1);
        builder.append(Slice("v1"), true);
        return builder.build(false);
    }
>>>>>>> c5d9e626b ([BugFix] Variable chunk's column types (#13629))
};

TEST_F(ColumnHelperTest, cast_to_nullable_column) {
    auto col = ColumnHelper::cast_to_nullable_column(create_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());

    col = ColumnHelper::cast_to_nullable_column(create_nullable_column());
    ASSERT_TRUE(col->is_nullable());
    ASSERT_FALSE(col->is_constant());
}

} // namespace starrocks::vectorized
