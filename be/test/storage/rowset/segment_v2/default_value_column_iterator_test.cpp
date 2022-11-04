// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/rowset/segment_v2/default_value_column_iterator.h"

#include "column/column_helper.h"
#include "gtest/gtest.h"
#include "storage/rowset/segment_v2/column_iterator.h"
#include "storage/types.h"
#include "storage/vectorized/column_predicate.h"

namespace starrocks::segment_v2 {
class DefaultValueColumnIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// NOLINTNEXTLINE
TEST_F(DefaultValueColumnIteratorTest, delete_after_column) {
    TypeInfoPtr type_info = get_type_info(OLAP_FIELD_TYPE_INT);
    DefaultValueColumnIterator iter(false, "", true, type_info, 0, 10);

    ColumnIteratorOptions opts;
    Status st = iter.init(opts);
    ASSERT_TRUE(st.ok());

    std::vector<const vectorized::ColumnPredicate*> preds;
    std::unique_ptr<vectorized::ColumnPredicate> del_pred(vectorized::new_column_null_predicate(type_info, 1, true));
    vectorized::SparseRange row_ranges;
    st = iter.get_row_ranges_by_zone_map(preds, del_pred.get(), &row_ranges);
    ASSERT_TRUE(st.ok());

    TypeDescriptor type_desc(PrimitiveType::TYPE_INT);
    vectorized::ColumnPtr column = vectorized::ColumnHelper::create_column(type_desc, true);

    size_t num_rows = 10;
    st = iter.next_batch(&num_rows, column.get());
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(column->delete_state(), DEL_PARTIAL_SATISFIED);
    ASSERT_EQ(num_rows, 10);
    ASSERT_EQ(column->size(), 10);
    for (size_t i = 0; i < 10; i++) {
        ASSERT_TRUE(column->is_null(i));
    }
}

} // namespace starrocks::segment_v2