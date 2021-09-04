// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/nullable_column.h"

#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "testutil/parallel_test.h"

namespace starrocks::vectorized {

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_copy_constructor) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(*c0);
    c0->reset_column();

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_move_constructor) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(std::move(*c0));

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_copy_assignment) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(Int32Column::create(), NullColumn::create());
    c1 = *c0;
    c0->reset_column();

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_move_assignment) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    c0->append_datum({}); // NULL
    c0->append_datum((int32_t)1);
    c0->append_datum((int32_t)2);
    c0->append_datum((int32_t)3);

    NullableColumn c1(Int32Column::create(), NullColumn::create());
    c1 = *c0;

    ASSERT_EQ(4, c1.size());
    ASSERT_TRUE(c1.data_column().unique());
    ASSERT_TRUE(c1.null_column().unique());
    ASSERT_EQ(4, c1.data_column()->size());
    ASSERT_EQ(4, c1.null_column()->size());
    ASSERT_TRUE(c1.get(0).is_null());
    ASSERT_EQ(1, c1.get(1).get_int32());
    ASSERT_EQ(2, c1.get(2).get_int32());
    ASSERT_EQ(3, c1.get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_clone) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto c1 = c0->clone();
    ASSERT_TRUE(c1->is_nullable());
    ASSERT_EQ(0, c1->size());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get()) != nullptr);
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->data_column().unique());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->null_column().unique());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->data_column()->size());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->null_column()->size());

    c1->append_datum({}); // NULL
    c1->append_datum({(int32_t)1});
    c1->append_datum({(int32_t)2});
    c1->append_datum({(int32_t)3});

    auto c2 = c1->clone();
    c1->reset_column();

    ASSERT_TRUE(c2->is_nullable());
    ASSERT_EQ(4, c2->size());
    ASSERT_EQ(4, down_cast<NullableColumn*>(c2.get())->data_column()->size());
    ASSERT_EQ(4, down_cast<NullableColumn*>(c2.get())->null_column()->size());
    ASSERT_TRUE(c2->get(0).is_null());
    ASSERT_EQ(1, c2->get(1).get_int32());
    ASSERT_EQ(2, c2->get(2).get_int32());
    ASSERT_EQ(3, c2->get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_clone_shared) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto c1 = c0->clone_shared();
    ASSERT_TRUE(c1.unique());
    ASSERT_TRUE(c1->is_nullable());
    ASSERT_EQ(0, c1->size());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c1) != nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c1)->data_column().unique());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c1)->null_column().unique());
    ASSERT_EQ(0, std::dynamic_pointer_cast<NullableColumn>(c1)->data_column()->size());
    ASSERT_EQ(0, std::dynamic_pointer_cast<NullableColumn>(c1)->null_column()->size());

    c1->append_datum({}); // NULL
    c1->append_datum({(int32_t)1});
    c1->append_datum({(int32_t)2});
    c1->append_datum({(int32_t)3});

    auto c2 = c1->clone_shared();
    c1->reset_column();

    ASSERT_TRUE(c2.unique());
    ASSERT_TRUE(c2->is_nullable());
    ASSERT_EQ(4, c2->size());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c2) != nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c2)->data_column().unique());
    ASSERT_TRUE(std::dynamic_pointer_cast<NullableColumn>(c2)->null_column().unique());
    ASSERT_EQ(4, std::dynamic_pointer_cast<NullableColumn>(c2)->data_column()->size());
    ASSERT_EQ(4, std::dynamic_pointer_cast<NullableColumn>(c2)->null_column()->size());
    ASSERT_TRUE(c2->get(0).is_null());
    ASSERT_EQ(1, c2->get(1).get_int32());
    ASSERT_EQ(2, c2->get(2).get_int32());
    ASSERT_EQ(3, c2->get(3).get_int32());
}

// NOLINTNEXTLINE
PARALLEL_TEST(NullableColumnTest, test_clone_empty) {
    auto c0 = NullableColumn::create(Int32Column::create(), NullColumn::create());

    auto c1 = c0->clone_empty();
    ASSERT_TRUE(c1->is_nullable());
    ASSERT_EQ(0, c1->size());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get()) != nullptr);
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->data_column().unique());
    ASSERT_TRUE(down_cast<NullableColumn*>(c1.get())->null_column().unique());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->data_column()->size());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c1.get())->null_column()->size());

    c1->append_datum({}); // NULL
    c1->append_datum({(int32_t)1});
    c1->append_datum({(int32_t)2});
    c1->append_datum({(int32_t)3});

    auto c2 = c1->clone_empty();

    ASSERT_TRUE(c2->is_nullable());
    ASSERT_EQ(0, c2->size());
    ASSERT_TRUE(down_cast<NullableColumn*>(c2.get()) != nullptr);
    ASSERT_TRUE(down_cast<NullableColumn*>(c2.get())->data_column().unique());
    ASSERT_TRUE(down_cast<NullableColumn*>(c2.get())->null_column().unique());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c2.get())->data_column()->size());
    ASSERT_EQ(0, down_cast<NullableColumn*>(c2.get())->null_column()->size());
}

} // namespace starrocks::vectorized