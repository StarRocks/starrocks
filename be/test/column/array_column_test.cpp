// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/array_column.h"

#include <gtest/gtest.h>

#include <cstdint>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "testutil/parallel_test.h"

namespace starrocks::vectorized {

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_create) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);
    ASSERT_TRUE(column->is_array());
    ASSERT_FALSE(column->is_nullable());
    ASSERT_EQ(0, column->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_get_elements) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    ASSERT_EQ("[1, 2, 3]", column->debug_item(0));
    ASSERT_EQ("[4, 5, 6]", column->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_byte_size) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    ASSERT_EQ(2, column->size());

    // elements has six element, with 24 bytes.
    // offsets has three element, with 12 bytes.
    ASSERT_EQ(36, column->byte_size());
    // elements 0 with 12 bytes.
    // offset 0 with 4 bytes.
    ASSERT_EQ(16, column->byte_size(0, 1));
    ASSERT_EQ(16, column->byte_size(0));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_filter) {
    // ARRAY<INT>
    {
        auto column = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                          UInt32Column::create());
        // The width of AVX2 register is 256 bits, aka 32 bytes, make the column size equals
        // to 2 * 32 + 31 in order to cover both the SIMD instructions and non-SIMD instructions.
        const int N = 2 * 32 + 31;
        for (int32_t i = 0; i < N; i++) {
            column->append_datum(DatumArray{i, i * 2});
        }

        Column::Filter filter(N, 1);

        column->filter_range(filter, 0, N);
        column->filter_range(filter, N / 10, N);
        column->filter_range(filter, N / 5, N);
        column->filter_range(filter, N / 2, N);
        ASSERT_EQ(N, column->size()) << column->debug_string();
        for (int i = 0; i < N; i++) {
            auto array = column->get(i).get_array();
            ASSERT_EQ(2, array.size());
            ASSERT_EQ(i, array[0].get_int32());
            ASSERT_EQ(i * 2, array[1].get_int32());
        }

        filter.clear();
        filter.resize(N, 0);
        for (int i = N - 40; i < N; i++) {
            filter[i] = (i % 2 == 1);
        }
        column->filter_range(filter, N - 40, N);
        ASSERT_EQ(N - 20, column->size()) << column->debug_string();
        // First N-40 elements should keep unchanged.
        for (int i = 0; i < N - 40; i++) {
            auto array = column->get(i).get_array();
            ASSERT_EQ(2, array.size());
            ASSERT_EQ(i, array[0].get_int32());
            ASSERT_EQ(i * 2, array[1].get_int32());
        }
        // Check the last 5 elements.
        int j = 0;
        for (int i = N - 40; i < N; i++) {
            if (i % 2 == 1) {
                auto array = column->get(N - 40 + j).get_array();
                ASSERT_EQ(2, array.size());
                ASSERT_EQ(i, array[0].get_int32());
                ASSERT_EQ(i * 2, array[1].get_int32());
                j++;
            }
        }

        // Remove the last 20 elements.
        filter.clear();
        filter.resize(column->size(), 0);
        column->filter_range(filter, N - 40, N - 20);
        ASSERT_EQ(N - 40, column->size());
        // First N-40 elements should keep unchanged.
        for (int i = 0; i < N - 40; i++) {
            auto array = column->get(i).get_array();
            ASSERT_EQ(2, array.size());
            ASSERT_EQ(i, array[0].get_int32());
            ASSERT_EQ(i * 2, array[1].get_int32());
        }

        size_t expect_size = 0;
        filter.clear();
        filter.resize(column->size(), 0);
        for (int i = 0; i < filter.size(); i++) {
            filter[i] = (i % 2 == 0);
            expect_size += filter[i];
        }
        column->filter_range(filter, 0, filter.size());
        EXPECT_EQ(expect_size, column->size());
        j = 0;
        for (int i = 0; i < N - 40; i++) {
            if (i % 2 == 0) {
                auto array = column->get(j).get_array();
                ASSERT_EQ(2, array.size());
                ASSERT_EQ(i, array[0].get_int32());
                ASSERT_EQ(i * 2, array[1].get_int32());
                j++;
            }
        }
    }
    // ARRAY<INT>
    {
        auto column = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                          UInt32Column::create());
        // The width of AVX2 register is 256 bits, aka 32 bytes, make the column size equals
        // to 2 * 32 + 31 in order to cover both the SIMD instructions and non-SIMD instructions.
        const int N = 3 * 32 + 31;
        for (int32_t i = 0; i < N; i++) {
            column->append_datum(DatumArray{i, i * 2});
        }

        Column::Filter filter(N, 0);
        for (int i = 0; i < 32; i++) {
            filter[i] = i % 2;
        }
        for (int i = 32; i < 96; i++) {
            filter[i] = 1;
        }

        column->filter_range(filter, 0, N);
        ASSERT_EQ(80, column->size());
        int j = 0;
        for (int i = 0; i < 96; i++) {
            if (i < 32 && i % 2) {
                auto array = column->get(j).get_array();
                ASSERT_EQ(2, array.size());
                ASSERT_EQ(i, array[0].get_int32());
                ASSERT_EQ(i * 2, array[1].get_int32());
                j++;
            } else if (32 <= i) {
                auto array = column->get(j).get_array();
                ASSERT_EQ(2, array.size());
                ASSERT_EQ(i, array[0].get_int32());
                ASSERT_EQ(i * 2, array[1].get_int32());
                j++;
            }
        }
        ASSERT_EQ(80, j);
    }
    // ARRAY<INT> with the number of elements > 2^16
    {
        auto column = ArrayColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                          UInt32Column::create());
        column->reserve(4096);
        for (int i = 0; i < 4096; i++) {
            DatumArray array;
            array.reserve(20);
            for (int j = 1; j <= 20; j++) {
                array.emplace_back(i * j);
            }
            column->append_datum(array);
        }
        Column::Filter filter(4096);
        for (int i = 0; i < 4096; i++) {
            filter[i] = i % 2;
        }
        column->filter_range(filter, 0, 4096);
        ASSERT_EQ(2048, column->size());
        int j = 0;
        for (int i = 0; i < 4096; i++) {
            if (i % 2) {
                auto array = column->get(j).get_array();
                ASSERT_EQ(20, array.size());
                for (int k = 1; k <= 20; k++) {
                    ASSERT_EQ(i * k, array[k - 1].get_int32());
                }
                j++;
            }
        }
        filter.clear();
        filter.resize(column->size(), 0);
        column->filter(filter);
        ASSERT_EQ(0, column->size());
        ASSERT_EQ(0, column->elements_column()->size());
    }
    // ARRAY<ARRAY<INT>>
    {
        const int N = 100;
        auto elements = ArrayColumn::create(Int32Column::create(), UInt32Column::create());
        auto nullable_elements = NullableColumn::create(std::move(elements), NullColumn::create());
        auto offsets = UInt32Column ::create();
        auto column = ArrayColumn::create(std::move(nullable_elements), std::move(offsets));

        for (int i = 0; i < N; i++) {
            column->append_datum(DatumArray{DatumArray{i * 3, i * 3 + 1}, DatumArray{i * 2, i * 2 + 1}});
        }

        Column::Filter filter(N, 1);
        column->filter_range(filter, 0, N);
        column->filter_range(filter, N / 10, N);
        column->filter_range(filter, N / 2, N);
        ASSERT_EQ(N, column->size());
        for (int i = 0; i < N; i++) {
            auto array = column->get(i).get_array();
            ASSERT_EQ(2, array.size());
            auto sub_array0 = array[0].get_array();
            auto sub_array1 = array[1].get_array();
            ASSERT_EQ(2, sub_array0.size());
            ASSERT_EQ(i * 3, sub_array0[0].get_int32());
            ASSERT_EQ(i * 3 + 1, sub_array0[1].get_int32());
            ASSERT_EQ(2, sub_array1.size());
            ASSERT_EQ(i * 2, sub_array1[0].get_int32());
            ASSERT_EQ(i * 2 + 1, sub_array1[1].get_int32());
        }

        size_t expect_size = 0;
        for (int i = 0; i < N; i++) {
            filter[i] = (i % 3 != 0);
            expect_size += filter[i];
        }
        column->filter(filter);
        ASSERT_EQ(expect_size, column->size());
        int j = 0;
        for (int i = 0; i < N; i++) {
            if (i % 3 != 0) {
                auto array = column->get(j).get_array();
                ASSERT_EQ(2, array.size());
                auto sub_array0 = array[0].get_array();
                auto sub_array1 = array[1].get_array();
                ASSERT_EQ(2, sub_array0.size());
                ASSERT_EQ(i * 3, sub_array0[0].get_int32());
                ASSERT_EQ(i * 3 + 1, sub_array0[1].get_int32());
                ASSERT_EQ(2, sub_array1.size());
                ASSERT_EQ(i * 2, sub_array1[0].get_int32());
                ASSERT_EQ(i * 2 + 1, sub_array1[1].get_int32());
                j++;
            }
        }
        filter.clear();
        filter.resize(column->size(), 0);
        for (int i = filter.size() - 10; i < filter.size(); i++) {
            filter[i] = 1;
        }
        // No record should be filtered out.
        column->filter_range(filter, filter.size() - 10, filter.size());
        EXPECT_EQ(filter.size(), column->size());
        j = 0;
        for (int i = 0; i < N; i++) {
            if (i % 3 != 0) {
                auto array = column->get(j).get_array();
                ASSERT_EQ(2, array.size());
                auto sub_array0 = array[0].get_array();
                auto sub_array1 = array[1].get_array();
                ASSERT_EQ(2, sub_array0.size());
                ASSERT_EQ(i * 3, sub_array0[0].get_int32());
                ASSERT_EQ(i * 3 + 1, sub_array0[1].get_int32());
                ASSERT_EQ(2, sub_array1.size());
                ASSERT_EQ(i * 2, sub_array1[0].get_int32());
                ASSERT_EQ(i * 2 + 1, sub_array1[1].get_int32());
                j++;
            }
        }
        filter.clear();
        filter.resize(column->size(), 0);
        // All records will be filtered out.
        column->filter_range(filter, 0, filter.size());
        ASSERT_EQ(0, column->size());
        ASSERT_EQ(1, column->offsets_column()->size());
        ASSERT_EQ(0, column->elements_column()->size());
    }
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_append_array) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    // append [7, 8, 9]
    elements->append(7);
    elements->append(8);
    elements->append(9);
    offsets->append(9);

    ASSERT_EQ("[7, 8, 9]", column->debug_item(2));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_append_nulls) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);
    auto null_column = NullColumn::create();
    auto nullable_column = NullableColumn::create(column, null_column);

    ASSERT_TRUE(nullable_column->append_nulls(1));

    // insert [1, 2, 3], [4, 5, 6]
    null_column->append(0);
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    null_column->append(0);
    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    ASSERT_EQ(3, nullable_column->size());
    ASSERT_TRUE(nullable_column->is_null(0));
    ASSERT_EQ("[4, 5, 6]", nullable_column->debug_item(2));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_append_defaults) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    // append_default
    column->append_default(2);

    ASSERT_EQ(4, column->size());
    ASSERT_EQ("[]", column->debug_item(2));
    ASSERT_EQ("[]", column->debug_item(3));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_compare_at) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    auto offsets_2 = UInt32Column::create();
    auto elements_2 = Int32Column::create();
    auto column_2 = ArrayColumn::create(elements_2, offsets_2);

    // insert [4, 5, 6], [7, 8, 9]
    elements_2->append(4);
    elements_2->append(5);
    elements_2->append(6);
    offsets_2->append(3);

    elements_2->append(7);
    elements_2->append(8);
    elements_2->append(9);
    offsets_2->append(6);

    ASSERT_EQ(2, column->size());
    ASSERT_EQ(2, column_2->size());

    ASSERT_EQ(0, column->compare_at(1, 0, *column_2, -1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_multi_dimension_array) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();

    auto offsets_1 = UInt32Column::create();
    auto elements_1 = ArrayColumn::create(elements, offsets);

    auto column = ArrayColumn::create(elements_1, offsets_1);

    // insert [[1, 2, 3], [4, 5, 6]], [[7], [8], [9]]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);
    offsets_1->append(2);

    elements->append(7);
    offsets->append(7);

    elements->append(8);
    offsets->append(8);

    elements->append(9);
    offsets->append(9);
    offsets_1->append(5);

    ASSERT_EQ("[[1, 2, 3], [4, 5, 6]]", column->debug_item(0));
    ASSERT_EQ("[[7], [8], [9]]", column->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_resize) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6], [7, 8, 9]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    elements->append(7);
    elements->append(8);
    elements->append(9);
    offsets->append(9);

    column->resize(1);
    ASSERT_EQ(1, column->size());
    ASSERT_EQ("[1, 2, 3]", column->debug_item(0));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_reset_column) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6], [7, 8, 9]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    elements->append(7);
    elements->append(8);
    elements->append(9);
    offsets->append(9);

    column->reset_column();
    ASSERT_EQ(0, column->size());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_swap_column) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    auto offsets_2 = UInt32Column::create();
    auto elements_2 = Int32Column::create();
    auto column_2 = ArrayColumn::create(elements_2, offsets_2);

    // insert [4, 5, 6], [7, 8, 9]
    elements_2->append(4);
    elements_2->append(5);
    elements_2->append(6);
    offsets_2->append(3);

    elements_2->append(7);
    elements_2->append(8);
    elements_2->append(9);
    offsets_2->append(6);

    column->swap_column(*column_2);
    ASSERT_EQ("[4, 5, 6]", column->debug_item(0));
    ASSERT_EQ("[7, 8, 9]", column->debug_item(1));
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_copy_constructor) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    ArrayColumn c1(*c0);
    c0->reset_column();
    ASSERT_EQ("[1, 2, 3]", c1.debug_item(0));
    ASSERT_EQ("[4, 5, 6]", c1.debug_item(1));
    ASSERT_TRUE(c1.elements_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_move_constructor) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    ArrayColumn c1(std::move(*c0));
    ASSERT_EQ("[1, 2, 3]", c1.debug_item(0));
    ASSERT_EQ("[4, 5, 6]", c1.debug_item(1));
    ASSERT_TRUE(c1.elements_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_copy_assignment) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    ArrayColumn c1(Int32Column::create(), UInt32Column::create());
    c1 = *c0;
    c0->reset_column();
    ASSERT_EQ("[1, 2, 3]", c1.debug_item(0));
    ASSERT_EQ("[4, 5, 6]", c1.debug_item(1));
    ASSERT_TRUE(c1.elements_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_move_assignment) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    ArrayColumn c1(Int32Column ::create(), UInt32Column::create());
    c1 = std::move(*c0);
    ASSERT_EQ("[1, 2, 3]", c1.debug_item(0));
    ASSERT_EQ("[4, 5, 6]", c1.debug_item(1));
    ASSERT_TRUE(c1.elements_column().unique());
    ASSERT_TRUE(c1.offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_clone) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    auto c1 = c0->clone();
    c0->reset_column();
    ASSERT_EQ("[1, 2, 3]", c1->debug_item(0));
    ASSERT_EQ("[4, 5, 6]", c1->debug_item(1));
    ASSERT_TRUE(down_cast<ArrayColumn*>(c1.get())->elements_column().unique());
    ASSERT_TRUE(down_cast<ArrayColumn*>(c1.get())->offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_clone_shared) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    auto c1 = c0->clone_shared();
    c0->reset_column();
    ASSERT_EQ("[1, 2, 3]", c1->debug_item(0));
    ASSERT_EQ("[4, 5, 6]", c1->debug_item(1));
    ASSERT_TRUE(c1.unique());
    ASSERT_TRUE(down_cast<ArrayColumn*>(c1.get())->elements_column().unique());
    ASSERT_TRUE(down_cast<ArrayColumn*>(c1.get())->offsets_column().unique());
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_clone_column) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    auto cloned_column = c0->clone_empty();
    ASSERT_TRUE(cloned_column->is_array());
    ASSERT_EQ(0, cloned_column->size());
    ASSERT_EQ(0, down_cast<ArrayColumn*>(cloned_column.get())->elements_column()->size());
    ASSERT_EQ(1, down_cast<ArrayColumn*>(cloned_column.get())->offsets_column()->size());
}

PARALLEL_TEST(ArrayColumnTest, test_array_hash) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6]
    size_t array_size_1 = 3;
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    size_t array_size_2 = 3;
    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);

    uint32_t hash_value[2] = {0, 0};
    c0->crc32_hash(hash_value, 0, 2);

    uint32_t hash_value_1 = HashUtil::zlib_crc_hash(&array_size_1, sizeof(array_size_1), 0);
    for (int i = 0; i < 3; ++i) {
        elements->crc32_hash(&hash_value_1 - i, i, i + 1);
    }
    uint32_t hash_value_2 = HashUtil::zlib_crc_hash(&array_size_2, sizeof(array_size_2), 0);
    for (int i = 3; i < 6; ++i) {
        elements->crc32_hash(&hash_value_2 - i, i, i + 1);
    }
    ASSERT_EQ(hash_value_1, hash_value[0]);
    ASSERT_EQ(hash_value_2, hash_value[1]);

    uint32_t hash_value_fnv[2] = {0, 0};
    c0->fnv_hash(hash_value_fnv, 0, 2);
    uint32_t hash_value_1_fnv = HashUtil::fnv_hash(&array_size_1, sizeof(array_size_1), 0);
    for (int i = 0; i < 3; ++i) {
        elements->fnv_hash(&hash_value_1_fnv - i, i, i + 1);
    }
    uint32_t hash_value_2_fnv = HashUtil::fnv_hash(&array_size_2, sizeof(array_size_2), 0);
    for (int i = 3; i < 6; ++i) {
        elements->fnv_hash(&hash_value_2_fnv - i, i, i + 1);
    }

    ASSERT_EQ(hash_value_1_fnv, hash_value_fnv[0]);
    ASSERT_EQ(hash_value_2_fnv, hash_value_fnv[1]);

    // overflow test
    for (int i = 0; i < 100000; ++i) {
        elements->append(i);
    }
    offsets->append(elements->size());
    uint32_t hash_value_overflow_test[3] = {0, 0, 0};
    c0->crc32_hash(hash_value_overflow_test, 0, 3);

    auto& offset_values = offsets->get_data();
    size_t sz = offset_values[offset_values.size() - 1] - offset_values[offset_values.size() - 2];

    uint32_t hash_value_overflow = HashUtil::zlib_crc_hash(&sz, sizeof(sz), 0);
    for (int i = 0; i < 100000; ++i) {
        uint32_t value = i;
        hash_value_overflow = HashUtil::zlib_crc_hash(&value, sizeof(value), hash_value_overflow);
    }

    ASSERT_EQ(hash_value_overflow, hash_value_overflow_test[2]);
}

PARALLEL_TEST(ArrayColumnTest, test_xor_checksum) {
    auto c0 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());

    auto* offsets = down_cast<UInt32Column*>(c0->offsets_column().get());
    auto* elements = down_cast<Int32Column*>(c0->elements_column().get());

    // insert [1, 2, 3], [4, 5, 6, 7]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    elements->append(7);
    elements->append(8);
    offsets->append(8);

    int64_t checksum = c0->xor_checksum(0, 2);
    int64_t expected_checksum = 14;

    ASSERT_EQ(checksum, expected_checksum);
}

} // namespace starrocks::vectorized
