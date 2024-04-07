// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
PARALLEL_TEST(ArrayColumnTest, test_array_column_update_if_overflow) {
    // normal
    auto offsets = UInt32Column::create();
    auto elements = BinaryColumn::create();
    auto column = ArrayColumn::create(elements, offsets);

    elements->append("1");
    elements->append("2");
    offsets->append(2);
    auto ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_EQ(column->size(), 1);
    auto array = column->get(0).get_array();
    ASSERT_EQ(array[0].get_slice(), Slice("1"));
    ASSERT_EQ(array[1].get_slice(), Slice("2"));

#ifdef NDEBUG
    /*
    // the test case case will use a lot of memory, so temp comment it
    // upgrade
    offsets = UInt32Column::create();
    elements = BinaryColumn::create();
    column = ArrayColumn::create(elements, offsets);
    size_t item_count = 1<<30;
    for (size_t i = 0; i < item_count; i++) {
        elements->append(std::to_string(i));
    }
    offsets->resize(item_count + 1);
    for (size_t i = 0; i < item_count; i++) {
        offsets->get_data()[i + 1] = i + 1;
    }
    ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_TRUE(column->elements_column()->is_large_binary());
    */
#endif
}

// NOLINTNEXTLINE
PARALLEL_TEST(ArrayColumnTest, test_array_column_downgrade) {
    auto offsets = UInt32Column::create();
    auto elements = BinaryColumn::create();
    elements->append("1");
    elements->append("2");
    offsets->append(2);
    auto column = ArrayColumn::create(elements, offsets);
    ASSERT_FALSE(column->has_large_column());
    auto ret = column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);

    offsets = UInt32Column::create();
    auto large_elements = LargeBinaryColumn::create();
    column = ArrayColumn::create(large_elements, offsets);
    for (size_t i = 0; i < 10; i++) {
        large_elements->append(std::to_string(i));
        offsets->append(i + 1);
    }
    ASSERT_TRUE(column->has_large_column());
    ret = column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_FALSE(column->has_large_column());
    ASSERT_EQ(column->size(), 10);
    for (size_t i = 0; i < 10; i++) {
        ASSERT_EQ(column->get(i).get_array()[0].get_slice(), Slice(std::to_string(i)));
    }
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

    ASSERT_EQ("[1,2,3]", column->debug_item(0));
    ASSERT_EQ("[4,5,6]", column->debug_item(1));
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

    // elements 1 with 12 bytes.
    // offset 1 with 4 bytes.
    ASSERT_EQ(16, column->byte_size(1, 1));
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

    ASSERT_EQ("[7,8,9]", column->debug_item(2));
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
    ASSERT_EQ("[4,5,6]", nullable_column->debug_item(2));
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

    ASSERT_EQ("[[1,2,3],[4,5,6]]", column->debug_item(0));
    ASSERT_EQ("[[7],[8],[9]]", column->debug_item(1));
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
    ASSERT_EQ("[1,2,3]", column->debug_item(0));
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
    ASSERT_EQ("[4,5,6]", column->debug_item(0));
    ASSERT_EQ("[7,8,9]", column->debug_item(1));
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
    ASSERT_EQ("[1,2,3]", c1.debug_item(0));
    ASSERT_EQ("[4,5,6]", c1.debug_item(1));
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
    ASSERT_EQ("[1,2,3]", c1.debug_item(0));
    ASSERT_EQ("[4,5,6]", c1.debug_item(1));
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
    ASSERT_EQ("[1,2,3]", c1.debug_item(0));
    ASSERT_EQ("[4,5,6]", c1.debug_item(1));
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
    ASSERT_EQ("[1,2,3]", c1.debug_item(0));
    ASSERT_EQ("[4,5,6]", c1.debug_item(1));
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
    ASSERT_EQ("[1,2,3]", c1->debug_item(0));
    ASSERT_EQ("[4,5,6]", c1->debug_item(1));
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
    ASSERT_EQ("[1,2,3]", c1->debug_item(0));
    ASSERT_EQ("[4,5,6]", c1->debug_item(1));
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

PARALLEL_TEST(ArrayColumnTest, test_update_rows) {
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

    // append [10, 11, 12]
    elements->append(10);
    elements->append(11);
    elements->append(12);
    offsets->append(12);

    auto offset_col1 = UInt32Column::create();
    auto element_col1 = Int32Column::create();
    auto replace_col1 = ArrayColumn::create(element_col1, offset_col1);

    // insert [101, 102], [103, 104]
    element_col1->append(101);
    element_col1->append(102);
    offset_col1->append(2);

    element_col1->append(103);
    element_col1->append(104);
    offset_col1->append(4);

    std::vector<uint32_t> replace_idxes = {1, 3};
    ASSERT_TRUE(column->update_rows(*replace_col1.get(), replace_idxes.data()).ok());

    ASSERT_EQ(4, column->size());
    ASSERT_EQ("[1,2,3]", column->debug_item(0));
    ASSERT_EQ("[101,102]", column->debug_item(1));
    ASSERT_EQ("[7,8,9]", column->debug_item(2));
    ASSERT_EQ("[103,104]", column->debug_item(3));

    auto offset_col2 = UInt32Column::create();
    auto element_col2 = Int32Column::create();
    auto replace_col2 = ArrayColumn::create(element_col2, offset_col2);

    // insert [201, 202], [203, 204]
    element_col2->append(201);
    element_col2->append(202);
    offset_col2->append(2);

    element_col2->append(203);
    element_col2->append(204);
    offset_col2->append(4);

    ASSERT_TRUE(column->update_rows(*replace_col2.get(), replace_idxes.data()).ok());

    ASSERT_EQ(4, column->size());
    ASSERT_EQ("[1,2,3]", column->debug_item(0));
    ASSERT_EQ("[201,202]", column->debug_item(1));
    ASSERT_EQ("[7,8,9]", column->debug_item(2));
    ASSERT_EQ("[203,204]", column->debug_item(3));
}

PARALLEL_TEST(ArrayColumnTest, test_assign) {
    /// test assign comment arrays
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

    // assign
    column->assign(4, 0);
    ASSERT_EQ(4, column->size());
    ASSERT_EQ("[1,2,3]", column->debug_item(0));
    ASSERT_EQ("[1,2,3]", column->debug_item(1));
    ASSERT_EQ("[1,2,3]", column->debug_item(2));
    ASSERT_EQ("[1,2,3]", column->debug_item(3));

    /// test assign [null]
    elements = Int32Column::create();
    auto nullable_elements = NullableColumn::create(std::move(elements), NullColumn::create());
    offsets = UInt32Column ::create();
    column = ArrayColumn::create(std::move(nullable_elements), std::move(offsets));
    column->append_datum(DatumArray{Datum()});

    column->assign(5, 0);
    ASSERT_EQ(5, column->size());
    ASSERT_TRUE(column->get(0).get_array()[0].is_null());
    ASSERT_TRUE(column->get(4).get_array()[0].is_null());

    /// test assign []
    column->reset_column();
    column->append_datum(DatumArray{});

    column->assign(5, 0);
    ASSERT_EQ(5, column->size());
    ASSERT_TRUE(column->get(0).get_array().empty());
    ASSERT_TRUE(column->get(4).get_array().empty());

    /// test assign [null,null]
    column->reset_column();
    column->append_datum(DatumArray{Datum(), Datum()});

    column->assign(5, 0);
    ASSERT_EQ(5, column->size());
    ASSERT_TRUE(column->get(0).get_array()[0].is_null());
    ASSERT_TRUE(column->get(4).get_array()[1].is_null());
}

PARALLEL_TEST(ArrayColumnTest, test_empty_null_array) {
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

    auto null_map = NullColumn::create(2, 0);
    auto res = column->empty_null_array(null_map);
    ASSERT_FALSE(res);
    ASSERT_EQ(2, column->size());
    ASSERT_EQ("[1,2,3]", column->debug_item(0));
    ASSERT_EQ("[4,5,6]", column->debug_item(1));

    null_map->get_data()[0] = 1;
    res = column->empty_null_array(null_map);
    ASSERT_TRUE(res);
    ASSERT_EQ(2, column->size());
    ASSERT_EQ("[]", column->debug_item(0));
    ASSERT_EQ("[4,5,6]", column->debug_item(1));

    null_map->get_data()[1] = 1;
    res = column->empty_null_array(null_map);
    ASSERT_TRUE(res);
    ASSERT_EQ(2, column->size());
    ASSERT_EQ("[]", column->debug_item(0));
    ASSERT_EQ("[]", column->debug_item(1));
}

PARALLEL_TEST(ArrayColumnTest, test_replicate) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();
    auto column = ArrayColumn::create(elements, offsets);

    // insert [1, 2, 3], [4, 5, 6],[]
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    elements->append(4);
    elements->append(5);
    elements->append(6);
    offsets->append(6);
    offsets->append(6);

    Offsets off;
    off.push_back(0);
    off.push_back(3);
    off.push_back(5);
    off.push_back(7);

    auto res = column->replicate(off);

    ASSERT_EQ("[1,2,3]", res->debug_item(0));
    ASSERT_EQ("[1,2,3]", res->debug_item(1));
    ASSERT_EQ("[1,2,3]", res->debug_item(2));
    ASSERT_EQ("[4,5,6]", res->debug_item(3));
    ASSERT_EQ("[4,5,6]", res->debug_item(4));
    ASSERT_EQ("[]", res->debug_item(5));
    ASSERT_EQ("[]", res->debug_item(6));
}

PARALLEL_TEST(ArrayColumnTest, test_reference_memory_usage) {
    {
        auto offsets = UInt32Column::create();
        auto elements = Int32Column::create();
        auto column = ArrayColumn::create(elements, offsets);

        // insert [],[1],[2, 3],[4, 5, 6]
        offsets->append(0);

        elements->append(1);
        offsets->append(1);

        elements->append(2);
        elements->append(3);
        offsets->append(3);

        elements->append(4);
        elements->append(5);
        elements->append(6);
        offsets->append(6);

        ASSERT_EQ("[]", column->debug_item(0));
        ASSERT_EQ("[1]", column->debug_item(1));
        ASSERT_EQ("[2,3]", column->debug_item(2));
        ASSERT_EQ("[4,5,6]", column->debug_item(3));

        ASSERT_EQ(0, column->Column::reference_memory_usage());
    }
    {
        auto offsets = UInt32Column::create();
        auto elements = NullableColumn::create(JsonColumn::create(), NullColumn::create());
        auto column = ArrayColumn::create(elements, offsets);

        auto append_json_value = [&](const std::string& json_str) {
            auto json_value = JsonValue::parse(json_str).value();
            elements->append_datum(&json_value);
        };
        // insert [],["1"],["2","3"],["4","5","6"]
        offsets->append(0);

        append_json_value("1");
        offsets->append(1);

        append_json_value("2");
        append_json_value("3");
        offsets->append(3);

        append_json_value("4");
        append_json_value("5");
        append_json_value("6");
        offsets->append(6);

        std::cout << "json size: " << column->Column::reference_memory_usage() << std::endl;
        ASSERT_EQ(12, column->Column::reference_memory_usage());

        ASSERT_EQ(0, column->reference_memory_usage(0, 1));
        ASSERT_EQ(2, column->reference_memory_usage(0, 2));
        ASSERT_EQ(6, column->reference_memory_usage(0, 3));
        ASSERT_EQ(12, column->reference_memory_usage(0, 4));
        ASSERT_EQ(2, column->reference_memory_usage(1, 1));
        ASSERT_EQ(6, column->reference_memory_usage(1, 2));
        ASSERT_EQ(12, column->reference_memory_usage(1, 3));
        ASSERT_EQ(4, column->reference_memory_usage(2, 1));
        ASSERT_EQ(10, column->reference_memory_usage(2, 2));
        ASSERT_EQ(6, column->reference_memory_usage(3, 1));
    }
}

} // namespace starrocks::vectorized
