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

#include <numeric>
#include <vector>

#include "column/append_with_mask.h"
#include "column/binary_column.h"
#include "column/column_view/column_view.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "gtest/gtest.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

TEST(AppendWithMaskCoreTest, appendFixedLengthWithPositiveAndNegativeMask) {
    auto src = Int32Column::create();
    for (int32_t value : {1, 2, 3, 4, 5}) {
        src->append(value);
    }
    const uint8_t mask[] = {1, 0, 1, 0, 1};

    auto positive = Int32Column::create();
    ASSERT_TRUE(append_with_mask<true>(positive.get(), *src, mask, 5).ok());
    EXPECT_EQ("[1, 3, 5]", positive->debug_string());

    auto negative = Int32Column::create();
    ASSERT_TRUE(append_with_mask<false>(negative.get(), *src, mask, 5).ok());
    EXPECT_EQ("[2, 4]", negative->debug_string());
}

TEST(AppendWithMaskCoreTest, appendBinaryColumnWithMask) {
    auto src = ColumnTestHelper::build_column<Slice>({"alpha", "beta", "gamma", "delta"});
    const uint8_t mask[] = {0, 1, 1, 0};

    auto dst = BinaryColumn::create();
    ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask, 4).ok());
    EXPECT_EQ("['beta', 'gamma']", dst->debug_string());
}

TEST(AppendWithMaskCoreTest, appendNullableColumnWithMask) {
    auto src = NullableColumn::create(Int32Column::create(), NullColumn::create());
    src->append_datum(Datum(static_cast<int32_t>(10)));
    src->append_nulls(1);
    src->append_datum(Datum(static_cast<int32_t>(30)));
    const uint8_t mask[] = {1, 1, 0};

    auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
    ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask, 3).ok());

    ASSERT_EQ("[10, NULL]", dst->debug_string());
}

TEST(AppendWithMaskCoreTest, appendAllSelectedRows) {
    // 1. FixedLengthColumnBase<int64_t>
    {
        auto src = Int64Column::create();
        for (int64_t i = 100; i < 110; ++i) {
            src->append(i);
        }
        std::vector<uint8_t> mask(src->size(), 1);
        auto dst = Int64Column::create();
        dst->append(static_cast<int64_t>(99));
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(11, dst->size());
        EXPECT_EQ(99, dst->get_data()[0]);
        for (size_t i = 0; i < src->size(); ++i) {
            EXPECT_EQ(src->get_data()[i], dst->get_data()[i + 1]);
        }
    }

    // 2. BinaryColumn
    {
        auto src = BinaryColumn::create();
        for (int i = 0; i < 20; ++i) {
            src->append("str_" + std::to_string(i));
        }
        std::vector<uint8_t> mask(src->size(), 1);
        auto dst = BinaryColumn::create();
        dst->append("prefix");
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(21, dst->size());
        EXPECT_EQ("prefix", dst->get_slice(0).to_string());
        for (size_t i = 0; i < src->size(); ++i) {
            EXPECT_EQ(src->get_slice(i).to_string(), dst->get_slice(i + 1).to_string());
        }
    }

    // 3. NullableColumn (with nulls)
    {
        auto src = NullableColumn::create(Int32Column::create(), NullColumn::create());
        for (int32_t i = 0; i < 15; ++i) {
            if (i % 3 == 0) {
                src->append_nulls(1);
            } else {
                src->append_datum(Datum(i));
            }
        }
        std::vector<uint8_t> mask(src->size(), 1);
        auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
        dst->append_datum(Datum(static_cast<int32_t>(-1)));
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(16, dst->size());
        EXPECT_FALSE(dst->is_null(0));
        EXPECT_EQ(-1, dst->get(0).get_int32());
        for (size_t i = 0; i < src->size(); ++i) {
            EXPECT_EQ(src->is_null(i), dst->is_null(i + 1));
            if (!src->is_null(i)) {
                EXPECT_EQ(src->get(i).get_int32(), dst->get(i + 1).get_int32());
            }
        }
    }

    // 4. NullableColumn (without nulls)
    {
        auto src = NullableColumn::create(Int32Column::create(), NullColumn::create());
        for (int32_t i = 0; i < 10; ++i) {
            src->append_datum(Datum(i * 10));
        }
        std::vector<uint8_t> mask(src->size(), 1);
        auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(10, dst->size());
        EXPECT_FALSE(dst->has_null());
        for (size_t i = 0; i < src->size(); ++i) {
            EXPECT_EQ(src->get(i).get_int32(), dst->get(i).get_int32());
        }
    }

    // 5. NullableColumn (only nulls)
    {
        auto src = NullableColumn::create(Int32Column::create(), NullColumn::create());
        src->append_nulls(8);
        std::vector<uint8_t> mask(src->size(), 1);
        auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(8, dst->size());
        EXPECT_TRUE(dst->has_null());
        for (size_t i = 0; i < 8; ++i) {
            EXPECT_TRUE(dst->is_null(i));
        }
    }
}

TEST(AppendWithMaskCoreTest, appendNoneSelectedRows) {
    // 1. FixedLengthColumnBase<int64_t>
    {
        auto src = Int64Column::create();
        for (int64_t i = 0; i < 30; ++i) {
            src->append(i);
        }
        std::vector<uint8_t> mask(src->size(), 0);
        auto dst = Int64Column::create();
        dst->append(static_cast<int64_t>(42));
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(1, dst->size());
        EXPECT_EQ(42, dst->get_data()[0]);
    }

    // 2. BinaryColumn
    {
        auto src = BinaryColumn::create();
        for (int i = 0; i < 20; ++i) {
            src->append("data_" + std::to_string(i));
        }
        std::vector<uint8_t> mask(src->size(), 0);
        auto dst = BinaryColumn::create();
        dst->append("stay");
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(1, dst->size());
        EXPECT_EQ("stay", dst->get_slice(0).to_string());
    }

    // 3. NullableColumn
    {
        auto src = NullableColumn::create(Int32Column::create(), NullColumn::create());
        for (int32_t i = 0; i < 10; ++i) {
            src->append_datum(Datum(i));
        }
        std::vector<uint8_t> mask(src->size(), 0);
        auto dst = NullableColumn::create(Int32Column::create(), NullColumn::create());
        dst->append_datum(Datum(static_cast<int32_t>(7)));
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), src->size()).ok());
        ASSERT_EQ(1, dst->size());
        EXPECT_EQ(7, dst->get(0).get_int32());
    }
}

TEST(AppendWithMaskCoreTest, appendContiguousRunsVaryingLengths) {
    // Build a sequence with runs: 100 selected, 10 skipped, 50 selected, 5 skipped, 1 selected, 2 skipped, 30 selected
    const size_t total_rows = 100 + 10 + 50 + 5 + 1 + 2 + 30;
    std::vector<uint8_t> mask(total_rows, 0);

    size_t idx = 0;
    // 100 selected
    for (size_t i = 0; i < 100; ++i) mask[idx++] = 1;
    // 10 skipped
    for (size_t i = 0; i < 10; ++i) mask[idx++] = 0;
    // 50 selected
    for (size_t i = 0; i < 50; ++i) mask[idx++] = 1;
    // 5 skipped
    for (size_t i = 0; i < 5; ++i) mask[idx++] = 0;
    // 1 selected
    for (size_t i = 0; i < 1; ++i) mask[idx++] = 1;
    // 2 skipped
    for (size_t i = 0; i < 2; ++i) mask[idx++] = 0;
    // 30 selected
    for (size_t i = 0; i < 30; ++i) mask[idx++] = 1;
    ASSERT_EQ(idx, total_rows);

    const size_t expected_selected = 100 + 50 + 1 + 30;

    // Test on FixedLengthColumnBase<int64_t>
    {
        auto src = Int64Column::create();
        for (size_t i = 0; i < total_rows; ++i) {
            src->append(static_cast<int64_t>(1000 + i));
        }

        auto dst = Int64Column::create();
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), total_rows).ok());
        ASSERT_EQ(expected_selected, dst->size());

        size_t out_idx = 0;
        for (size_t i = 0; i < total_rows; ++i) {
            if (mask[i]) {
                EXPECT_EQ(src->get_data()[i], dst->get_data()[out_idx++]);
            }
        }
        EXPECT_EQ(expected_selected, out_idx);
    }

    // Test on BinaryColumn
    {
        auto src = BinaryColumn::create();
        for (size_t i = 0; i < total_rows; ++i) {
            src->append("value_string_" + std::to_string(i));
        }

        auto dst = BinaryColumn::create();
        ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), total_rows).ok());
        ASSERT_EQ(expected_selected, dst->size());

        size_t out_idx = 0;
        for (size_t i = 0; i < total_rows; ++i) {
            if (mask[i]) {
                EXPECT_EQ(src->get_slice(i).to_string(), dst->get_slice(out_idx++).to_string());
            }
        }
        EXPECT_EQ(expected_selected, out_idx);
    }
}

TEST(AppendWithMaskCoreTest, appendNegativeSelection) {
    // Negative selection: 0 means selected, non-zero means skipped.
    const size_t total_rows = 60;
    std::vector<uint8_t> mask(total_rows, 1);
    // Mark rows [10, 20) and [40, 55) with 0 (selected under negative selection)
    for (size_t i = 10; i < 20; ++i) mask[i] = 0;
    for (size_t i = 40; i < 55; ++i) mask[i] = 0;
    const size_t expected_selected = 10 + 15;

    // Fixed length column
    {
        auto src = Int32Column::create();
        for (size_t i = 0; i < total_rows; ++i) {
            src->append(static_cast<int32_t>(i));
        }
        auto dst = Int32Column::create();
        ASSERT_TRUE(append_with_mask<false>(dst.get(), *src, mask.data(), total_rows).ok());
        ASSERT_EQ(expected_selected, dst->size());

        size_t out_idx = 0;
        for (size_t i = 0; i < total_rows; ++i) {
            if (mask[i] == 0) {
                EXPECT_EQ(src->get_data()[i], dst->get_data()[out_idx++]);
            }
        }
        EXPECT_EQ(expected_selected, out_idx);
    }

    // Binary column
    {
        auto src = BinaryColumn::create();
        for (size_t i = 0; i < total_rows; ++i) {
            src->append("neg_str_" + std::to_string(i));
        }
        auto dst = BinaryColumn::create();
        ASSERT_TRUE(append_with_mask<false>(dst.get(), *src, mask.data(), total_rows).ok());
        ASSERT_EQ(expected_selected, dst->size());

        size_t out_idx = 0;
        for (size_t i = 0; i < total_rows; ++i) {
            if (mask[i] == 0) {
                EXPECT_EQ(src->get_slice(i).to_string(), dst->get_slice(out_idx++).to_string());
            }
        }
        EXPECT_EQ(expected_selected, out_idx);
    }

    // All unselected under negative selection (all mask bytes are non-zero)
    {
        auto src = Int32Column::create();
        src->append(100);
        std::vector<uint8_t> all_ones = {1, 2, 255};
        auto dst = Int32Column::create();
        ASSERT_TRUE(append_with_mask<false>(dst.get(), *src, all_ones.data(), 1).ok());
        EXPECT_EQ(0, dst->size());
    }

    // All selected under negative selection (all mask bytes are 0)
    {
        auto src = Int32Column::create();
        for (int32_t i = 0; i < 8; ++i) {
            src->append(i);
        }
        std::vector<uint8_t> all_zeros(8, 0);
        auto dst = Int32Column::create();
        ASSERT_TRUE(append_with_mask<false>(dst.get(), *src, all_zeros.data(), 8).ok());
        ASSERT_EQ(8, dst->size());
        for (int32_t i = 0; i < 8; ++i) {
            EXPECT_EQ(i, dst->get_data()[i]);
        }
    }
}

TEST(AppendWithMaskCoreTest, appendArbitraryNonZeroMaskBytes) {
    auto src = Int32Column::create();
    for (int32_t i = 0; i < 6; ++i) {
        src->append(i + 1);
    }
    // Non-zero mask bytes: 0xFF, 2, 0x80, 0, 127, 0
    const uint8_t mask[] = {0xFF, 2, 0x80, 0, 127, 0};

    // Positive selection: all non-zero bytes are selected (0xFF, 2, 0x80, 127) -> rows 0, 1, 2, 4
    auto pos_dst = Int32Column::create();
    ASSERT_TRUE(append_with_mask<true>(pos_dst.get(), *src, mask, 6).ok());
    EXPECT_EQ("[1, 2, 3, 5]", pos_dst->debug_string());

    // Negative selection: only 0 is selected -> rows 3, 5
    auto neg_dst = Int32Column::create();
    ASSERT_TRUE(append_with_mask<false>(neg_dst.get(), *src, mask, 6).ok());
    EXPECT_EQ("[4, 6]", neg_dst->debug_string());
}

TEST(AppendWithMaskCoreTest, appendBinaryViewColumnFallback) {
    auto base_col = BinaryColumn::create();
    base_col->append("apple");
    base_col->append("banana");
    base_col->append("cherry");
    base_col->append("date");

    auto view = ColumnView::create(BinaryColumn::create(), 1, -1);
    view->append(*base_col, 0, base_col->size());
    ASSERT_TRUE(view->is_binary_view());
    ASSERT_EQ(4, view->size());

    const uint8_t mask[] = {1, 0, 1, 0};

    auto dst = BinaryColumn::create();
    ASSERT_TRUE(append_with_mask<true>(dst.get(), *view, mask, 4).ok());
    ASSERT_EQ(2, dst->size());
    EXPECT_EQ("apple", dst->get_slice(0).to_string());
    EXPECT_EQ("cherry", dst->get_slice(1).to_string());

    auto neg_dst = BinaryColumn::create();
    ASSERT_TRUE(append_with_mask<false>(neg_dst.get(), *view, mask, 4).ok());
    ASSERT_EQ(2, neg_dst->size());
    EXPECT_EQ("banana", neg_dst->get_slice(0).to_string());
    EXPECT_EQ("date", neg_dst->get_slice(1).to_string());
}

TEST(AppendWithMaskCoreTest, appendViewToNullableColumnAllSelected) {
    auto base_col = BinaryColumn::create();
    base_col->append("star");
    base_col->append("rocks");
    base_col->append("simd");
    base_col->append("vector");

    auto view = ColumnView::create(BinaryColumn::create(), 1, -1);
    view->append(*base_col, 0, base_col->size());
    ASSERT_TRUE(view->is_binary_view());
    ASSERT_EQ(4, view->size());

    // 1. Direct BinaryView into NullableColumn with all-selected mask
    const uint8_t all_ones_mask[] = {1, 1, 1, 1};
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    ASSERT_TRUE(append_with_mask<true>(dst.get(), *view, all_ones_mask, 4).ok());
    ASSERT_EQ(4, dst->size());
    EXPECT_EQ("star", dst->get(0).get_slice().to_string());
    EXPECT_EQ("rocks", dst->get(1).get_slice().to_string());
    EXPECT_EQ("simd", dst->get(2).get_slice().to_string());
    EXPECT_EQ("vector", dst->get(3).get_slice().to_string());
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(dst->is_null(i));
    }

    // 2. Nullable-wrapped BinaryView into NullableColumn with all-selected mask
    auto view_null = NullColumn::create();
    view_null->append(0);
    view_null->append(1); // "rocks" is null
    view_null->append(0);
    view_null->append(0);
    auto nullable_view = NullableColumn::create(view, view_null);

    auto dst2 = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    ASSERT_TRUE(append_with_mask<true>(dst2.get(), *nullable_view, all_ones_mask, 4).ok());
    ASSERT_EQ(4, dst2->size());
    EXPECT_FALSE(dst2->is_null(0));
    EXPECT_TRUE(dst2->is_null(1));
    EXPECT_FALSE(dst2->is_null(2));
    EXPECT_FALSE(dst2->is_null(3));
    EXPECT_EQ("star", dst2->get(0).get_slice().to_string());
    EXPECT_EQ("simd", dst2->get(2).get_slice().to_string());
    EXPECT_EQ("vector", dst2->get(3).get_slice().to_string());
}

TEST(AppendWithMaskCoreTest, appendFixedLengthStrictBufferBoundaryUnderAsan) {
    // Under ASAN, if append_fixed_length resizes the destination buffer to only
    // orig_size + selected, vectorised store instructions (e.g. AVX-512 _mm512_storeu_si512)
    // could write past the end of the allocated buffer when selected is small and count is large.
    // Here we test counts from 32 up to 256 with only 1 or 2 items selected at varying positions.
    for (size_t count : {32, 48, 64, 96, 128, 256}) {
        auto src = Int32Column::create();
        for (size_t i = 0; i < count; ++i) {
            src->append(static_cast<int32_t>(i));
        }

        // Test 1: Only the first element selected
        {
            std::vector<uint8_t> mask(count, 0);
            mask[0] = 1;
            auto dst = Int32Column::create();
            ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), count).ok());
            ASSERT_EQ(1, dst->size());
            EXPECT_EQ(0, dst->get_data()[0]);
        }

        // Test 2: Only the last element selected
        {
            std::vector<uint8_t> mask(count, 0);
            mask[count - 1] = 1;
            auto dst = Int32Column::create();
            ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), count).ok());
            ASSERT_EQ(1, dst->size());
            EXPECT_EQ(static_cast<int32_t>(count - 1), dst->get_data()[0]);
        }

        // Test 3: Only two elements selected in the middle
        {
            std::vector<uint8_t> mask(count, 0);
            mask[count / 2] = 1;
            mask[count / 2 + 1] = 1;
            auto dst = Int32Column::create();
            ASSERT_TRUE(append_with_mask<true>(dst.get(), *src, mask.data(), count).ok());
            ASSERT_EQ(2, dst->size());
            EXPECT_EQ(static_cast<int32_t>(count / 2), dst->get_data()[0]);
            EXPECT_EQ(static_cast<int32_t>(count / 2 + 1), dst->get_data()[1]);
        }

        // Test 4: Verify on Int64Column as well (AVX-512 8-byte compress)
        {
            auto src64 = Int64Column::create();
            for (size_t i = 0; i < count; ++i) {
                src64->append(static_cast<int64_t>(10000 + i));
            }
            std::vector<uint8_t> mask(count, 0);
            mask[1] = 1;
            mask[count - 2] = 1;
            auto dst64 = Int64Column::create();
            ASSERT_TRUE(append_with_mask<true>(dst64.get(), *src64, mask.data(), count).ok());
            ASSERT_EQ(2, dst64->size());
            EXPECT_EQ(10001, dst64->get_data()[0]);
            EXPECT_EQ(static_cast<int64_t>(10000 + count - 2), dst64->get_data()[1]);
        }
    }
}

} // namespace starrocks
