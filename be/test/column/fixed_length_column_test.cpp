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

#include "column/fixed_length_column.h"

#include <gtest/gtest.h>

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "exec/sorting/sorting.h"

namespace starrocks {

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_basic) {
    auto column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i < 100; i++) {
        column->append(i);
    }

    ASSERT_EQ(true, column->is_numeric());
    ASSERT_EQ(100, column->size());
    ASSERT_EQ(100 * 4, column->byte_size());

    column = FixedLengthColumn<int32_t>::create();
    column->reserve(100);
    column->resize(100);

    auto* data = reinterpret_cast<int32_t*>(column->mutable_raw_data());
    for (int i = 0; i < 100; i++) {
        *data = i;
        data++;
    }

    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(column->get_data()[i], i);
    }

    column = FixedLengthColumn<int32_t>::create(100);
    ASSERT_EQ(100, column->size());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(column->get_data()[i], 0);
    }
    {
        column = FixedLengthColumn<int32_t>::create();
        for (int i = 0; i < 100; i++) {
            column->append(i);
        }

        Filter filter;
        for (int i = 0; i < 100; ++i) {
            filter.push_back(i % 2);
        }

        column->filter(filter);
        auto re = column;
        ASSERT_EQ(50, re->size());

        for (int k = 0; k < 50; ++k) {
            ASSERT_EQ(k * 2 + 1, re->get_data()[k]);
        }
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_nullable) {
    auto data_column = FixedLengthColumn<int32_t>::create();
    auto null_column = FixedLengthColumn<uint8_t>::create();

    for (int i = 0; i < 100; i++) {
        data_column->append(i);
        null_column->append(i % 2 ? 1 : 0);
    }

    auto column = NullableColumn::create(std::move(data_column), std::move(null_column));

    ASSERT_EQ(false, column->is_numeric());
    ASSERT_EQ(100, column->size());
    ASSERT_EQ(100 * 5, column->byte_size());

    const auto* data = reinterpret_cast<const int32_t*>(column->raw_data());
    for (int i = 0; i < 100; i++) {
        if (i % 2) {
            ASSERT_EQ(true, column->is_null(i));
            ASSERT_EQ(i, data[i]);
        } else {
            ASSERT_EQ(false, column->is_null(i));
        }
    }

    data_column = Int32Column::create();
    null_column = NullColumn::create();
    column = NullableColumn::create(std::move(data_column), std::move(null_column));
    column->reserve(100);

    for (int32_t i = 0; i < 100; i++) {
        if (i % 3) {
            column->append_nulls(1);
        } else {
            column->append_datum(Datum(i));
        }
    }

    data = reinterpret_cast<const int32_t*>(column->raw_data());
    for (int i = 0; i < 100; i++) {
        if (i % 3) {
            ASSERT_EQ(true, column->is_null(i));
            ASSERT_EQ(0, data[i]);
        } else {
            ASSERT_EQ(false, column->is_null(i));
            ASSERT_EQ(i, data[i]);
        }
    }
    {
        data_column = FixedLengthColumn<int32_t>::create();
        null_column = FixedLengthColumn<uint8_t>::create();
        column = NullableColumn::create(std::move(data_column), std::move(null_column));
        column->reserve(100);

        for (int32_t i = 0; i < 100; i++) {
            if (i % 3) {
                column->append_nulls(1);
            } else {
                column->append_datum(Datum(i));
            }
        }

        Filter filter;
        for (int k = 0; k < 50; ++k) {
            filter.push_back(0);
        }

        for (int k = 0; k < 50; ++k) {
            filter.push_back(1);
        }

        column->filter(filter);
        auto result = column;
        auto data_result = std::static_pointer_cast<Int32Column>(result->data_column());

        ASSERT_EQ(50, result->size());
        for (int j = 0; j < 50; ++j) {
            if ((j + 50) % 3) {
                ASSERT_TRUE(column->is_null(j + 50));
                ASSERT_TRUE(result->is_null(j));
            } else {
                ASSERT_EQ(j + 50, data_result->get_data()[j]);
            }
        }
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_append_strings) {
    std::vector<Slice> values{{"hello"}, {"starrocks"}};
    auto c1 = Int32Column::create();
    auto nullable_c1 = NullableColumn::create(c1, NullColumn::create());
    ASSERT_FALSE(c1->append_strings(values));
    ASSERT_FALSE(nullable_c1->append_strings(values));
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_append_numbers) {
    std::vector<int32_t> values{1, 2, 3, 4, 5};
    void* buff = values.data();
    size_t length = values.size() * sizeof(values[0]);

    // FixedLengthColumn
    auto c1 = Int32Column::create();
    ASSERT_EQ(values.size(), c1->append_numbers(buff, length));
    ASSERT_EQ(values.size(), c1->size());
    for (size_t i = 0; i < values.size(); i++) {
        auto* p = reinterpret_cast<const int32_t*>(c1->raw_data());
        ASSERT_EQ(values[i], p[i]);
    }

    // Nullable FixedLengthColumn
    auto c2 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    ASSERT_EQ(values.size(), c2->append_numbers(buff, length));
    ASSERT_EQ(values.size(), c2->size());
    for (size_t i = 0; i < values.size(); i++) {
        auto* p = reinterpret_cast<const int32_t*>(c2->data_column()->raw_data());
        ASSERT_EQ(values[i], p[i]);
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_append_nulls) {
    // FixedLengthColumn
    auto c1 = Int32Column::create();
    ASSERT_FALSE(c1->append_nulls(10));

    // NullableColumn
    auto c2 = NullableColumn::create(Int64Column::create(), NullColumn::create());
    ASSERT_TRUE(c2->append_nulls(10));
    ASSERT_EQ(10U, c2->size());
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(c2->is_null(i));
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_append_defaults) {
    // FixedLengthColumn
    auto c1 = Int32Column::create();
    c1->append_default(10);
    ASSERT_EQ(10U, c1->size());
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(0, c1->get_data()[i]);
    }

    // NullableColumn
    auto c2 = NullableColumn::create(Int16Column::create(), NullColumn::create());
    c2->append_default(10);
    ASSERT_EQ(10U, c2->size());
    for (int i = 0; i < 10; i++) {
        ASSERT_TRUE(c2->is_null(i));
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_compare_at) {
    // int32 basic
    {
        std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        c2->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(0, c1->compare_at(i, i, *c2, -1));
            ASSERT_EQ(0, c2->compare_at(i, i, *c1, -1));
        }
        for (size_t i = 0; i < numbers.size(); i++) {
            for (size_t j = i + 1; j < numbers.size(); j++) {
                ASSERT_LT(c1->compare_at(i, j, *c2, -1), 0);
                ASSERT_GT(c2->compare_at(j, i, *c1, -1), 0);
            }
        }
    }
    // int32 boundary test
    {
        std::vector<int32_t> numbers{-2147483648, 1514736000, 1577808000, 2147483647};
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        c2->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(0, c1->compare_at(i, i, *c2, -1));
            ASSERT_EQ(0, c2->compare_at(i, i, *c1, -1));
        }
        for (size_t i = 0; i < numbers.size(); i++) {
            for (size_t j = i + 1; j < numbers.size(); j++) {
                ASSERT_LT(c1->compare_at(i, j, *c2, -1), 0);
                ASSERT_GT(c2->compare_at(j, i, *c1, -1), 0);
            }
        }
    }
    // double
    {
        std::vector<double> numbers{1, 2, 3, 4, 5, 6, 7};
        auto c1 = DoubleColumn::create();
        auto c2 = DoubleColumn::create();
        c1->append_numbers(numbers.data(), numbers.size() * sizeof(double));
        c1->append(34315.800000033356);
        c2->append_numbers(numbers.data(), numbers.size() * sizeof(double));
        c2->append(34315.78000003359);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(0, c1->compare_at(i, i, *c2, -1));
            ASSERT_EQ(0, c2->compare_at(i, i, *c1, -1));
        }
        for (size_t i = 0; i < numbers.size(); i++) {
            for (size_t j = i + 1; j < numbers.size(); j++) {
                ASSERT_LT(c1->compare_at(i, j, *c2, -1), 0);
                ASSERT_GT(c2->compare_at(j, i, *c1, -1), 0);
            }
        }
        ASSERT_EQ(1, c1->compare_at(7, 7, *c2, -1));
        ASSERT_EQ(-1, c2->compare_at(7, 7, *c1, -1));
        ASSERT_EQ(0, c1->compare_at(7, 7, *c1, -1));
    }
    // nullable int32
    {
        std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
        auto c1 = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto c2 = NullableColumn::create(Int32Column::create(), NullColumn::create());
        c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        c2->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(0, c1->compare_at(i, i, *c2, -1));
            ASSERT_EQ(0, c2->compare_at(i, i, *c1, -1));
        }
        for (size_t i = 0; i < numbers.size(); i++) {
            for (size_t j = i + 1; j < numbers.size(); j++) {
                ASSERT_LT(c1->compare_at(i, j, *c2, -1), 0);
                ASSERT_GT(c2->compare_at(j, i, *c1, -1), 0);
            }
        }
        // compare with null
        size_t idx = numbers.size();
        c1->append_nulls(1);
        c2->append_nulls(1);
        ASSERT_EQ(0, c1->compare_at(idx, idx, *c2, -1));
        ASSERT_EQ(0, c1->compare_at(idx, idx, *c2, +1));

        ASSERT_LT(c1->compare_at(idx, 0, *c2, -1), 0);
        ASSERT_GT(c2->compare_at(0, idx, *c1, -1), 0);

        ASSERT_GT(c1->compare_at(idx, 0, *c2, +1), 0);
        ASSERT_LT(c2->compare_at(0, idx, *c1, +1), 0);
    }
    // big int
    {
        auto c1 = NullableColumn::create(Int64Column::create(), NullColumn::create());
        auto c2 = NullableColumn::create(Int64Column::create(), NullColumn::create());
        c1->append_datum(Datum(int64_t(53988727729812)));
        c2->append_datum(Datum(int64_t(10872854479952)));
        ASSERT_EQ(1, c1->compare_at(0, 0, *c2, -1));
        ASSERT_EQ(-1, c2->compare_at(0, 0, *c1, -1));
        ASSERT_EQ(0, c1->compare_at(0, 0, *c1, -1));
    }
    // large int
    {
        auto c1 = NullableColumn::create(Int128Column::create(), NullColumn::create());
        auto c2 = NullableColumn::create(Int128Column::create(), NullColumn::create());
        c1->append_datum(Datum(int128_t(5398872772981245727)));
        c2->append_datum(Datum(int128_t(1087285447995287452)));
        ASSERT_EQ(1, c1->compare_at(0, 0, *c2, -1));
        ASSERT_EQ(-1, c2->compare_at(0, 0, *c1, -1));
        ASSERT_EQ(0, c1->compare_at(0, 0, *c1, -1));
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_decimal) {
    auto dc = DecimalColumn::create();

    dc->append(DecimalV2Value(1));
    dc->append(DecimalV2Value(2));
    dc->append(DecimalV2Value(3));

    ASSERT_EQ(DecimalV2Value(1), dc->get_data()[0]);
    ASSERT_EQ(DecimalV2Value(2), dc->get_data()[1]);
    ASSERT_EQ(DecimalV2Value(3), dc->get_data()[2]);

    ASSERT_EQ(DecimalV2Value(2), dc->get_data()[0] + DecimalV2Value(1));
    ASSERT_EQ(DecimalV2Value(3), dc->get_data()[1] + DecimalV2Value(1));
    ASSERT_EQ(DecimalV2Value(4), dc->get_data()[2] + DecimalV2Value(1));

    ASSERT_EQ(DecimalV2Value(0), dc->get_data()[0] % DecimalV2Value(1));
    ASSERT_EQ(DecimalV2Value(0), dc->get_data()[1] % DecimalV2Value(1));
    ASSERT_EQ(DecimalV2Value(0), dc->get_data()[2] % DecimalV2Value(1));
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_append_numeric) {
    auto c1 = FixedLengthColumn<int64_t>::create();
    auto c2 = FixedLengthColumn<int64_t>::create();
    c1->append(0);

    c2->append(1);
    c2->append(2);

    c1->append(*c2, 0, 0);
    EXPECT_EQ(1u, c1->size());
    EXPECT_EQ(0, c1->get(0).get_int64());

    c1->append(*c2, 0, 2);
    EXPECT_EQ(3u, c1->size());
    EXPECT_EQ(0, c1->get(0).get_int64());
    EXPECT_EQ(1, c1->get(1).get_int64());
    EXPECT_EQ(2, c1->get(2).get_int64());

    c1->append(*c2, 1, 1);
    EXPECT_EQ(4u, c1->size());
    EXPECT_EQ(0, c1->get(0).get_int64());
    EXPECT_EQ(1, c1->get(1).get_int64());
    EXPECT_EQ(2, c1->get(2).get_int64());
    EXPECT_EQ(2, c1->get(3).get_int64());
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_append_nullable_numeric) {
    auto c1 = NullableColumn::create(FixedLengthColumn<int64_t>::create(), NullColumn::create());
    auto c2 = NullableColumn::create(FixedLengthColumn<int64_t>::create(), NullColumn::create());
    c1->append_datum(Datum((int64_t)0));

    c2->append_nulls(1);
    c2->append_datum(Datum((int64_t)100));

    c1->append(*c2, 0, 0);
    EXPECT_EQ(1u, c1->size());
    EXPECT_FALSE(c1->is_null(0));
    EXPECT_EQ(0, c1->get(0).get_int64());

    c1->append(*c2, 0, 2);
    EXPECT_EQ(3u, c1->size());
    EXPECT_FALSE(c1->is_null(0));
    EXPECT_EQ(0, c1->get(0).get_int64());
    EXPECT_TRUE(c1->is_null(1));
    EXPECT_FALSE(c1->is_null(2));
    EXPECT_EQ(100, c1->get(2).get_int64());

    c1->append(*c2, 1, 1);
    EXPECT_EQ(4u, c1->size());
    EXPECT_FALSE(c1->is_null(0));
    EXPECT_EQ(0, c1->get(0).get_int64());
    EXPECT_TRUE(c1->is_null(1));
    EXPECT_FALSE(c1->is_null(2));
    EXPECT_EQ(100, c1->get(2).get_int64());
    EXPECT_FALSE(c1->is_null(3));
    EXPECT_EQ(100, c1->get(3).get_int64());
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_assign) {
    // int32
    {
        std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        c2->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));

        c1->assign(c1->size(), 0);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(c1->get_data()[i], 1);
        }

        c2->assign(c2->size(), 3);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(c2->get_data()[i], 4);
        }
    }
    // nullable int32
    {
        std::vector<int32_t> numbers{1, 2, 3, 4, 5, 6, 7};
        auto c1 = NullableColumn::create(Int32Column::create(), NullColumn::create());
        auto c2 = NullableColumn::create(Int32Column::create(), NullColumn::create());
        c1->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        c1->append_nulls(1);
        c2->append_numbers(numbers.data(), numbers.size() * sizeof(int32_t));
        c2->append_nulls(1);

        c1->assign(c1->size(), 0);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(c1->is_null(i), false);
            ASSERT_EQ(c1->get(i).get_int32(), 1);
        }

        c2->assign(c2->size(), 7);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(c2->is_null(i), true);
        }
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_reset_column) {
    std::vector<int> numbers{1, 2, 3};
    auto c1 = FixedLengthColumn<int>::create();
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(int));
    c1->set_delete_state(DEL_PARTIAL_SATISFIED);

    c1->reset_column();
    ASSERT_EQ(0, c1->size());
    ASSERT_EQ(0, c1->get_data().size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c1->delete_state());
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_swap_column) {
    std::vector<int> numbers{1, 2, 3};
    auto c1 = FixedLengthColumn<int>::create();
    c1->append_numbers(numbers.data(), numbers.size() * sizeof(int));
    c1->set_delete_state(DEL_PARTIAL_SATISFIED);

    auto c2 = FixedLengthColumn<int>::create();

    c1->swap_column(*c2);

    ASSERT_EQ(0, c1->size());
    ASSERT_EQ(0, c1->get_data().size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c1->delete_state());

    ASSERT_EQ(3, c2->size());
    ASSERT_EQ(3, c2->get_data().size());
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, c2->delete_state());
    ASSERT_EQ(1, c2->get_data()[0]);
    ASSERT_EQ(2, c2->get_data()[1]);
    ASSERT_EQ(3, c2->get_data()[2]);
}

TEST(FixedLengthColumnTest, test_update_rows) {
    auto column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i < 100; i++) {
        column->append(i);
    }

    ASSERT_EQ(true, column->is_numeric());
    ASSERT_EQ(100, column->size());
    ASSERT_EQ(100 * 4, column->byte_size());

    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(column->get_data()[i], i);
    }

    auto replace_column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i < 10; i++) {
        replace_column->append(i + 100);
    }

    std::vector<uint32_t> replace_idxes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    column->update_rows(*replace_column.get(), replace_idxes.data());

    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(column->get_data()[i], i + 100);
    }

    for (int i = 10; i < 100; i++) {
        ASSERT_EQ(column->get_data()[i], i);
    }
}

TEST(FixedLengthColumnTest, test_xor_checksum) {
    auto column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i <= 100; i++) {
        column->append(i);
    }

    int64_t checksum = column->xor_checksum(0, 101);
    int64_t expected_checksum = 100;

    ASSERT_EQ(checksum, expected_checksum);
}

TEST(FixedLengthColumnTest, test_compare_row) {
    auto column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i <= 100; i++) {
        column->append(i);
    }

    CompareVector cmp_vector(column->size());

    // ascending
    EXPECT_EQ(1, compare_column(column, cmp_vector, {30}, SortDesc(1, 1)));
    EXPECT_EQ(30, std::count(cmp_vector.begin(), cmp_vector.end(), -1));
    EXPECT_EQ(70, std::count(cmp_vector.begin(), cmp_vector.end(), 1));
    EXPECT_EQ(1, std::count(cmp_vector.begin(), cmp_vector.end(), 0));

    // descending
    std::fill(cmp_vector.begin(), cmp_vector.end(), 0);
    EXPECT_EQ(1, compare_column(column, cmp_vector, {30}, SortDesc(-1, 1)));
    EXPECT_EQ(70, std::count(cmp_vector.begin(), cmp_vector.end(), -1));
    EXPECT_EQ(30, std::count(cmp_vector.begin(), cmp_vector.end(), 1));
    EXPECT_EQ(1, std::count(cmp_vector.begin(), cmp_vector.end(), 0));
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_upgrade_if_overflow) {
    auto column = FixedLengthColumn<uint32_t>::create();
    for (int i = 0; i < 10; i++) {
        column->append(i);
    }

    auto ret = column->upgrade_if_overflow();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_EQ(column->size(), 10);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(column->get(i).get_uint32(), i);
    }

#ifdef NDEBUG
    // This case will alloc a lot of memory and run slowly
    auto large_column = FixedLengthColumn<uint8_t>::create();
    large_column->resize(Column::MAX_CAPACITY_LIMIT + 5);
    ret = large_column->upgrade_if_overflow();
    ASSERT_FALSE(ret.ok());
#endif
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_fixed_length_column_downgrade) {
    auto column = FixedLengthColumn<uint32_t>::create();
    column->append(1);
    auto ret = column->downgrade();
    ASSERT_TRUE(ret.ok());
    ASSERT_TRUE(ret.value() == nullptr);
    ASSERT_FALSE(column->has_large_column());
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_replicate) {
    auto column = FixedLengthColumn<int32_t>::create();
    column->append(7);
    column->append(3);

    Offsets offsets;
    offsets.push_back(0);
    offsets.push_back(3);
    offsets.push_back(5);

    auto c2 = column->replicate(offsets);
    ASSERT_EQ(5, c2->size());
    ASSERT_EQ(c2->get(0).get_int32(), 7);
    ASSERT_EQ(c2->get(1).get_int32(), 7);
    ASSERT_EQ(c2->get(2).get_int32(), 7);
    ASSERT_EQ(c2->get(3).get_int32(), 3);
    ASSERT_EQ(c2->get(4).get_int32(), 3);
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_fill_range) {
    std::vector<int64_t> values{1, 2, 3, 4, 5};
    void* buff = values.data();
    size_t length = values.size() * sizeof(values[0]);

    auto c1 = Int64Column::create();
    ASSERT_EQ(values.size(), c1->append_numbers(buff, length));
    ASSERT_EQ(values.size(), c1->size());

    std::vector<int64_t> ids{0, 0, 0};
    std::vector<uint8_t> filter{1, 0, 1, 0, 1};
    c1->fill_range(ids, filter);

    auto* p = reinterpret_cast<const int64_t*>(c1->raw_data());
    ASSERT_EQ(0, p[0]);
    ASSERT_EQ(values[1], p[1]);
    ASSERT_EQ(0, p[2]);
    ASSERT_EQ(values[3], p[3]);
    ASSERT_EQ(0, p[4]);
}

} // namespace starrocks
