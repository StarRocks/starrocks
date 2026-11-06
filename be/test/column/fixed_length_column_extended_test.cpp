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

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/column_sorter_comparator.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/raw_data_visitor.h"
#include "column/sorting/sorting.h"
#include "column/vectorized_fwd.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

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
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(column.get())->get_data()[i], i);
    }

    column = FixedLengthColumn<int32_t>::create(100);
    ASSERT_EQ(100, column->size());
    for (int i = 0; i < 100; i++) {
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(column.get())->get_data()[i], 0);
    }
    {
        column = FixedLengthColumn<int32_t>::create();
        for (int i = 0; i < 100; i++) {
            column->append(i);
        }

        Filter filter;
        for (int i = 0; i < 100; ++i) {
            filter.emplace_back(i % 2);
        }

        column->filter(filter);
        auto re = std::move(column);
        ASSERT_EQ(50, re->size());

        for (int k = 0; k < 50; ++k) {
            ASSERT_EQ(k * 2 + 1, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(re.get())->get_data()[k]);
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

    RawDataVisitor rv;
    ASSERT_OK(column->accept(&rv));
    const auto* data = reinterpret_cast<const int32_t*>(rv.result());
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

    ASSERT_OK(column->accept(&rv));
    data = reinterpret_cast<const int32_t*>(rv.result());
    for (int i = 0; i < 100; i++) {
        if (i % 3) {
            ASSERT_EQ(true, column->is_null(i));
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
            filter.emplace_back(0);
        }

        for (int k = 0; k < 50; ++k) {
            filter.emplace_back(1);
        }

        column->filter(filter);
        auto result = std::move(column);
        auto* nullable_col = down_cast<NullableColumn*>(result.get());
        auto* data_result = down_cast<Int32Column*>(nullable_col->data_column_raw_ptr());

        ASSERT_EQ(50, result->size());
        for (int j = 0; j < 50; ++j) {
            if ((j + 50) % 3) {
                ASSERT_TRUE(result->is_null(j + 50));
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
    ASSERT_FALSE(c1->append_strings(values));

    auto nullable_c1 = NullableColumn::create(std::move(c1), NullColumn::create());
    ASSERT_FALSE(nullable_c1->append_strings(values.data(), values.size()));
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
    const auto& p = c1->immutable_data();
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], p[i]);
    }

    // Nullable FixedLengthColumn
    auto c2 = NullableColumn::create(Int32Column::create(), NullColumn::create());
    ASSERT_EQ(values.size(), c2->append_numbers(buff, length));
    ASSERT_EQ(values.size(), c2->size());
    const auto& datas = GetContainer<TYPE_INT>::get_data(c2.get());
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(values[i], datas[i]);
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
        ASSERT_EQ(0, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i]);
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
    EXPECT_EQ("[0, 1, 2]", c1->debug_string());

    c1->append(*c2, 1, 1);
    EXPECT_EQ("[0, 1, 2, 2]", c1->debug_string());
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
    EXPECT_EQ("[0, NULL, 100]", c1->debug_string());

    c1->append(*c2, 1, 1);
    EXPECT_EQ("[0, NULL, 100, 100]", c1->debug_string());
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
            ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data()[i], 1);
        }

        c2->assign(c2->size(), 3);
        for (size_t i = 0; i < numbers.size(); i++) {
            ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[i], 4);
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
    ASSERT_EQ(0, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data().size());
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
    ASSERT_EQ(0, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c1.get())->get_data().size());
    ASSERT_EQ(DEL_NOT_SATISFIED, c1->delete_state());

    ASSERT_EQ(3, c2->size());
    ASSERT_EQ(3, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data().size());
    ASSERT_EQ(DEL_PARTIAL_SATISFIED, c2->delete_state());
    ASSERT_EQ(1, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[0]);
    ASSERT_EQ(2, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[1]);
    ASSERT_EQ(3, ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(c2.get())->get_data()[2]);
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
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(column.get())->get_data()[i], i);
    }

    auto replace_column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i < 10; i++) {
        replace_column->append(i + 100);
    }

    std::vector<uint32_t> replace_idxes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    column->update_rows(*replace_column.get(), replace_idxes.data());

    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(column.get())->get_data()[i], i + 100);
    }

    for (int i = 10; i < 100; i++) {
        ASSERT_EQ(ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(column.get())->get_data()[i], i);
    }
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_murmur_hash) {
    auto column = Int32Column::create();
    column->append(34);

    std::vector<uint32_t> hash_values(1);
    column->murmur_hash3_x86_32(hash_values.data(), 0, 1);

    ASSERT_EQ(2017239379, hash_values[0]);
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_murmur_hash_int64) {
    auto column = Int64Column::create();
    column->append(34);

    std::vector<uint32_t> hash_values(1);
    column->murmur_hash3_x86_32(hash_values.data(), 0, 1);

    ASSERT_EQ(2017239379, hash_values[0]);
}

// NOLINTNEXTLINE
TEST(FixedLengthColumnTest, test_murmur_hash_date) {
    auto column = DateColumn::create();
    DateValue date = DateValue::create(2017, 11, 16);
    column->append(date);

    std::vector<uint32_t> hash_values(1);
    column->murmur_hash3_x86_32(hash_values.data(), 0, 1);

    ASSERT_EQ(-653330422, *reinterpret_cast<int32_t*>(&hash_values[0]));
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
    auto mutable_column = FixedLengthColumn<int32_t>::create();
    for (int i = 0; i <= 100; i++) {
        mutable_column->append(i);
    }

    CompareVector cmp_vector(mutable_column->size());

    // ascending
    ColumnPtr column = std::move(mutable_column);
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
TEST(FixedLengthColumnTest, test_replicate) {
    auto column = FixedLengthColumn<int32_t>::create();
    column->append(7);
    column->append(3);

    Buffer<uint32_t> offsets;
    offsets.emplace_back(0);
    offsets.emplace_back(3);
    offsets.emplace_back(5);

    auto c2 = column->replicate(offsets).value();
    ASSERT_EQ("[7, 7, 7, 3, 3]", c2->debug_string());
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
    Filter filter{1, 0, 1, 0, 1};
    ASSERT_TRUE(c1->fill_range(ids, filter).ok());

    const auto& p = c1->immutable_data();
    ASSERT_EQ(0, p[0]);
    ASSERT_EQ(values[1], p[1]);
    ASSERT_EQ(0, p[2]);
    ASSERT_EQ(values[3], p[3]);
    ASSERT_EQ(0, p[4]);
}

namespace {

template <typename T>
int scalar_oracle_compare_val(const T& lhs, const T& rhs) {
    if constexpr (std::is_same_v<T, TimestampValue>) {
        int64_t l = lhs.timestamp();
        int64_t r = rhs.timestamp();
        return (l < r) ? -1 : ((l > r) ? 1 : 0);
    } else {
        return SorterComparator<T>::compare(lhs, rhs);
    }
}

template <typename ColType, typename T>
void test_simd_int64_type_driver(const std::vector<size_t>& test_sizes, const std::vector<int>& tie_ratios,
                                 const std::vector<int>& sort_orders, const std::vector<T>& boundary_values) {
    std::mt19937_64 rng(0xABCD1234E);

    for (size_t size : test_sizes) {
        for (int sort_order : sort_orders) {
            for (int tie_ratio : tie_ratios) {
                for (const auto& rhs_val : boundary_values) {
                    auto col = ColType::create();
                    std::vector<T> lhs_vals(size);
                    for (size_t i = 0; i < size; ++i) {
                        if (i % 3 == 0 && !boundary_values.empty()) {
                            lhs_vals[i] = boundary_values[(i / 3) % boundary_values.size()];
                        } else {
                            if constexpr (std::is_same_v<T, TimestampValue>) {
                                TimestampValue tv;
                                tv.set_timestamp(static_cast<int64_t>(rng()));
                                lhs_vals[i] = tv;
                            } else {
                                lhs_vals[i] = static_cast<T>(rng());
                            }
                        }
                        col->append(lhs_vals[i]);
                    }

                    CompareVector cmp_actual(size, 0);
                    CompareVector cmp_expected(size, 0);
                    std::uniform_int_distribution<int> pct_dist(0, 99);
                    std::uniform_int_distribution<int> sign_dist(0, 1);
                    for (size_t i = 0; i < size; ++i) {
                        int8_t init_val = 0;
                        if (tie_ratio < 100 && pct_dist(rng) >= tie_ratio) {
                            init_val = sign_dist(rng) == 0 ? -1 : 1;
                        }
                        cmp_actual[i] = init_val;
                        cmp_expected[i] = init_val;
                    }

                    size_t oracle_equal_count = 0;
                    for (size_t i = 0; i < size; ++i) {
                        if (cmp_expected[i] == 0) {
                            int c = scalar_oracle_compare_val(lhs_vals[i], rhs_val) * sort_order;
                            cmp_expected[i] = static_cast<int8_t>(c);
                            if (c == 0) {
                                oracle_equal_count++;
                            }
                        }
                    }

                    int actual_equal_count = compare_column(col, cmp_actual, Datum(rhs_val), SortDesc(sort_order, 1));
                    ASSERT_EQ(actual_equal_count, static_cast<int>(oracle_equal_count));
                    ASSERT_EQ(cmp_actual.size(), cmp_expected.size());
                    for (size_t i = 0; i < size; ++i) {
                        ASSERT_EQ(cmp_actual[i], cmp_expected[i])
                                << "Direct column mismatch at i=" << i << ", size=" << size
                                << ", sort_order=" << sort_order << ", tie_ratio=" << tie_ratio;
                    }

                    // Odd-offset sliced column test via Filter
                    if (size > 0) {
                        auto sliced_col = col->clone();
                        Filter filter(size, 1);
                        filter[0] = 0; // Drop the first element -> odd offset slicing
                        sliced_col->filter(filter);
                        size_t sliced_size = size - 1;

                        CompareVector sliced_cmp_actual(sliced_size, 0);
                        CompareVector sliced_cmp_expected(sliced_size, 0);
                        for (size_t i = 0; i < sliced_size; ++i) {
                            int8_t init_val = 0;
                            if (tie_ratio < 100 && pct_dist(rng) >= tie_ratio) {
                                init_val = sign_dist(rng) == 0 ? -1 : 1;
                            }
                            sliced_cmp_actual[i] = init_val;
                            sliced_cmp_expected[i] = init_val;
                        }

                        size_t sliced_oracle_equal_count = 0;
                        for (size_t i = 0; i < sliced_size; ++i) {
                            if (sliced_cmp_expected[i] == 0) {
                                int c = scalar_oracle_compare_val(lhs_vals[i + 1], rhs_val) * sort_order;
                                sliced_cmp_expected[i] = static_cast<int8_t>(c);
                                if (c == 0) {
                                    sliced_oracle_equal_count++;
                                }
                            }
                        }

                        int sliced_actual_count =
                                compare_column(sliced_col, sliced_cmp_actual, Datum(rhs_val), SortDesc(sort_order, 1));
                        ASSERT_EQ(sliced_actual_count, static_cast<int>(sliced_oracle_equal_count));
                        ASSERT_EQ(sliced_cmp_actual.size(), sliced_cmp_expected.size());
                        for (size_t i = 0; i < sliced_size; ++i) {
                            ASSERT_EQ(sliced_cmp_actual[i], sliced_cmp_expected[i])
                                    << "Sliced column mismatch at i=" << i << ", sliced_size=" << sliced_size
                                    << ", sort_order=" << sort_order << ", tie_ratio=" << tie_ratio;
                        }
                    }
                }
            }
        }
    }
}

} // namespace

TEST(FixedLengthColumnTest, test_compare_column_simd_int64_comprehensive) {
    const std::vector<size_t> test_sizes = {0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 100, 4096, 4097};
    const std::vector<int> tie_ratios = {0, 1, 12, 50, 90, 100};
    const std::vector<int> sort_orders = {1, -1};

    // 1. Int64Column
    {
        const std::vector<int64_t> int64_boundary_values = {
                std::numeric_limits<int64_t>::min(),     std::numeric_limits<int64_t>::min() + 1, -1, 0, 1,
                std::numeric_limits<int64_t>::max() - 1, std::numeric_limits<int64_t>::max(),
        };
        test_simd_int64_type_driver<Int64Column, int64_t>(test_sizes, tie_ratios, sort_orders, int64_boundary_values);
    }

    // 2. UInt64Column
    {
        const std::vector<uint64_t> uint64_boundary_values = {
                0ULL,
                1ULL,
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1ULL,
                std::numeric_limits<uint64_t>::max() - 1ULL,
                std::numeric_limits<uint64_t>::max(),
        };
        test_simd_int64_type_driver<UInt64Column, uint64_t>(test_sizes, tie_ratios, sort_orders,
                                                            uint64_boundary_values);
    }

    // 3. TimestampColumn (TimestampValue)
    {
        auto make_ts = [](int64_t v) {
            TimestampValue tv;
            tv.set_timestamp(v);
            return tv;
        };
        const std::vector<TimestampValue> ts_boundary_values = {
                make_ts(std::numeric_limits<int64_t>::min()),
                make_ts(std::numeric_limits<int64_t>::min() + 1),
                make_ts(-1),
                make_ts(0),
                make_ts(1),
                make_ts(std::numeric_limits<int64_t>::max() - 1),
                make_ts(std::numeric_limits<int64_t>::max()),
        };
        test_simd_int64_type_driver<TimestampColumn, TimestampValue>(test_sizes, tie_ratios, sort_orders,
                                                                     ts_boundary_values);
    }
}

} // namespace starrocks
