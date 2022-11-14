// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/decimalv3_column.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/decimalv3.h"

namespace starrocks::vectorized {

template <typename T>
ColumnPtr create_decimal_column(int precision, int scale, size_t num_rows, const std::string& prefix) {
    auto col = DecimalV3Column<T>::create(precision, scale, num_rows);
    auto& data = down_cast<DecimalV3Column<T>*>(col.get())->get_data();
    for (auto i = 0; i < num_rows; ++i) {
        std::string s = prefix + std::to_string(i);
        DecimalV3Cast::from_string<T>(&data[i], precision, scale, s.c_str(), s.size());
    }
    return col;
}
// NOLINTNEXTLINE
TEST(DecimalV3ColumnTest, test_crc32_hash_decimal128p27s9) {
    constexpr auto num_rows = 17;
    auto col0 = create_decimal_column<int128_t>(27, 9, num_rows, "123456789.111");
    std::vector<uint32_t> hash0(num_rows, 0);
    col0->crc32_hash(&hash0.front(), 0, num_rows);

    auto col1 = DecimalColumn::create();
    auto& data0 = ColumnHelper::cast_to_raw<TYPE_DECIMAL128>(col0)->get_data();
    auto& data1 = ColumnHelper::cast_to_raw<TYPE_DECIMALV2>(col1)->get_data();
    std::swap((DecimalColumn::Container&)data0, data1);
    std::vector<uint32_t> hash1(num_rows, 0);
    col1->crc32_hash(&hash1.front(), 0, num_rows);
    for (auto i = 0; i < num_rows; ++i) {
        ASSERT_EQ(hash0[i], hash1[i]);
    }
}

// NOLINTNEXTLINE
TEST(DecimalV3ColumnTest, test_crc32_hash_decimal128p27s10) {
    constexpr auto num_rows = 17;
    auto col0 = create_decimal_column<int128_t>(27, 10, num_rows, "123456789.111");
    std::vector<uint32_t> hash0(num_rows, 0);
    col0->crc32_hash(&hash0.front(), 0, num_rows);

    auto col1 = Int128Column::create();
    auto& data0 = ColumnHelper::cast_to_raw<TYPE_DECIMAL128>(col0)->get_data();
    auto& data1 = ColumnHelper::cast_to_raw<TYPE_LARGEINT>(col1)->get_data();
    std::swap(data0, data1);
    std::vector<uint32_t> hash1(num_rows, 0);
    col1->crc32_hash(&hash1.front(), 0, num_rows);
    for (auto i = 0; i < num_rows; ++i) {
        ASSERT_EQ(hash0[i], hash1[i]);
    }
}

// NOLINTNEXTLINE
TEST(DecimalV3ColumnTest, test_crc32_hash_decimal64p15s6) {
    constexpr auto num_rows = 17;
    auto col0 = create_decimal_column<int64_t>(15, 6, num_rows, "123456.65");
    std::vector<uint32_t> hash0(17, 0);
    col0->crc32_hash(&hash0.front(), 0, num_rows);

    auto col1 = Int64Column::create();
    auto& data0 = ColumnHelper::cast_to_raw<TYPE_DECIMAL64>(col0)->get_data();
    auto& data1 = ColumnHelper::cast_to_raw<TYPE_BIGINT>(col1)->get_data();
    std::swap(data0, data1);
    std::vector<uint32_t> hash1(17, 0);
    col1->crc32_hash(&hash1.front(), 0, num_rows);
    for (auto i = 0; i < num_rows; ++i) {
        ASSERT_EQ(hash0[i], hash1[i]);
    }
}

TEST(DecimalV3ColumnTest, test_xor_checksum_decimal128p27s10) {
    constexpr auto num_rows = 101;
    auto col0 = create_decimal_column<int128_t>(27, 10, num_rows, "18446744073709551616.1");
    int64_t checksum = col0->xor_checksum(0, 101);
    int64_t expected_checksum = 9995422848;
    ASSERT_EQ(checksum, expected_checksum);
}

} // namespace starrocks::vectorized
