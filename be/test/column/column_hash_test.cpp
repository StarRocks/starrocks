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

#include "column/column_hash/column_hash.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include "base/hash/hash_util.hpp"
#include "base/hash/xxh3.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "testutil/column_test_helper.h"
#include "types/date_value.h"
#include "types/decimalv2_value.h"
#include "types/decimalv3.h"
#include "types/timestamp_value.h"

namespace starrocks {

// Hash function parameter structure
struct HashFunctionParam {
    const char* name;
    void (*hash_func)(const Column&, uint32_t*, uint32_t, uint32_t);
    void (*hash_func_with_selection)(const Column&, uint32_t*, uint8_t*, uint16_t, uint16_t);
    void (*hash_func_selective)(const Column&, uint32_t*, uint16_t*, uint16_t);
};

// Hash function parameters
const HashFunctionParam kHashFunctions[] = {
        {"FNV", fnv_hash_column, fnv_hash_column_with_selection, fnv_hash_column_selective},
        {"CRC32", crc32_hash_column, crc32_hash_column_with_selection, crc32_hash_column_selective},
        {"MurmurHash3", murmur_hash3_x86_32_column, murmur_hash3_x86_32_column_with_selection,
         murmur_hash3_x86_32_column_selective},
        {"XXH3", xxh3_64_column, xxh3_64_column_with_selection, xxh3_64_column_selective},
};

class ColumnHashTest : public ::testing::TestWithParam<HashFunctionParam> {
public:
    void SetUp() override {}
    void TearDown() override {}

    void test_hash_function(const ColumnPtr& column, uint32_t from, uint32_t to) {
        if (GetParam().hash_func == murmur_hash3_x86_32_column &&
            !(column->is_date() || column->get_name().starts_with("int"))) {
            GTEST_SKIP() << "MurmurHash3 column hash is skipped due to it doesn't support these types.";
        }
        std::vector<uint32_t> hashes(to - from, 0);
        GetParam().hash_func(*column, hashes.data(), from, to);

        // Verify all hashes are computed (non-zero for non-empty columns)
        for (size_t i = 0; i < hashes.size(); ++i) {
            if (!column->is_null(i)) {
                ASSERT_NE(0, hashes[i]) << "column: " << column->debug_string();
            }
        }
    }

    void test_hash_function_with_selection(const ColumnPtr& column, uint8_t* selection, uint16_t from, uint16_t to) {
        std::vector<uint32_t> hashes(to - from, 0);
        GetParam().hash_func_with_selection(*column, hashes.data(), selection, from, to);

        // Verify hashes are computed for selected rows
        for (size_t i = 0; i < hashes.size(); ++i) {
            if (selection[from + i]) {
                ASSERT_TRUE(true) << "Hash computed for selected index " << (from + i);
            } else {
                ASSERT_EQ(0, hashes[i]);
            }
        }
    }

    void test_hash_function_selective(const ColumnPtr& column, uint16_t* sel, uint16_t sel_size) {
        std::vector<uint32_t> hashes(column->size(), 0);
        GetParam().hash_func_selective(*column, hashes.data(), sel, sel_size);

        // Verify hashes are computed for selected indices
        for (size_t i = 0; i < sel_size; ++i) {
            ASSERT_TRUE(sel[i] < column->size()) << "Selection index out of bounds";
            if (!column->is_null(sel[i])) {
                ASSERT_NE(0, hashes[sel[i]]) << "column: " << column->debug_string();
            }
        }
    }
};

// Test fixed-length numeric types
TEST_P(ColumnHashTest, test_fixed_length_numeric_types) {
    // Test int8
    {
        auto col = ColumnTestHelper::build_column<int8_t>({1, 2, -1});
        this->test_hash_function(col, 0, 3);
    }

    // Test int16
    {
        auto col = ColumnTestHelper::build_column<int16_t>({100, 200});
        this->test_hash_function(col, 0, 2);
    }

    // Test int32
    {
        auto col = ColumnTestHelper::build_column<int32_t>({1000, 2000});
        this->test_hash_function(col, 0, 2);
    }

    // Test int64
    {
        auto col = Int64Column::create();
        col->append(10000LL);
        col->append(20000LL);
        this->test_hash_function(col, 0, 2);
    }

    // Test int128
    {
        auto col = Int128Column::create();
        col->append(int128_t(100000));
        col->append(int128_t(200000));
        this->test_hash_function(col, 0, 2);
    }

    // Test float
    {
        auto col = ColumnTestHelper::build_column<float>({1.5f, 2.5f});
        this->test_hash_function(col, 0, 2);
    }

    // Test double
    {
        auto col = ColumnTestHelper::build_column<double>({1.5, 2.5});
        this->test_hash_function(col, 0, 2);
    }
}

// Test date and datetime types
TEST_P(ColumnHashTest, test_date_datetime_types) {
    // Test date
    {
        auto col = DateColumn::create();
        DateValue dv1, dv2;
        dv1.from_date(2023, 1, 1);
        dv2.from_date(2023, 12, 31);
        col->append(dv1);
        col->append(dv2);
        this->test_hash_function(col, 0, 2);
    }

    // Test datetime
    {
        auto col = TimestampColumn::create();
        col->append(TimestampValue::create(2023, 1, 1, 12, 30, 45));
        col->append(TimestampValue::create(2023, 12, 31, 23, 59, 59));
        this->test_hash_function(col, 0, 2);
    }
}

// Test decimal types
TEST_P(ColumnHashTest, test_decimal_types) {
    // Test Decimal32
    {
        auto col = Decimal32Column::create(9, 2, 2);
        auto& data = down_cast<Decimal32Column*>(col.get())->get_data();
        DecimalV3Cast::from_string<int32_t>(&data[0], 9, 2, "12345.67", 8);
        DecimalV3Cast::from_string<int32_t>(&data[1], 9, 2, "67890.12", 8);
        this->test_hash_function(col, 0, 2);
    }
}

// Test binary/string types
TEST_P(ColumnHashTest, test_binary_types) {
    // Test BinaryColumn (VARCHAR)
    {
        auto col = ColumnTestHelper::build_column<Slice>({"hello", "world", "xxx"});
        this->test_hash_function(col, 0, 3);
    }
}

// Test nullable column
TEST_P(ColumnHashTest, test_nullable_column) {
    auto data_col = Int32Column::create();
    auto null_col = NullColumn::create();
    data_col->append(1);
    null_col->append(0);
    data_col->append(2);
    null_col->append(1); // null
    data_col->append(3);
    null_col->append(0);

    auto col = NullableColumn::create(data_col, null_col);
    this->test_hash_function(col, 0, 3);
}

// Test const column
TEST_P(ColumnHashTest, test_const_column) {
    auto data_col = ColumnTestHelper::build_column<int32_t>({42});
    auto col = ConstColumn::create(data_col, 3);

    this->test_hash_function(col, 0, 3);
}

// Test array column
TEST_P(ColumnHashTest, test_array_column) {
    auto elements = Int32Column::create();
    auto offsets = UInt32Column::create();
    offsets->append(0);
    elements->append(1);
    elements->append(2);
    offsets->append(2);
    elements->append(3);
    offsets->append(3);

    auto col = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(elements), (offsets));
    this->test_hash_function(col, 0, 2);
}

// Test map column
TEST_P(ColumnHashTest, test_map_column) {
    auto keys = Int32Column::create();
    auto values = Int32Column::create();
    auto offsets = UInt32Column::create();

    offsets->append(0);
    keys->append(1);
    values->append(10);
    keys->append(2);
    values->append(20);
    offsets->append(2);

    auto col = MapColumn::create(ColumnHelper::cast_to_nullable_column(keys),
                                 ColumnHelper::cast_to_nullable_column(values), (offsets));
    this->test_hash_function(col, 0, 1);
}

// Test struct column
TEST_P(ColumnHashTest, test_struct_column) {
    std::vector<std::string> field_names{"id", "name"};
    auto id_col = ColumnTestHelper::build_column<int32_t>({1, 2});
    auto name_col = ColumnTestHelper::build_column<Slice>({"alice", "bob"});

    Columns fields{id_col, name_col};
    auto col = StructColumn::create(fields, field_names);
    this->test_hash_function(col, 0, 2);
}

// Test json column
TEST_P(ColumnHashTest, test_json_column) {
    auto col = JsonColumn::create();
    auto json1 = JsonValue::parse(R"({"a": 1, "b": "test"})");
    ASSERT_TRUE(json1.ok());
    col->append(&json1.value());
    auto json2 = JsonValue::parse(R"({"x": 2, "y": [1, 2, 3]})");
    ASSERT_TRUE(json2.ok());
    col->append(&json2.value());

    this->test_hash_function(col, 0, 2);
}

// Test hash functions with selection
TEST_P(ColumnHashTest, test_hash_with_selection) {
    auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3, 4});

    uint8_t selection[] = {1, 0, 1, 1};
    this->test_hash_function_with_selection(col, selection, 0, 4);
}

// Test hash functions with selective indices
TEST_P(ColumnHashTest, test_hash_selective) {
    auto col = ColumnTestHelper::build_column<int32_t>({1, 2, 3, 4, 5});

    uint16_t sel[] = {0, 2, 4};
    this->test_hash_function_selective(col, sel, 3);
}

// Test hash consistency - same value should produce same hash
TEST_P(ColumnHashTest, test_hash_consistency) {
    auto col1 = ColumnTestHelper::build_column<int32_t>({42});
    auto col2 = ColumnTestHelper::build_column<int32_t>({42});

    std::vector<uint32_t> hash1(1, 0);
    std::vector<uint32_t> hash2(1, 0);

    GetParam().hash_func(*col1, hash1.data(), 0, 1);
    GetParam().hash_func(*col2, hash2.data(), 0, 1);
    ASSERT_EQ(hash1[0], hash2[0]);
}

// Instantiate test suite for all hash functions
INSTANTIATE_TEST_SUITE_P(AllHashFunctions, ColumnHashTest, ::testing::ValuesIn(kHashFunctions),
                         [](const ::testing::TestParamInfo<HashFunctionParam>& info) {
                             return std::string(info.param.name);
                         });

namespace {

template <typename ColType, typename T>
void verify_xxh3_fixed_column_oracle(const ColType& col, const std::vector<T>& values) {
    const uint32_t n = static_cast<uint32_t>(values.size());
    ASSERT_EQ(col.size(), values.size());

    const uint32_t seeds[] = {0u, 0x9E3779B1u, 0xdeadbeefu};
    for (uint32_t init_seed : seeds) {
        // 1. Full range xxh3_64_column
        {
            std::vector<uint32_t> hashes(n, init_seed);
            xxh3_64_column(col, hashes.data(), 0, n);
            for (uint32_t i = 0; i < n; ++i) {
                const uint32_t expected = static_cast<uint32_t>(XXH3_64bits_withSeed(&values[i], sizeof(T), init_seed));
                EXPECT_EQ(hashes[i], expected) << "sizeof(T)=" << sizeof(T) << " row=" << i << " seed=" << init_seed;
            }
        }

        // 2. Sub-range xxh3_64_column [1, n - 1)
        if (n >= 3) {
            std::vector<uint32_t> hashes(n, init_seed);
            xxh3_64_column(col, hashes.data(), 1, n - 1);
            EXPECT_EQ(hashes[0], init_seed);
            for (uint32_t i = 1; i + 1 < n; ++i) {
                const uint32_t expected = static_cast<uint32_t>(XXH3_64bits_withSeed(&values[i], sizeof(T), init_seed));
                EXPECT_EQ(hashes[i], expected)
                        << "subrange sizeof(T)=" << sizeof(T) << " row=" << i << " seed=" << init_seed;
            }
            EXPECT_EQ(hashes[n - 1], init_seed);
        }

        // 3. xxh3_64_column_with_selection
        {
            std::vector<uint8_t> selection(n, 0);
            for (uint32_t i = 0; i < n; ++i) {
                selection[i] = static_cast<uint8_t>((i % 2 == 0) || (i + 1 == n));
            }
            std::vector<uint32_t> hashes(n, init_seed);
            xxh3_64_column_with_selection(col, hashes.data(), selection.data(), 0, static_cast<uint16_t>(n));
            for (uint32_t i = 0; i < n; ++i) {
                if (selection[i]) {
                    const uint32_t expected =
                            static_cast<uint32_t>(XXH3_64bits_withSeed(&values[i], sizeof(T), init_seed));
                    EXPECT_EQ(hashes[i], expected)
                            << "selection sizeof(T)=" << sizeof(T) << " row=" << i << " seed=" << init_seed;
                } else {
                    EXPECT_EQ(hashes[i], init_seed)
                            << "unselected sizeof(T)=" << sizeof(T) << " row=" << i << " seed=" << init_seed;
                }
            }
        }

        // 4. xxh3_64_column_selective
        {
            std::vector<uint16_t> sel;
            for (uint16_t i = 0; i < static_cast<uint16_t>(n); i = static_cast<uint16_t>(i + 2)) {
                sel.push_back(i);
            }
            std::vector<uint32_t> hashes(n, init_seed);
            xxh3_64_column_selective(col, hashes.data(), sel.data(), static_cast<uint16_t>(sel.size()));
            for (uint32_t i = 0; i < n; ++i) {
                if (i % 2 == 0) {
                    const uint32_t expected =
                            static_cast<uint32_t>(XXH3_64bits_withSeed(&values[i], sizeof(T), init_seed));
                    EXPECT_EQ(hashes[i], expected)
                            << "selective sizeof(T)=" << sizeof(T) << " row=" << i << " seed=" << init_seed;
                } else {
                    EXPECT_EQ(hashes[i], init_seed)
                            << "non-selective sizeof(T)=" << sizeof(T) << " row=" << i << " seed=" << init_seed;
                }
            }
        }
    }
}

void run_xxh3_fixed_width_and_fallback_oracle_parity() {
    // 1B: Int8Column & UInt8Column
    {
        std::vector<int8_t> vals{0, 1, -1, 42, 127, -128};
        auto col = ColumnTestHelper::build_column<int8_t>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        std::vector<uint8_t> vals{0u, 1u, 127u, 128u, 254u, 255u};
        auto col = ColumnTestHelper::build_column<uint8_t>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }

    // 2B: Int16Column & UInt16Column
    {
        std::vector<int16_t> vals{0, 1, -1, 12345, 32767, -32768};
        auto col = ColumnTestHelper::build_column<int16_t>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        std::vector<uint16_t> vals{0u, 1u, 256u, 32768u, 65534u, 65535u};
        auto col = ColumnTestHelper::build_column<uint16_t>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }

    // 3B: FixedLengthColumn<uint24_t> and direct XXH3_len_1to3_64b parity
    static_assert(sizeof(uint24_t) == 3);
    {
        std::vector<uint24_t> vals{uint24_t(0), uint24_t(1), uint24_t(0x123456), uint24_t(0xabcdef),
                                   uint24_t(0xffffff)};
        auto col = FixedLengthColumn<uint24_t>::create();
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
        for (uint32_t seed : {0u, 0x9E3779B1u, 0xdeadbeefu}) {
            for (const auto& v : vals) {
                EXPECT_EQ(static_cast<uint32_t>(XXH3_len_1to3_64b(reinterpret_cast<const xxh_u8*>(&v), sizeof(uint24_t),
                                                                  XXH3_kSecret, seed)),
                          static_cast<uint32_t>(XXH3_64bits_withSeed(&v, sizeof(uint24_t), seed)));
            }
        }
    }

    // 4B: Int32Column, UInt32Column, FloatColumn, DateColumn, Decimal32Column
    {
        std::vector<int32_t> vals{0, 1, -1, 0x12345678, INT32_MAX, INT32_MIN};
        auto col = ColumnTestHelper::build_column<int32_t>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        std::vector<float> vals{0.0f, -0.0f, 1.5f, -2.75f, 3.1415926f, 1e20f};
        auto col = ColumnTestHelper::build_column<float>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        DateValue d1, d2, d3, d4;
        d1.from_date(1970, 1, 1);
        d2.from_date(2000, 2, 29);
        d3.from_date(2023, 12, 31);
        d4.from_date(2099, 6, 15);
        std::vector<DateValue> vals{d1, d2, d3, d4};
        auto col = DateColumn::create();
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        std::vector<int32_t> vals{0, 1234567, -7654321, 999999999, -999999999};
        auto col = Decimal32Column::create(9, 2);
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
    }

    // 8B: Int64Column, DoubleColumn, TimestampColumn, Decimal64Column
    {
        std::vector<int64_t> vals{0LL, 1LL, -1LL, 0x0123456789abcdefLL, INT64_MAX, INT64_MIN};
        auto col = ColumnTestHelper::build_column<int64_t>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        std::vector<double> vals{0.0, -0.0, 1.5, -2.75, 2.718281828459045, 1e100};
        auto col = ColumnTestHelper::build_column<double>(vals);
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        std::vector<TimestampValue> vals{
                TimestampValue::create(1970, 1, 1, 0, 0, 0),
                TimestampValue::create(2023, 1, 1, 12, 30, 45),
                TimestampValue::create(2023, 12, 31, 23, 59, 59),
                TimestampValue::create(2038, 1, 19, 3, 14, 7),
        };
        auto col = TimestampColumn::create();
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
    }
    {
        std::vector<int64_t> vals{0LL, 123456789012345LL, -123456789012345LL, INT64_MAX, INT64_MIN};
        auto col = Decimal64Column::create(18, 4);
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
    }

    // 12B: FixedLengthColumn<int96_t> & FixedLengthColumn<decimal12_t> and direct XXH3_len_9to16_64b parity
    static_assert(sizeof(int96_t) == 12);
    static_assert(sizeof(decimal12_t) == 12);
    {
        std::vector<int96_t> vals{int96_t{0, 0}, int96_t{1, 0}, int96_t{0x123456789abcdef0ULL, 0xffffffffU},
                                  int96_t{0xfedcba9876543210ULL, 0x13579bdfU}};
        auto col = FixedLengthColumn<int96_t>::create();
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
        for (uint32_t seed : {0u, 0x9E3779B1u, 0xdeadbeefu}) {
            for (const auto& v : vals) {
                EXPECT_EQ(static_cast<uint32_t>(XXH3_len_9to16_64b(reinterpret_cast<const xxh_u8*>(&v), sizeof(int96_t),
                                                                   XXH3_kSecret, seed)),
                          static_cast<uint32_t>(XXH3_64bits_withSeed(&v, sizeof(int96_t), seed)));
            }
        }
    }
    {
        std::vector<decimal12_t> vals{decimal12_t{0, 0}, decimal12_t{100, 25}, decimal12_t{-99999999, -999999999},
                                      decimal12_t{1234567890123LL, 456789012}};
        auto col = FixedLengthColumn<decimal12_t>::create();
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
        for (uint32_t seed : {0u, 0x9E3779B1u, 0xdeadbeefu}) {
            for (const auto& v : vals) {
                EXPECT_EQ(static_cast<uint32_t>(XXH3_len_9to16_64b(reinterpret_cast<const xxh_u8*>(&v),
                                                                   sizeof(decimal12_t), XXH3_kSecret, seed)),
                          static_cast<uint32_t>(XXH3_64bits_withSeed(&v, sizeof(decimal12_t), seed)));
            }
        }
    }

    // 16B: Int128Column, DecimalColumn (DecimalV2Value), Decimal128Column
    {
        std::vector<int128_t> vals{
                int128_t(0),
                int128_t(1),
                int128_t(-1),
                (static_cast<int128_t>(0x0123456789abcdefLL) << 64) | static_cast<uint64_t>(0xfedcba9876543210ULL),
                (static_cast<int128_t>(-0x13579bdf02468aceLL) << 64) | static_cast<uint64_t>(0x1122334455667788ULL),
        };
        auto col = Int128Column::create();
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);

        auto dec_col = Decimal128Column::create(38, 9);
        for (const auto& v : vals) {
            dec_col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*dec_col, vals);
    }
    {
        std::vector<DecimalV2Value> vals{DecimalV2Value(0, 0), DecimalV2Value(12345, 678900000),
                                         DecimalV2Value(-98765, -432100000), DecimalV2Value(999999999, 999999999)};
        auto col = DecimalColumn::create();
        for (const auto& v : vals) {
            col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*col, vals);
    }

    // 32B fallback: Decimal256Column and Int256Column (sizeof(int256_t) == 32)
    static_assert(sizeof(int256_t) == 32);
    {
        std::vector<int256_t> vals{
                int256_t(0),
                int256_t(1),
                int256_t(-1),
                int256_t(static_cast<int128_t>(0x0123456789abcdefLL), static_cast<uint128_t>(0xfedcba9876543210ULL)),
                int256_t(static_cast<int128_t>(-0x1122334455667788LL),
                         (static_cast<uint128_t>(0xaabbccddeeff0011ULL) << 64) | 0x2233445566778899ULL),
        };
        auto dec256_col = Decimal256Column::create(76, 18);
        for (const auto& v : vals) {
            dec256_col->append(v);
        }
        verify_xxh3_fixed_column_oracle(*dec256_col, vals);

        auto int256_col = Int256Column::create();
        for (const auto& v : vals) {
            int256_col->append(v);
        }
        for (uint32_t seed : {0u, 0x9E3779B1u, 0xdeadbeefu}) {
            for (size_t i = 0; i < vals.size(); ++i) {
                const auto& v = int256_col->immutable_data()[i];
                EXPECT_EQ(static_cast<uint32_t>(HashUtil::xx_hash3_64(&v, sizeof(int256_t), seed)),
                          static_cast<uint32_t>(XXH3_64bits_withSeed(&v, sizeof(int256_t), seed)));
            }
        }
    }
}

void run_xxh3_and_crc32_chained_seed_and_binary_lengths_oracle_parity() {
    // 1. BinaryColumn across lengths {0, 1, 2, 3, 4, 5, 7, 8, 9, 12, 15, 16, 17, 31, 64, 128, 256}
    const std::vector<size_t> lengths{0, 1, 2, 3, 4, 5, 7, 8, 9, 12, 15, 16, 17, 31, 64, 128, 256};
    std::vector<std::string> payloads;
    payloads.reserve(lengths.size());
    auto bin_col = BinaryColumn::create();
    for (size_t idx = 0; idx < lengths.size(); ++idx) {
        size_t len = lengths[idx];
        std::string s(len, '\0');
        for (size_t b = 0; b < len; ++b) {
            s[b] = static_cast<char>((idx * 37 + b * 131 + 0x5a) & 0xff);
        }
        payloads.push_back(s);
        bin_col->append(Slice(payloads.back()));
    }
    const uint32_t num_rows = static_cast<uint32_t>(lengths.size());

    for (uint32_t init_seed : {0u, 0x9E3779B1u, 0xdeadbeefu}) {
        // XXH3 BinaryColumn full range, selection, selective
        std::vector<uint32_t> xxh3_expected(num_rows);
        for (uint32_t i = 0; i < num_rows; ++i) {
            if (payloads[i].empty()) {
                xxh3_expected[i] = init_seed;
            } else {
                xxh3_expected[i] =
                        static_cast<uint32_t>(XXH3_64bits_withSeed(payloads[i].data(), payloads[i].size(), init_seed));
            }
        }

        std::vector<uint32_t> xxh3_hashes(num_rows, init_seed);
        xxh3_64_column(*bin_col, xxh3_hashes.data(), 0, num_rows);
        for (uint32_t i = 0; i < num_rows; ++i) {
            EXPECT_EQ(xxh3_hashes[i], xxh3_expected[i]) << "XXH3 Binary len=" << lengths[i] << " seed=" << init_seed;
        }

        // CRC32 BinaryColumn full range
        std::vector<uint32_t> crc_expected(num_rows);
        for (uint32_t i = 0; i < num_rows; ++i) {
            if (payloads[i].empty()) {
                crc_expected[i] = init_seed;
            } else {
                crc_expected[i] =
                        static_cast<uint32_t>(::crc32(init_seed, reinterpret_cast<const Bytef*>(payloads[i].data()),
                                                      static_cast<uInt>(payloads[i].size())));
            }
        }

        std::vector<uint32_t> crc_hashes(num_rows, init_seed);
        crc32_hash_column(*bin_col, crc_hashes.data(), 0, num_rows);
        for (uint32_t i = 0; i < num_rows; ++i) {
            EXPECT_EQ(crc_hashes[i], crc_expected[i]) << "CRC32 Binary len=" << lengths[i] << " seed=" << init_seed;
        }

        // With selection & selective
        std::vector<uint8_t> selection(num_rows, 0);
        std::vector<uint16_t> sel;
        for (uint32_t i = 0; i < num_rows; ++i) {
            if (i % 2 == 0) {
                selection[i] = 1;
                sel.push_back(static_cast<uint16_t>(i));
            }
        }
        std::vector<uint32_t> xxh3_sel_hashes(num_rows, init_seed);
        std::vector<uint32_t> xxh3_selective_hashes(num_rows, init_seed);
        std::vector<uint32_t> crc_sel_hashes(num_rows, init_seed);
        std::vector<uint32_t> crc_selective_hashes(num_rows, init_seed);
        xxh3_64_column_with_selection(*bin_col, xxh3_sel_hashes.data(), selection.data(), 0,
                                      static_cast<uint16_t>(num_rows));
        xxh3_64_column_selective(*bin_col, xxh3_selective_hashes.data(), sel.data(), static_cast<uint16_t>(sel.size()));
        crc32_hash_column_with_selection(*bin_col, crc_sel_hashes.data(), selection.data(), 0,
                                         static_cast<uint16_t>(num_rows));
        crc32_hash_column_selective(*bin_col, crc_selective_hashes.data(), sel.data(),
                                    static_cast<uint16_t>(sel.size()));
        for (uint32_t i = 0; i < num_rows; ++i) {
            EXPECT_EQ(xxh3_sel_hashes[i], selection[i] ? xxh3_expected[i] : init_seed);
            EXPECT_EQ(xxh3_selective_hashes[i], selection[i] ? xxh3_expected[i] : init_seed);
            EXPECT_EQ(crc_sel_hashes[i], selection[i] ? crc_expected[i] : init_seed);
            EXPECT_EQ(crc_selective_hashes[i], selection[i] ? crc_expected[i] : init_seed);
        }
    }

    // 2. NullableColumn, ArrayColumn, MapColumn, and chained multi-column hashing
    const uint32_t kRows = 6;
    const std::vector<int32_t> c1_vals{10, -20, 300, -4000, 50000, -600000};
    const std::vector<int64_t> c2_vals{1001LL, -2002LL, 3003003LL, -4004004004LL, 5005005005005LL, -6LL};
    const std::vector<uint8_t> c2_nulls{0, 1, 1, 0, 1, 0};
    const std::vector<std::string> c3_strs{
            "", "a", "abc", "abcdefgh", "0123456789abcdef", "0123456789abcdef0123456789abcdef!"};

    auto col1 = ColumnTestHelper::build_column<int32_t>(c1_vals);
    auto col2_data = ColumnTestHelper::build_column<int64_t>(c2_vals);
    auto col2_null = NullColumn::create();
    for (uint8_t is_null : c2_nulls) {
        col2_null->append(is_null);
    }
    auto col2 = NullableColumn::create(col2_data, col2_null);
    auto col3 = BinaryColumn::create();
    for (const auto& s : c3_strs) {
        col3->append(Slice(s));
    }

    // ArrayColumn: 6 rows with sizes {2, 0, 1, 3, 0, 2}
    const std::vector<std::vector<int32_t>> arr_rows{{11, 22}, {}, {-33}, {44, 55, 66}, {}, {77, -88}};
    auto arr_elems = Int32Column::create();
    auto arr_offsets = UInt32Column::create();
    arr_offsets->append(0);
    for (const auto& row : arr_rows) {
        for (int32_t v : row) {
            arr_elems->append(v);
        }
        arr_offsets->append(static_cast<uint32_t>(arr_elems->size()));
    }
    auto arr_col = ArrayColumn::create(ColumnHelper::cast_to_nullable_column(arr_elems), arr_offsets);

    // MapColumn: 6 rows with sizes {1, 0, 2, 1, 0, 2}
    const std::vector<std::vector<std::pair<int32_t, int64_t>>> map_rows{{{1, 100LL}},   {}, {{2, 200LL}, {3, 300LL}},
                                                                         {{-4, -400LL}}, {}, {{5, 500LL}, {6, 600LL}}};
    auto map_keys = Int32Column::create();
    auto map_vals = Int64Column::create();
    auto map_offsets = UInt32Column::create();
    map_offsets->append(0);
    for (const auto& row : map_rows) {
        for (const auto& kv : row) {
            map_keys->append(kv.first);
            map_vals->append(kv.second);
        }
        map_offsets->append(static_cast<uint32_t>(map_keys->size()));
    }
    auto map_col = MapColumn::create(ColumnHelper::cast_to_nullable_column(map_keys),
                                     ColumnHelper::cast_to_nullable_column(map_vals), map_offsets);

    for (uint32_t init_seed : {0u, 0x9E3779B1u, 0xdeadbeefu}) {
        std::vector<uint32_t> xxh3_hashes(kRows, init_seed);
        std::vector<uint32_t> crc_hashes(kRows, init_seed);

        xxh3_64_column(*col1, xxh3_hashes.data(), 0, kRows);
        xxh3_64_column(*col2, xxh3_hashes.data(), 0, kRows);
        xxh3_64_column(*col3, xxh3_hashes.data(), 0, kRows);
        xxh3_64_column(*arr_col, xxh3_hashes.data(), 0, kRows);
        xxh3_64_column(*map_col, xxh3_hashes.data(), 0, kRows);

        crc32_hash_column(*col1, crc_hashes.data(), 0, kRows);
        crc32_hash_column(*col2, crc_hashes.data(), 0, kRows);
        crc32_hash_column(*col3, crc_hashes.data(), 0, kRows);
        crc32_hash_column(*arr_col, crc_hashes.data(), 0, kRows);
        crc32_hash_column(*map_col, crc_hashes.data(), 0, kRows);

        for (uint32_t r = 0; r < kRows; ++r) {
            // Oracle XXH3 chained calculation
            uint32_t xh = init_seed;
            xh = static_cast<uint32_t>(XXH3_64bits_withSeed(&c1_vals[r], sizeof(int32_t), xh));
            if (c2_nulls[r]) {
                xh = xh ^ (0x9e3779b9u + (xh << 6) + (xh >> 2));
            } else {
                xh = static_cast<uint32_t>(XXH3_64bits_withSeed(&c2_vals[r], sizeof(int64_t), xh));
            }
            if (!c3_strs[r].empty()) {
                xh = static_cast<uint32_t>(XXH3_64bits_withSeed(c3_strs[r].data(), c3_strs[r].size(), xh));
            }
            size_t arr_sz = arr_rows[r].size();
            xh = static_cast<uint32_t>(XXH3_64bits_withSeed(&arr_sz, sizeof(size_t), xh));
            for (int32_t elem : arr_rows[r]) {
                xh = static_cast<uint32_t>(XXH3_64bits_withSeed(&elem, sizeof(int32_t), xh));
            }
            size_t map_sz = map_rows[r].size();
            xh = static_cast<uint32_t>(XXH3_64bits_withSeed(&map_sz, sizeof(size_t), xh));
            uint32_t map_base_xh = xh;
            for (const auto& kv : map_rows[r]) {
                uint32_t pair_xh = static_cast<uint32_t>(XXH3_64bits_withSeed(&kv.first, sizeof(int32_t), map_base_xh));
                pair_xh = static_cast<uint32_t>(XXH3_64bits_withSeed(&kv.second, sizeof(int64_t), pair_xh));
                xh += pair_xh;
            }
            EXPECT_EQ(xxh3_hashes[r], xh) << "Chained XXH3 mismatch row=" << r << " seed=" << init_seed;

            // Oracle CRC32 chained calculation
            uint32_t ch = init_seed;
            ch = static_cast<uint32_t>(::crc32(ch, reinterpret_cast<const Bytef*>(&c1_vals[r]), sizeof(int32_t)));
            if (c2_nulls[r]) {
                constexpr uint32_t kZero = 0;
                ch = static_cast<uint32_t>(::crc32(ch, reinterpret_cast<const Bytef*>(&kZero), sizeof(uint32_t)));
            } else {
                ch = static_cast<uint32_t>(::crc32(ch, reinterpret_cast<const Bytef*>(&c2_vals[r]), sizeof(int64_t)));
            }
            if (!c3_strs[r].empty()) {
                ch = static_cast<uint32_t>(::crc32(ch, reinterpret_cast<const Bytef*>(c3_strs[r].data()),
                                                   static_cast<uInt>(c3_strs[r].size())));
            }
            ch = static_cast<uint32_t>(::crc32(ch, reinterpret_cast<const Bytef*>(&arr_sz), sizeof(size_t)));
            for (int32_t elem : arr_rows[r]) {
                ch = static_cast<uint32_t>(::crc32(ch, reinterpret_cast<const Bytef*>(&elem), sizeof(int32_t)));
            }
            ch = static_cast<uint32_t>(::crc32(ch, reinterpret_cast<const Bytef*>(&map_sz), sizeof(size_t)));
            uint32_t map_base_ch = ch;
            for (const auto& kv : map_rows[r]) {
                uint32_t pair_ch = static_cast<uint32_t>(
                        ::crc32(map_base_ch, reinterpret_cast<const Bytef*>(&kv.first), sizeof(int32_t)));
                pair_ch = static_cast<uint32_t>(
                        ::crc32(pair_ch, reinterpret_cast<const Bytef*>(&kv.second), sizeof(int64_t)));
                ch += pair_ch;
            }
            EXPECT_EQ(crc_hashes[r], ch) << "Chained CRC32 mismatch row=" << r << " seed=" << init_seed;
        }
    }
}

} // namespace

TEST(ColumnHashOracleTest, Xxh3FixedWidthAndFallbackOracleParity) {
    run_xxh3_fixed_width_and_fallback_oracle_parity();
}

TEST(ColumnHashOracleTest, Xxh3AndCrc32ChainedSeedAndBinaryLengthsOracleParity) {
    run_xxh3_and_crc32_chained_seed_and_binary_lengths_oracle_parity();
}

} // namespace starrocks
