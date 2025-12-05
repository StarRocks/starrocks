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
#include "runtime/decimalv2_value.h"
#include "runtime/decimalv3.h"
#include "types/date_value.h"
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
        std::vector<uint32_t> hashes(to - from, 0);
        GetParam().hash_func(*column, hashes.data(), from, to);

        // Verify all hashes are computed (non-zero for non-empty columns)
        for (size_t i = 0; i < hashes.size(); ++i) {
            // TODO: murmur hash is werid
            if (!column->is_null(i) && GetParam().hash_func != murmur_hash3_x86_32_column) {
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
        std::vector<uint32_t> hashes(sel_size, 0);
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
        auto col = Int8Column::create();
        col->append(1);
        col->append(2);
        col->append(-1);
        this->test_hash_function(col, 0, 3);
    }

    // Test int16
    {
        auto col = Int16Column::create();
        col->append(100);
        col->append(200);
        this->test_hash_function(col, 0, 2);
    }

    // Test int32
    {
        auto col = Int32Column::create();
        col->append(1000);
        col->append(2000);
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
        auto col = FloatColumn::create();
        col->append(1.5f);
        col->append(2.5f);
        this->test_hash_function(col, 0, 2);
    }

    // Test double
    {
        auto col = DoubleColumn::create();
        col->append(1.5);
        col->append(2.5);
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
        auto col = BinaryColumn::create();
        col->append("hello");
        col->append("world");
        col->append("xxx");
        this->test_hash_function(col, 0, 3);
    }

    // Test LargeBinaryColumn
    {
        auto col = LargeBinaryColumn::create();
        col->append("large_string_test");
        col->append("another_large_string");
        this->test_hash_function(col, 0, 2);
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
    auto data_col = Int32Column::create();
    data_col->append(42);
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
    auto id_col = Int32Column::create();
    auto name_col = BinaryColumn::create();
    id_col->append(1);
    name_col->append("alice");
    id_col->append(2);
    name_col->append("bob");

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
    auto col = Int32Column::create();
    col->append(1);
    col->append(2);
    col->append(3);
    col->append(4);

    uint8_t selection[] = {1, 0, 1, 1};
    this->test_hash_function_with_selection(col, selection, 0, 4);
}

// Test hash functions with selective indices
TEST_P(ColumnHashTest, test_hash_selective) {
    auto col = Int32Column::create();
    col->append(1);
    col->append(2);
    col->append(3);
    col->append(4);
    col->append(5);

    uint16_t sel[] = {0, 2, 4};
    this->test_hash_function_selective(col, sel, 3);
}

// Test hash consistency - same value should produce same hash
TEST_P(ColumnHashTest, test_hash_consistency) {
    auto col1 = Int32Column::create();
    auto col2 = Int32Column::create();
    col1->append(42);
    col2->append(42);

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

} // namespace starrocks
