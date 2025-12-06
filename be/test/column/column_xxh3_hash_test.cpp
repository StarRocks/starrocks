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

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "util/hash_util.hpp"

namespace starrocks {

class ColumnXxh3HashTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

// Test xxh3_hash for FixedLengthColumn<int32_t>
TEST_F(ColumnXxh3HashTest, test_fixed_length_column_int32) {
    auto column = FixedLengthColumn<int32_t>::create();
    column->append(1);
    column->append(2);
    column->append(3);

    uint32_t hash_values[3] = {0, 0, 0};
    column->xxh3_hash(hash_values, 0, 3);

    // Verify hash values are computed (non-zero for non-zero seed)
    uint32_t expected[3];
    int32_t data[3] = {1, 2, 3};
    for (int i = 0; i < 3; ++i) {
        expected[i] = static_cast<uint32_t>(HashUtil::xx_hash3_64(&data[i], sizeof(int32_t), 0));
    }

    ASSERT_EQ(expected[0], hash_values[0]);
    ASSERT_EQ(expected[1], hash_values[1]);
    ASSERT_EQ(expected[2], hash_values[2]);
}

// Test xxh3_hash for FixedLengthColumn<int64_t>
TEST_F(ColumnXxh3HashTest, test_fixed_length_column_int64) {
    auto column = FixedLengthColumn<int64_t>::create();
    column->append(100L);
    column->append(200L);

    uint32_t hash_values[2] = {0, 0};
    column->xxh3_hash(hash_values, 0, 2);

    int64_t data[2] = {100L, 200L};
    uint32_t expected[2];
    for (int i = 0; i < 2; ++i) {
        expected[i] = static_cast<uint32_t>(HashUtil::xx_hash3_64(&data[i], sizeof(int64_t), 0));
    }

    ASSERT_EQ(expected[0], hash_values[0]);
    ASSERT_EQ(expected[1], hash_values[1]);
}

// Test xxh3_hash for BinaryColumn
TEST_F(ColumnXxh3HashTest, test_binary_column) {
    auto column = BinaryColumn::create();
    column->append("hello");
    column->append("world");
    column->append("starrocks");

    uint32_t hash_values[3] = {0, 0, 0};
    column->xxh3_hash(hash_values, 0, 3);

    // Verify hash values are computed
    const char* strs[] = {"hello", "world", "starrocks"};
    uint32_t expected[3];
    for (int i = 0; i < 3; ++i) {
        expected[i] = static_cast<uint32_t>(HashUtil::xx_hash3_64(strs[i], strlen(strs[i]), 0));
    }

    ASSERT_EQ(expected[0], hash_values[0]);
    ASSERT_EQ(expected[1], hash_values[1]);
    ASSERT_EQ(expected[2], hash_values[2]);
}

// Test xxh3_hash for NullableColumn
TEST_F(ColumnXxh3HashTest, test_nullable_column) {
    auto data_column = FixedLengthColumn<int32_t>::create();
    auto null_column = NullColumn::create();

    data_column->append(10);
    null_column->append(0); // not null

    data_column->append(20);
    null_column->append(1); // null

    data_column->append(30);
    null_column->append(0); // not null

    auto column = NullableColumn::create(data_column, null_column);

    uint32_t hash_values[3] = {0, 0, 0};
    column->xxh3_hash(hash_values, 0, 3);

    // Non-null values should have data-based hash
    int32_t val1 = 10;
    int32_t val3 = 30;
    uint32_t expected1 = static_cast<uint32_t>(HashUtil::xx_hash3_64(&val1, sizeof(int32_t), 0));
    uint32_t expected3 = static_cast<uint32_t>(HashUtil::xx_hash3_64(&val3, sizeof(int32_t), 0));

    ASSERT_EQ(expected1, hash_values[0]);
    ASSERT_EQ(expected3, hash_values[2]);

    // Null value should have special hash (using 0x9e3779b9)
    uint32_t null_value = 0x9e3779b9;
    uint32_t expected_null = 0 ^ (null_value + (0 << 6) + (0 >> 2));
    ASSERT_EQ(expected_null, hash_values[1]);
}

// Test xxh3_hash for ConstColumn
TEST_F(ColumnXxh3HashTest, test_const_column) {
    auto data_column = FixedLengthColumn<int32_t>::create();
    data_column->append(42);

    auto column = ConstColumn::create(data_column, 3);

    uint32_t hash_values[3] = {0, 0, 0};
    column->xxh3_hash(hash_values, 0, 3);

    int32_t val = 42;
    uint32_t expected = static_cast<uint32_t>(HashUtil::xx_hash3_64(&val, sizeof(int32_t), 0));

    // All values should have the same hash since it's a const column
    ASSERT_EQ(expected, hash_values[0]);
    ASSERT_EQ(expected, hash_values[1]);
    ASSERT_EQ(expected, hash_values[2]);
}

// Test xxh3_hash for ArrayColumn
TEST_F(ColumnXxh3HashTest, test_array_column) {
    auto offsets = UInt32Column::create();
    auto elements = Int32Column::create();

    // First array: [1, 2, 3]
    offsets->append(0);
    elements->append(1);
    elements->append(2);
    elements->append(3);
    offsets->append(3);

    // Second array: [4, 5]
    elements->append(4);
    elements->append(5);
    offsets->append(5);

    auto column = ArrayColumn::create(NullableColumn::wrap_if_necessary(elements), offsets);

    uint32_t hash_values[2] = {0, 0};
    column->xxh3_hash(hash_values, 0, 2);

    // Verify hash values are non-zero (actual values depend on implementation)
    ASSERT_NE(0u, hash_values[0]);
    ASSERT_NE(0u, hash_values[1]);
    // Different arrays should have different hashes
    ASSERT_NE(hash_values[0], hash_values[1]);
}

// Test xxh3_hash for MapColumn
TEST_F(ColumnXxh3HashTest, test_map_column) {
    auto offsets = UInt32Column::create();
    auto keys = Int32Column::create();
    auto values = Int32Column::create();

    // First map: {1: 10, 2: 20}
    offsets->append(0);
    keys->append(1);
    values->append(10);
    keys->append(2);
    values->append(20);
    offsets->append(2);

    // Second map: {3: 30}
    keys->append(3);
    values->append(30);
    offsets->append(3);

    auto column = MapColumn::create(NullableColumn::wrap_if_necessary(keys), NullableColumn::wrap_if_necessary(values),
                                    offsets);

    uint32_t hash_values[2] = {0, 0};
    column->xxh3_hash(hash_values, 0, 2);

    // Verify hash values are non-zero
    ASSERT_NE(0u, hash_values[0]);
    ASSERT_NE(0u, hash_values[1]);
    // Different maps should have different hashes
    ASSERT_NE(hash_values[0], hash_values[1]);
}

// Test xxh3_hash for StructColumn
TEST_F(ColumnXxh3HashTest, test_struct_column) {
    auto field1 = Int32Column::create();
    auto field2 = BinaryColumn::create();

    field1->append(1);
    field1->append(2);
    field2->append("a");
    field2->append("b");

    Columns fields;
    fields.push_back(NullableColumn::wrap_if_necessary(field1));
    fields.push_back(NullableColumn::wrap_if_necessary(field2));

    auto column = StructColumn::create(fields, std::vector<std::string>{"f1", "f2"});

    uint32_t hash_values[2] = {0, 0};
    column->xxh3_hash(hash_values, 0, 2);

    // Verify hash values are non-zero
    ASSERT_NE(0u, hash_values[0]);
    ASSERT_NE(0u, hash_values[1]);
    // Different structs should have different hashes
    ASSERT_NE(hash_values[0], hash_values[1]);
}

// Test that xxh3_hash produces different results than fnv_hash
TEST_F(ColumnXxh3HashTest, test_xxh3_differs_from_fnv) {
    auto column = FixedLengthColumn<int32_t>::create();
    column->append(12345);

    uint32_t xxh3_hash[1] = {0};
    uint32_t fnv_hash[1] = {0};

    column->xxh3_hash(xxh3_hash, 0, 1);
    column->fnv_hash(fnv_hash, 0, 1);

    // XXH3 and FNV should produce different hash values
    ASSERT_NE(xxh3_hash[0], fnv_hash[0]);
}

// Test xxh3_hash with seed propagation
TEST_F(ColumnXxh3HashTest, test_xxh3_seed_propagation) {
    auto column = FixedLengthColumn<int32_t>::create();
    column->append(100);

    uint32_t hash_with_seed_0[1] = {0};
    uint32_t hash_with_seed_1[1] = {12345};

    column->xxh3_hash(hash_with_seed_0, 0, 1);
    column->xxh3_hash(hash_with_seed_1, 0, 1);

    // Different seeds should produce different hash values
    ASSERT_NE(hash_with_seed_0[0], hash_with_seed_1[0]);
}

} // namespace starrocks
