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

#include "column/copied_datum.h"

#include <gtest/gtest.h>

namespace starrocks {

// Test default constructor creates null datum
TEST(CopiedDatumTest, test_default_construct) {
    CopiedDatum copied;
    ASSERT_TRUE(copied.get().is_null());
}

// Test constructing from null Datum (this is the main fix scenario)
TEST(CopiedDatumTest, test_construct_from_null_datum) {
    Datum null_datum;
    ASSERT_TRUE(null_datum.is_null());

    CopiedDatum copied(null_datum);
    ASSERT_TRUE(copied.get().is_null());
}

// Test constructing from integer types
TEST(CopiedDatumTest, test_construct_from_int) {
    {
        Datum datum;
        datum.set_int8(42);
        CopiedDatum copied(datum);
        ASSERT_FALSE(copied.get().is_null());
        ASSERT_EQ(42, copied.get().get_int8());
    }
    {
        Datum datum;
        datum.set_int32(12345);
        CopiedDatum copied(datum);
        ASSERT_FALSE(copied.get().is_null());
        ASSERT_EQ(12345, copied.get().get_int32());
    }
    {
        Datum datum;
        datum.set_int64(1234567890123LL);
        CopiedDatum copied(datum);
        ASSERT_FALSE(copied.get().is_null());
        ASSERT_EQ(1234567890123LL, copied.get().get_int64());
    }
}

// Test constructing from float/double types
TEST(CopiedDatumTest, test_construct_from_float_double) {
    {
        Datum datum;
        datum.set_float(3.14f);
        CopiedDatum copied(datum);
        ASSERT_FALSE(copied.get().is_null());
        ASSERT_FLOAT_EQ(3.14f, copied.get().get_float());
    }
    {
        Datum datum;
        datum.set_double(2.71828);
        CopiedDatum copied(datum);
        ASSERT_FALSE(copied.get().is_null());
        ASSERT_DOUBLE_EQ(2.71828, copied.get().get_double());
    }
}

// Test constructing from Slice (string data)
TEST(CopiedDatumTest, test_construct_from_slice) {
    std::string str = "hello world";
    Slice slice(str);
    Datum datum;
    datum.set_slice(slice);

    CopiedDatum copied(datum);
    ASSERT_FALSE(copied.get().is_null());

    // The copied datum should have its own copy of the string
    const Slice& copied_slice = copied.get().get_slice();
    ASSERT_EQ(str.size(), copied_slice.size);
    ASSERT_EQ(0, memcmp(str.data(), copied_slice.data, str.size()));

    // Verify it's a different memory location (deep copy)
    ASSERT_NE(slice.data, copied_slice.data);
}

// Test constructing from empty Slice
TEST(CopiedDatumTest, test_construct_from_empty_slice) {
    Slice empty_slice;
    Datum datum;
    datum.set_slice(empty_slice);

    CopiedDatum copied(datum);
    ASSERT_FALSE(copied.get().is_null());
    ASSERT_TRUE(copied.get().get_slice().empty());
}

// Test copy constructor
TEST(CopiedDatumTest, test_copy_construct) {
    std::string str = "test string";
    Slice slice(str);
    Datum datum;
    datum.set_slice(slice);

    CopiedDatum copied1(datum);
    CopiedDatum copied2(copied1);

    ASSERT_FALSE(copied2.get().is_null());
    const Slice& slice1 = copied1.get().get_slice();
    const Slice& slice2 = copied2.get().get_slice();

    ASSERT_EQ(slice1.size, slice2.size);
    ASSERT_EQ(0, memcmp(slice1.data, slice2.data, slice1.size));
    // Should be different memory locations
    ASSERT_NE(slice1.data, slice2.data);
}

// Test copy constructor with null datum
TEST(CopiedDatumTest, test_copy_construct_null) {
    CopiedDatum copied1;
    CopiedDatum copied2(copied1);
    ASSERT_TRUE(copied2.get().is_null());
}

// Test move constructor
TEST(CopiedDatumTest, test_move_construct) {
    std::string str = "move test";
    Slice slice(str);
    Datum datum;
    datum.set_slice(slice);

    CopiedDatum copied1(datum);
    const char* original_ptr = copied1.get().get_slice().data;

    CopiedDatum copied2(std::move(copied1));

    // After move, copied1 should be null
    ASSERT_TRUE(copied1.get().is_null());

    // copied2 should have the data
    ASSERT_FALSE(copied2.get().is_null());
    ASSERT_EQ(original_ptr, copied2.get().get_slice().data);
}

// Test move constructor with null datum
TEST(CopiedDatumTest, test_move_construct_null) {
    CopiedDatum copied1;
    CopiedDatum copied2(std::move(copied1));
    ASSERT_TRUE(copied1.get().is_null());
    ASSERT_TRUE(copied2.get().is_null());
}

// Test copy assignment operator
TEST(CopiedDatumTest, test_copy_assign) {
    std::string str = "assign test";
    Slice slice(str);
    Datum datum;
    datum.set_slice(slice);

    CopiedDatum copied1(datum);
    CopiedDatum copied2;

    copied2 = copied1;

    ASSERT_FALSE(copied2.get().is_null());
    const Slice& slice1 = copied1.get().get_slice();
    const Slice& slice2 = copied2.get().get_slice();

    ASSERT_EQ(slice1.size, slice2.size);
    ASSERT_NE(slice1.data, slice2.data);
}

// Test copy assignment with null datum
TEST(CopiedDatumTest, test_copy_assign_null) {
    Datum datum;
    datum.set_int32(100);
    CopiedDatum copied1(datum);
    CopiedDatum copied2;

    // Assign null to non-null
    copied1 = copied2;
    ASSERT_TRUE(copied1.get().is_null());
}

// Test self-assignment
TEST(CopiedDatumTest, test_self_assign) {
    std::string str = "self assign";
    Slice slice(str);
    Datum datum;
    datum.set_slice(slice);

    CopiedDatum copied(datum);
    const char* original_ptr = copied.get().get_slice().data;

    copied = copied;

    ASSERT_FALSE(copied.get().is_null());
    ASSERT_EQ(original_ptr, copied.get().get_slice().data);
}

// Test move assignment operator
TEST(CopiedDatumTest, test_move_assign) {
    std::string str = "move assign";
    Slice slice(str);
    Datum datum;
    datum.set_slice(slice);

    CopiedDatum copied1(datum);
    const char* original_ptr = copied1.get().get_slice().data;
    CopiedDatum copied2;

    copied2 = std::move(copied1);

    ASSERT_TRUE(copied1.get().is_null());
    ASSERT_FALSE(copied2.get().is_null());
    ASSERT_EQ(original_ptr, copied2.get().get_slice().data);
}

// Test move assignment with null datum
TEST(CopiedDatumTest, test_move_assign_null) {
    Datum datum;
    datum.set_int32(100);
    CopiedDatum copied1(datum);
    CopiedDatum copied2;

    // Move null to non-null
    copied1 = std::move(copied2);
    ASSERT_TRUE(copied1.get().is_null());
    ASSERT_TRUE(copied2.get().is_null());
}

// Test set() method
TEST(CopiedDatumTest, test_set_method) {
    CopiedDatum copied;
    ASSERT_TRUE(copied.get().is_null());

    // Set to integer
    Datum int_datum;
    int_datum.set_int32(42);
    copied.set(int_datum);
    ASSERT_FALSE(copied.get().is_null());
    ASSERT_EQ(42, copied.get().get_int32());

    // Set to string
    std::string str = "set method test";
    Slice slice(str);
    Datum str_datum;
    str_datum.set_slice(slice);
    copied.set(str_datum);
    ASSERT_FALSE(copied.get().is_null());
    ASSERT_EQ(str.size(), copied.get().get_slice().size);

    // Set to null
    Datum null_datum;
    copied.set(null_datum);
    ASSERT_TRUE(copied.get().is_null());
}

// Test set() replaces existing slice correctly (no memory leak)
TEST(CopiedDatumTest, test_set_replaces_slice) {
    std::string str1 = "first string";
    Slice slice1(str1);
    Datum datum1;
    datum1.set_slice(slice1);

    CopiedDatum copied(datum1);

    std::string str2 = "second string";
    Slice slice2(str2);
    Datum datum2;
    datum2.set_slice(slice2);

    copied.set(datum2);

    ASSERT_FALSE(copied.get().is_null());
    const Slice& result = copied.get().get_slice();
    ASSERT_EQ(str2.size(), result.size);
    ASSERT_EQ(0, memcmp(str2.data(), result.data, str2.size()));
}

// Test with int128_t type
TEST(CopiedDatumTest, test_construct_from_int128) {
    int128_t big_value = static_cast<int128_t>(1) << 100;
    Datum datum;
    datum.set_int128(big_value);

    CopiedDatum copied(datum);
    ASSERT_FALSE(copied.get().is_null());
    ASSERT_EQ(big_value, copied.get().get_int128());
}

// Test with DecimalV2Value type
TEST(CopiedDatumTest, test_construct_from_decimal) {
    DecimalV2Value decimal_value(12345, 67);
    Datum datum;
    datum.set_decimal(decimal_value);

    CopiedDatum copied(datum);
    ASSERT_FALSE(copied.get().is_null());
    ASSERT_EQ(decimal_value, copied.get().get_decimal());
}

} // namespace starrocks
