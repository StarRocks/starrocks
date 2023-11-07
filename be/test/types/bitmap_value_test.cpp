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

#include "types/bitmap_value.h"

#include <gtest/gtest.h>

<<<<<<< HEAD
#include "util/phmap/phmap.h"

namespace starrocks {

class BitmapTest : public testing::Test {};

TEST_F(BitmapTest, Constructor) {
    BitmapValue bitmap;
=======
#include <cstdint>
#include <string>

#include "column/vectorized_fwd.h"
#include "types/bitmap_value_detail.h"
#include "util/coding.h"

namespace starrocks {

using BitmapDataType = BitmapValue::BitmapDataType;

class BitmapValueTest : public ::testing::Test {
public:
    void SetUp() override;
    BitmapValue gen_bitmap(uint64_t start, uint64_t end);
    void check_bitmap(BitmapDataType type, const BitmapValue& bitmap, uint64_t start, uint64_t end);
    void check_bitmap(BitmapDataType type, const BitmapValue& bitmap, uint64_t start_1, uint64_t end_1,
                      uint64_t start_2, uint64_t end_2);

protected:
    BitmapValue _large_bitmap;
    BitmapValue _medium_bitmap;
    BitmapValue _single_bitmap;
    BitmapValue _empty_bitmap;
};

void BitmapValueTest::SetUp() {
>>>>>>> 44ae317858 ([Enhancement] BitmapValue support copy on write (#34047))
    for (size_t i = 0; i < 64; i++) {
        _large_bitmap.add(i);
    }
    for (size_t i = 0; i < 14; i++) {
        _medium_bitmap.add(i);
    }
    _single_bitmap.add(0);
}

BitmapValue BitmapValueTest::gen_bitmap(uint64_t start, uint64_t end) {
    BitmapValue bitmap;
    for (auto i = start; i < end; i++) {
        bitmap.add(i);
    }
    return bitmap;
}

void BitmapValueTest::check_bitmap(BitmapDataType type, const BitmapValue& bitmap, uint64_t start, uint64_t end) {
    ASSERT_EQ(bitmap.type(), type);
    ASSERT_EQ(bitmap.cardinality(), end - start);
    for (auto i = start; i < end; i++) {
        ASSERT_TRUE(bitmap.contains(i));
    }
}

void BitmapValueTest::check_bitmap(BitmapDataType type, const BitmapValue& bitmap, uint64_t start_1, uint64_t end_1,
                                   uint64_t start_2, uint64_t end_2) {
    ASSERT_EQ(bitmap.type(), type);
    ASSERT_EQ(bitmap.cardinality(), end_1 - start_1 + end_2 - start_2);
    for (auto i = start_1; i < end_1; i++) {
        ASSERT_TRUE(bitmap.contains(i));
    }
    for (auto i = start_2; i < end_2; i++) {
        ASSERT_TRUE(bitmap.contains(i));
    }
}

TEST_F(BitmapValueTest, copy_construct) {
    BitmapValue bitmap_1(_large_bitmap);
    bitmap_1.add(64);
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 65);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_2(_medium_bitmap);
    bitmap_2.add(14);
    check_bitmap(BitmapDataType::SET, bitmap_2, 0, 15);
}

TEST_F(BitmapValueTest, assign_operator) {
    BitmapValue bitmap_1 = _large_bitmap;
    bitmap_1.add(64);
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 65);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_2 = _medium_bitmap;
    bitmap_2.add(14);
    check_bitmap(BitmapDataType::SET, bitmap_2, 0, 15);

    BitmapValue* bitmap_3 = &_large_bitmap;
    *bitmap_3 = _large_bitmap;
    bitmap_3->add(64);
    check_bitmap(BitmapDataType::BITMAP, *bitmap_3, 0, 65);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 65);
}

TEST_F(BitmapValueTest, move_construct) {
    BitmapValue bitmap_1(std::move(_large_bitmap));
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 64);
    check_bitmap(BitmapDataType::EMPTY, _large_bitmap, 0, 0);

    BitmapValue bitmap_2(std::move(_medium_bitmap));
    check_bitmap(BitmapDataType::SET, bitmap_2, 0, 14);
    check_bitmap(BitmapDataType::EMPTY, _medium_bitmap, 0, 0);
}

TEST_F(BitmapValueTest, move_assign_operator) {
    BitmapValue* bitmap_1 = &_large_bitmap;
    *bitmap_1 = std::move(_large_bitmap);
    bitmap_1->add(64);
    check_bitmap(BitmapDataType::BITMAP, *bitmap_1, 0, 65);

    BitmapValue bitmap_2 = std::move(_large_bitmap);
    bitmap_2.add(64);
    check_bitmap(BitmapDataType::BITMAP, bitmap_2, 0, 65);
    check_bitmap(BitmapDataType::EMPTY, _large_bitmap, 0, 0);

    BitmapValue bitmap_3 = std::move(_medium_bitmap);
    bitmap_3.add(14);
    check_bitmap(BitmapDataType::SET, bitmap_3, 0, 15);
    check_bitmap(BitmapDataType::EMPTY, _medium_bitmap, 0, 0);
}

TEST_F(BitmapValueTest, single_construct) {
    BitmapValue bitmap(1);
    check_bitmap(BitmapDataType::SINGLE, bitmap, 1, 2);
}

TEST_F(BitmapValueTest, construct_from_deserialize) {
    size_t size = _large_bitmap.serialize_size();
    uint8_t buf[size];
    _large_bitmap.serialize(buf);

    BitmapValue bitmap_1((char*)buf);
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 64);

    Slice slice{(char*)&buf, size};
    BitmapValue bitmap_2(slice);
    check_bitmap(BitmapDataType::BITMAP, bitmap_2, 0, 64);
}

TEST_F(BitmapValueTest, construct_from_vector) {
    std::vector<uint64_t> v1{};
    std::vector<uint64_t> v2{0};
    std::vector<uint64_t> v3{1, 2, 3};
    std::vector<uint64_t> v4;
    for (size_t i = 0; i < 64; i++) {
        v4.emplace_back(i);
    }

    BitmapValue bitmap_1(v1);
    check_bitmap(BitmapDataType::EMPTY, bitmap_1, 0, 0);

    BitmapValue bitmap_2(v2);
    check_bitmap(BitmapDataType::SINGLE, bitmap_2, 0, 1);

    BitmapValue bitmap_3(v3);
    check_bitmap(BitmapDataType::BITMAP, bitmap_3, 1, 4);

    BitmapValue bitmap_4(v4);
    check_bitmap(BitmapDataType::BITMAP, bitmap_4, 0, 64);
}

TEST_F(BitmapValueTest, add) {
    BitmapValue bitmap_1;
    bitmap_1.add(0);
    check_bitmap(BitmapDataType::SINGLE, bitmap_1, 0, 1);

    bitmap_1.add(0);
    check_bitmap(BitmapDataType::SINGLE, bitmap_1, 0, 1);

    bitmap_1.add(1);
    check_bitmap(BitmapDataType::SET, bitmap_1, 0, 2);

    bitmap_1.add(2);
    check_bitmap(BitmapDataType::SET, bitmap_1, 0, 3);

    for (size_t i = 0; i < 33; i++) {
        bitmap_1.add(i);
    }
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 33);

    for (size_t i = 0; i < 64; i++) {
        bitmap_1.add(i);
    }
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 64);

    BitmapValue bitmap_2(bitmap_1);
    bitmap_2.add(64);
    check_bitmap(BitmapDataType::BITMAP, bitmap_2, 0, 65);
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 64);
}

TEST_F(BitmapValueTest, add_many) {
    BitmapValue bitmap_1(_single_bitmap);
    std::vector<uint32_t> v1{1, 2, 3};
    bitmap_1.add_many(3, v1.data());
    check_bitmap(BitmapDataType::SET, bitmap_1, 0, 4);

    BitmapValue bitmap_2(_large_bitmap);
    std::vector<uint32_t> v2{64, 65, 66};
    bitmap_2.add_many(3, v2.data());
    check_bitmap(BitmapDataType::BITMAP, bitmap_2, 0, 67);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);
}

TEST_F(BitmapValueTest, bitmap_union) {
    auto bitmap_1 = gen_bitmap(0, 64);
    bitmap_1 |= _empty_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 64);

    auto bitmap_2 = gen_bitmap(1, 2);
    bitmap_2 |= _single_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_2, 0, 2);

    BitmapValue bitmap_3;
    bitmap_3 |= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_3, 0, 14);

    BitmapValue bitmap_4(14);
    bitmap_4 |= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_4, 0, 15);

    auto bitmap_5 = gen_bitmap(0, 32);
    auto bitmap_6 = gen_bitmap(32, 33);
    bitmap_6 |= bitmap_5;
    check_bitmap(BitmapDataType::BITMAP, bitmap_6, 0, 33);

    auto bitmap_7 = gen_bitmap(14, 34);
    bitmap_7 |= _medium_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_7, 0, 34);

    BitmapValue bitmap_8(_large_bitmap);
    auto bitmap_9 = gen_bitmap(64, 84);
    bitmap_8 |= bitmap_9;
    check_bitmap(BitmapDataType::BITMAP, bitmap_8, 0, 84);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_10;
    bitmap_10 |= _large_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_10, 0, 64);

    BitmapValue bitmap_11(64);
    bitmap_11 |= _large_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_11, 0, 65);

    BitmapValue bitmap_12(_large_bitmap);
    auto bitmap_13 = gen_bitmap(64, 164);
    bitmap_12 |= bitmap_13;
    check_bitmap(BitmapDataType::BITMAP, bitmap_12, 0, 164);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_14(_medium_bitmap);
    auto bitmap_15 = gen_bitmap(14, 132);
    bitmap_14 |= bitmap_15;
    check_bitmap(BitmapDataType::BITMAP, bitmap_14, 0, 132);
}

TEST_F(BitmapValueTest, bitmap_intersect) {
    auto bitmap_1 = gen_bitmap(0, 100);
    bitmap_1 &= _empty_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_1, 0, 0);

    BitmapValue bitmap_2;
    bitmap_2 &= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_2, 0, 0);

    BitmapValue bitmap_3((uint64_t)0);
    bitmap_3 &= _single_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_3, 0, 1);

    BitmapValue bitmap_4(1);
    bitmap_4 &= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_4, 0, 0);

    auto bitmap_5 = gen_bitmap(0, 40);
    bitmap_5 &= _single_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_5, 0, 1);

    auto bitmap_6 = gen_bitmap(1, 40);
    bitmap_6 &= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_6, 0, 0);

    auto bitmap_7 = gen_bitmap(0, 10);
    bitmap_7 &= _single_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_7, 0, 1);

    auto bitmap_8 = gen_bitmap(1, 10);
    bitmap_8 &= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_8, 0, 0);

    auto bitmap_9 = gen_bitmap(0, 64);
    bitmap_9 &= _empty_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_9, 0, 0);

    auto bitmap_10 = gen_bitmap(0, 64);
    bitmap_10 &= _single_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_10, 0, 1);

    auto bitmap_11 = gen_bitmap(1, 65);
    bitmap_11 &= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_11, 0, 0);

    auto bitmap_13 = gen_bitmap(0, 100);
    BitmapValue bitmap_14(bitmap_13);
    auto bitmap_15 = gen_bitmap(80, 180);
    bitmap_14 &= bitmap_15;
    check_bitmap(BitmapDataType::BITMAP, bitmap_14, 80, 100);
    check_bitmap(BitmapDataType::BITMAP, bitmap_13, 0, 100);
    auto bitmap_16 = gen_bitmap(100, 200);
    bitmap_16 &= bitmap_13;
    check_bitmap(BitmapDataType::EMPTY, bitmap_16, 0, 0);
    auto bitmap_17 = gen_bitmap(99, 299);
    bitmap_17 &= bitmap_13;
    check_bitmap(BitmapDataType::SINGLE, bitmap_17, 99, 100);

    auto bitmap_18 = gen_bitmap(60, 80);
    bitmap_18 &= _large_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_18, 60, 64);

    BitmapValue bitmap_19;
    bitmap_19 &= _medium_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_19, 0, 0);

    BitmapValue bitmap_20((uint64_t)0);
    bitmap_20 &= _medium_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_20, 0, 1);

    BitmapValue bitmap_21(100);
    bitmap_21 &= _medium_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_21, 0, 0);

    auto bitmap_22 = gen_bitmap(10, 160);
    bitmap_22 &= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_22, 10, 14);

    auto bitmap_23 = gen_bitmap(10, 20);
    bitmap_23 &= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_23, 10, 14);
}

TEST_F(BitmapValueTest, test_remove) {
    BitmapValue bitmap_1;
    bitmap_1.remove(1);
    check_bitmap(BitmapDataType::EMPTY, bitmap_1, 0, 0);

    BitmapValue bitmap_2(1);
    bitmap_2.remove(1);
    check_bitmap(BitmapDataType::EMPTY, bitmap_2, 0, 0);

    BitmapValue bitmap_3(1);
    bitmap_3.remove(2);
    check_bitmap(BitmapDataType::SINGLE, bitmap_3, 1, 2);

    BitmapValue bitmap_4(_large_bitmap);
    bitmap_4.remove(0);
    check_bitmap(BitmapDataType::BITMAP, bitmap_4, 1, 64);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_5(_medium_bitmap);
    bitmap_5.remove(0);
    check_bitmap(BitmapDataType::SET, bitmap_5, 1, 14);
}

TEST_F(BitmapValueTest, bitmap_sub) {
    BitmapValue bitmap_1(_large_bitmap);
    bitmap_1 -= _empty_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 64);

    BitmapValue bitmap_2;
    bitmap_2 -= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_2, 0, 0);

    BitmapValue bitmap_3((uint64_t)0);
    bitmap_3 -= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_3, 0, 0);

    BitmapValue bitmap_4(1);
    bitmap_4 -= _single_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_4, 1, 2);

    BitmapValue bitmap_5(_large_bitmap);
    bitmap_5 -= _single_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_5, 1, 64);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_6(_medium_bitmap);
    bitmap_6 -= _single_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_6, 1, 14);

    BitmapValue bitmap_7;
    bitmap_7 -= _large_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_7, 0, 0);

    BitmapValue bitmap_8((uint64_t)0);
    bitmap_8 -= _large_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_8, 0, 0);

    BitmapValue bitmap_9(128);
    bitmap_9 -= _large_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_9, 128, 129);

    auto bitmap_10 = gen_bitmap(30, 200);
    BitmapValue bitmap_11(bitmap_10);
    bitmap_11 -= _large_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_11, 64, 200);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    auto bitmap_12 = gen_bitmap(50, 120);
    bitmap_12 -= _large_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_12, 64, 120);

    auto bitmap_13 = gen_bitmap(50, 70);
    bitmap_13 -= _large_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_13, 64, 70);

    BitmapValue bitmap_14;
    bitmap_14 -= _medium_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_14, 0, 0);

    BitmapValue bitmap_15((uint64_t)0);
    bitmap_15 -= _medium_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_15, 0, 0);

    BitmapValue bitmap_16(100);
    bitmap_16 -= _medium_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_16, 100, 101);

    BitmapValue bitmap_17(_large_bitmap);
    bitmap_17 -= _medium_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_17, 14, 64);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    auto bitmap_18 = gen_bitmap(1, 65);
    bitmap_18 -= _large_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_18, 64, 65);

    auto bitmap_19 = gen_bitmap(3, 16);
    bitmap_19 -= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_19, 14, 16);
}

TEST_F(BitmapValueTest, bitmap_xor) {
    BitmapValue bitmap_1(_large_bitmap);
    bitmap_1 ^= _empty_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_1, 0, 64);

    BitmapValue bitmap_2;
    bitmap_2 ^= _single_bitmap;
    check_bitmap(BitmapDataType::SINGLE, bitmap_2, 0, 1);

    BitmapValue bitmap_3(1);
    bitmap_3 ^= _single_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_3, 0, 2);

    BitmapValue bitmap_4((uint64_t)0);
    bitmap_4 ^= _single_bitmap;
    check_bitmap(BitmapDataType::EMPTY, bitmap_4, 0, 0);

    BitmapValue bitmap_5(_large_bitmap);
    bitmap_5 ^= _single_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_5, 1, 64);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_6(_large_bitmap);
    auto bitmap_7 = gen_bitmap(64, 65);
    bitmap_6 ^= bitmap_7;
    check_bitmap(BitmapDataType::BITMAP, bitmap_6, 0, 65);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    BitmapValue bitmap_8(_medium_bitmap);
    bitmap_8 ^= _single_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_8, 1, 14);

    BitmapValue bitmap_9(_medium_bitmap);
    auto bitmap_10 = gen_bitmap(14, 15);
    bitmap_9 ^= bitmap_10;
    check_bitmap(BitmapDataType::SET, bitmap_9, 0, 15);

    BitmapValue bitmap_11(_large_bitmap);
    bitmap_11 ^= _empty_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_11, 0, 64);

    BitmapValue bitmap_12((uint64_t)0);
    bitmap_12 ^= _large_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_12, 1, 64);

    BitmapValue bitmap_13(64);
    bitmap_13 ^= _large_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_13, 0, 65);

    // BITMAP ^= SET
    auto bitmap_14 = gen_bitmap(20, 80);
    BitmapValue bitmap_15(_large_bitmap);
    bitmap_15 ^= bitmap_14;
    check_bitmap(BitmapDataType::BITMAP, bitmap_15, 0, 20, 64, 80);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    auto bitmap_16 = gen_bitmap(60, 80);
    bitmap_16 ^= _large_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_16, 0, 60, 64, 80);

    // EMPTY ^= SET
    BitmapValue bitmap_17;
    bitmap_17 ^= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_17, 0, 14);

    BitmapValue bitmap_18((uint64_t)0);
    bitmap_18 ^= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_18, 1, 14);

    // SINGLE ^= SET
    BitmapValue bitmap_19(100);
    bitmap_19 ^= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_19, 0, 14, 100, 101);

    // BITMAP ^= SET -> BITMAP
    auto bitmap_20 = gen_bitmap(10, 80);
    bitmap_20 ^= _medium_bitmap;
    check_bitmap(BitmapDataType::BITMAP, bitmap_20, 0, 10, 14, 80);

    // SET ^= SET -> SET
    auto bitmap_21 = gen_bitmap(10, 20);
    bitmap_21 ^= _medium_bitmap;
    check_bitmap(BitmapDataType::SET, bitmap_21, 0, 10, 14, 20);
}

TEST_F(BitmapValueTest, bitmap_contains) {
    ASSERT_FALSE(_empty_bitmap.contains(5));
    ASSERT_TRUE(_single_bitmap.contains(0));
    ASSERT_FALSE(_single_bitmap.contains(1));
    ASSERT_TRUE(_large_bitmap.contains(5));
    ASSERT_FALSE(_large_bitmap.contains(100));
    ASSERT_TRUE(_medium_bitmap.contains(5));
    ASSERT_FALSE(_large_bitmap.contains(100));
}

TEST_F(BitmapValueTest, bitmap_cardinality) {
    ASSERT_EQ(_empty_bitmap.cardinality(), 0);
    ASSERT_EQ(_single_bitmap.cardinality(), 1);
    ASSERT_EQ(_medium_bitmap.cardinality(), 14);
    ASSERT_EQ(_large_bitmap.cardinality(), 64);
}

TEST_F(BitmapValueTest, bitmap_max) {
    ASSERT_FALSE(_empty_bitmap.max().has_value());
    ASSERT_EQ(_single_bitmap.max().value(), 0);
    ASSERT_EQ(_medium_bitmap.max().value(), 13);
    ASSERT_EQ(_large_bitmap.max().value(), 63);
}

TEST_F(BitmapValueTest, bitmap_min) {
    ASSERT_FALSE(_empty_bitmap.min().has_value());
    ASSERT_EQ(_single_bitmap.min().value(), 0);
    ASSERT_EQ(_medium_bitmap.min().value(), 0);
    ASSERT_EQ(_large_bitmap.min().value(), 0);
}

TEST_F(BitmapValueTest, bitmap_serialize_deserialize) {
    // empty bitmap
    size_t size = _empty_bitmap.getSizeInBytes();
    char buf_1[size];
    _empty_bitmap.write(buf_1);
    BitmapValue bitmap_1;
    bool ret = bitmap_1.deserialize(buf_1);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::EMPTY, bitmap_1, 0, 0);

    // single bitmap
    size = _single_bitmap.getSizeInBytes();
    char buf_2[size];
    _single_bitmap.write(buf_2);
    BitmapValue bitmap_2;
    ret = bitmap_2.deserialize(buf_2);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::SINGLE, bitmap_2, 0, 1);

    // medium bitmap
    size = _medium_bitmap.getSizeInBytes();
    char buf_3[size];
    _medium_bitmap.write(buf_3);
    BitmapValue bitmap_3;
    ret = bitmap_3.deserialize(buf_3);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::SET, bitmap_3, 0, 14);

    // large bitmap
    size = _large_bitmap.getSizeInBytes();
    char buf_4[size];
    _large_bitmap.write(buf_4);
    BitmapValue bitmap_4;
    ret = bitmap_4.deserialize(buf_4);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::BITMAP, bitmap_4, 0, 64);
}

TEST_F(BitmapValueTest, test_valid_and_deserialize) {
    // empty bitmap
    size_t size = _empty_bitmap.getSizeInBytes();
    char buf_1[size];
    _empty_bitmap.write(buf_1);
    BitmapValue bitmap_1;
    bool ret = bitmap_1.valid_and_deserialize(buf_1, size);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::EMPTY, bitmap_1, 0, 0);

    // single bitmap
    size = _single_bitmap.getSizeInBytes();
    char buf_2[size];
    _single_bitmap.write(buf_2);
    BitmapValue bitmap_2;
    ret = bitmap_2.valid_and_deserialize(buf_2, size);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::SINGLE, bitmap_2, 0, 1);

    // medium bitmap
    size = _medium_bitmap.getSizeInBytes();
    char buf_3[size];
    _medium_bitmap.write(buf_3);
    BitmapValue bitmap_3;
    ret = bitmap_3.valid_and_deserialize(buf_3, size);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::SET, bitmap_3, 0, 14);

    // large bitmap
    size = _large_bitmap.getSizeInBytes();
    char buf_4[size];
    _large_bitmap.write(buf_4);
    BitmapValue bitmap_4(_large_bitmap);
    ret = bitmap_4.valid_and_deserialize(buf_4, size);
    ASSERT_TRUE(ret);
    check_bitmap(BitmapDataType::BITMAP, bitmap_4, 0, 64);
    ASSERT_FALSE(bitmap_4.is_shared());

    // invalid bitmap (invalid size)
    BitmapValue bitmap_5;
    ret = bitmap_5.valid_and_deserialize(buf_4, 0);
    ASSERT_FALSE(ret);

    // invalid bitmap (nullptr)
    BitmapValue bitmap_6;
    ret = bitmap_6.valid_and_deserialize(nullptr, 10);
    ASSERT_TRUE(ret);

    // invalid bitmap (invalid string)
    BitmapValue bitmap_7;
    char buf_5[5] = "1234";
    ret = bitmap_7.valid_and_deserialize(buf_5, 5);
    ASSERT_FALSE(ret);
}

TEST_F(BitmapValueTest, bitmap_to_string) {
    ASSERT_STREQ("", _empty_bitmap.to_string().c_str());
    ASSERT_STREQ("0", _single_bitmap.to_string().c_str());
    ASSERT_STREQ("0,1,2,3,4,5,6,7,8,9,10,11,12,13", _medium_bitmap.to_string().c_str());
    ASSERT_STREQ(
            "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,"
            "37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63",
            _large_bitmap.to_string().c_str());
}

TEST_F(BitmapValueTest, bitmap_to_array) {
    std::vector<int64_t> array_1;
    _empty_bitmap.to_array(&array_1);
    ASSERT_EQ(array_1.size(), 0);

    std::vector<int64_t> array_2;
    _single_bitmap.to_array(&array_2);
    ASSERT_EQ(array_2.size(), 1);
    ASSERT_EQ(array_2[0], 0);

    std::vector<int64_t> array_3;
    _medium_bitmap.to_array(&array_3);
    ASSERT_EQ(array_3.size(), 14);
    for (size_t i = 0; i < 14; i++) {
        ASSERT_EQ(array_3[i], i);
    }

    std::vector<int64_t> array_4;
    _large_bitmap.to_array(&array_4);
    ASSERT_EQ(array_4.size(), 64);
    for (size_t i = 0; i < 64; i++) {
        ASSERT_EQ(array_4[i], i);
    }
}

TEST_F(BitmapValueTest, bitmap_compress) {
    BitmapValue bitmap;
    for (size_t i = 0; i < 1000; i++) {
        bitmap.add(i);
    }

    size_t size_1 = bitmap.serialize_size();
    bitmap.compress();
    size_t size_2 = bitmap.serialize_size();
    ASSERT_LT(size_2, size_1);
}

<<<<<<< HEAD
=======
TEST_F(BitmapValueTest, bitmap_clear) {
    BitmapValue bitmap_1(_large_bitmap);
    bitmap_1.clear();
    check_bitmap(BitmapDataType::EMPTY, bitmap_1, 0, 0);
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);

    auto bitmap_2 = gen_bitmap(0, 64);
    bitmap_2.clear();
    check_bitmap(BitmapDataType::EMPTY, bitmap_2, 0, 0);
}

TEST_F(BitmapValueTest, bitmap_reset) {
    BitmapValue bitmap_1(_large_bitmap);
    bitmap_1.reset();
    ASSERT_EQ(BitmapDataType::EMPTY, bitmap_1.type());
    check_bitmap(BitmapDataType::BITMAP, _large_bitmap, 0, 64);
}

TEST_F(BitmapValueTest, sub_bitmap_internal) {
    // empty
    BitmapValue bitmap_1;
    int64_t ret = _empty_bitmap.sub_bitmap_internal(0, 1, &bitmap_1);
    ASSERT_EQ(ret, 0);

    // single
    BitmapValue bitmap_2;
    ret = _single_bitmap.sub_bitmap_internal(0, 1, &bitmap_2);
    ASSERT_EQ(ret, 1);
    check_bitmap(BitmapDataType::SINGLE, bitmap_2, 0, 1);

    // set (offset > 0)
    BitmapValue bitmap_3;
    ret = _medium_bitmap.sub_bitmap_internal(3, 5, &bitmap_3);
    ASSERT_EQ(ret, 5);
    check_bitmap(BitmapDataType::SET, bitmap_3, 3, 8);

    // set (offset < 0)
    BitmapValue bitmap_4;
    ret = _medium_bitmap.sub_bitmap_internal(-5, 2, &bitmap_4);
    ASSERT_EQ(ret, 2);
    check_bitmap(BitmapDataType::SET, bitmap_4, 9, 11);

    // bitmap (offset > 0)
    BitmapValue bitmap_5;
    ret = _large_bitmap.sub_bitmap_internal(3, 5, &bitmap_5);
    ASSERT_EQ(ret, 5);
    check_bitmap(BitmapDataType::SET, bitmap_5, 3, 8);

    // bitmap (offset < 0)
    BitmapValue bitmap_6;
    ret = _large_bitmap.sub_bitmap_internal(-5, 2, &bitmap_6);
    ASSERT_EQ(ret, 2);
    check_bitmap(BitmapDataType::SET, bitmap_6, 59, 61);

    // set (offset > 0 && invalid offset)
    BitmapValue bitmap_7;
    ret = _medium_bitmap.sub_bitmap_internal(100, 2, &bitmap_7);
    ASSERT_EQ(ret, 0);

    // set (offset < 0 && invalid offset)
    BitmapValue bitmap_8;
    ret = _medium_bitmap.sub_bitmap_internal(-100, 2, &bitmap_8);
    ASSERT_EQ(ret, 0);

    // bitmap (offset > 0 && invalid offset)
    BitmapValue bitmap_9;
    ret = _large_bitmap.sub_bitmap_internal(100, 2, &bitmap_9);
    ASSERT_EQ(ret, 0);

    // bitmap (offset < 0 && invalid offset)
    BitmapValue bitmap_10;
    ret = _large_bitmap.sub_bitmap_internal(100, 2, &bitmap_10);
    ASSERT_EQ(ret, 0);
}

TEST_F(BitmapValueTest, subset_limit) {
    // empty
    BitmapValue bitmap_1;
    int64_t ret = _empty_bitmap.bitmap_subset_limit_internal(0, 1, &bitmap_1);
    ASSERT_EQ(ret, 0);

    // single
    BitmapValue bitmap_2;
    ret = _single_bitmap.bitmap_subset_limit_internal(0, 1, &bitmap_2);
    ASSERT_EQ(ret, 1);
    check_bitmap(BitmapDataType::SINGLE, bitmap_2, 0, 1);

    // set
    BitmapValue bitmap_3;
    ret = _medium_bitmap.bitmap_subset_limit_internal(5, 2, &bitmap_3);
    ASSERT_EQ(ret, 2);
    check_bitmap(BitmapDataType::SET, bitmap_3, 5, 7);

    // bitmap
    BitmapValue bitmap_4;
    ret = _large_bitmap.bitmap_subset_limit_internal(5, 2, &bitmap_4);
    ASSERT_EQ(ret, 2);
    check_bitmap(BitmapDataType::SET, bitmap_4, 5, 7);

    // single (invalid)
    BitmapValue bitmap_5;
    ret = _single_bitmap.bitmap_subset_limit_internal(0, 0, &bitmap_5);
    ASSERT_EQ(ret, 0);
    ret = _single_bitmap.bitmap_subset_limit_internal(2, 1, &bitmap_5);
    ASSERT_EQ(ret, 0);

    // set (invalid)
    BitmapValue bitmap_6;
    ret = _medium_bitmap.bitmap_subset_limit_internal(3, 0, &bitmap_6);
    ASSERT_EQ(ret, 0);
    ret = _medium_bitmap.bitmap_subset_limit_internal(15, 1, &bitmap_6);
    ASSERT_EQ(ret, 0);

    // bitmap (invalid)
    BitmapValue bitmap_7;
    ret = _large_bitmap.bitmap_subset_limit_internal(3, 0, &bitmap_7);
    ASSERT_EQ(ret, 0);
    ret = _large_bitmap.bitmap_subset_limit_internal(68, 1, &bitmap_7);
    ASSERT_EQ(ret, 0);
}

TEST_F(BitmapValueTest, subset_in_range) {
    // empty
    BitmapValue bitmap_1;
    int64_t ret = _empty_bitmap.bitmap_subset_in_range_internal(0, 1, &bitmap_1);
    ASSERT_EQ(ret, 0);

    // single
    BitmapValue bitmap_2;
    ret = _single_bitmap.bitmap_subset_in_range_internal(0, 1, &bitmap_2);
    ASSERT_EQ(ret, 1);
    check_bitmap(BitmapDataType::SINGLE, bitmap_2, 0, 1);

    // set
    BitmapValue bitmap_3;
    ret = _medium_bitmap.bitmap_subset_in_range_internal(5, 7, &bitmap_3);
    ASSERT_EQ(ret, 2);
    check_bitmap(BitmapDataType::SET, bitmap_3, 5, 7);

    // bitmap
    BitmapValue bitmap_4;
    ret = _large_bitmap.bitmap_subset_in_range_internal(5, 7, &bitmap_4);
    ASSERT_EQ(ret, 2);
    check_bitmap(BitmapDataType::SET, bitmap_4, 5, 7);

    // single (invalid)
    BitmapValue bitmap_5;
    ret = _single_bitmap.bitmap_subset_in_range_internal(0, 0, &bitmap_5);
    ASSERT_EQ(ret, 0);
    ret = _single_bitmap.bitmap_subset_in_range_internal(2, 3, &bitmap_5);
    ASSERT_EQ(ret, 0);

    // set (invalid)
    BitmapValue bitmap_6;
    ret = _medium_bitmap.bitmap_subset_limit_internal(15, 16, &bitmap_6);
    ASSERT_EQ(ret, 0);

    // bitmap (invalid)
    BitmapValue bitmap_7;
    ret = _medium_bitmap.bitmap_subset_limit_internal(68, 69, &bitmap_7);
    ASSERT_EQ(ret, 0);
}

std::string convert_bitmap_to_string(BitmapValue& bitmap) {
    std::string buf;
    buf.resize(bitmap.getSizeInBytes());
    bitmap.write((char*)buf.c_str());
    return buf;
}

TEST(BitmapValueTest1, bitmap_serde) {
    bool use_v1 = config::bitmap_serialize_version == 1;
    BitmapTypeCode::type type_bitmap32 = BitmapTypeCode::BITMAP32_SERIV2;
    BitmapTypeCode::type type_bitmap64 = BitmapTypeCode::BITMAP64_SERIV2;
    if (use_v1) {
        type_bitmap32 = BitmapTypeCode::BITMAP32;
        type_bitmap64 = BitmapTypeCode::BITMAP64;
    }
    { // EMPTY
        BitmapValue empty;
        std::string buffer = convert_bitmap_to_string(empty);
        std::string expect_buffer(1, BitmapTypeCode::EMPTY);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(0, out.cardinality());
    }
    { // SINGLE32
        uint32_t i = UINT32_MAX;
        BitmapValue single32(i);
        std::string buffer = convert_bitmap_to_string(single32);
        std::string expect_buffer(1, BitmapTypeCode::SINGLE32);
        put_fixed32_le(&expect_buffer, i);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(1, out.cardinality());
        ASSERT_TRUE(out.contains(i));
    }
    { // BITMAP32
        BitmapValue bitmap32(std::vector<uint64_t>{0, UINT32_MAX});
        std::string buffer = convert_bitmap_to_string(bitmap32);

        roaring::Roaring roaring;
        roaring.add(0);
        roaring.add(UINT32_MAX);
        std::string expect_buffer(1, type_bitmap32);
        expect_buffer.resize(1 + roaring.getSizeInBytes(use_v1));
        roaring.write(&expect_buffer[1], use_v1);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(2, out.cardinality());
        ASSERT_TRUE(out.contains(0));
        ASSERT_TRUE(out.contains(UINT32_MAX));
    }
    { // SINGLE64
        uint64_t i = static_cast<uint64_t>(UINT32_MAX) + 1;
        BitmapValue single64(i);
        std::string buffer = convert_bitmap_to_string(single64);
        std::string expect_buffer(1, BitmapTypeCode::SINGLE64);
        put_fixed64_le(&expect_buffer, i);
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(1, out.cardinality());
        ASSERT_TRUE(out.contains(i));
    }
    { // BITMAP64
        BitmapValue bitmap64(std::vector<uint64_t>{0, static_cast<uint64_t>(UINT32_MAX) + 1});
        std::string buffer = convert_bitmap_to_string(bitmap64);

        roaring::Roaring roaring;
        roaring.add(0);
        std::string expect_buffer(1, type_bitmap64);
        put_varint64(&expect_buffer, 2); // map size
        for (uint32_t i = 0; i < 2; ++i) {
            std::string map_entry;
            put_fixed32_le(&map_entry, i); // map key
            map_entry.resize(sizeof(uint32_t) + roaring.getSizeInBytes(use_v1));
            roaring.write(&map_entry[4], use_v1); // map value

            expect_buffer.append(map_entry);
        }
        ASSERT_EQ(expect_buffer, buffer);

        BitmapValue out(buffer.data());
        ASSERT_EQ(2, out.cardinality());
        ASSERT_TRUE(out.contains(0));
        ASSERT_TRUE(out.contains(static_cast<uint64_t>(UINT32_MAX) + 1));
    }
}

// Forked from CRoaring's UT of Roaring64Map
TEST(BitmapValueTest1, Roaring64Map) {
    using starrocks::detail::Roaring64Map;
    // create a new empty bitmap
    Roaring64Map r1;
    uint64_t r1_sum = 0;
    // then we can add values
    for (uint64_t i = 100; i < 1000; i++) {
        r1.add(i);
        r1_sum += i;
    }
    for (uint64_t i = 14000000000000000100ull; i < 14000000000000001000ull; i++) {
        r1.add(i);
        r1_sum += i;
    }
    ASSERT_TRUE(r1.contains((uint64_t)14000000000000000500ull));
    ASSERT_EQ(1800, r1.cardinality());
    size_t size_before = r1.getSizeInBytes(config::bitmap_serialize_version);
    r1.runOptimize();
    size_t size_after = r1.getSizeInBytes(config::bitmap_serialize_version);
    ASSERT_LT(size_after, size_before);

    Roaring64Map r2 = Roaring64Map::bitmapOf(5, 1ull, 2ull, 234294967296ull, 195839473298ull, 14000000000000000100ull);
    ASSERT_EQ(1ull, r2.minimum());
    ASSERT_EQ(14000000000000000100ull, r2.maximum());
    ASSERT_EQ(4ull, r2.rank(234294967296ull));

    // we can also create a bitmap from a pointer to 32-bit integers
    const uint32_t values[] = {2, 3, 4};
    Roaring64Map r3(3, values);
    ASSERT_EQ(3, r3.cardinality());

    // we can also go in reverse and go from arrays to bitmaps
    uint64_t card1 = r1.cardinality();
    auto* arr1 = new uint64_t[card1];
    ASSERT_TRUE(arr1 != nullptr);
    r1.toUint64Array(arr1);
    Roaring64Map r1f(card1, arr1);
    delete[] arr1;
    // bitmaps shall be equal
    ASSERT_TRUE(r1 == r1f);

    // we can copy and compare bitmaps
    Roaring64Map z(r3);
    ASSERT_TRUE(r3 == z);

    // we can compute union two-by-two
    Roaring64Map r1_2_3 = r1 | r2;
    r1_2_3 |= r3;

    // we can compute a big union
    const Roaring64Map* allmybitmaps[] = {&r1, &r2, &r3};
    Roaring64Map bigunion = Roaring64Map::fastunion(3, allmybitmaps);
    ASSERT_TRUE(r1_2_3 == bigunion);
    ASSERT_EQ(1806, r1_2_3.cardinality());

    // we can compute intersection two-by-two
    Roaring64Map i1_2 = r1 & r2;
    ASSERT_EQ(1, i1_2.cardinality());

    // we can write a bitmap to a pointer and recover it later
    uint32_t expectedsize = r1.getSizeInBytes(config::bitmap_serialize_version);
    char* serializedbytes = new char[expectedsize];
    r1.write(serializedbytes, config::bitmap_serialize_version);
    Roaring64Map t = Roaring64Map::read(serializedbytes);
    ASSERT_TRUE(r1 == t);
    delete[] serializedbytes;

    // we can iterate over all values using custom functions
    uint64_t sum = 0;
    auto func = [](uint64_t value, void* param) {
        *(uint64_t*)param += value;
        return true; // we always process all values
    };
    r1.iterate(func, &sum);
    ASSERT_EQ(r1_sum, sum);

    // we can also iterate the C++ way
    sum = 0;
    for (unsigned long i : t) {
        sum += i;
    }
    ASSERT_EQ(r1_sum, sum);
}

>>>>>>> 44ae317858 ([Enhancement] BitmapValue support copy on write (#34047))
} // namespace starrocks
