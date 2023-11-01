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

#include <cstdint>
#include <string>

#include "column/vectorized_fwd.h"
#include "exprs/bitmap_functions.h"
#include "exprs/function_context.h"
#include "types/bitmap_value_detail.h"
#include "util/coding.h"

namespace starrocks {

TEST(BitmapValueTest, constructor) {
    BitmapValue bitmap;
    for (size_t i = 0; i < 64; i++) {
        bitmap.add(i);
    }

    BitmapValue shallow_bitmap(bitmap, false);
    shallow_bitmap.add(64);
    ASSERT_EQ(bitmap.cardinality(), 65);
}

TEST(BitmapValueTest, bitmap_union) {
    BitmapValue empty;
    BitmapValue single(1024);
    BitmapValue bitmap({1024, 1025, 1026});

    ASSERT_EQ(0, empty.cardinality());
    ASSERT_EQ(1, single.cardinality());
    ASSERT_EQ(3, bitmap.cardinality());

    BitmapValue empty2;
    empty2 |= empty;
    ASSERT_EQ(0, empty2.cardinality());
    empty2 |= single;
    ASSERT_EQ(1, empty2.cardinality());
    BitmapValue empty3;
    empty3 |= bitmap;
    ASSERT_EQ(3, empty3.cardinality());

    BitmapValue single2(1025);
    single2 |= empty;
    ASSERT_EQ(1, single2.cardinality());
    single2 |= single;
    ASSERT_EQ(2, single2.cardinality());

    BitmapValue bitmap2;
    bitmap2.add(1024);
    bitmap2.add(2048);
    bitmap2.add(4096);
    bitmap2 |= empty;
    ASSERT_EQ(3, bitmap2.cardinality());
    bitmap2 |= single;
    ASSERT_EQ(3, bitmap2.cardinality());
    bitmap2 |= bitmap;
    ASSERT_EQ(5, bitmap2.cardinality());
}

TEST(BitmapValueTest, bitmap_intersect) {
    BitmapValue empty;
    BitmapValue single(1024);
    BitmapValue bitmap({1024, 1025, 1026});

    BitmapValue empty2;
    empty2 &= empty;
    ASSERT_EQ(0, empty2.cardinality());
    empty2 &= single;
    ASSERT_EQ(0, empty2.cardinality());
    empty2 &= bitmap;
    ASSERT_EQ(0, empty2.cardinality());

    BitmapValue single2(1025);
    single2 &= empty;
    ASSERT_EQ(0, single2.cardinality());

    BitmapValue single4(1025);
    single4 &= single;
    ASSERT_EQ(0, single4.cardinality());

    BitmapValue single3(1024);
    single3 &= single;
    ASSERT_EQ(1, single3.cardinality());

    single3 &= bitmap;
    ASSERT_EQ(1, single3.cardinality());

    BitmapValue single5(2048);
    single5 &= bitmap;
    ASSERT_EQ(0, single5.cardinality());

    BitmapValue bitmap2;
    bitmap2.add(1024);
    bitmap2.add(2048);
    bitmap2 &= empty;
    ASSERT_EQ(0, bitmap2.cardinality());

    BitmapValue bitmap3;
    bitmap3.add(1024);
    bitmap3.add(2048);
    bitmap3 &= single;
    ASSERT_EQ(1, bitmap3.cardinality());

    BitmapValue bitmap4;
    bitmap4.add(2049);
    bitmap4.add(2048);
    bitmap4 &= single;
    ASSERT_EQ(0, bitmap4.cardinality());

    BitmapValue bitmap5;
    bitmap5.add(2049);
    bitmap5.add(2048);
    bitmap5 &= bitmap;
    ASSERT_EQ(0, bitmap5.cardinality());

    BitmapValue bitmap6;
    bitmap6.add(1024);
    bitmap6.add(1025);
    bitmap6 &= bitmap;
    ASSERT_EQ(2, bitmap6.cardinality());
}

std::string convert_bitmap_to_string(BitmapValue& bitmap) {
    std::string buf;
    buf.resize(bitmap.getSizeInBytes());
    bitmap.write((char*)buf.c_str());
    return buf;
}

TEST(BitmapValueTest, bitmap_serde) {
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
TEST(BitmapValueTest, Roaring64Map) {
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

TEST(BitmapValueTest, bitmap_to_string) {
    BitmapValue empty;
    ASSERT_STREQ("", empty.to_string().c_str());
    empty.add(1);
    ASSERT_STREQ("1", empty.to_string().c_str());
    empty.add(2);
    ASSERT_STREQ("1,2", empty.to_string().c_str());
}

TEST(BitmapValueTest, bitmap_single_convert) {
    BitmapValue bitmap;
    ASSERT_STREQ("", bitmap.to_string().c_str());
    bitmap.add(1);
    ASSERT_STREQ("1", bitmap.to_string().c_str());
    bitmap.add(1);
    ASSERT_STREQ("1", bitmap.to_string().c_str());
    ASSERT_EQ(BitmapValue::SINGLE, bitmap.type());

    BitmapValue bitmap_u;
    bitmap_u.add(1);
    bitmap |= bitmap_u;
    ASSERT_EQ(BitmapValue::SINGLE, bitmap.type());

    bitmap_u.add(2);
    ASSERT_EQ(BitmapValue::SET, bitmap_u.type());

    bitmap |= bitmap_u;
    ASSERT_EQ(BitmapValue::SET, bitmap.type());
}

TEST(BitmapValueTest, bitmap_max) {
    std::unique_ptr<FunctionContext> ctx_ptr;
    FunctionContext* ctx;
    ctx_ptr.reset(FunctionContext::create_test_context());
    ctx = ctx_ptr.get();

    BitmapValue bitmap;
    Columns columns;
    auto s = BitmapColumn::create();
    s->append(&bitmap);
    columns.push_back(s);
    auto column = BitmapFunctions::bitmap_max(ctx, columns).value();
    ASSERT_TRUE(column->is_null(0));

    bitmap.add(0);
    ASSERT_EQ(bitmap.max(), 0);
    bitmap.add(1);
    ASSERT_EQ(bitmap.max(), 1);
    bitmap.add(std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(bitmap.max(), std::numeric_limits<uint64_t>::max());
    bitmap.add(std::numeric_limits<uint64_t>::lowest());
    ASSERT_EQ(bitmap.max(), std::numeric_limits<uint64_t>::max());
}

TEST(BitmapValueTest, bitmap_min) {
    std::unique_ptr<FunctionContext> ctx_ptr;
    FunctionContext* ctx;
    ctx_ptr.reset(FunctionContext::create_test_context());
    ctx = ctx_ptr.get();

    BitmapValue bitmap;
    Columns columns;
    auto s = BitmapColumn::create();
    s->append(&bitmap);
    columns.push_back(s);
    auto column = BitmapFunctions::bitmap_min(ctx, columns).value();
    ASSERT_TRUE(column->is_null(0));

    bitmap.add(std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(bitmap.min(), std::numeric_limits<uint64_t>::max());
    bitmap.add(1);
    ASSERT_EQ(bitmap.min(), 1);
    bitmap.add(5);
    ASSERT_EQ(bitmap.min(), 1);
    bitmap.add(0);
    ASSERT_EQ(bitmap.min(), 0);
}

TEST(BitmapValueTest, bitmap_xor) {
    // {3} ^ {1,2} = {1,2,3}
    {
        BitmapValue bm1;
        bm1.add(3);
        BitmapValue bm2;
        bm2.add(1);
        bm2.add(2);

        bm1 ^= bm2;
        ASSERT_EQ(3, bm1.cardinality());
        ASSERT_EQ(3, bm1.max());
        ASSERT_EQ(1, bm1.min());

        // and b2 should not be changed
        ASSERT_EQ(2, bm2.cardinality());
    }
}

} // namespace starrocks
