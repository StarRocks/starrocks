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

#include "formats/puffin/deletion_vector_blob.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include <vector>

#include "formats/iceberg/iceberg_deletion_vector_reader.h"
#include "gutil/endian.h"

namespace starrocks::formats {

static std::vector<uint64_t> to_vec(roaring64_bitmap_t* b) {
    std::vector<uint64_t> out(roaring64_bitmap_get_cardinality(b));
    roaring64_bitmap_to_uint64_array(b, out.data());
    return out;
}

TEST(DeletionVectorBlobTest, RoundTripThroughReader) {
    roaring64_bitmap_t* b = roaring64_bitmap_create();
    std::vector<uint64_t> pos = {3, 4, 7, 11, 18, 29};
    for (uint64_t p : pos) roaring64_bitmap_add(b, p);

    auto blob = build_deletion_vector_blob(b);
    roaring::api::roaring64_bitmap_free(b);
    ASSERT_TRUE(blob.ok()) << blob.status().message();

    const auto* data = reinterpret_cast<const uint8_t*>(blob->data());
    auto st = IcebergDeletionVectorReader::parse_dv_blob(data, blob->size(), pos.size(), nullptr);
    ASSERT_TRUE(st.ok()) << st.status().message();
    roaring64_bitmap_t* got = st.value();
    EXPECT_EQ(pos, to_vec(got));
    roaring::api::roaring64_bitmap_free(got);
}

TEST(DeletionVectorBlobTest, Supports64BitPositions) {
    roaring64_bitmap_t* b = roaring64_bitmap_create();
    std::vector<uint64_t> pos = {1ULL, (1ULL << 32) + 5, (1ULL << 40)};
    for (uint64_t p : pos) roaring64_bitmap_add(b, p);

    auto blob = build_deletion_vector_blob(b);
    roaring::api::roaring64_bitmap_free(b);
    ASSERT_TRUE(blob.ok()) << blob.status().message();

    auto st = IcebergDeletionVectorReader::parse_dv_blob(reinterpret_cast<const uint8_t*>(blob->data()), blob->size(),
                                                         pos.size(), nullptr);
    ASSERT_TRUE(st.ok()) << st.status().message();
    roaring64_bitmap_t* got = st.value();
    EXPECT_EQ(pos, to_vec(got));
    roaring::api::roaring64_bitmap_free(got);
}

TEST(DeletionVectorBlobTest, FramingIsByteExact) {
    roaring64_bitmap_t* b = roaring64_bitmap_create();
    for (uint64_t p : {5ULL, 6ULL, 100ULL}) roaring64_bitmap_add(b, p);
    const size_t body_len = roaring64_bitmap_portable_size_in_bytes(b);
    auto blob = build_deletion_vector_blob(b);
    roaring::api::roaring64_bitmap_free(b);
    ASSERT_TRUE(blob.ok()) << blob.status().message();

    const auto* p = reinterpret_cast<const uint8_t*>(blob->data());
    const int64_t size = blob->size();
    // total size == length(4) + magic(4) + body + crc(4)
    EXPECT_EQ(4 + 4 + static_cast<int64_t>(body_len) + 4, size);
    // length prefix (BE) == size - 8
    EXPECT_EQ(static_cast<uint32_t>(size - 8), BigEndian::Load32(p));
    // magic D1 D3 39 64
    EXPECT_EQ(0xD1, p[4]);
    EXPECT_EQ(0xD3, p[5]);
    EXPECT_EQ(0x39, p[6]);
    EXPECT_EQ(0x64, p[7]);
    // crc (BE) == crc32 over magic + body
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(p + 4), static_cast<uInt>(4 + body_len));
    EXPECT_EQ(static_cast<uint32_t>(crc), BigEndian::Load32(p + size - 4));
}

TEST(DeletionVectorBlobTest, NullBitmapIsRejected) {
    auto blob = build_deletion_vector_blob(nullptr);
    ASSERT_FALSE(blob.ok());
}

TEST(DeletionVectorBlobTest, HighBitPositionIsRejected) {
    roaring64_bitmap_t* b = roaring64_bitmap_create();
    roaring64_bitmap_add(b, 7);                 // a valid position
    roaring64_bitmap_add(b, uint64_t(1) << 63); // MSB set: not a valid Iceberg row position
    auto blob = build_deletion_vector_blob(b);
    roaring::api::roaring64_bitmap_free(b);
    ASSERT_FALSE(blob.ok());
}

} // namespace starrocks::formats
