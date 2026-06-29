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

#include "exec/iceberg/iceberg_deletion_vector_reader.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include <vector>

#include "gutil/endian.h"

namespace starrocks {

// Build a complete Iceberg DV blob from a set of positions.
// length(4B BE) | magic D1 D3 39 64 | roaring64 portable body | crc32(4B BE over magic+body)
static std::vector<uint8_t> make_blob(const std::vector<uint64_t>& positions, bool corrupt_magic = false,
                                      bool corrupt_crc = false, int length_delta = 0) {
    roaring64_bitmap_t* b = roaring64_bitmap_create();
    for (uint64_t p : positions) {
        roaring64_bitmap_add(b, p);
    }
    size_t body_len = roaring64_bitmap_portable_size_in_bytes(b);
    std::vector<char> body(body_len);
    roaring64_bitmap_portable_serialize(b, body.data());
    roaring::api::roaring64_bitmap_free(b);

    int64_t size = 4 + 4 + static_cast<int64_t>(body_len) + 4;
    std::vector<uint8_t> blob(size);
    // length prefix (BE) = size - 8
    BigEndian::Store32(blob.data(), static_cast<uint32_t>(size - 8 + length_delta));
    // magic
    blob[4] = 0xD1;
    blob[5] = 0xD3;
    blob[6] = 0x39;
    blob[7] = corrupt_magic ? 0x00 : 0x64;
    // body
    memcpy(blob.data() + 8, body.data(), body_len);
    // crc over magic + body (BE)
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(blob.data() + 4), static_cast<uInt>(4 + body_len));
    uint32_t crc32v = static_cast<uint32_t>(crc) ^ (corrupt_crc ? 0xFFFFFFFFu : 0u);
    BigEndian::Store32(blob.data() + size - 4, crc32v);
    return blob;
}

static std::vector<uint64_t> to_vec(roaring64_bitmap_t* b) {
    std::vector<uint64_t> out(roaring64_bitmap_get_cardinality(b));
    roaring64_bitmap_to_uint64_array(b, out.data());
    return out;
}

TEST(IcebergDeletionVectorReaderTest, ParseSingleBlob) {
    std::vector<uint64_t> pos = {3, 4, 7, 11, 18, 29};
    auto blob = make_blob(pos);
    IcebergDVBuildStats stats;
    auto st = IcebergDeletionVectorReader::parse_dv_blob(blob.data(), blob.size(), 6, &stats);
    ASSERT_TRUE(st.ok()) << st.status().message();
    roaring64_bitmap_t* b = st.value();
    EXPECT_EQ(pos, to_vec(b));
    EXPECT_EQ(6, stats.cardinality);
    EXPECT_EQ(1, stats.build_count);
    roaring::api::roaring64_bitmap_free(b);
}

TEST(IcebergDeletionVectorReaderTest, MultiBlobSelectByOffsetAndSize) {
    // Concatenate two independent blobs; parse the second one by offset/size only.
    auto blob0 = make_blob({1, 2});
    auto blob1 = make_blob({100, (1ULL << 40) + 7});
    std::vector<uint8_t> file;
    file.insert(file.end(), blob0.begin(), blob0.end());
    file.insert(file.end(), blob1.begin(), blob1.end());

    int64_t offset = static_cast<int64_t>(blob0.size());
    int64_t size = static_cast<int64_t>(blob1.size());
    auto st = IcebergDeletionVectorReader::parse_dv_blob(file.data() + offset, size, 2, nullptr);
    ASSERT_TRUE(st.ok()) << st.status().message();
    roaring64_bitmap_t* b = st.value();
    std::vector<uint64_t> expected = {100, (1ULL << 40) + 7};
    EXPECT_EQ(expected, to_vec(b));
    roaring::api::roaring64_bitmap_free(b);
}

TEST(IcebergDeletionVectorReaderTest, Supports64BitPosition) {
    std::vector<uint64_t> pos = {1, (1ULL << 40) + 5};
    auto blob = make_blob(pos);
    auto st = IcebergDeletionVectorReader::parse_dv_blob(blob.data(), blob.size(), 2, nullptr);
    ASSERT_TRUE(st.ok());
    roaring64_bitmap_t* b = st.value();
    EXPECT_EQ(pos, to_vec(b));
    roaring::api::roaring64_bitmap_free(b);
}

TEST(IcebergDeletionVectorReaderTest, BadMagicIsCorruption) {
    auto blob = make_blob({1, 2}, /*corrupt_magic=*/true);
    auto st = IcebergDeletionVectorReader::parse_dv_blob(blob.data(), blob.size(), 2, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(IcebergDeletionVectorReaderTest, BadCrcIsCorruption) {
    auto blob = make_blob({1, 2}, false, /*corrupt_crc=*/true);
    auto st = IcebergDeletionVectorReader::parse_dv_blob(blob.data(), blob.size(), 2, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(IcebergDeletionVectorReaderTest, BadLengthPrefixIsCorruption) {
    auto blob = make_blob({1, 2}, false, false, /*length_delta=*/1);
    auto st = IcebergDeletionVectorReader::parse_dv_blob(blob.data(), blob.size(), 2, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(IcebergDeletionVectorReaderTest, CardinalityMismatchIsCorruption) {
    auto blob = make_blob({1, 2, 3});
    auto st = IcebergDeletionVectorReader::parse_dv_blob(blob.data(), blob.size(), 99, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(IcebergDeletionVectorReaderTest, TooSmallIsCorruption) {
    std::vector<uint8_t> tiny(8, 0);
    auto st = IcebergDeletionVectorReader::parse_dv_blob(tiny.data(), tiny.size(), 0, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

} // namespace starrocks