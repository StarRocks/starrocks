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

#include <cstring>
#include <vector>

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
    auto st = parse_deletion_vector_blob(data, blob->size(), pos.size(), nullptr);
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

    auto st = parse_deletion_vector_blob(reinterpret_cast<const uint8_t*>(blob->data()), blob->size(), pos.size(),
                                         nullptr);
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

// ===== parse-side validation =====
// The builder cannot produce a corrupt blob, so these hand-frame one to exercise each check.

// Wrap an arbitrary body in the Iceberg DV framing (length | magic | body | crc) with a valid
// length prefix, magic and crc, so only the roaring body itself is (in)valid.
static std::vector<uint8_t> frame_body(const std::vector<uint8_t>& body) {
    int64_t size = 4 + 4 + static_cast<int64_t>(body.size()) + 4;
    std::vector<uint8_t> blob(size);
    BigEndian::Store32(blob.data(), static_cast<uint32_t>(size - 8));
    blob[4] = 0xD1;
    blob[5] = 0xD3;
    blob[6] = 0x39;
    blob[7] = 0x64;
    if (!body.empty()) {
        memcpy(blob.data() + 8, body.data(), body.size());
    }
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(blob.data() + 4), static_cast<uInt>(4 + body.size()));
    BigEndian::Store32(blob.data() + size - 4, static_cast<uint32_t>(crc));
    return blob;
}

// Build a complete blob from a set of positions, optionally corrupting one framing field.
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
    BigEndian::Store32(blob.data(), static_cast<uint32_t>(size - 8 + length_delta));
    blob[4] = 0xD1;
    blob[5] = 0xD3;
    blob[6] = 0x39;
    blob[7] = corrupt_magic ? 0x00 : 0x64;
    memcpy(blob.data() + 8, body.data(), body_len);
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(blob.data() + 4), static_cast<uInt>(4 + body_len));
    uint32_t crc32v = static_cast<uint32_t>(crc) ^ (corrupt_crc ? 0xFFFFFFFFu : 0u);
    BigEndian::Store32(blob.data() + size - 4, crc32v);
    return blob;
}

TEST(DeletionVectorBlobTest, ParseSingleBlobFillsStats) {
    std::vector<uint64_t> pos = {3, 4, 7, 11, 18, 29};
    auto blob = make_blob(pos);
    IcebergDVBuildStats stats;
    auto st = parse_deletion_vector_blob(blob.data(), blob.size(), 6, &stats);
    ASSERT_TRUE(st.ok()) << st.status().message();
    roaring64_bitmap_t* b = st.value();
    EXPECT_EQ(pos, to_vec(b));
    EXPECT_EQ(6, stats.cardinality);
    EXPECT_EQ(1, stats.build_count);
    roaring::api::roaring64_bitmap_free(b);
}

TEST(DeletionVectorBlobTest, MultiBlobSelectByOffsetAndSize) {
    // Concatenate two independent blobs; parse the second one by offset/size only.
    auto blob0 = make_blob({1, 2});
    auto blob1 = make_blob({100, (1ULL << 40) + 7});
    std::vector<uint8_t> file;
    file.insert(file.end(), blob0.begin(), blob0.end());
    file.insert(file.end(), blob1.begin(), blob1.end());

    int64_t offset = static_cast<int64_t>(blob0.size());
    int64_t size = static_cast<int64_t>(blob1.size());
    auto st = parse_deletion_vector_blob(file.data() + offset, size, 2, nullptr);
    ASSERT_TRUE(st.ok()) << st.status().message();
    roaring64_bitmap_t* b = st.value();
    std::vector<uint64_t> expected = {100, (1ULL << 40) + 7};
    EXPECT_EQ(expected, to_vec(b));
    roaring::api::roaring64_bitmap_free(b);
}

TEST(DeletionVectorBlobTest, BadMagicIsCorruption) {
    auto blob = make_blob({1, 2}, /*corrupt_magic=*/true);
    auto st = parse_deletion_vector_blob(blob.data(), blob.size(), 2, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(DeletionVectorBlobTest, BadCrcIsCorruption) {
    auto blob = make_blob({1, 2}, false, /*corrupt_crc=*/true);
    auto st = parse_deletion_vector_blob(blob.data(), blob.size(), 2, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(DeletionVectorBlobTest, BadLengthPrefixIsCorruption) {
    auto blob = make_blob({1, 2}, false, false, /*length_delta=*/1);
    auto st = parse_deletion_vector_blob(blob.data(), blob.size(), 2, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(DeletionVectorBlobTest, CardinalityMismatchIsCorruption) {
    auto blob = make_blob({1, 2, 3});
    auto st = parse_deletion_vector_blob(blob.data(), blob.size(), 99, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

TEST(DeletionVectorBlobTest, TooSmallIsCorruption) {
    std::vector<uint8_t> tiny(8, 0);
    auto st = parse_deletion_vector_blob(tiny.data(), tiny.size(), 0, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

// A blob whose framing (length/magic/crc) is valid but whose roaring body is not a portable
// serialization: the safe deserializer returns null and parse must report corruption.
TEST(DeletionVectorBlobTest, RoaringDeserializeFailureIsCorruption) {
    auto blob = frame_body({0xFF, 0xFF, 0xFF, 0xFF});
    auto st = parse_deletion_vector_blob(blob.data(), blob.size(), -1, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

// record_count < 0 disables the cardinality cross-check.
TEST(DeletionVectorBlobTest, NegativeRecordCountSkipsCardinalityCheck) {
    auto blob = make_blob({1, 2, 3});
    auto st = parse_deletion_vector_blob(blob.data(), blob.size(), -1, nullptr);
    ASSERT_TRUE(st.ok()) << st.status().message();
    roaring::api::roaring64_bitmap_free(st.value());
}

} // namespace starrocks::formats
