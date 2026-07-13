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

#include "formats/iceberg/iceberg_deletion_vector_reader.h"

#include <gtest/gtest.h>
#include <zlib.h>

#include <filesystem>
#include <vector>

#include "base/testutil/assert.h"
#include "common/runtime_profile.h"
#include "fs/fs.h"
#include "gutil/endian.h"

namespace starrocks::formats {

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

// A blob whose framing (length/magic/crc) is valid but whose roaring body is not a portable
// serialization: the safe deserializer returns null and parse must report corruption.
TEST(IcebergDeletionVectorReaderTest, RoaringDeserializeFailureIsCorruption) {
    auto blob = frame_body({0xFF, 0xFF, 0xFF, 0xFF});
    auto st = IcebergDeletionVectorReader::parse_dv_blob(blob.data(), blob.size(), -1, nullptr);
    EXPECT_TRUE(st.status().is_corruption());
}

class IcebergDeletionVectorReaderFillTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = "./iceberg_dv_reader_test_" + std::to_string(::getpid());
        std::filesystem::create_directories(_test_dir);
    }
    void TearDown() override {
        if (std::filesystem::exists(_test_dir)) {
            std::filesystem::remove_all(_test_dir);
        }
    }

    std::string write_file(const std::string& name, const std::vector<uint8_t>& bytes) {
        std::string path = _test_dir + "/" + name;
        ASSIGN_OR_ABORT(auto wf, FileSystem::Default()->new_writable_file(path));
        CHECK_OK(wf->append(Slice(reinterpret_cast<const char*>(bytes.data()), bytes.size())));
        CHECK_OK(wf->close());
        return path;
    }

    static IcebergDeletionVectorReaderOptions make_options(const std::string& path, int64_t offset, int64_t size,
                                                           int64_t record_count,
                                                           RuntimeProfile* runtime_profile = nullptr) {
        TIcebergDeletionVectorDescriptor descriptor;
        descriptor.puffin_file_path = path;
        descriptor.content_offset = offset;
        descriptor.content_size_in_bytes = size;
        descriptor.record_count = record_count;
        descriptor.referenced_data_file = "data.parquet";
        return IcebergDeletionVectorReaderOptions{
                .descriptor = std::move(descriptor),
                .fs = FileSystem::Default(),
                .runtime_profile = runtime_profile,
        };
    }

    std::string _test_dir;
};

TEST_F(IcebergDeletionVectorReaderFillTest, FillRowIndexesSuccess) {
    auto blob = make_blob({3, 7, 42});
    std::string path = write_file("dv.puffin", blob);

    IcebergDeletionVectorReader reader(make_options(path, 0, static_cast<int64_t>(blob.size()), 3));
    auto skip_ctx = std::make_shared<SkipRowsContext>();
    ASSERT_OK(reader.fill_row_indexes(skip_ctx));
    ASSERT_NE(nullptr, skip_ctx->deletion_bitmap);
    EXPECT_EQ(3, skip_ctx->deletion_bitmap->get_cardinality());
}

// With a non-null runtime_profile, fill_row_indexes must publish counters via update_counter.
TEST_F(IcebergDeletionVectorReaderFillTest, FillRowIndexesUpdatesProfile) {
    auto blob = make_blob({1, 2, 3, 4});
    std::string path = write_file("dv_profile.puffin", blob);

    RuntimeProfile profile("IcebergDVTest");
    IcebergDeletionVectorReader reader(make_options(path, 0, static_cast<int64_t>(blob.size()), 4, &profile));
    auto skip_ctx = std::make_shared<SkipRowsContext>();
    ASSERT_OK(reader.fill_row_indexes(skip_ctx));

    auto* cardinality = profile.get_counter("IcebergDVCardinality");
    ASSERT_NE(nullptr, cardinality);
    EXPECT_EQ(4, cardinality->value());
    auto* build_count = profile.get_counter("IcebergDVBuildCount");
    ASSERT_NE(nullptr, build_count);
    EXPECT_EQ(1, build_count->value());
}

// The blob is read strictly from [content_offset, content_offset+content_size_in_bytes).
TEST_F(IcebergDeletionVectorReaderFillTest, FillRowIndexesRespectsOffset) {
    auto blob0 = make_blob({1, 2});
    auto blob1 = make_blob({100, 200, 300});
    std::vector<uint8_t> file;
    file.insert(file.end(), blob0.begin(), blob0.end());
    file.insert(file.end(), blob1.begin(), blob1.end());
    std::string path = write_file("dv_multi.puffin", file);

    IcebergDeletionVectorReader reader(
            make_options(path, static_cast<int64_t>(blob0.size()), static_cast<int64_t>(blob1.size()), 3));
    auto skip_ctx = std::make_shared<SkipRowsContext>();
    ASSERT_OK(reader.fill_row_indexes(skip_ctx));
    EXPECT_EQ(3, skip_ctx->deletion_bitmap->get_cardinality());
}

// A corrupt blob surfaces as Corruption, with the puffin location appended to the message.
TEST_F(IcebergDeletionVectorReaderFillTest, FillRowIndexesCorruptBlob) {
    auto blob = make_blob({1, 2}, /*corrupt_magic=*/true);
    std::string path = write_file("dv_bad.puffin", blob);

    IcebergDeletionVectorReader reader(make_options(path, 0, static_cast<int64_t>(blob.size()), 2));
    auto skip_ctx = std::make_shared<SkipRowsContext>();
    auto st = reader.fill_row_indexes(skip_ctx);
    EXPECT_TRUE(st.is_corruption());
    EXPECT_NE(std::string::npos, st.message().find("dv_bad.puffin"));
    EXPECT_EQ(nullptr, skip_ctx->deletion_bitmap);
}

// A missing puffin file makes the range read fail before parsing.
TEST_F(IcebergDeletionVectorReaderFillTest, FillRowIndexesReadError) {
    IcebergDeletionVectorReader reader(make_options(_test_dir + "/does_not_exist.puffin", 0, 32, 1));
    auto skip_ctx = std::make_shared<SkipRowsContext>();
    auto st = reader.fill_row_indexes(skip_ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(nullptr, skip_ctx->deletion_bitmap);
}

} // namespace starrocks::formats
