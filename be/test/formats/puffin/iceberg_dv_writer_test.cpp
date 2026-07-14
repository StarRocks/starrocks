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

#include "formats/puffin/iceberg_dv_writer.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <cstring>
#include <string>
#include <vector>

#include "formats/iceberg/iceberg_deletion_vector_reader.h"
#include "fs/fs_memory.h"
#include "gutil/endian.h"

namespace starrocks::formats {

static std::string read_all(MemoryFileSystem& fs, const std::string& path, uint64_t size) {
    auto rf = fs.new_random_access_file(path).value();
    std::string buf;
    buf.resize(size);
    EXPECT_TRUE(rf->read_at_fully(0, buf.data(), size).ok());
    return buf;
}

// Parse the DV blob at [entry.content_offset, +content_size_in_bytes) out of the whole
// Puffin file bytes and return its positions; also asserts cardinality == record_count.
static std::vector<uint64_t> positions_of(const std::string& file_bytes, const IcebergDvCommitEntry& entry) {
    const auto* p = reinterpret_cast<const uint8_t*>(file_bytes.data());
    auto st = IcebergDeletionVectorReader::parse_dv_blob(p + entry.content_offset, entry.content_size_in_bytes,
                                                         entry.record_count, nullptr);
    EXPECT_TRUE(st.ok()) << st.status().message();
    if (!st.ok()) return {};
    roaring64_bitmap_t* b = st.value();
    std::vector<uint64_t> out(roaring64_bitmap_get_cardinality(b));
    roaring64_bitmap_to_uint64_array(b, out.data());
    roaring::api::roaring64_bitmap_free(b);
    return out;
}

// Parse the Puffin footer FileMetadata JSON: tail is [JSON][size 4B LE][flags 4B][magic 4B].
static void parse_footer(const std::string& bytes, rapidjson::Document* doc) {
    const auto* p = reinterpret_cast<const uint8_t*>(bytes.data());
    const uint64_t fsize = bytes.size();
    ASSERT_GE(fsize, 12u);
    uint32_t payload_size = LittleEndian::Load32(p + fsize - 12);
    doc->Parse(reinterpret_cast<const char*>(p + fsize - 12 - payload_size), payload_size);
    ASSERT_FALSE(doc->HasParseError());
}

TEST(IcebergDvWriterTest, MultiFileRoundTripAndFooterMetadata) {
    IcebergDvWriter writer;
    writer.add("/data/a.parquet", 1);
    writer.add("/data/a.parquet", 2);
    writer.add("/data/a.parquet", 3);
    writer.add("/data/b.parquet", (1ULL << 33));
    writer.add("/data/b.parquet", (1ULL << 33) + 9);
    EXPECT_FALSE(writer.empty());
    EXPECT_EQ(2u, writer.num_data_files());

    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/dv.puffin").value();
    auto res = writer.finish(wf.get());
    ASSERT_TRUE(res.ok()) << res.status().message();
    const uint64_t fsize = wf->size();
    ASSERT_TRUE(wf->close().ok());

    std::vector<IcebergDvCommitEntry> entries = res.value();
    ASSERT_EQ(2u, entries.size());
    EXPECT_EQ("/data/a.parquet", entries[0].referenced_data_file);
    EXPECT_EQ("/data/b.parquet", entries[1].referenced_data_file);
    EXPECT_EQ(3, entries[0].record_count);
    EXPECT_EQ(2, entries[1].record_count);
    EXPECT_EQ(4, entries[0].content_offset); // right after the 4-byte header magic
    EXPECT_EQ(entries[0].content_offset + entries[0].content_size_in_bytes, entries[1].content_offset);

    std::string bytes = read_all(fs, "/dv.puffin", fsize);
    EXPECT_EQ(0, memcmp(bytes.data(), "PFA1", 4));
    EXPECT_EQ(0, memcmp(bytes.data() + fsize - 4, "PFA1", 4));
    EXPECT_EQ((std::vector<uint64_t>{1, 2, 3}), positions_of(bytes, entries[0]));
    EXPECT_EQ((std::vector<uint64_t>{(1ULL << 33), (1ULL << 33) + 9}), positions_of(bytes, entries[1]));

    // Footer metadata must be exactly what a DV blob requires.
    rapidjson::Document doc;
    parse_footer(bytes, &doc);
    const auto& blobs = doc["blobs"];
    ASSERT_EQ(2u, blobs.Size());
    for (int i = 0; i < 2; i++) {
        const auto& b = blobs[i];
        EXPECT_STREQ("deletion-vector-v1", b["type"].GetString());
        ASSERT_EQ(1u, b["fields"].Size());
        EXPECT_EQ(2147483645, b["fields"][0].GetInt());
        EXPECT_EQ(-1, b["snapshot-id"].GetInt64());
        EXPECT_EQ(-1, b["sequence-number"].GetInt64());
    }
    EXPECT_STREQ("/data/a.parquet", blobs[0]["properties"]["referenced-data-file"].GetString());
    EXPECT_STREQ("3", blobs[0]["properties"]["cardinality"].GetString());
    EXPECT_STREQ("/data/b.parquet", blobs[1]["properties"]["referenced-data-file"].GetString());
    EXPECT_STREQ("2", blobs[1]["properties"]["cardinality"].GetString());
}

TEST(IcebergDvWriterTest, DuplicatePositionsAreDeduped) {
    IcebergDvWriter writer;
    writer.add("/data/a.parquet", 5);
    writer.add("/data/a.parquet", 5);
    writer.add("/data/a.parquet", 7);

    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/dv.puffin").value();
    auto res = writer.finish(wf.get());
    ASSERT_TRUE(res.ok()) << res.status().message();
    const uint64_t fsize = wf->size();
    ASSERT_TRUE(wf->close().ok());

    auto entries = res.value();
    ASSERT_EQ(1u, entries.size());
    EXPECT_EQ(2, entries[0].record_count); // {5,7}
    EXPECT_EQ((std::vector<uint64_t>{5, 7}), positions_of(read_all(fs, "/dv.puffin", fsize), entries[0]));
}

TEST(IcebergDvWriterTest, MergeBitmapUnionsPositions) {
    IcebergDvWriter writer;
    writer.add("/data/a.parquet", 1);
    writer.add("/data/a.parquet", 2);

    // Old DV for /data/a.parquet: {2, 3, 100} -> union into the accumulator.
    roaring64_bitmap_t* old_a = roaring64_bitmap_create();
    for (uint64_t v : {2ULL, 3ULL, 100ULL}) roaring64_bitmap_add(old_a, v);
    writer.merge_bitmap("/data/a.parquet", old_a);
    roaring::api::roaring64_bitmap_free(old_a);

    // merge_bitmap into a brand-new data file (no prior add()).
    roaring64_bitmap_t* old_c = roaring64_bitmap_create();
    roaring64_bitmap_add(old_c, 7);
    writer.merge_bitmap("/data/c.parquet", old_c);
    roaring::api::roaring64_bitmap_free(old_c);

    EXPECT_EQ(2u, writer.num_data_files());

    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/dv.puffin").value();
    auto res = writer.finish(wf.get());
    ASSERT_TRUE(res.ok()) << res.status().message();
    const uint64_t fsize = wf->size();
    ASSERT_TRUE(wf->close().ok());

    auto entries = res.value();
    ASSERT_EQ(2u, entries.size());
    std::string bytes = read_all(fs, "/dv.puffin", fsize);
    EXPECT_EQ("/data/a.parquet", entries[0].referenced_data_file);
    EXPECT_EQ(4, entries[0].record_count); // {1,2,3,100}
    EXPECT_EQ((std::vector<uint64_t>{1, 2, 3, 100}), positions_of(bytes, entries[0]));
    EXPECT_EQ("/data/c.parquet", entries[1].referenced_data_file);
    EXPECT_EQ((std::vector<uint64_t>{7}), positions_of(bytes, entries[1]));
}

TEST(IcebergDvWriterTest, EmptyWriterWritesNothing) {
    IcebergDvWriter writer;
    EXPECT_TRUE(writer.empty());
    EXPECT_EQ(0u, writer.num_data_files());

    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/dv.puffin").value();
    auto res = writer.finish(wf.get());
    ASSERT_TRUE(res.ok()) << res.status().message();
    EXPECT_TRUE(res.value().empty());
    EXPECT_EQ(0u, wf->size()); // nothing written -> no orphan Puffin
    ASSERT_TRUE(wf->close().ok());
}

TEST(IcebergDvWriterTest, RejectsNullFileAndDoubleFinish) {
    IcebergDvWriter writer;
    writer.add("/data/a.parquet", 1);
    EXPECT_FALSE(writer.finish(nullptr).ok()); // non-empty + null file

    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/dv.puffin").value();
    ASSERT_TRUE(writer.finish(wf.get()).ok());
    EXPECT_FALSE(writer.finish(wf.get()).ok()); // double finish
}

TEST(IcebergDvWriterTest, RejectsEmptyReferencedDataFile) {
    IcebergDvWriter writer;
    writer.add("", 1);
    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/dv.puffin").value();
    EXPECT_FALSE(writer.finish(wf.get()).ok());
}

} // namespace starrocks::formats
