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

#include "formats/puffin/puffin_writer.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <cstring>
#include <string>

#include "formats/iceberg/iceberg_deletion_vector_reader.h"
#include "formats/puffin/deletion_vector_blob.h"
#include "fs/fs_memory.h"
#include "gutil/endian.h"

namespace starrocks::formats {

TEST(PuffinWriterTest, WritesHeaderBlobsAndFooter) {
    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/dv.puffin").value();

    PuffinWriter writer(wf.get());
    ASSERT_TRUE(writer.init().ok());

    // Blob A: small positions.
    roaring64_bitmap_t* a = roaring64_bitmap_create();
    for (uint64_t p : {1ULL, 2ULL, 3ULL}) roaring64_bitmap_add(a, p);
    auto blob_a = build_deletion_vector_blob(a);
    roaring::api::roaring64_bitmap_free(a);
    ASSERT_TRUE(blob_a.ok());
    auto meta_a = writer.add_blob(kDeletionVectorBlobType, {kRowPositionFieldId},
                                  {{kDvPropReferencedDataFile, "/data/a.parquet"}, {kDvPropCardinality, "3"}},
                                  reinterpret_cast<const uint8_t*>(blob_a->data()), blob_a->size());
    ASSERT_TRUE(meta_a.ok());
    EXPECT_EQ(4, meta_a->offset); // first blob starts right after the 4-byte header magic

    // Blob B: 64-bit positions.
    roaring64_bitmap_t* b = roaring64_bitmap_create();
    for (uint64_t p : {(1ULL << 33), (1ULL << 33) + 9}) roaring64_bitmap_add(b, p);
    auto blob_b = build_deletion_vector_blob(b);
    roaring::api::roaring64_bitmap_free(b);
    ASSERT_TRUE(blob_b.ok());
    auto meta_b = writer.add_blob(kDeletionVectorBlobType, {kRowPositionFieldId},
                                  {{kDvPropReferencedDataFile, "/data/b.parquet"}, {kDvPropCardinality, "2"}},
                                  reinterpret_cast<const uint8_t*>(blob_b->data()), blob_b->size());
    ASSERT_TRUE(meta_b.ok());
    EXPECT_EQ(meta_a->offset + meta_a->length, meta_b->offset);

    ASSERT_TRUE(writer.finish().ok());
    const uint64_t fsize = wf->size();
    ASSERT_TRUE(wf->close().ok());

    // Read the whole file back.
    auto rf = fs.new_random_access_file("/dv.puffin").value();
    std::string buf;
    buf.resize(fsize);
    ASSERT_TRUE(rf->read_at_fully(0, buf.data(), fsize).ok());
    const auto* p = reinterpret_cast<const uint8_t*>(buf.data());

    // Header + trailing magic are "PFA1".
    EXPECT_EQ(0, memcmp(p, "PFA1", 4));
    EXPECT_EQ(0, memcmp(p + fsize - 4, "PFA1", 4));

    // Each blob parses by its returned (offset, length).
    auto st_a = IcebergDeletionVectorReader::parse_dv_blob(p + meta_a->offset, meta_a->length, 3, nullptr);
    ASSERT_TRUE(st_a.ok()) << st_a.status().message();
    roaring::api::roaring64_bitmap_free(st_a.value());
    auto st_b = IcebergDeletionVectorReader::parse_dv_blob(p + meta_b->offset, meta_b->length, 2, nullptr);
    ASSERT_TRUE(st_b.ok()) << st_b.status().message();
    roaring::api::roaring64_bitmap_free(st_b.value());

    // Footer tail: [JSON][payloadSize 4B LE][flags 4B = 0][magic 4B].
    const uint32_t payload_size = LittleEndian::Load32(p + fsize - 12);
    for (int i = 0; i < 4; i++) EXPECT_EQ(0, p[fsize - 8 + i]); // flags all zero
    const char* json = reinterpret_cast<const char*>(p + fsize - 12 - payload_size);

    rapidjson::Document doc;
    doc.Parse(json, payload_size);
    ASSERT_FALSE(doc.HasParseError());
    ASSERT_TRUE(doc.HasMember("blobs"));
    const auto& blobs = doc["blobs"];
    ASSERT_EQ(2u, blobs.Size());

    // Common metadata on every blob.
    for (int i = 0; i < 2; i++) {
        const auto& blob = blobs[i];
        EXPECT_STREQ("deletion-vector-v1", blob["type"].GetString());
        ASSERT_EQ(1u, blob["fields"].Size());
        EXPECT_EQ(2147483645, blob["fields"][0].GetInt()); // ROW_POSITION field id
        EXPECT_EQ(-1, blob["snapshot-id"].GetInt64());
        EXPECT_EQ(-1, blob["sequence-number"].GetInt64());
    }
    // Per-blob offset/length/properties.
    EXPECT_EQ(meta_a->offset, blobs[0]["offset"].GetInt64());
    EXPECT_EQ(meta_a->length, blobs[0]["length"].GetInt64());
    EXPECT_STREQ("/data/a.parquet", blobs[0]["properties"]["referenced-data-file"].GetString());
    EXPECT_STREQ("3", blobs[0]["properties"]["cardinality"].GetString());
    EXPECT_EQ(meta_b->offset, blobs[1]["offset"].GetInt64());
    EXPECT_EQ(meta_b->length, blobs[1]["length"].GetInt64());
    EXPECT_STREQ("/data/b.parquet", blobs[1]["properties"]["referenced-data-file"].GetString());
    EXPECT_STREQ("2", blobs[1]["properties"]["cardinality"].GetString());
}

TEST(PuffinWriterTest, RejectsLifecycleMisuse) {
    MemoryFileSystem fs;
    auto wf = fs.new_writable_file("/lc.puffin").value();
    PuffinWriter writer(wf.get());

    const uint8_t dummy[1] = {0};
    // Before init: add_blob and finish are rejected.
    EXPECT_FALSE(writer.add_blob("t", {}, {}, dummy, 1).ok());
    EXPECT_FALSE(writer.finish().ok());

    ASSERT_TRUE(writer.init().ok());
    // Double init rejected.
    EXPECT_FALSE(writer.init().ok());

    ASSERT_TRUE(writer.finish().ok());
    // After finish: double finish and add_blob are rejected.
    EXPECT_FALSE(writer.finish().ok());
    EXPECT_FALSE(writer.add_blob("t", {}, {}, dummy, 1).ok());
}

} // namespace starrocks::formats
