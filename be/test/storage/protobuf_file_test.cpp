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

#include "storage/protobuf_file.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "base/testutil/assert.h"
#include "base/testutil/sync_point.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "common/status.h"
#include "common/storage_define.h"
#include "fs/fs.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/strings/util.h"

namespace starrocks {

template <class T>
class ProtobufFileTest : public ::testing::Test {
public:
    using ProtobufFileType = T;

    static std::string gen_test_file_name() { return generate_uuid_string() + ".bin"; }
};

using MyTypes = ::testing::Types<ProtobufFile, ProtobufFileWithHeader>;
TYPED_TEST_SUITE(ProtobufFileTest, MyTypes);

TYPED_TEST(ProtobufFileTest, test_save_load_tablet_meta) {
    const std::string kFileName = TestFixture::gen_test_file_name();
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    TabletMetaPB tablet_meta;
    tablet_meta.set_table_id(10001);
    tablet_meta.set_tablet_id(10002);
    tablet_meta.set_creation_time(87654);
    tablet_meta.set_partition_id(10);
    tablet_meta.set_schema_hash(54321);
    tablet_meta.set_shard_id(0);

    {
        typename TestFixture::ProtobufFileType file(kFileName);
        Status st = file.save(tablet_meta, true);
        ASSERT_TRUE(st.ok()) << st;
    }

    {
        typename TestFixture::ProtobufFileType file(kFileName);
        TabletMetaPB tablet_meta_2;
        auto st = file.load(&tablet_meta_2);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(tablet_meta.table_id(), tablet_meta_2.table_id());
        ASSERT_EQ(tablet_meta.tablet_id(), tablet_meta_2.tablet_id());
        ASSERT_EQ(tablet_meta.creation_time(), tablet_meta_2.creation_time());
        ASSERT_EQ(tablet_meta.partition_id(), tablet_meta_2.partition_id());
        ASSERT_EQ(tablet_meta.schema_hash(), tablet_meta_2.schema_hash());
        ASSERT_EQ(tablet_meta.shard_id(), tablet_meta_2.shard_id());
    }
}

TYPED_TEST(ProtobufFileTest, test_serialize_failed) {
    const std::string kFileName = TestFixture::gen_test_file_name();
    typename TestFixture::ProtobufFileType file(kFileName);
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    TabletMetaPB tablet_meta;
    tablet_meta.set_table_id(10001);
    tablet_meta.set_tablet_id(10002);
    tablet_meta.set_creation_time(87654);
    tablet_meta.set_partition_id(10);
    tablet_meta.set_schema_hash(54321);
    tablet_meta.set_shard_id(0);

    if constexpr (std::is_same_v<typename TestFixture::ProtobufFileType, ProtobufFile>) {
        SyncPoint::GetInstance()->SetCallBack("ProtobufFile::save:serialize", [](void* arg) { *(bool*)arg = false; });
    }
    if constexpr (std::is_same_v<typename TestFixture::ProtobufFileType, ProtobufFileWithHeader>) {
        SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::save:serialize",
                                              [](void* arg) { *(bool*)arg = false; });
    }

    SyncPoint::GetInstance()->EnableProcessing();

    Status st = file.save(tablet_meta, true);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(MatchPattern(std::string(st.message()),
                             "*failed to serialize protobuf to string, maybe the protobuf is too large*"))
            << st.message();

    if constexpr (std::is_same_v<typename TestFixture::ProtobufFileType, ProtobufFile>) {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFile::save:serialize");
    }
    if constexpr (std::is_same_v<typename TestFixture::ProtobufFileType, ProtobufFileWithHeader>) {
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::save:serialize");
    }
    SyncPoint::GetInstance()->DisableProcessing();
}

TYPED_TEST(ProtobufFileTest, test_corrupted_file0) {
    const std::string kFileName = TestFixture::gen_test_file_name();
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    TabletMetaPB tablet_meta;
    tablet_meta.set_table_id(10001);
    tablet_meta.set_tablet_id(10002);
    tablet_meta.set_creation_time(87654);
    tablet_meta.set_partition_id(10);
    tablet_meta.set_schema_hash(54321);
    tablet_meta.set_shard_id(0);

    {
        typename TestFixture::ProtobufFileType file(kFileName);
        auto st = file.save(tablet_meta, true);
        ASSERT_TRUE(st.ok()) << st;
    }

    {
        std::unique_ptr<WritableFile> f;
        WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN};
        f = *FileSystem::Default()->new_writable_file(opts, kFileName);
        EXPECT_TRUE(f->append("xx").ok());
        EXPECT_TRUE(f->close().ok());
    }

    {
        typename TestFixture::ProtobufFileType file(kFileName);
        TabletMetaPB tablet_meta_2;
        auto st = file.load(&tablet_meta_2);
        ASSERT_FALSE(st.ok());
    }
}

// ---------------------------------------------------------------------------
// Tests for the shared-data (lake) checksummed format: a separate magic number
// plus legacy-headerless fallback on read.
// ---------------------------------------------------------------------------

namespace {

std::string lake_test_file_name() {
    return generate_uuid_string() + ".bin";
}

TabletMetaPB make_sample_meta() {
    TabletMetaPB meta;
    meta.set_table_id(10001);
    meta.set_tablet_id(10002);
    meta.set_creation_time(87654);
    meta.set_partition_id(10);
    meta.set_schema_hash(54321);
    meta.set_shard_id(0);
    return meta;
}

void expect_meta_eq(const TabletMetaPB& a, const TabletMetaPB& b) {
    ASSERT_EQ(a.table_id(), b.table_id());
    ASSERT_EQ(a.tablet_id(), b.tablet_id());
    ASSERT_EQ(a.creation_time(), b.creation_time());
    ASSERT_EQ(a.partition_id(), b.partition_id());
    ASSERT_EQ(a.schema_hash(), b.schema_hash());
    ASSERT_EQ(a.shard_id(), b.shard_id());
}

std::string read_whole_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

void write_whole_file(const std::string& path, const std::string& content) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(content.data(), content.size());
}

} // namespace

// New format round-trips, and the first on-disk byte is the distinctive 0xFE that legacy
// protobuf can never produce as a leading byte.
TEST(LakeProtobufFileTest, new_format_roundtrip) {
    const std::string kFileName = lake_test_file_name();
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    auto meta = make_sample_meta();
    {
        ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
        ASSERT_OK(file.save(meta, true));
    }

    auto content = read_whole_file(kFileName);
    ASSERT_FALSE(content.empty());
    ASSERT_EQ(static_cast<uint8_t>(content[0]), 0xFE);

    {
        ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
        TabletMetaPB out;
        ASSERT_OK(file.load(&out));
        expect_meta_eq(meta, out);
    }
}

// A new reader (lake magic + fallback) must be able to read a legacy headerless protobuf file.
TEST(LakeProtobufFileTest, read_legacy_plain_protobuf) {
    const std::string kFileName = lake_test_file_name();
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    auto meta = make_sample_meta();
    {
        ProtobufFile legacy(kFileName);
        ASSERT_OK(legacy.save(meta, true));
    }

    ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
    TabletMetaPB out;
    ASSERT_OK(file.load(&out));
    expect_meta_eq(meta, out);
}

// Legacy files smaller than the header must still fall back instead of erroring on the size check.
TEST(LakeProtobufFileTest, read_legacy_small_protobuf) {
    const std::string kFileName = lake_test_file_name();
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    TabletMetaPB tiny;
    tiny.set_tablet_id(7);
    std::string serialized;
    ASSERT_TRUE(tiny.SerializeToString(&serialized));
    ASSERT_LT(serialized.size(), 40u); // smaller than sizeof(FixedFileHeader)
    write_whole_file(kFileName, serialized);

    ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
    TabletMetaPB out;
    ASSERT_OK(file.load(&out));
    ASSERT_EQ(7, out.tablet_id());
}

// Corrupting the protobuf body must be detected via the checksum even with fallback enabled,
// because the magic still matches so we take the checksummed path.
TEST(LakeProtobufFileTest, checksum_mismatch_detected) {
    const std::string kFileName = lake_test_file_name();
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    auto meta = make_sample_meta();
    {
        ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
        ASSERT_OK(file.save(meta, true));
    }

    auto content = read_whole_file(kFileName);
    content.back() ^= 0xFF; // flip a byte in the protobuf body
    write_whole_file(kFileName, content);

    ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
    TabletMetaPB out;
    auto st = file.load(&out);
    ASSERT_TRUE(st.is_corruption()) << st;
}

// Without fallback (the shared-nothing default magic), a legacy/foreign file is still rejected.
TEST(LakeProtobufFileTest, no_fallback_rejects_unrecognized_magic) {
    const std::string kFileName = lake_test_file_name();
    DeferOp defer([&]() { std::filesystem::remove(kFileName); });

    auto meta = make_sample_meta();
    {
        ProtobufFile legacy(kFileName);
        ASSERT_OK(legacy.save(meta, true));
    }

    // Lake magic, fallback disabled: legacy file has no header -> corruption.
    ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/false);
    TabletMetaPB out;
    ASSERT_TRUE(file.load(&out).is_corruption());
}

// Buffer-based static loader honors magic + fallback the same way.
TEST(LakeProtobufFileTest, static_buffer_load_fallback) {
    auto meta = make_sample_meta();

    std::string plain;
    ASSERT_TRUE(meta.SerializeToString(&plain));
    {
        TabletMetaPB out;
        ASSERT_OK(ProtobufFileWithHeader::load_from_buffer(&out, plain, LAKE_META_HEADER_MAGIC_NUMBER,
                                                           /*allow_plain_protobuf_fallback=*/true));
        expect_meta_eq(meta, out);
    }
    {
        // Same plain buffer, but fallback disabled -> corruption.
        TabletMetaPB out;
        ASSERT_TRUE(ProtobufFileWithHeader::load_from_buffer(&out, plain, LAKE_META_HEADER_MAGIC_NUMBER,
                                                             /*allow_plain_protobuf_fallback=*/false)
                            .is_corruption());
    }
}

// A file truncated to 0 bytes must not be accepted as a valid (empty) legacy protobuf via the
// fallback path: an empty buffer otherwise parses as an all-default message, masking truncation.
TEST(LakeProtobufFileTest, empty_file_rejected_in_fallback) {
    // Static buffer loader: empty data with fallback -> corruption, not a default message.
    {
        TabletMetaPB out;
        ASSERT_TRUE(ProtobufFileWithHeader::load_from_buffer(&out, std::string_view{}, LAKE_META_HEADER_MAGIC_NUMBER,
                                                             /*allow_plain_protobuf_fallback=*/true)
                            .is_corruption());
    }
    // File-based loader over a 0-byte file with fallback enabled -> corruption.
    {
        const std::string kFileName = lake_test_file_name();
        DeferOp defer([&]() { std::filesystem::remove(kFileName); });
        write_whole_file(kFileName, "");
        ProtobufFileWithHeader file(kFileName, LAKE_META_HEADER_MAGIC_NUMBER, /*allow_plain_protobuf_fallback=*/true);
        TabletMetaPB out;
        ASSERT_TRUE(file.load(&out).is_corruption());
    }
}

} // namespace starrocks
