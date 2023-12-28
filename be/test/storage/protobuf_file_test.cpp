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

#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/strings/util.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

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

} // namespace starrocks
