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
#include "util/defer_op.h"

namespace starrocks {

TEST(ProtobufFileTest, test_save_load_tablet_meta) {
    ProtobufFile file("ProtobufFileTest_test_save_load_tablet_meta.bin");
    DeferOp defer([&]() { std::filesystem::remove("ProtobufFileTest_test_save_load_tablet_meta.bin"); });

    TabletMetaPB tablet_meta;
    tablet_meta.set_table_id(10001);
    tablet_meta.set_tablet_id(10002);
    tablet_meta.set_creation_time(87654);
    tablet_meta.set_partition_id(10);
    tablet_meta.set_schema_hash(54321);
    tablet_meta.set_shard_id(0);

    Status st = file.save(tablet_meta, true);
    ASSERT_TRUE(st.ok()) << st;

    TabletMetaPB tablet_meta_2;
    st = file.load(&tablet_meta_2);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(tablet_meta.table_id(), tablet_meta_2.table_id());
    ASSERT_EQ(tablet_meta.tablet_id(), tablet_meta_2.tablet_id());
    ASSERT_EQ(tablet_meta.creation_time(), tablet_meta_2.creation_time());
    ASSERT_EQ(tablet_meta.partition_id(), tablet_meta_2.partition_id());
    ASSERT_EQ(tablet_meta.schema_hash(), tablet_meta_2.schema_hash());
    ASSERT_EQ(tablet_meta.shard_id(), tablet_meta_2.shard_id());
}

TEST(ProtobufFileTest, test_corruption) {
    ProtobufFile file("ProtobufFileTest_test_corruption.bin");
    DeferOp defer([&]() { std::filesystem::remove("ProtobufFileTest_test_corruption.bin"); });

    TabletMetaPB tablet_meta;
    tablet_meta.set_table_id(10001);
    tablet_meta.set_tablet_id(10002);
    tablet_meta.set_creation_time(87654);
    tablet_meta.set_partition_id(10);
    tablet_meta.set_schema_hash(54321);
    tablet_meta.set_shard_id(0);

    Status st = file.save(tablet_meta, true);
    ASSERT_TRUE(st.ok()) << st;

    std::unique_ptr<WritableFile> f;
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN};
    f = *FileSystem::Default()->new_writable_file(opts, "ProtobufFileTest_test_corruption.bin");

    f->append("xx");
    TabletMetaPB tablet_meta_2;
    st = file.load(&tablet_meta_2);
    ASSERT_FALSE(st.ok());
}

} // namespace starrocks
