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

#include "storage/snapshot_meta.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "fs/fs.h"
#include "util/defer_op.h"

namespace starrocks {

class SnapshotMetaTest : public ::testing::Test {
public:
    SnapshotMetaTest() {
        _snapshot_meta.set_snapshot_type(SNAPSHOT_TYPE_FULL);
        _snapshot_meta.set_snapshot_format(4);
        _snapshot_meta.set_snapshot_version(10);

        _snapshot_meta.tablet_meta().set_tablet_id(12345);
        _snapshot_meta.tablet_meta().set_schema_hash(45678);
        _snapshot_meta.tablet_meta().set_tablet_state(PB_RUNNING);
        _snapshot_meta.tablet_meta().set_tablet_type(TABLET_TYPE_DISK);
        _snapshot_meta.tablet_meta().set_creation_time(8765);
        _snapshot_meta.tablet_meta().set_shard_id(123);

        _snapshot_meta.tablet_meta().mutable_updates()->set_next_log_id(0);
        _snapshot_meta.tablet_meta().mutable_updates()->set_next_rowset_id(4);

        EditVersionMetaPB* v = _snapshot_meta.tablet_meta().mutable_updates()->add_versions();
        v->set_creation_time(time(nullptr));
        v->add_rowsets(1);
        v->add_rowsets(2);
        v->add_rowsets(5);

        _snapshot_meta.rowset_metas().resize(3);

        _snapshot_meta.rowset_metas()[0].set_creation_time(time(nullptr));
        _snapshot_meta.rowset_metas()[0].set_tablet_id(_snapshot_meta.tablet_meta().tablet_id());
        _snapshot_meta.rowset_metas()[0].set_deprecated_rowset_id(0);
        _snapshot_meta.rowset_metas()[0].set_rowset_seg_id(1);
        _snapshot_meta.rowset_metas()[0].set_empty(false);
        _snapshot_meta.rowset_metas()[0].set_data_disk_size(1024);
        _snapshot_meta.rowset_metas()[0].set_num_segments(1);
        _snapshot_meta.rowset_metas()[0].set_num_rows(10);
        _snapshot_meta.rowset_metas()[0].set_start_version(0);
        _snapshot_meta.rowset_metas()[0].set_end_version(5);
        _snapshot_meta.rowset_metas()[0].set_num_delete_files(0);
        _snapshot_meta.rowset_metas()[0].set_partition_id(1);
        _snapshot_meta.rowset_metas()[0].set_rowset_state(VISIBLE);

        _snapshot_meta.rowset_metas()[1].set_creation_time(time(nullptr));
        _snapshot_meta.rowset_metas()[1].set_tablet_id(_snapshot_meta.tablet_meta().tablet_id());
        _snapshot_meta.rowset_metas()[1].set_deprecated_rowset_id(0);
        _snapshot_meta.rowset_metas()[1].set_rowset_seg_id(2);
        _snapshot_meta.rowset_metas()[1].set_empty(false);
        _snapshot_meta.rowset_metas()[1].set_data_disk_size(2048);
        _snapshot_meta.rowset_metas()[1].set_num_segments(3);
        _snapshot_meta.rowset_metas()[1].set_num_rows(30);
        _snapshot_meta.rowset_metas()[1].set_start_version(5);
        _snapshot_meta.rowset_metas()[1].set_end_version(8);
        _snapshot_meta.rowset_metas()[1].set_num_delete_files(0);
        _snapshot_meta.rowset_metas()[1].set_partition_id(1);
        _snapshot_meta.rowset_metas()[1].set_rowset_state(VISIBLE);

        _snapshot_meta.rowset_metas()[2].set_creation_time(time(nullptr));
        _snapshot_meta.rowset_metas()[2].set_tablet_id(_snapshot_meta.tablet_meta().tablet_id());
        _snapshot_meta.rowset_metas()[2].set_deprecated_rowset_id(0);
        _snapshot_meta.rowset_metas()[2].set_rowset_seg_id(5);
        _snapshot_meta.rowset_metas()[2].set_empty(false);
        _snapshot_meta.rowset_metas()[2].set_data_disk_size(2048);
        _snapshot_meta.rowset_metas()[2].set_num_segments(3);
        _snapshot_meta.rowset_metas()[2].set_num_rows(30);
        _snapshot_meta.rowset_metas()[2].set_start_version(8);
        _snapshot_meta.rowset_metas()[2].set_end_version(10);
        _snapshot_meta.rowset_metas()[2].set_num_delete_files(0);
        _snapshot_meta.rowset_metas()[2].set_partition_id(1);
        _snapshot_meta.rowset_metas()[2].set_rowset_state(VISIBLE);

        DelVector empty_del_vec;

        auto& del_vec = _snapshot_meta.delete_vectors();
        for (uint32_t seg_id = 1; seg_id <= 7; seg_id++) {
            del_vec.emplace(seg_id, DelVector());
        }
    }

protected:
    SnapshotMeta _snapshot_meta;
};

// NOLINTNEXTLINE
TEST_F(SnapshotMetaTest, test_serialize_and_parse) {
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    auto wf = *FileSystem::Default()->new_writable_file(opts, "test_serialize_and_parse.meta");
    ASSERT_TRUE(_snapshot_meta.serialize_to_file(wf.get()).ok());
    wf->close();
    DeferOp defer([&]() { std::filesystem::remove("test_serialize_and_parse.meta"); });

    auto rf = *FileSystem::Default()->new_random_access_file("test_serialize_and_parse.meta");
    SnapshotMeta meta;
    auto st = meta.parse_from_file(rf.get());
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(_snapshot_meta.snapshot_version(), meta.snapshot_version());
    ASSERT_EQ(_snapshot_meta.snapshot_type(), meta.snapshot_type());
    ASSERT_EQ(_snapshot_meta.snapshot_format(), meta.snapshot_format());
    ASSERT_EQ(_snapshot_meta.tablet_meta().tablet_id(), meta.tablet_meta().tablet_id());
    ASSERT_EQ(_snapshot_meta.tablet_meta().schema_hash(), meta.tablet_meta().schema_hash());
    ASSERT_EQ(_snapshot_meta.tablet_meta().shard_id(), meta.tablet_meta().shard_id());
    ASSERT_EQ(_snapshot_meta.tablet_meta().tablet_state(), meta.tablet_meta().tablet_state());
    ASSERT_EQ(_snapshot_meta.tablet_meta().updates().versions_size(), meta.tablet_meta().updates().versions_size());
    ASSERT_EQ(_snapshot_meta.tablet_meta().updates().next_log_id(), meta.tablet_meta().updates().next_log_id());
    ASSERT_EQ(_snapshot_meta.tablet_meta().updates().next_rowset_id(), meta.tablet_meta().updates().next_rowset_id());
    ASSERT_EQ(_snapshot_meta.rowset_metas().size(), meta.rowset_metas().size());
    ASSERT_EQ(_snapshot_meta.delete_vectors().size(), meta.delete_vectors().size());
    ASSERT_EQ(_snapshot_meta.rowset_metas()[0].rowset_seg_id(), meta.rowset_metas()[0].rowset_seg_id());
    ASSERT_EQ(_snapshot_meta.rowset_metas()[1].rowset_seg_id(), meta.rowset_metas()[1].rowset_seg_id());
    ASSERT_EQ(_snapshot_meta.rowset_metas()[2].rowset_seg_id(), meta.rowset_metas()[2].rowset_seg_id());
}

} // namespace starrocks
