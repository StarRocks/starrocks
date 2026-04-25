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

#include "storage/lake/lake_persistent_index_snapshot.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <fstream>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"

namespace starrocks::lake {

namespace {

constexpr int64_t kTestTabletId = 999001;
constexpr int64_t kTestVersion = 42;
constexpr int64_t kTestSchemaId = 7777;

std::atomic<int> g_temp_path_counter{0};

std::string make_temp_path(const std::string& tag) {
    return "/tmp/lake_pk_snapshot_test_" + std::to_string(::getpid()) + "_" +
           std::to_string(g_temp_path_counter.fetch_add(1)) + "_" + tag;
}

LakePersistentIndexSnapshotMetaPB make_test_meta() {
    LakePersistentIndexSnapshotMetaPB meta;
    meta.set_format_version(kSnapshotFormatVersion);
    meta.set_tablet_id(kTestTabletId);
    meta.set_captured_version(kTestVersion);
    meta.set_schema_id(kTestSchemaId);
    meta.set_captured_at_unix_sec(1714000000);
    auto* e1 = meta.add_memtable_entries();
    e1->set_key("alpha-pk");
    e1->set_value(0x0000000100000005LL);
    e1->set_version(40);
    auto* e2 = meta.add_memtable_entries();
    e2->set_key("beta-pk-longer");
    e2->set_value(0x0000000200000013LL);
    e2->set_version(41);
    auto* fs1 = meta.add_filesets();
    fs1->set_fileset_version(kTestVersion);
    fs1->add_sst_filenames("00000000000001.sst");
    fs1->add_sst_filenames("00000000000002.sst");
    fs1->set_max_rss_rowid(0x0000000200000020LL);
    return meta;
}

} // namespace

class LakePersistentIndexSnapshotIoTest : public ::testing::Test {
protected:
    void SetUp() override { _path = make_temp_path("io"); }
    void TearDown() override { (void)std::remove(_path.c_str()); }
    std::string _path;
};

TEST_F(LakePersistentIndexSnapshotIoTest, round_trip) {
    auto meta = make_test_meta();
    ASSERT_TRUE(write_lake_persistent_index_snapshot(_path, meta).ok());

    LakePersistentIndexSnapshotMetaPB roundtripped;
    ASSERT_TRUE(read_lake_persistent_index_snapshot(_path, &roundtripped).ok());
    EXPECT_EQ(meta.format_version(), roundtripped.format_version());
    EXPECT_EQ(meta.tablet_id(), roundtripped.tablet_id());
    EXPECT_EQ(meta.captured_version(), roundtripped.captured_version());
    EXPECT_EQ(meta.schema_id(), roundtripped.schema_id());
    EXPECT_EQ(meta.captured_at_unix_sec(), roundtripped.captured_at_unix_sec());
    ASSERT_EQ(meta.memtable_entries_size(), roundtripped.memtable_entries_size());
    for (int i = 0; i < meta.memtable_entries_size(); ++i) {
        EXPECT_EQ(meta.memtable_entries(i).key(), roundtripped.memtable_entries(i).key());
        EXPECT_EQ(meta.memtable_entries(i).value(), roundtripped.memtable_entries(i).value());
        EXPECT_EQ(meta.memtable_entries(i).version(), roundtripped.memtable_entries(i).version());
    }
    ASSERT_EQ(meta.filesets_size(), roundtripped.filesets_size());
    EXPECT_EQ(meta.filesets(0).sst_filenames_size(), roundtripped.filesets(0).sst_filenames_size());
}

TEST_F(LakePersistentIndexSnapshotIoTest, missing_file_returns_not_found_or_io) {
    LakePersistentIndexSnapshotMetaPB out;
    Status st = read_lake_persistent_index_snapshot(_path + "_does_not_exist", &out);
    EXPECT_FALSE(st.ok());
    // File-system layer maps absent files to NotFound; tolerate either NotFound or IOError so
    // the test does not depend on the underlying FS impl.
    EXPECT_TRUE(st.is_not_found() || st.is_io_error()) << st.to_string();
}

TEST_F(LakePersistentIndexSnapshotIoTest, magic_mismatch_returns_corruption) {
    auto meta = make_test_meta();
    ASSERT_TRUE(write_lake_persistent_index_snapshot(_path, meta).ok());
    // Flip first byte of the magic.
    std::fstream f(_path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.is_open());
    f.seekp(0);
    char bad = 'X';
    f.write(&bad, 1);
    f.close();

    LakePersistentIndexSnapshotMetaPB out;
    Status st = read_lake_persistent_index_snapshot(_path, &out);
    EXPECT_TRUE(st.is_corruption()) << st.to_string();
}

TEST_F(LakePersistentIndexSnapshotIoTest, crc_mismatch_returns_corruption) {
    auto meta = make_test_meta();
    ASSERT_TRUE(write_lake_persistent_index_snapshot(_path, meta).ok());
    // Flip a payload byte (well past the header so magic+format_version+size are intact).
    std::fstream f(_path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.is_open());
    f.seekp(kSnapshotHeaderLen + 4);
    char flipped = 0x55;
    f.write(&flipped, 1);
    f.close();

    LakePersistentIndexSnapshotMetaPB out;
    Status st = read_lake_persistent_index_snapshot(_path, &out);
    EXPECT_TRUE(st.is_corruption()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotValidate, happy_path) {
    auto meta = make_test_meta();
    Status st = validate_lake_persistent_index_snapshot(meta, kTestTabletId, kTestVersion, kTestSchemaId,
                                                        meta.captured_at_unix_sec() + 60, /*max_age_sec=*/86400);
    EXPECT_TRUE(st.ok()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotValidate, tablet_id_mismatch) {
    auto meta = make_test_meta();
    Status st = validate_lake_persistent_index_snapshot(meta, kTestTabletId + 1, kTestVersion, kTestSchemaId,
                                                        meta.captured_at_unix_sec() + 60, 86400);
    EXPECT_TRUE(st.is_not_found()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotValidate, version_mismatch) {
    auto meta = make_test_meta();
    Status st = validate_lake_persistent_index_snapshot(meta, kTestTabletId, kTestVersion + 1, kTestSchemaId,
                                                        meta.captured_at_unix_sec() + 60, 86400);
    EXPECT_TRUE(st.is_not_found()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotValidate, schema_id_mismatch) {
    auto meta = make_test_meta();
    Status st = validate_lake_persistent_index_snapshot(meta, kTestTabletId, kTestVersion, kTestSchemaId + 1,
                                                        meta.captured_at_unix_sec() + 60, 86400);
    EXPECT_TRUE(st.is_not_found()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotValidate, expired) {
    auto meta = make_test_meta();
    Status st = validate_lake_persistent_index_snapshot(meta, kTestTabletId, kTestVersion, kTestSchemaId,
                                                        meta.captured_at_unix_sec() + 86401, 86400);
    EXPECT_TRUE(st.is_not_found()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotValidate, age_check_disabled_when_max_age_zero) {
    auto meta = make_test_meta();
    // very old, but max_age_sec <= 0 disables the age check.
    Status st = validate_lake_persistent_index_snapshot(meta, kTestTabletId, kTestVersion, kTestSchemaId,
                                                        meta.captured_at_unix_sec() + 99999999, /*max_age_sec=*/0);
    EXPECT_TRUE(st.ok()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotValidate, future_format_version_rejected) {
    auto meta = make_test_meta();
    meta.set_format_version(kSnapshotFormatVersion + 1);
    Status st = validate_lake_persistent_index_snapshot(meta, kTestTabletId, kTestVersion, kTestSchemaId,
                                                        meta.captured_at_unix_sec() + 60, 86400);
    EXPECT_TRUE(st.is_not_found()) << st.to_string();
}

TEST(LakePersistentIndexSnapshotPath, uses_local_dir_override_when_set) {
    const std::string saved_local_dir = config::pk_index_snapshot_local_dir;
    config::pk_index_snapshot_local_dir = "/tmp/snapshot_root_override";
    std::string out;
    Status st = get_lake_persistent_index_snapshot_path(123, 7, &out);
    config::pk_index_snapshot_local_dir = saved_local_dir;
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_NE(out.find("/tmp/snapshot_root_override/"), std::string::npos);
    EXPECT_NE(out.find("/123/v7.snapshot"), std::string::npos);
}

TEST(LakePersistentIndexSnapshotPath, falls_back_to_storage_root_path) {
    const std::string saved_local_dir = config::pk_index_snapshot_local_dir;
    const std::string saved_root = config::storage_root_path;
    config::pk_index_snapshot_local_dir = "";
    config::storage_root_path = "/tmp/storage_root_a;/tmp/storage_root_b";
    std::string out;
    Status st = get_lake_persistent_index_snapshot_path(456, 11, &out);
    config::pk_index_snapshot_local_dir = saved_local_dir;
    config::storage_root_path = saved_root;
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_NE(out.find("/tmp/storage_root_a/lake_pk_snapshot/456/v11.snapshot"), std::string::npos);
}

} // namespace starrocks::lake
