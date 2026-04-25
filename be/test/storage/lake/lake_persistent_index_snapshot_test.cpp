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
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>

#include "base/utility/defer_op.h"
#include "common/config.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"

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

TEST(LakePersistentIndexSnapshotRoot, derives_with_trailing_slash) {
    const std::string saved_local_dir = config::pk_index_snapshot_local_dir;
    config::pk_index_snapshot_local_dir = "/tmp/derives_with_trailing_slash";
    std::string root;
    Status st = get_lake_persistent_index_snapshot_root(&root);
    config::pk_index_snapshot_local_dir = saved_local_dir;
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(root, "/tmp/derives_with_trailing_slash/lake_pk_snapshot/");
}

class LakePersistentIndexSnapshotGcTest : public ::testing::Test {
protected:
    void SetUp() override {
        _root_dir = make_temp_path("gc_root") + "/";
        _saved_local_dir = config::pk_index_snapshot_local_dir;
        // Strip trailing slash for the config; the helper appends lake_pk_snapshot/ itself.
        std::string parent = _root_dir;
        while (!parent.empty() && parent.back() == '/') parent.pop_back();
        config::pk_index_snapshot_local_dir = parent;
        ASSERT_TRUE(get_lake_persistent_index_snapshot_root(&_snapshot_root).ok());
        ASSERT_TRUE(fs::create_directories(_snapshot_root).ok());
    }
    void TearDown() override {
        config::pk_index_snapshot_local_dir = _saved_local_dir;
        // Best-effort recursive cleanup of the test root.
        std::string parent = _root_dir;
        while (!parent.empty() && parent.back() == '/') parent.pop_back();
        (void)fs::remove_all(parent);
    }

    // Write a snapshot file at <snapshot_root>/<tablet>/v<version>.snapshot, then back-date
    // its mtime by `age_sec` seconds. Returns the full path.
    std::string write_snapshot_with_age(int64_t tablet_id, int64_t version, int64_t age_sec) {
        const std::string tablet_dir = _snapshot_root + std::to_string(tablet_id);
        EXPECT_TRUE(fs::create_directories(tablet_dir).ok());
        const std::string path = tablet_dir + "/v" + std::to_string(version) + ".snapshot";
        // Empty file is fine — GC only looks at mtime + name.
        std::ofstream f(path);
        f.put('x');
        f.close();
        if (age_sec > 0) {
            struct stat st_buf;
            EXPECT_EQ(0, ::stat(path.c_str(), &st_buf));
            struct utimbuf utb;
            utb.actime = st_buf.st_atime;
            utb.modtime = st_buf.st_mtime - age_sec;
            EXPECT_EQ(0, ::utime(path.c_str(), &utb));
        }
        return path;
    }

    std::string _root_dir;
    std::string _snapshot_root;
    std::string _saved_local_dir;
};

TEST_F(LakePersistentIndexSnapshotGcTest, removes_only_stale_files) {
    auto stale1 = write_snapshot_with_age(1001, 5, /*age_sec=*/3700);
    auto stale2 = write_snapshot_with_age(1002, 7, /*age_sec=*/86400);
    auto fresh1 = write_snapshot_with_age(1001, 6, /*age_sec=*/30);
    auto fresh2 = write_snapshot_with_age(1003, 1, /*age_sec=*/0);

    int64_t removed = 0;
    Status st = gc_stale_lake_persistent_index_snapshots(_snapshot_root, /*max_age_sec=*/3600, &removed);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(2, removed);
    EXPECT_FALSE(fs::path_exist(stale1));
    EXPECT_FALSE(fs::path_exist(stale2));
    EXPECT_TRUE(fs::path_exist(fresh1));
    EXPECT_TRUE(fs::path_exist(fresh2));
}

TEST_F(LakePersistentIndexSnapshotGcTest, max_age_zero_is_no_op) {
    auto very_stale = write_snapshot_with_age(2001, 1, /*age_sec=*/99999999);
    int64_t removed = -1;
    Status st = gc_stale_lake_persistent_index_snapshots(_snapshot_root, /*max_age_sec=*/0, &removed);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, removed);
    EXPECT_TRUE(fs::path_exist(very_stale));
}

TEST_F(LakePersistentIndexSnapshotGcTest, missing_root_is_ok) {
    // GC against a directory that was never created — should return OK without error.
    int64_t removed = -1;
    Status st = gc_stale_lake_persistent_index_snapshots(_snapshot_root + "definitely_missing_subdir/",
                                                         /*max_age_sec=*/3600, &removed);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(0, removed);
}

TEST_F(LakePersistentIndexSnapshotGcTest, non_snapshot_files_ignored) {
    auto stale = write_snapshot_with_age(3001, 1, /*age_sec=*/86400);
    // Drop a stray file in the tablet dir that does NOT end in .snapshot — must not be deleted.
    const std::string tablet_dir = _snapshot_root + "3001";
    const std::string stray = tablet_dir + "/scratch.txt";
    {
        std::ofstream f(stray);
        f << "leftover";
    }
    struct stat st_buf;
    ASSERT_EQ(0, ::stat(stray.c_str(), &st_buf));
    struct utimbuf utb;
    utb.actime = st_buf.st_atime;
    utb.modtime = st_buf.st_mtime - 86400;
    ASSERT_EQ(0, ::utime(stray.c_str(), &utb));

    int64_t removed = 0;
    Status st = gc_stale_lake_persistent_index_snapshots(_snapshot_root, /*max_age_sec=*/3600, &removed);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(1, removed);
    EXPECT_FALSE(fs::path_exist(stale));
    EXPECT_TRUE(fs::path_exist(stray));
}

// Stub-snapshot helper: validates that `write_stub_lake_persistent_index_snapshot`
// produces a file readable by `read_lake_persistent_index_snapshot`, with empty
// `memtable_entries`, populated `filesets`, and consistent (tablet_id,
// captured_version, schema_id) fields. Used by the shutdown evicted-tablet
// capture path; correctness on restore depends on the read path being able to
// round-trip what we wrote.
TEST(LakePersistentIndexSnapshotStubTest, write_then_read_roundtrip) {
    const auto previous_dir = config::pk_index_snapshot_local_dir;
    auto root = make_temp_path("stub_root");
    config::pk_index_snapshot_local_dir = root;
    DeferOp restore_dir([&] { config::pk_index_snapshot_local_dir = previous_dir; });

    constexpr int64_t kStubTabletId = 4242;
    constexpr int64_t kStubVersion = 17;
    constexpr int64_t kStubSchemaId = 77;

    TabletMetadataPB metadata;
    metadata.set_id(kStubTabletId);
    metadata.set_version(kStubVersion);
    metadata.mutable_schema()->set_id(kStubSchemaId);
    metadata.set_enable_persistent_index(true);
    metadata.set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    auto* sst1 = metadata.mutable_sstable_meta()->add_sstables();
    sst1->set_filename("sst-aaa.sst");
    sst1->set_max_rss_rowid(0x100000001ULL);
    auto* sst2 = metadata.mutable_sstable_meta()->add_sstables();
    sst2->set_filename("sst-bbb.sst");
    sst2->set_max_rss_rowid(0x200000002ULL);

    std::string out_path;
    Status write_st = write_stub_lake_persistent_index_snapshot(kStubTabletId, metadata, &out_path);
    ASSERT_TRUE(write_st.ok()) << write_st.to_string();
    ASSERT_FALSE(out_path.empty());
    EXPECT_TRUE(fs::path_exist(out_path));

    LakePersistentIndexSnapshotMetaPB read_back;
    Status read_st = read_lake_persistent_index_snapshot(out_path, &read_back);
    ASSERT_TRUE(read_st.ok()) << read_st.to_string();
    EXPECT_EQ(kStubTabletId, read_back.tablet_id());
    EXPECT_EQ(kStubVersion, read_back.captured_version());
    EXPECT_EQ(kStubSchemaId, read_back.schema_id());
    EXPECT_EQ(0, read_back.memtable_entries_size());
    ASSERT_EQ(2, read_back.filesets_size());
    EXPECT_EQ("sst-aaa.sst", read_back.filesets(0).sst_filenames(0));
    EXPECT_EQ("sst-bbb.sst", read_back.filesets(1).sst_filenames(0));

    // The stub must be accepted by the read-side validity rule when version /
    // schema match — otherwise it would never produce a restore HIT in
    // production. Pure helper, so we can call it directly here.
    int64_t now_sec = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
    Status ok_st = validate_lake_persistent_index_snapshot(read_back, kStubTabletId, kStubVersion, kStubSchemaId,
                                                           now_sec, /*max_age_sec=*/3600);
    EXPECT_TRUE(ok_st.ok()) << ok_st.to_string();

    // A version-mismatched lookup (e.g. workload advanced after eviction) must
    // be rejected so the caller falls through to a cold rebuild.
    Status mismatch_st = validate_lake_persistent_index_snapshot(read_back, kStubTabletId, kStubVersion + 1,
                                                                 kStubSchemaId, now_sec, /*max_age_sec=*/3600);
    EXPECT_TRUE(mismatch_st.is_not_found()) << mismatch_st.to_string();
}

} // namespace starrocks::lake
