// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <filesystem>
#include <ranges>

#include "common/system/cpu_info.h"
#include "exec/exec_env.h"
#include "fs/fs.h"

#define private public // hack complier
#define protected public

#include "data_workflows/snapshot/snapshot_loader.h"

#undef protected
#undef private

namespace starrocks {

class SnapshotLoaderTest : public testing::Test {
public:
    SnapshotLoaderTest() = default;

private:
    ExecEnv* _exec_env;
};

TEST_F(SnapshotLoaderTest, NormalCase) {
    SnapshotLoader loader(_exec_env, 1L, 2L);

    ASSERT_TRUE(loader._end_with("abt.dat", ".dat"));
    ASSERT_FALSE(loader._end_with("abt.dat", ".da"));

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    Status st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678", &tablet_id, &schema_hash);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1234, tablet_id);
    ASSERT_EQ(5678, schema_hash);

    st = loader._get_tablet_id_and_schema_hash_from_file_path("/path/to/1234/5678/", &tablet_id, &schema_hash);
    ASSERT_FALSE(st.ok());

    std::filesystem::remove_all("./ss_test/");
    std::map<std::string, std::string> src_to_dest;
    src_to_dest["./ss_test/"] = "./ss_test";
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    ASSERT_FALSE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    ASSERT_FALSE(st.ok());

    std::filesystem::create_directory("./ss_test/");
    st = loader._check_local_snapshot_paths(src_to_dest, true);
    ASSERT_TRUE(st.ok());
    st = loader._check_local_snapshot_paths(src_to_dest, false);
    ASSERT_TRUE(st.ok());
    std::filesystem::remove_all("./ss_test/");

    std::filesystem::create_directory("./ss_test/");
    std::vector<std::string> files;
    st = loader._get_existing_files_from_local("./ss_test/", &files);
    ASSERT_EQ(0, files.size());
    std::filesystem::remove_all("./ss_test/");

    std::string snapshot_file;
    std::string tablet_file;
    loader._assemble_file_name("/snapshot/path", "/tablet/path", 1234, 2, 5, 12345, 1, ".dat", &snapshot_file,
                               &tablet_file);
    ASSERT_EQ("/snapshot/path/1234_2_5_12345_1.dat", snapshot_file);
    ASSERT_EQ("/tablet/path/1234_2_5_12345_1.dat", tablet_file);

    std::string new_name;
    st = loader._replace_tablet_id("12345.hdr", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("5678.hdr", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.dat", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.dat", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.upt", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.upt", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.cols", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.cols", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.idx", 5678, &new_name);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ("1234_2_5_12345_1.idx", new_name);

    st = loader._replace_tablet_id("1234_2_5_12345_1.xxx", 5678, &new_name);
    ASSERT_FALSE(st.ok());

    st = loader._get_tablet_id_from_remote_path("/__tbl_10004/__part_10003/__idx_10004/__10005", &tablet_id);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(10005, tablet_id);
}

TEST_F(SnapshotLoaderTest, SnapshotManifestReadWrite) {
    SnapshotLoader loader(_exec_env, 1L, 2L);
    FileSystem* fs = FileSystem::Default();

    const std::string dir = "./ss_manifest_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);

    // no manifest in the dir yet -> found=false, still ok (legacy snapshot path)
    {
        std::vector<std::string> names;
        bool found = true;
        Status st = loader._read_snapshot_manifest(fs, dir, &names, &found);
        ASSERT_TRUE(st.ok()) << st.message();
        ASSERT_FALSE(found);
        ASSERT_TRUE(names.empty());
    }

    // write a manifest then read it back verbatim, order preserved
    {
        std::vector<std::string> to_write = {"0_0_0.dat", "0.hdr", "0_0_1.dat"};
        Status st = loader._write_snapshot_manifest(fs, dir, to_write);
        ASSERT_TRUE(st.ok()) << st.message();

        std::vector<std::string> names;
        bool found = false;
        st = loader._read_snapshot_manifest(fs, dir, &names, &found);
        ASSERT_TRUE(st.ok()) << st.message();
        ASSERT_TRUE(found);
        ASSERT_EQ(to_write, names);
    }

    // the manifest file name carries no '.', so the remote listing (which keys files by the part
    // before the last '.') never mistakes it for a data file
    ASSERT_EQ(std::string::npos, std::string("__starrocks_snapshot_manifest").find('.'));

    // an empty file list round-trips as a present-but-empty manifest
    {
        Status st = loader._write_snapshot_manifest(fs, dir, {});
        ASSERT_TRUE(st.ok()) << st.message();

        std::vector<std::string> names;
        bool found = false;
        st = loader._read_snapshot_manifest(fs, dir, &names, &found);
        ASSERT_TRUE(st.ok()) << st.message();
        ASSERT_TRUE(found);
        ASSERT_TRUE(names.empty());
    }

    // a manifest whose last line has no trailing newline still parses every name
    {
        const std::string manifest_path = dir + "/__starrocks_snapshot_manifest";
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        auto wf_or = fs->new_writable_file(opts, manifest_path);
        ASSERT_TRUE(wf_or.ok()) << wf_or.status().message();
        auto wf = std::move(wf_or.value());
        ASSERT_TRUE(wf->append(Slice("a.dat\nb.dat")).ok());
        ASSERT_TRUE(wf->close().ok());

        std::vector<std::string> names;
        bool found = false;
        Status st = loader._read_snapshot_manifest(fs, dir, &names, &found);
        ASSERT_TRUE(st.ok()) << st.message();
        ASSERT_TRUE(found);
        ASSERT_EQ((std::vector<std::string>{"a.dat", "b.dat"}), names);
    }

    std::filesystem::remove_all(dir);
}

TEST_F(SnapshotLoaderTest, VerifySnapshotManifest) {
    SnapshotLoader loader(_exec_env, 1L, 2L);
    FileSystem* fs = FileSystem::Default();

    const std::string dir = "./ss_verify_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);

    std::map<std::string, FileStat> remote_files;
    remote_files.emplace("0_0_0.dat", FileStat{"0_0_0.dat", "0123456789abcdef0123456789abcdef", 100});
    remote_files.emplace("0.hdr", FileStat{"0.hdr", "fedcba9876543210fedcba9876543210", 50});

    // no manifest present -> ok (legacy snapshot, nothing to verify)
    {
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files);
        ASSERT_TRUE(st.ok()) << st.message();
    }

    // manifest lists exactly the files present in the remote listing -> ok
    {
        ASSERT_TRUE(loader._write_snapshot_manifest(fs, dir, {"0_0_0.dat", "0.hdr"}).ok());
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files);
        ASSERT_TRUE(st.ok()) << st.message();
    }

    // manifest lists a file missing from the remote listing -> error
    {
        ASSERT_TRUE(loader._write_snapshot_manifest(fs, dir, {"0_0_0.dat", "0.hdr", "0_0_1.dat"}).ok());
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files);
        ASSERT_FALSE(st.ok());
        ASSERT_NE(st.message().find("missing"), std::string::npos);
    }

    // an empty manifest records no files -> nothing to check, ok
    {
        ASSERT_TRUE(loader._write_snapshot_manifest(fs, dir, {}).ok());
        Status st = loader._verify_snapshot_manifest(fs, dir, remote_files);
        ASSERT_TRUE(st.ok()) << st.message();
    }

    std::filesystem::remove_all(dir);
}

} // namespace starrocks
