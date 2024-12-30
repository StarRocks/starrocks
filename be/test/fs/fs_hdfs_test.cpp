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

#include "fs/hdfs/fs_hdfs.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "fs/fs_util.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"

namespace starrocks {

class HdfsFileSystemTest : public ::testing::Test {
public:
    HdfsFileSystemTest() = default;
    ~HdfsFileSystemTest() override = default;

    void SetUp() override {
        const testing::TestInfo* const test_info = testing::UnitTest::GetInstance()->current_test_info();
        std::string root = std::string("hdfs_filesystem_test_") + test_info->name();
        std::filesystem::path rootpath(root);
        _root_path = std::filesystem::absolute(rootpath).string();
        ASSERT_TRUE(fs::create_directories(_root_path).ok());
    }
    void TearDown() override { ASSERT_TRUE(fs::remove_all(_root_path).ok()); }

    void create_file_and_destroy();

public:
    std::string _root_path;
};

void HdfsFileSystemTest::create_file_and_destroy() {
    auto fs = new_fs_hdfs(FSOptions());
    // use file:// as the fs scheme, which will leverage Hadoop LocalFileSystem for testing
    std::string filepath = "file://" + _root_path + "/create_file_and_destroy_file";
    auto st = fs->path_exists(filepath);
    EXPECT_TRUE(st.is_not_found());
    auto wfile = fs->new_writable_file(filepath);
    EXPECT_TRUE(wfile.ok());
    (*wfile)->close();
    EXPECT_TRUE(fs->new_sequential_file(filepath).ok());
    EXPECT_TRUE(fs->new_random_access_file(filepath).ok());

    // error injection during HDFSWritableFile::close, it is hard to simulate a sync failure with a POSIX filesystem.
    // HDFS FileSystem sync operation will fail if the file is removed from remote name node.
    SyncPoint::GetInstance()->SetCallBack("HDFSWritableFile::close", [](void* arg) { *(int*)arg = -1; });
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("HDFSWritableFile::close");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    // done the file, check if there is any memory leak
    (*wfile).reset();
}

TEST_F(HdfsFileSystemTest, create_file_and_destroy) {
    // NOTE: use separate thread to run the test case to avoid some weird tls memory issue introduced by JVM
    auto thread = std::thread([this] { create_file_and_destroy(); });
    thread.join();
}

TEST_F(HdfsFileSystemTest, create_file_with_open_truncate) {
    auto fs = new_fs_hdfs(FSOptions());
    const std::string filepath = "file://" + _root_path + "/create_file_with_open_truncate";
    auto st = fs->path_exists(filepath);
    EXPECT_TRUE(st.is_not_found());
    std::string str1 = "123";
    std::string str2 = "456";
    Slice data1(str1);
    Slice data2(str2);

    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};

    auto wfile_1 = fs->new_writable_file(opts, filepath);
    EXPECT_TRUE(wfile_1.ok());
    EXPECT_TRUE((*wfile_1)->append(data1).ok());
    EXPECT_TRUE((*wfile_1)->flush(WritableFile::FlushMode::FLUSH_SYNC).ok());
    EXPECT_TRUE((*wfile_1)->sync().ok());
    (*wfile_1)->close();

    st = fs->path_exists(filepath);
    EXPECT_TRUE(st.ok());

    auto open_res1 = fs->new_random_access_file(filepath);
    EXPECT_TRUE(open_res1.ok());
    auto rfile1 = std::move(open_res1.value());
    auto read_res_1 = rfile1->read_all();
    EXPECT_TRUE(read_res_1.ok());
    EXPECT_TRUE(read_res_1.value() == str1);

    auto wfile_2 = fs->new_writable_file(opts, filepath);
    EXPECT_TRUE(wfile_2.ok());
    EXPECT_TRUE((*wfile_2)->append(data2).ok());
    EXPECT_TRUE((*wfile_2)->flush(WritableFile::FlushMode::FLUSH_SYNC).ok());
    EXPECT_TRUE((*wfile_2)->sync().ok());
    (*wfile_2)->close();

    st = fs->path_exists(filepath);
    EXPECT_TRUE(st.ok());

    auto open_res2 = fs->new_random_access_file(filepath);
    EXPECT_TRUE(open_res2.ok());
    auto rfile2 = std::move(open_res2.value());
    auto read_res_2 = rfile2->read_all();
    EXPECT_TRUE(read_res_2.ok());
    EXPECT_TRUE(read_res_2.value() == str2);

    (*wfile_1).reset();
    (*wfile_2).reset();
}

} // namespace starrocks
