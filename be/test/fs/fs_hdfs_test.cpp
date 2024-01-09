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

} // namespace starrocks
