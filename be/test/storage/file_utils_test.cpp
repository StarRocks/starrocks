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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/olap/file_utils_test.cpp

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

#include <filesystem>
#include <fstream>
#include <set>
#include <vector>

#include "common/configbase.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "storage/olap_define.h"
#include "testutil/assert.h"
#include "util/logging.h"

#ifndef BE_TEST
#define BE_TEST
#endif

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace starrocks {

class FileUtilsTest : public testing::Test {
public:
    void SetUp() override { ASSERT_TRUE(std::filesystem::create_directory(_s_test_data_path)); }

    void TearDown() override { ASSERT_TRUE(std::filesystem::remove_all(_s_test_data_path)); }

    static std::string _s_test_data_path;
};

std::string FileUtilsTest::_s_test_data_path = "./file_utils_testxxxx123";

void save_string_file(const std::filesystem::path& p, const std::string& str) {
    std::ofstream file;
    file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    file.open(p, std::ios_base::binary);
    file.write(str.c_str(), str.size());
};

TEST_F(FileUtilsTest, TestCopyFile) {
    std::string src_file_name = _s_test_data_path + "/abcd12345.txt";
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    std::unique_ptr<WritableFile> src_file = *FileSystem::Default()->new_writable_file(opts, src_file_name);

    char large_bytes2[(1 << 12)];
    memset(large_bytes2, 0, sizeof(char) * ((1 << 12)));
    int i = 0;
    while (i < 1 << 10) {
        ASSERT_TRUE(src_file->append(Slice(large_bytes2, 1 << 12)).ok());
        ++i;
    }
    ASSERT_TRUE(src_file->append(Slice(large_bytes2, 13)).ok());
    ASSERT_TRUE(src_file->close().ok());

    std::string dst_file_name = _s_test_data_path + "/abcd123456.txt";
    fs::copy_file(src_file_name, dst_file_name);

    ASSERT_EQ(4194317, std::filesystem::file_size(dst_file_name));
}

TEST_F(FileUtilsTest, TestCopyFileByRange) {
    std::string src_file_name = _s_test_data_path + "/abcd12345_2.txt";
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    std::unique_ptr<WritableFile> src_file = *FileSystem::Default()->new_writable_file(opts, src_file_name);

    char large_bytes2[(1 << 12)];
    memset(large_bytes2, 0, sizeof(char) * ((1 << 12)));
    int i = 0;
    while (i < 1 << 10) {
        ASSERT_TRUE(src_file->append(Slice(large_bytes2, 1 << 12)).ok());
        ++i;
    }
    ASSERT_TRUE(src_file->append(Slice(large_bytes2, 13)).ok());
    ASSERT_TRUE(src_file->close().ok());

    std::string dst_file_name = _s_test_data_path + "/abcd123456_2.txt";
    fs::copy_file_by_range(src_file_name, dst_file_name, 10, 1024);

    ASSERT_EQ(1024, std::filesystem::file_size(dst_file_name));
}

TEST_F(FileUtilsTest, TestRemove) {
    // remove_all
    ASSERT_OK(fs::remove_all("./file_test"));
    ASSERT_FALSE(fs::path_exist("./file_test"));

    ASSERT_TRUE(fs::create_directories("./file_test/123/456/789").ok());
    ASSERT_TRUE(fs::create_directories("./file_test/abc/def/zxc").ok());
    ASSERT_TRUE(fs::create_directories("./file_test/abc/123").ok());

    save_string_file("./file_test/s1", "123");
    save_string_file("./file_test/123/s2", "123");

    ASSERT_TRUE(fs::path_exist("./file_test"));
    ASSERT_TRUE(fs::remove_all("./file_test").ok());
    ASSERT_FALSE(fs::path_exist("./file_test"));

    // remove
    ASSERT_TRUE(fs::create_directories("./file_test/abc/123").ok());
    save_string_file("./file_test/abc/123/s2", "123");

    ASSERT_FALSE(fs::remove("./file_test").ok());
    ASSERT_FALSE(fs::remove("./file_test/abc/").ok());
    ASSERT_FALSE(fs::remove("./file_test/abc/123").ok());

    ASSERT_TRUE(fs::path_exist("./file_test/abc/123/s2"));
    ASSERT_TRUE(fs::remove("./file_test/abc/123/s2").ok());
    ASSERT_FALSE(fs::path_exist("./file_test/abc/123/s2"));

    ASSERT_TRUE(fs::path_exist("./file_test/abc/123"));
    ASSERT_TRUE(fs::remove("./file_test/abc/123/").ok());
    ASSERT_FALSE(fs::path_exist("./file_test/abc/123"));

    ASSERT_TRUE(fs::remove_all("./file_test").ok());
    ASSERT_FALSE(fs::path_exist("./file_test"));

    // remove paths
    ASSERT_TRUE(fs::create_directories("./file_test/123/456/789").ok());
    ASSERT_TRUE(fs::create_directories("./file_test/abc/def/zxc").ok());
    save_string_file("./file_test/s1", "123");
    save_string_file("./file_test/s2", "123");

    std::vector<std::string> ps;
    ps.emplace_back("./file_test/123/456/789");
    ps.emplace_back("./file_test/123/456");
    ps.emplace_back("./file_test/123");

    ASSERT_TRUE(fs::path_exist("./file_test/123"));
    ASSERT_TRUE(fs::remove(ps).ok());
    ASSERT_FALSE(fs::path_exist("./file_test/123"));

    ps.clear();
    ps.emplace_back("./file_test/s1");
    ps.emplace_back("./file_test/abc/def");

    ASSERT_FALSE(fs::remove(ps).ok());
    ASSERT_FALSE(fs::path_exist("./file_test/s1"));
    ASSERT_TRUE(fs::path_exist("./file_test/abc/def/"));

    ps.clear();
    ps.emplace_back("./file_test/abc/def/zxc");
    ps.emplace_back("./file_test/s2");
    ps.emplace_back("./file_test/abc/def");
    ps.emplace_back("./file_test/abc");

    ASSERT_TRUE(fs::remove(ps).ok());
    ASSERT_FALSE(fs::path_exist("./file_test/s2"));
    ASSERT_FALSE(fs::path_exist("./file_test/abc"));

    ASSERT_TRUE(fs::remove_all("./file_test").ok());
}

TEST_F(FileUtilsTest, TestCreateDir) {
    // normal
    std::string path = "./file_test/123/456/789";
    fs::remove_all("./file_test");
    ASSERT_FALSE(fs::path_exist(path));

    ASSERT_TRUE(fs::create_directories(path).ok());

    ASSERT_TRUE(fs::path_exist(path));
    ASSERT_TRUE(fs::is_directory("./file_test").value());
    ASSERT_TRUE(fs::is_directory("./file_test/123").value());
    ASSERT_TRUE(fs::is_directory("./file_test/123/456").value());
    ASSERT_TRUE(fs::is_directory("./file_test/123/456/789").value());

    fs::remove_all("./file_test");

    // normal
    path = "./file_test/123/456/789/";
    fs::remove_all("./file_test");
    ASSERT_FALSE(fs::path_exist(path));

    ASSERT_TRUE(fs::create_directories(path).ok());

    ASSERT_TRUE(fs::path_exist(path));
    ASSERT_TRUE(fs::is_directory("./file_test").value());
    ASSERT_TRUE(fs::is_directory("./file_test/123").value());
    ASSERT_TRUE(fs::is_directory("./file_test/123/456").value());
    ASSERT_TRUE(fs::is_directory("./file_test/123/456/789").value());

    fs::remove_all("./file_test");

    // absolute path;
    std::string real_path;
    FileSystem::Default()->canonicalize(".", &real_path);
    ASSERT_TRUE(fs::create_directories(real_path + "/file_test/absolute/path/123/asdf").ok());
    ASSERT_TRUE(fs::is_directory("./file_test/absolute/path/123/asdf").value());
    fs::remove_all("./file_test");
}

TEST_F(FileUtilsTest, TestListDirsFiles) {
    std::string path = "./file_test/";
    fs::remove_all(path);
    fs::create_directories("./file_test/1");
    fs::create_directories("./file_test/2");
    fs::create_directories("./file_test/3");
    fs::create_directories("./file_test/4");
    fs::create_directories("./file_test/5");

    std::set<string> dirs;
    std::set<string> files;

    ASSERT_TRUE(fs::list_dirs_files("./file_test", &dirs, &files).ok());
    ASSERT_EQ(5, dirs.size());
    ASSERT_EQ(0, files.size());

    dirs.clear();
    files.clear();

    ASSERT_TRUE(fs::list_dirs_files("./file_test", &dirs, nullptr).ok());
    ASSERT_EQ(5, dirs.size());
    ASSERT_EQ(0, files.size());

    save_string_file("./file_test/f1", "just test");
    save_string_file("./file_test/f2", "just test");
    save_string_file("./file_test/f3", "just test");

    dirs.clear();
    files.clear();

    ASSERT_TRUE(fs::list_dirs_files("./file_test", &dirs, &files).ok());
    ASSERT_EQ(5, dirs.size());
    ASSERT_EQ(3, files.size());

    dirs.clear();
    files.clear();

    ASSERT_TRUE(fs::list_dirs_files("./file_test", nullptr, &files).ok());
    ASSERT_EQ(0, dirs.size());
    ASSERT_EQ(3, files.size());

    fs::remove_all(path);
}
} // namespace starrocks
