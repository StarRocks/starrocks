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
//   https://github.com/apache/incubator-doris/blob/master/be/test/env/env_posix_test.cpp

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

#include <algorithm>

#include "common/logging.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "testutil/assert.h"

namespace starrocks {

class PosixFileSystemTest : public testing::Test {
public:
    PosixFileSystemTest() = default;
    ~PosixFileSystemTest() override = default;
    void SetUp() override { ASSERT_TRUE(fs::create_directories("./ut_dir/fs_posix").ok()); }
    void TearDown() override { ASSERT_TRUE(fs::remove_all("./ut_dir").ok()); }
};

TEST_F(PosixFileSystemTest, random_access) {
    std::string fname = "./ut_dir/fs_posix/random_access";
    std::unique_ptr<WritableFile> wfile;
    auto fs = FileSystem::Default();
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    wfile = *fs->new_writable_file(opts, fname);
    auto st = wfile->pre_allocate(1024);
    ASSERT_TRUE(st.ok());
    // wirte data
    Slice field1("123456789");
    st = wfile->append(field1);
    ASSERT_TRUE(st.ok());
    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = wfile->append(buf);
    ASSERT_TRUE(st.ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2]{abc, bcd};
    st = wfile->appendv(slices, 2);
    ASSERT_TRUE(st.ok());
    st = wfile->flush(WritableFile::FLUSH_ASYNC);
    ASSERT_TRUE(st.ok());
    st = wfile->sync();
    ASSERT_TRUE(st.ok());
    st = wfile->close();
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(115, wfile->size());

    const auto status_or = fs->get_file_size(fname);
    ASSERT_TRUE(status_or.ok());
    const uint64_t size = status_or.value();
    ASSERT_EQ(115, size);
    {
        char mem[1024];
        auto rfile = *fs->new_random_access_file(fname);

        Slice slice1(mem, 9);
        Slice slice2(mem + 9, 100);
        Slice slice3(mem + 9 + 100, 3);

        ASSERT_OK(rfile->read_at_fully(0, slice1.data, slice1.size));
        ASSERT_OK(rfile->read_at_fully(9, slice2.data, slice2.size));
        ASSERT_OK(rfile->read_at_fully(109, slice3.data, slice3.size));
        ASSERT_STREQ("123456789", std::string(slice1.data, slice1.size).c_str());
        ASSERT_STREQ("abc", std::string(slice3.data, slice3.size).c_str());

        st = rfile->read_at_fully(112, mem, 3);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_STREQ("bcd", std::string(mem, 3).c_str());

        ASSERT_ERROR(rfile->read_at_fully(114, mem, 3));
    }
    // test try read
    {
        char mem[1024];
        auto rfile = *fs->new_random_access_file(fname);

        // normal read
        {
            ASSIGN_OR_ABORT(auto nread, rfile->read_at(0, mem, 9));
            ASSERT_EQ(9, nread);
            ASSERT_EQ("123456789", std::string_view(mem, 9));
        }
        // read too many
        {
            ASSIGN_OR_ABORT(auto nread, rfile->read_at(16, mem, 100));
            ASSERT_EQ(99, nread);
        }
        // read empty
        {
            Slice slice(mem, 100);
            ASSIGN_OR_ABORT(auto nread, rfile->read_at(115, mem, 100));
            ASSERT_EQ(0, nread);
        }
    }
}

TEST_F(PosixFileSystemTest, iterate_dir) {
    const std::string dir_path = "./ut_dir/fs_posix/iterate_dir";
    ASSERT_OK(fs::remove_all(dir_path));
    ASSERT_OK(FileSystem::Default()->create_dir_if_missing(dir_path));

    ASSERT_OK(FileSystem::Default()->create_dir_if_missing(dir_path + "/abc"));

    ASSERT_OK(FileSystem::Default()->create_dir_if_missing(dir_path + "/123"));

    {
        std::vector<std::string> children;
        ASSERT_OK(FileSystem::Default()->get_children(dir_path, &children));
        ASSERT_EQ(2, children.size());
        std::sort(children.begin(), children.end());

        ASSERT_STREQ("123", children[0].c_str());
        ASSERT_STREQ("abc", children[1].c_str());
    }
    {
        std::vector<std::string> children;
        ASSERT_OK(FileSystem::Default()->get_children(dir_path, &children));
        ASSERT_EQ(2, children.size());
        std::sort(children.begin(), children.end());

        ASSERT_STREQ("123", children[0].c_str());
        ASSERT_STREQ("abc", children[1].c_str());
    }

    // Delete non-empty directory, should fail.
    ASSERT_ERROR(FileSystem::Default()->delete_dir(dir_path));
    {
        std::vector<std::string> children;
        ASSERT_OK(FileSystem::Default()->get_children(dir_path, &children));
        ASSERT_EQ(2, children.size());
        std::sort(children.begin(), children.end());

        ASSERT_STREQ("123", children[0].c_str());
        ASSERT_STREQ("abc", children[1].c_str());
    }

    // Delete directory recursively, should success.
    ASSERT_OK(FileSystem::Default()->delete_dir_recursive(dir_path));
    ASSERT_TRUE(FileSystem::Default()->path_exists(dir_path).is_not_found());
}

TEST_F(PosixFileSystemTest, create_dir_recursive) {
    const std::string dir_path = "./ut_dir/fs_posix/a/b/c/d";

    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a").status().is_not_found());
    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a/b").status().is_not_found());
    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a/b/c").status().is_not_found());
    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a/b/c/d").status().is_not_found());

    ASSERT_OK(FileSystem::Default()->create_dir_recursive(dir_path));
    ASSERT_OK(FileSystem::Default()->create_dir_recursive(dir_path));

    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir").value());
    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a").value());
    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a/b").value());
    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a/b/c").value());
    ASSERT_TRUE(FileSystem::Default()->is_directory("./ut_dir/fs_posix/a/b/c/d").value());

    ASSERT_OK(FileSystem::Default()->delete_dir_recursive(dir_path));
    ASSERT_TRUE(FileSystem::Default()->path_exists(dir_path).is_not_found());
}

TEST_F(PosixFileSystemTest, iterate_dir2) {
    auto fs = FileSystem::Default();
    auto now = ::time(nullptr);
    ASSERT_OK(fs->create_dir_recursive("./ut_dir/fs_posix/iterate_dir2.d"));
    ASSIGN_OR_ABORT(auto f, fs->new_writable_file("./ut_dir/fs_posix/iterate_dir2"));
    ASSERT_OK(f->append("test"));
    ASSERT_OK(f->close());

    ASSERT_OK(fs->iterate_dir2("./ut_dir/fs_posix/", [&](DirEntry entry) -> bool {
        auto name = entry.name;
        if (name == "iterate_dir2.d") {
            CHECK(entry.is_dir.has_value());
            CHECK(entry.is_dir.value());
            CHECK(entry.mtime.has_value());
            CHECK_GE(entry.mtime.value(), now);
        } else if (name == "iterate_dir2") {
            CHECK(entry.is_dir.has_value());
            CHECK(!entry.is_dir.value());
            CHECK(entry.size.has_value());
            CHECK_EQ(4, entry.size.value());
            CHECK(entry.mtime.has_value());
            CHECK_GE(entry.mtime.value(), now);
        } else {
            CHECK(false) << "Unexpected file " << name;
        }
        return true;
    }));
}

TEST_F(PosixFileSystemTest, test_delete_files) {
    auto fs = FileSystem::Default();

    auto path1 = std::string("./ut_dir/fs_posix/f1");
    ASSIGN_OR_ABORT(auto wf1, fs->new_writable_file(path1));
    EXPECT_OK(wf1->append("hello"));
    EXPECT_OK(wf1->append(" world!"));
    EXPECT_OK(wf1->sync());
    EXPECT_OK(wf1->close());
    EXPECT_EQ(sizeof("hello world!"), wf1->size() + 1);
    EXPECT_OK(fs->path_exists(path1));

    auto path2 = std::string("./ut_dir/fs_posix/f2");
    ASSIGN_OR_ABORT(auto wf2, fs->new_writable_file(path2));
    EXPECT_OK(wf2->append("hello"));
    EXPECT_OK(wf2->append(" world!"));
    EXPECT_OK(wf2->sync());
    EXPECT_OK(wf2->close());
    EXPECT_EQ(sizeof("hello world!"), wf2->size() + 1);
    EXPECT_OK(fs->path_exists(path2));

    std::vector<std::string> paths{path1, path2};
    EXPECT_OK(fs->delete_files(paths));
    EXPECT_TRUE(fs->path_exists(path1).is_not_found());
    EXPECT_TRUE(fs->path_exists(path2).is_not_found());
}

} // namespace starrocks
