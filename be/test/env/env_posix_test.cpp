// This file is made available under Elastic License 2.0.
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
#include "env/env.h"
#include "testutil/assert.h"
#include "util/file_utils.h"

namespace starrocks {

class EnvPosixTest : public testing::Test {
public:
    EnvPosixTest() {}
    virtual ~EnvPosixTest() {}
    void SetUp() override { ASSERT_TRUE(FileUtils::create_dir("./ut_dir/env_posix").ok()); }
    void TearDown() override { ASSERT_TRUE(FileUtils::remove_all("./ut_dir").ok()); }
};

TEST_F(EnvPosixTest, random_access) {
    std::string fname = "./ut_dir/env_posix/random_access";
    WritableFileOptions ops;
    std::unique_ptr<WritableFile> wfile;
    auto env = Env::Default();
    wfile = *env->new_writable_file(fname);
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

    const auto status_or = env->get_file_size(fname);
    ASSERT_TRUE(status_or.ok());
    const uint64_t size = status_or.value();
    ASSERT_EQ(115, size);
    {
        char mem[1024];
        auto rfile = *env->new_random_access_file(fname);

        Slice slice1(mem, 9);
        Slice slice2(mem + 9, 100);
        Slice slice3(mem + 9 + 100, 3);

        Slice read_slices[3]{slice1, slice2, slice3};
        st = rfile->readv_at(0, read_slices, 3);
        ASSERT_TRUE(st.ok());
        ASSERT_STREQ("123456789", std::string(slice1.data, slice1.size).c_str());
        ASSERT_STREQ("abc", std::string(slice3.data, slice3.size).c_str());

        st = rfile->read_at_fully(112, mem, 3);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_STREQ("bcd", std::string(mem, 3).c_str());

        // end of file
        st = rfile->read_at_fully(114, mem, 3);
        ASSERT_EQ(TStatusCode::END_OF_FILE, st.code());
        LOG(INFO) << "st=" << st.to_string();
    }
    // test try read
    {
        char mem[1024];
        auto rfile = *env->new_random_access_file(fname);

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

TEST_F(EnvPosixTest, random_rw) {
    std::string fname = "./ut_dir/env_posix/random_rw";
    WritableFileOptions ops;
    std::unique_ptr<RandomRWFile> wfile;
    auto env = Env::Default();
    wfile = *env->new_random_rw_file(fname);
    // wirte data
    Slice field1("123456789");
    auto st = wfile->write_at(0, field1);
    ASSERT_TRUE(st.ok());
    std::string buf;
    for (int i = 0; i < 100; ++i) {
        buf.push_back((char)i);
    }
    st = wfile->write_at(9, buf);
    ASSERT_TRUE(st.ok());
    Slice abc("abc");
    Slice bcd("bcd");
    Slice slices[2]{abc, bcd};
    st = wfile->writev_at(0, slices, 2);
    ASSERT_TRUE(st.ok());

    ASSIGN_OR_ABORT(uint64_t size, wfile->get_size());
    ASSERT_EQ(109, size);

    st = wfile->flush(RandomRWFile::FLUSH_ASYNC, 0, 0);
    ASSERT_TRUE(st.ok());
    st = wfile->sync();
    ASSERT_TRUE(st.ok());
    st = wfile->close();
    ASSERT_TRUE(st.ok());

    const auto status_or = env->get_file_size(fname);
    ASSERT_TRUE(status_or.ok());
    size = status_or.value();
    ASSERT_EQ(109, size);
    {
        char mem[1024];
        std::unique_ptr<RandomRWFile> rfile;
        RandomRWFileOptions opts;
        opts.mode = Env::MUST_EXIST;
        rfile = *env->new_random_rw_file(opts, fname);

        Slice slice1(mem, 3);
        Slice slice2(mem + 3, 3);
        Slice slice3(mem + 6, 3);

        Slice read_slices[3]{slice1, slice2, slice3};
        st = rfile->readv_at(0, read_slices, 3);
        LOG(INFO) << st.to_string();
        ASSERT_TRUE(st.ok());
        ASSERT_STREQ("abc", std::string(slice1.data, slice1.size).c_str());
        ASSERT_STREQ("bcd", std::string(slice2.data, slice2.size).c_str());
        ASSERT_STREQ("789", std::string(slice3.data, slice3.size).c_str());

        Slice slice4(mem, 100);
        st = rfile->read_at(9, slice4);
        ASSERT_TRUE(st.ok());

        // end of file
        st = rfile->read_at(102, slice4);
        ASSERT_EQ(TStatusCode::END_OF_FILE, st.code());
        LOG(INFO) << "st=" << st.to_string();
    }
    // SequentialFile
    {
        char mem[1024];
        auto rfile = *env->new_sequential_file(fname);

        ASSIGN_OR_ABORT(auto nread, rfile->read(mem, 3));
        ASSERT_EQ(3, nread);
        ASSERT_EQ("abc", std::string_view(mem, nread));

        st = rfile->skip(3);
        ASSERT_TRUE(st.ok());

        ASSIGN_OR_ABORT(nread, rfile->read(mem, 3));
        ASSERT_EQ(3, nread);
        ASSERT_EQ("789", std::string_view(mem, 3));

        st = rfile->skip(90);
        ASSERT_TRUE(st.ok());

        ASSIGN_OR_ABORT(nread, rfile->read(mem, 15));
        ASSERT_EQ(10, nread);

        ASSIGN_OR_ABORT(nread, rfile->read(mem, 15));
        ASSERT_EQ(0, nread);
    }
}

TEST_F(EnvPosixTest, iterate_dir) {
    const std::string dir_path = "./ut_dir/env_posix/iterate_dir";
    FileUtils::remove_all(dir_path);
    ASSERT_OK(Env::Default()->create_dir_if_missing(dir_path));

    ASSERT_OK(Env::Default()->create_dir_if_missing(dir_path + "/abc"));

    ASSERT_OK(Env::Default()->create_dir_if_missing(dir_path + "/123"));

    {
        std::vector<std::string> children;
        ASSERT_OK(Env::Default()->get_children(dir_path, &children));
        ASSERT_EQ(4, children.size());
        std::sort(children.begin(), children.end());

        ASSERT_STREQ(".", children[0].c_str());
        ASSERT_STREQ("..", children[1].c_str());
        ASSERT_STREQ("123", children[2].c_str());
        ASSERT_STREQ("abc", children[3].c_str());
    }
    {
        std::vector<std::string> children;
        ASSERT_OK(FileUtils::list_files(Env::Default(), dir_path, &children));
        ASSERT_EQ(2, children.size());
        std::sort(children.begin(), children.end());

        ASSERT_STREQ("123", children[0].c_str());
        ASSERT_STREQ("abc", children[1].c_str());
    }

    // Delete non-empty directory, should fail.
    ASSERT_ERROR(Env::Default()->delete_dir(dir_path));
    {
        std::vector<std::string> children;
        ASSERT_OK(Env::Default()->get_children(dir_path, &children));
        ASSERT_EQ(4, children.size());
        std::sort(children.begin(), children.end());

        ASSERT_STREQ(".", children[0].c_str());
        ASSERT_STREQ("..", children[1].c_str());
        ASSERT_STREQ("123", children[2].c_str());
        ASSERT_STREQ("abc", children[3].c_str());
    }

    // Delete directory recursively, should success.
    ASSERT_OK(Env::Default()->delete_dir_recursive(dir_path));
    ASSERT_TRUE(Env::Default()->path_exists(dir_path).is_not_found());
}

TEST_F(EnvPosixTest, create_dir_recursive) {
    const std::string dir_path = "./ut_dir/env_posix/a/b/c/d";

    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a").status().is_not_found());
    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a/b").status().is_not_found());
    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a/b/c").status().is_not_found());
    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a/b/c/d").status().is_not_found());

    ASSERT_OK(Env::Default()->create_dir_recursive(dir_path));

    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir").value());
    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a").value());
    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a/b").value());
    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a/b/c").value());
    ASSERT_TRUE(Env::Default()->is_directory("./ut_dir/env_posix/a/b/c/d").value());

    ASSERT_OK(Env::Default()->delete_dir_recursive(dir_path));
    ASSERT_TRUE(Env::Default()->path_exists(dir_path).is_not_found());
}

} // namespace starrocks
