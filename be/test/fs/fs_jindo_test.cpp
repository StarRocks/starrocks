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

#include "fs/jindo/fs_jindo.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "fmt/format.h"
#include "jindosdk/jdo_api.h"
#include "testutil/assert.h"
#include "util/uid_util.h"

namespace starrocks {

class JindoFileSystemTest : public testing::Test {
public:
    JindoFileSystemTest()
            : _root_dir(fmt::format("{}/tmp/jindo_test", config::jindo_fs_test_endpoint)),
              _test_file(fmt::format("{}/test_file", _root_dir)),
              _fs(new_fs_jindo(FSOptions{})) {};
    ~JindoFileSystemTest() override = default;

protected:
    void SetUp() override { ASSERT_OK(_fs->create_dir_if_missing(_root_dir, nullptr)); };

    void TearDown() override { ASSERT_OK(_fs->delete_file(_test_file)); };

    const std::string _root_dir;
    const std::string _test_file;
    const std::unique_ptr<FileSystem> _fs;
};

TEST_F(JindoFileSystemTest, io_test) {
    const std::string head = "hello";
    const std::string tail = " kitty";
    const std::string raw = head + tail;
    const starrocks::Slice data(raw);

#define OPEN_FOR_WRITE(open_mode, expect_failure, clean_file)                                         \
    do {                                                                                              \
        if (clean_file) {                                                                             \
            ASSERT_OK(_fs->delete_file(_test_file));                                                  \
        }                                                                                             \
        auto maybe_file = _fs->new_writable_file(WritableFileOptions{.mode = open_mode}, _test_file); \
        if (expect_failure) {                                                                         \
            ASSERT_ERROR(maybe_file);                                                                 \
        } else {                                                                                      \
            ASSERT_OK(maybe_file);                                                                    \
            ASSERT_OK(maybe_file.value()->close());                                                   \
        }                                                                                             \
    } while (0)

    // test for open mode for writting when file exists
    OPEN_FOR_WRITE(FileSystem::OpenMode::CREATE_OR_OPEN_WITH_TRUNCATE, false, false);
    OPEN_FOR_WRITE(FileSystem::OpenMode::CREATE_OR_OPEN, false, false);
    OPEN_FOR_WRITE(FileSystem::OpenMode::MUST_CREATE, true, false);
    OPEN_FOR_WRITE(FileSystem::OpenMode::MUST_EXIST, false, false);

    // test for open mode for writting when file not exists
    OPEN_FOR_WRITE(FileSystem::OpenMode::CREATE_OR_OPEN_WITH_TRUNCATE, false, true);
    OPEN_FOR_WRITE(FileSystem::OpenMode::CREATE_OR_OPEN, false, true);
    OPEN_FOR_WRITE(FileSystem::OpenMode::MUST_CREATE, false, true);
    OPEN_FOR_WRITE(FileSystem::OpenMode::MUST_EXIST, true, true);

    // populate file content
    {
        auto maybe_file = _fs->new_writable_file(
                WritableFileOptions{.mode = FileSystem::OpenMode::CREATE_OR_OPEN_WITH_TRUNCATE}, _test_file);
        ASSERT_OK(maybe_file);
        auto file = std::move(maybe_file.value());

        auto maybe_size = _fs->get_file_size(_test_file);
        ASSERT_OK(maybe_size);
        ASSERT_EQ(maybe_size.value(), 0);

        ASSERT_OK(file->append(data));
        ASSERT_OK(file->flush(starrocks::WritableFile::FLUSH_SYNC));
        ASSERT_OK(file->close());
    }

    // verify readable and content match
    {
        auto maybe_file = _fs->new_sequential_file(_test_file);
        ASSERT_OK(maybe_file);
        auto file = std::move(maybe_file.value());

        auto maybe_size = _fs->get_file_size(_test_file);
        ASSERT_OK(maybe_size);
        ASSERT_EQ(maybe_size.value(), data.get_size());

        OwnedSlice buffer(new uint8_t[maybe_size.value()](), maybe_size.value());
        auto slice = buffer.slice();
        ASSERT_OK(file->read_fully(slice.mutable_data(), slice.get_size()));
        ASSERT_TRUE(data == slice);
    }

    // verify seek read correctness
    {
        auto maybe_file = _fs->new_random_access_file(_test_file);
        ASSERT_OK(maybe_file);
        auto file = std::move(maybe_file.value());

        ASSERT_OK(file->seek(head.length()));

        OwnedSlice buffer(new uint8_t[tail.length()](), tail.length());
        auto slice = buffer.slice();
        auto maybe_read = file->read_fully(slice.mutable_data(), slice.get_size());
        ASSERT_OK(maybe_read);

        ASSERT_TRUE(slice == Slice(tail));
    }
};

} // namespace starrocks