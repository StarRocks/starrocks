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

#include "util/download_util.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "fmt/format.h"
#include "fs/fs_util.h"
#include "testutil/assert.h"

namespace starrocks {

class DownloadUtilTest : public testing::Test {
public:
    DownloadUtilTest() = default;
    ~DownloadUtilTest() override = default;

    void SetUp() override {
        _test_dir = fmt::format("{}/{}", std::filesystem::current_path().string(), kTestDirectory);
        CHECK_OK(fs::remove_all(_test_dir));
        CHECK_OK(fs::create_directories(_test_dir));

        _source_file = fmt::format("{}/source_file", _test_dir);
        _dest_file = fmt::format("{}/dest_file", _test_dir);

        auto fp = fopen(_source_file.c_str(), "w");
        ASSERT_TRUE(fp != nullptr);
        std::string data = "abcd";
        fwrite(data.c_str(), data.size(), 1, fp);
        fclose(fp);

        _source_md5 = fs::md5sum(_source_file).value();
    }

    void TearDown() override { ASSERT_OK(fs::remove_all(_test_dir)); }

private:
    constexpr static const char* const kTestDirectory = "test_download_util";

    std::string _test_dir;
    std::string _source_file;
    std::string _source_md5;
    std::string _dest_file;
};

TEST_F(DownloadUtilTest, test_normal) {
    ASSERT_FALSE(fs::path_exist(_dest_file));
    auto url = fmt::format("file://{}", _source_file);
    ASSERT_OK(DownloadUtil::download(url, _dest_file, _source_md5));
    ASSERT_TRUE(fs::path_exist(_dest_file));
    ASSERT_EQ(_source_md5, fs::md5sum(_dest_file).value());

    std::set<std::string> dirs;
    std::set<std::string> files;
    ASSERT_OK(fs::list_dirs_files(_test_dir, &dirs, &files));
    ASSERT_EQ(2, files.size());
}

TEST_F(DownloadUtilTest, test_wrong_md5) {
    ASSERT_FALSE(fs::path_exist(_dest_file));
    auto url = fmt::format("file://{}", _source_file);
    ASSERT_ERROR(DownloadUtil::download(url, _dest_file, "xxxxxx"));
    ASSERT_FALSE(fs::path_exist(_dest_file));

    std::set<std::string> dirs;
    std::set<std::string> files;
    ASSERT_OK(fs::list_dirs_files(_test_dir, &dirs, &files));
    ASSERT_EQ(1, files.size());
}

} // namespace starrocks
