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

#include "storage/index/inverted/tantivy/random_access_bridge.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "fs/fs.h"
#include "testutil/assert.h"

namespace starrocks {

namespace {

std::string make_tempdir(const std::string& prefix) {
    const char* base = std::getenv("TEST_TMPDIR");
    if (base == nullptr || *base == '\0') base = "/tmp";
    std::string tmpl = std::string(base) + "/" + prefix + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char* dir = ::mkdtemp(buf.data());
    return dir ? std::string(dir) : std::string{};
}

struct PathCleanup {
    std::string p;
    ~PathCleanup() {
        std::error_code ec;
        std::filesystem::remove_all(p, ec);
    }
};

} // namespace

TEST(RandomAccessBridgeTest, NormalRead) {
    std::string temp_dir = make_tempdir("ra_bridge_test");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    std::string file_path = temp_dir + "/test_data.bin";
    std::string content = "Hello, StarRocks RandomAccessFile bridge!";
    {
        std::ofstream ofs(file_path, std::ios::binary);
        ofs.write(content.data(), content.size());
    }

    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto ra_file, fs->new_random_access_file(file_path));

    // Read entire content.
    std::vector<uint8_t> buf(content.size());
    int rc = sr_random_access_read(ra_file.get(), 0, buf.data(), buf.size());
    ASSERT_EQ(0, rc);
    EXPECT_EQ(content, std::string(reinterpret_cast<char*>(buf.data()), buf.size()));

    // Read a substring from offset 7, length 10 → "StarRocks "
    std::vector<uint8_t> sub(10);
    rc = sr_random_access_read(ra_file.get(), 7, sub.data(), sub.size());
    ASSERT_EQ(0, rc);
    EXPECT_EQ("StarRocks ", std::string(reinterpret_cast<char*>(sub.data()), sub.size()));
}

TEST(RandomAccessBridgeTest, OutOfBoundsReadFails) {
    std::string temp_dir = make_tempdir("ra_bridge_oob");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    std::string file_path = temp_dir + "/small.bin";
    {
        std::ofstream ofs(file_path, std::ios::binary);
        ofs.write("ABCD", 4);
    }

    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto ra_file, fs->new_random_access_file(file_path));

    // Read beyond file end → should fail.
    std::vector<uint8_t> buf(100);
    int rc = sr_random_access_read(ra_file.get(), 0, buf.data(), 100);
    EXPECT_EQ(-1, rc);
}

TEST(RandomAccessBridgeTest, NullHandleFails) {
    uint8_t buf[4];
    int rc = sr_random_access_read(nullptr, 0, buf, 4);
    EXPECT_EQ(-1, rc);
}

TEST(RandomAccessBridgeTest, NullBufFails) {
    int rc = sr_random_access_read(reinterpret_cast<void*>(0x1234), 0, nullptr, 4);
    EXPECT_EQ(-1, rc);
}

} // namespace starrocks
