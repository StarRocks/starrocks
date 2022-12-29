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

#include "util/file_util.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace starrocks {

TEST(FileUtilTest, test_read_whole_content) {
    std::string name = std::tmpnam(nullptr);

    std::string content;
    ASSERT_FALSE(FileUtil::read_contents(name, content));

    std::ofstream ofs;
    ofs.open(name);
    ASSERT_TRUE(ofs.good());
    ofs.close();

    ASSERT_TRUE(FileUtil::read_contents(name, content));

    std::filesystem::remove(name);
}

TEST(FileUtilTest, test_read_contents) {
    std::string name = std::tmpnam(nullptr);

    int first = -1;
    double second = -1;
    long third = -1;
    ASSERT_FALSE(FileUtil::read_contents(name, first, second, third));
    ASSERT_EQ(-1, first);
    ASSERT_EQ(-1, second);
    ASSERT_EQ(-1, third);

    std::ofstream ofs;
    ofs.open(name);
    ASSERT_TRUE(ofs.good());
    ofs << "1 2.5 3";
    ofs.close();
    first = second = third = -1;
    ASSERT_TRUE(FileUtil::read_contents(name, first, second, third));
    ASSERT_EQ(1, first);
    ASSERT_EQ(2.5, second);
    ASSERT_EQ(3, third);

    ofs.open(name);
    ASSERT_TRUE(ofs.good());
    ofs << "1 2.5";
    ofs.close();
    first = second = third = -1;
    ASSERT_FALSE(FileUtil::read_contents(name, first, second, third));
    ASSERT_EQ(1, first);
    ASSERT_EQ(2.5, second);
    ASSERT_EQ(-1, third);

    std::filesystem::remove(name);
}
}; // namespace starrocks
