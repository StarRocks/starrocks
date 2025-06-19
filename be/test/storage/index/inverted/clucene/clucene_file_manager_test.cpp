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

#include "storage/index/inverted/clucene/clucene_file_manager.h"

#include <gtest/gtest.h>

#include "fs/fs_util.h"
#include "storage/index/inverted/clucene/clucene_file_writer.h"

namespace starrocks {

class CLuceneFileManagerTest : public ::testing::Test {
    CLuceneFileManagerTest() = default;
    ~CLuceneFileManagerTest() override = default;
    void SetUp() override { ASSERT_TRUE(fs::create_directories("./ut_dir/clucene_fs").ok()); }
    void TearDown() override { ASSERT_TRUE(fs::remove_all("./ut_dir/clucene_fs").ok()); }
};

TEST(CLuceneFileManagerTest, get_writer) {
    const std::string file("./ut_dir/clucene_fs/f1");
    auto st1 = CLuceneFileManager::getInstance().get_or_create_clucene_file_writer(file);
    ASSERT_TRUE(st1.ok());
    auto st2 = CLuceneFileManager::getInstance().get_or_create_clucene_file_writer(file);
    ASSERT_TRUE(st2.ok());

    const auto f1 = std::move(st1).value();
    const auto f2 = std::move(st2).value();
    ASSERT_EQ(reinterpret_cast<uint64_t>(f1.get()), reinterpret_cast<uint64_t>(f2.get()));
    ASSERT_TRUE(f1->close().ok());
    ASSERT_TRUE(f2->close().ok());

    ASSERT_TRUE(CLuceneFileManager::getInstance().remove_clucene_file_writer(file).ok());
    auto st3 = CLuceneFileManager::getInstance().get_or_create_clucene_file_writer(file);
    ASSERT_TRUE(st3.ok());
    const auto f3 = std::move(st3).value();
    ASSERT_EQ(reinterpret_cast<uint64_t>(f1.get()), reinterpret_cast<uint64_t>(f3.get()));
    ASSERT_TRUE(f3->close().ok());
}

} // namespace starrocks