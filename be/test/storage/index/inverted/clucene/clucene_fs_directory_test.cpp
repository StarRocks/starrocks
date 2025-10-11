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

#include "storage/index/inverted/clucene/clucene_fs_directory.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "fs/fs_util.h"

namespace starrocks {

class CLuceneFsDirectoryTest : public ::testing::Test {
    CLuceneFsDirectoryTest() = default;
    ~CLuceneFsDirectoryTest() override = default;
    void SetUp() override { ASSERT_TRUE(fs::create_directories("./ut_dir/clucene_fs").ok()); }
    void TearDown() override { ASSERT_TRUE(fs::remove_all("./ut_dir/clucene_fs").ok()); }
};

TEST(CLuceneFsDirectoryTest, basic) {
    FileSystem* fs = FileSystem::Default();
    const std::string& filename("./ut_dir/clucene_fs/ivt_dir");
    auto status = StarRocksFSDirectoryFactory::getDirectory(fs, filename);
    ASSERT_TRUE(status.ok());
    auto dir = std::move(status).value();
    ASSERT_EQ(filename, dir->getDirName());

    {
        const std::string& f1 = "f1";
        dir->touchFile(f1.c_str());
        ASSERT_TRUE(dir->fileExists(f1.c_str()));
    }

    {
        const std::string& f2 = "f2";
        const auto out = dir->createOutput(f2.c_str());

        std::string content("CLuceneFsDirectory");
        const uint8_t* b = reinterpret_cast<uint8_t*>(content.data());
        out->writeBytes(b, content.size());
        out->flush();

        out->close();
        ASSERT_EQ(content.size(), dir->fileLength(f2.c_str()));

        lucene::store::IndexInput* tmp;
        CLuceneError err;
        ASSERT_TRUE(dir->openInput(f2.c_str(), tmp, err));
        ASSERT_EQ(0, err.number());

        std::string result;
        result.reserve(content.size());
        tmp->readBytes(reinterpret_cast<uint8_t*>(result.data()), content.size());
        ASSERT_EQ(result, content);
    }

    {
        std::vector<std::string> files;
        dir->list(&files);
        ASSERT_EQ(files.size(), 2);
        ASSERT_EQ(files[0], "f1");
        ASSERT_EQ(files[1], "f2");
    }

    {
        dir->renameFile("f1", "f3");
        ASSERT_FALSE(dir->fileExists("f1"));
        ASSERT_TRUE(dir->fileExists("f2"));
        ASSERT_TRUE(dir->fileExists("f3"));
    }

    {
        ASSERT_TRUE(dir->deleteDirectory());
        auto st = fs->path_exists(filename);
        ASSERT_TRUE(st.is_not_found());
    }
}

TEST(CLuceneFsDirectoryTest, ram_directory) {
    FileSystem* fs = FileSystem::Default();
    const std::string& filename("./ut_dir/clucene_fs/ivt_dir");
    auto status = StarRocksFSDirectoryFactory::getDirectory(fs, filename, true);
    ASSERT_TRUE(status.ok());
    auto dir = std::move(status).value();
    ASSERT_EQ(filename, dir->getDirName());

    {
        const std::string& f1 = "f1";
        dir->touchFile(f1.c_str());
        ASSERT_TRUE(dir->fileExists(f1.c_str()));
    }

    {
        const std::string& f2 = "f2";
        const auto out = dir->createOutput(f2.c_str());

        std::string content("CLuceneFsDirectory");
        const uint8_t* b = reinterpret_cast<uint8_t*>(content.data());
        out->writeBytes(b, content.size());
        out->flush();

        out->close();
        ASSERT_EQ(content.size(), dir->fileLength(f2.c_str()));

        lucene::store::IndexInput* tmp;
        CLuceneError err;
        ASSERT_TRUE(dir->openInput(f2.c_str(), tmp, err));
        ASSERT_EQ(0, err.number());

        std::string result;
        result.reserve(content.size());
        tmp->readBytes(reinterpret_cast<uint8_t*>(result.data()), content.size());
        ASSERT_EQ(result, content);
    }

    {
        std::vector<std::string> files;
        dir->list(&files);
        ASSERT_EQ(files.size(), 2);
        ASSERT_EQ(files[0], "f1");
        ASSERT_EQ(files[1], "f2");
    }

    {
        dir->renameFile("f1", "f3");
        ASSERT_FALSE(dir->fileExists("f1"));
        ASSERT_TRUE(dir->fileExists("f2"));
        ASSERT_TRUE(dir->fileExists("f3"));
    }

    {
        ASSERT_TRUE(dir->deleteDirectory());
        auto st = fs->path_exists(filename);
        ASSERT_TRUE(st.is_not_found());
    }
}

} // namespace starrocks