// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "fs/fs.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"
namespace starrocks {

TEST(FileSystemTest, test_good_construction) {
    struct Case {
        std::string uri;
        FileSystem::Type type;
    };

    std::vector<Case> cases = {
            {.uri = "viewfs://aaa", .type = FileSystem::HDFS}, {.uri = "hdfs://aaa", .type = FileSystem::HDFS},
            {.uri = "s3a://aaa", .type = FileSystem::S3},      {.uri = "s3n://aaa", .type = FileSystem::S3},
            {.uri = "s3://aaa", .type = FileSystem::S3},       {.uri = "oss://aaa", .type = FileSystem::S3},
            {.uri = "cos://aaa", .type = FileSystem::S3},
    };

    for (auto& c : cases) {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString(c.uri));
        ASSERT_EQ(fs->type(), c.type);
    }

    for (auto& c : cases) {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(c.uri));
        ASSERT_EQ(fs->type(), c.type);
    }
}

TEST(FileSystemTest, test_bad_construction) {
    std::vector<std::string> cases = {"webhdfs://aaa", "simplefs://"};
    for (auto& c : cases) {
        EXPECT_ERROR(FileSystem::CreateUniqueFromString(c));
    }

    for (auto& c : cases) {
        EXPECT_ERROR(FileSystem::CreateSharedFromString(c));
    }
}

} // namespace starrocks