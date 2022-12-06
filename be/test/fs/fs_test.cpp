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

} // namespace starrocks
