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

#include "base/testutil/assert.h"

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

    {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("unknown1://"));
        ASSERT_EQ(fs->type(), FileSystem::HDFS);
    }

    {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString("unknown1://"));
        ASSERT_EQ(fs->type(), FileSystem::HDFS);
    }

    {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString("unknown2://"));
        ASSERT_EQ(fs->type(), FileSystem::S3);
    }

    {
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString("unknown2://"));
        ASSERT_EQ(fs->type(), FileSystem::S3);
    }

    {
        std::unordered_map<std::string, std::string> params = {{"fs.s3a.readahead.range", "100"}};
        std::unique_ptr<FSOptions> fs_options = std::make_unique<FSOptions>(params);
        ASSIGN_OR_ABORT(auto fs, FileSystem::Create("unknown2://", *fs_options));
        ASSERT_EQ(fs->type(), FileSystem::S3);
    }

    {
        std::string uri = "wasbs://container_name@account_name.blob.core.windows.net/blob_name";

        TCloudConfiguration cloud_configuration;
        cloud_configuration.__set_cloud_type(TCloudType::AZURE);
        cloud_configuration.__set_cloud_properties({});
        cloud_configuration.__set_azure_use_native_sdk(true);
        THdfsProperties hdfs_properties;
        hdfs_properties.__set_cloud_configuration(cloud_configuration);
        TBrokerScanRangeParams scan_range_params;
        scan_range_params.__set_hdfs_properties(hdfs_properties);
        FSOptions options(&scan_range_params);

        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString(uri, options));
        ASSERT_EQ(fs->type(), FileSystem::AZBLOB);
    }
}

} // namespace starrocks
