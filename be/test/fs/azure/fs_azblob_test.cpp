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

#include "fs/azure/fs_azblob.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "fs/credential/cloud_configuration_factory.h"

namespace starrocks {

class AzBlobFileSystemTest : public ::testing::Test {};

TEST_F(AzBlobFileSystemTest, test_new_random_access_file) {
    std::string uri = "wasbs://container_name@account_name.blob.core.windows.net/blob_name";

    {
        std::map<std::string, std::string> properties;
        properties.emplace(AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id_xxx");

        TCloudConfiguration cloud_configuration;
        cloud_configuration.__set_cloud_type(TCloudType::AZURE);
        cloud_configuration.__set_cloud_properties(properties);
        cloud_configuration.__set_azure_use_native_sdk(true);
        THdfsProperties hdfs_properties;
        hdfs_properties.__set_cloud_configuration(cloud_configuration);
        TBrokerScanRangeParams scan_range_params;
        scan_range_params.__set_hdfs_properties(hdfs_properties);
        FSOptions options(&scan_range_params);

        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString(uri, options));
        ASSERT_EQ(fs->type(), FileSystem::AZBLOB);

        ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(uri));
        EXPECT_TRUE(dynamic_cast<RandomAccessFile*>(file.get()) != nullptr);
    }

    {
        std::map<std::string, std::string> properties;
        properties.emplace(AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id_yyy");
        properties.emplace(AZURE_BLOB_OAUTH2_CLIENT_SECRET, "client_secret_yyy");
        properties.emplace(AZURE_BLOB_OAUTH2_TENANT_ID, "11111111-2222-3333-4444-555555555555");

        TCloudConfiguration cloud_configuration;
        cloud_configuration.__set_cloud_type(TCloudType::AZURE);
        cloud_configuration.__set_cloud_properties(properties);
        cloud_configuration.__set_azure_use_native_sdk(true);
        THdfsProperties hdfs_properties;
        hdfs_properties.__set_cloud_configuration(cloud_configuration);
        TBrokerScanRangeParams scan_range_params;
        scan_range_params.__set_hdfs_properties(hdfs_properties);
        FSOptions options(&scan_range_params);

        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateUniqueFromString(uri, options));
        ASSERT_EQ(fs->type(), FileSystem::AZBLOB);

        ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(uri));
        EXPECT_TRUE(dynamic_cast<RandomAccessFile*>(file.get()) != nullptr);
    }
}

} // namespace starrocks
