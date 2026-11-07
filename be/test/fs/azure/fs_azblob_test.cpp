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
#include "fs/fs_factory.h"
#include "fs/fs_registry.h"

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

        ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateUniqueFromString(uri, options));
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

        ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateUniqueFromString(uri, options));
        ASSERT_EQ(fs->type(), FileSystem::AZBLOB);

        ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(uri));
        EXPECT_TRUE(dynamic_cast<RandomAccessFile*>(file.get()) != nullptr);
    }
}

TEST_F(AzBlobFileSystemTest, test_adls2_opt_in_routing_and_credentials) {
    const std::string uri = "abfss://container@account.dfs.core.windows.net/table/file.parquet";
    const auto provider = fs::new_azblob_file_system_provider();
    TCloudConfiguration configuration;
    configuration.__set_cloud_type(TCloudType::AZURE);
    configuration.__set_cloud_properties({{"fs.azure.account.auth.type.account.dfs.core.windows.net", "SAS"},
                                          {"fs.azure.sas.fixed.token.account.dfs.core.windows.net", "sig=test"}});
    FSOptions options(&configuration);
    EXPECT_FALSE(provider.match_unique(uri, options));
    configuration.__set_azure_use_native_sdk(true);
    EXPECT_TRUE(provider.match_unique(uri, options));
    ASSIGN_OR_ABORT(auto fs, provider.create_unique(uri, options));
    EXPECT_EQ(FileSystem::AZBLOB, fs->type());
    ASSIGN_OR_ABORT(auto file, fs->new_random_access_file(uri));
    EXPECT_NE(nullptr, file.get());
    EXPECT_TRUE(fs->new_writable_file(uri).status().is_not_supported());
    // Never silently fall back to the process's default Azure credential for another account.
    EXPECT_FALSE(fs->new_random_access_file("abfss://container@other.dfs.core.windows.net/file").ok());
    configuration.__set_azure_use_native_sdk(false);
    EXPECT_FALSE(provider.match_unique(uri, options));
}

} // namespace starrocks
