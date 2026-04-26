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

#include "fs/credential/cloud_configuration_factory.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"

namespace starrocks {

class CloudConfigurationFactoryTest : public ::testing::Test {};

TEST_F(CloudConfigurationFactoryTest, test_create_azure) {
    TCloudConfiguration t_cloud_configuration;
    t_cloud_configuration.__set_cloud_type(TCloudType::AZURE);

    {
        std::map<std::string, std::string> properties;
        properties.emplace(AZURE_BLOB_SHARED_KEY, "shared_key");
        t_cloud_configuration.__set_cloud_properties(properties);

        const auto& cloud_configuration = CloudConfigurationFactory::create_azure(t_cloud_configuration);
        const auto& azure_cloud_credential = cloud_configuration.azure_cloud_credential;

        EXPECT_STREQ(azure_cloud_credential.shared_key.c_str(), "shared_key");
    }

    {
        std::map<std::string, std::string> properties;
        properties.emplace(AZURE_BLOB_SAS_TOKEN, "sas_token");
        t_cloud_configuration.__set_cloud_properties(properties);

        const auto& cloud_configuration = CloudConfigurationFactory::create_azure(t_cloud_configuration);
        const auto& azure_cloud_credential = cloud_configuration.azure_cloud_credential;

        EXPECT_STREQ(azure_cloud_credential.sas_token.c_str(), "sas_token");
    }

    {
        std::map<std::string, std::string> properties;
        properties.emplace(AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id");
        properties.emplace(AZURE_BLOB_OAUTH2_CLIENT_SECRET, "client_secret");
        properties.emplace(AZURE_BLOB_OAUTH2_TENANT_ID, "tenant_id");
        t_cloud_configuration.__set_cloud_properties(properties);

        const auto& cloud_configuration = CloudConfigurationFactory::create_azure(t_cloud_configuration);
        const auto& azure_cloud_credential = cloud_configuration.azure_cloud_credential;

        EXPECT_STREQ(azure_cloud_credential.client_id.c_str(), "client_id");
        EXPECT_STREQ(azure_cloud_credential.client_secret.c_str(), "client_secret");
        EXPECT_STREQ(azure_cloud_credential.tenant_id.c_str(), "tenant_id");
    }

    {
        std::map<std::string, std::string> properties;
        properties.emplace(AZURE_BLOB_OAUTH2_CLIENT_ID, "client_id");
        t_cloud_configuration.__set_cloud_properties(properties);

        const auto& cloud_configuration = CloudConfigurationFactory::create_azure(t_cloud_configuration);
        const auto& azure_cloud_credential = cloud_configuration.azure_cloud_credential;

        EXPECT_STREQ(azure_cloud_credential.client_id.c_str(), "client_id");
    }
}

} // namespace starrocks
