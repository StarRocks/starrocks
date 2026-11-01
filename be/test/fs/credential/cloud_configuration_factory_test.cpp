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

TEST_F(CloudConfigurationFactoryTest, test_create_aws_web_identity) {
    TCloudConfiguration t_cloud_configuration;
    t_cloud_configuration.__set_cloud_type(TCloudType::AWS);

    std::map<std::string, std::string> properties;
    properties.emplace(AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE, "true");
    t_cloud_configuration.__set_cloud_properties(properties);

    const auto& cloud_configuration = CloudConfigurationFactory::create_aws(t_cloud_configuration);
    const auto& cred = cloud_configuration.aws_cloud_credential;

    EXPECT_TRUE(cred.use_web_identity_profile);
    EXPECT_FALSE(cred.use_instance_profile);
    EXPECT_FALSE(cred.use_aws_sdk_default_behavior);
}

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

TEST_F(CloudConfigurationFactoryTest, test_create_adls2_account_scope) {
    TCloudConfiguration configuration;
    configuration.__set_cloud_type(TCloudType::AZURE);
    const std::string host = "account.dfs.core.windows.net";
    configuration.__set_cloud_properties(
            {{"fs.azure.account.key", "global"}, {"fs.azure.account.key." + host, "scoped"}});
    auto result = CloudConfigurationFactory::create_adls2(configuration, host);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("scoped", result->azure_cloud_credential.shared_key);
    EXPECT_EQ("global", CloudConfigurationFactory::create_adls2(configuration, "other.dfs.core.windows.net")
                                ->azure_cloud_credential.shared_key);
    configuration.__set_cloud_properties(
            {{"fs.azure.account.auth.type." + host, "SAS"}, {"fs.azure.sas.fixed.token." + host, "?sig=scoped"}});
    result = CloudConfigurationFactory::create_adls2(configuration, host);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("sig=scoped", result->azure_cloud_credential.sas_token);
    EXPECT_FALSE(CloudConfigurationFactory::create_adls2(configuration, "other.dfs.core.windows.net").ok());
}

TEST_F(CloudConfigurationFactoryTest, test_create_adls2_oauth) {
    TCloudConfiguration configuration;
    configuration.__set_cloud_type(TCloudType::AZURE);
    const std::string host = "account.dfs.core.windows.net";
    const std::string provider = "org.apache.hadoop.fs.azurebfs.oauth2.";
    std::map<std::string, std::string> properties = {
            {"fs.azure.account.auth.type", "OAuth"},
            {"fs.azure.account.oauth.provider.type", provider + "ClientCredsTokenProvider"},
            {"fs.azure.account.oauth2.client.id", "client"},
            {"fs.azure.account.oauth2.client.secret", "secret"},
            {"fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/tenant/oauth2/token"}};
    configuration.__set_cloud_properties(properties);
    auto result = CloudConfigurationFactory::create_adls2(configuration, host);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("client", result->azure_cloud_credential.client_id);
    EXPECT_EQ("secret", result->azure_cloud_credential.client_secret);
    EXPECT_EQ("tenant", result->azure_cloud_credential.tenant_id);
    EXPECT_EQ("https://login.microsoftonline.com", result->azure_cloud_credential.authority_host);

    properties["fs.azure.account.oauth.provider.type"] = provider + "WorkloadIdentityTokenProvider";
    properties["fs.azure.account.oauth2.token.file"] = "/var/run/token";
    properties["fs.azure.account.oauth2.msi.tenant"] = "tenant";
    configuration.__set_cloud_properties(properties);
    result = CloudConfigurationFactory::create_adls2(configuration, host);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("/var/run/token", result->azure_cloud_credential.token_file);
    EXPECT_TRUE(result->azure_cloud_credential.client_secret.empty());
    auto other = result->azure_cloud_credential;
    other.token_file = "/var/run/other-token";
    EXPECT_FALSE(other == result->azure_cloud_credential);
    other = result->azure_cloud_credential;
    other.authority_host = "https://login.microsoftonline.us/";
    EXPECT_FALSE(other == result->azure_cloud_credential);

    properties["fs.azure.account.oauth.provider.type"] = provider + "MsiTokenProvider";
    configuration.__set_cloud_properties(properties);
    result = CloudConfigurationFactory::create_adls2(configuration, host);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ("client", result->azure_cloud_credential.client_id);
    EXPECT_TRUE(result->azure_cloud_credential.token_file.empty());
    EXPECT_TRUE(result->azure_cloud_credential.client_secret.empty());
}

TEST_F(CloudConfigurationFactoryTest, test_create_adls2_reject_unsupported_credentials) {
    TCloudConfiguration configuration;
    configuration.__set_cloud_type(TCloudType::AZURE);
    const std::string host = "account.dfs.core.windows.net";
    EXPECT_FALSE(CloudConfigurationFactory::create_adls2(configuration, host).ok());
    configuration.__set_cloud_properties({{"fs.azure.account.auth.type", "Custom"}});
    EXPECT_FALSE(CloudConfigurationFactory::create_adls2(configuration, host).ok());
    configuration.__set_cloud_properties({{"fs.azure.account.auth.type", "OAuth"},
                                          {"fs.azure.account.oauth2.client.id", "client"},
                                          {"fs.azure.account.oauth.provider.type", "custom.Provider"}});
    EXPECT_FALSE(CloudConfigurationFactory::create_adls2(configuration, host).ok());
    configuration.__set_cloud_properties(
            {{"fs.azure.account.auth.type", "SAS"}, {"fs.azure.sas.token.provider.type", "custom.Provider"}});
    EXPECT_FALSE(CloudConfigurationFactory::create_adls2(configuration, host).ok());
}

} // namespace starrocks
