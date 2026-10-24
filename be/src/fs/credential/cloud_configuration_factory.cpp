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

#include <glog/logging.h>

#include <boost/lexical_cast.hpp>

namespace starrocks {

const AWSCloudConfiguration CloudConfigurationFactory::create_aws(const TCloudConfiguration& t_cloud_configuration) {
    DCHECK(t_cloud_configuration.__isset.cloud_type);
    DCHECK(t_cloud_configuration.cloud_type == TCloudType::AWS);
    std::map<std::string, std::string> properties{};
    if (t_cloud_configuration.__isset.cloud_properties) {
        properties = t_cloud_configuration.cloud_properties;
    }

    AWSCloudConfiguration aws_cloud_configuration{};
    AWSCloudCredential aws_cloud_credential{};

    // Set aws cloud configuration first
    aws_cloud_configuration.enable_path_style_access =
            get_or_default(properties, AWS_S3_ENABLE_PATH_STYLE_ACCESS, false);
    aws_cloud_configuration.enable_ssl = get_or_default(properties, AWS_S3_ENABLE_SSL, true);

    // Set aws cloud credential next
    aws_cloud_credential.use_aws_sdk_default_behavior =
            get_or_default(properties, AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, false);
    aws_cloud_credential.use_instance_profile = get_or_default(properties, AWS_S3_USE_INSTANCE_PROFILE, false);
    aws_cloud_credential.use_web_identity_profile =
            get_or_default(properties, AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE, false);
    aws_cloud_credential.access_key = get_or_default(properties, AWS_S3_ACCESS_KEY, std::string());
    aws_cloud_credential.secret_key = get_or_default(properties, AWS_S3_SECRET_KEY, std::string());
    aws_cloud_credential.session_token = get_or_default(properties, AWS_S3_SESSION_TOKEN, std::string());
    aws_cloud_credential.iam_role_arn = get_or_default(properties, AWS_S3_IAM_ROLE_ARN, std::string());
    aws_cloud_credential.sts_region = get_or_default(properties, AWS_S3_STS_REGION, std::string());
    aws_cloud_credential.sts_endpoint = get_or_default(properties, AWS_S3_STS_ENDPOINT, std::string());
    aws_cloud_credential.external_id = get_or_default(properties, AWS_S3_EXTERNAL_ID, std::string());
    aws_cloud_credential.region = get_or_default(properties, AWS_S3_REGION, std::string());
    aws_cloud_credential.endpoint = get_or_default(properties, AWS_S3_ENDPOINT, std::string());

    aws_cloud_configuration.aws_cloud_credential = aws_cloud_credential;
    return aws_cloud_configuration;
}

const AliyunCloudConfiguration CloudConfigurationFactory::create_aliyun(
        const TCloudConfiguration& t_cloud_configuration) {
    DCHECK(t_cloud_configuration.__isset.cloud_type);
    DCHECK(t_cloud_configuration.cloud_type == TCloudType::ALIYUN);
    std::map<std::string, std::string> properties{};
    if (t_cloud_configuration.__isset.cloud_properties) {
        properties = t_cloud_configuration.cloud_properties;
    }

    AliyunCloudConfiguration aliyun_cloud_configuration{};
    AliyunCloudCredential aliyun_cloud_credential{};

    aliyun_cloud_credential.access_key = get_or_default(properties, ALIYUN_OSS_ACCESS_KEY, std::string());
    aliyun_cloud_credential.secret_key = get_or_default(properties, ALIYUN_OSS_SECRET_KEY, std::string());
    aliyun_cloud_credential.endpoint = get_or_default(properties, ALIYUN_OSS_ENDPOINT, std::string());

    aliyun_cloud_configuration.aliyun_cloud_credential = aliyun_cloud_credential;
    return aliyun_cloud_configuration;
}

const AzureCloudConfiguration CloudConfigurationFactory::create_azure(
        const TCloudConfiguration& t_cloud_configuration) {
    DCHECK(t_cloud_configuration.__isset.cloud_type);
    DCHECK(t_cloud_configuration.cloud_type == TCloudType::AZURE);

    std::map<std::string, std::string> properties{};
    if (t_cloud_configuration.__isset.cloud_properties) {
        properties = t_cloud_configuration.cloud_properties;
    }

    AzureCloudCredential azure_cloud_credential{};
    azure_cloud_credential.shared_key = get_or_default(properties, AZURE_BLOB_SHARED_KEY, std::string());
    azure_cloud_credential.sas_token = get_or_default(properties, AZURE_BLOB_SAS_TOKEN, std::string());
    azure_cloud_credential.client_id = get_or_default(properties, AZURE_BLOB_OAUTH2_CLIENT_ID, std::string());
    azure_cloud_credential.client_secret = get_or_default(properties, AZURE_BLOB_OAUTH2_CLIENT_SECRET, std::string());
    azure_cloud_credential.tenant_id = get_or_default(properties, AZURE_BLOB_OAUTH2_TENANT_ID, std::string());

    AzureCloudConfiguration azure_cloud_configuration{};
    azure_cloud_configuration.azure_cloud_credential = azure_cloud_credential;
    return azure_cloud_configuration;
}

StatusOr<AzureCloudConfiguration> CloudConfigurationFactory::create_adls2(const TCloudConfiguration& configuration,
                                                                          const std::string& dfs_host) {
    if (!configuration.__isset.cloud_type || configuration.cloud_type != TCloudType::AZURE) {
        return Status::InvalidArgument("Native ADLS requires Azure cloud configuration");
    }
    const auto& properties = configuration.cloud_properties;
    auto get = [&](const std::string& key, std::string fallback = "") {
        auto it = properties.find(key + "." + dfs_host);
        if (it != properties.end()) return it->second;
        it = properties.find(key);
        return it != properties.end() ? it->second : fallback;
    };
    AzureCloudConfiguration result;
    auto& credential = result.azure_cloud_credential;
    const auto type = get("fs.azure.account.auth.type", "SharedKey");
    if (type == "SharedKey") {
        if (!get("fs.azure.account.keyprovider").empty()) {
            return Status::NotSupported("Native ADLS does not support custom key providers");
        }
        credential.shared_key = get("fs.azure.account.key");
        if (credential.shared_key.empty()) return Status::InvalidArgument("Missing native ADLS shared key");
    } else if (type == "SAS") {
        const auto provider = get("fs.azure.sas.token.provider.type");
        if (!provider.empty() && provider != "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider") {
            return Status::NotSupported("Native ADLS supports fixed SAS tokens only");
        }
        credential.sas_token = get("fs.azure.sas.fixed.token");
        if (!credential.sas_token.empty() && credential.sas_token.front() == '?') credential.sas_token.erase(0, 1);
        if (credential.sas_token.empty()) return Status::InvalidArgument("Missing native ADLS SAS token");
    } else if (type == "OAuth") {
        credential.client_id = get("fs.azure.account.oauth2.client.id");
        if (credential.client_id.empty()) return Status::InvalidArgument("Missing native ADLS OAuth client ID");
        const auto provider = get("fs.azure.account.oauth.provider.type");
        const std::string prefix = "org.apache.hadoop.fs.azurebfs.oauth2.";
        if (provider == prefix + "ClientCredsTokenProvider") {
            credential.client_secret = get("fs.azure.account.oauth2.client.secret");
            const auto endpoint = get("fs.azure.account.oauth2.client.endpoint");
            const auto host_end = endpoint.find('/', 8);
            const auto tenant_end =
                    host_end == std::string::npos ? std::string::npos : endpoint.find('/', host_end + 1);
            if (endpoint.rfind("https://", 0) != 0 || endpoint.find_first_of("?#@") != std::string::npos ||
                host_end == std::string::npos || host_end == 8 || tenant_end == std::string::npos ||
                tenant_end == host_end + 1 ||
                (endpoint.substr(tenant_end) != "/oauth2/token" &&
                 endpoint.substr(tenant_end) != "/oauth2/v2.0/token") ||
                credential.client_secret.empty()) {
                return Status::InvalidArgument("Native ADLS requires a client secret and HTTPS tenant OAuth endpoint");
            }
            credential.tenant_id = endpoint.substr(host_end + 1, tenant_end - host_end - 1);
            credential.authority_host = endpoint.substr(0, host_end);
        } else if (provider == prefix + "WorkloadIdentityTokenProvider") {
            credential.token_file = get("fs.azure.account.oauth2.token.file");
            credential.tenant_id = get("fs.azure.account.oauth2.msi.tenant");
            if (credential.token_file.empty() || credential.tenant_id.empty()) {
                return Status::InvalidArgument("Native ADLS workload identity requires a token file and tenant ID");
            }
            std::string authority = "https://login.microsoftonline.com/";
            if (dfs_host.ends_with(".chinacloudapi.cn")) authority = "https://login.chinacloudapi.cn/";
            if (dfs_host.ends_with(".usgovcloudapi.net")) authority = "https://login.microsoftonline.us/";
            credential.authority_host = get("fs.azure.account.oauth2.msi.authority", authority);
            if (credential.authority_host.rfind("https://", 0) != 0) {
                return Status::InvalidArgument("Native ADLS requires an HTTPS authority host");
            }
        } else if (provider == prefix + "MsiTokenProvider") {
            if (!get("fs.azure.account.oauth2.msi.endpoint").empty()) {
                return Status::NotSupported("Native ADLS does not support a custom MSI endpoint");
            }
        } else {
            return Status::NotSupported("Unsupported native ADLS OAuth provider");
        }
    } else {
        return Status::NotSupported("Unsupported native ADLS authentication type");
    }
    return result;
}

template <typename ReturnType>
ReturnType CloudConfigurationFactory::get_or_default(const std::map<std::string, std::string>& properties,
                                                     const std::string& key, ReturnType default_value) {
    auto it = properties.find(key);
    if (it != properties.end()) {
        std::string value = it->second;
        if (std::is_same<bool, ReturnType>::value) {
            // Change value to "0" or "1" before use boost::lexical_cast()
            value = (value == "true") ? "1" : "0";
        }
        return boost::lexical_cast<ReturnType>(value);
    } else {
        return default_value;
    }
}

} // namespace starrocks
