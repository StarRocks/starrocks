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

    AzureCloudConfiguration azure_cloud_configuration{};
    azure_cloud_configuration.azure_cloud_credential = azure_cloud_credential;
    return azure_cloud_configuration;
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
