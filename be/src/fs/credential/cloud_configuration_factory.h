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

#pragma once

#include <boost/lexical_cast.hpp>
#include <string>
#include <unordered_map>

#include "fs/credential/cloud_configuration.h"
#include "gen_cpp/CloudConfiguration_types.h"
#include "glog/logging.h"

namespace starrocks {
// The same as CloudConfiguration.java in FE
// Credential for AWS s3
static const std::string AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR = "aws.s3.use_aws_sdk_default_behavior";
static const std::string AWS_S3_USE_INSTANCE_PROFILE = "aws.s3.use_instance_profile";
static const std::string AWS_S3_ACCESS_KEY = "aws.s3.access_key";
static const std::string AWS_S3_SECRET_KEY = "aws.s3.secret_key";
static const std::string AWS_S3_SESSION_TOKEN = "aws.s3.session_token";
static const std::string AWS_S3_IAM_ROLE_ARN = "aws.s3.iam_role_arn";
static const std::string AWS_S3_EXTERNAL_ID = "aws.s3.external_id";
static const std::string AWS_S3_REGION = "aws.s3.region";
static const std::string AWS_S3_ENDPOINT = "aws.s3.endpoint";

// Configuration for AWS s3
/**
     * Enable S3 path style access ie disabling the default virtual hosting behaviour.
     * Useful for S3A-compliant storage providers as it removes the need to set up DNS for virtual hosting.
     * Default value: [false]
     */
static const std::string AWS_S3_ENABLE_PATH_STYLE_ACCESS = "aws.s3.enable_path_style_access";

/**
     * Enables or disables SSL connections to AWS services.
     * You must set true if you want to assume role.
     * Default value: [true]
     */
static const std::string AWS_S3_ENABLE_SSL = "aws.s3.enable_ssl";

static const std::string ALIYUN_OSS_ACCESS_KEY = "aliyun.oss.access_key";
static const std::string ALIYUN_OSS_SECRET_KEY = "aliyun.oss.secret_key";
static const std::string ALIYUN_OSS_ENDPOINT = "aliyun.oss.endpoint";

class CloudConfigurationFactory {
public:
    static const AWSCloudConfiguration create_aws(const TCloudConfiguration& t_cloud_configuration) {
        DCHECK(t_cloud_configuration.__isset.cloud_type);
        DCHECK(t_cloud_configuration.cloud_type == TCloudType::AWS);
        std::unordered_map<std::string, std::string> properties;
        _insert_properties(properties, t_cloud_configuration);

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
        aws_cloud_credential.external_id = get_or_default(properties, AWS_S3_EXTERNAL_ID, std::string());
        aws_cloud_credential.region = get_or_default(properties, AWS_S3_REGION, std::string());
        aws_cloud_credential.endpoint = get_or_default(properties, AWS_S3_ENDPOINT, std::string());

        aws_cloud_configuration.aws_cloud_credential = aws_cloud_credential;
        return aws_cloud_configuration;
    }

    // This is a reserved interface for aliyun EMR starrocks, and cannot be deleted
    static const AliyunCloudConfiguration create_aliyun(const TCloudConfiguration& t_cloud_configuration) {
        DCHECK(t_cloud_configuration.__isset.cloud_type);
        DCHECK(t_cloud_configuration.cloud_type == TCloudType::ALIYUN);
        std::unordered_map<std::string, std::string> properties;
        _insert_properties(properties, t_cloud_configuration);

        AliyunCloudConfiguration aliyun_cloud_configuration{};
        AliyunCloudCredential aliyun_cloud_credential{};

        aliyun_cloud_credential.access_key = get_or_default(properties, ALIYUN_OSS_ACCESS_KEY, std::string());
        aliyun_cloud_credential.secret_key = get_or_default(properties, ALIYUN_OSS_SECRET_KEY, std::string());
        aliyun_cloud_credential.endpoint = get_or_default(properties, ALIYUN_OSS_ENDPOINT, std::string());

        aliyun_cloud_configuration.aliyun_cloud_credential = aliyun_cloud_credential;
        return aliyun_cloud_configuration;
    }

private:
    static void _insert_properties(std::unordered_map<std::string, std::string>& properties,
                                   const TCloudConfiguration& t_cloud_configuration) {
        if (t_cloud_configuration.__isset.cloud_properties) {
            for (const auto& property : t_cloud_configuration.cloud_properties) {
                properties.insert({property.key, property.value});
            }
        } else {
            DCHECK(t_cloud_configuration.__isset.cloud_properties_v2);
            properties.insert(t_cloud_configuration.cloud_properties_v2.begin(),
                              t_cloud_configuration.cloud_properties_v2.end());
        }
    }

    template <typename ReturnType>
    static ReturnType get_or_default(const std::unordered_map<std::string, std::string>& properties,
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
};
} // namespace starrocks
