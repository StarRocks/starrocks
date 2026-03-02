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

#include <string>
#include <unordered_map>

#include "fs/credential/cloud_configuration.h"
#include "gen_cpp/CloudConfiguration_types.h"

namespace starrocks {
// The same as CloudConfiguration.java in FE
// Credential for AWS s3
static const std::string AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR = "aws.s3.use_aws_sdk_default_behavior";
static const std::string AWS_S3_USE_INSTANCE_PROFILE = "aws.s3.use_instance_profile";
static const std::string AWS_S3_ACCESS_KEY = "aws.s3.access_key";
static const std::string AWS_S3_SECRET_KEY = "aws.s3.secret_key";
static const std::string AWS_S3_SESSION_TOKEN = "aws.s3.session_token";
static const std::string AWS_S3_IAM_ROLE_ARN = "aws.s3.iam_role_arn";
static const std::string AWS_S3_STS_REGION = "aws.s3.sts.region";
static const std::string AWS_S3_STS_ENDPOINT = "aws.s3.sts.endpoint";
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

// Configuration for Azure
// Currently only supported for Azure Blob Storage
static const std::string AZURE_BLOB_SHARED_KEY = "azure.blob.shared_key";
static const std::string AZURE_BLOB_SAS_TOKEN = "azure.blob.sas_token";
static const std::string AZURE_BLOB_OAUTH2_CLIENT_ID = "azure.blob.oauth2_client_id";
static const std::string AZURE_BLOB_OAUTH2_CLIENT_SECRET = "azure.blob.oauth2_client_secret";
static const std::string AZURE_BLOB_OAUTH2_TENANT_ID = "azure.blob.oauth2_tenant_id";

// Credentials for Huawei OBS
static const std::string HUAWEI_OBS_ACCESS_KEY = "fs.obs.access_key";
static const std::string HUAWEI_OBS_SECRET_KEY = "fs.obs.secret_key";
static const std::string HUAWEI_OBS_ACCESS_KEY_DOT = "fs.obs.access.key";
static const std::string HUAWEI_OBS_SECRET_KEY_DOT = "fs.obs.secret.key";
static const std::string HUAWEI_OBS_ENDPOINT = "fs.obs.endpoint";

class CloudConfigurationFactory {
public:
    static const AWSCloudConfiguration create_aws(const TCloudConfiguration& t_cloud_configuration);

    // This is a reserved interface for aliyun EMR starrocks, and cannot be deleted
    static const AliyunCloudConfiguration create_aliyun(const TCloudConfiguration& t_cloud_configuration);

    static const AzureCloudConfiguration create_azure(const TCloudConfiguration& t_cloud_configuration);

private:
    template <typename ReturnType>
    static ReturnType get_or_default(const std::map<std::string, std::string>& properties, const std::string& key,
                                     ReturnType default_value);
};
} // namespace starrocks
