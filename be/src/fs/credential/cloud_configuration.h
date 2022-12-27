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

namespace starrocks {
class CloudCredential {
public:
    virtual ~CloudCredential() = default;
};

class AWSCloudCredential final : public CloudCredential {
public:
    bool use_aws_sdk_default_behavior;
    bool use_instance_profile;
    std::string access_key;
    std::string secret_key;
    std::string iam_role_arn;
    std::string external_id;
    std::string region;
    std::string endpoint;
};

class CloudConfiguration {
public:
    virtual ~CloudConfiguration() = default;
};

class AWSCloudConfiguration final : public CloudConfiguration {
public:
    std::shared_ptr<AWSCloudCredential> aws_cloud_credential;
    bool enable_path_style_access = false;
    bool enable_ssl = true;
};
} // namespace starrocks