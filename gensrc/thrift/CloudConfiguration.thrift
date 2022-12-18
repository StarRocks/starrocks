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

namespace cpp starrocks
namespace java com.starrocks.thrift

enum TCloudType {
    AWS
}

struct TAWSCloudCredential {
    1: optional bool use_aws_sdk_default_behavior;
    2: optional bool use_instance_profile;
    3: optional string access_key;
    4: optional string secret_key;
    5: optional string iam_role_arn;
    6: optional string external_id;
    7: optional string region;
    8: optional string endpoint;
}

struct TCloudProperty {
    1: required string key;
    2: required string value;
}

struct TCloudConfiguration {
    1: optional TCloudType cloud_type;
    2: optional list<TCloudProperty> cloud_properties;
    3: optional TAWSCloudCredential aws_cloud_credential; 
}
