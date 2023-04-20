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

package com.starrocks.storagevolume.storageparams;

import com.starrocks.storagevolume.StorageVolume;

import java.util.Map;

public class S3StorageParams implements StorageParams {
    public static String s3Region = "aws.s3.region";
    public static String s3Endpoint = "aws.s3.endpoint";
    public static String s3useDefaultBehaviour = "aws.s3.use_aws_sdk_default_behavior";
    public static String s3UseInstanceProfile = "aws.s3.use_instance_profile";
    public static String s3IamRoleArn = "aws.s3.iam_role_arn";
    public static String s3ExternalId = "aws.s3.external_id";

    private Boolean useDefaultBehaviour;
    private String region;
    private String endpoint;

    @Override
    public StorageVolume.StorageVolumeType type() {
        return StorageVolume.StorageVolumeType.S3;
    }

    public S3StorageParams(Map<String, String> params) {
        region = params.get(s3Region);
        endpoint = params.get(s3Endpoint);


    }
}
