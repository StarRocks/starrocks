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

import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.storagevolume.credential.aws.AWSCredential;
import com.starrocks.storagevolume.credential.aws.AWSDefaultCredential;
import com.starrocks.storagevolume.credential.aws.AWSSimpleCredential;
import com.starrocks.storagevolume.credential.aws.AwsAssumeIamRoleCredential;
import com.starrocks.storagevolume.credential.aws.AwsInstanceProfileCredential;

import java.util.Map;


public class S3StorageParams implements StorageParams {
    private String region;
    private String endpoint;
    private AWSCredential credential;

    @Override
    public StorageVolume.StorageVolumeType type() {
        return StorageVolume.StorageVolumeType.S3;
    }

    public S3StorageParams(Map<String, String> params) {
        region = params.get(CloudConfigurationConstants.AWS_S3_REGION);
        endpoint = params.get(CloudConfigurationConstants.AWS_S3_ENDPOINT);
        credential = buildCredential(params);
    }

    private AWSCredential buildCredential(Map<String, String> params) {
        boolean useAWSSDKDefaultBehavior =
                Boolean.parseBoolean(params.getOrDefault(
                        CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false"));
        if (useAWSSDKDefaultBehavior) {
            return new AWSDefaultCredential();
        }

        boolean useInstanceProfile =
                Boolean.parseBoolean(params.getOrDefault(
                        CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "false"));
        if (useInstanceProfile) {
            if (params.containsKey(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN)) {
                return new AwsAssumeIamRoleCredential(params.getOrDefault(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN, ""),
                        params.getOrDefault(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID, ""));
            }
            return new AwsInstanceProfileCredential();
        }

        return new AWSSimpleCredential(params.getOrDefault(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, ""),
                params.getOrDefault(CloudConfigurationConstants.AWS_S3_SECRET_KEY, ""));
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public AWSCredential getCredential() {
        return credential;
    }
}
