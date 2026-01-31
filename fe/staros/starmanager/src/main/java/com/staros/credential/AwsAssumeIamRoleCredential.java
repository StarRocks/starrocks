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

package com.staros.credential;

import com.staros.proto.AwsAssumeIamRoleCredentialInfo;
import com.staros.proto.AwsCredentialType;

public class AwsAssumeIamRoleCredential implements AwsCredential {
    private String iamRoleArn;
    private String externalId;

    public AwsAssumeIamRoleCredential(String arn, String externalId) {
        this.iamRoleArn = arn;
        this.externalId = externalId;
    }

    // Return aws credential type, 'simple', 'iam role' etc
    public AwsCredentialType type() {
        return AwsCredentialType.ASSUME_ROLE;
    }

    // Serialize simple credential to protobuf
    public AwsAssumeIamRoleCredentialInfo toProtobuf() {
        AwsAssumeIamRoleCredentialInfo.Builder builder = AwsAssumeIamRoleCredentialInfo.newBuilder();
        builder.setIamRoleArn(iamRoleArn);
        builder.setExternalId(externalId);
        return builder.build();
    }

    public static AwsAssumeIamRoleCredential fromProtobuf(AwsAssumeIamRoleCredentialInfo credentialInfo) {
        String iamRoleArn = credentialInfo.getIamRoleArn();
        String externalId = credentialInfo.getExternalId();
        return new AwsAssumeIamRoleCredential(iamRoleArn, externalId);
    }
}
