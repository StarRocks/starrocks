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

package com.starrocks.storagevolume.credential.aws;

public class AWSAssumeIamRoleCredential implements AWSCredential {
    private String iamRoleArn;
    private String externalId;

    public AWSAssumeIamRoleCredential(String iamRoleArn, String externalId) {
        this.iamRoleArn = iamRoleArn;
        this.externalId = externalId;
    }

    @Override
    public AWSCredentialType type() {
        return AWSCredentialType.ASSUME_ROLE;
    }

    public String getIamRoleArn() {
        return iamRoleArn;
    }

    public String getExternalId() {
        return externalId;
    }
}
