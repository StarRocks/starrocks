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
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsCredentialType;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.AwsInstanceProfileCredentialInfo;
import com.staros.proto.AwsSimpleCredentialInfo;
import com.staros.util.Config;

public class AwsCredentialMgr {
    public static AwsCredential getCredentialFromConfig() {
        String credenType = Config.AWS_CREDENTIAL_TYPE;
        if (credenType.equals("simple")) {
            return new AwsSimpleCredential(Config.SIMPLE_CREDENTIAL_ACCESS_KEY_ID, Config.SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET);
        } else if (credenType.equals("instance_profile")) {
            return new AwsInstanceProfileCredential();
        } else if (credenType.equals("assume_role")) {
            return new AwsAssumeIamRoleCredential(Config.ASSUME_ROLE_CREDENTIAL_ARN,
                                                  Config.ASSUME_ROLE_CREDENTIAL_EXTERNAL_ID);
        } else {
            return new AwsDefaultCredential();
        }
    }

    public static AwsCredentialInfo toProtobuf(AwsCredential credential) {
        AwsCredentialInfo.Builder builder = AwsCredentialInfo.newBuilder();
        AwsCredentialType type = credential.type();
        if (type == AwsCredentialType.DEFAULT) {
            AwsDefaultCredential defaultCredential = (AwsDefaultCredential) credential;
            builder.setDefaultCredential(defaultCredential.toProtobuf());
        } else if (type == AwsCredentialType.SIMPLE) {
            AwsSimpleCredential simpleCredential = (AwsSimpleCredential) credential;
            builder.setSimpleCredential(simpleCredential.toProtobuf());
        } else if (type == AwsCredentialType.INSTANCE_PROFILE) {
            AwsInstanceProfileCredential profileCredential = (AwsInstanceProfileCredential) credential;
            builder.setProfileCredential(profileCredential.toProtobuf());
        } else if (type == AwsCredentialType.ASSUME_ROLE) {
            AwsAssumeIamRoleCredential roleCredential = (AwsAssumeIamRoleCredential) credential;
            builder.setAssumeRoleCredential(roleCredential.toProtobuf());
        }
        return builder.build();
    }

    public static AwsCredential fromProtobuf(AwsCredentialInfo credentialInfo) {
        if (credentialInfo.hasDefaultCredential()) {
            AwsDefaultCredentialInfo defaultCredential = credentialInfo.getDefaultCredential();
            return AwsDefaultCredential.fromProtobuf(defaultCredential);
        } else if (credentialInfo.hasSimpleCredential()) {
            AwsSimpleCredentialInfo simpleCredential = credentialInfo.getSimpleCredential();
            return AwsSimpleCredential.fromProtobuf(simpleCredential);
        } else if (credentialInfo.hasProfileCredential()) {
            AwsInstanceProfileCredentialInfo profileCredential = credentialInfo.getProfileCredential();
            return AwsInstanceProfileCredential.fromProtobuf(profileCredential); 
        } else if (credentialInfo.hasAssumeRoleCredential()) {
            AwsAssumeIamRoleCredentialInfo iamCredential = credentialInfo.getAssumeRoleCredential();
            return AwsAssumeIamRoleCredential.fromProtobuf(iamCredential);
        } else {
            return null;
        }
    }
}
