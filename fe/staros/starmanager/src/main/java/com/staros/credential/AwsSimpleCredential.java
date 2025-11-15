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

import com.staros.proto.AwsCredentialType;
import com.staros.proto.AwsSimpleCredentialInfo;

public class AwsSimpleCredential implements AwsCredential {
    private String accessKey;
    private String accessKeySecret;
    private boolean encrypted;

    public AwsSimpleCredential(String accessKey, String accessKeySecret) {
        this.accessKey = accessKey;
        this.accessKeySecret = accessKeySecret;
        this.encrypted = false;
    }

    public AwsSimpleCredential(String accessKey, String accessKeySecret, boolean encrypted) {
        this.accessKey = accessKey;
        this.accessKeySecret = accessKeySecret;
        this.encrypted = encrypted;
    }

    // Return aws credential type, 'simple', 'iam role' etc
    public AwsCredentialType type() {
        return AwsCredentialType.SIMPLE;
    }

    // Serialize simple credential to protobuf
    public AwsSimpleCredentialInfo toProtobuf() {
        AwsSimpleCredentialInfo.Builder builder = AwsSimpleCredentialInfo.newBuilder();
        builder.setAccessKey(accessKey);
        builder.setAccessKeySecret(accessKeySecret);
        builder.setEncrypted(encrypted);
        return builder.build();
    }

    public static AwsSimpleCredential fromProtobuf(AwsSimpleCredentialInfo credentialInfo) {
        String ak = credentialInfo.getAccessKey();
        String sk = credentialInfo.getAccessKeySecret();
        boolean encrypted = credentialInfo.getEncrypted();
        return new AwsSimpleCredential(ak, sk, encrypted);
    }
}
