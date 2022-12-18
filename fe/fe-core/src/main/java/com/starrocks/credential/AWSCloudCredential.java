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

package com.starrocks.credential;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.base.Preconditions;
import com.starrocks.credential.provider.AssumedRoleCredentialProvider;
import com.starrocks.thrift.TAWSCloudCredential;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;

import java.util.UUID;

/**
 * Authenticate process (It's fake code):
 * Credentials credentials = null;
 * if (useAWSSDKDefaultBehavior) {
 *     return new DefaultAWSCredentialsProviderChain();
 * } else if (useInstanceProfile) {
 *      credentials = GetInstanceProfileCredentials();
 *     if (useIamRoleArn) {
 *        credentials = GetAssumeRole(credentials, iamRoleArn, externalId);
 *     }
 *     return credentials;
 * } else if (exist(accessKey) && exist(secretKey)) {
 *     credentials = GetAKSKCredentials(accessKey, secretKey);
 *     if (useIamRoleArn) {
 *         credentials = GetAssumeRole(credentials, iamRoleArn, externalId);
 *     }
 *     return credentials;
 * } else {
 *     // Unreachable!!!!
 *     // We don't allowed to create anonymous credentials, we will check it in validate() method.
 *     // If user want to use anonymous credentials, they just don't set cloud credential directly.
 * }
 */
public class AWSCloudCredential implements CloudCredential {

    private final boolean useAWSSDKDefaultBehavior;

    private final boolean useInstanceProfile;

    private final String accessKey;

    private final String secretKey;

    private final String iamRoleArn;

    private final String externalId;

    private final String region;

    private final String endpoint;

    protected AWSCloudCredential(boolean useAWSSDKDefaultBehavior, boolean useInstanceProfile, String accessKey,
                              String secretKey, String iamRoleArn, String externalId, String region, String endpoint) {
        Preconditions.checkNotNull(accessKey);
        Preconditions.checkNotNull(secretKey);
        Preconditions.checkNotNull(iamRoleArn);
        Preconditions.checkNotNull(externalId);
        Preconditions.checkNotNull(region);
        Preconditions.checkNotNull(endpoint);
        this.useAWSSDKDefaultBehavior = useAWSSDKDefaultBehavior;
        this.useInstanceProfile = useInstanceProfile;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.iamRoleArn = iamRoleArn;
        this.externalId = externalId;
        this.region = region;
        this.endpoint = endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public AWSCredentialsProvider generateAWSCredentialsProvider() {
        if (useAWSSDKDefaultBehavior) {
            return new DefaultAWSCredentialsProviderChain();
        }

        AWSCredentialsProvider awsCredentialsProvider = getBaseAWSCredentialsProvider();
        if (!iamRoleArn.isEmpty()) {
            // Generate random session name
            String sessionName = UUID.randomUUID().toString();
            STSAssumeRoleSessionCredentialsProvider.Builder builder =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(iamRoleArn, sessionName);
            if (!externalId.isEmpty()) {
                builder.withExternalId(externalId);
            }
            AWSSecurityTokenService token =
                    AWSSecurityTokenServiceClientBuilder.standard().withCredentials(awsCredentialsProvider)
                            .build();
            builder.withStsClient(token);
            awsCredentialsProvider = builder.build();
        }
        return awsCredentialsProvider;
    }

    private AWSCredentialsProvider getBaseAWSCredentialsProvider() {
        if (useInstanceProfile) {
            return new InstanceProfileCredentialsProvider(true);
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        } else {
            Preconditions.checkArgument(false, "Unreachable");
            return new AnonymousAWSCredentialsProvider();
        }
    }

    @Override
    public void setConfiguration(Configuration configuration) {
        if (useAWSSDKDefaultBehavior) {
            configuration.set("fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        } else if (useInstanceProfile) {
            if (!iamRoleArn.isEmpty()) {
                configuration.set("fs.s3a.assumed.role.credentials.provider",
                        "com.amazonaws.auth.InstanceProfileCredentialsProvider");
                // Original "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider" don't support external id,
                // so we use our own AssumedRoleCredentialProvider.
                configuration.set("fs.s3a.aws.credentials.provider",
                        "com.starrocks.credential.provider.AssumedRoleCredentialProvider");
                configuration.set("fs.s3a.assumed.role.arn", iamRoleArn);
                configuration.set(AssumedRoleCredentialProvider.CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID, externalId);
            } else {
                configuration.set("fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.InstanceProfileCredentialsProvider");
            }
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            configuration.set("fs.s3a.access.key", accessKey);
            configuration.set("fs.s3a.secret.key", secretKey);
            if (!iamRoleArn.isEmpty()) {
                configuration.set("fs.s3a.assumed.role.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
                // Original "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider" don't support external id,
                // so we use our own AssumedRoleCredentialProvider.
                configuration.set("fs.s3a.aws.credentials.provider",
                        "com.starrocks.credential.provider.AssumedRoleCredentialProvider");
                configuration.set("fs.s3a.assumed.role.arn", iamRoleArn);
                configuration.set(AssumedRoleCredentialProvider.CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID, externalId);
            } else {
                configuration.set("fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            }
        } else {
            Preconditions.checkArgument(false, "Unreachable");
        }
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        if (!region.isEmpty()) {
            configuration.set("fs.s3a.endpoint.region", region);
        }
        if (!endpoint.isEmpty()) {
            configuration.set("fs.s3a.endpoint", endpoint);
        }
    }

    @Override
    public boolean validate() {
        if (useAWSSDKDefaultBehavior) {
            return true;
        }

        if (useInstanceProfile) {
            return true;
        }

        if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        TAWSCloudCredential tAWSCloudCredential = new TAWSCloudCredential();
        tAWSCloudCredential.setUse_aws_sdk_default_behavior(useAWSSDKDefaultBehavior);
        tAWSCloudCredential.setUse_instance_profile(useInstanceProfile);

        if (!accessKey.isEmpty()) {
            tAWSCloudCredential.setAccess_key(accessKey);
        }
        if (!secretKey.isEmpty()) {
            tAWSCloudCredential.setSecret_key(secretKey);
        }
        if (!iamRoleArn.isEmpty()) {
            tAWSCloudCredential.setIam_role_arn(iamRoleArn);
        }
        if (!externalId.isEmpty()) {
            tAWSCloudCredential.setExternal_id(externalId);
        }
        if (!region.isEmpty()) {
            tAWSCloudCredential.setRegion(region);
        }
        if (!endpoint.isEmpty()) {
            tAWSCloudCredential.setEndpoint(endpoint);
        }

        tCloudConfiguration.setAws_cloud_credential(tAWSCloudCredential);
    }

    @Override
    public String toString() {
        return "AWSCloudCredential{" +
                "useAWSSDKDefaultBehavior=" + useAWSSDKDefaultBehavior +
                ", useInstanceProfile=" + useInstanceProfile +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", iamRoleArn='" + iamRoleArn + '\'' +
                ", externalId='" + externalId + '\'' +
                ", region='" + region + '\'' +
                ", endpoint='" + endpoint + '\'' +
                '}';
    }
}
