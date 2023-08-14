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

package com.starrocks.credential.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.base.Preconditions;
import com.staros.proto.AwsAssumeIamRoleCredentialInfo;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.AwsInstanceProfileCredentialInfo;
import com.staros.proto.AwsSimpleCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudCredential;
import com.starrocks.credential.provider.AssumedRoleCredentialProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;

import java.util.Map;
import java.util.UUID;

/**
 * Authenticating process (It's a pseudocode code):
 * Credentials credentials = null;
 * if (useAWSSDKDefaultBehavior) {
 *   return new DefaultAWSCredentialsProviderChain();
 * } else if (useInstanceProfile) {
 *   credentials = GetInstanceProfileCredentials();
 *   if (useIamRoleArn) {
 *     credentials = GetAssumeRole(credentials, iamRoleArn, externalId);
 *   }
 *   return credentials;
 * } else if (exist(accessKey) && exist(secretKey)) {
 *   // sessionToken is optional, if you use sessionToken, GetAKSKCredentials() will return a temporary credential
 *   credentials = GetAKSKCredentials(accessKey, secretKey, [sessionToken]);
 *   if (useIamRoleArn) {
 *     credentials = GetAssumeRole(credentials, iamRoleArn, externalId);
 *   }
 *   return credentials;
 * } else {
 *   // Unreachable!!!!
 *   // We don't allowed to create anonymous credentials, we will check it in validate() method.
 *   // If user want to use anonymous credentials, they just don't set cloud credential directly.
 * }
 */
public class AWSCloudCredential implements CloudCredential {

    private final boolean useAWSSDKDefaultBehavior;

    private final boolean useInstanceProfile;

    private final String accessKey;

    private final String secretKey;

    private final String sessionToken;

    private final String iamRoleArn;

    private final String externalId;

    private final String region;

    private final String endpoint;

    protected AWSCloudCredential(boolean useAWSSDKDefaultBehavior, boolean useInstanceProfile, String accessKey,
                                 String secretKey, String sessionToken, String iamRoleArn, String externalId, String region,
                                 String endpoint) {
        Preconditions.checkNotNull(accessKey);
        Preconditions.checkNotNull(secretKey);
        Preconditions.checkNotNull(sessionToken);
        Preconditions.checkNotNull(iamRoleArn);
        Preconditions.checkNotNull(externalId);
        Preconditions.checkNotNull(region);
        Preconditions.checkNotNull(endpoint);
        this.useAWSSDKDefaultBehavior = useAWSSDKDefaultBehavior;
        this.useInstanceProfile = useInstanceProfile;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
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

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public AWSCredentialsProvider generateAWSCredentialsProvider() {
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
        if (useAWSSDKDefaultBehavior) {
            return new DefaultAWSCredentialsProviderChain();
        } else if (useInstanceProfile) {
            return new InstanceProfileCredentialsProvider(true);
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            if (!sessionToken.isEmpty()) {
                // Build temporary aws credentials with session token
                AWSCredentials awsCredentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
                return new AWSStaticCredentialsProvider(awsCredentials);
            } else {
                return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
            }
        } else {
            Preconditions.checkArgument(false, "Unreachable");
            return new AnonymousAWSCredentialsProvider();
        }
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        if (useAWSSDKDefaultBehavior) {
            if (!iamRoleArn.isEmpty()) {
                configuration.set("fs.s3a.assumed.role.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
                configuration.set("fs.s3a.aws.credentials.provider",
                        "com.starrocks.credential.provider.AssumedRoleCredentialProvider");
                configuration.set("fs.s3a.assumed.role.arn", iamRoleArn);
                configuration.set(AssumedRoleCredentialProvider.CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID, externalId);
            } else {
                configuration.set("fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");
            }
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
                if (!sessionToken.isEmpty()) {
                    configuration.set("fs.s3a.session.token", sessionToken);
                    configuration.set("fs.s3a.aws.credentials.provider",
                            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
                } else {
                    configuration.set("fs.s3a.aws.credentials.provider",
                            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
                }
            }
        } else {
            Preconditions.checkArgument(false, "Unreachable");
        }
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
    public void toThrift(Map<String, String> properties) {
        properties.put(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR,
                String.valueOf(useAWSSDKDefaultBehavior));
        properties.put(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE,
                String.valueOf(useInstanceProfile));
        properties.put(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, accessKey);
        properties.put(CloudConfigurationConstants.AWS_S3_SECRET_KEY, secretKey);
        properties.put(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, sessionToken);
        properties.put(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN, iamRoleArn);
        properties.put(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID, externalId);
        properties.put(CloudConfigurationConstants.AWS_S3_REGION, region);
        properties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, endpoint);
    }

    @Override
    public String getCredentialString() {
        return "AWSCloudCredential{" +
                "useAWSSDKDefaultBehavior=" + useAWSSDKDefaultBehavior +
                ", useInstanceProfile=" + useInstanceProfile +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", sessionToken='" + sessionToken + '\'' +
                ", iamRoleArn='" + iamRoleArn + '\'' +
                ", externalId='" + externalId + '\'' +
                ", region='" + region + '\'' +
                ", endpoint='" + endpoint + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder fileStore = FileStoreInfo.newBuilder();
        fileStore.setFsType(FileStoreType.S3);
        S3FileStoreInfo.Builder s3FileStoreInfo = S3FileStoreInfo.newBuilder();
        s3FileStoreInfo.setRegion(region).setEndpoint(endpoint);
        AwsCredentialInfo.Builder awsCredentialInfo = AwsCredentialInfo.newBuilder();
        if (useAWSSDKDefaultBehavior) {
            AwsDefaultCredentialInfo.Builder defaultCredentialInfo = AwsDefaultCredentialInfo.newBuilder();
            awsCredentialInfo.setDefaultCredential(defaultCredentialInfo.build());
        } else if (useInstanceProfile) {
            if (!iamRoleArn.isEmpty()) {
                AwsAssumeIamRoleCredentialInfo.Builder assumeIamRowCredentialInfo
                        = AwsAssumeIamRoleCredentialInfo.newBuilder();
                assumeIamRowCredentialInfo.setIamRoleArn(iamRoleArn);
                assumeIamRowCredentialInfo.setExternalId(externalId);
                awsCredentialInfo.setAssumeRoleCredential(assumeIamRowCredentialInfo.build());
            } else {
                awsCredentialInfo.setProfileCredential(AwsInstanceProfileCredentialInfo.newBuilder().build());
            }
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            // TODO: Support assumeRole with AK/SK
            // TODO: Support sessionToken with AK/SK
            AwsSimpleCredentialInfo.Builder simpleCredentialInfo = AwsSimpleCredentialInfo.newBuilder();
            simpleCredentialInfo.setAccessKey(accessKey);
            simpleCredentialInfo.setAccessKeySecret(secretKey);
            awsCredentialInfo.setSimpleCredential(simpleCredentialInfo.build());
        } else {
            Preconditions.checkArgument(false, "Unreachable");
        }
        s3FileStoreInfo.setCredential(awsCredentialInfo.build());
        fileStore.setS3FsInfo(s3FileStoreInfo.build());
        return fileStore.build();
    }
}
