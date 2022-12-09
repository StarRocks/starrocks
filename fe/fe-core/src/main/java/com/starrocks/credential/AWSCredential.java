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
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.google.common.base.Preconditions;
import com.starrocks.thrift.TAWSCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;
import java.util.UUID;

public class AWSCredential implements CloudCredential {

    private final boolean useInstanceProfile;

    private final String accessKey;

    private final String secretKey;

    private final String iamRoleArn;

    private final String externalId;

    private final String region;

    private final String endpoint;

    private AWSCredential(boolean useInstanceProfile, String accessKey, String secretKey, String iamRoleArn,
                          String externalId, String region, String endpoint) {
        Preconditions.checkNotNull(accessKey);
        Preconditions.checkNotNull(secretKey);
        Preconditions.checkNotNull(iamRoleArn);
        Preconditions.checkNotNull(externalId);
        Preconditions.checkNotNull(region);
        Preconditions.checkNotNull(endpoint);
        this.useInstanceProfile = useInstanceProfile;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.iamRoleArn = iamRoleArn;
        this.externalId = externalId;
        this.region = region;
        this.endpoint = endpoint;
    }

    public boolean isUseInstanceProfile() {
        return useInstanceProfile;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getIamRoleArn() {
        return iamRoleArn;
    }

    public String getExternalId() {
        return externalId;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public static AWSCredential buildS3Credential(Map<String, String> properties) {
        AWSCredential awsCredential = new AWSCredential(
                Boolean.parseBoolean(properties.getOrDefault(CredentialConstants.AWS_S3_USE_INSTANCE_PROFILE, "false")),
                properties.getOrDefault(CredentialConstants.AWS_S3_ACCESS_KEY, ""),
                properties.getOrDefault(CredentialConstants.AWS_S3_SECRET_KEY, ""),
                properties.getOrDefault(CredentialConstants.AWS_S3_IAM_ROLE_ARN, ""),
                properties.getOrDefault(CredentialConstants.AWS_S3_EXTERNAL_ID, ""),
                properties.getOrDefault(CredentialConstants.AWS_S3_REGION, ""),
                properties.getOrDefault(CredentialConstants.AWS_S3_ENDPOINT, "")
        );
        if (!awsCredential.validate()) {
            return null;
        }
        return awsCredential;
    }

    public AWSCredentialsProvider generateAWSCredentialsProvider() {
        AWSCredentialsProvider awsCredentialsProvider = null;
        String sessionName = UUID.randomUUID().toString();
        if (useInstanceProfile) {
            awsCredentialsProvider = new InstanceProfileCredentialsProvider(true);
            if (!iamRoleArn.isEmpty()) {
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
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            awsCredentialsProvider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secretKey));
            if (!iamRoleArn.isEmpty()) {
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
        } else {
            awsCredentialsProvider = new AnonymousAWSCredentialsProvider();
        }
        return awsCredentialsProvider;
    }

    public Configuration generateHadoopConfiguration() {
        Configuration configuration = new Configuration();
        if (useInstanceProfile) {
            if (!iamRoleArn.isEmpty()) {
                configuration.set("fs.s3a.assumed.role.credentials.provider",
                        "com.amazonaws.auth.InstanceProfileCredentialsProvider");
                configuration.set("fs.s3a.aws.credentials.provider",
                        "com.starrocks.credential.AssumedRoleCredentialProvider");
                configuration.set("fs.s3a.assumed.role.arn", iamRoleArn);
                configuration.set("starrocks.s3.external-id", externalId);
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
                configuration.set("fs.s3a.aws.credentials.provider",
                        "com.starrocks.credential.AssumedRoleCredentialProvider");
                configuration.set("fs.s3a.assumed.role.arn", iamRoleArn);
                configuration.set("starrocks.s3.external-id", externalId);
            } else {
                configuration.set("fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            }
        } else {
            configuration.set("fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider");
        }
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        if (!endpoint.isEmpty()) {
            configuration.set("fs.s3a.endpoint", endpoint);
        }
        if (!endpoint.isEmpty()) {
            configuration.set("fs.s3a.endpoint.region", region);
        }
        return configuration;
    }

    public static AWSCredential buildGlueCredential(HiveConf properties) {
        AWSCredential glueCredential = new AWSCredential(
                properties.getBoolean(CredentialConstants.AWS_GLUE_USE_INSTANCE_PROFILE, false),
                properties.get(CredentialConstants.AWS_GLUE_ACCESS_KEY, ""),
                properties.get(CredentialConstants.AWS_GLUE_SECRET_KEY, ""),
                properties.get(CredentialConstants.AWS_GLUE_IAM_ROLE_ARN, ""),
                properties.get(CredentialConstants.AWS_GLUE_EXTERNAL_ID, ""),
                properties.get(CredentialConstants.AWS_GLUE_REGION, ""),
                properties.get(CredentialConstants.AWS_GLUE_ENDPOINT, "")
        );

        if (!glueCredential.validate()) {
            return null;
        }
        return glueCredential;
    }

    @Override
    public boolean validate() {
        if (!useInstanceProfile && accessKey.isEmpty() && secretKey.isEmpty()) {
            return false;
        }

        return true;
    }

    @Override
    public void toThrift(TAWSCredential msg) {
        msg.setUse_instance_profile(useInstanceProfile);
        if (!accessKey.isEmpty()) {
            msg.setAccess_key(accessKey);
        }
        if (!secretKey.isEmpty()) {
            msg.setSecret_key(secretKey);
        }
        if (!iamRoleArn.isEmpty()) {
            msg.setIam_role_arn(iamRoleArn);
        }
        if (!externalId.isEmpty()) {
            msg.setExternal_id(externalId);
        }
        if (!region.isEmpty()) {
            msg.setRegion(region);
        }
        if (!endpoint.isEmpty()) {
            msg.setEndpoint(endpoint);
        }
    }
}
