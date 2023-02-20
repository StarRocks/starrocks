// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.common.base.Preconditions;
import com.starrocks.thrift.TCloudProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider;

import java.util.List;
import java.util.UUID;

/**
 * Authenticating process (It's a pseudocode code):
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

        }
        return awsCredentialsProvider;
    }

    private AWSCredentialsProvider getBaseAWSCredentialsProvider() {
        return null;
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
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
    public void toThrift(List<TCloudProperty> properties) {
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR,
                String.valueOf(useAWSSDKDefaultBehavior)));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE,
                String.valueOf(useInstanceProfile)));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, accessKey));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_SECRET_KEY, secretKey));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN, iamRoleArn));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID, externalId));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_REGION, region));
        properties.add(new TCloudProperty(CloudConfigurationConstants.AWS_S3_ENDPOINT, endpoint));
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
