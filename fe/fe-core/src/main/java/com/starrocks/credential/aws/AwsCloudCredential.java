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

import com.google.common.base.Preconditions;
import com.staros.proto.AwsAssumeIamRoleCredentialInfo;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.AwsInstanceProfileCredentialInfo;
import com.staros.proto.AwsSimpleCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudCredential;
import com.starrocks.credential.provider.AssumedRoleCredentialProvider;
import com.starrocks.credential.provider.OverwriteAwsDefaultCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.DEFAULT_AWS_REGION;

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
public class AwsCloudCredential implements CloudCredential {

    private static final Logger LOG = LoggerFactory.getLogger(AwsCloudCredential.class);

    private static final String DEFAULT_CREDENTIAL_PROVIDER = OverwriteAwsDefaultCredentialsProvider.class.getName();
    private static final String IAM_CREDENTIAL_PROVIDER = IAMInstanceCredentialsProvider.class.getName();
    private static final String ASSUME_ROLE_CREDENTIAL_PROVIDER = AssumedRoleCredentialProvider.class.getName();
    private static final String SIMPLE_CREDENTIAL_PROVIDER = SimpleAWSCredentialsProvider.class.getName();
    private static final String TEMPORARY_CREDENTIAL_PROVIDER = TemporaryAWSCredentialsProvider.class.getName();

    private final boolean useAWSSDKDefaultBehavior;

    private final boolean useInstanceProfile;

    private final String accessKey;

    private final String secretKey;

    private final String sessionToken;

    private final String iamRoleArn;

    private final String stsRegion;

    private final String stsEndpoint;

    private final String externalId;

    private final String region;

    private final String endpoint;

    protected AwsCloudCredential(boolean useAWSSDKDefaultBehavior, boolean useInstanceProfile, String accessKey,
                                 String secretKey, String sessionToken, String iamRoleArn, String stsRegion,
                                 String stsEndpoint, String externalId, String region,
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
        this.stsRegion = stsRegion;
        this.stsEndpoint = stsEndpoint;
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

    public AwsCredentialsProvider generateAWSCredentialsProvider() {
        AwsCredentialsProvider awsCredentialsProvider =
                getBaseAWSCredentialsProvider(useAWSSDKDefaultBehavior, useInstanceProfile, accessKey, secretKey,
                        sessionToken);
        if (!iamRoleArn.isEmpty()) {
            awsCredentialsProvider =
                    getAssumeRoleCredentialsProvider(awsCredentialsProvider, iamRoleArn, externalId, stsRegion, stsEndpoint);
        }
        return awsCredentialsProvider;
    }

    private StsAssumeRoleCredentialsProvider getAssumeRoleCredentialsProvider(AwsCredentialsProvider baseCredentials,
                                                                              String iamRoleArn, String externalId,
                                                                              String region, String endpoint) {
        // Build sts client
        StsClientBuilder stsClientBuilder = StsClient.builder().credentialsProvider(baseCredentials);
        if (!region.isEmpty()) {
            stsClientBuilder.region(Region.of(region));
        }
        if (!endpoint.isEmpty()) {
            stsClientBuilder.endpointOverride(URI.create(endpoint));
        }

        // Build AssumeRoleRequest
        AssumeRoleRequest.Builder assumeRoleBuilder = AssumeRoleRequest.builder();
        assumeRoleBuilder.roleArn(iamRoleArn);
        assumeRoleBuilder.roleSessionName(UUID.randomUUID().toString());
        if (!externalId.isEmpty()) {
            assumeRoleBuilder.externalId(externalId);
        }

        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClientBuilder.build())
                .refreshRequest(assumeRoleBuilder.build())
                .build();
    }

    private AwsCredentialsProvider getBaseAWSCredentialsProvider(boolean useAWSSDKDefaultBehavior,
                                                                 boolean useInstanceProfile,
                                                                 String accessKey, String secretKey,
                                                                 String sessionToken) {
        if (useAWSSDKDefaultBehavior) {
            return DefaultCredentialsProvider.builder().build();
        } else if (useInstanceProfile) {
            return InstanceProfileCredentialsProvider.builder().build();
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            if (!sessionToken.isEmpty()) {
                return StaticCredentialsProvider.create(
                        AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
            } else {
                return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
            }
        } else {
            LOG.info("User didn't configure any credentials in aws credential, " +
                    "we will use AWS DefaultCredentialsProvider instead");
            return DefaultCredentialsProvider.builder().build();
        }
    }

    private void applyAssumeRole(String baseCredentialsProvider, Configuration configuration) {
        configuration.set(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER, baseCredentialsProvider);
        // Original "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider" don't support external id,
        // so we use our own AssumedRoleCredentialProvider.
        configuration.set(Constants.AWS_CREDENTIALS_PROVIDER, ASSUME_ROLE_CREDENTIAL_PROVIDER);
        configuration.set(Constants.ASSUMED_ROLE_ARN, iamRoleArn);
        if (!stsRegion.isEmpty()) {
            configuration.set(Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION, stsRegion);
        }
        if (!stsEndpoint.isEmpty()) {
            // Hadoop is using aws sdk v1. If the user provides the sts endpoint, the sts region must also be specified.
            // But in aws sdk v2, user only need to provide one of the two
            Preconditions.checkArgument(!stsRegion.isEmpty(),
                    String.format("STS endpoint is set to %s but no signing region was provided", stsEndpoint));
            configuration.set(Constants.ASSUMED_ROLE_STS_ENDPOINT, stsEndpoint);
        }
        configuration.set(AssumedRoleCredentialProvider.CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID, externalId);
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        if (useAWSSDKDefaultBehavior) {
            if (!iamRoleArn.isEmpty()) {
                applyAssumeRole(DEFAULT_CREDENTIAL_PROVIDER, configuration);
            } else {
                configuration.set(Constants.AWS_CREDENTIALS_PROVIDER, DEFAULT_CREDENTIAL_PROVIDER);
            }
        } else if (useInstanceProfile) {
            if (!iamRoleArn.isEmpty()) {
                applyAssumeRole(IAM_CREDENTIAL_PROVIDER, configuration);
            } else {
                configuration.set(Constants.AWS_CREDENTIALS_PROVIDER, IAM_CREDENTIAL_PROVIDER);
            }
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            configuration.set(Constants.ACCESS_KEY, accessKey);
            configuration.set(Constants.SECRET_KEY, secretKey);
            if (!iamRoleArn.isEmpty()) {
                applyAssumeRole(SIMPLE_CREDENTIAL_PROVIDER, configuration);
            } else {
                if (!sessionToken.isEmpty()) {
                    configuration.set(Constants.SESSION_TOKEN, sessionToken);
                    configuration.set(Constants.AWS_CREDENTIALS_PROVIDER, TEMPORARY_CREDENTIAL_PROVIDER);
                } else {
                    configuration.set(Constants.AWS_CREDENTIALS_PROVIDER, SIMPLE_CREDENTIAL_PROVIDER);
                }
            }
        } else {
            Preconditions.checkArgument(false, "Unreachable");
        }
        if (!region.isEmpty()) {
            configuration.set(Constants.AWS_REGION, region);
        }
        if (!endpoint.isEmpty()) {
            configuration.set(Constants.ENDPOINT, endpoint);
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
        properties.put(CloudConfigurationConstants.AWS_S3_STS_REGION, stsRegion);
        properties.put(CloudConfigurationConstants.AWS_S3_STS_ENDPOINT, stsEndpoint);
        properties.put(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID, externalId);
        properties.put(CloudConfigurationConstants.AWS_S3_REGION, region);
        properties.put(CloudConfigurationConstants.AWS_S3_ENDPOINT, endpoint);
    }

    @Override
    public String toCredString() {
        return "AWSCloudCredential{" +
                "useAWSSDKDefaultBehavior=" + useAWSSDKDefaultBehavior +
                ", useInstanceProfile=" + useInstanceProfile +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", sessionToken='" + sessionToken + '\'' +
                ", iamRoleArn='" + iamRoleArn + '\'' +
                ", stsRegion='" + stsRegion + '\'' +
                ", stsEndpoint='" + stsEndpoint + '\'' +
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
                // TODO: Support assumeRole with custom sts region and sts endpoint
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

    public Region tryToResolveRegion() {
        if (!region.isEmpty()) {
            return Region.of(region);
        }
        Region region = Region.of(DEFAULT_AWS_REGION);
        try {
            DefaultAwsRegionProviderChain providerChain = DefaultAwsRegionProviderChain.builder()
                    .profileFile(ProfileFile::defaultProfileFile)
                    .profileName(ProfileFileSystemSetting.AWS_PROFILE.getStringValueOrThrow()).build();
            region = providerChain.getRegion();
        } catch (Exception e) {
            LOG.info(
                    "AWS sdk unable to load region from DefaultAwsRegionProviderChain, using default region us-east-1 instead",
                    e);
        }
        return region;
    }
}
