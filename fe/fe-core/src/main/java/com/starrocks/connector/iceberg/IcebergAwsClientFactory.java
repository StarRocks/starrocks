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

package com.starrocks.connector.iceberg;

import com.starrocks.credential.aws.AWSCloudConfigurationProvider;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_EXTERNAL_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_IAM_ROLE_ARN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_SECRET_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_SESSION_TOKEN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_STS_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_STS_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_GLUE_USE_INSTANCE_PROFILE;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_EXTERNAL_ID;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SESSION_TOKEN;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_STS_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_STS_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE;

public class IcebergAwsClientFactory implements AwsClientFactory {
    private static final Logger LOG = LogManager.getLogger(IcebergAwsClientFactory.class);
    public static final String HTTPS_SCHEME = "https://";

    private AwsProperties awsProperties;

    private boolean s3UseAWSSDKDefaultBehavior;
    private boolean s3UseInstanceProfile;
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3SessionToken;
    private String s3IamRoleArn;
    private String s3StsRegion;
    private String s3StsEndpoint;
    private String s3ExternalId;
    private String s3Region;
    private String s3Endpoint;
    private boolean s3EnablePathStyleAccess;
    private boolean glueUseAWSSDKDefaultBehavior;
    private boolean glueUseInstanceProfile;
    private String glueAccessKey;
    private String glueSecretKey;
    private String glueSessionToken;
    private String glueIamRoleArn;
    private String glueStsRegion;
    private String glueStsEndpoint;
    private String glueExternalId;
    private String glueRegion;
    private String glueEndpoint;

    @Override
    public void initialize(Map<String, String> properties) {
        this.awsProperties = new AwsProperties(properties);

        s3UseAWSSDKDefaultBehavior = Boolean.parseBoolean(properties.getOrDefault(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false"));
        s3UseInstanceProfile = Boolean.parseBoolean(properties.getOrDefault(AWS_S3_USE_INSTANCE_PROFILE, "false"));
        s3AccessKey = properties.getOrDefault(AWS_S3_ACCESS_KEY, "");
        s3SecretKey = properties.getOrDefault(AWS_S3_SECRET_KEY, "");
        s3SessionToken = properties.getOrDefault(AWS_S3_SESSION_TOKEN, "");
        s3IamRoleArn = properties.getOrDefault(AWS_S3_IAM_ROLE_ARN, "");
        s3StsRegion = properties.getOrDefault(AWS_S3_STS_REGION, "");
        s3StsEndpoint = properties.getOrDefault(AWS_S3_STS_ENDPOINT, "");
        s3ExternalId = properties.getOrDefault(AWS_S3_EXTERNAL_ID, "");
        s3Region = properties.getOrDefault(AWS_S3_REGION, "");
        s3Endpoint = properties.getOrDefault(AWS_S3_ENDPOINT, "");
        s3EnablePathStyleAccess =
                Boolean.parseBoolean(properties.getOrDefault(AWS_S3_ENABLE_PATH_STYLE_ACCESS, "false"));

        glueUseAWSSDKDefaultBehavior = Boolean.parseBoolean(
                properties.getOrDefault(AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false"));
        glueUseInstanceProfile = Boolean.parseBoolean(properties.getOrDefault(AWS_GLUE_USE_INSTANCE_PROFILE, "false"));
        glueAccessKey = properties.getOrDefault(AWS_GLUE_ACCESS_KEY, "");
        glueSecretKey = properties.getOrDefault(AWS_GLUE_SECRET_KEY, "");
        glueSessionToken = properties.getOrDefault(AWS_GLUE_SESSION_TOKEN, "");
        glueIamRoleArn = properties.getOrDefault(AWS_GLUE_IAM_ROLE_ARN, "");
        glueStsRegion = properties.getOrDefault(AWS_GLUE_STS_REGION, "");
        glueStsEndpoint = properties.getOrDefault(AWS_GLUE_STS_ENDPOINT, "");
        glueExternalId = properties.getOrDefault(AWS_GLUE_EXTERNAL_ID, "");
        glueRegion = properties.getOrDefault(AWS_GLUE_REGION, "");
        glueEndpoint = properties.getOrDefault(AWS_GLUE_ENDPOINT, "");
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
        // Below two configuration copied from Iceberg's official SDK
        assumeRoleBuilder.durationSeconds(awsProperties.clientAssumeRoleTimeoutSec());
        assumeRoleBuilder.tags(awsProperties.stsClientAssumeRoleTags());

        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClientBuilder.build())
                .refreshRequest(assumeRoleBuilder.build())
                .build();
    }

    @Override
    public S3Client s3() {
        AwsCredentialsProvider baseAWSCredentialsProvider =
                getBaseAWSCredentialsProvider(s3UseAWSSDKDefaultBehavior, s3UseInstanceProfile, s3AccessKey,
                        s3SecretKey, s3SessionToken);
        S3ClientBuilder s3ClientBuilder = S3Client.builder();

        if (!s3IamRoleArn.isEmpty()) {
            s3ClientBuilder.credentialsProvider(getAssumeRoleCredentialsProvider(baseAWSCredentialsProvider,
                    s3IamRoleArn, s3ExternalId, s3StsRegion, s3StsEndpoint));
        } else {
            s3ClientBuilder.credentialsProvider(baseAWSCredentialsProvider);
        }

        s3ClientBuilder.region(tryToResolveRegion(s3Region));

        // To prevent the 's3ClientBuilder' (NPE) exception, when 'aws.s3.endpoint' does not have
        // 'scheme', we will add https scheme.
        if (!s3Endpoint.isEmpty()) {
            s3ClientBuilder.endpointOverride(ensureSchemeInEndpoint(s3Endpoint));
        }

        // set for s3 path style access
        s3ClientBuilder.serviceConfiguration(
                S3Configuration.builder().pathStyleAccessEnabled(s3EnablePathStyleAccess).build());

        return s3ClientBuilder.build();
    }

    @Override
    public GlueClient glue() {
        AwsCredentialsProvider baseAWSCredentialsProvider =
                getBaseAWSCredentialsProvider(glueUseAWSSDKDefaultBehavior, glueUseInstanceProfile, glueAccessKey,
                        glueSecretKey, glueSessionToken);
        GlueClientBuilder glueClientBuilder = GlueClient.builder();

        if (!glueIamRoleArn.isEmpty()) {
            glueClientBuilder.credentialsProvider(getAssumeRoleCredentialsProvider(baseAWSCredentialsProvider,
                    glueIamRoleArn, glueExternalId, glueStsRegion, glueStsEndpoint));
        } else {
            glueClientBuilder.credentialsProvider(baseAWSCredentialsProvider);
        }

        glueClientBuilder.region(tryToResolveRegion(glueRegion));

        // To prevent the 'glueClientBuilder' (NPE) exception, when 'aws.s3.endpoint' does not have
        // 'scheme', we will add https scheme.
        if (!glueEndpoint.isEmpty()) {
            glueClientBuilder.endpointOverride(ensureSchemeInEndpoint(glueEndpoint));
        }

        return glueClientBuilder.build();
    }

    private AwsCredentialsProvider getBaseAWSCredentialsProvider(boolean useAWSSDKDefaultBehavior, boolean useInstanceProfile,
                                                                 String accessKey, String secretKey, String sessionToken) {
        if (useAWSSDKDefaultBehavior) {
            return DefaultCredentialsProvider.builder().build();
        } else if (useInstanceProfile) {
            return InstanceProfileCredentialsProvider.builder().build();
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            if (!sessionToken.isEmpty()) {
                return StaticCredentialsProvider.create(AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
            } else {
                return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
            }
        } else {
            LOG.info("User didn't configure any credentials in Iceberg catalog, " +
                    "we will use AWS DefaultCredentialsProvider instead");
            return DefaultCredentialsProvider.builder().build();
        }
    }

    @Override
    public KmsClient kms() {
        return null;
    }

    @Override
    public DynamoDbClient dynamo() {
        return null;
    }

    /**
     * Checks if the given 'endpoint' contains a scheme. If not, the default HTTPS scheme is added.
     *
     * @param endpoint The endpoint string to be checked
     * @return The URI with the added scheme
     */
    public static URI ensureSchemeInEndpoint(String endpoint) {
        URI uri = URI.create(endpoint);
        if (uri.getScheme() != null) {
            return uri;
        }
        return URI.create(HTTPS_SCHEME + endpoint);
    }

    public static Region tryToResolveRegion(String strRegion) {
        if (!strRegion.isEmpty()) {
            return Region.of(strRegion);
        }
        Region region = Region.of(AWSCloudConfigurationProvider.DEFAULT_AWS_REGION);
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
