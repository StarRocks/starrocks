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

package com.starrocks.connector.share.credential;

import com.starrocks.connector.share.credential.provider.AssumedRoleCredentialProvider;
import com.starrocks.connector.share.credential.provider.OverwriteAwsDefaultCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Translates StarRocks AWS cloud configuration properties into Hadoop S3A configuration.
 * <p>
 * This is used by JNI scanners (Hive RCFile/AVRO/SequenceFile, Hudi) on the BE side
 * to properly configure Hadoop's S3AFileSystem with credentials including assume-role support.
 * <p>
 * The logic mirrors {@code AwsCloudCredential.applyToConfiguration()} and
 * {@code AwsCloudConfiguration.applyToConfiguration()} from the FE module.
 */
public class CloudConfigurationApplier {

    private static final Logger LOG = LoggerFactory.getLogger(CloudConfigurationApplier.class);

    private static final String DEFAULT_CREDENTIAL_PROVIDER = OverwriteAwsDefaultCredentialsProvider.class.getName();
    private static final String IAM_CREDENTIAL_PROVIDER = IAMInstanceCredentialsProvider.class.getName();
    private static final String ASSUME_ROLE_CREDENTIAL_PROVIDER = AssumedRoleCredentialProvider.class.getName();
    private static final String SIMPLE_CREDENTIAL_PROVIDER = SimpleAWSCredentialsProvider.class.getName();
    private static final String TEMPORARY_CREDENTIAL_PROVIDER = TemporaryAWSCredentialsProvider.class.getName();

    /**
     * Applies StarRocks cloud configuration properties to a Hadoop Configuration.
     * <p>
     * Detects if the properties contain AWS S3 credential keys and translates them
     * into Hadoop S3A configuration. Non-cloud properties are ignored.
     *
     * @param props StarRocks cloud properties (from TCloudConfiguration.cloud_properties)
     * @param conf  Hadoop Configuration to apply settings to
     */
    public static void applyCloudConfiguration(Map<String, String> props, Configuration conf) {
        if (props == null || props.isEmpty()) {
            return;
        }

        // Check if this is an AWS configuration by looking for any aws.s3.* key
        boolean isAwsConfig = props.keySet().stream().anyMatch(k -> k.startsWith("aws.s3."));
        if (!isAwsConfig) {
            return;
        }

        applyAwsCloudConfiguration(props, conf);
    }

    private static void applyAwsCloudConfiguration(Map<String, String> props, Configuration conf) {
        // Set S3A filesystem implementations
        String s3aFileSystem = S3AFileSystem.class.getName();
        conf.set("fs.s3.impl", s3aFileSystem);
        conf.set("fs.s3a.impl", s3aFileSystem);
        conf.set("fs.s3n.impl", s3aFileSystem);
        conf.set("fs.oss.impl", s3aFileSystem);
        conf.set("fs.ks3.impl", s3aFileSystem);
        conf.set("fs.obs.impl", s3aFileSystem);
        conf.set("fs.tos.impl", s3aFileSystem);
        conf.set("fs.cosn.impl", s3aFileSystem);

        // Set retry limits (same as AwsCloudConfiguration.applyToConfiguration)
        conf.set(Constants.RETRY_LIMIT, "3");
        conf.set(Constants.MAX_ERROR_RETRIES, "5");

        // Path style access and SSL
        String enablePathStyleAccess = props.getOrDefault(
                CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS, "false");
        conf.set(Constants.PATH_STYLE_ACCESS, enablePathStyleAccess);

        String enableSSL = props.getOrDefault(
                CloudConfigurationConstants.AWS_S3_ENABLE_SSL, "true");
        conf.set(Constants.SECURE_CONNECTIONS, enableSSL);

        // Apply credentials
        applyAwsCredentials(props, conf);
    }

    private static void applyAwsCredentials(Map<String, String> props, Configuration conf) {
        boolean useAWSSDKDefaultBehavior = Boolean.parseBoolean(
                props.getOrDefault(CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "false"));
        boolean useInstanceProfile = Boolean.parseBoolean(
                props.getOrDefault(CloudConfigurationConstants.AWS_S3_USE_INSTANCE_PROFILE, "false"));
        String accessKey = props.getOrDefault(CloudConfigurationConstants.AWS_S3_ACCESS_KEY, "");
        String secretKey = props.getOrDefault(CloudConfigurationConstants.AWS_S3_SECRET_KEY, "");
        String sessionToken = props.getOrDefault(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN, "");
        String iamRoleArn = props.getOrDefault(CloudConfigurationConstants.AWS_S3_IAM_ROLE_ARN, "");
        String stsRegion = props.getOrDefault(CloudConfigurationConstants.AWS_S3_STS_REGION, "");
        String stsEndpoint = props.getOrDefault(CloudConfigurationConstants.AWS_S3_STS_ENDPOINT, "");
        String externalId = props.getOrDefault(CloudConfigurationConstants.AWS_S3_EXTERNAL_ID, "");
        String region = props.getOrDefault(CloudConfigurationConstants.AWS_S3_REGION, "");
        String endpoint = props.getOrDefault(CloudConfigurationConstants.AWS_S3_ENDPOINT, "");

        if (useAWSSDKDefaultBehavior) {
            if (!iamRoleArn.isEmpty()) {
                applyAssumeRole(DEFAULT_CREDENTIAL_PROVIDER, conf,
                        iamRoleArn, stsRegion, stsEndpoint, externalId);
            } else {
                conf.set(Constants.AWS_CREDENTIALS_PROVIDER, DEFAULT_CREDENTIAL_PROVIDER);
            }
        } else if (useInstanceProfile) {
            if (!iamRoleArn.isEmpty()) {
                applyAssumeRole(IAM_CREDENTIAL_PROVIDER, conf,
                        iamRoleArn, stsRegion, stsEndpoint, externalId);
            } else {
                conf.set(Constants.AWS_CREDENTIALS_PROVIDER, IAM_CREDENTIAL_PROVIDER);
            }
        } else if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            conf.set(Constants.ACCESS_KEY, accessKey);
            conf.set(Constants.SECRET_KEY, secretKey);
            if (!iamRoleArn.isEmpty()) {
                applyAssumeRole(SIMPLE_CREDENTIAL_PROVIDER, conf,
                        iamRoleArn, stsRegion, stsEndpoint, externalId);
            } else {
                if (!sessionToken.isEmpty()) {
                    conf.set(Constants.SESSION_TOKEN, sessionToken);
                    conf.set(Constants.AWS_CREDENTIALS_PROVIDER, TEMPORARY_CREDENTIAL_PROVIDER);
                } else {
                    conf.set(Constants.AWS_CREDENTIALS_PROVIDER, SIMPLE_CREDENTIAL_PROVIDER);
                }
            }
        } else {
            LOG.warn("No valid AWS credentials found in cloud configuration properties");
            return;
        }

        if (!region.isEmpty()) {
            conf.set(Constants.AWS_REGION, region);
        }
        if (!endpoint.isEmpty()) {
            conf.set(Constants.ENDPOINT, endpoint);
        }
    }

    private static void applyAssumeRole(String baseCredentialsProvider, Configuration conf,
                                         String iamRoleArn, String stsRegion,
                                         String stsEndpoint, String externalId) {
        conf.set(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER, baseCredentialsProvider);
        // Use our custom AssumedRoleCredentialProvider which supports external id
        conf.set(Constants.AWS_CREDENTIALS_PROVIDER, ASSUME_ROLE_CREDENTIAL_PROVIDER);
        conf.set(Constants.ASSUMED_ROLE_ARN, iamRoleArn);
        if (!stsRegion.isEmpty()) {
            conf.set(Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION, stsRegion);
        }
        if (!stsEndpoint.isEmpty()) {
            // Hadoop is using aws sdk v1. If the user provides the sts endpoint,
            // the sts region must also be specified.
            if (stsRegion.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("STS endpoint is set to %s but no signing region was provided", stsEndpoint));
            }
            conf.set(Constants.ASSUMED_ROLE_STS_ENDPOINT, stsEndpoint);
        }
        conf.set(AssumedRoleCredentialProvider.CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID, externalId);
    }
}
