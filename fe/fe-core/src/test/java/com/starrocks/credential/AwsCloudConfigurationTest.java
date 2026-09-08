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

import com.staros.proto.FileStoreInfo;
import com.starrocks.connector.share.credential.AwsSseCUtil;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.credential.aws.AwsCloudCredential;
import com.starrocks.credential.provider.OverwriteAwsDefaultCredentialsProvider;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class AwsCloudConfigurationTest {

    @Test
    public void testUseAwsSDKDefaultBehavior() throws Exception {
        // Test hadoop configuration
        Map<String, String>  properties = new HashMap<>();
        properties.put("aws.s3.use_aws_sdk_default_behavior", "true");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assertions.assertEquals(OverwriteAwsDefaultCredentialsProvider.class.getName(),
                configuration.get("fs.s3a.aws.credentials.provider"));
        S3AFileSystem fs = (S3AFileSystem) FileSystem.get(new URI("s3://hi/a.parquet"), configuration);
        AWSCredentialProviderList list =  fs.shareCredentials("ut");
        String previousProviderName = list.getProviders().get(0).getClass().getName();
        int previousHashCode = list.getProviders().get(0).hashCode();
        fs.close();

        fs = (S3AFileSystem) FileSystem.get(new URI("s3://hi/a.parquet"), configuration);
        list =  fs.shareCredentials("ut");
        String currentProviderName = list.getProviders().get(0).getClass().getName();
        int currentHashCode = list.getProviders().get(0).hashCode();
        fs.close();

        // Make sure two DefaultCredentialsProviders are the same class
        Assertions.assertEquals(previousProviderName, currentProviderName);
        // Make sure the provider is DefaultCredentialsProvider
        Assertions.assertEquals(DefaultCredentialsProvider.class.getName(), previousProviderName);
        // Make sure two DefaultCredentialsProviders are different instances
        Assertions.assertNotEquals(previousHashCode, currentHashCode);
    }

    @Test
    public void testAwsDefaultCredentialsProvider() {
        OverwriteAwsDefaultCredentialsProvider provider = new OverwriteAwsDefaultCredentialsProvider();
        AwsCredentials credentials = provider.resolveCredentials();
        Assertions.assertNull(credentials.accessKeyId());
        Assertions.assertNull(credentials.secretAccessKey());
    }

    @Test
    public void testUseWebIdentityProfile() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.use_web_identity_token_file", "true");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        Assertions.assertTrue(cloudConfiguration instanceof AwsCloudConfiguration);

        Map<String, String> thriftProperties = new HashMap<>();
        ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential().toThrift(thriftProperties);
        Assertions.assertEquals("true", thriftProperties.get("aws.s3.use_web_identity_token_file"));
    }

    @Test
    public void testWebIdentityApplyToConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.use_web_identity_token_file", "true");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assertions.assertEquals(OverwriteAwsDefaultCredentialsProvider.class.getName(),
                configuration.get("fs.s3a.aws.credentials.provider"));
    }

    @Test
    public void testWebIdentityApplyToConfigurationWithAssumeRole() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.use_web_identity_token_file", "true");
        properties.put("aws.s3.iam_role_arn", "arn:aws:iam::123456789:role/MyRole");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assertions.assertEquals(OverwriteAwsDefaultCredentialsProvider.class.getName(),
                configuration.get("fs.s3a.assumed.role.credentials.provider"));
        Assertions.assertEquals("com.starrocks.credential.provider.AssumedRoleCredentialProvider",
                configuration.get("fs.s3a.aws.credentials.provider"));
        Assertions.assertEquals("arn:aws:iam::123456789:role/MyRole", configuration.get("fs.s3a.assumed.role.arn"));
    }

    @Test
    public void testWebIdentityGenerateCredentialsProvider() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.use_web_identity_token_file", "true");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        AwsCredentialsProvider provider =
                ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential().generateAWSCredentialsProvider();
        Assertions.assertInstanceOf(WebIdentityTokenFileCredentialsProvider.class, provider);
    }

    @Test
    public void testWebIdentityToFileStoreInfo() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.use_web_identity_token_file", "true");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        FileStoreInfo fileStoreInfo = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStoreInfo.getS3FsInfo().getCredential().hasWebIdentityCredential());
        Assertions.assertTrue(fileStoreInfo.getS3FsInfo().getCredential()
                .getWebIdentityCredential().getIamRoleArn().isEmpty());
    }

    @Test
    public void testWebIdentityToFileStoreInfoWithAssumeRole() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.use_web_identity_token_file", "true");
        properties.put("aws.s3.iam_role_arn", "arn:aws:iam::123456789:role/MyRole");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        FileStoreInfo fileStoreInfo = cloudConfiguration.toFileStoreInfo();
        Assertions.assertTrue(fileStoreInfo.getS3FsInfo().getCredential().hasWebIdentityCredential());
        Assertions.assertEquals("arn:aws:iam::123456789:role/MyRole",
                fileStoreInfo.getS3FsInfo().getCredential().getWebIdentityCredential().getIamRoleArn());
    }

    @Test
    public void testUseAwsSDKDefaultBehaviorPlusAssumeRole() {
        // Test hadoop configuration
        Map<String, String>  properties = new HashMap<>();
        properties.put("aws.s3.use_aws_sdk_default_behavior", "true");
        properties.put("aws.s3.iam_role_arn", "smith");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assertions.assertEquals(OverwriteAwsDefaultCredentialsProvider.class.getName(),
                configuration.get("fs.s3a.assumed.role.credentials.provider"));
        Assertions.assertEquals("com.starrocks.credential.provider.AssumedRoleCredentialProvider",
                configuration.get("fs.s3a.aws.credentials.provider"));
        Assertions.assertEquals("smith", configuration.get("fs.s3a.assumed.role.arn"));
    }

    @Test
    public void testBuildGlueCloudCredential() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("aws.glue.access_key", "ak");
        hiveConf.set("aws.glue.secret_key", "sk");
        hiveConf.set("aws.glue.region", "us-west-1");
        AwsCloudCredential awsCloudCredential = CloudConfigurationFactory.buildGlueCloudCredential(hiveConf);
        Assertions.assertNotNull(awsCloudCredential);
        Assertions.assertEquals("AWSCloudCredential{useAWSSDKDefaultBehavior=false, " +
                "useInstanceProfile=false, useWebIdentityProfile=false, accessKey='ak', secretKey='sk', " +
                "sessionToken='', iamRoleArn='', stsRegion='', stsEndpoint='', externalId='', " +
                "region='us-west-1', endpoint=''}",
                awsCloudCredential.toCredString());

        hiveConf = new HiveConf();
        awsCloudCredential = CloudConfigurationFactory.buildGlueCloudCredential(hiveConf);
        Assertions.assertNull(awsCloudCredential);
    }

    @Test
    public void testForAwsRegion() {
        Map<String, String>  properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        properties.put("aws.s3.endpoint", "endpoint");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assertions.assertEquals("us-east-1", configuration.get("fs.s3a.endpoint.region"));
    }

    @Test
    public void testS3AssumeRoleRegionEndpoint() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        properties.put("aws.s3.iam_role_arn", "arn");
        properties.put("aws.s3.sts.endpoint", "endpoint");
        {
            CloudConfiguration cloudConfiguration =
                    CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
            Assertions.assertNotNull(cloudConfiguration);
            Configuration configuration = new Configuration();
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> cloudConfiguration.applyToConfiguration(configuration));
        }

        properties.put("aws.s3.sts.region", "region");
        {
            CloudConfiguration cloudConfiguration =
                    CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
            Assertions.assertNotNull(cloudConfiguration);
            Configuration configuration = new Configuration();
            cloudConfiguration.applyToConfiguration(configuration);
            Assertions.assertEquals("region", configuration.get("fs.s3a.assumed.role.sts.endpoint.region"));
            Assertions.assertEquals("endpoint", configuration.get("fs.s3a.assumed.role.sts.endpoint"));
        }
    }

    @Test
    public void testGlueAssumeRoleRegionEndpoint() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("aws.glue.access_key", "ak");
        hiveConf.set("aws.glue.secret_key", "sk");
        hiveConf.set("aws.glue.iam_role_arn", "arn");
        hiveConf.set("aws.glue.sts.endpoint", "endpoint");
        {
            AwsCloudCredential credential = CloudConfigurationFactory.buildGlueCloudCredential(hiveConf);
            Assertions.assertNotNull(credential);
            // After fixing ensureSchemeInEndpoint, the endpoint URI is now properly formatted,
            // so AWS SDK validates the configuration and throws SdkClientException when region
            // is missing (instead of NullPointerException from malformed URI)
            Assertions.assertThrows(SdkClientException.class, credential::generateAWSCredentialsProvider);
        }

        hiveConf.set("aws.glue.sts.region", "region");
        {
            AwsCloudCredential credential = CloudConfigurationFactory.buildGlueCloudCredential(hiveConf);
            Assertions.assertNotNull(credential);
        }
    }

    // A valid SSE-C key is a base64-encoded 256-bit (32-byte) value.
    private static final String VALID_SSE_C_KEY =
            Base64.getEncoder().encodeToString("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));

    @Test
    public void testSseCApplyToConfigurationAndThrift() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        properties.put("aws.s3.sse.type", "sse-c");
        properties.put("aws.s3.sse.customer_key", VALID_SSE_C_KEY);
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Assertions.assertNotNull(cloudConfiguration);

        // FE metadata / S3A path: Hadoop S3A SSE-C keys are set.
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assertions.assertEquals("SSE-C", configuration.get("fs.s3a.encryption.algorithm"));
        Assertions.assertEquals(VALID_SSE_C_KEY, configuration.get("fs.s3a.encryption.key"));

        // BE data path: SSE-C material is carried in the thrift cloud_properties, with a computed MD5.
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        Map<String, String> thriftProperties = tCloudConfiguration.getCloud_properties();
        Assertions.assertEquals("sse-c", thriftProperties.get("aws.s3.sse.type"));
        Assertions.assertEquals(VALID_SSE_C_KEY, thriftProperties.get("aws.s3.sse.customer_key"));
        String expectedMd5 = AwsSseCUtil.computeMd5(Base64.getDecoder().decode(VALID_SSE_C_KEY));
        Assertions.assertEquals(expectedMd5, thriftProperties.get("aws.s3.sse.customer_key_md5"));
    }

    @Test
    public void testNoSseCByDefault() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        CloudConfiguration cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        Configuration configuration = new Configuration();
        cloudConfiguration.applyToConfiguration(configuration);
        Assertions.assertNull(configuration.get("fs.s3a.encryption.algorithm"));
        Assertions.assertNull(configuration.get("fs.s3a.encryption.key"));

        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        Assertions.assertFalse(tCloudConfiguration.getCloud_properties().containsKey("aws.s3.sse.type"));
        Assertions.assertFalse(tCloudConfiguration.getCloud_properties().containsKey("aws.s3.sse.customer_key"));
    }

    @Test
    public void testSseCInvalidKeyRejected() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        properties.put("aws.s3.sse.type", "sse-c");
        // Decodes to fewer than 32 bytes.
        properties.put("aws.s3.sse.customer_key", Base64.getEncoder().encodeToString("short".getBytes()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudConfigurationFactory.buildCloudConfigurationForStorage(properties));
    }

    @Test
    public void testSseCMissingKeyRejected() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        properties.put("aws.s3.sse.type", "sse-c");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudConfigurationFactory.buildCloudConfigurationForStorage(properties));
    }

    @Test
    public void testSseCUnknownTypeRejected() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        properties.put("aws.s3.sse.type", "sse-kms");
        properties.put("aws.s3.sse.customer_key", VALID_SSE_C_KEY);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudConfigurationFactory.buildCloudConfigurationForStorage(properties));
    }

    @Test
    public void testEnablePartitionedPrefixConfiguration() {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.s3.access_key", "ak");
        properties.put("aws.s3.secret_key", "sk");
        properties.put("aws.s3.iam_role_arn", "arn");
        properties.put("aws.s3.sts.endpoint", "endpoint");

        {
            CloudConfiguration cloudConfiguration =
                    CloudConfigurationFactory.buildCloudConfigurationForStorage(properties, true);
            Assertions.assertNotNull(cloudConfiguration);
            Assertions.assertTrue(cloudConfiguration instanceof AwsCloudConfiguration);
            FileStoreInfo fsInfo = cloudConfiguration.toFileStoreInfo();
            Assertions.assertFalse(fsInfo.getS3FsInfo().getPartitionedPrefixEnabled());
            Assertions.assertEquals(0, fsInfo.getS3FsInfo().getNumPartitionedPrefix());
        }

        properties.put("aws.s3.enable_partitioned_prefix", "true");
        {
            CloudConfiguration cloudConfiguration =
                    CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
            Assertions.assertTrue(cloudConfiguration instanceof AwsCloudConfiguration);
            FileStoreInfo fsInfo = cloudConfiguration.toFileStoreInfo();
            Assertions.assertTrue(fsInfo.getS3FsInfo().getPartitionedPrefixEnabled());
            // set default to 256
            Assertions.assertEquals(256, fsInfo.getS3FsInfo().getNumPartitionedPrefix());
        }

        properties.put("aws.s3.num_partitioned_prefix", "not_a_number");
        {
            // invalid number for partitioned_prefix property
            Assertions.assertThrows(IllegalArgumentException.class, () ->
                    CloudConfigurationFactory.buildCloudConfigurationForStorage(properties));
        }

        properties.put("aws.s3.num_partitioned_prefix", "-12");
        {
            // must be positive integer
            Assertions.assertThrows(IllegalArgumentException.class, () ->
                    CloudConfigurationFactory.buildCloudConfigurationForStorage(properties));
        }
        properties.put("aws.s3.num_partitioned_prefix", "1024");
        {
            CloudConfiguration cloudConfiguration =
                    CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
            Assertions.assertTrue(cloudConfiguration instanceof AwsCloudConfiguration);
            FileStoreInfo fsInfo = cloudConfiguration.toFileStoreInfo();
            Assertions.assertTrue(fsInfo.getS3FsInfo().getPartitionedPrefixEnabled());
            Assertions.assertEquals(1024, fsInfo.getS3FsInfo().getNumPartitionedPrefix());
        }
    }
}
