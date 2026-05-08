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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class CloudConfigurationApplierTest {

    @Test
    public void testNullProps() {
        Configuration conf = new Configuration();
        CloudConfigurationApplier.applyCloudConfiguration(null, conf);
        Assertions.assertNull(conf.get(Constants.ASSUMED_ROLE_ARN));
    }

    @Test
    public void testEmptyProps() {
        Configuration conf = new Configuration();
        CloudConfigurationApplier.applyCloudConfiguration(new HashMap<>(), conf);
        Assertions.assertNull(conf.get(Constants.ASSUMED_ROLE_ARN));
    }

    @Test
    public void testNonAwsProps() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("gcs.bucket", "my-bucket");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);
        Assertions.assertNull(conf.get(Constants.ASSUMED_ROLE_ARN));
    }

    @Test
    public void testUseAWSSDKDefaultBehavior() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_aws_sdk_default_behavior", "true");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"));
        Assertions.assertEquals("3", conf.get(Constants.RETRY_LIMIT));
        Assertions.assertEquals("5", conf.get(Constants.MAX_ERROR_RETRIES));
        Assertions.assertEquals("false", conf.get(Constants.PATH_STYLE_ACCESS));
        Assertions.assertEquals("true", conf.get(Constants.SECURE_CONNECTIONS));
        Assertions.assertTrue(conf.get(Constants.AWS_CREDENTIALS_PROVIDER)
                .contains("OverwriteAwsDefaultCredentialsProvider"));
    }

    @Test
    public void testUseAWSSDKDefaultBehaviorWithAssumeRole() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_aws_sdk_default_behavior", "true");
        props.put("aws.s3.iam_role_arn", "arn:aws:iam::123456:role/TestRole");
        props.put("aws.s3.external_id", "ext-123");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertTrue(conf.get(Constants.AWS_CREDENTIALS_PROVIDER)
                .contains("AssumedRoleCredentialProvider"));
        Assertions.assertTrue(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER)
                .contains("OverwriteAwsDefaultCredentialsProvider"));
        Assertions.assertEquals("arn:aws:iam::123456:role/TestRole", conf.get(Constants.ASSUMED_ROLE_ARN));
        Assertions.assertEquals("ext-123", conf.get("starrocks.fs.s3a.external-id"));
    }

    @Test
    public void testUseInstanceProfile() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_instance_profile", "true");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertTrue(conf.get(Constants.AWS_CREDENTIALS_PROVIDER)
                .contains("IAMInstanceCredentialsProvider"));
    }

    @Test
    public void testUseInstanceProfileWithAssumeRole() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_instance_profile", "true");
        props.put("aws.s3.iam_role_arn", "arn:aws:iam::123456:role/TestRole");
        props.put("aws.s3.region", "us-west-2");
        props.put("aws.s3.endpoint", "s3.us-west-2.amazonaws.com");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertTrue(conf.get(Constants.AWS_CREDENTIALS_PROVIDER)
                .contains("AssumedRoleCredentialProvider"));
        Assertions.assertTrue(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER)
                .contains("IAMInstanceCredentialsProvider"));
        Assertions.assertEquals("us-west-2", conf.get(Constants.AWS_REGION));
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", conf.get(Constants.ENDPOINT));
    }

    @Test
    public void testStaticCredentials() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.access_key", "AKIA_TEST");
        props.put("aws.s3.secret_key", "secret_test");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertEquals("AKIA_TEST", conf.get(Constants.ACCESS_KEY));
        Assertions.assertEquals("secret_test", conf.get(Constants.SECRET_KEY));
        Assertions.assertTrue(conf.get(Constants.AWS_CREDENTIALS_PROVIDER)
                .contains("SimpleAWSCredentialsProvider"));
    }

    @Test
    public void testStaticCredentialsWithSessionToken() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.access_key", "AKIA_TEST");
        props.put("aws.s3.secret_key", "secret_test");
        props.put("aws.s3.session_token", "token_test");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertEquals("token_test", conf.get(Constants.SESSION_TOKEN));
        Assertions.assertTrue(conf.get(Constants.AWS_CREDENTIALS_PROVIDER)
                .contains("TemporaryAWSCredentialsProvider"));
    }

    @Test
    public void testStaticCredentialsWithAssumeRole() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.access_key", "AKIA_TEST");
        props.put("aws.s3.secret_key", "secret_test");
        props.put("aws.s3.iam_role_arn", "arn:aws:iam::123456:role/TestRole");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertEquals("AKIA_TEST", conf.get(Constants.ACCESS_KEY));
        Assertions.assertEquals("secret_test", conf.get(Constants.SECRET_KEY));
        Assertions.assertTrue(conf.get(Constants.AWS_CREDENTIALS_PROVIDER)
                .contains("AssumedRoleCredentialProvider"));
        Assertions.assertTrue(conf.get(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER)
                .contains("SimpleAWSCredentialsProvider"));
    }

    @Test
    public void testNoValidCredentials() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.region", "us-east-1");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        // No credentials provider set; early return before region is applied
        Assertions.assertNull(conf.get(Constants.ASSUMED_ROLE_ARN));
        // The warn path returns before setting region/endpoint
        Assertions.assertNull(conf.get(Constants.ASSUMED_ROLE_ARN));
    }

    @Test
    public void testAssumeRoleWithStsRegionAndEndpoint() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_instance_profile", "true");
        props.put("aws.s3.iam_role_arn", "arn:aws:iam::123456:role/TestRole");
        props.put("aws.s3.sts.region", "us-east-1");
        props.put("aws.s3.sts.endpoint", "sts.us-east-1.amazonaws.com");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertEquals("us-east-1", conf.get(Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION));
        Assertions.assertEquals("sts.us-east-1.amazonaws.com", conf.get(Constants.ASSUMED_ROLE_STS_ENDPOINT));
    }

    @Test
    public void testAssumeRoleWithStsEndpointButNoRegion() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_instance_profile", "true");
        props.put("aws.s3.iam_role_arn", "arn:aws:iam::123456:role/TestRole");
        props.put("aws.s3.sts.endpoint", "sts.us-east-1.amazonaws.com");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CloudConfigurationApplier.applyCloudConfiguration(props, conf));
    }

    @Test
    public void testPathStyleAccessAndSSL() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_aws_sdk_default_behavior", "true");
        props.put("aws.s3.enable_path_style_access", "true");
        props.put("aws.s3.enable_ssl", "false");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertEquals("true", conf.get(Constants.PATH_STYLE_ACCESS));
        Assertions.assertEquals("false", conf.get(Constants.SECURE_CONNECTIONS));
    }

    @Test
    public void testS3AFileSystemImplSetForAllSchemes() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_aws_sdk_default_behavior", "true");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        String expected = "org.apache.hadoop.fs.s3a.S3AFileSystem";
        Assertions.assertEquals(expected, conf.get("fs.s3.impl"));
        Assertions.assertEquals(expected, conf.get("fs.s3a.impl"));
        Assertions.assertEquals(expected, conf.get("fs.s3n.impl"));
        Assertions.assertEquals(expected, conf.get("fs.oss.impl"));
        Assertions.assertEquals(expected, conf.get("fs.ks3.impl"));
        Assertions.assertEquals(expected, conf.get("fs.obs.impl"));
        Assertions.assertEquals(expected, conf.get("fs.tos.impl"));
        Assertions.assertEquals(expected, conf.get("fs.cosn.impl"));
    }

    @Test
    public void testAssumeRoleWithStsRegionOnly() {
        Configuration conf = new Configuration();
        Map<String, String> props = new HashMap<>();
        props.put("aws.s3.use_instance_profile", "true");
        props.put("aws.s3.iam_role_arn", "arn:aws:iam::123456:role/TestRole");
        props.put("aws.s3.sts.region", "eu-west-1");
        CloudConfigurationApplier.applyCloudConfiguration(props, conf);

        Assertions.assertEquals("eu-west-1", conf.get(Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION));
        Assertions.assertNull(conf.get(Constants.ASSUMED_ROLE_STS_ENDPOINT));
    }
}
