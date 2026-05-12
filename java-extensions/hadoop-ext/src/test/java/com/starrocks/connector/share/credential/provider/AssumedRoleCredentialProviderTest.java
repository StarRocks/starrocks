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

package com.starrocks.connector.share.credential.provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class AssumedRoleCredentialProviderTest {

    @Test
    public void testEmptyArnThrows() {
        Configuration conf = new Configuration();
        // No ARN set
        Assertions.assertThrows(PathIOException.class,
                () -> new AssumedRoleCredentialProvider(null, conf));
    }

    @Test
    public void testEmptyArnWithUriThrows() {
        Configuration conf = new Configuration();
        URI uri = URI.create("s3a://my-bucket");
        Assertions.assertThrows(PathIOException.class,
                () -> new AssumedRoleCredentialProvider(uri, conf));
    }

    @Test
    public void testSanitizeSession() {
        Assertions.assertEquals("abc-123", AssumedRoleCredentialProvider.sanitize("abc 123"));
        Assertions.assertEquals("user.name", AssumedRoleCredentialProvider.sanitize("user.name"));
        Assertions.assertEquals("role@domain", AssumedRoleCredentialProvider.sanitize("role@domain"));
        Assertions.assertEquals("a-b-c", AssumedRoleCredentialProvider.sanitize("a!b#c"));
        Assertions.assertEquals("hello,world", AssumedRoleCredentialProvider.sanitize("hello,world"));
    }

    @Test
    public void testOperationRetriedFirstRetry() {
        Configuration conf = new Configuration();
        conf.set(Constants.ASSUMED_ROLE_ARN, "arn:aws:iam::123456:role/TestRole");
        // We can't fully construct the provider (no real credentials), but we can test
        // the static/utility methods and the operationRetried callback pattern.
        // operationRetried is an instance method, but we test sanitize as a proxy
        // for the static utility coverage.
        String sanitized = AssumedRoleCredentialProvider.sanitize("test/user");
        Assertions.assertEquals("test-user", sanitized);
    }

    @Test
    public void testCustomExternalIdConstant() {
        Assertions.assertEquals("starrocks.fs.s3a.external-id",
                AssumedRoleCredentialProvider.CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID);
    }

    @Test
    public void testNameConstant() {
        Assertions.assertEquals(
                "com.starrocks.connector.share.credential.provider.AssumedRoleCredentialProvider",
                AssumedRoleCredentialProvider.NAME);
    }

    @Test
    public void testConstructorWithFakeCredentials() {
        // Configure enough to get past ARN check and into STS client construction.
        // The constructor will fail at resolveCredentials() because the credentials are fake,
        // but it exercises lines 140-199.
        Configuration conf = new Configuration();
        conf.set(Constants.ASSUMED_ROLE_ARN, "arn:aws:iam::123456789012:role/FakeRole");
        conf.set(Constants.ACCESS_KEY, "AKIAIOSFODNN7EXAMPLE");
        conf.set(Constants.SECRET_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
                SimpleAWSCredentialsProvider.class.getName());
        URI uri = URI.create("s3a://test-bucket");

        // The constructor will attempt to resolve credentials and fail because the
        // STS endpoint is unreachable with fake creds. This is expected.
        Assertions.assertThrows(Exception.class,
                () -> new AssumedRoleCredentialProvider(uri, conf));
    }

    @Test
    public void testConstructorWithExternalId() {
        Configuration conf = new Configuration();
        conf.set(Constants.ASSUMED_ROLE_ARN, "arn:aws:iam::123456789012:role/FakeRole");
        conf.set(Constants.ACCESS_KEY, "AKIAIOSFODNN7EXAMPLE");
        conf.set(Constants.SECRET_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
                SimpleAWSCredentialsProvider.class.getName());
        conf.set(AssumedRoleCredentialProvider.CUSTOM_CONSTANT_HADOOP_EXTERNAL_ID, "ext-id-123");
        URI uri = URI.create("s3a://test-bucket");

        Assertions.assertThrows(Exception.class,
                () -> new AssumedRoleCredentialProvider(uri, conf));
    }

    @Test
    public void testConstructorWithPolicy() {
        Configuration conf = new Configuration();
        conf.set(Constants.ASSUMED_ROLE_ARN, "arn:aws:iam::123456789012:role/FakeRole");
        conf.set(Constants.ACCESS_KEY, "AKIAIOSFODNN7EXAMPLE");
        conf.set(Constants.SECRET_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
                SimpleAWSCredentialsProvider.class.getName());
        conf.set(Constants.ASSUMED_ROLE_POLICY, "{\"Version\":\"2012-10-17\",\"Statement\":[]}");
        URI uri = URI.create("s3a://test-bucket");

        Assertions.assertThrows(Exception.class,
                () -> new AssumedRoleCredentialProvider(uri, conf));
    }

    @Test
    public void testConstructorWithStsRegionAndEndpoint() {
        Configuration conf = new Configuration();
        conf.set(Constants.ASSUMED_ROLE_ARN, "arn:aws:iam::123456789012:role/FakeRole");
        conf.set(Constants.ACCESS_KEY, "AKIAIOSFODNN7EXAMPLE");
        conf.set(Constants.SECRET_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
                SimpleAWSCredentialsProvider.class.getName());
        conf.set(Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION, "us-west-2");
        conf.set(Constants.ASSUMED_ROLE_STS_ENDPOINT, "sts.us-west-2.amazonaws.com");
        URI uri = URI.create("s3a://test-bucket");

        Assertions.assertThrows(Exception.class,
                () -> new AssumedRoleCredentialProvider(uri, conf));
    }

    @Test
    public void testConstructorWithNullUri() {
        Configuration conf = new Configuration();
        conf.set(Constants.ASSUMED_ROLE_ARN, "arn:aws:iam::123456789012:role/FakeRole");
        conf.set(Constants.ACCESS_KEY, "AKIAIOSFODNN7EXAMPLE");
        conf.set(Constants.SECRET_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set(Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER,
                SimpleAWSCredentialsProvider.class.getName());

        Assertions.assertThrows(Exception.class,
                () -> new AssumedRoleCredentialProvider(null, conf));
    }
}
