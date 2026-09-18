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

import com.starrocks.common.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExplicitCredentialPolicyTest {

    private static final String BLOB_PATH = "wasbs://container@account.blob.core.windows.net/dir/*";

    private static Map<String, String> awsKeys() {
        Map<String, String> p = new HashMap<>();
        p.put("aws.s3.access_key", "ak");
        p.put("aws.s3.secret_key", "sk");
        p.put("aws.s3.region", "us-west-2");
        return p;
    }

    private static Map<String, String> gcpServiceAccount() {
        Map<String, String> p = new HashMap<>();
        p.put("gcp.gcs.service_account_email", "sa@project.iam.gserviceaccount.com");
        p.put("gcp.gcs.service_account_private_key_id", "id");
        p.put("gcp.gcs.service_account_private_key", "key");
        return p;
    }

    private static Map<String, String> blobSharedKey() {
        Map<String, String> p = new HashMap<>();
        p.put("azure.blob.shared_key", "key");
        return p;
    }

    private static Map<String, String> adls2SharedKey() {
        Map<String, String> p = new HashMap<>();
        p.put("azure.adls2.shared_key", "key");
        p.put("azure.adls2.storage_account", "account");
        return p;
    }

    private static void assertViolation(String expected, String path, Map<String, String> properties) {
        Optional<String> violation = ExplicitCredentialPolicy.check(path, properties);
        Assertions.assertTrue(violation.isPresent(), "expected a violation for " + path + " " + properties);
        Assertions.assertTrue(violation.get().contains(expected),
                "expected '" + expected + "' in: " + violation.get());
    }

    private static void assertAllowed(String path, Map<String, String> properties) {
        Optional<String> violation = ExplicitCredentialPolicy.check(path, properties);
        Assertions.assertFalse(violation.isPresent(), () -> violation.get());
    }

    @Test
    public void testAwsExplicitKeys() {
        assertAllowed("s3://bucket/dir/*", awsKeys());
        assertAllowed("s3a://bucket/dir/*", awsKeys());
        assertAllowed("s3://bucket/a/*, s3://bucket/b/*", awsKeys());

        Map<String, String> withToken = awsKeys();
        withToken.put("aws.s3.session_token", "token");
        assertAllowed("s3://bucket/dir/*", withToken);

        Map<String, String> explicitFalse = awsKeys();
        explicitFalse.put("aws.s3.use_instance_profile", "false");
        assertAllowed("s3://bucket/dir/*", explicitFalse);

        // S3-compatible storage reached through an explicit endpoint
        Map<String, String> compatible = awsKeys();
        compatible.put("aws.s3.endpoint", "https://minio.example.com");
        assertAllowed("s3://bucket/dir/*", compatible);
    }

    @Test
    public void testAwsDelegatedIdentity() {
        Map<String, String> p = new HashMap<>();
        p.put("aws.s3.use_instance_profile", "true");
        assertViolation("instance profile", "s3://bucket/dir/*", p);

        p = new HashMap<>();
        p.put("aws.s3.use_aws_sdk_default_behavior", "true");
        assertViolation("default credential chain", "s3://bucket/dir/*", p);

        p = new HashMap<>();
        p.put("aws.s3.use_web_identity_token_file", "true");
        assertViolation("web identity", "s3://bucket/dir/*", p);

        p = new HashMap<>();
        p.put("aws.s3.use_instance_profile", "true");
        p.put("aws.s3.iam_role_arn", "arn:aws:iam::123456789012:role/r");
        assertViolation("instance profile", "s3://bucket/dir/*", p);

        // explicit keys may not chain through a role either
        p = awsKeys();
        p.put("aws.s3.iam_role_arn", "arn:aws:iam::123456789012:role/r");
        assertViolation("iam_role_arn", "s3://bucket/dir/*", p);

        // the flag wins over the keys at runtime, so it is rejected even when keys are present
        p = awsKeys();
        p.put("aws.s3.use_instance_profile", "true");
        assertViolation("instance profile", "s3://bucket/dir/*", p);
    }

    @Test
    public void testNoCredentials() {
        Map<String, String> p = new HashMap<>();
        p.put("format", "parquet");
        assertViolation("no explicit storage credentials", "s3://bucket/dir/*", p);
        assertViolation("no explicit storage credentials", "gs://bucket/dir/*", p);
        assertViolation("no explicit storage credentials", BLOB_PATH, p);
        assertViolation("no explicit storage credentials", "oss://bucket/dir/*", p);
    }

    @Test
    public void testSchemeMustMatchCredentialType() {
        // credentials for another cloud would leave that connector on its default (node) identity
        assertViolation("requires GCP credentials", "gs://bucket/dir/*", awsKeys());
        assertViolation("requires AZURE credentials", BLOB_PATH, awsKeys());
        assertViolation("requires ALIYUN credentials", "oss://bucket/dir/*", awsKeys());
        assertViolation("requires TENCENT credentials", "cosn://bucket/dir/*", awsKeys());
        assertViolation("requires AWS credentials", "s3://bucket/dir/*", gcpServiceAccount());

        assertViolation("different storage types", "s3://bucket/a/*, gs://bucket/b/*", awsKeys());
    }

    @Test
    public void testUnsupportedSchemes() {
        // HDFS is out of scope: its identity comes from the node's core-site.xml / hdfs-site.xml
        assertViolation("not supported", "hdfs://nn:8020/dir/*", awsKeys());
        assertViolation("not supported", "viewfs://cluster/dir/*", awsKeys());

        // Only wasb/wasbs are served by the native Azure SDK; the rest go through Hadoop
        assertViolation("not supported", "abfss://container@account.dfs.core.windows.net/dir/*", adls2SharedKey());
        assertViolation("not supported", "abfs://container@account.dfs.core.windows.net/dir/*", adls2SharedKey());
        assertViolation("not supported", "adl://account.azuredatalakestore.net/dir/*", blobSharedKey());
        assertViolation("not supported", "azblob://container/dir/*", blobSharedKey());
        assertViolation("not supported", "adls2://container/dir/*", adls2SharedKey());

        assertViolation("not supported", "file:///tmp/dir/*", awsKeys());
        assertViolation("not supported", "fake://bucket/dir/*", awsKeys());
        assertViolation("not supported", "tos://bucket/dir/*", awsKeys());
        assertViolation("not supported", "obs://bucket/dir/*", awsKeys());
        assertViolation("not supported", "/no/scheme/*", awsKeys());
        assertViolation("path is empty", "", awsKeys());
        assertViolation("path is empty", null, awsKeys());

        // HdfsFsManager dispatches on lowercase scheme literals, so an upper-case scheme reaches the
        // universal filesystem, which is configured from the node's Hadoop files.
        assertViolation("not supported", "S3://bucket/dir/*", awsKeys());
        assertViolation("not supported", "S3A://bucket/dir/*", awsKeys());
        assertViolation("not supported", "WASBS://container@account.blob.core.windows.net/dir/*", blobSharedKey());
    }

    @Test
    public void testHadoopPropertiesAreRefused() {
        // Hadoop configuration is resolved on the node, and these keys can name a credential provider,
        // remap a filesystem, load node-local files, or assert an unauthenticated identity.
        String[] hadoopKeys = {
                "hadoop.security.authentication",
                "hadoop.username",
                "hadoop.config.resources",
                "hadoop.runtime.jars",
                "hadoop.security.credential.provider.path",
                "hadoop.kerberos.keytab",
                "fs.s3a.aws.credentials.provider",
                "fs.s3a.access.key",
                "fs.hdfs.impl",
                "fs.AbstractFileSystem.hdfs.impl",
                "viewfs.mounttable.default.link./data",
                "dfs.nameservices",
                "io.file.buffer.size",
        };
        for (String key : hadoopKeys) {
            Map<String, String> p = awsKeys();
            p.put(key, "value");
            assertViolation("Hadoop property '" + key + "'", "s3://bucket/dir/*", p);
        }
    }

    @Test
    public void testGcp() {
        assertAllowed("gs://bucket/dir/*", gcpServiceAccount());

        Map<String, String> p = new HashMap<>();
        p.put("gcp.gcs.use_compute_engine_service_account", "true");
        assertViolation("compute engine service account", "gs://bucket/dir/*", p);

        p = gcpServiceAccount();
        p.put("gcp.gcs.impersonation_service_account", "other@project.iam.gserviceaccount.com");
        assertViolation("impersonation", "gs://bucket/dir/*", p);

        p = new HashMap<>();
        p.put("gcp.gcs.use_compute_engine_service_account", "true");
        p.put("gcp.gcs.impersonation_service_account", "other@project.iam.gserviceaccount.com");
        assertViolation("compute engine service account", "gs://bucket/dir/*", p);
    }

    @Test
    public void testAliyunAndTencent() {
        Map<String, String> p = new HashMap<>();
        p.put("aliyun.oss.access_key", "ak");
        p.put("aliyun.oss.secret_key", "sk");
        p.put("aliyun.oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        assertAllowed("oss://bucket/dir/*", p);
        assertViolation("requires AWS credentials", "s3://bucket/dir/*", p);

        p = new HashMap<>();
        p.put("tencent.cos.access_key", "ak");
        p.put("tencent.cos.secret_key", "sk");
        p.put("tencent.cos.endpoint", "cos.ap-beijing.myqcloud.com");
        assertAllowed("cosn://bucket/dir/*", p);
    }

    @Test
    public void testAzureBlobThroughTheNativeSdk() {
        // The native Blob client takes the account from the URI and the credential from azure.blob.*,
        // so a secret written there is the whole of what it will use.
        assertAllowed(BLOB_PATH, blobSharedKey());
        assertAllowed("wasb://container@account.blob.core.windows.net/dir/*", blobSharedKey());

        Map<String, String> p = new HashMap<>();
        p.put("azure.blob.sas_token", "sas");
        assertAllowed(BLOB_PATH, p);

        p = new HashMap<>();
        p.put("azure.blob.oauth2_client_id", "client");
        p.put("azure.blob.oauth2_client_secret", "secret");
        p.put("azure.blob.oauth2_tenant_id", "tenant");
        assertAllowed(BLOB_PATH, p);

        // Managed identity is the node's identity.
        p = new HashMap<>();
        p.put("azure.blob.oauth2_use_managed_identity", "true");
        p.put("azure.blob.oauth2_client_id", "client");
        assertViolation("managed identity", BLOB_PATH, p);

        p = blobSharedKey();
        p.put("azure.blob.oauth2_use_managed_identity", "true");
        assertViolation("managed identity", BLOB_PATH, p);

        // A client id on its own would make the Blob client use a managed identity, and it is not a
        // credential this policy can accept, so it never validates as one.
        p = new HashMap<>();
        p.put("azure.blob.oauth2_client_id", "client");
        assertViolation("no explicit storage credentials", BLOB_PATH, p);

        // Gen2 credentials leave the Blob client with nothing of its own to use.
        assertViolation("different Azure storage service", BLOB_PATH, adls2SharedKey());
    }

    @Test
    public void testAzureNeedsTheNativeSdk() {
        boolean saved = Config.azure_use_native_sdk;
        Config.azure_use_native_sdk = false;
        try {
            // With the flag off the same path is opened by the Hadoop client instead, which resolves
            // what the properties do not supply from the node's configuration.
            assertViolation("azure_use_native_sdk", BLOB_PATH, blobSharedKey());
            assertViolation("azure_use_native_sdk", "wasb://container@account.blob.core.windows.net/dir/*",
                    blobSharedKey());

            // Other clouds are unaffected.
            assertAllowed("s3://bucket/dir/*", awsKeys());
        } finally {
            Config.azure_use_native_sdk = saved;
        }
    }

    @Test
    public void testViolationsDoNotEchoSecretsFromThePath() {
        // A rejected URI is user-controlled and is written to the FE log, so the query string (where a
        // SAS token lives), the object names, and any user-info must not survive.
        String sasPath = "ftp://host/secret-report/dir/*?sv=2021-06-08&sig=SIGNATUREVALUE";
        Optional<String> violation = ExplicitCredentialPolicy.check(sasPath, awsKeys());
        Assertions.assertTrue(violation.isPresent());
        Assertions.assertFalse(violation.get().contains("sig="), violation.get());
        Assertions.assertFalse(violation.get().contains("SIGNATUREVALUE"), violation.get());
        Assertions.assertFalse(violation.get().contains("secret-report"), violation.get());
        Assertions.assertTrue(violation.get().contains("ftp://host/..."), violation.get());

        // A password in the authority is masked whether or not it is percent-encoded.
        Assertions.assertEquals("s3://******@bucket/...",
                ExplicitCredentialPolicy.redactPath("s3://user:hunter2@bucket/key/part-0.parquet"));
        Assertions.assertEquals("s3://******@bucket/...",
                ExplicitCredentialPolicy.redactPath("s3://user%3Ahunter2@bucket/key/part-0.parquet"));
        Assertions.assertEquals("s3://bucket/...",
                ExplicitCredentialPolicy.redactPath("s3://bucket/key/part-0.parquet?X-Amz-Signature=abc"));
        Assertions.assertEquals("wasbs://******@account.blob.core.windows.net/...",
                ExplicitCredentialPolicy.redactPath(BLOB_PATH));
        Assertions.assertEquals("s3://a/..., s3://b/...",
                ExplicitCredentialPolicy.redactPathList("s3://a/x/*, s3://b/y/*"));
        Assertions.assertEquals("<redacted>", ExplicitCredentialPolicy.redactPath("/no/scheme/*"));
        Assertions.assertEquals("<redacted>", ExplicitCredentialPolicy.redactPath(""));
        Assertions.assertEquals("<redacted>", ExplicitCredentialPolicy.redactPath(null));
    }
}
