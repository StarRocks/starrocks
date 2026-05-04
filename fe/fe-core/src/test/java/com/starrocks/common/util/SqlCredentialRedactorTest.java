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

package com.starrocks.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlCredentialRedactorTest {

    @Test
    public void testRedactAwsCredentials() {
        String sql = "select * from FILES(\n" +
                "        \"path\" = \"s3://chaoyli/people.parquet\",\n" +
                "        \"format\" = \"parquet\"\n" +
                "        \"aws.s3.access_key\" = \"AKIA3NNAD3JMMNRBH\",\n" +
                "        \"aws.s3.secret_key\" = \"KnJRWyHrQP0aN4B8Wo3X5hlcIIhU9q+Zxc\",\n" +
                "        \"aws.s3.region\" = \"us-east-1\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("AKIA3NNAD3JMMNRBH"), "Access key should be redacted");
        Assertions.assertFalse(redacted.contains("KnJRWyHrQP0aN4B8Wo3X5hlcIIhU9q+Zxc"), "Secret key should be redacted");
        Assertions.assertTrue(redacted.contains("us-east-1"), "Non-sensitive values should remain");
        Assertions.assertTrue(redacted.contains("***"), "Should contain redacted marker");
    }

    @Test
    public void testRedactAzureCredentials() {
        String sql = "CREATE EXTERNAL TABLE test (\n" +
                "    id INT\n" +
                ") ENGINE=file\n" +
                "PROPERTIES (\n" +
                "    \"path\" = \"abfs://container@account.dfs.core.windows.net/path\",\n" +
                "    \"azure.blob.shared_key\" = \"base64encodedkey==\",\n" +
                "    \"azure.blob.oauth2_client_secret\" = \"abcdefg\",\n" +
                "    \"azure.blob.sas_token\" = \"?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("base64encodedkey=="), "Shared key should be redacted");
        Assertions.assertFalse(redacted.contains("abcdefg"), "oauth2_client_secret should be redacted");
        Assertions.assertFalse(redacted.contains("sv=2020-08-04"), "SAS token should be redacted");
        Assertions.assertTrue(redacted.contains("abfs://container@account.dfs.core.windows.net/path"), "Path should remain");
    }

    @Test
    public void testRedactGcpCredentials() {
        String sql = "CREATE EXTERNAL CATALOG gcs_catalog\n" +
                "PROPERTIES (\n" +
                " \"type\" = \"hive\",\n" +
                " \"gcp.gcs.service_account_private_key\" = " +
                "   \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqh\\n" +
                "     -----END PRIVATE KEY-----\",\n" +
                " \"gcp.gcs.service_account_private_key_id\" = \"key123456789\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("BEGIN PRIVATE KEY"), "Private key should be redacted");
        Assertions.assertFalse(redacted.contains("key123456789"), "Private key ID should be redacted");
        Assertions.assertTrue(redacted.contains("hive"), "Type should remain");
    }

    @Test
    public void testRedactHdfsCredentials() {
        String sql = "LOAD LABEL test_load\n" +
                "(\n" +
                "    DATA INFILE(\"hdfs://namenode:9000/path/to/file\")\n" +
                "    INTO TABLE test_table\n" +
                ")\n" +
                "WITH BROKER hdfs_broker\n" +
                "(\n" +
                "    \"hadoop.security.authentication\" = \"simple\",\n" +
                "    \"username\" = \"hdfs_user\",\n" +
                "    \"password\" = \"hdfs_password123\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("hdfs_password123"), "Hadoop password should be redacted");
        Assertions.assertFalse(redacted.contains("broker_password456"), "Broker password should be redacted");
        Assertions.assertTrue(redacted.contains("hdfs_user"), "Username can remain");
    }

    @Test
    public void testRedactFSCredentials() {
        String sql = "select * from FILES(\n" +
                "        \"path\" = \"s3://fs/people.parquet\",\n" +
                "        \"format\" = \"parquet\"\n" +
                "        \"fs.s3a.access.key\" = \"aaa\",\n" +
                "        \"fs.s3a.secret.key\" = \"bbb\",\n" +
                "        \"fs.ks3.AccessKey\" = \"ccc\",\n" +
                "        \"fs.ks3.AccessSecret\" = \"ddd\",\n" +
                "        \"fs.oss.accessKeyId\" = \"eee\",\n" +
                "        \"fs.oss.accessKeySecret\" = \"fff\",\n" +
                "        \"fs.cosn.userinfo.secretId\" = \"ggg\",\n" +
                "        \"fs.cosn.userinfo.secretKey\" = \"hhh\",\n" +
                "        \"fs.obs.access.key\" = \"iii\",\n" +
                "        \"fs.obs.secret.key\" = \"jjj\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("aaa"), "fs.s3a.access.key should be redacted");
        Assertions.assertFalse(redacted.contains("bbb"), "fs.s3a.secret.key should be redacted");
        Assertions.assertFalse(redacted.contains("ccc"), "fs.ks3.AccessKey should be redacted");
        Assertions.assertFalse(redacted.contains("ddd"), "fs.ks3.AccessSecret should be redacted");
        Assertions.assertFalse(redacted.contains("eee"), "fs.oss.accessKeyId should be redacted");
        Assertions.assertFalse(redacted.contains("fff"), "fs.oss.accessKeySecret should be redacted");
        Assertions.assertFalse(redacted.contains("ggg"), "fs.cosn.userinfo.secretId should be redacted");
        Assertions.assertFalse(redacted.contains("hhh"), "fs.cosn.userinfo.secretKey should be redacted");
        Assertions.assertFalse(redacted.contains("iii"), "fs.obs.access.key should be redacted");
        Assertions.assertFalse(redacted.contains("jjj"), "fs.obs.secret.key should be redacted");
        Assertions.assertTrue(redacted.contains("***"), "Should contain redacted marker");
    }

    @Test
    public void testRedactWithDifferentFormats() {
        // Test without spaces around equals
        String sql1 = "\"aws.s3.access_key\"=\"AKIAIOSFODNN7EXAMPLE\"";
        String redacted1 = SqlCredentialRedactor.redact(sql1);
        Assertions.assertFalse(redacted1.contains("AKIAIOSFODNN7EXAMPLE"));

        // Test without quotes on key
        String sql2 = "aws.s3.secret_key = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"";
        String redacted2 = SqlCredentialRedactor.redact(sql2);
        Assertions.assertFalse(redacted2.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));

        // Test with extra spaces
        String sql3 = "\"aws.s3.access_key\"   =   \"AKIAIOSFODNN7EXAMPLE\"";
        String redacted3 = SqlCredentialRedactor.redact(sql3);
        Assertions.assertFalse(redacted3.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    @Test
    public void testCaseInsensitive() {
        String sql = "\"AWS.S3.ACCESS_KEY\" = \"AKIAIOSFODNN7EXAMPLE\",\n" +
                "\"aws.s3.SECRET_key\" = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("AKIAIOSFODNN7EXAMPLE"), "Should redact case-insensitive");
        Assertions.assertFalse(redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"), "Should redact case-insensitive");
    }

    @Test
    public void testNullAndEmptySql() {
        Assertions.assertNull(SqlCredentialRedactor.redact(null));
        Assertions.assertEquals("", SqlCredentialRedactor.redact(""));
    }

    @Test
    public void testNoCredentials() {
        String sql = "SELECT * FROM table WHERE id = 1";
        Assertions.assertEquals(sql, SqlCredentialRedactor.redact(sql));
    }

    @Test
    public void testMultipleCredentials() {
        String sql = "CREATE STORAGE VOLUME test_volume\n" +
                "TYPE = S3\n" +
                "LOCATIONS = ('s3://bucket/path')\n" +
                "PROPERTIES (\n" +
                "    \"aws.s3.access_key\" = \"AKIAIOSFODNN7EXAMPLE\",\n" +
                "    \"aws.s3.secret_key\" = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\",\n" +
                "    \"aws.s3.region\" = \"us-west-2\",\n" +
                "    \"aws.s3.endpoint\" = \"s3.us-west-2.amazonaws.com\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("AKIAIOSFODNN7EXAMPLE"), "Access key should be redacted");
        Assertions.assertFalse(redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"), "Secret key should be redacted");
        Assertions.assertTrue(redacted.contains("us-west-2"), "Region should remain");
        Assertions.assertTrue(redacted.contains("s3.us-west-2.amazonaws.com"), "Endpoint should remain");

        // Count occurrences of ***
        int count = 0;
        int index = 0;
        while ((index = redacted.indexOf("***", index)) != -1) {
            count++;
            index += 3;
        }
        Assertions.assertEquals(2, count, "Should have exactly 2 redacted values");
    }
}
