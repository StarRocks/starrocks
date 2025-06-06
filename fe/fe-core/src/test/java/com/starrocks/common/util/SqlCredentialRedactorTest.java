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

import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertFalse("Access key should be redacted", redacted.contains("AKIA3NNAD3JMMNRBH"));
        Assert.assertFalse("Secret key should be redacted", redacted.contains("KnJRWyHrQP0aN4B8Wo3X5hlcIIhU9q+Zxc"));
        Assert.assertTrue("Non-sensitive values should remain", redacted.contains("us-east-1"));
        Assert.assertTrue("Should contain redacted marker", redacted.contains("***"));
    }

    @Test
    public void testRedactAzureCredentials() {
        String sql = "CREATE EXTERNAL TABLE test (\n" +
                "    id INT\n" +
                ") ENGINE=file\n" +
                "PROPERTIES (\n" +
                "    \"path\" = \"abfs://container@account.dfs.core.windows.net/path\",\n" +
                "    \"azure.blob.shared_key\" = \"base64encodedkey==\",\n" +
                "    \"azure.blob.sas_token\" = \"?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assert.assertFalse("Shared key should be redacted", redacted.contains("base64encodedkey=="));
        Assert.assertFalse("SAS token should be redacted", redacted.contains("sv=2020-08-04"));
        Assert.assertTrue("Path should remain", redacted.contains("abfs://container@account.dfs.core.windows.net/path"));
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
        Assert.assertFalse("Private key should be redacted", redacted.contains("BEGIN PRIVATE KEY"));
        Assert.assertFalse("Private key ID should be redacted", redacted.contains("key123456789"));
        Assert.assertTrue("Type should remain", redacted.contains("hive"));
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
        Assert.assertFalse("Hadoop password should be redacted", redacted.contains("hdfs_password123"));
        Assert.assertFalse("Broker password should be redacted", redacted.contains("broker_password456"));
        Assert.assertTrue("Username can remain", redacted.contains("hdfs_user"));
    }

    @Test
    public void testRedactWithDifferentFormats() {
        // Test without spaces around equals
        String sql1 = "\"aws.s3.access_key\"=\"AKIAIOSFODNN7EXAMPLE\"";
        String redacted1 = SqlCredentialRedactor.redact(sql1);
        Assert.assertFalse(redacted1.contains("AKIAIOSFODNN7EXAMPLE"));

        // Test without quotes on key
        String sql2 = "aws.s3.secret_key = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"";
        String redacted2 = SqlCredentialRedactor.redact(sql2);
        Assert.assertFalse(redacted2.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));

        // Test with extra spaces
        String sql3 = "\"aws.s3.access_key\"   =   \"AKIAIOSFODNN7EXAMPLE\"";
        String redacted3 = SqlCredentialRedactor.redact(sql3);
        Assert.assertFalse(redacted3.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    @Test
    public void testCaseInsensitive() {
        String sql = "\"AWS.S3.ACCESS_KEY\" = \"AKIAIOSFODNN7EXAMPLE\",\n" +
                "\"aws.s3.SECRET_key\" = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assert.assertFalse("Should redact case-insensitive", redacted.contains("AKIAIOSFODNN7EXAMPLE"));
        Assert.assertFalse("Should redact case-insensitive", redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
    }

    @Test
    public void testNullAndEmptySql() {
        Assert.assertNull(SqlCredentialRedactor.redact(null));
        Assert.assertEquals("", SqlCredentialRedactor.redact(""));
    }

    @Test
    public void testNoCredentials() {
        String sql = "SELECT * FROM table WHERE id = 1";
        Assert.assertEquals(sql, SqlCredentialRedactor.redact(sql));
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
        Assert.assertFalse("Access key should be redacted", redacted.contains("AKIAIOSFODNN7EXAMPLE"));
        Assert.assertFalse("Secret key should be redacted", redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
        Assert.assertTrue("Region should remain", redacted.contains("us-west-2"));
        Assert.assertTrue("Endpoint should remain", redacted.contains("s3.us-west-2.amazonaws.com"));

        // Count occurrences of ***
        int count = 0;
        int index = 0;
        while ((index = redacted.indexOf("***", index)) != -1) {
            count++;
            index += 3;
        }
        Assert.assertEquals("Should have exactly 2 redacted values", 2, count);
    }
}
