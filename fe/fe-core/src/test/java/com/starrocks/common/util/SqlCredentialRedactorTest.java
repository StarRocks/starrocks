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
import org.junit.jupiter.api.Timeout;

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
    public void testRedactFilesCredentialScenarios() {
        // Case 1: INSERT INTO ... SELECT FROM FILES(...)
        String insertSelectSql = "INSERT INTO t0 SELECT * FROM FILES(\n" +
                "        \"path\" = \"s3://bucket/data.parquet\",\n" +
                "        \"format\" = \"parquet\",\n" +
                "        \"aws.s3.access_key\" = \"AKIA_INSERT_SOURCE\",\n" +
                "        \"aws.s3.secret_key\" = \"SOURCE_SECRET\"\n" +
                ")";
        String insertSelectRedacted = SqlCredentialRedactor.redact(insertSelectSql);
        String insertSelectExpected = "INSERT INTO t0 SELECT * FROM FILES(\n" +
                "        \"path\" = \"s3://bucket/data.parquet\",\n" +
                "        \"format\" = \"parquet\",\n" +
                "        \"aws.s3.access_key\" = ***,\n" +
                "        \"aws.s3.secret_key\" = ***\n" +
                ")";
        Assertions.assertEquals(insertSelectExpected, insertSelectRedacted);

        // Case 2: INSERT INTO FILES(...) SELECT ...
        String insertIntoFilesSql = "INSERT INTO FILES(\n" +
                "        \"path\" = \"s3://bucket/output/\",\n" +
                "        \"format\" = \"parquet\",\n" +
                "        \"aws.s3.access_key\" = \"AKIA_INSERT_TARGET\",\n" +
                "        \"aws.s3.secret_key\" = \"TARGET_SECRET\"\n" +
                ")\n" +
                "SELECT 1";
        String insertIntoFilesRedacted = SqlCredentialRedactor.redact(insertIntoFilesSql);
        Assertions.assertFalse(insertIntoFilesRedacted.contains("AKIA_INSERT_TARGET"),
                "FILES target access key should be redacted");
        Assertions.assertFalse(insertIntoFilesRedacted.contains("TARGET_SECRET"),
                "FILES target secret key should be redacted");
        Assertions.assertTrue(insertIntoFilesRedacted.contains("***"), "Should contain redacted marker");
    }

    @Test
    public void testMayNeedCredentialRedactionScenarios() {
        // Case 1: null/empty/plain SQL should be skipped.
        Assertions.assertFalse(SqlCredentialRedactor.mayNeedCredentialRedaction(null));
        Assertions.assertFalse(SqlCredentialRedactor.mayNeedCredentialRedaction(""));
        Assertions.assertFalse(SqlCredentialRedactor.mayNeedCredentialRedaction("SELECT 1"));
        Assertions.assertFalse(SqlCredentialRedactor.mayNeedCredentialRedaction("SELECT * FROM t0 WHERE id = 1"));

        // Case 2: direct credential markers should be detected.
        Assertions.assertTrue(SqlCredentialRedactor.mayNeedCredentialRedaction(
                "\"aws.s3.secret_key\" = \"x\""));
        Assertions.assertTrue(SqlCredentialRedactor.mayNeedCredentialRedaction(
                "CREATE USER u IDENTIFIED BY 'secret'"));
        Assertions.assertTrue(SqlCredentialRedactor.mayNeedCredentialRedaction(
                "SET PASSWORD FOR u = PASSWORD('p')"));

        // Case 3: whitespace variations in IDENTIFIED clauses should be detected.
        Assertions.assertTrue(SqlCredentialRedactor.mayNeedCredentialRedaction(
                "CREATE USER u IDENTIFIED\nBY 'secret'"));
        Assertions.assertTrue(SqlCredentialRedactor.mayNeedCredentialRedaction(
                "CREATE USER u IDENTIFIED\t BY 'secret'"));
        Assertions.assertTrue(SqlCredentialRedactor.mayNeedCredentialRedaction(
                "ALTER USER u IDENTIFIED\n  WITH mysql_native_password BY 'x'"));
        Assertions.assertTrue(SqlCredentialRedactor.mayNeedCredentialRedaction(
                "SET\n PASSWORD FOR u = PASSWORD('p')"));
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
    public void testRedactAzureOAuth2ClientId() {
        String sql = "select * from FILES(\n" +
                "    \"path\" = \"abfs://container@account.dfs.core.windows.net/path\",\n" +
                "    \"format\" = \"parquet\",\n" +
                "    \"azure.adls2.oauth2_client_id\" = \"fab0d1b8-3af6-4e75-b934-a6f7f93dedd7\",\n" +
                "    \"azure.adls2.oauth2_client_secret\" = \"DevId_A08-MpqyRQGByA5\",\n" +
                "    \"azure.adls2.oauth2_client_endpoint\" = \"https://login.microsoftonline.com/tenant-id/oauth2/token\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("fab0d1b8-3af6-4e75-b934-a6f7f93dedd7"),
                "OAuth2 client_id should be redacted");
        Assertions.assertFalse(redacted.contains("DevId_A08-MpqyRQGByA5"),
                "OAuth2 client_secret should be redacted");
        Assertions.assertTrue(redacted.contains("oauth2_client_endpoint"),
                "oauth2_client_endpoint key should remain");
        Assertions.assertTrue(redacted.contains("https://login.microsoftonline.com"),
                "OAuth endpoint URL (non-credential) should remain");
        Assertions.assertTrue(redacted.contains("***"), "Should contain redacted marker");
    }

    @Test
    public void testRedactAzureBlobOAuth2ClientId() {
        String sql = "select * from FILES(\n" +
                "    \"path\" = \"abfs://container@blobaccount.dfs.core.windows.net/path\",\n" +
                "    \"format\" = \"parquet\",\n" +
                "    \"azure.blob.oauth2_client_id\" = \"11111111-2222-3333-4444-555555555555\",\n" +
                "    \"azure.blob.oauth2_client_secret\" = \"BlobSecret123!\",\n" +
                "    \"azure.blob.oauth2_client_endpoint\" = " +
                "        \"https://login.microsoftonline.com/blob-tenant/oauth2/token\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("11111111-2222-3333-4444-555555555555"),
                "Azure Blob OAuth2 client_id should be redacted");
        Assertions.assertFalse(redacted.contains("BlobSecret123!"),
                "Azure Blob OAuth2 client_secret should be redacted");
        Assertions.assertTrue(redacted.contains("oauth2_client_endpoint"),
                "Azure Blob oauth2_client_endpoint key should remain");
        Assertions.assertTrue(redacted.contains("https://login.microsoftonline.com"),
                "Azure Blob OAuth endpoint URL (non-credential) should remain");
        Assertions.assertTrue(redacted.contains("***"), "Should contain redacted marker");
    }

    @Test
    public void testRedactAzureAdls1OAuth2ClientId() {
        String sql = "select * from FILES(\n" +
                "    \"path\" = \"abfs://container@adls1account.dfs.core.windows.net/path\",\n" +
                "    \"format\" = \"parquet\",\n" +
                "    \"azure.adls1.oauth2_client_id\" = \"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\",\n" +
                "    \"azure.adls1.oauth2_credential\" = \"Adls1Secret456!\",\n" +
                "    \"azure.adls1.oauth2_client_endpoint\" = " +
                "        \"https://login.microsoftonline.com/adls1-tenant/oauth2/token\"\n" +
                ")";

        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertFalse(redacted.contains("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "Azure ADLS1 OAuth2 client_id should be redacted");
        Assertions.assertFalse(redacted.contains("Adls1Secret456!"),
                "Azure ADLS1 OAuth2 credential should be redacted");
        Assertions.assertTrue(redacted.contains("oauth2_client_endpoint"),
                "Azure ADLS1 oauth2_client_endpoint key should remain");
        Assertions.assertTrue(redacted.contains("https://login.microsoftonline.com"),
                "Azure ADLS1 OAuth endpoint URL (non-credential) should remain");
        Assertions.assertTrue(redacted.contains("***"), "Should contain redacted marker");
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

        // Test without quotes on key(not supported)
        String sql2 = "aws.s3.secret_key = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"";
        String redacted2 = SqlCredentialRedactor.redact(sql2);
        Assertions.assertTrue(redacted2.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));

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

    @Test
    public void testRedactCreateAndAlterUserCredentialClauses() {
        String createPlainSql = "CREATE USER 'u1' IDENTIFIED BY 'secret'";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED BY '***'",
                SqlCredentialRedactor.redact(createPlainSql));

        String createHashedSql = "CREATE USER 'u1' IDENTIFIED BY PASSWORD '*59C70DA2'";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED BY PASSWORD '***'",
                SqlCredentialRedactor.redact(createHashedSql));

        String createNativePluginSql = "CREATE USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2'";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '***'",
                SqlCredentialRedactor.redact(createNativePluginSql));

        String createLdapSql = "CREATE USER 'u1' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE "
                + "AS 'uid=test,dc=example,dc=io'";
        Assertions.assertEquals(createLdapSql, SqlCredentialRedactor.redact(createLdapSql));

        String alterPlainSql = "ALTER USER 'u1' IDENTIFIED BY 'secret'";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED BY '***'",
                SqlCredentialRedactor.redact(alterPlainSql));

        String alterHashedSql = "ALTER USER 'u1' IDENTIFIED BY PASSWORD '*59C70DA2'";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED BY PASSWORD '***'",
                SqlCredentialRedactor.redact(alterHashedSql));

        String alterNativePluginSql = "ALTER USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'secret'";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY '***'",
                SqlCredentialRedactor.redact(alterNativePluginSql));

        String alterLdapSql = "ALTER USER 'u1' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE "
                + "BY 'uid=test,dc=example,dc=io'";
        Assertions.assertEquals(alterLdapSql, SqlCredentialRedactor.redact(alterLdapSql));
    }

    @Test
    public void testRedactSetPasswordClause() {
        String plainSql = "SET PASSWORD = 'secret'";
        Assertions.assertEquals("SET PASSWORD = '***'", SqlCredentialRedactor.redact(plainSql));

        String plainForUserSql = "SET PASSWORD FOR 'test'@'%' = 'secret'";
        Assertions.assertEquals("SET PASSWORD FOR 'test'@'%' = '***'",
                SqlCredentialRedactor.redact(plainForUserSql));

        String sql = "SET PASSWORD FOR 'test'@'%' = PASSWORD('secret')";
        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertEquals("SET PASSWORD FOR 'test'@'%' = PASSWORD('***')", redacted);
    }

    /**
     * java.regex uses NFA algorithm, it cannot guarantee O(N) complexity, might run into timeout
     * when the string is very long.
     * RE2 is O(n)
     */
    @Test
    @Timeout(10)
    public void testLongString() {
        {
            // long key
            String longString = "1234567890".repeat(104857);
            String longSql = "INSERT INTO t1 VALUES(\"" + longString + "\", 1)";
            String redacted = SqlCredentialRedactor.redact(longSql);
            Assertions.assertEquals(longSql, redacted);
        }
        {
            // long key & value
            String longString = "a".repeat(104857);
            String longSql = "ALTER TABLE t1 SET PROPERTIES(\"" + longString + "\"= \"" + longString + "\")";
            String redacted = SqlCredentialRedactor.redact(longSql);
            Assertions.assertEquals(longSql, redacted);
            longSql = "ALTER TABLE t1 SET PROPERTIES('" + longString + "'= '" + longString + "')";
            redacted = SqlCredentialRedactor.redact(longSql);
            Assertions.assertEquals(longSql, redacted);
        }
        {
            // long value
            String longString = '"' + "a".repeat(104857) + '"';
            String longSql = "ALTER  TABLE t1 SET PROPERTIES('key'= " + longString + ")";
            String redacted = SqlCredentialRedactor.redact(longSql);
            Assertions.assertEquals(longSql, redacted);
        }
    }
}
