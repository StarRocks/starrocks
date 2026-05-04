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

        String createPlainDoubleQuotedSql = "CREATE USER 'u1' IDENTIFIED BY \"secret\"";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED BY \"***\"",
                SqlCredentialRedactor.redact(createPlainDoubleQuotedSql));

        String createHashedSql = "CREATE USER 'u1' IDENTIFIED BY PASSWORD '*59C70DA2'";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED BY PASSWORD '***'",
                SqlCredentialRedactor.redact(createHashedSql));

        String createHashedDoubleQuotedSql = "CREATE USER 'u1' IDENTIFIED BY PASSWORD \"*59C70DA2\"";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED BY PASSWORD \"***\"",
                SqlCredentialRedactor.redact(createHashedDoubleQuotedSql));

        String createNativePluginSql = "CREATE USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '*59C70DA2'";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS '***'",
                SqlCredentialRedactor.redact(createNativePluginSql));

        String createNativePluginDoubleQuotedSql = "CREATE USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS "
                + "\"*59C70DA2\"";
        Assertions.assertEquals("CREATE USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD AS \"***\"",
                SqlCredentialRedactor.redact(createNativePluginDoubleQuotedSql));

        String createLdapSql = "CREATE USER 'u1' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE "
                + "AS 'uid=test,dc=example,dc=io'";
        Assertions.assertEquals(createLdapSql, SqlCredentialRedactor.redact(createLdapSql));

        String createLdapDoubleQuotedSql = "CREATE USER 'u1' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE "
                + "AS \"uid=test,dc=example,dc=io\"";
        Assertions.assertEquals(createLdapDoubleQuotedSql, SqlCredentialRedactor.redact(createLdapDoubleQuotedSql));

        String alterPlainSql = "ALTER USER 'u1' IDENTIFIED BY 'secret'";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED BY '***'",
                SqlCredentialRedactor.redact(alterPlainSql));

        String alterPlainDoubleQuotedSql = "ALTER USER 'u1' IDENTIFIED BY \"secret\"";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED BY \"***\"",
                SqlCredentialRedactor.redact(alterPlainDoubleQuotedSql));

        String alterHashedSql = "ALTER USER 'u1' IDENTIFIED BY PASSWORD '*59C70DA2'";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED BY PASSWORD '***'",
                SqlCredentialRedactor.redact(alterHashedSql));

        String alterHashedDoubleQuotedSql = "ALTER USER 'u1' IDENTIFIED BY PASSWORD \"*59C70DA2\"";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED BY PASSWORD \"***\"",
                SqlCredentialRedactor.redact(alterHashedDoubleQuotedSql));

        String alterNativePluginSql = "ALTER USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY 'secret'";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY '***'",
                SqlCredentialRedactor.redact(alterNativePluginSql));

        String alterNativePluginDoubleQuotedSql = "ALTER USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY "
                + "\"secret\"";
        Assertions.assertEquals("ALTER USER 'u1' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY \"***\"",
                SqlCredentialRedactor.redact(alterNativePluginDoubleQuotedSql));

        String alterLdapSql = "ALTER USER 'u1' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE "
                + "BY 'uid=test,dc=example,dc=io'";
        Assertions.assertEquals(alterLdapSql, SqlCredentialRedactor.redact(alterLdapSql));

        String alterLdapDoubleQuotedSql = "ALTER USER 'u1' IDENTIFIED WITH AUTHENTICATION_LDAP_SIMPLE "
                + "BY \"uid=test,dc=example,dc=io\"";
        Assertions.assertEquals(alterLdapDoubleQuotedSql, SqlCredentialRedactor.redact(alterLdapDoubleQuotedSql));
    }

    @Test
    public void testRedactSetPasswordClause() {
        String plainSql = "SET PASSWORD = 'secret'";
        Assertions.assertEquals("SET PASSWORD = '***'", SqlCredentialRedactor.redact(plainSql));

        String doubleQuotedPlainSql = "SET PASSWORD = \"secret\"";
        Assertions.assertEquals("SET PASSWORD = \"***\"", SqlCredentialRedactor.redact(doubleQuotedPlainSql));

        String plainForUserSql = "SET PASSWORD FOR 'test'@'%' = 'secret'";
        Assertions.assertEquals("SET PASSWORD FOR 'test'@'%' = '***'",
                SqlCredentialRedactor.redact(plainForUserSql));

        String doubleQuotedPlainForUserSql = "SET PASSWORD FOR 'test'@'%' = \"secret\"";
        Assertions.assertEquals("SET PASSWORD FOR 'test'@'%' = \"***\"",
                SqlCredentialRedactor.redact(doubleQuotedPlainForUserSql));

        String sql = "SET PASSWORD FOR 'test'@'%' = PASSWORD('secret')";
        String redacted = SqlCredentialRedactor.redact(sql);
        Assertions.assertEquals("SET PASSWORD FOR 'test'@'%' = PASSWORD('***')", redacted);

        String doubleQuotedFunctionSql = "SET PASSWORD FOR 'test'@'%' = PASSWORD(\"secret\")";
        redacted = SqlCredentialRedactor.redact(doubleQuotedFunctionSql);
        Assertions.assertEquals("SET PASSWORD FOR 'test'@'%' = PASSWORD(\"***\")", redacted);
    }

    @Test
    public void testExplicitLongLiteralSql() {
        // Use explicit literals instead of generated huge payloads to keep this deterministic in CI.
        String longNonCredentialKey = "this_is_a_very_long_property_key_that_should_not_match_any_credential_pattern_"
                + "because_it_is_plain_metadata_and_not_a_secret";
        String longValue = "this_is_a_long_but_explicit_literal_value_used_to_verify_that_non_credential_entries_"
                + "stay_unchanged_even_when_the_sql_text_is_lengthy_and_contains_many_characters_0123456789";

        String nonCredentialSql = "ALTER TABLE t1 SET PROPERTIES(\"" + longNonCredentialKey + "\" = \"" + longValue + "\")";
        Assertions.assertEquals(nonCredentialSql, SqlCredentialRedactor.redact(nonCredentialSql));

        String credentialSql = "ALTER TABLE t1 SET PROPERTIES("
                + "\"aws.s3.secret_key\" = \"LONG_SECRET_VALUE_ABCDEF1234567890\", "
                + "\"comment\" = \"" + longValue + "\")";
        String redacted = SqlCredentialRedactor.redact(credentialSql);
        Assertions.assertFalse(redacted.contains("LONG_SECRET_VALUE_ABCDEF1234567890"));
        Assertions.assertTrue(redacted.contains("\"aws.s3.secret_key\""));
        Assertions.assertTrue(redacted.contains("***"));
        Assertions.assertTrue(redacted.contains("\"comment\" = \"" + longValue + "\""));
    }
}
