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

import com.google.common.collect.ImmutableSet;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LoadStmt;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to redact sensitive credentials from SQL strings.
 * This is primarily used when SQL parsing fails and we need to log the original SQL statement
 * without exposing credentials in audit logs.
 */
public class SqlCredentialRedactor {

    // Set of credential keys that should be redacted
    // some of them are taken from common.util.PrintableMap
    private static final Set<String> CREDENTIAL_KEYS = ImmutableSet.<String>builder()
            .add(CloudConfigurationConstants.AWS_S3_ACCESS_KEY)
            .add(CloudConfigurationConstants.AWS_S3_SECRET_KEY)
            .add(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN)
            .add(CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY)
            .add(CloudConfigurationConstants.AWS_GLUE_SECRET_KEY)
            .add(CloudConfigurationConstants.AWS_GLUE_SESSION_TOKEN)
            .add(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY)
            .add(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN)
            .add(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID)
            .add(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET)
            .add(CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CLIENT_ID)
            .add(CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CREDENTIAL)
            .add(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY)
            .add(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_ID)
            .add(CloudConfigurationConstants.AZURE_ADLS2_SAS_TOKEN)
            .add(CloudConfigurationConstants.AZURE_ADLS2_OAUTH2_CLIENT_SECRET)
            .add(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY)
            .add(CloudConfigurationConstants.GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID)
            .add(CloudConfigurationConstants.HDFS_PASSWORD)
            .add(CloudConfigurationConstants.HDFS_PASSWORD_DEPRECATED)
            .add(CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_DEPRECATED)
            .add(CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB)
            .add(CloudConfigurationConstants.HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED)
            .add(CloudConfigurationConstants.HADOOP_KERBEROS_KEYTAB_CONTENT)
            .add(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY)
            .add(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY)
            .add(CloudConfigurationConstants.TENCENT_COS_ACCESS_KEY)
            .add(CloudConfigurationConstants.TENCENT_COS_SECRET_KEY)
            .add(CreateRoutineLoadStmt.CONFLUENT_SCHEMA_REGISTRY_URL)
            .add(HdfsFsManager.FS_S3A_ACCESS_KEY)
            .add(HdfsFsManager.FS_S3A_SECRET_KEY)
            .add(HdfsFsManager.FS_KS3_ACCESS_KEY)
            .add(HdfsFsManager.FS_KS3_SECRET_KEY)
            .add(HdfsFsManager.FS_OSS_ACCESS_KEY)
            .add(HdfsFsManager.FS_OSS_SECRET_KEY)
            .add(HdfsFsManager.FS_COS_ACCESS_KEY)
            .add(HdfsFsManager.FS_COS_SECRET_KEY)
            .add(HdfsFsManager.FS_OBS_ACCESS_KEY)
            .add(HdfsFsManager.FS_OBS_SECRET_KEY)
            .add(HdfsFsManager.FS_TOS_ACCESS_KEY)
            .add(HdfsFsManager.FS_TOS_SECRET_KEY)
            .add(LoadStmt.BOS_SECRET_ACCESSKEY)
            .add("password")
            .add("passwd")
            .add("pwd")
            .add("property.sasl.password")
            .add("broker.password")
            .build();

    // Pattern to match key-value pairs in SQL
    // This pattern handles cases like:
    // "key"="value"
    // 'key'='value'
    // key=value
    // 'key='value
    // "key="value
    // 'key=value'
    // "key=value"
    // key'=value'
    // key"=value"
    // key'='value
    // key"="value
    // Values can contain spaces and span multiple lines, separated by commas
    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile(
            "(?:([\"']?)(" + String.join("|", CREDENTIAL_KEYS.stream()
                    .map(Pattern::quote)
                    .toArray(String[]::new)) + ")([\"']?))\\s*=\\s*" +
            "(?:([\"'])((?:[^\\\\]|\\\\.)*?)\\4|([^,]*?))" +
            "(?=\\s*,|\\s*$|\\s*\\)|\\s*\\n)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE
    );

    private static final Pattern IDENTIFIED_BY_PATTERN = Pattern.compile(
            "(IDENTIFIED\\s+BY\\s+(?:PASSWORD\\s+)?)" +
                    "(?:'((?:[^'\\\\]|\\\\.)*)'|\"((?:[^\"\\\\]|\\\\.)*)\")",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE
    );

    private static final Pattern IDENTIFIED_WITH_PATTERN = Pattern.compile(
            "(IDENTIFIED\\s+WITH\\s+)" +
                    "([^\\s]+)" +
                    "(\\s+(?:BY|AS)\\s+)" +
                    "(?:'((?:[^'\\\\]|\\\\.)*)'|\"((?:[^\"\\\\]|\\\\.)*)\")",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE
    );

    private static final Pattern SET_PASSWORD_PATTERN = Pattern.compile(
            "(SET\\s+PASSWORD(?:\\s+FOR\\s+.+?)?\\s*=\\s*(?:PASSWORD\\s*\\()?)" +
                    "(?:'((?:[^'\\\\]|\\\\.)*)'|\"((?:[^\"\\\\]|\\\\.)*)\")" +
                    "(\\s*\\))?",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE
    );

    private static final String REDACTED_VALUE = "***";
    private static final String LDAP_SIMPLE_AUTH_PLUGIN = "AUTHENTICATION_LDAP_SIMPLE";

    /**
     * Redact sensitive credentials from SQL string.
     *
     * @param sql the SQL string that may contain credentials
     * @return the SQL string with credentials redacted
     */
    public static String redact(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        sql = redactKeyValueCredentials(sql);
        sql = redactSqlCredentialClause(sql, IDENTIFIED_BY_PATTERN, 1, 2, 3);
        sql = redactIdentifiedWithClause(sql);
        sql = redactSqlCredentialClause(sql, SET_PASSWORD_PATTERN, 1, 2, 3, 4);
        return sql;
    }

    private static String redactKeyValueCredentials(String sql) {
        Matcher matcher = KEY_VALUE_PATTERN.matcher(sql);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String replacement;
            String keyPrefix = matcher.group(1) != null ? matcher.group(1) : "";
            String key = matcher.group(2);
            String keySuffix = matcher.group(3) != null ? matcher.group(3) : "";

            // Determine if value is quoted or unquoted
            if (matcher.group(4) != null && matcher.group(5) != null) {
                // Quoted value case
                String valueQuote = matcher.group(4);
                replacement = keyPrefix + key + keySuffix + " = " + valueQuote + REDACTED_VALUE + valueQuote;
            } else {
                // Unquoted value case
                replacement = keyPrefix + key + keySuffix + " = " + REDACTED_VALUE;
            }

            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(result);
        return result.toString();
    }

    private static String redactIdentifiedWithClause(String sql) {
        Matcher matcher = IDENTIFIED_WITH_PATTERN.matcher(sql);
        StringBuilder result = new StringBuilder(sql.length() + 32);
        int lastEnd = 0;
        while (matcher.find()) {
            result.append(sql, lastEnd, matcher.start());

            String plugin = matcher.group(2);
            if (LDAP_SIMPLE_AUTH_PLUGIN.equalsIgnoreCase(plugin)) {
                result.append(sql, matcher.start(), matcher.end());
            } else {
                result.append(matcher.group(1)).append(plugin).append(matcher.group(3));
                if (matcher.group(4) != null) {
                    result.append('\'').append(REDACTED_VALUE).append('\'');
                } else if (matcher.group(5) != null) {
                    result.append('"').append(REDACTED_VALUE).append('"');
                } else {
                    result.append(REDACTED_VALUE);
                }
            }
            lastEnd = matcher.end();
        }

        result.append(sql, lastEnd, sql.length());
        return result.toString();
    }

    private static String redactSqlCredentialClause(String sql, Pattern pattern,
                                                    int prefixGroup, int singleQuotedGroup, int doubleQuotedGroup) {
        return redactSqlCredentialClause(sql, pattern, prefixGroup, singleQuotedGroup, doubleQuotedGroup, 0);
    }

    private static String redactSqlCredentialClause(String sql, Pattern pattern, int prefixGroup,
                                                    int singleQuotedGroup, int doubleQuotedGroup, int suffixGroup) {
        Matcher matcher = pattern.matcher(sql);
        StringBuilder result = new StringBuilder(sql.length() + 32);
        int lastEnd = 0;
        while (matcher.find()) {
            result.append(sql, lastEnd, matcher.start());

            result.append(matcher.group(prefixGroup));
            if (matcher.group(singleQuotedGroup) != null) {
                result.append('\'').append(REDACTED_VALUE).append('\'');
            } else if (matcher.group(doubleQuotedGroup) != null) {
                result.append('"').append(REDACTED_VALUE).append('"');
            } else {
                result.append(REDACTED_VALUE);
            }
            if (suffixGroup > 0 && matcher.group(suffixGroup) != null) {
                result.append(matcher.group(suffixGroup));
            }
            lastEnd = matcher.end();
        }

        result.append(sql, lastEnd, sql.length());
        return result.toString();
    }
}
