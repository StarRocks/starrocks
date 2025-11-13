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
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.LoadStmt;

import java.util.HashSet;
import java.util.Set;

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
            .add(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET)
            .add(CloudConfigurationConstants.AZURE_ADLS1_OAUTH2_CREDENTIAL)
            .add(CloudConfigurationConstants.AZURE_ADLS2_SHARED_KEY)
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

    // Lowercase set for O(1) lookup (case-insensitive matching)
    private static final Set<String> CREDENTIAL_KEYS_LOWERCASE = new HashSet<>();

    static {
        for (String key : CREDENTIAL_KEYS) {
            CREDENTIAL_KEYS_LOWERCASE.add(key.toLowerCase());
        }
    }

    // Simplified pattern to match any key-value pair in SQL
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
    private static final int MAX_KEY_LENGTH =
            CREDENTIAL_KEYS.stream().map(String::length).max(Integer::compareTo).orElse(1);
    // NOTE: MAX_KEY_LENGTH is used to avoid matching too many characters of a long string
    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile(
            "([\"'])" +                                    // quote
                    "([^\"'=\\s,()]{1," + MAX_KEY_LENGTH + "})" + // key
                    "([\"'])" +                                  // quote
                    "\\s*=\\s*" +                                 // =
                    "(?:'((?:[^'\\\\]|\\\\.)*)'|\"((?:[^\"\\\\]|\\\\.)*)\"|([^,()\\n]*))",
            Pattern.DOTALL | Pattern.MULTILINE
    );

    private static final String REDACTED_VALUE = "***";

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

        Matcher matcher = KEY_VALUE_PATTERN.matcher(sql);
        StringBuilder result = new StringBuilder(sql.length() + 100);

        int lastEnd = 0;
        while (matcher.find()) {
            String keyPrefix = matcher.group(1) != null ? matcher.group(1) : "";
            String key = matcher.group(2);
            String keySuffix = matcher.group(3) != null ? matcher.group(3) : "";

            // Check if this key should be redacted (case-insensitive)
            if (CREDENTIAL_KEYS_LOWERCASE.contains(key.toLowerCase())) {
                // Append text before the match
                result.append(sql, lastEnd, matcher.start());

                // Build replacement for redacted value
                String replacement;
                if (matcher.group(4) != null && matcher.group(5) != null) {
                    // Quoted value case
                    String valueQuote = matcher.group(4);
                    replacement = keyPrefix + key + keySuffix + " = " + valueQuote + REDACTED_VALUE + valueQuote;
                } else {
                    // Unquoted value case
                    replacement = keyPrefix + key + keySuffix + " = " + REDACTED_VALUE;
                }
                result.append(replacement);
            } else {
                // Not a credential key, append original match
                result.append(sql, lastEnd, matcher.end());
            }
            lastEnd = matcher.end();
        }

        // Append remaining text
        result.append(sql, lastEnd, sql.length());
        return result.toString();
    }
}
