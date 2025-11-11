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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
public class SqlCredentialRedactorBench {

    private String sqlWithManyKeys;
    private String sqlWithFewKeys;
    private String sqlWithoutCredentials;
    private String sqlLong;
    private Pattern oldPattern;

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(SqlCredentialRedactorBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @org.openjdk.jmh.annotations.Setup
    public void setup() {
        // Pre-compile the old pattern (simulating old approach with all keys in regex)
        String[] allKeys = {
                "aws\\.s3\\.access_key", "aws\\.s3\\.secret_key", "aws\\.s3\\.session_token",
                "aws\\.glue\\.access_key", "aws\\.glue\\.secret_key", "aws\\.glue\\.session_token",
                "azure\\.blob\\.shared_key", "azure\\.blob\\.sas_token", "azure\\.blob\\.oauth2_client_secret",
                "azure\\.adls1\\.oauth2_credential", "azure\\.adls2\\.shared_key", "azure\\.adls2\\.sas_token",
                "azure\\.adls2\\.oauth2_client_secret", "gcp\\.gcs\\.service_account_private_key",
                "gcp\\.gcs\\.service_account_private_key_id", "hdfs\\.password",
                "hadoop\\.security\\.authentication\\.kerberos\\.keytab",
                "hadoop\\.security\\.authentication\\.kerberos\\.keytab\\.content",
                "aliyun\\.oss\\.access_key", "aliyun\\.oss\\.secret_key",
                "tencent\\.cos\\.access_key", "tencent\\.cos\\.secret_key",
                "fs\\.s3a\\.access\\.key", "fs\\.s3a\\.secret\\.key", "fs\\.ks3\\.access\\.key",
                "fs\\.ks3\\.secret\\.key",
                "fs\\.oss\\.access\\.key", "fs\\.oss\\.secret\\.key", "fs\\.cos\\.access\\.key",
                "fs\\.cos\\.secret\\.key",
                "fs\\.obs\\.access\\.key", "fs\\.obs\\.secret\\.key", "fs\\.tos\\.access\\.key",
                "fs\\.tos\\.secret\\.key",
                "password", "passwd", "pwd", "property\\.sasl\\.password", "broker\\.password"
        };
        String keysPattern = String.join("|", allKeys);
        oldPattern = Pattern.compile(
                "(?:([\"']?)(" + keysPattern + ")([\"']?))\\s*=\\s*" +
                        "(?:([\"'])((?:[^\\\\]|\\\\.)*?)\\4|([^,()\\n]*?))" +
                        "(?=\\s*,|\\s*$|\\s*\\)|\\s*\\n)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE
        );

        // Build SQL with many key-value pairs (simulates real-world scenario)
        StringBuilder sqlBuilder = new StringBuilder(10000);
        sqlBuilder.append("CREATE EXTERNAL CATALOG test_catalog\n");
        sqlBuilder.append("PROPERTIES (\n");

        // Add many non-sensitive keys
        for (int i = 0; i < 100; i++) {
            sqlBuilder.append("    \"property").append(i).append("\" = \"value").append(i).append("\",\n");
        }

        // Add some sensitive keys
        sqlBuilder.append("    \"aws.s3.access_key\" = \"AKIAIOSFODNN7EXAMPLE\",\n");
        sqlBuilder.append("    \"aws.s3.secret_key\" = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\",\n");
        sqlBuilder.append("    \"password\" = \"secret123\",\n");
        sqlBuilder.append("    \"fs.s3a.access.key\" = \"access_key_value\",\n");
        sqlBuilder.append("    \"fs.s3a.secret.key\" = \"secret_key_value\"\n");
        sqlBuilder.append(")");

        sqlWithManyKeys = sqlBuilder.toString();

        // Build SQL with few keys
        sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE EXTERNAL CATALOG test_catalog\n");
        sqlBuilder.append("PROPERTIES (\n");
        sqlBuilder.append("    \"aws.s3.access_key\" = \"AKIAIOSFODNN7EXAMPLE\",\n");
        sqlBuilder.append("    \"aws.s3.secret_key\" = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"\n");
        sqlBuilder.append(")");
        sqlWithFewKeys = sqlBuilder.toString();

        // SQL without credentials
        sqlWithoutCredentials = "SELECT * FROM table WHERE id = 1";

        // long string
        String longString = "abcdefdhi".repeat(104857);
        sqlLong = "INSERT INTO t1 VALUES(\"" + longString + "\",)";
    }

    @Benchmark
    public void benchmarkManyKeys() {
        SqlCredentialRedactor.redact(sqlWithManyKeys);
    }

    @Benchmark
    public void benchmarkLongString() {
        SqlCredentialRedactor.redact(sqlLong);
    }

    @Benchmark
    public void benchmarkOldVersionLongString() {
        redactOldVersion(sqlLong);
    }

    @Benchmark
    public void benchmarkFewKeys() {
        SqlCredentialRedactor.redact(sqlWithFewKeys);
    }

    @Benchmark
    public void benchmarkNoCredentials() {
        SqlCredentialRedactor.redact(sqlWithoutCredentials);
    }

    @Benchmark
    public void benchmarkOldVersionManyKeys() {
        redactOldVersion(sqlWithManyKeys);
    }

    // Simulate the old version with complex regex pattern containing all keys
    // This uses alternation with many keys, causing regex backtracking performance issues
    private String redactOldVersion(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }

        Matcher matcher = oldPattern.matcher(sql);
        StringBuilder result = new StringBuilder(sql.length() + 100);

        int lastEnd = 0;
        while (matcher.find()) {
            String keyPrefix = matcher.group(1) != null ? matcher.group(1) : "";
            String key = matcher.group(2);
            String keySuffix = matcher.group(3) != null ? matcher.group(3) : "";

            result.append(sql, lastEnd, matcher.start());

            String replacement;
            if (matcher.group(4) != null && matcher.group(5) != null) {
                String valueQuote = matcher.group(4);
                replacement = keyPrefix + key + keySuffix + " = " + valueQuote + "***" + valueQuote;
            } else {
                replacement = keyPrefix + key + keySuffix + " = ***";
            }
            result.append(replacement);
            lastEnd = matcher.end();
        }

        result.append(sql, lastEnd, sql.length());
        return result.toString();
    }
}

