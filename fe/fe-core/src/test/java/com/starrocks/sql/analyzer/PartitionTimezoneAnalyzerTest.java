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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.KeysType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class PartitionTimezoneAnalyzerTest {
    private static final String TIMEZONE_ERROR = "The partition expr should not depend on the session time zone";

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSessionTimezoneOverloadsRejectedForEveryKeysType() {
        for (KeysType keysType : KeysType.values()) {
            for (String expression : new String[] {
                    "from_unixtime(ts)",
                    "from_unixtime(ts, '%Y%m%d')",
                    "from_unixtime_ms(ts)"
            }) {
                analyzeFail(createTableSql(keysType, "BIGINT", expression), TIMEZONE_ERROR);
            }
        }
        // The INT overloads are session-dependent too; an implicit cast must not bypass the check.
        analyzeFail(createTableSql(KeysType.DUP_KEYS, "INT", "from_unixtime(ts)"), TIMEZONE_ERROR);
        analyzeFail(createTableSql(KeysType.DUP_KEYS, "INT", "from_unixtime(ts, '%Y%m%d')"), TIMEZONE_ERROR);
    }

    @Test
    public void testExplicitTimezoneOverloadRetainedForEveryKeysType() {
        for (KeysType keysType : KeysType.values()) {
            analyzeSuccess(createTableSql(keysType, "BIGINT", "from_unixtime(ts, '%Y%m%d', 'UTC')"));
            analyzeSuccess(createTableSql(keysType, "BIGINT", "from_unixtime(ts, '%Y%m%d', 'Asia/Shanghai')"));
        }
        analyzeSuccess(createTableSql(KeysType.DUP_KEYS, "INT", "from_unixtime(ts, '%Y%m%d', 'UTC')"));
    }

    @Test
    public void testNestedTimezoneDependentFunctionsRejected() {
        analyzeFail(createTableSql(KeysType.AGG_KEYS, "BIGINT", "substr(from_unixtime(ts), 1, 10)"), TIMEZONE_ERROR);
        analyzeFail(createTableSql(KeysType.UNIQUE_KEYS, "BIGINT", "substr(from_unixtime_ms(ts), 1, 10)"),
                TIMEZONE_ERROR);
        analyzeFail(createTableSql(KeysType.DUP_KEYS, "BIGINT", "FrOm_UnIxTiMe(ts, '%Y%m%d')"), TIMEZONE_ERROR);
        analyzeSuccess(createTableSql(KeysType.AGG_KEYS, "BIGINT", "substr(from_unixtime(ts, '%Y%m%d', 'UTC'), 1, 4)"));
    }

    @Test
    public void testGeneratedPartitionColumnUsesItsRealExpression() {
        analyzeFail(generatedPartitionTableSql("VARCHAR(32)", "from_unixtime(ts, '%Y%m%d')"), TIMEZONE_ERROR);
        analyzeFail(generatedPartitionTableSql("VARCHAR(32)", "substr(from_unixtime_ms(ts), 1, 10)"), TIMEZONE_ERROR);
        analyzeFail(generatedPartitionTableSql("BIGINT", "ts + CAST(rand() * 2 AS BIGINT)"),
                "The partition expr should be deterministic");
        analyzeSuccess(generatedPartitionTableSql("VARCHAR(32)", "from_unixtime(ts, '%Y%m%d', 'UTC')"));
    }

    @Test
    public void testOrdinaryCallsStillAllowed() {
        analyzeSuccess("SELECT from_unixtime(v1), from_unixtime(v1, '%Y%m%d'), "
                + "from_unixtime(v1, '%Y%m%d', 'UTC'), from_unixtime_ms(v1) FROM t0");
    }

    private static String createTableSql(KeysType keysType, String timestampType, String expression) {
        String aggregateType = keysType == KeysType.AGG_KEYS ? " SUM" : "";
        return "CREATE TABLE test.t_timezone_partition (ts " + timestampType + " NOT NULL, pv BIGINT" + aggregateType + ")\n"
                + keysType.toSql() + "(ts)\n"
                + "PARTITION BY " + expression + "\n"
                + "DISTRIBUTED BY HASH(ts) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')";
    }

    private static String generatedPartitionTableSql(String type, String expression) {
        return "CREATE TABLE test.t_generated_timezone_partition (ts BIGINT NOT NULL, pv BIGINT, p " + type
                + " AS " + expression + ")\n"
                + "DUPLICATE KEY(ts)\n"
                + "PARTITION BY (p)\n"
                + "DISTRIBUTED BY HASH(ts) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')";
    }
}
