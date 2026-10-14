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

import com.starrocks.common.Config;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class PartitionContextAnalyzerTest {
    private static final String SESSION_ERROR = "The partition expr should not depend on session context";
    private static final String UDF_ERROR = "The partition expr should only use built-in functions";

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSessionNodesRejectedInsideFunctionsForEveryKeysType() {
        for (KeysType keysType : KeysType.values()) {
            for (String expression : new String[] {
                    "concat(k, session_user())",
                    "concat(k, current_user())",
                    "concat(k, database())",
                    "concat(k, current_role())",
                    "concat(k, connection_id())",
                    "concat(k, @@session.time_zone)",
                    "concat(k, @partition_context_tag)",
                    "substr(concat(k, session_user()), 1, 32)"
            }) {
                analyzeFail(createTableSql(keysType, expression), SESSION_ERROR);
            }
            analyzeSuccess(createTableSql(keysType, "abs(k)"));
        }
    }

    @Test
    public void testIdentifierPartitionChecksItsRealSessionExpression() {
        for (String expression : new String[] {
                "concat(k, session_user())",
                "concat(k, @@session.time_zone)",
                "concat(k, @partition_context_tag)",
                "substr(concat(k, connection_id()), 1, 32)"
        }) {
            analyzeFail(generatedTableSql("VARCHAR(128)", expression), SESSION_ERROR);
        }
        analyzeSuccess("SELECT session_user(), current_user(), database(), current_role(), connection_id(), "
                + "@@session.time_zone, @partition_context_tag, concat(v1, session_user()) FROM t0");
    }

    @Test
    public void testResolvedSqlAndExternalFunctionsRejectedOnlyForPartitioning() throws Exception {
        boolean originalEnableUdf = Config.enable_udf;
        try {
            AnalyzeTestUtil.starRocksAssert.withFunction(
                    "CREATE FUNCTION test.partition_context_stable(x BIGINT) RETURNS abs(x)");
            AnalyzeTestUtil.starRocksAssert.withFunction(
                    "CREATE FUNCTION test.partition_context_random(x BIGINT) RETURNS x + CAST(rand() * 2 AS BIGINT)");
            AnalyzeTestUtil.starRocksAssert.withFunction(
                    "CREATE FUNCTION test.partition_context_external(BIGINT) RETURNS BIGINT "
                            + "PROPERTIES ('type'='StarrocksJar','file'='file:///partition_context_test.jar',"
                            + "'symbol'='test.PartitionContextUdf')");
            Config.enable_udf = true;

            // These DDLs traverse real function binding in generated-column analysis. The SQL body
            // is not a child of the outer call, and even the stable body can later be replaced.
            for (KeysType keysType : KeysType.values()) {
                for (String expression : new String[] {
                        "test.partition_context_stable(k)",
                        "abs(test.partition_context_stable(k))",
                        "test.partition_context_random(k)",
                        "abs(test.partition_context_random(k))",
                        "test.partition_context_external(k)",
                        "abs(test.partition_context_external(k))"
                }) {
                    analyzeFail(createTableSql(keysType, expression), UDF_ERROR);
                }
            }
            analyzeFail(generatedTableSql("BIGINT", "test.partition_context_stable(k)"), UDF_ERROR);
            analyzeFail(generatedTableSql("BIGINT", "abs(test.partition_context_random(k))"), UDF_ERROR);
            analyzeFail(generatedTableSql("BIGINT", "test.partition_context_external(k)"), UDF_ERROR);

            // Catalog registration is mocked, but function resolution and all DDL checks are real.
            // Ordinary UDF calls and non-partition generated columns must retain their support.
            analyzeSuccess("SELECT test.partition_context_stable(v1), test.partition_context_random(v1), "
                    + "test.partition_context_external(v1) FROM t0");
            analyzeSuccess("CREATE TABLE test.partition_context_ordinary (k BIGINT NOT NULL, "
                    + "g BIGINT AS test.partition_context_stable(k)) DUPLICATE KEY(k) "
                    + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        } finally {
            try {
                for (String name : new String[] {"stable", "random", "external"}) {
                    DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                            "DROP FUNCTION IF EXISTS test.partition_context_" + name + "(BIGINT)",
                            AnalyzeTestUtil.connectContext), AnalyzeTestUtil.connectContext);
                }
            } finally {
                Config.enable_udf = originalEnableUdf;
            }
        }
    }

    private String createTableSql(KeysType keysType, String expression) {
        String aggregate = keysType == KeysType.AGG_KEYS ? " SUM" : "";
        return "CREATE TABLE test.partition_context_table (k BIGINT NOT NULL, pv BIGINT" + aggregate + ") "
                + keysType.toSql() + "(k) PARTITION BY " + expression + " "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')";
    }

    private String generatedTableSql(String type, String expression) {
        return "CREATE TABLE test.partition_context_generated (k BIGINT NOT NULL, pv BIGINT, g " + type
                + " AS " + expression + ") DUPLICATE KEY(k) PARTITION BY (g) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')";
    }
}
