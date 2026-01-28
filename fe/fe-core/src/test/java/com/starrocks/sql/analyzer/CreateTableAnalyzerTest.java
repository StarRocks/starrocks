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
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.RangeDistributionDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateTableAnalyzerTest {

    private static ConnectContext connectContext;


    @BeforeAll
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_create_table_db");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        Config.max_column_number_per_table = 10000;
    }

    @Test
    public void testAnalyze() {
        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` string,\n" +
                "    `tag_name` string\n" +
                ") ENGINE = OLAP PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "ORDER BY(`id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")\n";

        Throwable exception = assertThrows(SemanticException.class, () -> {
            CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                    .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
            CreateTableAnalyzer.analyze(createTableStmt, connectContext);
        });
        assertThat(exception.getMessage(), containsString("doesn't exist"));
    }

    @Test
    public void testAnalyzeMaxBucket() {
        Config.max_column_number_per_table = 10000;

        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` bigint not null,\n" +
                "    `tag_name` string\n" +
                ") DUPLICATE KEY(`tag_id`)\n" +
                "PARTITION BY (`tag_id`)\n" +
                "DISTRIBUTED BY HASH(`tag_id`) BUCKETS 1025\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";

        Throwable exception = assertThrows(SemanticException.class, () -> {
            CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                    .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
            CreateTableAnalyzer.analyze(createTableStmt, connectContext);
        });
        assertThat(exception.getMessage(), containsString("max_bucket_number_per_partition"));
    }

    @Test
    public void testMaxColumn() {
        Config.max_column_number_per_table = 1;

        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` bigint not null,\n" +
                "    `tag_name` string\n" +
                ") DUPLICATE KEY(`tag_id`)\n" +
                "PARTITION BY (`tag_id`)\n" +
                "DISTRIBUTED BY HASH(`tag_id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";
        Throwable exception = assertThrows(SemanticException.class, () -> {
            CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                    .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
            CreateTableAnalyzer.analyze(createTableStmt, connectContext);
        });
        assertThat(exception.getMessage(), containsString("max_column_number_per_table"));
        Config.max_column_number_per_table = 10000;
    }

    private void testValidComplexDefault(String columnDef) {
        String sql = "CREATE TABLE test_create_table_db.test_complex_default (\n" +
                "    id INT,\n" +
                "    " + columnDef + "\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES(\"replication_num\" = \"1\", \"fast_schema_evolution\" = \"true\")";

        analyzeSuccess(sql);
    }

    private void testInvalidComplexDefault(String columnDef, String expectedErrorMsg) {
        String sql = "CREATE TABLE test_create_table_db.test_complex_default (\n" +
                "    id INT,\n" +
                "    " + columnDef + "\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES(\"replication_num\" = \"1\", \"fast_schema_evolution\" = \"true\")";

        analyzeFail(sql, expectedErrorMsg);
    }

    @Test
    public void testValidArrayDefaults() {
        testValidComplexDefault("c1 ARRAY<INT> DEFAULT [1, 2, 3]");
        testValidComplexDefault("c2 ARRAY<STRING> DEFAULT ['a', 'b', 'c']");
        testValidComplexDefault("c3 ARRAY<DOUBLE> DEFAULT [1.1, 2.2, 3.3]");
        testValidComplexDefault("c4 ARRAY<BOOLEAN> DEFAULT [true, false, true]");
        testValidComplexDefault("c5 ARRAY<DATE> DEFAULT ['2024-01-01', '2024-12-31']");
        testValidComplexDefault("c6 ARRAY<DATETIME> DEFAULT ['2024-01-01 10:00:00', '2024-12-31 23:59:59']");
        testValidComplexDefault("c7 ARRAY<DECIMAL(10,2)> DEFAULT [123.45, 678.90]");

        testValidComplexDefault("c8 ARRAY<ARRAY<INT>> DEFAULT [[1,2], [3,4], [5]]");
        testValidComplexDefault("c9 ARRAY<ARRAY<STRING>> DEFAULT [['a','b'], ['c','d','e']]");
        testValidComplexDefault("c10 ARRAY<ARRAY<ARRAY<INT>>> DEFAULT [[[1,2],[3,4]], [[5,6]]]");
    }

    @Test
    public void testValidMapDefaults() {
        testValidComplexDefault("c1 MAP<STRING, INT> DEFAULT map{'k1': 1, 'k2': 2}");
        testValidComplexDefault("c2 MAP<INT, STRING> DEFAULT map{1: 'one', 2: 'two'}");
        testValidComplexDefault("c3 MAP<STRING, DOUBLE> DEFAULT map{'price1': 99.99, 'price2': 199.99}");
        testValidComplexDefault("c4 MAP<STRING, BOOLEAN> DEFAULT map{'flag1': true, 'flag2': false}");
        testValidComplexDefault("c5 MAP<INT, INT> DEFAULT map{1: 10, 2: 20, 3: 30}");
    }

    @Test
    public void testValidStructDefaults() {
        testValidComplexDefault("c1 STRUCT<id INT, name STRING> DEFAULT row(1, 'test')");
        testValidComplexDefault("c2 STRUCT<code INT, value DOUBLE, active BOOLEAN> DEFAULT row(100, 95.5, true)");
        testValidComplexDefault("c3 STRUCT<dt DATE, name STRING> DEFAULT row('2024-01-01', 'event')");
        testValidComplexDefault("c4 STRUCT<price DECIMAL(10,2), qty INT> DEFAULT row(99.99, 10)");

        testValidComplexDefault("c5 STRUCT<id INT, inner_field STRUCT<code INT, value STRING>> " +
                "DEFAULT row(1, row(100, 'nested'))");
        testValidComplexDefault("c6 STRUCT<user STRUCT<id INT, name STRING>, status STRING> " +
                "DEFAULT row(row(1, 'user1'), 'active')");
    }

    @Test
    public void testValidArrayOfStructDefaults() {
        testValidComplexDefault("c1 ARRAY<STRUCT<id INT, name STRING>> " +
                "DEFAULT [row(1, 'alice'), row(2, 'bob')]");
        testValidComplexDefault("c2 ARRAY<STRUCT<code INT, value DOUBLE, flag BOOLEAN>> " +
                "DEFAULT [row(1, 95.5, true), row(2, 88.8, false)]");
    }

    @Test
    public void testValidMapOfStructDefaults() {
        testValidComplexDefault("c1 MAP<STRING, STRUCT<id INT, score DOUBLE>> " +
                "DEFAULT map{'user1': row(1, 95.5), 'user2': row(2, 88.8)}");
        testValidComplexDefault("c2 MAP<INT, STRUCT<name STRING, active BOOLEAN>> " +
                "DEFAULT map{1: row('item1', true), 2: row('item2', false)}");
    }

    @Test
    public void testValidStructWithArrayDefaults() {
        testValidComplexDefault("c1 STRUCT<id INT, tags ARRAY<STRING>> " +
                "DEFAULT row(1, ['tag1', 'tag2', 'tag3'])");
        testValidComplexDefault("c2 STRUCT<user STRING, scores ARRAY<INT>> " +
                "DEFAULT row('alice', [90, 85, 92])");
        testValidComplexDefault("c3 STRUCT<id INT, dates ARRAY<DATE>> " +
                "DEFAULT row(1, ['2024-01-01', '2024-12-31'])");
    }

    @Test
    public void testValidStructWithMapDefaults() {
        testValidComplexDefault("c1 STRUCT<id INT, properties MAP<STRING, INT>> " +
                "DEFAULT row(1, map{'k1': 10, 'k2': 20})");
        testValidComplexDefault("c2 STRUCT<name STRING, attrs MAP<STRING, STRING>> " +
                "DEFAULT row('test', map{'attr1': 'v1', 'attr2': 'v2'})");
    }

    @Test
    public void testValidMapWithArrayDefaults() {
        testValidComplexDefault("c1 MAP<STRING, ARRAY<INT>> " +
                "DEFAULT map{'scores1': [90, 85, 92], 'scores2': [78, 82, 88]}");
        testValidComplexDefault("c2 MAP<STRING, ARRAY<STRING>> " +
                "DEFAULT map{'tags1': ['a', 'b'], 'tags2': ['x', 'y', 'z']}");
    }

    @Test
    public void testValidDeeplyNestedDefaults() {
        // STRUCT<STRUCT<ARRAY>>
        testValidComplexDefault("c1 STRUCT<id INT, data STRUCT<code INT, value ARRAY<INT>>> " +
                "DEFAULT row(1, row(100, [1, 2, 3]))");

        // ARRAY<STRUCT<STRUCT>>
        testValidComplexDefault("c2 ARRAY<STRUCT<id INT, inner_field STRUCT<code INT, name STRING>>> " +
                "DEFAULT [row(1, row(10, 'test1')), row(2, row(20, 'test2'))]");

        // MAP<K, STRUCT<ARRAY>>
        testValidComplexDefault("c3 MAP<STRING, STRUCT<id INT, tags ARRAY<STRING>>> " +
                "DEFAULT map{'k1': row(1, ['a', 'b']), 'k2': row(2, ['c', 'd'])}");

        // STRUCT<MAP<K, ARRAY>>
        testValidComplexDefault("c4 STRUCT<id INT, data MAP<STRING, ARRAY<INT>>> " +
                "DEFAULT row(1, map{'scores': [90, 85], 'grades': [80, 75]})");
    }

    @Test
    public void testInvalidArithmeticExpressions() {
        testInvalidComplexDefault(
                "c1 ARRAY<INT> DEFAULT [1+2, 3*4]",
                "Expression type 'ArithmeticExpr' is not supported");

        testInvalidComplexDefault(
                "c2 ARRAY<DOUBLE> DEFAULT [1.0/2.0, 3.0-1.0]",
                "Expression type 'ArithmeticExpr' is not supported");

        testInvalidComplexDefault(
                "c3 MAP<STRING, INT> DEFAULT map{'k1': 10+20, 'k2': 30}",
                "Expression type 'ArithmeticExpr' is not supported");

        testInvalidComplexDefault(
                "c4 STRUCT<id INT, value INT> DEFAULT row(1+1, 2*2)",
                "Expression type 'ArithmeticExpr' is not supported");
    }

    @Test
    public void testInvalidFunctionCalls() {
        // Time functions
        testInvalidComplexDefault(
                "c1 ARRAY<DATETIME> DEFAULT [now()]",
                "Function 'now' is not supported");

        testInvalidComplexDefault(
                "c2 STRUCT<id INT, ts DATETIME> DEFAULT row(1, now())",
                "Function 'now' is not supported");

        testInvalidComplexDefault(
                "c3 MAP<STRING, DATETIME> DEFAULT map{'created': current_timestamp()}",
                "Function 'CURRENT_TIMESTAMP' is not supported");

        // String functions
        testInvalidComplexDefault(
                "c4 ARRAY<STRING> DEFAULT [concat('a', 'b')]",
                "Function 'concat' is not supported");

        testInvalidComplexDefault(
                "c5 ARRAY<STRING> DEFAULT [upper('test')]",
                "Function 'upper' is not supported");

        testInvalidComplexDefault(
                "c6 STRUCT<name STRING, value STRING> DEFAULT row('test', substring('hello', 1, 2))",
                "Function 'substring' is not supported");

        // Math functions
        testInvalidComplexDefault(
                "c7 ARRAY<INT> DEFAULT [abs(-10)]",
                "Function 'abs' is not supported");

        testInvalidComplexDefault(
                "c8 ARRAY<DOUBLE> DEFAULT [rand()]",
                "Function 'rand' is not supported");

        testInvalidComplexDefault(
                "c9 MAP<STRING, INT> DEFAULT map{'k1': floor(3.14)}",
                "Function 'floor' is not supported");

        // UUID function
        testInvalidComplexDefault(
                "c10 ARRAY<STRING> DEFAULT [uuid()]",
                "Function 'uuid' is not supported");

        // Conditional functions
        testInvalidComplexDefault(
                "c11 ARRAY<INT> DEFAULT [if(true, 10, 20)]",
                "Function 'if' is not supported");

        testInvalidComplexDefault(
                "c12 STRUCT<id INT, value STRING> DEFAULT row(1, coalesce(null, 'default'))",
                "Function 'coalesce' is not supported");

        testInvalidComplexDefault(
                "c13 MAP<STRING, INT> DEFAULT map{'k1': ifnull(null, 10)}",
                "Function 'ifnull' is not supported");
    }

    @Test
    public void testInvalidCaseAndCastExpressions() {
        testInvalidComplexDefault(
                "c1 ARRAY<INT> DEFAULT [CASE WHEN 1=1 THEN 10 ELSE 20 END]",
                "Expression type 'CaseExpr' is not supported");

        testInvalidComplexDefault(
                "c2 MAP<STRING, INT> DEFAULT map{'k1': CAST('10' AS INT)}",
                "CAST expression is not allowed in complex type default value");

        testInvalidComplexDefault(
                "c3 STRUCT<id INT, value STRING> DEFAULT row(1, CAST(100 AS STRING))",
                "CAST expression is not allowed in complex type default value");
    }

    @Test
    public void testInvalidNestedFunctions() {
        testInvalidComplexDefault(
                "c1 ARRAY<STRUCT<id INT, name STRING>> DEFAULT [row(1, upper('test'))]",
                "Function 'upper' is not supported");

        testInvalidComplexDefault(
                "c2 STRUCT<id INT, data ARRAY<DATETIME>> DEFAULT row(1, [now()])",
                "Function 'now' is not supported");

        testInvalidComplexDefault(
                "c3 MAP<STRING, STRUCT<id INT, value STRING>> " +
                        "DEFAULT map{'k1': row(1, concat('a', 'b'))}",
                "Function 'concat' is not supported");

        testInvalidComplexDefault(
                "c4 STRUCT<outer_field STRUCT<inner_field ARRAY<STRING>>> " +
                        "DEFAULT row(row([uuid()]))",
                "Function 'uuid' is not supported");
    }

    @Test
    public void testEmptyAndNullValues() {
        testValidComplexDefault("c1 ARRAY<INT> DEFAULT []");
        testValidComplexDefault("c2 ARRAY<STRING> DEFAULT []");
        testValidComplexDefault("c3 ARRAY<ARRAY<INT>> DEFAULT []");
        testValidComplexDefault("c4 ARRAY<STRUCT<id INT, name STRING>> DEFAULT []");

        testValidComplexDefault("c5 ARRAY<STRING> DEFAULT ['', 'a', '']");
        testValidComplexDefault("c6 ARRAY<STRING> DEFAULT ['']");

        testValidComplexDefault("c7 MAP<STRING, INT> DEFAULT map{}");
        testValidComplexDefault("c8 MAP<INT, STRING> DEFAULT map{}");

        testValidComplexDefault("c9 STRUCT<id INT, name STRING> DEFAULT row(1, '')");
        testValidComplexDefault("c10 STRUCT<name STRING, value STRING> DEFAULT row('', '')");

        testValidComplexDefault("c11 STRUCT<id INT, tags ARRAY<STRING>> DEFAULT row(1, [])");
        testValidComplexDefault("c12 STRUCT<id INT, attrs MAP<STRING, INT>> DEFAULT row(1, map{})");

        testValidComplexDefault("c13 ARRAY<ARRAY<INT>> DEFAULT [[], [1, 2], []]");
        testValidComplexDefault("c14 MAP<STRING, ARRAY<INT>> DEFAULT map{'k1': [], 'k2': [1, 2]}");
        testValidComplexDefault("c15 STRUCT<id INT, data STRUCT<tags ARRAY<STRING>, count INT>> " +
                "DEFAULT row(1, row([], 0))");
    }

    @Test
    public void testNullSubFieldsNotAllowed() {
        testInvalidComplexDefault(
                "c1 STRUCT<id INT, name STRING> DEFAULT row(null, 'test')",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c2 STRUCT<id INT, name STRING> DEFAULT row(1, null)",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c3 STRUCT<id INT, name STRING, value INT> DEFAULT row(null, null, null)",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c4 STRUCT<id INT, inner_field STRUCT<code INT, name STRING>> " +
                        "DEFAULT row(1, row(null, 'test'))",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c5 ARRAY<INT> DEFAULT [1, null, 3]",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c6 ARRAY<STRING> DEFAULT ['a', null, 'b']",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c7 ARRAY<STRUCT<id INT, name STRING>> " +
                        "DEFAULT [row(1, 'a'), row(null, 'b')]",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c8 MAP<STRING, INT> DEFAULT map{'k1': 1, 'k2': null}",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c9 MAP<STRING, STRING> DEFAULT map{'k1': 'v1', 'k2': null}",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c10 MAP<STRING, INT> DEFAULT map{null: 1, 'k2': 2}",
                "NULL literal is not supported in complex type default value");

        testInvalidComplexDefault(
                "c11 MAP<STRING, STRUCT<id INT, value STRING>> " +
                        "DEFAULT map{'k1': row(1, 'v1'), 'k2': row(null, 'v2')}",
                "NULL literal is not supported in complex type default value");
    }

    @Test
    public void testComplexTypeWithTypeCast() {
        testValidComplexDefault("c1 ARRAY<INT> DEFAULT [1, 2, 3]");
        testValidComplexDefault("c2 ARRAY<BIGINT> DEFAULT [100, 200]");
        testValidComplexDefault("c3 ARRAY<DOUBLE> DEFAULT [1, 2, 3]");
        testValidComplexDefault("c4 STRUCT<id BIGINT, value DOUBLE> DEFAULT row(1, 2)");
        testValidComplexDefault("c5 MAP<STRING, BIGINT> DEFAULT map{'k1': 1, 'k2': 2}");
        testValidComplexDefault("c6 ARRAY<ARRAY<BIGINT>> DEFAULT [[1, 2], [3, 4]]");
        testValidComplexDefault("c7 STRUCT<id INT, scores ARRAY<DOUBLE>> DEFAULT row(1, [90, 85, 92])");
    }

    @Test
    public void testComplexTypeInvalidCast() {
        testInvalidComplexDefault(
                "c1 ARRAY<INT> DEFAULT ['not_a_number']",
                "Invalid number format: not_a_number");

        testInvalidComplexDefault(
                "c3 MAP<INT, STRING> DEFAULT [123]",
                "Invalid default value for 'c3': Default value type ARRAY<TINYINT> cannot be cast " +
                        "to column type MAP<INT,VARCHAR(65533)");

        testInvalidComplexDefault(
                "c4 STRUCT<id INT, name STRING> DEFAULT row('not_int', 123, 456)",
                "Invalid default value for 'c4': Default value type struct<col1 varchar, col2 tinyint(4), col3 smallint(6)> " +
                        "cannot be cast to column type struct<id int(11), name varchar(65533)>");

        testInvalidComplexDefault("c1 ARRAY<STRUCT<id INT>> DEFAULT [row(1, 'extra_field')]", "");

        testInvalidComplexDefault("c2 MAP<STRING, INT> DEFAULT map{'k1'}", "");

        testInvalidComplexDefault("c2 MAP<STRING, INT> DEFAULT '123'", "Invalid default value for 'c2':" +
                " Default value for complex type 'MAP<VARCHAR(65533),INT>' requires expression syntax (e.g., [], map{}, row())");
    }

    @Test
    public void testFastSchemaEvolutionRequired() {
        String sql = "CREATE TABLE test_create_table_db.test_no_fast_schema (\n" +
                "    id INT,\n" +
                "    c1 ARRAY<INT> DEFAULT [1, 2, 3]\n" +
                ") DUPLICATE KEY(id)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES(\"replication_num\" = \"1\", \"fast_schema_evolution\" = \"false\")";

        analyzeFail(sql, "Complex type (ARRAY/MAP/STRUCT) default values require fast schema evolution");
    }

    @Test
    public void testPkTableSortKeyOrder() {
        boolean oldEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        try {
            // PK columns: (v1, v2), Sort keys: (v2, v1) -> Should fail
            String sql1 = "CREATE TABLE test_create_table_db.pk_table_wrong_order\n" +
                    "(\n" +
                    "    v1 int not null,\n" +
                    "    v2 int not null,\n" +
                    "    v3 int\n" +
                    ") PRIMARY KEY(v1, v2)\n" +
                    "ORDER BY(v2, v1)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");";
            analyzeFail(sql1, "The sort columns must be same with primary key columns and the order must be consistent");

            // PK columns: (v1, v2), Sort keys: (v1, v2) -> Should pass
            String sql2 = "CREATE TABLE test_create_table_db.pk_table_correct_order\n" +
                    "(\n" +
                    "    v1 int not null,\n" +
                    "    v2 int not null,\n" +
                    "    v3 int\n" +
                    ") PRIMARY KEY(v1, v2)\n" +
                    "ORDER BY(v1, v2)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");";
            analyzeSuccess(sql2);

            // enable_range_distribution = false -> Should pass even if order is different
            Config.enable_range_distribution = false;
            String sql3 = "CREATE TABLE test_create_table_db.pk_table_diff_order_range_off\n" +
                    "(\n" +
                    "    v1 int not null,\n" +
                    "    v2 int not null,\n" +
                    "    v3 int\n" +
                    ") PRIMARY KEY(v1, v2)\n" +
                    "DISTRIBUTED BY HASH(v1)\n" +
                    "ORDER BY(v2, v1)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");";
            analyzeSuccess(sql3);
        } finally {
            Config.enable_range_distribution = oldEnableRangeDistribution;
        }
    }

    @Test
    public void testCreateTableForceRange() {
        boolean oldEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = false;
        try {
            String sql = "CREATE TABLE test_create_table_db.force_range_table\n" +
                    "(\n" +
                    "    k1 int,\n" +
                    "    k2 int,\n" +
                    "    v1 int\n" +
                    ")\n" +
                    "DUPLICATE KEY(k1, k2)\n" +
                    "PROPERTIES('replication_num' = '1');";

            // 1. Default: should NOT be range distribution if Config is false
            CreateTableStmt stmt1 = (CreateTableStmt) analyzeSuccess(sql);
            Assertions.assertFalse(stmt1.getDistributionDesc() instanceof RangeDistributionDesc);

            // 2. Set session variable to true: should be range distribution
            connectContext.getSessionVariable().setEnableRangeDistribution(true);
            try {
                CreateTableStmt stmt2 = (CreateTableStmt) analyzeSuccess(sql);
                Assertions.assertTrue(stmt2.getDistributionDesc() instanceof RangeDistributionDesc);
            } finally {
                connectContext.getSessionVariable().setEnableRangeDistribution(false);
            }

            // 3. Set Config to true: should be range distribution even if session variable is false
            Config.enable_range_distribution = true;
            CreateTableStmt stmt3 = (CreateTableStmt) analyzeSuccess(sql);
            Assertions.assertTrue(stmt3.getDistributionDesc() instanceof RangeDistributionDesc);

        } finally {
            Config.enable_range_distribution = oldEnableRangeDistribution;
        }
    }
}
