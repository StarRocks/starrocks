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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.RangeDistributionDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    @Test
    public void testPrimaryKeyTablePartitionSourceColumnsMustBePrimaryKeys() {
        String nonKeyGeneratedPartitionColumnSql = "CREATE TABLE test_create_table_db.t_pk_partition_non_key (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `transaction_time` datetime NOT NULL,\n" +
                "  `transaction_date` date NULL AS date(transaction_time)\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`)\n" +
                "PARTITION BY (`transaction_date`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "PROPERTIES(\"replication_num\" = \"1\")";
        analyzeFail(nonKeyGeneratedPartitionColumnSql,
                "The partition expr should base on key column");

        String nonKeyPartitionExprSourceSql = "CREATE TABLE test_create_table_db.t_pk_partition_expr_non_key_source (\n" +
                "  `id` bigint NOT NULL,\n" +
                "  `transaction_time` bigint NOT NULL,\n" +
                "  `v1` int\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`)\n" +
                "PARTITION BY from_unixtime(transaction_time)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "PROPERTIES(\"replication_num\" = \"1\")";
        analyzeFail(nonKeyPartitionExprSourceSql,
                "The partition expr should base on key column");

        String keyPartitionExprSourceSql = "CREATE TABLE test_create_table_db.t_pk_partition_on_key_expr (\n" +
                "  `id` bigint NOT NULL,\n" +
                "  `v1` int\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`id`)\n" +
                "PARTITION BY from_unixtime(id)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "PROPERTIES(\"replication_num\" = \"1\")";
        analyzeSuccess(keyPartitionExprSourceSql);
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
                "Invalid default value for 'c4': Default value type struct<`col1` varchar, `col2` tinyint(4), " +
                        "`col3` smallint(6)> cannot be cast to column type struct<`id` int(11), `name` " +
                        "varchar(65533)>");

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
        // Force range distribution via the session variable (ungated by run mode) so the range
        // sort-key-order validation is exercised regardless of the suite's ambient run mode.
        connectContext.getSessionVariable().setEnableRangeDistribution(true);
        try {
            // PK columns: (v1, v2), Sort keys: (v2, v1). A permutation of the primary key is still a
            // sort key that differs from it, so this is the supported ORDER BY != PK shape rather than a
            // mistake -- file_bundling defaults on, which is the only remaining requirement.
            String sql1 = "CREATE TABLE test_create_table_db.pk_table_permuted_order\n" +
                    "(\n" +
                    "    v1 int not null,\n" +
                    "    v2 int not null,\n" +
                    "    v3 int\n" +
                    ") PRIMARY KEY(v1, v2)\n" +
                    "ORDER BY(v2, v1)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");";
            analyzeSuccess(sql1);

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

            String sqlWithSeparateSortKey = "CREATE TABLE test_create_table_db.pk_table_separate_sort_key\n" +
                    "(v1 int not null, v2 int not null, v3 int) PRIMARY KEY(v1, v2)\n" +
                    "ORDER BY(v3) PROPERTIES (\"replication_num\" = \"1\", \"file_bundling\" = \"true\");";
            analyzeSuccess(sqlWithSeparateSortKey);

            String sqlWithoutFileBundling = "CREATE TABLE test_create_table_db.pk_table_no_bundle\n" +
                    "(v1 int not null, v2 int not null, v3 int) PRIMARY KEY(v1, v2)\n" +
                    "ORDER BY(v3) PROPERTIES (\"replication_num\" = \"1\", \"file_bundling\" = \"false\");";
            analyzeFail(sqlWithoutFileBundling, "require file_bundling=true");

            // range distribution off -> Should pass even if order is different (hash-distributed)
            connectContext.getSessionVariable().setEnableRangeDistribution(false);
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
            connectContext.getSessionVariable().setEnableRangeDistribution(false);
        }
    }

    @Test
    public void testDupTableSortKeyTypeRestriction() {
        // A sort key column is encoded on the BE via a KeyCoder; types without one (JSON, TIME, ...)
        // crash the short-key encoder, so they must be rejected at CREATE TABLE for duplicate key
        // tables under BOTH range and non-range distribution (#11611).
        connectContext.getSessionVariable().setEnableRangeDistribution(true);
        try {
            // JSON sort key, range distribution -> reject (JSON has no BE key coder).
            analyzeFail("CREATE TABLE test_create_table_db.dup_range_json_sortkey\n" +
                    "(k1 int, c json) DUPLICATE KEY(k1) ORDER BY(c)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");",
                    "Sort key column[c] type not supported");

            // TIME sort key, range distribution -> reject (canDistributedBy() allows TIME, but the BE
            // has no TIME key coder; canDistributedBy() excludes it).
            analyzeFail("CREATE TABLE test_create_table_db.dup_range_time_sortkey\n" +
                    "(k1 int, c time) DUPLICATE KEY(k1) ORDER BY(c)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");",
                    "Sort key column[c] type not supported");

            // A normal (int) sort key -> pass. The ORDER BY reference uses a different case than the
            // column definition to confirm case-insensitive resolution (matching OlapTableFactory).
            analyzeSuccess("CREATE TABLE test_create_table_db.dup_range_int_sortkey\n" +
                    "(k1 int, c int) DUPLICATE KEY(k1) ORDER BY(C)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");");

            // Non-range distribution: the same crash is reachable (the sort key is still short-key
            // encoded), so JSON/TIME must be rejected here too.
            connectContext.getSessionVariable().setEnableRangeDistribution(false);
            analyzeFail("CREATE TABLE test_create_table_db.dup_norange_json_sortkey\n" +
                    "(k1 int, c json) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) ORDER BY(c)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");",
                    "Sort key column[c] type not supported");
            analyzeFail("CREATE TABLE test_create_table_db.dup_norange_time_sortkey\n" +
                    "(k1 int, c time) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) ORDER BY(c)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");",
                    "Sort key column[c] type not supported");

            // Non-range int sort key still passes.
            analyzeSuccess("CREATE TABLE test_create_table_db.dup_norange_int_sortkey\n" +
                    "(k1 int, c int) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) ORDER BY(c)\n" +
                    "PROPERTIES (\"replication_num\" = \"1\");");
        } finally {
            connectContext.getSessionVariable().setEnableRangeDistribution(false);
        }
    }

    @Test
    public void testTimeKeyColumnRejected() {
        // TIME has no BE key coder, so it can't be a key column (a key column is also the implicit
        // short/sort key). canDistributedBy() now excludes TIME, so ColumnDefAnalyzer rejects it.
        analyzeFail("CREATE TABLE test_create_table_db.time_key_tbl\n" +
                "(k1 time, v int) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(v)\n" +
                "PROPERTIES (\"replication_num\" = \"1\");",
                "Invalid data type of key column");
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

            // 3. Set Config to true: the config-driven default only takes effect in shared-data mode
            // (range distribution is shared-data only), so the outcome tracks the current run mode.
            Config.enable_range_distribution = true;
            CreateTableStmt stmt3 = (CreateTableStmt) analyzeSuccess(sql);
            Assertions.assertEquals(RunMode.isSharedDataMode(),
                    stmt3.getDistributionDesc() instanceof RangeDistributionDesc);

        } finally {
            Config.enable_range_distribution = oldEnableRangeDistribution;
        }
    }

    @Test
    public void testAnalyzeEngineNameUnifiedCatalogRequiresEngine() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withCatalog("create external catalog test_unified_requires_engine properties (" +
                "\"type\"=\"unified\", \"unified.metastore.type\"=\"hive\", " +
                "\"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")");

        String sql = "CREATE TABLE test_unified_requires_engine.db.t (a INT)";
        Throwable exception = assertThrows(SemanticException.class, () -> {
            CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                    .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
            CreateTableAnalyzer.analyzeEngineName(createTableStmt, "test_unified_requires_engine");
        });
        assertThat(exception.getMessage(), containsString("requires engine clause"));
    }

    @Test
    public void testAnalyzeEngineNameUnifiedCatalogAcceptsExplicitEngine() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withCatalog("create external catalog test_unified_accepts_engine properties (" +
                "\"type\"=\"unified\", \"unified.metastore.type\"=\"hive\", " +
                "\"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")");

        String sql = "CREATE TABLE test_unified_accepts_engine.db.t (a INT) ENGINE=hive";
        CreateTableStmt createTableStmt = (CreateTableStmt) com.starrocks.sql.parser.SqlParser
                .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        CreateTableAnalyzer.analyzeEngineName(createTableStmt, "test_unified_accepts_engine");
        Assertions.assertEquals("hive", createTableStmt.getEngineName());
    }

    private static Column getGeneratedPartitionColumn(String tableName) throws Exception {
        OlapTable table = (OlapTable) AnalyzeTestUtil.getStarRocksAssert().getTable("test", tableName);
        return table.getBaseSchema().stream()
                .filter(Column::isGeneratedPartitionColumn)
                .findFirst()
                .orElse(null);
    }

    @Test
    public void testAggregateTableWithGeneratedPartitionColumn() throws Exception {
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        starRocksAssert.withTable("CREATE TABLE test.t_agg_week (\n" +
                "  `dt` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `v` bigint SUM\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`dt`, `city`)\n" +
                "PARTITION BY date_trunc('week', dt)\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")");

        // the partition column is materialized as a hidden value column carrying REPLACE, which is an
        // identity operation because it only depends on key columns
        Column generatedColumn = getGeneratedPartitionColumn("t_agg_week");
        Assertions.assertNotNull(generatedColumn);
        Assertions.assertFalse(generatedColumn.isKey());
        Assertions.assertEquals(AggregateType.REPLACE, generatedColumn.getAggregationType());

        // and it stays invisible to users
        String ddl = starRocksAssert.showCreateTable("show create table test.t_agg_week");
        Assertions.assertFalse(ddl.contains(FeConstants.GENERATED_PARTITION_COLUMN_PREFIX));
        assertThat(ddl, containsString("date_trunc('week', dt)"));
    }

    @Test
    public void testAggregateTableWithInferredKeysAndGeneratedPartitionColumn() throws Exception {
        // the keys type is inferred from the value columns, so it is not known yet when the
        // generated partition column is created
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE test.t_agg_inferred_keys (\n" +
                "  `dt` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `pv` bigint SUM\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY date_trunc('week', dt)\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")");

        OlapTable table = (OlapTable) AnalyzeTestUtil.getStarRocksAssert().getTable("test", "t_agg_inferred_keys");
        Assertions.assertEquals(KeysType.AGG_KEYS, table.getKeysType());
        Column generatedColumn = getGeneratedPartitionColumn("t_agg_inferred_keys");
        Assertions.assertNotNull(generatedColumn);
        // the generated column must not be swept into the inferred key list
        Assertions.assertFalse(generatedColumn.isKey());
        Assertions.assertEquals(AggregateType.REPLACE, generatedColumn.getAggregationType());
        Assertions.assertEquals(Set.of("dt", "city"), table.getKeyColumns().stream()
                .map(Column::getName).collect(Collectors.toSet()));
    }

    @Test
    public void testNonDeterministicPartitionExprIsRejected() {
        // rand() would send rows of the same aggregate key to different partitions, so the generated
        // column would no longer be determined by the keys
        analyzeFail("CREATE TABLE test.t_agg_nondeterministic (\n" +
                        "  `k` bigint NOT NULL,\n" +
                        "  `city` varchar(64) NOT NULL,\n" +
                        "  `pv` bigint SUM\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k`, `city`)\n" +
                        "PARTITION BY floor(rand() * 2 + k)\n" +
                        "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                        "PROPERTIES(\"replication_num\" = \"1\")",
                "The partition expr should be deterministic");
        // the same holds for a duplicate key table: partition pruning would be wrong there too
        analyzeFail("CREATE TABLE test.t_dup_nondeterministic (\n" +
                        "  `k` bigint NOT NULL,\n" +
                        "  `city` varchar(64) NOT NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k`, `city`)\n" +
                        "PARTITION BY floor(rand() * 2 + k)\n" +
                        "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                        "PROPERTIES(\"replication_num\" = \"1\")",
                "The partition expr should be deterministic");
    }

    @Test
    public void testPartitionExprSpellingMustMatchTheDeclaredColumn() {
        // a generated column resolves its source columns case sensitively, so a partition expression
        // that spells the column differently cannot create a table at all, for any keys type. The
        // source column of such a table can therefore never be dropped later.
        String ddl = "CREATE TABLE test.t_agg_mixed_case_key (\n" +
                "  `event_day` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `pv` bigint SUM\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`event_day`, `city`)\n" +
                "PARTITION BY date_trunc('week', EVENT_DAY)\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")";
        analyzeFail(ddl, "does not exist");
    }

    @Test
    public void testAggregateTablePartitionExprOnValueColumnIsCaseInsensitive() {
        // the expression spells the column differently from the declaration; the guard that keeps the
        // generated column functionally determined by the keys must still reject it
        analyzeFail("CREATE TABLE test.t_agg_mixed_case_value (\n" +
                        "  `dt` datetime NOT NULL,\n" +
                        "  `city` varchar(64) NOT NULL,\n" +
                        "  `last_day` datetime MAX,\n" +
                        "  `pv` bigint SUM\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`dt`, `city`)\n" +
                        "PARTITION BY date_trunc('week', LAST_DAY)\n" +
                        "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                        "PROPERTIES(\"replication_num\" = \"1\")",
                "The partition expr should base on key column");
    }

    @Test
    public void testAggregateTableWithMultiExpressionPartition() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE test.t_agg_multi_expr (\n" +
                "  `dt` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `v` bigint SUM\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`dt`, `city`)\n" +
                "PARTITION BY (`city`, date_trunc('day', dt))\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")");

        Column generatedColumn = getGeneratedPartitionColumn("t_agg_multi_expr");
        Assertions.assertNotNull(generatedColumn);
        Assertions.assertEquals(AggregateType.REPLACE, generatedColumn.getAggregationType());
    }

    @Test
    public void testUniqueTableWithGeneratedPartitionColumn() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE test.t_uniq_week (\n" +
                "  `dt` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `v` bigint\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`dt`, `city`)\n" +
                "PARTITION BY date_trunc('week', dt)\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")");

        Assertions.assertNotNull(getGeneratedPartitionColumn("t_uniq_week"));
    }

    @Test
    public void testDuplicateTableGeneratedPartitionColumnKeepsNoAggregateType() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE test.t_dup_week (\n" +
                "  `dt` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `v` bigint\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `city`)\n" +
                "PARTITION BY date_trunc('week', dt)\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")");

        Column generatedColumn = getGeneratedPartitionColumn("t_dup_week");
        Assertions.assertNotNull(generatedColumn);
        Assertions.assertNotEquals(AggregateType.REPLACE, generatedColumn.getAggregationType());
    }

    @Test
    public void testAggregateTablePartitionExprMustBaseOnKeyColumn() {
        analyzeFail("CREATE TABLE test.t_agg_partition_on_value (\n" +
                        "  `dt` datetime NOT NULL,\n" +
                        "  `city` varchar(64) NOT NULL,\n" +
                        "  `vdt` datetime MAX,\n" +
                        "  `v` bigint SUM\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`dt`, `city`)\n" +
                        "PARTITION BY date_trunc('week', vdt)\n" +
                        "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                        "PROPERTIES(\"replication_num\" = \"1\")",
                "The partition expr should base on key column");
    }

    @Test
    public void testAggregateTableStillRejectsUserDefinedGeneratedColumn() {
        analyzeFail("CREATE TABLE test.t_agg_user_generated (\n" +
                        "  `dt` datetime NOT NULL,\n" +
                        "  `city` varchar(64) NOT NULL,\n" +
                        "  `v` bigint SUM,\n" +
                        "  `week_start` datetime NULL AS date_trunc('week', dt)\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`dt`, `city`)\n" +
                        "PARTITION BY date_trunc('day', dt)\n" +
                        "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                        "PROPERTIES(\"replication_num\" = \"1\")",
                "Generated Column does not support AGG table");
    }

    @Test
    public void testAggregateTableWithGeneratedPartitionColumnCanDropKeyColumn() throws Exception {
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        starRocksAssert.withTable("CREATE TABLE test.t_agg_drop_key (\n" +
                "  `dt` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `channel` varchar(64) NOT NULL,\n" +
                "  `v` bigint SUM\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`dt`, `city`, `channel`)\n" +
                "PARTITION BY date_trunc('week', dt)\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")");

        // the hidden REPLACE column must not make the table look like it carries a user REPLACE value,
        // which would forbid dropping a key column. The schema change itself is asynchronous, so this
        // only asserts the statement is accepted; the column actually disappearing is covered by
        // test_agg_table_generated_partition_column_alter in the SQL suite.
        Assertions.assertDoesNotThrow(
                () -> starRocksAssert.alterTable("ALTER TABLE test.t_agg_drop_key DROP COLUMN `channel`"));
    }

    @Test
    public void testAggregateTableWithGeneratedPartitionColumnCanAddColumn() throws Exception {
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        starRocksAssert.withTable("CREATE TABLE test.t_agg_add_column (\n" +
                "  `dt` datetime NOT NULL,\n" +
                "  `city` varchar(64) NOT NULL,\n" +
                "  `v` bigint SUM\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`dt`, `city`)\n" +
                "PARTITION BY date_trunc('week', dt)\n" +
                "DISTRIBUTED BY HASH(`city`) BUCKETS 3\n" +
                "PROPERTIES(\"replication_num\" = \"1\")");
        starRocksAssert.alterTable("ALTER TABLE test.t_agg_add_column ADD COLUMN `v2` bigint SUM DEFAULT \"0\"");

        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "t_agg_add_column");
        List<String> columnNames = table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
        // new value columns are appended before the hidden partition column
        Assertions.assertTrue(columnNames.indexOf("v2")
                < columnNames.indexOf(FeConstants.GENERATED_PARTITION_COLUMN_PREFIX + "0"));
    }
}
