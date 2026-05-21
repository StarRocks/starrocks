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

package com.starrocks.sql;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeIntoIntegrationTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.initCtxForNewPrivilege();
        starRocksAssert = new StarRocksAssert(ctx);
        
        starRocksAssert.withDatabase("test").useDatabase("test");
        
        starRocksAssert.withCatalog(
                "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\n" +
                "\"type\" = \"iceberg\",\n" +
                "\"iceberg.catalog.type\" = \"hive\",\n" +
                "\"hive.metastore.uris\" = \"thrift://localhost:9083\"\n" +
                ")");
        
        starRocksAssert.useDatabase("iceberg_catalog.test");
        
        starRocksAssert.withTable(
                "CREATE EXTERNAL TABLE iceberg_catalog.test.target_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  city string,\n" +
                "  salary double\n" +
                ") ENGINE=iceberg;");
        
        starRocksAssert.withTable(
                "CREATE EXTERNAL TABLE iceberg_catalog.test.part_target (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  value int,\n" +
                "  year int,\n" +
                "  month int\n" +
                ") PARTITIONED BY (year, month) ENGINE=iceberg;");
        
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable(
                "CREATE TABLE source_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  city string,\n" +
                "  salary double\n" +
                ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        
        starRocksAssert.withTable(
                "CREATE TABLE updates_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  value int,\n" +
                "  year int,\n" +
                "  month int\n" +
                ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisit();
    }

    private MergeIntoStmt parseAndAnalyze(String sql) throws Exception {
        List<StatementBase> stmts = SqlParser.parse(sql, ctx.getSessionVariable());
        assertEquals(1, stmts.size());
        StatementBase stmt = stmts.get(0);
        assertTrue(stmt instanceof MergeIntoStmt);
        Analyzer.analyze(stmt, ctx);
        return (MergeIntoStmt) stmt;
    }

    @Test
    public void testEndToEndUpdateAndInsert() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age, city = s.city, salary = s.salary " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city, s.salary)";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        assertTrue(stmt.getTargetTable() instanceof IcebergTable);
        assertEquals("target_tbl", stmt.getTargetTable().getName());
        
        QueryStatement deleteQuery = stmt.getDeleteQueryStatement();
        assertNotNull(deleteQuery);
        
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        assertNotNull(insertQuery);
        
        assertTrue(stmt.hasMatchedClauses());
        assertTrue(stmt.hasUpdateClause());
        assertTrue(stmt.hasInsertClause());
        assertFalse(stmt.hasDeleteClause());
    }

    @Test
    public void testEndToEndDeleteOnly() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        QueryStatement deleteQuery = stmt.getDeleteQueryStatement();
        assertNotNull(deleteQuery);
        
        assertTrue(stmt.hasMatchedClauses());
        assertTrue(stmt.hasDeleteClause());
        assertFalse(stmt.hasUpdateClause());
        assertFalse(stmt.hasInsertClause());
    }

    @Test
    public void testEndToEndConditionalDelete() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.salary < 30000 THEN DELETE " +
                "WHEN MATCHED THEN UPDATE SET salary = s.salary * 1.1";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        QueryStatement deleteQuery = stmt.getDeleteQueryStatement();
        assertNotNull(deleteQuery);
        String deleteQueryStr = deleteQuery.toSql();
        assertTrue(deleteQueryStr.toLowerCase().contains("salary"));
        
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        assertNotNull(insertQuery);
    }

    @Test
    public void testEndToEndPartitionedTable() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.part_target AS t " +
                "USING test.updates_tbl AS s " +
                "ON t.id = s.id AND t.year = s.year AND t.month = s.month " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.value, s.year, s.month)";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        IcebergTable table = (IcebergTable) stmt.getTargetTable();
        assertNotNull(table.getPartitionColumns());
        assertFalse(table.getPartitionColumns().isEmpty());
        
        QueryStatement deleteQuery = stmt.getDeleteQueryStatement();
        assertNotNull(deleteQuery);
        String deleteQueryStr = deleteQuery.toSql().toLowerCase();
        assertTrue(deleteQueryStr.contains("year"));
        assertTrue(deleteQueryStr.contains("month"));
    }

    @Test
    public void testEndToEndMultipleConditions() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age < 25 THEN UPDATE SET city = 'Young City' " +
                "WHEN MATCHED AND s.age >= 25 AND s.age < 50 THEN UPDATE SET city = 'Mid City' " +
                "WHEN MATCHED AND s.age >= 50 THEN UPDATE SET city = 'Senior City' " +
                "WHEN NOT MATCHED AND s.age >= 18 THEN INSERT VALUES (s.id, s.name, s.age, s.city, s.salary)";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        assertNotNull(insertQuery);
        String insertQueryStr = insertQuery.toSql();
        assertTrue(insertQueryStr.contains("UNION ALL"));
    }

    @Test
    public void testEndToEndPartialColumnInsert() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, name, age) VALUES (s.id, s.name, s.age)";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        assertNotNull(insertQuery);
        String insertQueryStr = insertQuery.toSql().toLowerCase();
        assertTrue(insertQueryStr.contains("null"));
    }

    @Test
    public void testEndToEndSubquerySource() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING (SELECT id, name, age, city, salary FROM test.source_tbl WHERE age > 21) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        assertNotNull(stmt.getSourceRelation());
        assertNotNull(stmt.getDeleteQueryStatement());
        assertNotNull(stmt.getInsertQueryStatement());
    }

    @Test
    public void testEndToEndComplexExpressions() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET " +
                "  name = concat(upper(s.name), '_UPDATED'), " +
                "  age = s.age + 1, " +
                "  salary = s.salary * 1.05 " +
                "WHEN NOT MATCHED THEN INSERT VALUES (" +
                "  s.id, " +
                "  lower(s.name), " +
                "  s.age, " +
                "  coalesce(s.city, 'Unknown'), " +
                "  s.salary)";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        assertNotNull(insertQuery);
        String queryStr = insertQuery.toSql();
        assertTrue(queryStr.contains("concat") || queryStr.contains("upper") || 
                   queryStr.contains("CONCAT") || queryStr.contains("UPPER"));
    }

    @Test
    public void testTargetAliasHandling() throws Exception {
        String sql1 = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        MergeIntoStmt stmt1 = parseAndAnalyze(sql1);
        assertEquals("t", stmt1.getTargetAlias());
        
        String sql2 = "MERGE INTO iceberg_catalog.test.target_tbl " +
                "USING test.source_tbl " +
                "ON target_tbl.id = source_tbl.id " +
                "WHEN MATCHED THEN UPDATE SET name = source_tbl.name";
        
        MergeIntoStmt stmt2 = parseAndAnalyze(sql2);
        assertNull(stmt2.getTargetAlias());
    }

    @Test
    public void testMergeClauseCounting() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age < 30 THEN UPDATE SET city = 'A' " +
                "WHEN MATCHED AND s.age >= 30 THEN DELETE " +
                "WHEN NOT MATCHED AND s.salary > 50000 THEN INSERT VALUES (s.id, s.name, s.age, s.city, s.salary) " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, 'Default', s.salary)";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        assertEquals(4, stmt.getMergeClauses().size());
        assertTrue(stmt.hasMatchedClauses());
        assertTrue(stmt.hasUpdateClause());
        assertTrue(stmt.hasDeleteClause());
        assertTrue(stmt.hasInsertClause());
    }

    @Test
    public void testInvalidValueCount() {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name)";
        
        Exception exception = assertThrows(Exception.class, () -> {
            parseAndAnalyze(sql);
        });
        assertTrue(exception.getMessage().contains("doesn't match target column count") ||
                   exception.getMessage().contains("column") ||
                   exception.getMessage().contains("value"));
    }

    @Test
    public void testComplexJoinCondition() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id AND t.name = s.name AND t.city = s.city " +
                "WHEN MATCHED THEN UPDATE SET age = s.age, salary = s.salary";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        assertNotNull(stmt.getMergeCondition());
        String conditionStr = stmt.getMergeCondition().toSql();
        assertTrue(conditionStr.contains("id"));
        assertTrue(conditionStr.contains("name"));
        assertTrue(conditionStr.contains("city"));
    }

    @Test
    public void testCaseExpressionInUpdate() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.target_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET " +
                "  city = CASE " +
                "    WHEN s.age < 25 THEN 'Young' " +
                "    WHEN s.age < 50 THEN 'Middle' " +
                "    ELSE 'Senior' " +
                "  END";
        
        MergeIntoStmt stmt = parseAndAnalyze(sql);
        
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        assertNotNull(insertQuery);
        String queryStr = insertQuery.toSql();
        assertTrue(queryStr.contains("CASE") || queryStr.contains("case"));
    }
}
