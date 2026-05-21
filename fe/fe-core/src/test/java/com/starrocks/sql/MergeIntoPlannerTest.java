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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeIntoPlannerTest {
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
                "CREATE EXTERNAL TABLE iceberg_catalog.test.iceberg_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  city string\n" +
                ") ENGINE=iceberg;");
        
        starRocksAssert.withTable(
                "CREATE EXTERNAL TABLE iceberg_catalog.test.iceberg_part_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  dt string\n" +
                ") PARTITIONED BY (dt) ENGINE=iceberg;");
        
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable(
                "CREATE TABLE source_tbl (\n" +
                "  id bigint,\n" +
                "  name string,\n" +
                "  age int,\n" +
                "  city string\n" +
                ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        UtFrameUtils.tearDownForPersisit();
    }

    private MergeIntoStmt analyzeMergeInto(String sql) throws Exception {
        StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(stmt, ctx);
        return (MergeIntoStmt) stmt;
    }

    @Test
    public void testBasicMergeIntoPlanning() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertNotNull(stmt.getTargetTable());
        assertTrue(stmt.getTargetTable() instanceof IcebergTable);
        assertNotNull(stmt.getDeleteQueryStatement());
        assertNotNull(stmt.getInsertQueryStatement());
    }

    @Test
    public void testDeleteQueryGeneration() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        QueryStatement deleteQuery = stmt.getDeleteQueryStatement();
        
        assertNotNull(deleteQuery);
        String queryStr = deleteQuery.toSql().toLowerCase();
        assertTrue(queryStr.contains("file_path"));
        assertTrue(queryStr.contains("row_position"));
        assertTrue(queryStr.contains("inner join"));
    }

    @Test
    public void testInsertQueryGeneration() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        
        assertNotNull(insertQuery);
        String queryStr = insertQuery.toSql().toLowerCase();
        assertTrue(queryStr.contains("anti join") || queryStr.contains("left anti join"));
    }

    @Test
    public void testUpdateQueryGeneration() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name, age = s.age";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertNotNull(stmt.getDeleteQueryStatement());
        assertNotNull(stmt.getInsertQueryStatement());
        
        String insertQueryStr = stmt.getInsertQueryStatement().toSql().toLowerCase();
        assertTrue(insertQueryStr.contains("inner join"));
    }

    @Test
    public void testMultipleClausesQueryGeneration() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age > 30 THEN UPDATE SET city = 'Senior' " +
                "WHEN MATCHED THEN UPDATE SET city = 'Junior' " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertNotNull(stmt.getDeleteQueryStatement());
        assertNotNull(stmt.getInsertQueryStatement());
        
        String insertQueryStr = stmt.getInsertQueryStatement().toSql();
        assertTrue(insertQueryStr.contains("UNION ALL"));
    }

    @Test
    public void testPartitionedTableQueryGeneration() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_part_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        QueryStatement deleteQuery = stmt.getDeleteQueryStatement();
        
        assertNotNull(deleteQuery);
        String queryStr = deleteQuery.toSql().toLowerCase();
        assertTrue(queryStr.contains("dt"));
    }

    @Test
    public void testDeleteOnlyClause() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertNotNull(stmt.getDeleteQueryStatement());
        assertTrue(stmt.getInsertQueryStatement() == null || 
                   stmt.getInsertQueryStatement().getQueryRelation() == null);
    }

    @Test
    public void testInsertOnlyClause() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertTrue(stmt.getDeleteQueryStatement() == null || 
                   stmt.getDeleteQueryStatement().getQueryRelation() == null);
        assertNotNull(stmt.getInsertQueryStatement());
    }

    @Test
    public void testMergeIntoWithSubquery() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING (SELECT id, name, age, city FROM test.source_tbl WHERE age > 25) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertNotNull(stmt.getDeleteQueryStatement());
        String deleteQueryStr = stmt.getDeleteQueryStatement().toSql().toLowerCase();
        assertTrue(deleteQueryStr.contains("select"));
    }

    @Test
    public void testPartialInsert() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        
        assertNotNull(insertQuery);
        String queryStr = insertQuery.toSql().toLowerCase();
        assertTrue(queryStr.contains("null"));
    }

    @Test
    public void testConditionalClauses() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.age < 30 THEN UPDATE SET city = 'Young' " +
                "WHEN MATCHED AND s.age >= 30 THEN DELETE " +
                "WHEN NOT MATCHED AND s.age > 18 THEN INSERT VALUES (s.id, s.name, s.age, s.city)";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertNotNull(stmt.getDeleteQueryStatement());
        assertNotNull(stmt.getInsertQueryStatement());
        
        String deleteQueryStr = stmt.getDeleteQueryStatement().toSql().toLowerCase();
        assertTrue(deleteQueryStr.contains("where") || deleteQueryStr.contains("and"));
    }

    @Test
    public void testMergeIntoTargetTableType() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET name = s.name";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        
        assertTrue(stmt.getTargetTable() instanceof IcebergTable);
        assertEquals("iceberg_tbl", stmt.getTargetTable().getName());
    }

    @Test
    public void testMergeIntoWithExpressionsInSet() throws Exception {
        String sql = "MERGE INTO iceberg_catalog.test.iceberg_tbl AS t " +
                "USING test.source_tbl AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET age = s.age + 1, name = concat(s.name, '_suffix')";
        
        MergeIntoStmt stmt = analyzeMergeInto(sql);
        QueryStatement insertQuery = stmt.getInsertQueryStatement();
        
        assertNotNull(insertQuery);
        String queryStr = insertQuery.toSql();
        assertTrue(queryStr.contains("+") || queryStr.contains("concat"));
    }
}
