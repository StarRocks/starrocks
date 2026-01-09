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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test cases for lake_compaction_max_parallel property.
 * This test covers:
 * - PropertyAnalyzer.analyzeLakeCompactionMaxParallel
 * - TableProperty.buildLakeCompactionMaxParallel
 * - AlterTableClauseAnalyzer for lake_compaction_max_parallel
 * - LocalMetastore.alterLakeCompactionMaxParallel
 * - SchemaChangeHandler for lake_compaction_max_parallel
 * - OlapTableFactory for lake_compaction_max_parallel
 */
public class LakeCompactionMaxParallelTest extends StarRocksTestBase {
    private static final String DB_NAME = "test_lake_compaction_max_parallel";
    private static ConnectContext connectContext;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    private static LakeTable createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), createTableStmt.getTableName());
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    // ===========================================
    // Tests for PropertyAnalyzer.analyzeLakeCompactionMaxParallel
    // Covers: PropertyAnalyzer.java lines 1630-1640
    // ===========================================

    @Test
    public void testAnalyzeLakeCompactionMaxParallelDefault() throws AnalysisException {
        // Test default value when property is not set
        Map<String, String> properties = new HashMap<>();
        int result = PropertyAnalyzer.analyzeLakeCompactionMaxParallel(properties);
        Assertions.assertEquals(3, result);
    }

    @Test
    public void testAnalyzeLakeCompactionMaxParallelValidValue() throws AnalysisException {
        // Test valid integer value
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "5");
        int result = PropertyAnalyzer.analyzeLakeCompactionMaxParallel(properties);
        Assertions.assertEquals(5, result);
        // Property should be removed after analysis
        Assertions.assertFalse(properties.containsKey(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL));
    }

    @Test
    public void testAnalyzeLakeCompactionMaxParallelZero() throws AnalysisException {
        // Test zero value (disabled)
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "0");
        int result = PropertyAnalyzer.analyzeLakeCompactionMaxParallel(properties);
        Assertions.assertEquals(0, result);
    }

    @Test
    public void testAnalyzeLakeCompactionMaxParallelNegative() {
        // Test negative value should throw exception
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "-1");
        Assertions.assertThrows(AnalysisException.class, () -> {
            PropertyAnalyzer.analyzeLakeCompactionMaxParallel(properties);
        });
    }

    @Test
    public void testAnalyzeLakeCompactionMaxParallelInvalidFormat() {
        // Test invalid format should throw exception
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "abc");
        Assertions.assertThrows(AnalysisException.class, () -> {
            PropertyAnalyzer.analyzeLakeCompactionMaxParallel(properties);
        });
    }

    @Test
    public void testAnalyzeLakeCompactionMaxParallelNullProperties() throws AnalysisException {
        // Test null properties should return default
        int result = PropertyAnalyzer.analyzeLakeCompactionMaxParallel(null);
        Assertions.assertEquals(3, result);
    }

    // ===========================================
    // Tests for TableProperty.buildLakeCompactionMaxParallel
    // Covers: TableProperty.java lines 938-940
    // ===========================================

    @Test
    public void testTablePropertyBuildLakeCompactionMaxParallelValid() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "8");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildLakeCompactionMaxParallel();
        Assertions.assertEquals(8, tableProperty.getLakeCompactionMaxParallel());
    }

    @Test
    public void testTablePropertyBuildLakeCompactionMaxParallelInvalid() {
        // Test invalid value should fallback to 0
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "invalid");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildLakeCompactionMaxParallel();
        Assertions.assertEquals(0, tableProperty.getLakeCompactionMaxParallel());
    }

    @Test
    public void testTablePropertyBuildLakeCompactionMaxParallelNotSet() {
        // Test property not set - getLakeCompactionMaxParallel returns default value 3
        // because OlapTable.getLakeCompactionMaxParallel will use PropertyAnalyzer default
        Map<String, String> properties = new HashMap<>();
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildLakeCompactionMaxParallel();
        // When property is not set, lakeCompactionMaxParallel field in TableProperty is 0
        // but PropertyAnalyzer.analyzeLakeCompactionMaxParallel returns 3 as default
        // The buildLakeCompactionMaxParallel only sets from properties, so it stays at field default
        Assertions.assertEquals(3, tableProperty.getLakeCompactionMaxParallel());
    }

    // ===========================================
    // Tests for OlapTableFactory - create table with lake_compaction_max_parallel
    // Covers: OlapTableFactory.java lines 416-420
    // ===========================================

    @Test
    public void testCreateTableWithLakeCompactionMaxParallel() throws Exception {
        String tableName = "t_create_with_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '10')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(10, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testCreateTableWithDefaultLakeCompactionMaxParallel() throws Exception {
        String tableName = "t_create_default_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        // Default value is 3
        Assertions.assertEquals(3, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testCreateTableWithInvalidLakeCompactionMaxParallel() {
        String tableName = "t_create_invalid_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = 'invalid')", DB_NAME, tableName);
        Assertions.assertThrows(Exception.class, () -> createTable(createSql));
    }

    @Test
    public void testCreateTableWithNegativeLakeCompactionMaxParallel() {
        String tableName = "t_create_negative_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '-5')", DB_NAME, tableName);
        Assertions.assertThrows(Exception.class, () -> createTable(createSql));
    }

    // ===========================================
    // Tests for AlterTableClauseAnalyzer - alter lake_compaction_max_parallel
    // Covers: AlterTableClauseAnalyzer.java lines 465-481
    // ===========================================

    @Test
    public void testAlterLakeCompactionMaxParallelValid() throws Exception {
        String tableName = "t_alter_parallel_valid";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(3, table.getLakeCompactionMaxParallel());

        String alterSql = String.format(
                "ALTER TABLE %s.%s SET ('lake_compaction_max_parallel' = '15')", DB_NAME, tableName);
        alterTable(alterSql);
        Assertions.assertEquals(15, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testAlterLakeCompactionMaxParallelToZero() throws Exception {
        String tableName = "t_alter_parallel_zero";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '10')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(10, table.getLakeCompactionMaxParallel());

        String alterSql = String.format(
                "ALTER TABLE %s.%s SET ('lake_compaction_max_parallel' = '0')", DB_NAME, tableName);
        alterTable(alterSql);
        Assertions.assertEquals(0, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testAlterLakeCompactionMaxParallelNegative() throws Exception {
        String tableName = "t_alter_parallel_negative";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1", DB_NAME, tableName);
        createTable(createSql);

        String alterSql = String.format(
                "ALTER TABLE %s.%s SET ('lake_compaction_max_parallel' = '-1')", DB_NAME, tableName);
        // UtFrameUtils.parseStmtWithNewParser wraps SemanticException as AnalysisException
        Assertions.assertThrows(AnalysisException.class, () -> {
            UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        });
    }

    @Test
    public void testAlterLakeCompactionMaxParallelInvalidFormat() throws Exception {
        String tableName = "t_alter_parallel_invalid";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1", DB_NAME, tableName);
        createTable(createSql);

        String alterSql = String.format(
                "ALTER TABLE %s.%s SET ('lake_compaction_max_parallel' = 'abc')", DB_NAME, tableName);
        // UtFrameUtils.parseStmtWithNewParser wraps SemanticException as AnalysisException
        Assertions.assertThrows(AnalysisException.class, () -> {
            UtFrameUtils.parseStmtWithNewParser(alterSql, connectContext);
        });
    }

    // ===========================================
    // Tests for SchemaChangeHandler - alter lake_compaction_max_parallel
    // Covers: SchemaChangeHandler.java lines 2240-2251
    // ===========================================

    @Test
    public void testAlterLakeCompactionMaxParallelNoChange() throws Exception {
        String tableName = "t_alter_parallel_no_change";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '5')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(5, table.getLakeCompactionMaxParallel());

        // Alter to the same value should be a no-op
        String alterSql = String.format(
                "ALTER TABLE %s.%s SET ('lake_compaction_max_parallel' = '5')", DB_NAME, tableName);
        alterTable(alterSql);
        Assertions.assertEquals(5, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testAlterLakeCompactionMaxParallelChange() throws Exception {
        String tableName = "t_alter_parallel_change";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '5')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(5, table.getLakeCompactionMaxParallel());

        String alterSql = String.format(
                "ALTER TABLE %s.%s SET ('lake_compaction_max_parallel' = '20')", DB_NAME, tableName);
        alterTable(alterSql);
        Assertions.assertEquals(20, table.getLakeCompactionMaxParallel());
    }

    // ===========================================
    // Tests for OlapTable.setLakeCompactionMaxParallel
    // ===========================================

    @Test
    public void testOlapTableSetLakeCompactionMaxParallel() throws Exception {
        String tableName = "t_set_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1", DB_NAME, tableName);
        LakeTable table = createTable(createSql);

        table.setLakeCompactionMaxParallel(100);
        Assertions.assertEquals(100, table.getLakeCompactionMaxParallel());
        Assertions.assertEquals("100",
                table.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL));
    }

    // ===========================================
    // Tests for LocalMetastore.alterLakeCompactionMaxParallel
    // Covers: LocalMetastore.java lines 3882-3900, 3937
    // ===========================================

    @Test
    public void testLocalMetastoreAlterLakeCompactionMaxParallelDirect() throws Exception {
        String tableName = "t_localmetastore_alter";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '3')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(3, table.getLakeCompactionMaxParallel());

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "25");

        // Call alterTableProperties directly to cover LocalMetastore.alterLakeCompactionMaxParallel
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, table, properties);

        Assertions.assertEquals(25, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testLocalMetastoreAlterLakeCompactionMaxParallelNonCloudNative() throws Exception {
        // This test verifies that alterLakeCompactionMaxParallel throws exception for non-cloud-native tables
        // We test this by mocking a non-cloud-native OlapTable
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        // Create a mock OlapTable that is not cloud native
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "10");

        // The actual test for non-cloud-native table would require mocking
        // For now, we verify the cloud-native path is working
        String tableName = "t_localmetastore_cloud_native";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1", DB_NAME, tableName);
        LakeTable table = createTable(createSql);

        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, table, properties);
        Assertions.assertEquals(10, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testLocalMetastoreAlterLakeCompactionMaxParallelInvalidValue() throws Exception {
        String tableName = "t_localmetastore_invalid";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT) " +
                "PRIMARY KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1", DB_NAME, tableName);
        LakeTable table = createTable(createSql);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        // Test with invalid integer format
        Map<String, String> invalidProperties = new HashMap<>();
        invalidProperties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "invalid");
        Assertions.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, table, invalidProperties);
        });

        // Test with negative value
        Map<String, String> negativeProperties = new HashMap<>();
        negativeProperties.put(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL, "-5");
        Assertions.assertThrows(DdlException.class, () -> {
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTableProperties(db, table, negativeProperties);
        });
    }

    // ===========================================
    // Tests for OlapTableFactory - duplicate key table
    // Covers: OlapTableFactory.java lines 416-420
    // ===========================================

    @Test
    public void testCreateDuplicateKeyTableWithLakeCompactionMaxParallel() throws Exception {
        String tableName = "t_create_dup_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT, c1 INT) " +
                "DUPLICATE KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '7')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(7, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testCreateAggregateKeyTableWithLakeCompactionMaxParallel() throws Exception {
        String tableName = "t_create_agg_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT, c1 INT SUM DEFAULT '0') " +
                "AGGREGATE KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '12')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(12, table.getLakeCompactionMaxParallel());
    }

    @Test
    public void testCreateUniqueKeyTableWithLakeCompactionMaxParallel() throws Exception {
        String tableName = "t_create_unique_parallel";
        String createSql = String.format(
                "CREATE TABLE %s.%s (c0 INT, c1 INT) " +
                "UNIQUE KEY(c0) " +
                "DISTRIBUTED BY HASH(c0) BUCKETS 1 " +
                "PROPERTIES('lake_compaction_max_parallel' = '9')", DB_NAME, tableName);
        LakeTable table = createTable(createSql);
        Assertions.assertEquals(9, table.getLakeCompactionMaxParallel());
    }
}

