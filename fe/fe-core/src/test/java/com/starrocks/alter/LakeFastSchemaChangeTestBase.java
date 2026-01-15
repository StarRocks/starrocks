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

import com.google.common.collect.Lists;
import com.google.gson.stream.JsonReader;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.TableColumnAlterInfo;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.transaction.TransactionState;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

public abstract class LakeFastSchemaChangeTestBase extends StarRocksTestBase {
    protected static final String DB_NAME = "test_lake_fast_schema_evolution";
    protected static ConnectContext connectContext;
    protected static SchemaChangeHandler schemaChangeHandler;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
        UtFrameUtils.stopBackgroundSchemaChangeHandler(60000);
        UtFrameUtils.setUpForPersistTest();
        schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
    }

    protected static LakeTable createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), createTableStmt.getTableName());
        table.addRelatedMaterializedView(new MvId(db.getId(), GlobalStateMgr.getCurrentState().getNextId()));
        return table;
    }

    protected static void alterTable(ConnectContext connectContext, String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    protected abstract boolean isFastSchemaEvolutionV2();

    protected AlterJobV2 executeAlterAndWaitFinish(
            LakeTable table, String sql, boolean expectFastSchemaEvolution) throws Exception {
        alterTable(connectContext, sql);
        AlterJobV2 alterJob = expectFastSchemaEvolution && isFastSchemaEvolutionV2() ?
                getLatestAlterJob(table) : getUnfinishedAlterJob(table);
        long startTime = System.currentTimeMillis();
        long timeoutMs = 10 * 60 * 1000; // 10 minutes timeout
        while (alterJob.getJobState() != AlterJobV2.JobState.FINISHED
                || table.getState() != OlapTable.OlapTableState.NORMAL) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - startTime > timeoutMs) {
                throw new RuntimeException(
                        String.format("Alter job timeout after 10 minutes. Job state: %s, table state: %s",
                                alterJob.getJobState(), table.getState()));
            }
            alterJob.run();
            Thread.sleep(100);
        }
        if (expectFastSchemaEvolution) {
            Assertions.assertEquals(isFastSchemaEvolutionV2(), (alterJob instanceof SchemaChangeJobV2));
            Assertions.assertEquals(!isFastSchemaEvolutionV2(), (alterJob instanceof LakeTableAsyncFastSchemaChangeJob));
        }
        return alterJob;
    }

    private AlterJobV2 getLatestAlterJob(Table table) {
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        return alterJobMgr.getSchemaChangeHandler().getAlterJobsV2().values().stream()
                .filter(job -> table.getId() == job.getTableId())
                .max(Comparator.comparingLong(AlterJobV2::getJobId)).orElse(null);
    }

    private AlterJobV2 getUnfinishedAlterJob(Table table) {
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        List<AlterJobV2> jobs = alterJobMgr.getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size());
        return jobs.get(0);
    }

    @Test
    public void testBasic() throws Exception {
        String createSql = String.format("""
                CREATE TABLE t0 (
                c0 INT
                ) DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 2
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", isFastSchemaEvolutionV2());
        LakeTable table = createTable(connectContext, createSql);
        long oldSchemaId = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();

        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t0 ADD COLUMN c1 BIGINT", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(1, columns.get(1).getUniqueId());
            Assertions.assertTrue(table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId() > oldSchemaId);
        }
        oldSchemaId = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();
        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t0 DROP COLUMN c1", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(1, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertTrue(table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId() > oldSchemaId);
        }
        oldSchemaId = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();
        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t0 ADD COLUMN c1 BIGINT", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(2, columns.get(1).getUniqueId());
            Assertions.assertTrue(table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId() > oldSchemaId);
        }
    }

    @Test
    public void testGetInfo() throws Exception {
        String createSql = String.format("""
                CREATE TABLE t1 (
                c0 INT
                ) DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 2
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", isFastSchemaEvolutionV2());
        LakeTable table = createTable(connectContext, createSql);
        AlterJobV2 job = executeAlterAndWaitFinish(table, "ALTER TABLE t1 ADD COLUMN c1 BIGINT", true);
        List<List<Comparable>> infoList = new ArrayList<>();
        job.getInfo(infoList);
        Assertions.assertEquals(1, infoList.size());
        List<Comparable> info = infoList.get(0);
        Assertions.assertEquals(14, info.size());
        Assertions.assertEquals(job.getJobId(), info.get(0));
        Assertions.assertEquals(table.getName(), info.get(1));
        Assertions.assertEquals(TimeUtils.longToTimeString(job.createTimeMs), info.get(2));
        Assertions.assertEquals(TimeUtils.longToTimeString(job.finishedTimeMs), info.get(3));
        Assertions.assertEquals(table.getIndexNameByMetaId(table.getBaseIndexMetaId()), info.get(4));
        Assertions.assertEquals(table.getBaseIndexMetaId(), info.get(5));
        Assertions.assertEquals(table.getBaseIndexMetaId(), info.get(6));
        Assertions.assertEquals(String.format("%d:0", table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaVersion()),
                info.get(7));
        Assertions.assertEquals(job.getTransactionId().get(), info.get(8));
        Assertions.assertEquals(job.getJobState().name(), info.get(9));
        Assertions.assertEquals(job.errMsg, info.get(10));
        Assertions.assertEquals(job.getTimeoutMs() / 1000, info.get(12));
        Assertions.assertEquals("default_warehouse", info.get(13));
    }

    @Test
    public void testSortKey() throws Exception {
        String createSql = String.format("""
                CREATE TABLE t_test_sort_key (
                c0 INT,
                c1 BIGINT
                ) PRIMARY KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 2
                ORDER BY(c1)
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", isFastSchemaEvolutionV2());
        LakeTable table = createTable(connectContext, createSql);
        long oldSchemaId = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();

        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t_test_sort_key ADD COLUMN c2 BIGINT", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(3, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(1, columns.get(1).getUniqueId());
            Assertions.assertEquals("c2", columns.get(2).getName());
            Assertions.assertEquals(2, columns.get(2).getUniqueId());
            Assertions.assertTrue(table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId() > oldSchemaId);
        }
        oldSchemaId = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();
        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t_test_sort_key DROP COLUMN c2", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(1, columns.get(1).getUniqueId());
            Assertions.assertTrue(table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId() > oldSchemaId);
        }
    }

    @Test
    public void testSortKeyIndexesChanged() throws Exception {
        String createSql = String.format("""
                CREATE TABLE t_test_sort_key_index_changed (
                c0 INT,
                c1 BIGINT,
                c2 BIGINT
                ) PRIMARY KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 2
                ORDER BY(c2)
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", isFastSchemaEvolutionV2());
        LakeTable table = createTable(connectContext, createSql);
        long oldSchemaId = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();

        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t_test_sort_key_index_changed DROP COLUMN c1", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c2", columns.get(1).getName());
            Assertions.assertEquals(2, columns.get(1).getUniqueId());
            MaterializedIndexMeta indexMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
            Assertions.assertTrue(indexMeta.getSchemaId() > oldSchemaId);
            Assertions.assertEquals(Arrays.asList(1), indexMeta.getSortKeyIdxes());
            Assertions.assertEquals(Arrays.asList(2), indexMeta.getSortKeyUniqueIds());
        }
    }

    @Test
    public void testModifyColumnType() throws Exception {
        String createSql = String.format("""
                CREATE TABLE t_modify_type (
                c0 INT,
                c1 INT,
                c2 FLOAT,
                c3 DATE,
                c4 VARCHAR(10)
                ) DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", isFastSchemaEvolutionV2());
        LakeTable table = createTable(connectContext, createSql);
        long oldSchemaId = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();

        // zonemap index can be reused
        {
            String alterSql = """
                        ALTER TABLE t_modify_type 
                            MODIFY COLUMN c1 BIGINT,
                            MODIFY COLUMN c2 DOUBLE,
                            MODIFY COLUMN c3 DATETIME,
                            MODIFY COLUMN c4 VARCHAR(20)
                        """;
            executeAlterAndWaitFinish(table, alterSql, true);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            List<Pair<String, Type>> columnInfos = new ArrayList<>();
            columnInfos.add(Pair.create("c0", IntegerType.INT));
            columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
            columnInfos.add(Pair.create("c2", FloatType.DOUBLE));
            columnInfos.add(Pair.create("c3", DateType.DATETIME));
            columnInfos.add(Pair.create("c4", TypeFactory.createVarcharType(20)));
            Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));
            Assertions.assertTrue(table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId() > oldSchemaId);
        }

        // zonemap index can not be reused
        {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            executeAlterAndWaitFinish(table, "ALTER TABLE t_modify_type MODIFY COLUMN c3 DATE", false);
            List<Pair<String, Type>> columnInfos = new ArrayList<>();
            columnInfos.add(Pair.create("c0", IntegerType.INT));
            columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
            columnInfos.add(Pair.create("c2", FloatType.DOUBLE));
            columnInfos.add(Pair.create("c3", DateType.DATE));
            columnInfos.add(Pair.create("c4", TypeFactory.createVarcharType(20)));
            Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));

            executeAlterAndWaitFinish(table, "ALTER TABLE t_modify_type MODIFY COLUMN c4 INT", false);
            columnInfos = new ArrayList<>();
            columnInfos.add(Pair.create("c0", IntegerType.INT));
            columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
            columnInfos.add(Pair.create("c2", FloatType.DOUBLE));
            columnInfos.add(Pair.create("c3", DateType.DATE));
            columnInfos.add(Pair.create("c4", IntegerType.INT));
            Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));
        }
    }

    protected List<Column> getShortKeyColumns(Database db, OlapTable table) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            MaterializedIndexMeta indexMeta = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
            List<Column> schema = indexMeta.getSchema();
            List<Column> shortKeyCols = new ArrayList<>();
            List<Integer> sortKeyIdxes = indexMeta.getSortKeyIdxes();
            int count = indexMeta.getShortKeyColumnCount();
            if (sortKeyIdxes != null && !sortKeyIdxes.isEmpty()) {
                for (int i = 0; i < count; i++) {
                    shortKeyCols.add(schema.get(sortKeyIdxes.get(i)));
                }
            } else {
                for (int i = 0; i < count; i++) {
                    shortKeyCols.add(schema.get(i));
                }
            }
            return shortKeyCols;
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
    }

    protected boolean isShortKeyChanged(Database db, OlapTable table, List<Column> oldShortKeys) {
        List<Column> newShortKeys = getShortKeyColumns(db, table);
        if (oldShortKeys.size() != newShortKeys.size()) {
            return true;
        }
        for (int i = 0; i < oldShortKeys.size(); i++) {
            if (!oldShortKeys.get(i).equals(newShortKeys.get(i))) {
                return true;
            }
        }
        return false;
    }

    protected boolean isSchemaNameMatch(Database db, OlapTable table, List<String> columnNames) {
        List<Pair<String, Type>> columnInfos = new ArrayList<>();
        for (String name : columnNames) {
            columnInfos.add(Pair.create(name, null));
        }
        return isSchemaTypeMatches(db, table, columnInfos);
    }

    protected boolean isSchemaTypeMatches(Database db, OlapTable table, List<Pair<String, Type>> columnInfos) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            long baseIndexMetaId = table.getBaseIndexMetaId();
            MaterializedIndexMeta indexMeta = table.getIndexMetaByMetaId(baseIndexMetaId);
            List<Column> schema = indexMeta.getSchema();
            if (schema.size() != columnInfos.size()) {
                return false;
            }
            for (int i = 0; i < schema.size(); i++) {
                Column column = schema.get(i);
                Pair<String, Type> columnInfo = columnInfos.get(i);
                String expectedName = columnInfo.first;
                Type expectedType = columnInfo.second;
                
                if (!column.getName().equals(expectedName)) {
                    return false;
                }
                if (expectedType != null && !column.getType().equals(expectedType)) {
                    return false;
                }
            }
            return true;
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
    }

    private void createDupTableForAddKeyColumnTest(String tableName) throws Exception {
        String createStmt = String.format("""
                CREATE TABLE IF NOT EXISTS %s.%s (
                k0 DATETIME,
                k1 INT,
                k2 BIGINT,\
                v0 VARCHAR(1024)
                ) DUPLICATE  KEY(k0, k1, k2)
                DISTRIBUTED BY HASH(k0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", DB_NAME, tableName, isFastSchemaEvolutionV2());
        createTable(connectContext, createStmt);
    }

    private void createUniqueTableForAddKeyColumnTest(String tableName) throws Exception {
        String createStmt = String.format("""
                CREATE TABLE IF NOT EXISTS  %s.%s (
                k0 DATETIME,
                k1 INT,
                k2 BIGINT,\
                v0 VARCHAR(1024)
                ) UNIQUE KEY(k0, k1, k2)
                DISTRIBUTED BY HASH(k0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", DB_NAME, tableName, isFastSchemaEvolutionV2());
        createTable(connectContext, createStmt);
    }

    private void createAggTableForAddKeyColumnTest(String tableName) throws Exception {
        String createStmt = String.format("""
                CREATE TABLE IF NOT EXISTS %s.%s (
                k0 DATETIME,
                k1 INT,
                k2 BIGINT,\
                v0 INT SUM DEFAULT '0'
                ) AGGREGATE KEY(k0, k1, k2)
                DISTRIBUTED BY HASH(k0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", DB_NAME, tableName, isFastSchemaEvolutionV2());
        createTable(connectContext, createStmt);
    }

    private void testAddKeyColumnWithFastSchemaEvolutionBase(String  tableName) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable tbl = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        Assertions.assertNotNull(tbl);
        List<Column> oldShortKeys = getShortKeyColumns(db, tbl);

        Pair<String, Type> k0Col = Pair.create("k0", tbl.getBaseSchema().get(0).getType());
        Pair<String, Type> k1Col = Pair.create("k1", tbl.getBaseSchema().get(1).getType());
        Pair<String, Type> k2Col = Pair.create("k2", tbl.getBaseSchema().get(2).getType());
        Pair<String, Type> v0Col = Pair.create("v0", tbl.getBaseSchema().get(3).getType());

        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k1 INT KEY DEFAULT '0'", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        List<Pair<String, Type>> columnInfos = new ArrayList<>();
        columnInfos.add(k0Col);
        columnInfos.add(k1Col);
        columnInfos.add(k2Col);
        columnInfos.add(Pair.create("new_k1", IntegerType.INT));
        columnInfos.add(v0Col);
        Assertions.assertTrue(isSchemaTypeMatches(db, tbl, columnInfos));

        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k2 INT KEY DEFAULT NULL", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        columnInfos = new ArrayList<>();
        columnInfos.add(k0Col);
        columnInfos.add(k1Col);
        columnInfos.add(k2Col);
        columnInfos.add(Pair.create("new_k1", IntegerType.INT));
        columnInfos.add(Pair.create("new_k2", IntegerType.INT));
        columnInfos.add(v0Col);
        Assertions.assertTrue(isSchemaTypeMatches(db, tbl, columnInfos));

        executeAlterAndWaitFinish(tbl,
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN new_k3 DATETIME KEY DEFAULT CURRENT_TIMESTAMP", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        columnInfos = new ArrayList<>();
        columnInfos.add(k0Col);
        columnInfos.add(k1Col);
        columnInfos.add(k2Col);
        columnInfos.add(Pair.create("new_k1", IntegerType.INT));
        columnInfos.add(Pair.create("new_k2", IntegerType.INT));
        columnInfos.add(Pair.create("new_k3", DateType.DATETIME));
        columnInfos.add(v0Col);
        Assertions.assertTrue(isSchemaTypeMatches(db, tbl, columnInfos));

        executeAlterAndWaitFinish(tbl,
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN new_k4 VARCHAR(100) KEY AFTER k2", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        columnInfos = new ArrayList<>();
        columnInfos.add(k0Col);
        columnInfos.add(k1Col);
        columnInfos.add(k2Col);
        columnInfos.add(Pair.create("new_k4", TypeFactory.createVarcharType(100)));
        columnInfos.add(Pair.create("new_k1", IntegerType.INT));
        columnInfos.add(Pair.create("new_k2", IntegerType.INT));
        columnInfos.add(Pair.create("new_k3", DateType.DATETIME));
        columnInfos.add(v0Col);
        Assertions.assertTrue(isSchemaTypeMatches(db, tbl, columnInfos));
    }

    private void testAddKeyColumnWithoutFastSchemaEvolutionBase(String tableName) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable tbl = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        Assertions.assertNotNull(tbl);
        List<Column> oldShortKeys = getShortKeyColumns(db, tbl);

        Pair<String, Type> k0Col = Pair.create("k0", tbl.getBaseSchema().get(0).getType());
        Pair<String, Type> k1Col = Pair.create("k1", tbl.getBaseSchema().get(1).getType());
        Pair<String, Type> k2Col = Pair.create("k2", tbl.getBaseSchema().get(2).getType());
        Pair<String, Type> v0Col = Pair.create("v0", tbl.getBaseSchema().get(3).getType());

        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k1 INT KEY FIRST", DB_NAME, tableName), false);
        Assertions.assertTrue(isShortKeyChanged(db, tbl, oldShortKeys));
        List<Pair<String, Type>> columnInfos = new ArrayList<>();
        columnInfos.add(Pair.create("new_k1", IntegerType.INT));
        columnInfos.add(k0Col);
        columnInfos.add(k1Col);
        columnInfos.add(k2Col);
        columnInfos.add(v0Col);
        Assertions.assertTrue(isSchemaTypeMatches(db, tbl, columnInfos));

        oldShortKeys = getShortKeyColumns(db, tbl);
        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k2 VARCHAR(100) KEY AFTER k0", DB_NAME, tableName), false);
        Assertions.assertTrue(isShortKeyChanged(db, tbl, oldShortKeys));
        columnInfos = new ArrayList<>();
        columnInfos.add(Pair.create("new_k1", IntegerType.INT));
        columnInfos.add(k0Col);
        columnInfos.add(Pair.create("new_k2", TypeFactory.createVarcharType(100)));
        columnInfos.add(k1Col);
        columnInfos.add(k2Col);
        columnInfos.add(v0Col);
        Assertions.assertTrue(isSchemaTypeMatches(db, tbl, columnInfos));

        oldShortKeys = getShortKeyColumns(db, tbl);
        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s MODIFY COLUMN new_k1 BIGINT KEY", DB_NAME, tableName), false);
        Assertions.assertTrue(isShortKeyChanged(db, tbl, oldShortKeys));
        columnInfos = new ArrayList<>();
        columnInfos.add(Pair.create("new_k1", IntegerType.BIGINT));
        columnInfos.add(k0Col);
        columnInfos.add(Pair.create("new_k2", TypeFactory.createVarcharType(100)));
        columnInfos.add(k1Col);
        columnInfos.add(k2Col);
        columnInfos.add(v0Col);
        Assertions.assertTrue(isSchemaTypeMatches(db, tbl, columnInfos));
    }

    @Test
    public void testDupTableAddKeyColumnWithFastSchemaEvolution() throws Exception {
        String tableName = "dup_add_key_with_fse";
        createDupTableForAddKeyColumnTest(tableName);
        testAddKeyColumnWithFastSchemaEvolutionBase(tableName);
    }

    @Test
    public void testDupTableAddKeyColumnWithoutFastSchemaEvolution() throws Exception {
        String tableName = "dup_add_key_without_fse";
        createDupTableForAddKeyColumnTest(tableName);
        testAddKeyColumnWithoutFastSchemaEvolutionBase(tableName);
    }

    @Test
    public void testUniqueTableAddKeyColumnWithFastSchemaEvolution() throws Exception {
        String tableName = "unique_add_key_with_fse";
        createUniqueTableForAddKeyColumnTest(tableName);
        testAddKeyColumnWithFastSchemaEvolutionBase(tableName);
    }

    @Test
    public void testUniqueTableAddKeyColumnWithoutFastSchemaEvolution() throws Exception {
        String tableName = "unique_add_key_without_fse";
        createUniqueTableForAddKeyColumnTest(tableName);
        testAddKeyColumnWithoutFastSchemaEvolutionBase(tableName);
    }

    @Test
    public void testAggTableAddKeyColumnWithFastSchemaEvolution() throws Exception {
        String tableName = "agg_add_key_with_fse";
        createAggTableForAddKeyColumnTest(tableName);
        testAddKeyColumnWithFastSchemaEvolutionBase(tableName);
    }

    @Test
    public void testAggTableAddKeyColumnWithoutFastSchemaEvolution() throws Exception {
        String tableName = "agg_add_key_without_fse";
        createAggTableForAddKeyColumnTest(tableName);
        testAddKeyColumnWithoutFastSchemaEvolutionBase(tableName);
    }

    @Test
    public void testModifyColumnTypeWithManuallyCreatedIndex() throws Exception {
        LakeTable table = createTable(connectContext, String.format(
                """
                CREATE TABLE t_modify_index_type (
                    c0 INT,
                    c1 INT,
                    c2 INT,
                    INDEX idx1 (c1) USING BITMAP
                )
                DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES(
                    'cloud_native_fast_schema_evolution_v2'='%s',
                    'bloom_filter_columns' = 'c2'
                )
                """, isFastSchemaEvolutionV2())
        );

        // bitmap index can not use fast schema evolution
        {
            String alterSql = "ALTER TABLE t_modify_index_type MODIFY COLUMN c1 BIGINT";
            executeAlterAndWaitFinish(table, alterSql, false);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            List<Pair<String, Type>> columnInfos = new ArrayList<>();
            columnInfos.add(Pair.create("c0", IntegerType.INT));
            columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
            columnInfos.add(Pair.create("c2", IntegerType.INT));
            Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));
        }

        // bloomfilter index can use fast schema evolution
        {
            String alterSql = "ALTER TABLE t_modify_index_type MODIFY COLUMN c2 BIGINT";
            executeAlterAndWaitFinish(table, alterSql, true);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
            List<Pair<String, Type>> columnInfos = new ArrayList<>();
            columnInfos.add(Pair.create("c0", IntegerType.INT));
            columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
            columnInfos.add(Pair.create("c2", IntegerType.BIGINT));
            Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));
        }
    }

    @Test
    public void testHistorySchema() throws Exception {
        String createSql = String.format("""
                CREATE TABLE t_history_schema (
                c0 INT
                ) DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", isFastSchemaEvolutionV2());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createTable(connectContext, createSql);

        TransactionState.TxnCoordinator coordinator = new TransactionState.TxnCoordinator(
                TransactionState.TxnSourceType.BE, "127.0.0.1");

        long baseIndexMetaId = table.getBaseIndexMetaId();
        MaterializedIndexMeta preAlterIndexMeta1 = table.getIndexMetaByMetaId(baseIndexMetaId).shallowCopy();
        long runningTxn1 = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), List.of(table.getId()), UUID.randomUUID().toString(), coordinator,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, 60000L);
        AlterJobV2 job1 = executeAlterAndWaitFinish(
                table, "ALTER TABLE t_history_schema ADD COLUMN c1 BIGINT", true);
        List<Pair<String, Type>> columnInfos = new ArrayList<>();
        columnInfos.add(Pair.create("c0", IntegerType.INT));
        columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
        Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));
        OlapTableHistorySchema historySchema1 = getHistorySchema(job1);
        Assertions.assertNotNull(historySchema1);
        Assertions.assertTrue(historySchema1.getHistoryTxnIdThreshold() > runningTxn1);
        assertHistorySchemaMatches(table, baseIndexMetaId, preAlterIndexMeta1, historySchema1);

        MaterializedIndexMeta preAlterIndexMeta2 = table.getIndexMetaByMetaId(baseIndexMetaId).shallowCopy();
        long runningTxn2 = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), List.of(table.getId()), UUID.randomUUID().toString(), coordinator,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, 60000L);
        AlterJobV2 job2 = executeAlterAndWaitFinish(
                table, "ALTER TABLE t_history_schema ADD COLUMN c2 BIGINT", true);
        columnInfos = new ArrayList<>();
        columnInfos.add(Pair.create("c0", IntegerType.INT));
        columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
        columnInfos.add(Pair.create("c2", IntegerType.BIGINT));
        Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));
        OlapTableHistorySchema historySchema2 = getHistorySchema(job2);
        Assertions.assertNotNull(historySchema2);
        Assertions.assertTrue(historySchema2.getHistoryTxnIdThreshold() > runningTxn2);
        assertHistorySchemaMatches(table, baseIndexMetaId, preAlterIndexMeta2, historySchema2);

        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        job1.setFinishedTimeMs(System.currentTimeMillis() / 1000 - Config.alter_table_timeout_second - 100);
        schemaChangeHandler.runAfterCatalogReady();
        Assertions.assertSame(job1, schemaChangeHandler.getAlterJobsV2().get(job1.getJobId()));
        Assertions.assertFalse(getHistorySchema(job1).isExpired());
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(db.getId(), runningTxn1, "fail1");
        schemaChangeHandler.runAfterCatalogReady();
        Assertions.assertNull(schemaChangeHandler.getAlterJobsV2().get(job1.getJobId()));

        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(db.getId(), runningTxn2, "fail2");
        schemaChangeHandler.runAfterCatalogReady();
        Assertions.assertSame(job2, schemaChangeHandler.getAlterJobsV2().get(job2.getJobId()));
        Assertions.assertTrue(historySchema2.isExpired());
        Assertions.assertNull(historySchema2.getSchemaByIndexMetaId(baseIndexMetaId).orElse(null));
        Assertions.assertNull(historySchema2.getSchemaBySchemaId(preAlterIndexMeta2.getSchemaId()).orElse(null));
        job2.setFinishedTimeMs(System.currentTimeMillis() / 1000 - Config.alter_table_timeout_second - 100);
        schemaChangeHandler.runAfterCatalogReady();
        Assertions.assertNull(schemaChangeHandler.getAlterJobsV2().get(job2.getJobId()));
    }

    @Test
    public void testReplay() throws Exception {
        String createSql = String.format("""
                CREATE TABLE test_replay (
                c0 INT,
                c1 INT,
                c2 FLOAT,
                c3 DATE
                ) DUPLICATE KEY(c0)
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES('cloud_native_fast_schema_evolution_v2'='%s');""", isFastSchemaEvolutionV2());
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createTable(connectContext, createSql);

        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        ImageWriter imageWriter = initialImage.getImageWriter();
        GlobalStateMgr.getCurrentState().getLocalMetastore().save(imageWriter);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().save(imageWriter);

        long baseMetaIndexId = table.getBaseIndexMetaId();
        MaterializedIndexMeta preAlterIndexMeta = table.getIndexMetaByMetaId(baseMetaIndexId).shallowCopy();

        String alterSql = """
                        ALTER TABLE test_replay 
                            MODIFY COLUMN c1 BIGINT,
                            MODIFY COLUMN c2 DOUBLE,
                            MODIFY COLUMN c3 DATETIME,
                            ADD COLUMN c4 VARCHAR(20)
                        """;
        AlterJobV2 job = executeAlterAndWaitFinish(table, alterSql, true);
        List<Pair<String, Type>> columnInfos = new ArrayList<>();
        columnInfos.add(Pair.create("c0", IntegerType.INT));
        columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
        columnInfos.add(Pair.create("c2", FloatType.DOUBLE));
        columnInfos.add(Pair.create("c3", DateType.DATETIME));
        columnInfos.add(Pair.create("c4", TypeFactory.createVarcharType(20)));
        Assertions.assertTrue(isSchemaTypeMatches(db, table, columnInfos));
        OlapTableHistorySchema historySchema = getHistorySchema(job);
        Assertions.assertNotNull(historySchema);
        assertHistorySchemaMatches(table, baseMetaIndexId, preAlterIndexMeta, historySchema);

        LocalMetastore restoredMetastore =
                new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        AlterJobMgr restoredAlterJobMgr =
                new AlterJobMgr(new SchemaChangeHandler(), new MaterializedViewHandler(), new SystemHandler());
        JsonReader jsonReader = initialImage.getJsonReader();
        SRMetaBlockReader blockReader = new SRMetaBlockReaderV2(jsonReader);
        restoredMetastore.load(blockReader);
        blockReader.close();
        blockReader = new SRMetaBlockReaderV2(jsonReader);
        restoredAlterJobMgr.load(blockReader);
        blockReader.close();
        Database restoredDb = restoredMetastore.getDb(DB_NAME);
        Assertions.assertNotNull(restoredDb, "Restored database not found");
        LakeTable restoredTable = (LakeTable) restoredMetastore.getTable(DB_NAME, table.getName());
        Assertions.assertNotNull(restoredTable, "Restored table not found");
        Assertions.assertEquals(SchemaInfo.fromMaterializedIndex(table, baseMetaIndexId, preAlterIndexMeta),
                SchemaInfo.fromMaterializedIndex(restoredTable, baseMetaIndexId,
                        restoredTable.getIndexMetaByMetaId(baseMetaIndexId)));

        SchemaChangeHandler restoredHandler = restoredAlterJobMgr.getSchemaChangeHandler();
        LocalMetastore originalMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        try {
            // restoredHandler can restore state to restoredMetastore
            GlobalStateMgr.getCurrentState().setLocalMetastore(restoredMetastore);
            if (isFastSchemaEvolutionV2()) {
                // there should be only 1 edit log for v2
                TableColumnAlterInfo alterInfo = (TableColumnAlterInfo)
                        UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_FAST_ALTER_TABLE_COLUMNS);
                restoredHandler.replayFastSchemaEvolutionMetaChange(alterInfo);
            } else {
                // there is expected 4 edit log in the lifecycle of LakeTableAsyncFastSchemaChangeJob
                for (int i = 0; i < 4; i++) {
                    AlterJobV2 alterJob = (AlterJobV2)
                            UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_JOB_V2);
                    Assertions.assertInstanceOf(LakeTableAsyncFastSchemaChangeJob.class, alterJob);
                    Assertions.assertEquals(job.getJobId(), alterJob.getJobId());
                    restoredHandler.replayAlterJobV2(alterJob);
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().setLocalMetastore(originalMetastore);
        }
        columnInfos = new ArrayList<>();
        columnInfos.add(Pair.create("c0", IntegerType.INT));
        columnInfos.add(Pair.create("c1", IntegerType.BIGINT));
        columnInfos.add(Pair.create("c2", FloatType.DOUBLE));
        columnInfos.add(Pair.create("c3", DateType.DATETIME));
        columnInfos.add(Pair.create("c4", TypeFactory.createVarcharType(20)));
        Assertions.assertTrue(isSchemaTypeMatches(restoredDb, restoredTable, columnInfos));
        Assertions.assertEquals(
                SchemaInfo.fromMaterializedIndex(table, baseMetaIndexId, table.getIndexMetaByMetaId(baseMetaIndexId)),
                SchemaInfo.fromMaterializedIndex(restoredTable, baseMetaIndexId,
                        restoredTable.getIndexMetaByMetaId(baseMetaIndexId)));
        AlterJobV2 restoredJob = restoredHandler.getAlterJobsV2().get(job.getJobId());
        Assertions.assertNotNull(restoredJob);
        OlapTableHistorySchema restoredHistorySchema = getHistorySchema(restoredJob);
        Assertions.assertNotNull(restoredHistorySchema);
        Assertions.assertEquals(historySchema.getHistoryTxnIdThreshold(), restoredHistorySchema.getHistoryTxnIdThreshold());
        assertHistorySchemaMatches(table, baseMetaIndexId, preAlterIndexMeta, restoredHistorySchema);
    }

    private OlapTableHistorySchema getHistorySchema(AlterJobV2 alterJob) {
        if (alterJob instanceof LakeTableAsyncFastSchemaChangeJob) {
            return ((LakeTableAsyncFastSchemaChangeJob) alterJob).getHistorySchema().orElse(null);
        } else if (alterJob instanceof SchemaChangeJobV2) {
            return ((SchemaChangeJobV2) alterJob).getHistorySchema().orElse(null);
        } else {
            return null;
        }
    }

    protected void assertHistorySchemaMatches(LakeTable table, long indexMetaId, MaterializedIndexMeta originMeta,
                                            OlapTableHistorySchema historySchema) {
        SchemaInfo originSchemaInfo = SchemaInfo.fromMaterializedIndex(table, indexMetaId, originMeta);
        Assertions.assertNotNull(historySchema);
        Assertions.assertEquals(originSchemaInfo, historySchema.getSchemaByIndexMetaId(indexMetaId).orElse(null));
        Assertions.assertEquals(originSchemaInfo, historySchema.getSchemaBySchemaId(originMeta.getSchemaId()).orElse(null));
    }
}
