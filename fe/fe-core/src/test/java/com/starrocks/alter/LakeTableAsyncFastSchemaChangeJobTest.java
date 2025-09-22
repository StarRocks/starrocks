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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LakeTableAsyncFastSchemaChangeJobTest extends StarRocksTestBase {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_fast_schema_evolution";

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
    }

    private static LakeTable createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        LakeTable table = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), createTableStmt.getTableName());
        table.addRelatedMaterializedView(new MvId(db.getId(), GlobalStateMgr.getCurrentState().getNextId()));
        return table;
    }

    private static void alterTable(ConnectContext connectContext, String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    private AlterJobV2 getAlterJob(Table table) {
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        List<AlterJobV2> jobs = alterJobMgr.getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assertions.assertEquals(1, jobs.size());
        return jobs.get(0);
    }

    private AlterJobV2 executeAlterAndWaitFinish(Table table, String sql, boolean expectFastSchemaEvolution) throws Exception {
        alterTable(connectContext, sql);
        AlterJobV2 alterJob = getAlterJob(table);
        alterJob.run();
        while (alterJob.getJobState() != AlterJobV2.JobState.FINISHED) {
            alterJob.run();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJob.getJobState());
        Assertions.assertEquals(expectFastSchemaEvolution, (alterJob instanceof LakeTableAsyncFastSchemaChangeJob));
        return alterJob;
    }

    @Test
    public void test() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t0(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                    "BUCKETS 2 PROPERTIES('fast_schema_evolution'='true')");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();

        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t0 ADD COLUMN c1 BIGINT", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(1, columns.get(1).getUniqueId());
            Assertions.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
        oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();
        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t0 DROP COLUMN c1", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(1, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
        oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();
        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t0 ADD COLUMN c1 BIGINT", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(2, columns.get(1).getUniqueId());
            Assertions.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
    }

    @Test
    public void testGetInfo() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t1(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                    "BUCKETS 2 PROPERTIES('fast_schema_evolution'='true')");
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
        Assertions.assertEquals(table.getIndexNameById(table.getBaseIndexId()), info.get(4));
        Assertions.assertEquals(table.getBaseIndexId(), info.get(5));
        Assertions.assertEquals(table.getBaseIndexId(), info.get(6));
        Assertions.assertEquals(String.format("%d:0", table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaVersion()),
                    info.get(7));
        Assertions.assertEquals(job.getTransactionId().get(), info.get(8));
        Assertions.assertEquals(job.getJobState().name(), info.get(9));
        Assertions.assertEquals(job.errMsg, info.get(10));
        Assertions.assertEquals(job.getTimeoutMs() / 1000, info.get(12));
        Assertions.assertEquals("default_warehouse", info.get(13));
    }

    @Test
    public void testReplay() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable table = createTable(connectContext, "CREATE TABLE t3(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                    "BUCKETS 2 PROPERTIES('fast_schema_evolution'='true')");
        SchemaChangeHandler handler = GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler();
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser("ALTER TABLE t3 ADD COLUMN c1 INT",
                    connectContext);
        LakeTableAsyncFastSchemaChangeJob job = (LakeTableAsyncFastSchemaChangeJob)
                    handler.analyzeAndCreateJob(stmt.getAlterClauseList(), db, table);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Partition partition = table.getPartition("t3");
        long baseIndexId = table.getBaseIndexId();
        long initVisibleVersion = partition.getDefaultPhysicalPartition().getVisibleVersion();
        long initNextVersion = partition.getDefaultPhysicalPartition().getNextVersion();
        Assertions.assertEquals(initVisibleVersion + 1, initNextVersion);

        LakeTableAsyncFastSchemaChangeJob replaySourceJob = new LakeTableAsyncFastSchemaChangeJob(job);
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, replaySourceJob.getJobState());
        job.replay(replaySourceJob);
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());

        replaySourceJob.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);
        replaySourceJob.getCommitVersionMap().put(partition.getDefaultPhysicalPartition().getId(), initNextVersion);
        replaySourceJob.addDirtyPartitionIndex(partition.getDefaultPhysicalPartition().getId(), baseIndexId,
                partition.getDefaultPhysicalPartition().getIndex(baseIndexId));
        job.replay(replaySourceJob);
        Assertions.assertEquals(initNextVersion + 1, partition.getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(initVisibleVersion, partition.getDefaultPhysicalPartition().getVisibleVersion());

        replaySourceJob.setJobState(AlterJobV2.JobState.FINISHED);
        replaySourceJob.setFinishedTimeMs(System.currentTimeMillis());
        Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        job.replay(replaySourceJob);
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assertions.assertEquals(replaySourceJob.getFinishedTimeMs(), job.getFinishedTimeMs());
        Assertions.assertEquals(initVisibleVersion + 1, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                partition.getDefaultPhysicalPartition().getNextVersion());
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(2, table.getBaseSchema().size());
        Assertions.assertEquals(0, table.getBaseSchema().get(0).getUniqueId());
        Assertions.assertEquals("c0", table.getBaseSchema().get(0).getName());
        Assertions.assertEquals(1, table.getBaseSchema().get(1).getUniqueId());
        Assertions.assertEquals("c1", table.getBaseSchema().get(1).getName());
    }

    @Test
    public void testSortKey() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t_test_sort_key(c0 INT, c1 BIGINT) PRIMARY KEY(c0) " +
                    "DISTRIBUTED BY HASH(c0) BUCKETS 2 ORDER BY(c1)");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();

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
            Assertions.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
        oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();
        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t_test_sort_key DROP COLUMN c2", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(1, columns.get(1).getUniqueId());
            Assertions.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
    }

    @Test
    public void testSortKeyIndexesChanged() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t_test_sort_key_index_changed" +
                    "(c0 INT, c1 BIGINT, c2 BIGINT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 ORDER BY(c2)");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();

        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t_test_sort_key_index_changed DROP COLUMN c1", true);
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals("c2", columns.get(1).getName());
            Assertions.assertEquals(2, columns.get(1).getUniqueId());
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(table.getBaseIndexId());
            Assertions.assertTrue(indexMeta.getSchemaId() > oldSchemaId);
            Assertions.assertEquals(Arrays.asList(1), indexMeta.getSortKeyIdxes());
            Assertions.assertEquals(Arrays.asList(2), indexMeta.getSortKeyUniqueIds());
        }
    }

    @Test
    public void testModifyColumnType() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t_modify_type" +
                "(c0 INT, c1 INT, c2 FLOAT, c3 DATE, c4 VARCHAR(10))" +
                "DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                "BUCKETS 1 PROPERTIES('fast_schema_evolution'='true')");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();

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
            List<Column> columns = table.getBaseSchema();
            Assertions.assertEquals(5, columns.size());

            Assertions.assertEquals("c0", columns.get(0).getName());
            Assertions.assertEquals(0, columns.get(0).getUniqueId());
            Assertions.assertEquals(ScalarType.INT, columns.get(0).getType());

            Assertions.assertEquals("c1", columns.get(1).getName());
            Assertions.assertEquals(1, columns.get(1).getUniqueId());
            Assertions.assertEquals(ScalarType.BIGINT, columns.get(1).getType());

            Assertions.assertEquals("c2", columns.get(2).getName());
            Assertions.assertEquals(2, columns.get(2).getUniqueId());
            Assertions.assertEquals(ScalarType.DOUBLE, columns.get(2).getType());

            Assertions.assertEquals("c3", columns.get(3).getName());
            Assertions.assertEquals(3, columns.get(3).getUniqueId());
            Assertions.assertEquals(ScalarType.DATETIME, columns.get(3).getType());

            Assertions.assertEquals("c4", columns.get(4).getName());
            Assertions.assertEquals(4, columns.get(4).getUniqueId());
            Assertions.assertEquals(PrimitiveType.VARCHAR, columns.get(4).getType().getPrimitiveType());
            Assertions.assertEquals(20, ((ScalarType) columns.get(4).getType()).getLength());

            Assertions.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }

        // zonemap index can not be reused
        {
            executeAlterAndWaitFinish(table, "ALTER TABLE t_modify_type MODIFY COLUMN c3 DATE", false);
            Assertions.assertEquals(ScalarType.DATE, table.getBaseSchema().get(3).getType());

            executeAlterAndWaitFinish(table, "ALTER TABLE t_modify_type MODIFY COLUMN c4 INT", false);
            Assertions.assertEquals(ScalarType.INT, table.getBaseSchema().get(4).getType());
        }
    }

    private List<Column> getShortKeyColumns(Database db, OlapTable table) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            long baseIndexId = table.getBaseIndexId();
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(baseIndexId);
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

    private boolean isShortKeyChanged(Database db, OlapTable table, List<Column> oldShortKeys) {
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

    private boolean isSchemaMatch(Database db, OlapTable table, List<String> columNames) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            long baseIndexId = table.getBaseIndexId();
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(baseIndexId);
            List<Column> schema = indexMeta.getSchema();
            if (schema.size() != columNames.size()) {
                return false;

            }
            for (int i = 0; i < schema.size(); i++) {
                if (!schema.get(i).getName().equals(columNames.get(i))) {
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
                PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');""", DB_NAME, tableName);
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
                PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');""", DB_NAME, tableName);
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
                PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true');""", DB_NAME, tableName);
        createTable(connectContext, createStmt);
    }

    private void testAddKeyColumnWithFastSchemaEvolutionBase(String  tableName) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable tbl = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        Assertions.assertNotNull(tbl);
        List<Column> oldShortKeys = getShortKeyColumns(db, tbl);

        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k1 INT KEY DEFAULT '0'", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        Assertions.assertTrue(isSchemaMatch(db, tbl,
                Lists.newArrayList("k0", "k1", "k2", "new_k1", "v0")));

        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k2 INT KEY DEFAULT NULL", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        Assertions.assertTrue(isSchemaMatch(db, tbl,
                Lists.newArrayList("k0", "k1", "k2", "new_k1", "new_k2", "v0")));

        executeAlterAndWaitFinish(tbl,
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN new_k3 DATETIME KEY DEFAULT CURRENT_TIMESTAMP", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        Assertions.assertTrue(isSchemaMatch(db, tbl,
                Lists.newArrayList("k0", "k1", "k2", "new_k1", "new_k2", "new_k3", "v0")));

        executeAlterAndWaitFinish(tbl,
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN new_k4 VARCHAR(100) KEY AFTER k2", DB_NAME, tableName), true);
        Assertions.assertFalse(isShortKeyChanged(db, tbl, oldShortKeys));
        Assertions.assertTrue(isSchemaMatch(db, tbl,
                Lists.newArrayList("k0", "k1", "k2", "new_k4", "new_k1", "new_k2", "new_k3", "v0")));
    }

    private void testAddKeyColumnWithoutFastSchemaEvolutionBase(String tableName) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        LakeTable tbl = (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), tableName);
        Assertions.assertNotNull(tbl);
        List<Column> oldShortKeys = getShortKeyColumns(db, tbl);

        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k1 INT KEY FIRST", DB_NAME, tableName), false);
        Assertions.assertTrue(isShortKeyChanged(db, tbl, oldShortKeys));
        Assertions.assertTrue(isSchemaMatch(db, tbl,
                Lists.newArrayList("new_k1", "k0", "k1", "k2", "v0")));

        oldShortKeys = getShortKeyColumns(db, tbl);
        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s ADD COLUMN new_k2 VARCHAR(100) KEY AFTER k0", DB_NAME, tableName), false);
        Assertions.assertTrue(isShortKeyChanged(db, tbl, oldShortKeys));
        Assertions.assertTrue(isSchemaMatch(db, tbl,
                Lists.newArrayList("new_k1", "k0", "new_k2", "k1", "k2", "v0")));

        oldShortKeys = getShortKeyColumns(db, tbl);
        executeAlterAndWaitFinish(tbl,
                String.format("ALTER TABLE %s.%s MODIFY COLUMN new_k1 BIGINT KEY", DB_NAME, tableName), false);
        Assertions.assertTrue(isShortKeyChanged(db, tbl, oldShortKeys));
        Assertions.assertTrue(isSchemaMatch(db, tbl,
                Lists.newArrayList("new_k1", "k0", "new_k2", "k1", "k2", "v0")));
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
}
