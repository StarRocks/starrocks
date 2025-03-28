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
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LakeTableAsyncFastSchemaChangeJobTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_fast_schema_evolution";

    @BeforeClass
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

    private LakeTableAsyncFastSchemaChangeJob getAlterJob(Table table) {
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        List<AlterJobV2> jobs = alterJobMgr.getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        Assert.assertEquals(1, jobs.size());
        AlterJobV2 alterJob = jobs.get(0);
        Assert.assertTrue(alterJob instanceof LakeTableAsyncFastSchemaChangeJob);
        return (LakeTableAsyncFastSchemaChangeJob) alterJob;
    }

    private LakeTableAsyncFastSchemaChangeJob removeAlterJob(Table table) {
        LakeTableAsyncFastSchemaChangeJob job = getAlterJob(table);
        AlterHandler handler = GlobalStateMgr.getCurrentState().getAlterJobMgr().getSchemaChangeHandler();
        handler.alterJobsV2.remove(job.getJobId());
        return job;
    }

    private AlterJobV2 mustAlterTable(Table table, String sql) throws Exception {
        alterTable(connectContext, sql);
        AlterJobV2 alterJob = getAlterJob(table);
        alterJob.run();
        while (alterJob.getJobState() != AlterJobV2.JobState.FINISHED) {
            alterJob.run();
            Thread.sleep(100);
        }
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJob.getJobState());
        return alterJob;
    }

    @Test
    public void test() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t0(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                    "BUCKETS 2 PROPERTIES('fast_schema_evolution'='true')");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();

        {
            mustAlterTable(table, "ALTER TABLE t0 ADD COLUMN c1 BIGINT");
            List<Column> columns = table.getBaseSchema();
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals("c0", columns.get(0).getName());
            Assert.assertEquals(0, columns.get(0).getUniqueId());
            Assert.assertEquals("c1", columns.get(1).getName());
            Assert.assertEquals(1, columns.get(1).getUniqueId());
            Assert.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
        oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();
        {
            mustAlterTable(table, "ALTER TABLE t0 DROP COLUMN c1");
            List<Column> columns = table.getBaseSchema();
            Assert.assertEquals(1, columns.size());
            Assert.assertEquals("c0", columns.get(0).getName());
            Assert.assertEquals(0, columns.get(0).getUniqueId());
            Assert.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
        oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();
        {
            mustAlterTable(table, "ALTER TABLE t0 ADD COLUMN c1 BIGINT");
            List<Column> columns = table.getBaseSchema();
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals("c0", columns.get(0).getName());
            Assert.assertEquals(0, columns.get(0).getUniqueId());
            Assert.assertEquals("c1", columns.get(1).getName());
            Assert.assertEquals(2, columns.get(1).getUniqueId());
            Assert.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
    }

    @Test
    public void testGetInfo() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t1(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                    "BUCKETS 2 PROPERTIES('fast_schema_evolution'='true')");
        AlterJobV2 job = mustAlterTable(table, "ALTER TABLE t1 ADD COLUMN c1 BIGINT");
        List<List<Comparable>> infoList = new ArrayList<>();
        job.getInfo(infoList);
        Assert.assertEquals(1, infoList.size());
        List<Comparable> info = infoList.get(0);
        Assert.assertEquals(14, info.size());
        Assert.assertEquals(job.getJobId(), info.get(0));
        Assert.assertEquals(table.getName(), info.get(1));
        Assert.assertEquals(TimeUtils.longToTimeString(job.createTimeMs), info.get(2));
        Assert.assertEquals(TimeUtils.longToTimeString(job.finishedTimeMs), info.get(3));
        Assert.assertEquals(table.getIndexNameById(table.getBaseIndexId()), info.get(4));
        Assert.assertEquals(table.getBaseIndexId(), info.get(5));
        Assert.assertEquals(table.getBaseIndexId(), info.get(6));
        Assert.assertEquals(String.format("%d:0", table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaVersion()),
                    info.get(7));
        Assert.assertEquals(job.getTransactionId().get(), info.get(8));
        Assert.assertEquals(job.getJobState().name(), info.get(9));
        Assert.assertEquals(job.errMsg, info.get(10));
        Assert.assertEquals(job.getTimeoutMs() / 1000, info.get(12));
        Assert.assertEquals("default_warehouse", info.get(13));
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
        Assert.assertNotNull(job);
        Assert.assertEquals(AlterJobV2.JobState.PENDING, job.getJobState());
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Partition partition = table.getPartition("t3");
        long baseIndexId = table.getBaseIndexId();
        long initVisibleVersion = partition.getDefaultPhysicalPartition().getVisibleVersion();
        long initNextVersion = partition.getDefaultPhysicalPartition().getNextVersion();
        Assert.assertEquals(initVisibleVersion + 1, initNextVersion);

        LakeTableAsyncFastSchemaChangeJob replaySourceJob = new LakeTableAsyncFastSchemaChangeJob(job);
        Assert.assertEquals(AlterJobV2.JobState.PENDING, replaySourceJob.getJobState());
        job.replay(replaySourceJob);
        Assert.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());

        replaySourceJob.setJobState(AlterJobV2.JobState.FINISHED_REWRITING);
        replaySourceJob.getCommitVersionMap().put(partition.getDefaultPhysicalPartition().getId(), initNextVersion);
        replaySourceJob.addDirtyPartitionIndex(partition.getDefaultPhysicalPartition().getId(), baseIndexId,
                partition.getDefaultPhysicalPartition().getIndex(baseIndexId));
        job.replay(replaySourceJob);
        Assert.assertEquals(initNextVersion + 1, partition.getDefaultPhysicalPartition().getNextVersion());
        Assert.assertEquals(initVisibleVersion, partition.getDefaultPhysicalPartition().getVisibleVersion());

        replaySourceJob.setJobState(AlterJobV2.JobState.FINISHED);
        replaySourceJob.setFinishedTimeMs(System.currentTimeMillis());
        Assert.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
        job.replay(replaySourceJob);
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, job.getJobState());
        Assert.assertEquals(replaySourceJob.getFinishedTimeMs(), job.getFinishedTimeMs());
        Assert.assertEquals(initVisibleVersion + 1, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assert.assertEquals(partition.getDefaultPhysicalPartition().getVisibleVersion() + 1,
                partition.getDefaultPhysicalPartition().getNextVersion());
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assert.assertEquals(2, table.getBaseSchema().size());
        Assert.assertEquals(0, table.getBaseSchema().get(0).getUniqueId());
        Assert.assertEquals("c0", table.getBaseSchema().get(0).getName());
        Assert.assertEquals(1, table.getBaseSchema().get(1).getUniqueId());
        Assert.assertEquals("c1", table.getBaseSchema().get(1).getName());
    }

    @Test
    public void testSortKey() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t_test_sort_key(c0 INT, c1 BIGINT) PRIMARY KEY(c0) " +
                    "DISTRIBUTED BY HASH(c0) BUCKETS 2 ORDER BY(c1)");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();

        {
            mustAlterTable(table, "ALTER TABLE t_test_sort_key ADD COLUMN c2 BIGINT");
            List<Column> columns = table.getBaseSchema();
            Assert.assertEquals(3, columns.size());
            Assert.assertEquals("c0", columns.get(0).getName());
            Assert.assertEquals(0, columns.get(0).getUniqueId());
            Assert.assertEquals("c1", columns.get(1).getName());
            Assert.assertEquals(1, columns.get(1).getUniqueId());
            Assert.assertEquals("c2", columns.get(2).getName());
            Assert.assertEquals(2, columns.get(2).getUniqueId());
            Assert.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
        oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();
        {
            mustAlterTable(table, "ALTER TABLE t_test_sort_key DROP COLUMN c2");
            List<Column> columns = table.getBaseSchema();
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals("c0", columns.get(0).getName());
            Assert.assertEquals(0, columns.get(0).getUniqueId());
            Assert.assertEquals("c1", columns.get(1).getName());
            Assert.assertEquals(1, columns.get(1).getUniqueId());
            Assert.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
    }

    @Test
    public void testSortKeyIndexesChanged() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t_test_sort_key_index_changed" +
                    "(c0 INT, c1 BIGINT, c2 BIGINT) PRIMARY KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 2 ORDER BY(c2)");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();

        {
            mustAlterTable(table, "ALTER TABLE t_test_sort_key_index_changed DROP COLUMN c1");
            List<Column> columns = table.getBaseSchema();
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals("c0", columns.get(0).getName());
            Assert.assertEquals(0, columns.get(0).getUniqueId());
            Assert.assertEquals("c2", columns.get(1).getName());
            Assert.assertEquals(2, columns.get(1).getUniqueId());
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(table.getBaseIndexId());
            Assert.assertTrue(indexMeta.getSchemaId() > oldSchemaId);
            Assert.assertEquals(Arrays.asList(1), indexMeta.getSortKeyIdxes());
            Assert.assertEquals(Arrays.asList(2), indexMeta.getSortKeyUniqueIds());
        }
    }

    @Test
    public void testModifyColumnType() throws Exception {
        LakeTable table = createTable(connectContext, "CREATE TABLE t_modify_type" +
                    "(c0 INT, c1 INT, c2 VARCHAR(5), c3 DATE)" +
                    "DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) " +
                    "BUCKETS 1 PROPERTIES('fast_schema_evolution'='true')");
        long oldSchemaId = table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId();
        {
            mustAlterTable(table, "ALTER TABLE t_modify_type MODIFY COLUMN c1 BIGINT, MODIFY COLUMN c2 VARCHAR(10)," +
                        "MODIFY COLUMN c3 DATETIME");
            List<Column> columns = table.getBaseSchema();
            Assert.assertEquals(4, columns.size());

            Assert.assertEquals("c0", columns.get(0).getName());
            Assert.assertEquals(0, columns.get(0).getUniqueId());
            Assert.assertEquals(ScalarType.INT, columns.get(0).getType());

            Assert.assertEquals("c1", columns.get(1).getName());
            Assert.assertEquals(1, columns.get(1).getUniqueId());
            Assert.assertEquals(ScalarType.BIGINT, columns.get(1).getType());

            Assert.assertEquals("c2", columns.get(2).getName());
            Assert.assertEquals(2, columns.get(2).getUniqueId());
            Assert.assertEquals(PrimitiveType.VARCHAR, columns.get(2).getType().getPrimitiveType());
            Assert.assertEquals(10, ((ScalarType) columns.get(2).getType()).getLength());

            Assert.assertEquals("c3", columns.get(3).getName());
            Assert.assertEquals(3, columns.get(3).getUniqueId());
            Assert.assertEquals(ScalarType.DATETIME, columns.get(3).getType());

            Assert.assertTrue(table.getIndexIdToMeta().get(table.getBaseIndexId()).getSchemaId() > oldSchemaId);
            Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        }
    }
}
