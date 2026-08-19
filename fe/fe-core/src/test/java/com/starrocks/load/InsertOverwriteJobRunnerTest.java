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


package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.InsertPreSplitHook;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.common.DmlException;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.List;

public class InsertOverwriteJobRunnerTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static PseudoCluster cluster;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();

        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }

        starRocksAssert.withDatabase("insert_overwrite_test").useDatabase("insert_overwrite_test")
                .withTable(
                        "CREATE TABLE insert_overwrite_test.t1(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable(
                        "CREATE TABLE insert_overwrite_test.t2(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        starRocksAssert
                .withTable("create table insert_overwrite_test.t3(c1 int, c2 int, c3 int) " +
                        "DUPLICATE KEY(c1, c2) PARTITION BY RANGE(c1) "
                        + "(PARTITION p1 VALUES [('-2147483648'), ('10')), PARTITION p2 VALUES [('10'), ('20')))"
                        + " DISTRIBUTED BY HASH(`c2`) BUCKETS 2 PROPERTIES('replication_num'='1');")
                .withTable("create table insert_overwrite_test.t4(c1 int, c2 int, c3 int) " +
                        "DUPLICATE KEY(c1, c2) PARTITION BY RANGE(c1) "
                        + "(PARTITION p1 VALUES [('-2147483648'), ('10')), PARTITION p2 VALUES [('10'), ('20')))"
                        + " DISTRIBUTED BY HASH(`c2`) BUCKETS 2 PROPERTIES('replication_num'='1');")
                .withTable("create table insert_overwrite_test.t_lambda_target(k1 int, k2 array<int>) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable("create table insert_overwrite_test.t_lambda_src1(k1 int, k2 array<int>) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable("create table insert_overwrite_test.t_lambda_src2(k1 int, k2 array<int>) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    @Test
    public void testReplayInsertOverwrite() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t1");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(100L, database.getId(), olapTable.getId(),
                Lists.newArrayList(olapTable.getPartition("t1").getId()), false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);
        runner.cancel();
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob.getJobState());

        InsertOverwriteJob insertOverwriteJob2 = new InsertOverwriteJob(100L, database.getId(), olapTable.getId(),
                Lists.newArrayList(olapTable.getPartition("t1").getId()), false);
        InsertOverwriteStateChangeInfo stateChangeInfo = new InsertOverwriteStateChangeInfo(100L,
                InsertOverwriteJobState.OVERWRITE_PENDING, InsertOverwriteJobState.OVERWRITE_RUNNING,
                Lists.newArrayList(2000L), null, Lists.newArrayList(2001L));
        Assertions.assertEquals(100L, stateChangeInfo.getJobId());
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, stateChangeInfo.getFromState());
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, stateChangeInfo.getToState());
        Assertions.assertEquals(Lists.newArrayList(2000L), stateChangeInfo.getSourcePartitionIds());
        Assertions.assertEquals(Lists.newArrayList(2001L), stateChangeInfo.getTmpPartitionIds());

        InsertOverwriteJobRunner runner2 = new InsertOverwriteJobRunner(insertOverwriteJob2);
        runner2.replayStateChange(stateChangeInfo);
        runner2.cancel();
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob2.getJobState());
    }

    @Test
    public void testInsertOverwriteFromStmtExecutor() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String sql = "insert overwrite t1 select * from t2";
        cluster.runSql("insert_overwrite_test", sql);
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getForceDeleteTablets().isEmpty());
    }

    @Test
    public void testInsertOverwrite() throws Exception {
        String sql = "insert overwrite t1 select * from t2";
        InsertStmt insertStmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, insertStmt);
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t1");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(100L, insertStmt, database.getId(), olapTable.getId(),
                WarehouseManager.DEFAULT_WAREHOUSE_ID, false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob, connectContext, executor);
        Assertions.assertFalse(runner.isFinished());
    }

    @Test
    public void testDynamicOverwritePreSplitRunsAfterTransactionIsAssigned() {
        InsertStmt insertStmt = Mockito.mock(InsertStmt.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        InsertOverwriteJob job = new InsertOverwriteJob(
                101L, insertStmt, 11L, 12L, WarehouseManager.DEFAULT_WAREHOUSE_ID, true);
        job.setTxnId(42L);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job, context, executor);

        try (MockedStatic<InsertPreSplitHook> hook = Mockito.mockStatic(InsertPreSplitHook.class)) {
            runner.preSplitDynamicOverwriteTempPartitions();

            hook.verify(() -> InsertPreSplitHook.maybeRunDynamicOverwritePreSplit(
                    insertStmt, context, 42L));
        }
    }

    // ---- static INSERT OVERWRITE pre-split hook ----

    /** A table shaped like a static overwrite target mid-job: real partitions plus a cloned temporary one. */
    private void withClonedTemporaryPartition(String tableName, StaticOverwriteCase body) throws Exception {
        starRocksAssert.withTable(
                "create table insert_overwrite_test." + tableName + "(c1 int, c2 int) "
                        + "DUPLICATE KEY(c1, c2) PARTITION BY RANGE(c1) "
                        + "(PARTITION p1 VALUES [('0'), ('10')), PARTITION p2 VALUES [('10'), ('20'))) "
                        + "DISTRIBUTED BY HASH(c2) BUCKETS 1 PROPERTIES('replication_num'='1')",
                () -> {
                    starRocksAssert.alterTable("ALTER TABLE insert_overwrite_test." + tableName
                            + " ADD TEMPORARY PARTITION tp1 VALUES [('0'), ('10'))");
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getDb("insert_overwrite_test");
                    OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(db.getFullName(), tableName);
                    body.accept(db, table);
                });
    }

    private interface StaticOverwriteCase {
        void accept(Database db, OlapTable table) throws Exception;
    }

    private static InsertOverwriteJobRunner staticOverwriteRunner(
            long jobId, long dbId, long tableId, InsertStmt insertStmt, ConnectContext context,
            List<Long> tmpPartitionIds, List<String> sourcePartitionNames) {
        InsertOverwriteJob job = new InsertOverwriteJob(
                jobId, insertStmt, dbId, tableId, WarehouseManager.DEFAULT_WAREHOUSE_ID,
                /*dynamicOverwrite*/ false);
        job.setTmpPartitionIds(tmpPartitionIds);
        job.setSourcePartitionNames(sourcePartitionNames);
        return new InsertOverwriteJobRunner(job, context, Mockito.mock(StmtExecutor.class));
    }

    @Test
    public void testStaticOverwritePreSplitPassesClonedTemporaryPartitionNames() throws Exception {
        withClonedTemporaryPartition("t_static_presplit_ok", (db, table) -> {
            long temporaryPartitionId = table.getPartition("tp1", true).getId();
            InsertStmt insertStmt = Mockito.mock(InsertStmt.class);
            ConnectContext context = Mockito.mock(ConnectContext.class);
            InsertOverwriteJobRunner runner = staticOverwriteRunner(
                    401L, db.getId(), table.getId(), insertStmt, context,
                    Lists.newArrayList(temporaryPartitionId), Lists.newArrayList("p1"));

            try (MockedStatic<InsertPreSplitHook> hook = Mockito.mockStatic(InsertPreSplitHook.class)) {
                runner.preSplitStaticOverwriteTempPartitions();

                // The ids resolve through OlapTable#getPartition's temporary-partition fallback, and
                // the two lists must reach the hook index-aligned: PreSplitPartitionScope zips them
                // positionally to map each sampled logical partition onto the replacement partition
                // the load will actually write.
                hook.verify(() -> InsertPreSplitHook.maybeRunStaticOverwritePreSplit(
                        insertStmt, context, List.of("p1"), List.of("tp1"), Estimates.ZERO));
            }
        });
    }

    @Test
    public void testStaticOverwritePreSplitSkipsDynamicOverwrite() {
        // Dynamic overwrite has its own hook, which runs after its transaction is assigned so the
        // temporary partitions can be excluded from cleanup. Running both would split twice.
        InsertOverwriteJob job = new InsertOverwriteJob(
                402L, Mockito.mock(InsertStmt.class), 11L, 12L,
                WarehouseManager.DEFAULT_WAREHOUSE_ID, /*dynamicOverwrite*/ true);
        job.setTmpPartitionIds(Lists.newArrayList(2001L));
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(
                job, Mockito.mock(ConnectContext.class), Mockito.mock(StmtExecutor.class));

        try (MockedStatic<InsertPreSplitHook> hook = Mockito.mockStatic(InsertPreSplitHook.class)) {
            runner.preSplitStaticOverwriteTempPartitions();

            hook.verifyNoInteractions();
        }
    }

    @Test
    public void testStaticOverwritePreSplitSkipsWhenTmpPartitionIdsAreUnset() {
        // tmpPartitionIds stays null until prepare() assigns it.
        assertStaticOverwritePreSplitSkipped(403L, null);
    }

    @Test
    public void testStaticOverwritePreSplitSkipsWhenNoTemporaryPartitionWasCloned() {
        // An empty list means the overwrite resolved to no target partition, so there is nothing
        // to pre-split.
        assertStaticOverwritePreSplitSkipped(404L, Lists.newArrayList());
    }

    private static void assertStaticOverwritePreSplitSkipped(long jobId, List<Long> tmpPartitionIds) {
        InsertOverwriteJobRunner runner = staticOverwriteRunner(
                jobId, 11L, 12L, Mockito.mock(InsertStmt.class), Mockito.mock(ConnectContext.class),
                tmpPartitionIds, Lists.newArrayList("p1"));

        try (MockedStatic<InsertPreSplitHook> hook = Mockito.mockStatic(InsertPreSplitHook.class)) {
            runner.preSplitStaticOverwriteTempPartitions();

            hook.verifyNoInteractions();
        }
    }

    @Test
    public void testStaticOverwritePreSplitSkipsWhenDatabaseIsGone() {
        // The database can be dropped between createTempPartitions() and this hook. Skipping keeps
        // the overwrite itself free to fail (or not) on its own terms.
        InsertOverwriteJobRunner runner = staticOverwriteRunner(
                405L, /*dbId*/ 11L, /*tableId*/ 12L, Mockito.mock(InsertStmt.class),
                Mockito.mock(ConnectContext.class),
                Lists.newArrayList(2001L), Lists.newArrayList("p1"));

        try (MockedStatic<InsertPreSplitHook> hook = Mockito.mockStatic(InsertPreSplitHook.class)) {
            runner.preSplitStaticOverwriteTempPartitions();

            hook.verifyNoInteractions();
        }
    }

    @Test
    public void testStaticOverwritePreSplitSkipsWhenAClonedPartitionNoLongerResolves() throws Exception {
        // One id resolving to nothing is the dangerous case: the surviving names would be shorter
        // than job.getSourcePartitionNames(), so the positional zip in PreSplitPartitionScope would
        // silently map a source partition onto some other partition's replacement and split the
        // wrong target. The size check must drop the whole attempt instead.
        withClonedTemporaryPartition("t_static_presplit_vanished", (db, table) -> {
            long temporaryPartitionId = table.getPartition("tp1", true).getId();
            InsertOverwriteJobRunner runner = staticOverwriteRunner(
                    406L, db.getId(), table.getId(), Mockito.mock(InsertStmt.class),
                    Mockito.mock(ConnectContext.class),
                    Lists.newArrayList(temporaryPartitionId, /*already dropped*/ 987654321L),
                    Lists.newArrayList("p1", "p2"));

            try (MockedStatic<InsertPreSplitHook> hook = Mockito.mockStatic(InsertPreSplitHook.class)) {
                runner.preSplitStaticOverwriteTempPartitions();

                hook.verifyNoInteractions();
            }
        });
    }

    @Test
    public void testStaticOverwritePreSplitSwallowsFailureAndReleasesTheLock() {
        // The target table can be dropped between createTempPartitions() and this hook, making
        // checkAndGetTable throw from inside the table READ lock. By this point the temporary
        // partitions are already cloned and the overwrite is committed to running, so the throw
        // must not surface as a failed INSERT OVERWRITE -- and the lock must still be released.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        InsertOverwriteJobRunner runner = staticOverwriteRunner(
                407L, db.getId(), /*tableId that no longer exists*/ 987654321L,
                Mockito.mock(InsertStmt.class), Mockito.mock(ConnectContext.class),
                Lists.newArrayList(2001L), Lists.newArrayList("p1"));

        try (MockedStatic<InsertPreSplitHook> hook = Mockito.mockStatic(InsertPreSplitHook.class)) {
            Assertions.assertDoesNotThrow(runner::preSplitStaticOverwriteTempPartitions);

            hook.verifyNoInteractions();
        }

        // A leaked READ lock would deadlock the next writer; taking the same lock again proves the
        // try-with-resources released it on the throwing path.
        Assertions.assertDoesNotThrow(runner::preSplitStaticOverwriteTempPartitions);
    }

    @Test
    public void testDropUnusedDynamicOverwriteTempPartitions() {
        // Sampling and the load use different source snapshots, so a pre-created temporary
        // partition can end up with no rows and never be promoted. Only this transaction's
        // leftovers may be dropped -- another concurrent overwrite owns the rest.
        InsertOverwriteJob job = new InsertOverwriteJob(
                301L, Mockito.mock(InsertStmt.class), 11L, 12L, WarehouseManager.DEFAULT_WAREHOUSE_ID, true);
        job.setTxnId(42L);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(
                job, Mockito.mock(ConnectContext.class), Mockito.mock(StmtExecutor.class));

        // Build the partition mocks before opening the outer when(), otherwise Mockito sees a
        // nested stubbing and fails with UnfinishedStubbing.
        List<Partition> tempPartitions = Lists.newArrayList(
                mockPartitionNamed("txn42_p20260101"),
                mockPartitionNamed("txn7_p20260101"),
                mockPartitionNamed("p20260101"));
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getTempPartitions()).thenReturn(tempPartitions);

        runner.dropUnusedDynamicOverwriteTempPartitions(table);

        Mockito.verify(table).dropTempPartition("txn42_p20260101", true);
        Mockito.verify(table, Mockito.never()).dropTempPartition("txn7_p20260101", true);
        Mockito.verify(table, Mockito.never()).dropTempPartition("p20260101", true);
    }

    @Test
    public void testDropUnusedDynamicOverwriteTempPartitionsSkipsNonDynamicJob() {
        InsertOverwriteJob job = new InsertOverwriteJob(
                302L, Mockito.mock(InsertStmt.class), 11L, 12L, WarehouseManager.DEFAULT_WAREHOUSE_ID, false);
        job.setTxnId(42L);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(
                job, Mockito.mock(ConnectContext.class), Mockito.mock(StmtExecutor.class));

        OlapTable table = Mockito.mock(OlapTable.class);

        runner.dropUnusedDynamicOverwriteTempPartitions(table);

        Mockito.verifyNoInteractions(table);
    }

    @Test
    public void testGetDynamicOverwriteTempPartitionsFallsBackToPrefixScan() {
        // The transaction state is gone (here: never existed), which is exactly the case that used
        // to lose the pre-created partitions. The prefix scan must still find this transaction's
        // temporary partitions so GC can drop them.
        InsertOverwriteJob job = new InsertOverwriteJob(
                303L, Mockito.mock(InsertStmt.class), 11L, 12L, WarehouseManager.DEFAULT_WAREHOUSE_ID, true);
        job.setTxnId(4242L);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(
                job, Mockito.mock(ConnectContext.class), Mockito.mock(StmtExecutor.class));

        List<Partition> tempPartitions = Lists.newArrayList(
                mockPartitionNamed("txn4242_p20260101"),
                mockPartitionNamed("txn4243_p20260101"));
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getTempPartitions()).thenReturn(tempPartitions);

        Assertions.assertEquals(Lists.newArrayList("txn4242_p20260101"),
                runner.getDynamicOverwriteTempPartitions(table));
    }

    @Test
    public void testGetDynamicOverwriteTempPartitionsBeforePrepare() {
        InsertOverwriteJob job = new InsertOverwriteJob(
                304L, Mockito.mock(InsertStmt.class), 11L, 12L, WarehouseManager.DEFAULT_WAREHOUSE_ID, true);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(
                job, Mockito.mock(ConnectContext.class), Mockito.mock(StmtExecutor.class));

        OlapTable table = Mockito.mock(OlapTable.class);

        Assertions.assertTrue(runner.getDynamicOverwriteTempPartitions(table).isEmpty());
        Mockito.verifyNoInteractions(table);
    }

    private static Partition mockPartitionNamed(String name) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getName()).thenReturn(name);
        return partition;
    }

    @Test
    public void testInsertOverwriteAbortsWhenTableStateNotNormal() throws Exception {
        // Guards against the race where an overwrite passes analysis while the table is NORMAL,
        // then the table state flips (e.g. an ALTER submits) before the job's prepare() runs.
        String sql = "insert overwrite t1 select * from t2";
        InsertStmt insertStmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, insertStmt);
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t1");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(301L, insertStmt, database.getId(), olapTable.getId(),
                WarehouseManager.DEFAULT_WAREHOUSE_ID, false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob, connectContext, executor);

        olapTable.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            Assertions.assertThrows(DmlException.class, runner::run);
        } finally {
            olapTable.setState(OlapTable.OlapTableState.NORMAL);
        }
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob.getJobState());
        Assertions.assertTrue(olapTable.getTempPartitions().isEmpty());
    }

    @Test
    public void testDoCommitAbortsWhenTableStateNotNormal() {
        // Guards the same race as testInsertOverwriteAbortsWhenTableStateNotNormal, but at doCommit()
        // itself: the table can flip out of NORMAL after prepare()'s own check passed (e.g. an ALTER
        // submits while the load is running), so doCommit() re-checks right before the partition swap.
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t1");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(304L, database.getId(), olapTable.getId(),
                Lists.newArrayList(olapTable.getPartition("t1").getId()), false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);

        olapTable.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        try {
            // doCommit() wraps every exception from its try block into DmlException("replace partitions
            // failed", cause) (see its catch (Exception e) below the state check) -- the original message
            // survives only as the cause.
            DmlException ex = Assertions.assertThrows(DmlException.class, runner::testDoCommit);
            Assertions.assertNotNull(ex.getCause());
            Assertions.assertTrue(ex.getCause().getMessage().contains("table state is"));
        } finally {
            olapTable.setState(OlapTable.OlapTableState.NORMAL);
        }
    }

    @Test
    public void testInsertOverwriteWithDuplicatePartitions() throws SQLException {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String sql = "insert overwrite t3 partitions(p1, p1) select * from t4";
        cluster.runSql("insert_overwrite_test", sql);
    }

    @Test
    public void testInsertOverwriteConcurrencyWithSamePartitions() throws Exception {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t1");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(100L, database.getId(), olapTable.getId(),
                Lists.newArrayList(olapTable.getPartition("t1").getId()), false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);

        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String sql = "insert overwrite t1 partitions(t1) select * from t2";
        cluster.runSql("insert_overwrite_test", sql);

        Assertions.assertThrows(DmlException.class, () -> runner.testDoCommit());
        insertOverwriteJob.setSourcePartitionNames(Lists.newArrayList("t1"));
        Assertions.assertThrows(DmlException.class, () -> runner.testDoCommit());
    }

    @Test
    public void testEnsureTempPartitionsVisibleThrowsWhenPartitionMissing() {
        InsertOverwriteJob job = new InsertOverwriteJob(1L, 2L, 3L, Lists.newArrayList(4L), false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job) {
            @Override
            protected boolean hasCommittedNotVisible(long partitionId) {
                return false;
            }
        };
        OlapTable table = Mockito.mock(OlapTable.class);
        Assertions.assertThrows(DmlException.class,
                () -> runner.ensureTempPartitionsVisible(table, Lists.newArrayList(10L)));
    }

    @Test
    public void testEnsureTempPartitionsVisibleThrowsWhenNotVisible() {
        InsertOverwriteJob job = new InsertOverwriteJob(1L, 2L, 3L, Lists.newArrayList(4L), false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job) {
            @Override
            protected boolean hasCommittedNotVisible(long partitionId) {
                return partitionId == 10L;
            }
        };
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getName()).thenReturn("tmp_part");
        Mockito.when(table.getPartition(10L)).thenReturn(partition);
        Assertions.assertThrows(DmlException.class,
                () -> runner.ensureTempPartitionsVisible(table, Lists.newArrayList(10L)));
    }

    @Test
    public void testEnsureTempPartitionsVisiblePassesWhenVisible() {
        InsertOverwriteJob job = new InsertOverwriteJob(1L, 2L, 3L, Lists.newArrayList(4L), false);
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(job) {
            @Override
            protected boolean hasCommittedNotVisible(long partitionId) {
                return false;
            }
        };
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(table.getPartition(10L)).thenReturn(partition);
        Assertions.assertDoesNotThrow(
                () -> runner.ensureTempPartitionsVisible(table, Lists.newArrayList(10L)));
    }

    @Test
    public void testDynamicOverwriteGcAfterFeRestart() {
        // Test that dynamic overwrite can clean up temp partitions after FE restart
        // (when insertStmt is null because it's a transient field)
        // txnId is set in prepare() phase and persisted in log, so after FE restart
        // we can identify temp partitions by prefix "txn{txnId}_"
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t3");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;

        // Create a dynamic overwrite job with empty sourcePartitionIds (simulating dynamic overwrite)
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(200L, database.getId(), olapTable.getId(),
                Lists.newArrayList(), true);
        Assertions.assertTrue(insertOverwriteJob.isDynamicOverwrite());

        // Set txnId to simulate txnId was set in prepare() and restored from log after FE restart
        insertOverwriteJob.setTxnId(12345L);

        // Simulate FE restart scenario: insertStmt is null (transient field)
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);

        // After fix: txnId is set in prepare() phase, so we can identify temp partitions
        // with prefix "txn{txnId}_"
        // Since there are no temp partitions, it should complete without error
        runner.cancel();
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob.getJobState());
    }

    @Test
    public void testDynamicOverwriteReplayStateChange() {
        // Test that replaying state change for dynamic overwrite works correctly
        // txnId should be restored from log
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t3");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;

        // Create a dynamic overwrite job
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(201L, database.getId(), olapTable.getId(),
                Lists.newArrayList(), true);
        Assertions.assertTrue(insertOverwriteJob.isDynamicOverwrite());

        // Create state change info for transition to RUNNING state with txnId
        // (txnId is set in prepare() phase for dynamic overwrite)
        InsertOverwriteStateChangeInfo stateChangeInfo = new InsertOverwriteStateChangeInfo(201L,
                InsertOverwriteJobState.OVERWRITE_PENDING, InsertOverwriteJobState.OVERWRITE_RUNNING,
                Lists.newArrayList(), null, Lists.newArrayList(), 12345L);

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);
        runner.replayStateChange(stateChangeInfo);

        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, insertOverwriteJob.getJobState());
        Assertions.assertEquals(12345L, insertOverwriteJob.getTxnId());
    }

    @Test
    public void testDynamicOverwriteReplayFailedStateChange() {
        // Test that replaying OVERWRITE_FAILED state for dynamic overwrite works correctly
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t3");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;

        // Create a dynamic overwrite job
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(202L, database.getId(), olapTable.getId(),
                Lists.newArrayList(), true);
        Assertions.assertTrue(insertOverwriteJob.isDynamicOverwrite());

        // Create state change info for transition to FAILED state with txnId
        InsertOverwriteStateChangeInfo stateChangeInfo = new InsertOverwriteStateChangeInfo(202L,
                InsertOverwriteJobState.OVERWRITE_PENDING, InsertOverwriteJobState.OVERWRITE_FAILED,
                Lists.newArrayList(), null, Lists.newArrayList(), 12345L);

        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);
        runner.replayStateChange(stateChangeInfo);

        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob.getJobState());
        Assertions.assertEquals(12345L, insertOverwriteJob.getTxnId());
    }

    @Test
    public void testDynamicOverwriteCancelBeforePrepare() {
        // Test that cancelling a dynamic overwrite job before prepare() completes
        // (txnId not set) handles gracefully without assertion error
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("insert_overwrite_test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t3");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;

        // Create a dynamic overwrite job without txnId (simulating job cancelled before prepare())
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(203L, database.getId(), olapTable.getId(),
                Lists.newArrayList(), true);
        Assertions.assertTrue(insertOverwriteJob.isDynamicOverwrite());
        Assertions.assertEquals(-1, insertOverwriteJob.getTxnId());

        // Simulate FE restart scenario where job was in PENDING state
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);

        // Should complete without assertion error, even though txnId is not set
        // No temp partitions to clean up since prepare() never completed
        runner.cancel();
        Assertions.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob.getJobState());
    }

    @Test
    public void testInsertOverwriteWithUnionAllAndLambda() throws Exception {
        // Integration regression for issue #72831: INSERT OVERWRITE + UNION ALL + lambda used to
        // surface "expr_type does not match slot_type" because the second plan was reading
        // ColumnRefOperators allocated by the first plan's ColumnRefFactory. The lambda-arg cache
        // now lives on ColumnRefFactory, so re-plan automatically starts with a fresh cache.
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String sql = "insert overwrite t_lambda_target " +
                "select k1, array_map(x -> x + 1, k2) from t_lambda_src1 " +
                "union all " +
                "select k1, array_map(x -> x + 2, k2) from t_lambda_src2";
        cluster.runSql("insert_overwrite_test", sql);
    }
}
