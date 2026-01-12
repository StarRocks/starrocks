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
import org.mockito.Mockito;

import java.sql.SQLException;

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
                        + " DISTRIBUTED BY HASH(`c2`) BUCKETS 2 PROPERTIES('replication_num'='1');");
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

        Assertions.assertThrows(DmlException.class, () -> runner.testDoCommit(false));
        insertOverwriteJob.setSourcePartitionNames(Lists.newArrayList("t1"));
        Assertions.assertThrows(DmlException.class, () -> runner.testDoCommit(false));
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
}
