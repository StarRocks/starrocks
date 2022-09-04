// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class InsertOverwriteJobRunnerTest {

    private static ConnectContext connectContext;

    @Mocked
    private InsertOverwriteJobManager insertOverwriteJobManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);

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
        Database database = GlobalStateMgr.getCurrentState().getDb("insert_overwrite_test");
        Table table = database.getTable("t1");
        Assert.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(100L, database.getId(), olapTable.getId(),
                Lists.newArrayList(olapTable.getPartition("t1").getId()));
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob);
        runner.cancel();
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob.getJobState());

        InsertOverwriteJob insertOverwriteJob2 = new InsertOverwriteJob(100L, database.getId(), olapTable.getId(),
                Lists.newArrayList(olapTable.getPartition("t1").getId()));
        InsertOverwriteStateChangeInfo stateChangeInfo = new InsertOverwriteStateChangeInfo(100L,
                InsertOverwriteJobState.OVERWRITE_PENDING, InsertOverwriteJobState.OVERWRITE_RUNNING,
                Lists.newArrayList(2000L), Lists.newArrayList(2001L));
        Assert.assertEquals(100L, stateChangeInfo.getJobId());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_PENDING, stateChangeInfo.getFromState());
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_RUNNING, stateChangeInfo.getToState());
        Assert.assertEquals(Lists.newArrayList(2000L), stateChangeInfo.getSourcePartitionIds());
        Assert.assertEquals(Lists.newArrayList(2001L), stateChangeInfo.getTmpPartitionIds());

        InsertOverwriteJobRunner runner2 = new InsertOverwriteJobRunner(insertOverwriteJob2);
        runner2.replayStateChange(stateChangeInfo);
        runner2.cancel();
        Assert.assertEquals(InsertOverwriteJobState.OVERWRITE_FAILED, insertOverwriteJob2.getJobState());
    }

    @Test
    public void testInsertOverwriteFromStmtExecutor() throws Exception {
        String sql = "insert overwrite t1 select * from t2";
        InsertStmt insertStmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, insertStmt);
        executor.handleInsertOverwrite(insertStmt);
    }

    @Test
    public void testInsertOverwrite() throws Exception {
        String sql = "insert overwrite t1 select * from t2";
        InsertStmt insertStmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, insertStmt);
        Database database = GlobalStateMgr.getCurrentState().getDb("insert_overwrite_test");
        Table table = database.getTable("t1");
        Assert.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        InsertOverwriteJob insertOverwriteJob = new InsertOverwriteJob(100L, insertStmt, database.getId(), olapTable.getId());
        InsertOverwriteJobRunner runner = new InsertOverwriteJobRunner(insertOverwriteJob, connectContext, executor);
        Assert.assertFalse(runner.isFinished());
    }
}
