// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.OlapTable;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.ReplicatedObjectMeta;
import com.starrocks.epack.failover.ReplicatedObjectMeta.TableMeta;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CreateReplicatedTableJobTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public void execute(FailoverGroupJob job) {
                job.execute();
            }
        };
    }

    @Test
    public void testCreateUnPartitionedTable() throws Exception {
        String sql = "create table testCreateUnPartitionedTable (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateUnPartitionedTableGroup " +
                        "INCLUDE_TABLES = test.testCreateUnPartitionedTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        DropReplicatedTableJob dropJob = new DropReplicatedTableJob(failoverGroup, null, null,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), true, true);
        dropJob.execute();

        CreateReplicatedTableJob createJob = new CreateReplicatedTableJob(failoverGroup,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), tableMeta.getDatabase(), true);
        createJob.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testCreateListPartitionedTable() throws Exception {
        String sql = "create table testCreateListPartitionedTable (key1 int not null, key2 varchar(10))\n" +
                "partition by list(key1)\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateListPartitionedTableGroup " +
                        "INCLUDE_TABLES = test.testCreateListPartitionedTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        DropReplicatedTableJob dropJob = new DropReplicatedTableJob(failoverGroup, null, null,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), true, true);
        dropJob.execute();

        CreateReplicatedTableJob createJob = new CreateReplicatedTableJob(failoverGroup,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), tableMeta.getDatabase(), true);
        createJob.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testCreateRangePartitionedTable() throws Exception {
        String sql = "create table testCreateRangePartitionedTable (key1 int not null, key2 varchar(10))\n" +
                "partition by range(key1)(\n" +
                "START (\"1\") END (\"5\") EVERY (1))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateRangePartitionedTableGroup " +
                        "INCLUDE_TABLES = test.testCreateRangePartitionedTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        DropReplicatedTableJob dropJob = new DropReplicatedTableJob(failoverGroup, null, null,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), true, true);
        dropJob.execute();

        CreateReplicatedTableJob createJob = new CreateReplicatedTableJob(failoverGroup,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), tableMeta.getDatabase(), true);
        createJob.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testCreateDateExprPartitionedTable() throws Exception {
        String sql = "create table testCreateDateExprPartitionedTable (key1 date not null, key2 varchar(10))\n" +
                "partition by date_trunc('day', key1)\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateDateExprPartitionedTableGroup " +
                        "INCLUDE_TABLES = test.testCreateDateExprPartitionedTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        DropReplicatedTableJob dropJob = new DropReplicatedTableJob(failoverGroup, null, null,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), true, true);
        dropJob.execute();

        CreateReplicatedTableJob createJob = new CreateReplicatedTableJob(failoverGroup,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), tableMeta.getDatabase(), true);
        createJob.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testCreateColumnExprPartitionedTable() throws Exception {
        String sql = "create table testCreateColumnExprPartitionedTable (key1 int not null, key2 varchar(10))\n" +
                "partition by (key1)\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateColumnExprPartitionedTableGroup " +
                        "INCLUDE_TABLES = test.testCreateColumnExprPartitionedTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        DropReplicatedTableJob dropJob = new DropReplicatedTableJob(failoverGroup, null, null,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), true, true);
        dropJob.execute();

        CreateReplicatedTableJob createJob = new CreateReplicatedTableJob(failoverGroup,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), tableMeta.getDatabase(), true);
        createJob.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }
}