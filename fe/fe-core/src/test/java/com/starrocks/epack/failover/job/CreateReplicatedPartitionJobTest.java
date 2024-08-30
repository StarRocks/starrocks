// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.io.DeepCopy;
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

public class CreateReplicatedPartitionJobTest {
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
    public void testCreateListPartition() throws Exception {
        String sql = "create table testCreateListPartitionTable (key1 int not null, key2 varchar(10))\n" +
                "partition by list(key1)(\n" +
                "partition p1 values in (\"1\"))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateListPartitionTableGroup " +
                        "INCLUDE_TABLES = test.testCreateListPartitionTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();
        OlapTable sourceTable = DeepCopy.copyWithGson(tableMeta.getTable(), OlapTable.class);

        DropReplicatedPartitionJob dropJob = new DropReplicatedPartitionJob(failoverGroup, null, null,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), "p1", false, true);
        dropJob.execute();

        Assert.assertNull(tableMeta.getTable().getPartition("p1"));

        CreateReplicatedPartitionJob createJob = new CreateReplicatedPartitionJob(failoverGroup,
                        tableMeta.getDatabase(), sourceTable, sourceTable.getPartitions().iterator().next(),
                        tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), true);
        createJob.execute();

        Assert.assertNotNull(tableMeta.getTable().getPartition("p1"));
    }

    @Test
    public void testCreateRangePartition() throws Exception {
        String sql = "create table testCreateRangePartitionTable (key1 int not null, key2 varchar(10))\n" +
                "partition by range(key1)(\n" +
                "partition p1 values [(\"1\"), (\"2\")))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateRangePartitionedTableGroup " +
                        "INCLUDE_TABLES = test.testCreateRangePartitionTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();
        OlapTable sourceTable = DeepCopy.copyWithGson(tableMeta.getTable(), OlapTable.class);

        DropReplicatedPartitionJob dropJob = new DropReplicatedPartitionJob(failoverGroup, null, null,
                tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), "p1", false, true);
        dropJob.execute();

        Assert.assertNull(tableMeta.getTable().getPartition("p1"));

        CreateReplicatedPartitionJob createJob = new CreateReplicatedPartitionJob(failoverGroup,
                        tableMeta.getDatabase(), sourceTable, sourceTable.getPartitions().iterator().next(),
                        tableMeta.getDatabase(), (OlapTable) tableMeta.getTable(), true);
        createJob.execute();

        Assert.assertNotNull(tableMeta.getTable().getPartition("p1"));
    }
}