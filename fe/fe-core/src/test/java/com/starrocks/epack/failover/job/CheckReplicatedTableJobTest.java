// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
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

public class CheckReplicatedTableJobTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");

        String sql = "create table CheckReplicatedTableJobTestTable (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public void execute(Runnable command) {
                command.run();
            }
        };
    }

    @Test
    public void testCheckTableExisted() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCheckDatabaseExistedGroup " +
                        "INCLUDE_TABLES = test.CheckReplicatedTableJobTestTable " +
                        "MEMBERS = " +
                                "'az1:SELF'," +
                                "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");

        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup, tableMeta.getDatabase(),
                (OlapTable) tableMeta.getTable(), tableMeta.getDatabase(), true);
        job.execute();

        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testSkipDefaultPhysicalPartitionById() throws Exception {
        String sql = "create table testSkipDefaultPhysicalPartitionById (key1 int not null)\n" +
                "partition by range(key1)(\n" +
                "partition p1 values [(\"1\"), (\"2\")))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testSkipDefaultPhysicalPartitionByIdGroup " +
                        "INCLUDE_TABLES = test.testSkipDefaultPhysicalPartitionById " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");
        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        OlapTable localTable = (OlapTable) tableMeta.getTable();
        OlapTable remoteTable = DeepCopy.copyWithGson(localTable, OlapTable.class);

        Partition localPartition = localTable.getPartition("p1");
        int localPhysicalPartitionCount = localPartition.getSubPartitions().size();

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup, tableMeta.getDatabase(),
                remoteTable, tableMeta.getDatabase(), true);
        job.execute();

        Assert.assertEquals(localPhysicalPartitionCount, localPartition.getSubPartitions().size());
        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }

    @Test
    public void testCreatePhysicalPartitionWithSuffix() throws Exception {
        String sql = "create table testCreatePhysicalPartitionWithSuffix (key1 int not null)\n" +
                "partition by range(key1)(\n" +
                "partition p1 values [(\"1\"), (\"2\")))\n" +
                "distributed by random\n" +
                "properties('replication_num' = '1'); ";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql,
                AnalyzeTestUtil.getConnectContext());
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt));

        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreatePhysicalPartitionWithSuffixGroup " +
                        "INCLUDE_TABLES = test.testCreatePhysicalPartitionWithSuffix " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(2, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");
        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();

        OlapTable localTable = (OlapTable) tableMeta.getTable();
        OlapTable remoteTable = DeepCopy.copyWithGson(localTable, OlapTable.class);
        Partition remotePartition = remoteTable.getPartition("p1");
        long newPhysicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedIndex baseIndexCopy = DeepCopy.copyWithGson(
                remotePartition.getDefaultPhysicalPartition().getLatestBaseIndex(), MaterializedIndex.class);
        PhysicalPartition extraPartition = new PhysicalPartition(newPhysicalPartitionId,
                remotePartition.getId(), baseIndexCopy);
        extraPartition.setBucketNum(remotePartition.getDistributionInfo().getBucketNum());
        remotePartition.addSubPartition(extraPartition);
        remoteTable.addPhysicalPartition(extraPartition);

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup, tableMeta.getDatabase(),
                remoteTable, tableMeta.getDatabase(), true);
        Partition localPartition = localTable.getPartition("p1");
        int initialCount = localPartition.getSubPartitions().size();
        job.execute();

        Assert.assertEquals(initialCount + 1, localPartition.getSubPartitions().size());

        job.execute();
        Assert.assertEquals(initialCount + 1, localPartition.getSubPartitions().size());
        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }
}
