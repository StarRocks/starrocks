// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.jmockit.Deencapsulation;
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

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadPoolExecutor;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CheckReplicatedTableJobTest {
    private static StarRocksAssert starRocksAssert;
    private static final Deque<Runnable> STARTED_JOBS = new ConcurrentLinkedDeque<>();

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

        // A failover group job is queued rather than run inline: in production
        // CheckReplicatedTableJob#execute starts it while holding the database lock, and it runs on
        // the failover_group_job pool, which does not hold that lock. Run inline, it would inherit
        // the lock and send its create-replica RPCs under it -- a shape production does not have.
        // Nor can it run on a thread of its own and be joined: it takes the table's WRITE lock while
        // the caller still holds the database READ lock. So it runs once the caller has returned,
        // see executeAndRunTheJobsItStarts. Everything else still runs inline.
        new MockUp<ThreadPoolExecutor>() {
            @Mock
            public void execute(Runnable command) {
                if (command instanceof FailoverGroupJob) {
                    STARTED_JOBS.add(command);
                } else {
                    command.run();
                }
            }
        };
    }

    private static void executeAndRunTheJobsItStarts(CheckReplicatedTableJob job) {
        job.execute();
        Runnable started;
        while ((started = STARTED_JOBS.poll()) != null) {
            started.run();
        }
    }

    @Test
    public void testVersionEpochConsistency() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testVersionEpochConsistencyGroup " +
                        "INCLUDE_TABLES = test.CheckReplicatedTableJobTestTable " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        FailoverGroup failoverGroup = new FailoverGroup(1, stmt);
        ReplicatedObjectMeta objectMeta = failoverGroup.getIncludeMgr().toObjectMeta("test_token");
        TableMeta tableMeta = objectMeta.getTableMetas().values().iterator().next();
        OlapTable localTable = (OlapTable) tableMeta.getTable();
        Partition localPartition = localTable.getPartitions().iterator().next();

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup, tableMeta.getDatabase(),
                localTable, tableMeta.getDatabase(), true);

        // Either side at zero predates the field, so its lineage is unknown and must not be compared.
        Assert.assertTrue(isConsistent(job, localTable, localPartition, 0L, 12L));
        Assert.assertTrue(isConsistent(job, localTable, localPartition, 12L, 0L));
        Assert.assertTrue(isConsistent(job, localTable, localPartition, 0L, 0L));

        Assert.assertTrue(isConsistent(job, localTable, localPartition, 12L, 12L));
        Assert.assertFalse(isConsistent(job, localTable, localPartition, 12L, 13L));
    }

    private static boolean isConsistent(CheckReplicatedTableJob job, OlapTable localTable, Partition localPartition,
                                        long localVersionEpoch, long remoteVersionEpoch) {
        PhysicalPartition local = new PhysicalPartition(1L, localPartition.getId(), new MaterializedIndex(2L));
        PhysicalPartition remote = new PhysicalPartition(1L, localPartition.getId(), new MaterializedIndex(2L));
        local.setVersionEpoch(localVersionEpoch);
        remote.setVersionEpoch(remoteVersionEpoch);
        return Deencapsulation.invoke(job, "checkPhysicalPartitionConsistency",
                localTable, localPartition, remote, local);
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
        executeAndRunTheJobsItStarts(job);

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
        executeAndRunTheJobsItStarts(job);

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
        executeAndRunTheJobsItStarts(job);

        Assert.assertEquals(initialCount + 1, localPartition.getSubPartitions().size());

        executeAndRunTheJobsItStarts(job);
        Assert.assertEquals(initialCount + 1, localPartition.getSubPartitions().size());
        Assert.assertTrue(!failoverGroup.getJobExecutor().hasFailedJobs());
    }
}
