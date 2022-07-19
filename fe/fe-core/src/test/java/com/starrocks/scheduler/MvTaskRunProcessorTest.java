package com.starrocks.scheduler;

import com.starrocks.analysis.DmlStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

public class MvTaskRunProcessorTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values [('2022-02-01'),('2022-02-16')),\n" +
                        "    PARTITION p2 values [('2022-02-16'),('2022-03-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2022-02-01'),\n" +
                        "    PARTITION p2 values less than('2022-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withNewMaterializedView("create materialized view test.mv1\n" +
                        "partition by date_trunc('week',k1) \n" +
                        "distributed by hash(k2)\n" +
                        "refresh manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;")
                .withNewMaterializedView("create materialized view test.mv_inactive\n" +
                        "partition by date_trunc('week',k1) \n" +
                        "distributed by hash(k2)\n" +
                        "refresh manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;");
    }

    @Test
    public void test(){
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {}
        };

        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        try {
            // first sync partition
            taskRun.executeTaskRun();
            Collection<Partition> partitions = materializedView.getPartitions();
            Assert.assertEquals(partitions.size(), 2);
            ExpressionRangePartitionInfo partitionInfo = ((ExpressionRangePartitionInfo) materializedView.getPartitionInfo());
            Assert.assertEquals(partitionInfo.getIdToRange(false).size(), 2);
            // add partition
            String addPartitionSql = "ALTER TABLE test.tbl1 ADD\n" +
                    "PARTITION p3 VALUES [('2022-03-01'),('2022-03-16'))";
            new StmtExecutor(connectContext, addPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(partitions.size(), 3);
            partitionInfo = ((ExpressionRangePartitionInfo) materializedView.getPartitionInfo());
            Assert.assertEquals(partitionInfo.getIdToRange(false).size(), 3);
            // delete partition p3
            String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p3\n";
            new StmtExecutor(connectContext, dropPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(partitions.size(), 2);
            partitionInfo = ((ExpressionRangePartitionInfo) materializedView.getPartitionInfo());
            Assert.assertEquals(partitionInfo.getIdToRange(false).size(), 2);
            // delete partition p2
            dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p2\n";
            new StmtExecutor(connectContext, dropPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(partitions.size(), 1);
            partitionInfo = ((ExpressionRangePartitionInfo) materializedView.getPartitionInfo());
            Assert.assertEquals(partitionInfo.getIdToRange(false).size(), 1);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testInactive() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("default_cluster:test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_inactive"));
        materializedView.setActive(false);
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        try {
            taskRun.executeTaskRun();
            Assert.fail("should not be here. executeTaskRun will throw exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is not active, skip sync partition and data with base tables"));
        }
    }
}
