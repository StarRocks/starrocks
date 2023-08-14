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

package com.starrocks.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.scheduler.TaskRun.PARTITION_END;
import static com.starrocks.scheduler.TaskRun.PARTITION_START;

public class PartitionBasedMvRefreshProcessorTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);

        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        starRocksAssert.withDatabase("test");
        starRocksAssert.useDatabase("test");

        starRocksAssert.withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
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
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl4\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl5\n" +
                        "(\n" +
                        "    dt date,\n" +
                        "    k1 datetime,\n" +
                        "    k2 int,\n" +
                        "    k3 bigint\n" +
                        ")\n" +
                        "PARTITION BY RANGE(dt)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl6\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view test.union_all_mv\n" +
                        "partition by dt \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select dt, -1 as k2 from tbl5 where k2 is null union all select dt, k2 from tbl5;")
                .withMaterializedView("create materialized view test.mv1\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;")
                .withMaterializedView("create materialized view test.mv2\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl4.k1, tbl4.k2 from tbl4;")
                .withMaterializedView("create materialized view test.mv_inactive\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;")
                .withMaterializedView("create materialized view test.mv_without_partition\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1')\n" +
                        "as select k2, sum(v1) as total_sum from tbl3 group by k2;")
                .withTable("CREATE TABLE test.base\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view test.mv_with_test_refresh\n" +
                        "partition by k1\n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "as select k1, k2, sum(v1) as total_sum from base group by k1, k2;")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`hive_parttbl_mv`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`hive_mul_parttbl_mv1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`, sum(l_extendedprice) as total_price FROM " +
                        "`hive0`.`partitioned_db`.`lineitem_mul_par` as a group by `l_orderkey`, `l_suppkey`, `l_shipdate`;")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`hive_mul_parttbl_mv2`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`par_date`)\n" +
                        "DISTRIBUTED BY HASH(`par_col`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT c1, c2, par_date, par_col FROM `hive0`.`partitioned_db`.`t1_par`;")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`hive_join_mv`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`par_date`)\n" +
                        "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT t1.c1, t1.c2, t1_par.par_col, t1_par.par_date FROM `hive0`.`partitioned_db`.`t1` join " +
                        "`hive0`.`partitioned_db`.`t1_par` using (par_col)");

        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
                    OlapTable tbl = ((OlapTable) testDb.getTable(tableName.getTbl()));
                    if (tbl != null) {
                        for (Partition partition : tbl.getPartitions()) {
                            if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                                setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                            }
                        }
                    }
                }
            }
        };
    }

    protected void assertPlanContains(ExecPlan execPlan, String... explain) throws Exception {
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);

        for (String expected : explain) {
            Assert.assertTrue("expected is: " + expected + " but plan is \n" + explainString,
                    StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected));
        }
    }

    @Test
    public void testUnionAllMvWithPartition() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("union_all_mv"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        try {
            // base table partition insert data
            String insertSql = "insert into tbl5 partition(p4) values('2022-04-01', '2021-04-01 00:02:11', 3, 10);";
            new StmtExecutor(connectContext, insertSql).execute();

            taskRun.executeTaskRun();
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            System.out.println(plan);
            Assert.assertFalse(plan.contains("partitions=5/5"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testWithPartition() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv1"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        try {
            // first sync partition
            taskRun.executeTaskRun();
            Collection<Partition> partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // add tbl1 partition p5
            String addPartitionSql = "ALTER TABLE test.tbl1 ADD\n" +
                    "PARTITION p5 VALUES [('2022-05-01'),('2022-06-01'))";
            new StmtExecutor(connectContext, addPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(6, partitions.size());
            // drop tbl2 partition p5
            String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p5\n";
            new StmtExecutor(connectContext, dropPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // add tbl2 partition p3
            addPartitionSql = "ALTER TABLE test.tbl2 ADD PARTITION p3 values less than('2022-04-01')";
            new StmtExecutor(connectContext, addPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());
            // drop tbl2 partition p3
            dropPartitionSql = "ALTER TABLE test.tbl2 DROP PARTITION p3";
            new StmtExecutor(connectContext, dropPartitionSql).execute();
            taskRun.executeTaskRun();
            partitions = materializedView.getPartitions();
            Assert.assertEquals(5, partitions.size());

            // base table partition insert data
            testBaseTablePartitionInsertData(testDb, materializedView, taskRun);

            // base table partition replace
            testBaseTablePartitionReplace(testDb, materializedView, taskRun);

            // base table add partition
            testBaseTableAddPartitionWhileSync(testDb, materializedView, taskRun);
            testBaseTableAddPartitionWhileRefresh(testDb, materializedView, taskRun);

            // base table drop partition
            testBaseTableDropPartitionWhileSync(testDb, materializedView, taskRun);
            testBaseTableDropPartitionWhileRefresh(testDb, materializedView, taskRun);

            // base table partition rename
            testBaseTablePartitionRename(taskRun);

            testRefreshWithFailure(testDb, materializedView, taskRun);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testInactive() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
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

    @Test
    public void testMvWithoutPartition() {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_without_partition"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        try {
            taskRun.executeTaskRun();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("refresh failed");
        }
    }

    @Test
    public void testRangePartitionRefresh() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv2"));
        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(PARTITION_START, "2022-01-03");
        taskRunProperties.put(TaskRun.PARTITION_END, "2022-02-05");
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(false));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        String insertSql = "insert into tbl4 partition(p1) values('2022-01-02',2,10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun = TaskRunBuilder.newBuilder(task).properties(taskRunProperties).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        Assert.assertEquals(1, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202204_202205").getVisibleVersion());

        taskRunProperties.put(PARTITION_START, "2021-12-03");
        taskRunProperties.put(TaskRun.PARTITION_END, "2022-04-05");
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(false));
        taskRun = TaskRunBuilder.newBuilder(task).properties(taskRunProperties).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        Assert.assertEquals(1, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202204_202205").getVisibleVersion());

        taskRunProperties.put(PARTITION_START, "2021-12-03");
        taskRunProperties.put(TaskRun.PARTITION_END, "2022-03-01");
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(false));
        insertSql = "insert into tbl4 partition(p3) values('2022-03-02',21,102);";
        new StmtExecutor(connectContext, insertSql).execute();
        insertSql = "insert into tbl4 partition(p0) values('2021-12-02',81,182);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun = TaskRunBuilder.newBuilder(task).properties(taskRunProperties).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        Assert.assertEquals(2, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p202204_202205").getVisibleVersion());

        taskRunProperties.put(PARTITION_START, "2021-12-03");
        taskRunProperties.put(TaskRun.PARTITION_END, "2022-05-06");
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
        taskRun = TaskRunBuilder.newBuilder(task).properties(taskRunProperties).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        Assert.assertEquals(3, materializedView.getPartition("p202112_202201").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p202201_202202").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202202_202203").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202203_202204").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p202204_202205").getVisibleVersion());
    }

    @Test
    public void testRefreshPriority() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
                    OlapTable tbl = ((OlapTable) testDb.getTable(tableName.getTbl()));
                    for (Partition partition : tbl.getPartitions()) {
                        if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                            setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                        }
                    }
                }
            }
        };
        String mvName = "mv_refresh_priority";
        starRocksAssert.withMaterializedView("create materialized view test.mv_refresh_priority\n" +
                "partition by date_trunc('month',k1) \n" +
                "distributed by hash(k2) buckets 10\n" +
                "refresh deferred manual\n" +
                "properties('replication_num' = '1', 'partition_refresh_number'='1')\n" +
                "as select k1, k2 from tbl6;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRunManager trm = tm.getTaskRunManager();

        String insertSql = "insert into tbl6 partition(p1) values('2022-01-02',2,10);";
        new StmtExecutor(connectContext, insertSql).execute();
        insertSql = "insert into tbl6 partition(p2) values('2022-02-02',2,10);";
        new StmtExecutor(connectContext, insertSql).execute();

        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        long taskId = tm.getTask(TaskBuilder.getMvTaskName(materializedView.getId())).getId();
        TaskRun run = tm.getTaskRunManager().getRunnableTaskRun(taskId);
        Assert.assertEquals(Constants.TaskRunPriority.HIGHEST.value(), run.getStatus().getPriority());

        while (MapUtils.isNotEmpty(trm.getRunningTaskRunMap())) {
            Thread.sleep(100);
        }
    }

    @Test
    public void testAutoRefreshPartitionLimitWithHiveTable() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_parttbl_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_parttbl_mv1"));
        materializedView.getTableProperty().setAutoRefreshPartitionsLimit(2);

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        task.setType(Constants.TaskType.PERIODICAL);
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(1, materializedView.getPartition("p19980101").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980102").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980103").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getVisibleVersion());

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));
        taskRun.executeTaskRun();
        Assert.assertEquals(1, materializedView.getPartition("p19980101").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980102").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980103").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getVisibleVersion());

        task.setType(Constants.TaskType.MANUAL);
        taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980102").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980103").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    @Test
    public void testRefreshWithHiveTableJoin() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_join_mv"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        Assert.assertTrue(plan.contains("4:HASH JOIN"));
    }


    @Test
    public void testAutoPartitionRefreshWithPartitionChanged() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `test`.`hive_parttbl_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.addPartition("partitioned_db", "lineitem_par", "l_shipdate=1998-01-06");

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();

        assertPlanContains(execPlan, "partitions=1/7");
        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(7, partitions.size());

        mockedHiveMetadata.dropPartition("partitioned_db", "lineitem_par", "l_shipdate=1998-01-06");
        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    @Test
    public void testAutoPartitionRefreshWithHiveTableJoin1() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_join_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`par_date`)\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT t1.c1, t1.c2, t1_par.par_col, t1_par.par_date FROM `hive0`.`partitioned_db`.`t1` join " +
                "`hive0`.`partitioned_db`.`t1_par` using (par_col)");

        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_join_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "t1_par",
                ImmutableList.of("par_col=0/par_date=2020-01-03"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "par_date >= '2020-01-03', 9: par_date < '2020-01-04'", "partitions=2/6");

        mockedHiveMetadata.updatePartitions("partitioned_db", "t1",
                ImmutableList.of("par_col=0"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6", "partitions=3/3");
    }

    @Test
    public void testAutoPartitionRefreshWithHiveTableJoin2() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_join_mv2`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`,`o_custkey` FROM `hive0`.`partitioned_db`.`lineitem_par` " +
                "as a join `hive0`.`tpch`.`orders` on l_orderkey = o_orderkey");

        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_join_mv2"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-04"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();

        assertPlanContains(execPlan, "l_shipdate >= '1998-01-04', 16: l_shipdate < '1998-01-05'",
                "partitions=1/6");

        mockedHiveMetadata.updateTable("tpch", "orders");

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6", "partitions=1/1");
    }

    @Test
    public void testAutoPartitionRefreshWithUnPartitionedHiveTable() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_tbl_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "DISTRIBUTED BY HASH(`n_nationkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `n_nationkey`, `n_name`, `n_comment`  FROM `hive0`.`tpch`.`nation`;");

        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_tbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=1/1");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updateTable("tpch", "nation");

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=1/1");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("hive_tbl_mv1").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_tbl_mv1");
    }

    @Test
    public void testAutoPartitionRefreshWithPartitionedHiveTable1() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_parttbl_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        System.out.println(execPlan.getExplainString(TExplainLevel.NORMAL));
        assertPlanContains(execPlan, "partitions=6/6", "PARTITION PREDICATES: (16: l_shipdate < '1998-01-06') " +
                "OR (16: l_shipdate IS NULL)");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "PARTITION PREDICATES: 16: l_shipdate >= '1998-01-02', 16: l_shipdate < '1998-01-04'",
                "partitions=2/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980102").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980103").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    @Test
    public void testAutoPartitionRefreshWithPartitionedHiveTable2() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_tbl_mv2`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_tbl_mv2"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("hive_tbl_mv2").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_tbl_mv2");
    }

    @Test
    public void testAutoPartitionRefreshWithPartitionedHiveTableJoinInternalTable() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView(
                "CREATE MATERIALIZED VIEW `hive_join_internal_mv`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a" +
                        " join test.tbl1 b on a.l_suppkey=b.k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_join_internal_mv"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("hive_join_internal_mv").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_join_internal_mv");
    }

    @Test
    public void testPartitionRefreshWithUpperCaseTable() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_parttbl_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`LINEITEM_PAR` as " +
                "`LINEITEM_PAR_ALIAS`;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "PARTITION PREDICATES: 16: l_shipdate >= '1998-01-02', 16: l_shipdate < '1998-01-04'",
                "partitions=2/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980102").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980103").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    public void testPartitionRefreshWithUpperCaseDb() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("CREATE MATERIALIZED VIEW `hive_parttbl_mv1`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`l_shipdate`)\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"HDD\"\n" +
                ")\n" +
                "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_DB`.`LINEITEM_PAR` as " +
                "`LINEITEM_PAR_ALIAS`;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "PARTITION PREDICATES: 16: l_shipdate >= '1998-01-02', 16: l_shipdate < '1998-01-04'",
                "partitions=2/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980102").getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980103").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    @Test
    public void testPartitionRefreshWithLowerCase() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView(
                "CREATE MATERIALIZED VIEW `test`.`hive_parttbl_mv1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`par_col`)\n" +
                        "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT c1, c2, par_col FROM `hive0`.`partitioned_db2`.`t2`;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=3/3");

        MockedHiveMetadata mockedHiveMetadata = (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db2", "t2",
                ImmutableList.of("par_col=0"));

        taskRun.executeTaskRun();
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "par_col >= 0, 4: par_col < 1", "partitions=1/3");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(3, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("p0").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p1").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p2").getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    @Test
    public void testRangePartitionRefreshWithHiveTable() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_parttbl_mv"));
        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(PARTITION_START, "1998-01-01");
        taskRunProperties.put(TaskRun.PARTITION_END, "1998-01-03");
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(false));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(taskRunProperties).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        Collection<Partition> partitions = materializedView.getPartitions();

        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980102").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980103").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980104").getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980105").getVisibleVersion());

        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();
        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        Assert.assertTrue(plan.contains("PARTITION PREDICATES: 16: l_shipdate >= '1998-01-01', " +
                "16: l_shipdate < '1998-01-03'"));
        Assert.assertTrue(plan.contains("partitions=2/6"));
    }

    @Test
    public void testRefreshPartitionWithMulParColumnsHiveTable1() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_mul_parttbl_mv1"));
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PARTITION_START, "1998-01-01");
        mvProperties.put(PARTITION_END, "1998-01-03");

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(mvProperties).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        Assert.assertTrue(plan.contains("PARTITION PREDICATES: 15: l_shipdate >= '1998-01-01', 15: l_shipdate < '1998-01-03'"));
        Assert.assertTrue(plan.contains("partitions=5/8"));
    }

    @Test
    public void testRefreshPartitionWithMulParColumnsHiveTable2() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("hive_mul_parttbl_mv2"));
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PARTITION_START, "2020-01-01");
        mvProperties.put(PARTITION_END, "2020-01-03");

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(mvProperties).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        Assert.assertTrue(plan.contains("PARTITION PREDICATES: 5: par_date >= '2020-01-01', 5: par_date < '2020-01-03'"));
        Assert.assertTrue(plan.contains("partitions=3/6"));
    }

    @Test
    public void testMvWithoutPartitionRefreshTwice() throws Exception {
        final AtomicInteger taskRunCounter = new AtomicInteger();
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                taskRunCounter.incrementAndGet();
            }
        };
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_without_partition"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        String insertSql = "insert into tbl3 values('2021-12-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        try {
            for (int i = 0; i < 2; i++) {
                taskRun.executeTaskRun();
            }
            Assert.assertEquals(1, taskRunCounter.get());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("refresh failed");
        }
    }

    @Test
    public void testClearQueryInfo() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_without_partition"));
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                UUID uuid = UUID.randomUUID();
                TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                System.out.println("register query id: " + DebugUtil.printId(connectContext.getExecutionId()));
                LoadPlanner loadPlanner = new LoadPlanner(1, loadId, 1, 1, materializedView,
                        false, "UTC", 10, System.currentTimeMillis(),
                        false, connectContext, null, 10,
                        10, null, null, null, 1);
                DefaultCoordinator coordinator = new DefaultCoordinator.Factory().createBrokerLoadScheduler(loadPlanner);
            }
        };
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        String insertSql = "insert into tbl3 values('2021-12-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        System.out.println("unregister query id: " + DebugUtil.printId(connectContext.getExecutionId()));
        Assert.assertNull(QeProcessorImpl.INSTANCE.getCoordinator(connectContext.getExecutionId()));
    }

    private void testBaseTablePartitionInsertData(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        String insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        insertSql = "insert into tbl1 partition(p1) values('2022-01-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();

        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo basePartitionInfo = baseTableVisibleVersionMap.get(tbl1.getId()).get("p0");
        Assert.assertEquals(2, basePartitionInfo.getVersion());
        // insert new data into tbl1's p0 partition
        // update base table tbl1's p0 version to 2
        insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();

        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap2 =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo newP0PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p0");
        Assert.assertEquals(3, newP0PartitionInfo.getVersion());

        MaterializedView.BasePartitionInfo p1PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p1");
        Assert.assertEquals(2, p1PartitionInfo.getVersion());
    }

    private void testRefreshWithFailure(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        // insert new data into tbl1's p0 partition
        // update base table tbl1's p0 version to 3
        String insertSql = "insert into tbl1 partition(p0) values('2021-12-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();

        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public void processTaskRun(TaskRunContext context) throws Exception {
                throw new RuntimeException("new exception");
            }
        };
        try {
            taskRun.executeTaskRun();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap2 =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo newP0PartitionInfo = baseTableVisibleVersionMap2.get(tbl1.getId()).get("p0");
        Assert.assertEquals(3, newP0PartitionInfo.getVersion());
    }

    public void testBaseTablePartitionRename(TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p1, p1 renamed with p10 after collect and before insert overwrite
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            private Map<Long, Pair<BaseTableInfo, Table>> collectBaseTables(MaterializedView materializedView) {
                Map<Long, Pair<BaseTableInfo, Table>> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();

                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    if (!baseTableInfo.getTable().isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), Pair.create(baseTableInfo, copied));
                }

                String renamePartitionSql = "ALTER TABLE test.tbl1 RENAME PARTITION p1 p1_1";
                try {
                    // will fail when retry in second times
                    new StmtExecutor(connectContext, renamePartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };
        // insert new data into tbl1's p1 partition
        // update base table tbl1's p1 version to 2
        String insertSql = "insert into tbl1 partition(p1) values('2022-01-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        try {
            taskRun.executeTaskRun();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is not active, skip sync partition and data with base tables"));
        }
    }

    private void testBaseTablePartitionReplace(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p2, p2 replace with tp2 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public Map<Long, Pair<BaseTableInfo, Table>> collectBaseTables(MaterializedView materializedView) {
                Map<Long, Pair<BaseTableInfo, Table>> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    if (!baseTableInfo.getTable().isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), Pair.create(baseTableInfo, olapTable));
                }

                try {
                    String replacePartitionSql = "ALTER TABLE test.tbl1 REPLACE PARTITION (p3) WITH TEMPORARY PARTITION (tp3)\n" +
                            "PROPERTIES (\n" +
                            "    \"strict_range\" = \"false\",\n" +
                            "    \"use_temp_partition_name\" = \"false\"\n" +
                            ");";
                    new StmtExecutor(connectContext, replacePartitionSql).execute();
                    String insertSql = "insert into tbl1 partition(p3) values('2021-03-01', 2, 10);";
                    new StmtExecutor(connectContext, insertSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };
        // change partition and replica versions
        Partition partition = tbl1.getPartition("p3");
        String createTempPartitionSql =
                "ALTER TABLE test.tbl1 ADD TEMPORARY PARTITION tp3 values [('2022-03-01'),('2022-04-01'))";
        new StmtExecutor(connectContext, createTempPartitionSql).execute();
        String insertSql = "insert into tbl1 partition(p3) values('2021-03-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        MaterializedView.BasePartitionInfo basePartitionInfo = baseTableVisibleVersionMap.get(tbl1.getId()).get("p3");
        Assert.assertNotEquals(partition.getId(), basePartitionInfo.getId());
    }

    public void testBaseTableAddPartitionWhileSync(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p3, add partition p99 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public Map<Long, Pair<BaseTableInfo, Table>> collectBaseTables(MaterializedView materializedView) {
                Map<Long, Pair<BaseTableInfo, Table>> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    if (!baseTableInfo.getTable().isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), Pair.create(baseTableInfo, copied));
                }

                String addPartitionSql = "ALTER TABLE test.tbl1 ADD PARTITION p99 VALUES [('9999-03-01'),('9999-04-01'))";
                try {
                    new StmtExecutor(connectContext, addPartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String insertSql = "insert into tbl1 partition(p99) values('9999-03-01', 2, 10);";
                try {
                    new StmtExecutor(connectContext, insertSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 2, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(3, baseTableVisibleVersionMap.get(tbl1.getId()).get("p3").getVersion());
        Assert.assertNotNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p99"));
        Assert.assertEquals(2, baseTableVisibleVersionMap.get(tbl1.getId()).get("p99").getVersion());
    }

    public void testBaseTableAddPartitionWhileRefresh(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p3, add partition p99 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan,
                                                InsertStmt insertStmt) throws Exception {
                String addPartitionSql = "ALTER TABLE test.tbl1 ADD PARTITION p100 VALUES [('9999-04-01'),('9999-05-01'))";
                String insertSql = "insert into tbl1 partition(p100) values('9999-04-01', 3, 10);";
                try {
                    new StmtExecutor(connectContext, addPartitionSql).execute();
                    new StmtExecutor(connectContext, insertSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ConnectContext ctx = mvContext.getCtx();
                StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
                ctx.setExecutor(executor);
                ctx.setThreadLocalInfo();
                ctx.setStmtId(new AtomicInteger().incrementAndGet());
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
                executor.handleDMLStmt(execPlan, insertStmt);
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        new StmtExecutor(connectContext, insertSql).execute();

        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertEquals(4, baseTableVisibleVersionMap.get(tbl1.getId()).get("p3").getVersion());
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p100"));
    }

    public void testBaseTableDropPartitionWhileSync(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // mv need refresh with base table partition p4, drop partition p4 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            private Map<Long, Pair<BaseTableInfo, Table>> collectBaseTables(MaterializedView materializedView) {
                Map<Long, Pair<BaseTableInfo, Table>> olapTables = Maps.newHashMap();
                List<BaseTableInfo> baseTableInfos = materializedView.getBaseTableInfos();
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    if (!baseTableInfo.getTable().isOlapTable()) {
                        continue;
                    }
                    Database baseDb = GlobalStateMgr.getCurrentState().getDb(baseTableInfo.getDbId());
                    if (baseDb == null) {
                        throw new SemanticException("Materialized view base db: " +
                                baseTableInfo.getDbId() + " not exist.");
                    }
                    OlapTable olapTable = (OlapTable) baseDb.getTable(baseTableInfo.getTableId());
                    if (olapTable == null) {
                        throw new SemanticException("Materialized view base table: " +
                                baseTableInfo.getTableId() + " not exist.");
                    }
                    OlapTable copied = new OlapTable();
                    if (!DeepCopy.copy(olapTable, copied, OlapTable.class)) {
                        throw new SemanticException("Failed to copy olap table: " + olapTable.getName());
                    }
                    olapTables.put(olapTable.getId(), Pair.create(baseTableInfo, copied));
                }

                String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p4";
                try {
                    new StmtExecutor(connectContext, dropPartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return olapTables;
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p4"));
    }

    public void testBaseTableDropPartitionWhileRefresh(Database testDb, MaterializedView materializedView, TaskRun taskRun)
            throws Exception {
        // drop partition p4 after collect and before insert overwrite
        OlapTable tbl1 = ((OlapTable) testDb.getTable("tbl1"));
        new MockUp<PartitionBasedMvRefreshProcessor>() {
            @Mock
            public void refreshMaterializedView(MvTaskRunContext mvContext, ExecPlan execPlan,
                                                InsertStmt insertStmt) throws Exception {
                String dropPartitionSql = "ALTER TABLE test.tbl1 DROP PARTITION p100";
                try {
                    new StmtExecutor(connectContext, dropPartitionSql).execute();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                ConnectContext ctx = mvContext.getCtx();
                StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
                ctx.setExecutor(executor);
                ctx.setThreadLocalInfo();
                ctx.setStmtId(new AtomicInteger().incrementAndGet());
                ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
                executor.handleDMLStmt(execPlan, insertStmt);
            }
        };

        // insert new data into tbl1's p3 partition
        String insertSql = "insert into tbl1 partition(p3) values('2022-03-01', 3, 10);";
        new StmtExecutor(connectContext, insertSql).execute();
        taskRun.executeTaskRun();
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                materializedView.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        Assert.assertNotNull(baseTableVisibleVersionMap.get(tbl1.getId()).get("p100"));
        Assert.assertEquals(3, baseTableVisibleVersionMap.get(tbl1.getId()).get("p100").getVersion());
    }

    private static void setPartitionVersion(Partition partition, long version) {
        partition.setVisibleVersion(version, System.currentTimeMillis());
        MaterializedIndex baseIndex = partition.getBaseIndex();
        List<Tablet> tablets = baseIndex.getTablets();
        for (Tablet tablet : tablets) {
            List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
            for (Replica replica : replicas) {
                replica.updateVersionInfo(version, -1, version);
            }
        }
    }

    @Test
    public void testFilterPartitionByRefreshNumber() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_with_test_refresh"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();
        materializedView.getTableProperty().setPartitionRefreshNumber(3);
        PartitionBasedMvRefreshProcessor processor = new PartitionBasedMvRefreshProcessor();

        MvTaskRunContext mvContext = new MvTaskRunContext(new TaskRunContext());

        processor.setMvContext(mvContext);
        processor.filterPartitionByRefreshNumber(materializedView.getPartitionNames(), materializedView);

        mvContext = processor.getMvContext();
        Assert.assertEquals("2022-03-01", mvContext.getNextPartitionStart());
        Assert.assertEquals("2022-05-01", mvContext.getNextPartitionEnd());

        taskRun.executeTaskRun();

        processor.filterPartitionByRefreshNumber(Sets.newHashSet(), materializedView);
        mvContext = processor.getMvContext();
        Assert.assertNull(mvContext.getNextPartitionStart());
        Assert.assertNull(mvContext.getNextPartitionEnd());
    }

    @Test
    public void testSyncPartitionWithSsdStorage() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView("create materialized view test.mv_with_ssd\n" +
                "partition by date_trunc('month',k1) \n" +
                "distributed by hash(k2) buckets 10\n" +
                "refresh deferred manual\n" +
                "properties('replication_num' = '1',\n" +
                "'storage_medium' = 'SSD')\n" +
                "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_with_ssd"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        try {
            taskRun.executeTaskRun();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("refresh failed");
        }
    }

    @Test
    public void testSyncPartitionWithSsdStorageAndCooldownTime() throws Exception {
        starRocksAssert.useDatabase("test").withMaterializedView(
                "create materialized view test.mv_use_ssd_and_cooldown\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1',\n" +
                        "'storage_medium' = 'SSD',\n" +
                        "'storage_cooldown_time' = '2222-04-21 20:45:11')\n" +
                        "as select tbl1.k1, tbl2.k2 from tbl1 join tbl2 on tbl1.k2 = tbl2.k2;");
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable("mv_use_ssd_and_cooldown"));
        // build task
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());

        try {
            taskRun.executeTaskRun();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("refresh failed");
        }
    }
}
