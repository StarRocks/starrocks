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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.scheduler.TaskRun.PARTITION_END;
import static com.starrocks.scheduler.TaskRun.PARTITION_START;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshProcessorHiveTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        Config.enable_mv_refresh_query_rewrite = false;
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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv1"));
        materializedView.getTableProperty().setAutoRefreshPartitionsLimit(2);

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        task.setType(Constants.TaskType.PERIODICAL);
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(1, materializedView.getPartition("p19980101").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980102").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980103").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getDefaultPhysicalPartition()
                .getVisibleVersion());

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));
        initAndExecuteTaskRun(taskRun);
        Assert.assertEquals(1, materializedView.getPartition("p19980101").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980102").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(1, materializedView.getPartition("p19980103").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getDefaultPhysicalPartition()
                .getVisibleVersion());

        task.setType(Constants.TaskType.MANUAL);
        taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);

        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980102").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980103").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getDefaultPhysicalPartition()
                .getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    @Test
    public void testRefreshWithHiveTableJoin() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_join_mv"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        Assert.assertTrue(plan.contains("4:HASH JOIN"));
    }

    private static void triggerRefreshMv(Database testDb, MaterializedView partitionedMaterializedView)
            throws Exception {
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
    }

    @Test
    public void testAutoPartitionRefreshWithPartitionChanged() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`hive_parttbl_mv1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.addPartition("partitioned_db", "lineitem_par", "l_shipdate=1998-01-06");

        initAndExecuteTaskRun(taskRun);
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

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_join_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "t1_par",
                ImmutableList.of("par_col=0/par_date=2020-01-03"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "par_date >= '2020-01-03', 9: par_date < '2020-01-04'", "partitions=2/6");

        mockedHiveMetadata.updatePartitions("partitioned_db", "t1",
                ImmutableList.of("par_col=0"));

        initAndExecuteTaskRun(taskRun);
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

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_join_mv2"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-04"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();

        assertPlanContains(execPlan, "l_shipdate >= '1998-01-04', 16: l_shipdate < '1998-01-05'",
                "partitions=1/6");

        mockedHiveMetadata.updateTable("tpch", "orders");

        initAndExecuteTaskRun(taskRun);
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

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_tbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=1/1");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updateTable("tpch", "nation");

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=1/1");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("hive_tbl_mv1").getDefaultPhysicalPartition()
                .getVisibleVersion());

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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6", "PARTITION PREDICATES: ((16: l_shipdate < '0000-01-02') " +
                "OR ((16: l_shipdate >= '1998-01-01') AND (16: l_shipdate < '1998-01-06'))) OR (16: l_shipdate IS NULL)");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan,
                "PARTITION PREDICATES: 16: l_shipdate >= '1998-01-02', 16: l_shipdate < '1998-01-04'",
                "partitions=2/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980102").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980103").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getDefaultPhysicalPartition()
                .getVisibleVersion());

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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_tbl_mv2"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("hive_tbl_mv2").getDefaultPhysicalPartition()
                .getVisibleVersion());

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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_join_internal_mv"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("hive_join_internal_mv").getDefaultPhysicalPartition()
                .getVisibleVersion());

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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan,
                "PARTITION PREDICATES: 16: l_shipdate >= '1998-01-02', 16: l_shipdate < '1998-01-04'",
                "partitions=2/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980102").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980103").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getDefaultPhysicalPartition()
                .getVisibleVersion());

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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=6/6");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db", "lineitem_par",
                ImmutableList.of("l_shipdate=1998-01-02", "l_shipdate=1998-01-03"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan,
                "PARTITION PREDICATES: 16: l_shipdate >= '1998-01-02', 16: l_shipdate < '1998-01-04'",
                "partitions=2/6");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(6, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980102").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(3, materializedView.getPartition("p19980103").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980104").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980105").getDefaultPhysicalPartition()
                .getVisibleVersion());

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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "partitions=3/3");

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        mockedHiveMetadata.updatePartitions("partitioned_db2", "t2",
                ImmutableList.of("par_col=0"));

        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "par_col >= 0, 4: par_col < 1", "partitions=1/3");

        Collection<Partition> partitions = materializedView.getPartitions();
        Assert.assertEquals(3, partitions.size());
        Assert.assertEquals(3, materializedView.getPartition("p0").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p1").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p2").getDefaultPhysicalPartition()
                .getVisibleVersion());

        starRocksAssert.useDatabase("test").dropMaterializedView("hive_parttbl_mv1");
    }

    @Test
    public void testRangePartitionRefreshWithHiveTable() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv"));
        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(PARTITION_START, "1998-01-01");
        taskRunProperties.put(TaskRun.PARTITION_END, "1998-01-03");
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(false));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(taskRunProperties).build();
        initAndExecuteTaskRun(taskRun);
        Collection<Partition> partitions = materializedView.getPartitions();

        Assert.assertEquals(2, partitions.size());
        Assert.assertEquals(2, materializedView.getPartition("p19980101").getDefaultPhysicalPartition()
                .getVisibleVersion());
        Assert.assertEquals(2, materializedView.getPartition("p19980102").getDefaultPhysicalPartition()
                .getVisibleVersion());

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
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_mul_parttbl_mv1"));
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PARTITION_START, "1998-01-01");
        mvProperties.put(PARTITION_END, "1998-01-03");

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(mvProperties).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        Assert.assertTrue(
                plan.contains("PARTITION PREDICATES: 15: l_shipdate >= '1998-01-01', 15: l_shipdate < '1998-01-03'"));
        Assert.assertTrue(plan.contains("partitions=5/8"));
    }

    @Test
    public void testRefreshPartitionWithMulParColumnsHiveTable2() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_mul_parttbl_mv2"));
        Map<String, String> mvProperties = Maps.newHashMap();
        mvProperties.put(PARTITION_START, "2020-01-01");
        mvProperties.put(PARTITION_END, "2020-01-03");

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).properties(mvProperties).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
        PlanTestBase.assertContains(plan, "PARTITION PREDICATES: 5: par_date >= '2020-01-01', " +
                "5: par_date < '2020-01-03'");
        PlanTestBase.assertContains(plan, "partitions=3/7");
    }

    @Test
    public void testHivePartitionPruneNonRefBaseTable1() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW `test`.`hive_partition_prune_non_ref_tables2`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`par_date`)\n" +
                        "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT part_tbl1.c1, part_tbl2.c2, part_tbl1.par_date FROM " +
                        "`hive0`.`partitioned_db`.`part_tbl1` join " +
                        "`hive0`.`partitioned_db`.`part_tbl2` using (par_date)");

        MaterializedView materializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "hive_partition_prune_non_ref_tables2"));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        // run 1
        {
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "TABLE: part_tbl1\n" +
                    "     PARTITION PREDICATES: 4: par_date IS NOT NULL, 4: par_date >= '2020-01-01', " +
                    "4: par_date < '2020-01-05'\n" +
                    "     partitions=4/4");
            PlanTestBase.assertContains(plan, "TABLE: part_tbl2\n" +
                    "     PARTITION PREDICATES: 8: par_date IS NOT NULL, 8: par_date >= '2020-01-01', 8: par_date < " +
                    "'2020-01-05' \n" +
                    "     partitions=4/4");
        }

        // run 2
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.addPartition("partitioned_db", "part_tbl1", "par_date=2020-01-05");

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "  OLAP TABLE SINK\n" +
                    "    TABLE: hive_partition_prune_non_ref_tables2\n" +
                    "    TUPLE ID: 1\n" +
                    "    RANDOM\n" +
                    "\n" +
                    "  0:EMPTYSET");
        }

        // run 3
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.addPartition("partitioned_db", "part_tbl2", "par_date=2020-01-05");

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: part_tbl1\n" +
                    "     PARTITION PREDICATES: 4: par_date IS NOT NULL, 4: par_date >= '2020-01-05', 4: par_date < " +
                    "'2020-01-06'\n" +
                    "     partitions=1/5");
            PlanTestBase.assertContains(plan, "     TABLE: part_tbl2\n" +
                    "     PARTITION PREDICATES: 8: par_date IS NOT NULL, 8: par_date >= '2020-01-05', 8: par_date < " +
                    "'2020-01-06'\n" +
                    "     partitions=1/5");
        }

        // run 4
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.dropPartition("partitioned_db", "part_tbl1", "par_date=2020-01-05");

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertNull(execPlan);
        }

        // run 5
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.dropPartition("partitioned_db", "part_tbl2", "par_date=2020-01-05");

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertNull(execPlan);
        }

        starRocksAssert.dropMaterializedView("hive_partition_prune_non_ref_tables2");
    }

    @Test
    public void testHivePartitionPruneNonRefBaseTable2() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW `test`.`hive_partition_prune_non_ref_tables1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`par_date`)\n" +
                        "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT t2_par.c1, t2_par.c2, t1_par.par_col, t1_par.par_date " +
                        "FROM `hive0`.`partitioned_db`.`t2_par` join " +
                        "`hive0`.`partitioned_db`.`t1_par` using (par_col)");

        MaterializedView materializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "hive_partition_prune_non_ref_tables1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        // run 1
        {
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "TABLE: t1_par\n" +
                    "     PARTITION PREDICATES: 9: par_col IS NOT NULL, 10: par_date >= '2020-01-01', " +
                    "10: par_date < '2020-01-05'\n" +
                    "     partitions=6/6");
            PlanTestBase.assertContains(plan, "TABLE: t2_par\n" +
                    "     PARTITION PREDICATES: 4: par_col IS NOT NULL\n" +
                    "     partitions=6/6");
        }

        // run 2
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.addPartition("partitioned_db", "t1_par", "par_col=4/par_date=2020-01-05");

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "TABLE: t1_par\n" +
                    "     PARTITION PREDICATES: 9: par_col IS NOT NULL, 10: par_date >= '2020-01-05', " +
                    "10: par_date < '2020-01-06'\n" +
                    "     partitions=1/7");
            // TODO: multi-column partitions cannot prune partitions.
            PlanTestBase.assertContains(plan, "TABLE: t2_par\n" +
                    "     PARTITION PREDICATES: 4: par_col IS NOT NULL\n" +
                    "     partitions=6/6");
        }

        // run 3
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.addPartition("partitioned_db", "t2_par", "par_col=4/par_date=2020-01-05");

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            // TODO: non-ref base table's update will refresh all the materialized views' partitions.
            PlanTestBase.assertContains(plan, "TABLE: t1_par\n" +
                    "     PARTITION PREDICATES: 9: par_col IS NOT NULL, 10: par_date >= '2020-01-01', 10: par_date < " +
                    "'2020-01-06'\n" +
                    "     partitions=7/7");
            // TODO: multi-column partitions cannot prune partitions.
            PlanTestBase.assertContains(plan, "TABLE: t2_par\n" +
                    "     PARTITION PREDICATES: 4: par_col IS NOT NULL\n" +
                    "     partitions=7/7");
        }

        // run 4
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.dropPartition("partitioned_db", "t1_par", "par_col=3/par_date=2020-01-05");
            mockedHiveMetadata.dropPartition("partitioned_db", "t2_par", "par_col=3/par_date=2020-01-05");

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertTrue(execPlan == null);
        }
        starRocksAssert.dropMaterializedView("hive_partition_prune_non_ref_tables1");
    }

    @Test
    public void testHivePartitionPruneNonRefBaseTable3() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withTable("CREATE TABLE `test_partition_prune_tbl1` (\n" +
                "`k1` date,\n" +
                "`k2` int,\n" +
                "`k3` int\n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "START (\"2020-10-01\") END (\"2020-12-01\") EVERY (INTERVAL 15 day)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3 " +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withTable("CREATE TABLE `test_partition_prune_tbl2` (\n" +
                "`k1` date,\n" +
                "`k2` int,\n" +
                "`k3` int\n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW partition_prune_mv1 \n" +
                "PARTITION BY k3\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL \n" +
                "PROPERTIES('replication_num' = '1') \n" +
                "AS " +
                " SELECT test_partition_prune_tbl2.k1 as k1, test_partition_prune_tbl2.k2 as k2, " +
                " test_partition_prune_tbl1.k1 as k3, test_partition_prune_tbl1.k2 as k4\n" +
                "      FROM test_partition_prune_tbl1 join test_partition_prune_tbl2 on " +
                " test_partition_prune_tbl1.k1=test_partition_prune_tbl2.k1;");

        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "partition_prune_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());

        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000);

        // run 1
        {
            String insertSql = "INSERT INTO test_partition_prune_tbl1 VALUES (\"2020-11-10\",1,1);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "partitions=5/5\n" +
                    "     rollup: test_partition_prune_tbl1");
            PlanTestBase.assertContains(plan, "     TABLE: test_partition_prune_tbl2\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 4: k1 IS NOT NULL\n" +
                    "     partitions=1/1");
        }

        // run 2
        {
            String insertSql = "INSERT INTO test_partition_prune_tbl2 VALUES (\"2020-11-10\",1,1);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: test_partition_prune_tbl1\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: k1 IS NOT NULL");
            PlanTestBase.assertContains(plan, "     TABLE: test_partition_prune_tbl2\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 4: k1 IS NOT NULL\n" +
                    "     partitions=1/1");
        }

        // run 3
        {
            String insertSql = "INSERT INTO test_partition_prune_tbl1 VALUES (\"2020-11-10\",1,1);";
            executeInsertSql(connectContext, insertSql);

            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "partitions=5/5\n" +
                    "     rollup: test_partition_prune_tbl1");
            PlanTestBase.assertContains(plan, "     TABLE: test_partition_prune_tbl2\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 4: k1 IS NOT NULL\n" +
                    "     partitions=1/1");
        }

        starRocksAssert.dropMaterializedView("partition_prune_mv1");
    }

    @Test
    public void testCancelRefreshMV() throws Exception {
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`hive_parttbl_mv1`\n" +
                        "COMMENT \"MATERIALIZED_VIEW\"\n" +
                        "PARTITION BY (`l_shipdate`)\n" +
                        "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView materializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "hive_parttbl_mv1"));

        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        taskRun.kill();
        try {
            initAndExecuteTaskRun(taskRun);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("User Cancelled"));
            starRocksAssert.dropMaterializedView("hive_parttbl_mv1");
            return;
        }
        Assert.fail("should throw exception");
    }

    @Test
    public void testDropBaseVersionMetaOfExternalTable() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view test_drop_partition_mv1\n" +
                "PARTITION BY date_trunc('day', l_shipdate) \n" +
                "distributed by hash(l_orderkey) buckets 3\n" +
                "refresh async every (interval 1 day)\n" +
                "as SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_drop_partition_mv1"));
        Map<BaseTableInfo, Map<String, MaterializedView.BasePartitionInfo>> versionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableInfoVisibleVersionMap();
        Map<String, Set<String>> mvPartitionNameRefBaseTablePartitionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getMvPartitionNameRefBaseTablePartitionMap();
        Map<String, MaterializedView.BasePartitionInfo> tableMap = Maps.newHashMap();
        // TODO: how to get hive table meta from catalog.
        BaseTableInfo baseTableInfo = new BaseTableInfo("hive0", "partitioned_db", "lineitem_par", "lineitem_par:0");
        // case1: version map cannot decide whether it's safe to drop p1, drop the table from version map.
        {
            tableMap.put("p1", new MaterializedView.BasePartitionInfo(1, 2, -1));
            tableMap.put("p2", new MaterializedView.BasePartitionInfo(3, 4, -1));
            versionMap.put(baseTableInfo, tableMap);

            SyncPartitionUtils.dropBaseVersionMeta(mv, "p1", null);
            Assert.assertFalse(versionMap.containsKey(baseTableInfo));
        }
        {
            tableMap.put("p1", new MaterializedView.BasePartitionInfo(1, 2, -1));
            tableMap.put("p2", new MaterializedView.BasePartitionInfo(3, 4, -1));
            versionMap.put(baseTableInfo, tableMap);

            mvPartitionNameRefBaseTablePartitionMap.put("p1", Sets.newHashSet("p1"));
            mvPartitionNameRefBaseTablePartitionMap.put("p2", Sets.newHashSet("p2"));

            SyncPartitionUtils.dropBaseVersionMeta(mv, "p1", null);
            Assert.assertTrue(versionMap.containsKey(baseTableInfo));
            Assert.assertTrue(tableMap.containsKey("p2"));
        }
        {
            tableMap.put("p1", new MaterializedView.BasePartitionInfo(1, 2, -1));
            tableMap.put("p2", new MaterializedView.BasePartitionInfo(3, 4, -1));
            versionMap.put(baseTableInfo, tableMap);

            mvPartitionNameRefBaseTablePartitionMap.put("p1", Sets.newHashSet("p1"));
            mvPartitionNameRefBaseTablePartitionMap.put("p2", Sets.newHashSet("p2"));

            SyncPartitionUtils.dropBaseVersionMeta(mv, "p3", null);
            Assert.assertTrue(versionMap.containsKey(baseTableInfo));
            Assert.assertTrue(tableMap.containsKey("p2"));
        }
        starRocksAssert.dropMaterializedView("test_drop_partition_mv1");
    }

    /**
     * When refresh some partitions of MV, each refresh task should only refresh the corresponding partitions of base
     * table instead of all of them
     */
    @Test
    public void testRefreshExternalTablePrecise() throws Exception {
        starRocksAssert.withMaterializedView("create materialized view test_mv_external\n" +
                "PARTITION BY date_trunc('day', l_shipdate) \n" +
                "distributed by hash(l_orderkey) buckets 3\n" +
                "refresh deferred manual\n" +
                " properties ('partition_refresh_number'='1') \n" +
                "as SELECT `l_orderkey`, `l_suppkey`, `l_shipdate`  FROM `hive0`.`partitioned_db`.`lineitem_par` as a;");

        List<List<String>> calls = Lists.newArrayList();
        new MockUp<MetadataMgr>() {
            @Mock
            public void refreshTable(String catalogName, String srDbName, Table table,
                                     List<String> partitionNames, boolean onlyCachedPartitions) {
                calls.add(partitionNames);
            }
        };

        starRocksAssert.refreshMvPartition("refresh materialized view test_mv_external partition " +
                " start('1998-01-01') end('1998-01-04')");
        Assert.assertEquals(
                Lists.newArrayList(
                        Lists.newArrayList("l_shipdate=1998-01-01"),
                        Lists.newArrayList("l_shipdate=1998-01-02"),
                        Lists.newArrayList("l_shipdate=1998-01-03")
                ),
                calls);
    }

    @Test
    public void testCreateMVWithMultiPartitionColumns1() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW test_mv1\n" +
                        "PARTITION BY (par_date, par_col)\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "   \"replication_num\" = \"1\"\n" +
                        ")\n" +
                        "AS SELECT * FROM hive0.partitioned_db.t1_par;");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "test_mv1"));
        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        // run 1
        {
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: t1_par\n");
            PlanTestBase.assertContains(plan, "     partitions=6/6");
        }

        // run 2
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.addPartition("partitioned_db", "t1_par",
                    "par_col=4/par_date=2020-01-05");
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: t1_par\n" +
                    "     PARTITION PREDICATES: 5: par_date = '2020-01-05', 4: par_col = 4\n" +
                    "     partitions=1/7");
        }

        // run 3
        {
            MockedHiveMetadata mockedHiveMetadata =
                    (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
            mockedHiveMetadata.dropPartition("partitioned_db", "t1_par",
                    "par_col=4/par_date=2020-01-05");
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertTrue(execPlan == null);
        }
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testCreateMVWithMultiPartitionColumns2() throws Exception {
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW test_mv1\n" +
                        "PARTITION BY (par_col, par_date)\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "   \"replication_num\" = \"1\"\n" +
                        ")\n" +
                        "AS SELECT * FROM hive0.partitioned_db.t1_par;");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), "test_mv1"));

        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();

        MockedHiveMetadata mockedHiveMetadata =
                (MockedHiveMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME).get();
        // run 1
        {
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: t1_par\n");
            PlanTestBase.assertContains(plan, "     partitions=6/6");
        }

        // run 2
        {
            mockedHiveMetadata.addPartition("partitioned_db", "t1_par",
                    "par_col=4/par_date=2020-01-05");
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            String plan = execPlan.getExplainString(TExplainLevel.NORMAL);
            PlanTestBase.assertContains(plan, "     TABLE: t1_par\n" +
                    "     PARTITION PREDICATES: 4: par_col = 4, 5: par_date = '2020-01-05'\n" +
                    "     partitions=1/7");
        }

        // run 3
        {

            mockedHiveMetadata.dropPartition("partitioned_db", "t1_par",
                    "par_col=4/par_date=2020-01-05");
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor) taskRun.getProcessor();
            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            Assert.assertTrue(execPlan == null);
        }
        starRocksAssert.dropMaterializedView("test_mv1");
    }

    @Test
    public void testCreateMVWithMultiPartitionColumns3() {
        try {
            starRocksAssert.withMaterializedView(
                    "CREATE MATERIALIZED VIEW test_mv1\n" +
                            "PARTITION BY (par_date, par_col)\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "   \"replication_num\" = \"1\"\n" +
                            ")\n" +
                            "AS SELECT a.c1, a.c2, b.par_col, b.par_date \n" +
                            "FROM hive0.partitioned_db.t2_par a join \n" +
                            "hive0.partitioned_db.t1_par b using (par_col)");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("The current partition expr maps size 2 should be equal " +
                    "to the size of the first partition expr maps: 1."));
        }
    }
}