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
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
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
import java.util.stream.Collectors;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionBasedMvRefreshProcessorIcebergTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static void triggerRefreshMv(Database testDb, MaterializedView partitionedMaterializedView)
                throws Exception {
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
    }

    @Test
    public void testCreateNonPartitionedMVForIceberg() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_mv1` " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"storage_medium\" = \"HDD\"\n" +
                                ")\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_mv2` " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"storage_medium\" = \"HDD\"\n" +
                                ")\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");

        // Partitioned base table
        {
            String mvName = "iceberg_mv2";
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
            refreshMVRange(mvName, true);
            List<String> partitionNames = mv.getPartitions().stream().map(Partition::getName)
                        .sorted().collect(Collectors.toList());
            Assert.assertEquals(ImmutableList.of(mvName), partitionNames);
            String querySql = "SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1`";
            starRocksAssert.query(querySql).explainContains(mvName);
            starRocksAssert.dropMaterializedView(mvName);
        }

        // Non-Partitioned base table
        {
            String mvName = "iceberg_mv1";
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
            refreshMVRange(mvName, true);
            List<String> partitionNames = mv.getPartitions().stream().map(Partition::getName)
                        .sorted().collect(Collectors.toList());
            Assert.assertEquals(ImmutableList.of(mvName), partitionNames);

            // test rewrite
            String querySql = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0`";
            starRocksAssert.query(querySql).explainContains(mvName);
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testCreatePartitionedMVForIceberg() throws Exception {
        String mvName = "iceberg_parttbl_mv1";
        starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv1`\n" +
                                "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"storage_medium\" = \"HDD\"\n" +
                                ")\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), "iceberg_parttbl_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(4, partitions.size());

        MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                                getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
        mockIcebergMetadata.updatePartitions("partitioned_db", "t1",
                    ImmutableList.of("date=2020-01-02"));
        // refresh only one partition
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "3: date >= '2020-01-02', 3: date < '2020-01-03'");

        Map<String, Long> partitionVersionMap = new HashMap<>();
        for (Partition p : partitionedMaterializedView.getPartitions()) {
            partitionVersionMap.put(p.getName(), p.getDefaultPhysicalPartition().getVisibleVersion());
        }

        Assert.assertEquals(
                    ImmutableMap.of("p20200104_20200105", 2L,
                                "p20200101_20200102", 2L,
                                "p20200103_20200104", 2L,
                                "p20200102_20200103", 3L),
                    ImmutableMap.copyOf(partitionVersionMap));

        // add new row and refresh again
        mockIcebergMetadata.updatePartitions("partitioned_db", "t1",
                    ImmutableList.of("date=2020-01-01"));
        taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "3: date >= '2020-01-01', 3: date < '2020-01-02'");

        // test rewrite
        starRocksAssert.query("SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1`")
                    .explainContains(mvName);
        starRocksAssert.query("SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` where date = '2020-01-01'")
                    .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform1() throws Exception {
        // test partition by year(ts)
        String mvName = "iceberg_year_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_year_mv1`\n" +
                        "PARTITION BY ts\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_year` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_year_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(5, partitions.size());
        List<String> partitionNames = ImmutableList.of("p20190101000000", "p20200101000000", "p20210101000000",
                "p20220101000000", "p20230101000000");
        Assert.assertTrue(partitions.stream().map(Partition::getName).allMatch(partitionNames::contains));

        MockIcebergMetadata mockIcebergMetadata =
                (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
        mockIcebergMetadata.updatePartitions("partitioned_transforms_db", "t0_year",
                ImmutableList.of("ts_year=2020"));
        // refresh only one partition
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                taskRun.getProcessor();

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        System.out.println(execPlan.getExplainString(StatementBase.ExplainLevel.NORMAL));
        assertPlanContains(execPlan, "3: ts >= '2020-01-01 00:00:00', 3: ts < '2021-01-01 00:00:00'");

        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_year`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform2() throws Exception {
        // test partition by month(ts)
        String mvName = "iceberg_month_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_month_mv1`\n" +
                        "PARTITION BY ts\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_month_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(5, partitions.size());
        List<String> partitionNames = ImmutableList.of("p20220101000000", "p20220201000000", "p20220301000000",
                "p20220401000000", "p20220501000000");
        Assert.assertTrue(partitions.stream().map(Partition::getName).allMatch(partitionNames::contains));
        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform3() throws Exception {
        // test partition by day(ts)
        String mvName = "iceberg_day_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_mv1`\n" +
                        "PARTITION BY ts\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_day` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_day_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(5, partitions.size());
        List<String> partitionNames = ImmutableList.of("p20220101000000", "p20220102000000", "p20220103000000",
                "p20220104000000", "p20220105000000");
        Assert.assertTrue(partitions.stream().map(Partition::getName).allMatch(partitionNames::contains));
        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_day`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform4() throws Exception {
        // test partition by hour(ts)
        String mvName = "iceberg_hour_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_hour_mv1`\n" +
                        "PARTITION BY ts\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_hour` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_hour_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assert.assertEquals(5, partitions.size());
        List<String> partitionNames = ImmutableList.of("p20220101000000", "p20220101010000", "p20220101020000",
                "p20220101030000", "p20220101040000");
        Assert.assertTrue(partitions.stream().map(Partition::getName).allMatch(partitionNames::contains));
        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_hour`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testRefreshWithCachePartitionTraits() {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test_mv1`\n" +
                                "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;",
                    () -> {
                        MaterializedView mv = getMv("test", "test_mv1");
                        PartitionBasedMvRefreshProcessor processor = refreshMV("test", mv);
                        RuntimeProfile runtimeProfile = processor.getRuntimeProfile();
                        QueryMaterializationContext.QueryCacheStats queryCacheStats = getQueryCacheStats(runtimeProfile);
                        Assert.assertTrue(queryCacheStats != null);
                        queryCacheStats.getCounter().forEach((key, value) -> {
                            if (key.contains("cache_partitionNames")) {
                                Assert.assertEquals(1L, value.longValue());
                            } else if (key.contains("cache_getPartitionKeyRange")) {
                                Assert.assertEquals(3L, value.longValue());
                            } else {
                                Assert.assertEquals(1L, value.longValue());
                            }
                        });
                        Set<String> partitionsToRefresh1 = getPartitionNamesToRefreshForMv(mv);
                        Assert.assertTrue(partitionsToRefresh1.isEmpty());
                    });
    }

    private void testCreateMVWithMultiPartitionColumns(String icebergTable,
                                                       String updatePartitionName,
                                                       List<String> expectedPartitionNames,
                                                       String expectedExecPlan) throws Exception {
        String mvName = "test_mv1";
        try {
            String query = String.format("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.%s as a",
                    icebergTable);
            String ddl = String.format("CREATE MATERIALIZED VIEW `%s`\n" +
                    "PARTITION BY (id, data, ts)\n" +
                    "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS %s;", mvName, query);
            starRocksAssert.useDatabase("test").withMaterializedView(ddl);

            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView partitionedMaterializedView =
                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(testDb.getFullName(), mvName));
            triggerRefreshMv(testDb, partitionedMaterializedView);

            Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
            Assert.assertEquals(expectedPartitionNames.size(), partitions.size());
            List<String> partitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toList());
            System.out.println(partitionNames);
            Assert.assertTrue(partitionNames.stream().allMatch(expectedPartitionNames::contains));

            // update partition
            MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
            mockIcebergMetadata.updatePartitions("partitioned_transforms_db", icebergTable,
                    ImmutableList.of(updatePartitionName));

            // refresh only one partition
            Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            initAndExecuteTaskRun(taskRun);
            PartitionBasedMvRefreshProcessor processor = (PartitionBasedMvRefreshProcessor)
                    taskRun.getProcessor();

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            assertPlanContains(execPlan, expectedExecPlan);

            // test rewrite
            QueryDebugOptions debugOptions = new QueryDebugOptions();
            debugOptions.setEnableQueryTraceLog(true);
            connectContext.getSessionVariable().setQueryDebugOptions(debugOptions.toString());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, query);
            PlanTestBase.assertContains(plan, mvName);
        } catch (Exception e) {
            throw e;
        } finally {
            try {
                starRocksAssert.dropMaterializedView(mvName);
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsHour() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_hour", "id=1/data=a/ts_hour=2022-01-01-00",
                ImmutableList.of("p1_a_20220101000000", "p2_a_20220101010000"),
                "PREDICATES: 1: id = 1, 2: data = 'a', hour(3: ts) = '2022-01-01 00:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsDay() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_day", "id=1/data=a/ts_day=2022-01-01",
                ImmutableList.of("p1_a_20220101000000", "p2_a_20220102000000"),
                "PREDICATES: 1: id = 1, 2: data = 'a', day(3: ts) = '2022-01-01 00:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsMonth() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_month", "id=1/data=a/ts_month=2022-01",
                ImmutableList.of("p1_a_20220101000000", "p2_a_20220201000000"),
                "PREDICATES: 1: id = 1, 2: data = 'a', month(3: ts) = '2022-01-01 00:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsYear() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_year", "id=2/data=a/ts_year=2024",
                ImmutableList.of("p1_a_20240101000000", "p2_a_20240101000000"),
                "PREDICATES: 1: id = 2, 2: data = 'a', year(3: ts) = '2024-01-01 00:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsBucket() {
        try {
            testCreateMVWithMultiPartitionColumns("t0_multi_bucket", "id=1/data=a/ts_bucket=0",
                    ImmutableList.of("p1_a_20240101000000", "p2_a_20240101000000"),
                    "3: ts >= '2024-01-01 00:00:00', 3: ts < '2025-01-01 00:00:00'");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Do not support create materialized view when base " +
                    "iceberg table partition transform has bucket or truncate."));
        }
    }
}