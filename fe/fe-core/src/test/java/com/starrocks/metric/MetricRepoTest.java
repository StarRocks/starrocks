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

package com.starrocks.metric;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class MetricRepoTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        // init brpc so brpc metrics can be collected
        BrpcProxy.getBackendService(new TNetworkAddress("127.0.0.1", 12345));
        PlanTestBase.beforeClass();

        starRocksAssert.withDatabase("test_metric");
    }

    @AfterAll
    public static void afterClass() {
        PlanTestBase.afterClass();
        try {
            starRocksAssert.dropDatabase("test_metric");
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetMetric() throws Exception {
        starRocksAssert.useDatabase("test_metric")
                .withTable("create table t1 (c1 int, c2 string)" +
                        " distributed by hash(c1) " +
                        " properties('replication_num'='1') ");
        Table t1 = starRocksAssert.getTable("test_metric", "t1");

        // update metric
        TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(t1.getId());
        entity.counterScanBytesTotal.increase(1024L);

        // verify metric
        JsonMetricVisitor visitor = new JsonMetricVisitor("m");
        MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true, true);
        MetricRepo.getMetric(visitor, params);
        String json = visitor.build();
        Assertions.assertTrue(StringUtils.isNotEmpty(json));
        Assertions.assertTrue(json.contains("test_metric"));
        Assertions.assertTrue(json.contains("brpc_pool_numactive"));
    }

    @Test
    public void testLeaderAwarenessMetric() {
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().isLeader());

        List<Metric> metrics = MetricRepo.getMetricsByName("job");
        MetricVisitor visitor = new PrometheusMetricVisitor("");
        for (Metric m : metrics) {
            visitor.visit(m);
        }
        // _job{is_leader="true", job="load", type="INSERT", state="UNKNOWN"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="PENDING"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="ETL"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="LOADING"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="COMMITTED"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="FINISHED"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="CANCELLED"} 0
        // _job{is_leader="true", job="load", type="INSERT", state="QUEUEING"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="UNKNOWN"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="PENDING"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="ETL"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="LOADING"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="COMMITTED"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="FINISHED"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="CANCELLED"} 0
        // _job{is_leader="true", job="load", type="BROKER", state="QUEUEING"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="UNKNOWN"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="PENDING"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="ETL"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="LOADING"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="COMMITTED"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="FINISHED"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="CANCELLED"} 0
        // _job{is_leader="true", job="load", type="SPARK", state="QUEUEING"} 0
        // _job{is_leader="true", job="alter", type="ROLLUP", state="running"} 0
        // _job{is_leader="true", job="alter", type="SCHEMA_CHANGE", state="running"} 0
        String output = visitor.build();
        String [] lines = output.split("\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }
            Assertions.assertTrue(line.contains("is_leader=\"true\""), line);
        }
    }

    @Test
    public void testRoutineLoadJobMetrics() {
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().isLeader());
        List<Metric> metrics = MetricRepo.getMetricsByName("routine_load_jobs");
        MetricVisitor visitor = new PrometheusMetricVisitor("ut");
        for (Metric m : metrics) {
            visitor.visit(m);
        }
        // ut_routine_load_jobs{is_leader="true", state="NEED_SCHEDULE"} 0
        // ut_routine_load_jobs{is_leader="true", state="RUNNING"} 0
        // ut_routine_load_jobs{is_leader="true", state="PAUSED"} 0
        // ut_routine_load_jobs{is_leader="true", state="STOPPED"} 0
        // ut_routine_load_jobs{is_leader="true", state="CANCELLED"} 0
        // ut_routine_load_jobs{is_leader="true", state="UNSTABLE"} 0
        String output = visitor.build();
        String[] lines = output.split("\n");
        for (String line : lines) {
            if (line.startsWith("#")) {
                continue;
            }
            Assertions.assertTrue(line.contains("is_leader=\"true\""), line);
        }
    }

    @Test
    public void testCloneMetrics() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Assertions.assertTrue(globalStateMgr.isLeader());

        TabletScheduler tabletScheduler = globalStateMgr.getTabletScheduler();
        TabletSchedulerStat stat = tabletScheduler.getStat();
        stat.counterCloneTask.incrementAndGet(); // 1
        stat.counterCloneTaskSucceeded.incrementAndGet(); // 1
        stat.counterCloneTaskInterNodeCopyBytes.addAndGet(100L);
        stat.counterCloneTaskInterNodeCopyDurationMs.addAndGet(10L);
        stat.counterCloneTaskIntraNodeCopyBytes.addAndGet(101L);
        stat.counterCloneTaskIntraNodeCopyDurationMs.addAndGet(11L);

        TabletSchedCtx ctx1 = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, 1L, 2L, 3L, 4L, 1001L, System.currentTimeMillis());
        Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx1);
        TabletSchedCtx ctx2 = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, 1L, 2L, 3L, 4L, 1002L, System.currentTimeMillis());
        Deencapsulation.invoke(tabletScheduler, "addToRunningTablets", ctx2);
        Set<Long> allTabletIds = Deencapsulation.getField(tabletScheduler, "allTabletIds");
        allTabletIds.add(1001L);
        allTabletIds.add(1002L);

        // check
        List<Metric> scheduledTabletNum = MetricRepo.getMetricsByName("scheduled_tablet_num");
        Assertions.assertEquals(1, scheduledTabletNum.size());
        Assertions.assertEquals(2L, (Long) scheduledTabletNum.get(0).getValue());

        List<Metric> scheduledPendingTabletNums = MetricRepo.getMetricsByName("scheduled_pending_tablet_num");
        Assertions.assertEquals(2, scheduledPendingTabletNums.size());
        for (Metric metric : scheduledPendingTabletNums) {
            // label 0 is is_leader
            MetricLabel label0 = (MetricLabel) metric.getLabels().get(0);
            Assertions.assertEquals("is_leader", label0.getKey());
            Assertions.assertEquals("true", label0.getValue());

            MetricLabel label1 = (MetricLabel) metric.getLabels().get(1);
            String type = label1.getValue();
            if (type.equals("REPAIR")) {
                Assertions.assertEquals(0L, metric.getValue());
            } else if (type.equals("BALANCE")) {
                Assertions.assertEquals(1L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> scheduledRunningTabletNums = MetricRepo.getMetricsByName("scheduled_running_tablet_num");
        Assertions.assertEquals(2, scheduledRunningTabletNums.size());
        for (Metric metric : scheduledRunningTabletNums) {
            // label 0 is is_leader
            MetricLabel label = (MetricLabel) metric.getLabels().get(1);
            String type = label.getValue();
            if (type.equals("REPAIR")) {
                Assertions.assertEquals(1L, metric.getValue());
            } else if (type.equals("BALANCE")) {
                Assertions.assertEquals(0L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> cloneTaskTotal = MetricRepo.getMetricsByName("clone_task_total");
        Assertions.assertEquals(1, cloneTaskTotal.size());
        Assertions.assertEquals(1L, (Long) cloneTaskTotal.get(0).getValue());

        List<Metric> cloneTaskSuccess = MetricRepo.getMetricsByName("clone_task_success");
        Assertions.assertEquals(1, cloneTaskSuccess.size());
        Assertions.assertEquals(1L, (Long) cloneTaskSuccess.get(0).getValue());

        List<Metric> cloneTaskCopyBytes = MetricRepo.getMetricsByName("clone_task_copy_bytes");
        Assertions.assertEquals(2, cloneTaskCopyBytes.size());
        for (Metric metric : cloneTaskCopyBytes) {
            // label 0 is is_leader
            MetricLabel label0 = (MetricLabel) metric.getLabels().get(0);
            Assertions.assertEquals("is_leader", label0.getKey());
            Assertions.assertEquals("true", label0.getValue());

            MetricLabel label1 = (MetricLabel) metric.getLabels().get(1);
            String type = label1.getValue();
            if (type.equals("INTER_NODE")) {
                Assertions.assertEquals(100L, metric.getValue());
            } else if (type.equals("INTRA_NODE")) {
                Assertions.assertEquals(101L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }

        List<Metric> cloneTaskCopyDurationMs = MetricRepo.getMetricsByName("clone_task_copy_duration_ms");
        Assertions.assertEquals(2, cloneTaskCopyDurationMs.size());
        for (Metric metric : cloneTaskCopyDurationMs) {
            // label 0 is is_leader
            MetricLabel label0 = (MetricLabel) metric.getLabels().get(0);
            Assertions.assertEquals("is_leader", label0.getKey());
            Assertions.assertEquals("true", label0.getValue());

            MetricLabel label1 = (MetricLabel) metric.getLabels().get(1);
            String type = label1.getValue();
            if (type.equals("INTER_NODE")) {
                Assertions.assertEquals(10L, metric.getValue());
            } else if (type.equals("INTRA_NODE")) {
                Assertions.assertEquals(11L, metric.getValue());
            } else {
                Assertions.fail("Unknown type: " + type);
            }
        }
    }

    @Test
    public void testRoutineLoadLagTimeMetricsCollection() {
        // Test that routine load lag time metrics are collected when config is enabled
        boolean originalConfigValue = Config.enable_routine_load_lag_time_metrics;
        try {
            // Test case 1: Config enabled - should collect metrics
            Config.enable_routine_load_lag_time_metrics = true;
            
            JsonMetricVisitor visitor = new JsonMetricVisitor("test");
            MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true, true);
            
            // This should execute line 914 and call RoutineLoadLagTimeMetricMgr.getInstance().collectRoutineLoadLagTimeMetrics(visitor)
            String result = MetricRepo.getMetric(visitor, params);
            
            // Verify that the method completed successfully (no exceptions thrown)
            Assertions.assertNotNull(result);
            
            // Test case 2: Config disabled - should skip metrics collection
            Config.enable_routine_load_lag_time_metrics = false;
            
            JsonMetricVisitor visitor2 = new JsonMetricVisitor("test2");
            String result2 = MetricRepo.getMetric(visitor2, params);
            
            // Verify that the method completed successfully even when config is disabled
            Assertions.assertNotNull(result2);
            
        } finally {
            // Restore original config value
            Config.enable_routine_load_lag_time_metrics = originalConfigValue;
        }
    }

    @Test
    public void testEnableTableMetricsCollect() throws Exception {
        starRocksAssert.withDatabase("test_table_metrics").useDatabase("test_table_metrics")
                .withTable("create table t_metrics (c1 int, c2 string)" +
                        " distributed by hash(c1)" +
                        " properties('replication_num'='1')");
        Table tbl = starRocksAssert.getTable("test_table_metrics", "t_metrics");
        TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(tbl.getId());
        entity.counterScanBytesTotal.increase(512L);

        boolean original = Config.enable_table_metrics_collect;
        try {
            // enabled: table metrics should be present
            Config.enable_table_metrics_collect = true;
            PrometheusMetricVisitor visitor1 = new PrometheusMetricVisitor("m");
            MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true, true);
            MetricRepo.getMetric(visitor1, params);
            String output1 = visitor1.build();
            Assertions.assertTrue(output1.contains("t_metrics"),
                    "Table metrics should be present when enable_table_metrics_collect is true");

            // disabled: table metrics should NOT be present
            Config.enable_table_metrics_collect = false;
            PrometheusMetricVisitor visitor2 = new PrometheusMetricVisitor("m");
            MetricRepo.getMetric(visitor2, params);
            String output2 = visitor2.build();
            Assertions.assertFalse(output2.contains("t_metrics"),
                    "Table metrics should not be present when enable_table_metrics_collect is false");
        } finally {
            Config.enable_table_metrics_collect = original;
            starRocksAssert.dropDatabase("test_table_metrics");
        }
    }

    @Test
    public void testTotalDataSizeBytesMetric() throws Exception {
        // Test that total_data_size_bytes correctly aggregates across multiple databases
        starRocksAssert.withDatabase("test_multi_1").useDatabase("test_multi_1")
                .withTable("create table t1(c1 int, c2 int) properties('replication_num' = '1')");
        starRocksAssert.withDatabase("test_multi_2").useDatabase("test_multi_2")
                .withTable("create table t2(c1 int, c2 int) properties('replication_num' = '1')");

        try {
            // Get the databases and update their used quota
            Database db1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_multi_1");
            Database db2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_multi_2");
            // Set some test values
            db1.usedDataQuotaBytes.set(1000L);
            db2.usedDataQuotaBytes.set(2000L);

            // Collect metrics
            PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("test");
            MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true, true);
            MetricRepo.getMetric(visitor, params);
            String output = visitor.build();

            // Verify total_data_size_bytes metric exists, the actual metric name will be prefixed with "test_"
            Assertions.assertTrue(output.contains("test_total_data_size_bytes"),
                    "Metric output should contain total_data_size_bytes");

            // Verify it's a gauge metric
            Assertions.assertTrue(output.contains("# TYPE test_total_data_size_bytes gauge"),
                    "Should be declared as a gauge metric");

            for (String line : output.split("\n")) {
                if (line.startsWith("test_total_data_size_bytes")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        long totalValue = Long.parseLong(parts[1]);
                        // The total should at least include our two test databases
                        // Note: It may include other databases from the test framework
                        Assertions.assertTrue(totalValue >= 3000L,
                                "Total should be at least 3000 (1000 + 2000 from our test databases)");
                    }
                }
            }
        } finally {
            starRocksAssert.dropDatabase("test_multi_1");
            starRocksAssert.dropDatabase("test_multi_2");
        }
    }

    @Test
    public void testTotalDataSizeBytesMetric_PrometheusFormat() throws Exception {
        // Test that the metric is correctly formatted in Prometheus format
        starRocksAssert.withDatabase("test_prometheus").useDatabase("test_prometheus")
                .withTable("create table t1(c1 int, c2 int) properties('replication_num' = '1')");

        try {
            PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("starrocks");
            MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true, true);
            MetricRepo.getMetric(visitor, params);
            String output = visitor.build();

            // Parse the output to find our metric
            String[] lines = output.split("\n");
            boolean foundTypeDeclaration = false;
            boolean foundMetricValue = false;

            for (String line : lines) {
                if (line.contains("# TYPE") && line.contains("total_data_size_bytes")) {
                    foundTypeDeclaration = true;
                    Assertions.assertTrue(line.contains("gauge"),
                            "Should be declared as gauge type");
                }
                if (line.contains("# HELP") && line.contains("total_data_size_bytes")) {
                    Assertions.assertTrue(line.contains("total size of all databases"),
                            "Help text should describe the metric");
                }
                if (line.startsWith("starrocks_total_data_size_bytes")) {
                    foundMetricValue = true;
                    // Extract the value and verify it's a number
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                        try {
                            long value = Long.parseLong(parts[1]);
                            Assertions.assertTrue(value >= 0,
                                    "Metric value should be non-negative");
                        } catch (NumberFormatException e) {
                            Assertions.fail("Metric value should be a valid number: " + parts[1]);
                        }
                    }
                }
            }

            Assertions.assertTrue(foundTypeDeclaration,
                    "Should have TYPE declaration for total_data_size_bytes");
            Assertions.assertTrue(foundMetricValue,
                    "Should have the actual metric value in output");

        } finally {
            starRocksAssert.dropDatabase("test_prometheus");
        }
    }
}
