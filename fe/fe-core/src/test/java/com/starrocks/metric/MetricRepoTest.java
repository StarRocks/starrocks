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

import com.starrocks.catalog.Table;
import com.starrocks.clone.TabletSchedCtx;
import com.starrocks.clone.TabletScheduler;
import com.starrocks.clone.TabletSchedulerStat;
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
        MetricsAction.RequestParams params = new MetricsAction.RequestParams(true, true, true, true);
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
}
