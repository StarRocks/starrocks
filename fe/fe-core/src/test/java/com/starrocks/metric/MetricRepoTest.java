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
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MetricRepoTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        // init brpc so brpc metrics can be collected
        BrpcProxy.getBackendService(new TNetworkAddress("127.0.0.1", 12345));
        PlanTestBase.beforeClass();

        starRocksAssert.withDatabase("test_metric");
    }

    @AfterClass
    public static void afterClass() {
        PlanTestBase.afterClass();
        try {
            starRocksAssert.dropDatabase("test_metric");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
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
        Assert.assertTrue(StringUtils.isNotEmpty(json));
        Assert.assertTrue(json.contains("test_metric"));
        Assert.assertTrue(json.contains("brpc_pool_numactive"));
    }

    @Test
    public void testLeaderAwarenessMetric() {
        Assert.assertTrue(GlobalStateMgr.getCurrentState().isLeader());

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
            Assert.assertTrue(line, line.contains("is_leader=\"true\""));
        }
    }

    @Test
    public void testRoutineLoadJobMetrics() {
        Assert.assertTrue(GlobalStateMgr.getCurrentState().isLeader());
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
            Assert.assertTrue(line, line.contains("is_leader=\"true\""));
        }

    }
}
