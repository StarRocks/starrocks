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

package com.starrocks.sql.plan;

import com.starrocks.common.profile.Tracers;
import org.junit.Test;

public class TracerTest extends PlanTestBase {
    @Test
    public void testTracerDefault() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        assertNotContains(pr, "Trace");
    }

    @Test
    public void testTracerTimerNone1() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMER, "xx");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertNotContains(pr, "--");
    }

    @Test
    public void testTracerTimerNone2() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMER, "None");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertNotContains(pr, "--");
    }

    @Test
    public void testTracerTimerBase() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMER, "Base");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertContains(pr, "-- Planner");
    }

    @Test
    public void testTracerTimerOptimizer() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMER, "Optimizer");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertContains(pr, "--");
    }

    @Test
    public void testTracerTimingBase() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMING, "Base");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printTiming();
        Tracers.close();
        assertContains(pr, "ms| watchScope");
    }

    @Test
    public void testTracerLogOptimizer() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.TIMING, "Optimizer");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "origin logicOperatorTree");
        assertContains(pr, "TRACE QUERY");
    }

    @Test
    public void testTracerLog1Optimizer() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "Optimizer");
        String sql = "select l_returnflag, sum(l_quantity) as sum_qty,\n" +
                "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge\n" +
                "from lineitem\n" +
                "where l_shipdate <= date '1998-12-01'\n" +
                "group by l_returnflag \n" +
                "order by l_returnflag;";
        getFragmentPlan(sql);
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "origin logicOperatorTree");
        assertContains(pr, "TRACE QUERY");
    }

    @Test
    public void testTracerLogAnalyze() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, Tracers.Mode.LOGS, "Analyze");
        String sql = "select l_returnflag, sum(l_quantity) as sum_qty,\n" +
                "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge\n" +
                "from lineitem\n" +
                "where l_shipdate <= date '1998-12-01'\n" +
                "group by l_returnflag \n" +
                "order by l_returnflag;";
        getFragmentPlan(sql);
        String pr = Tracers.printLogs();
        Tracers.close();
        assertContains(pr, "QueryStatement");
    }
}
