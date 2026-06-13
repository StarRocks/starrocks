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

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TracerTest extends PlanTestBase {
    @Test
    public void testTracerDefault() throws Exception {
        Tracers.close();
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        assertNotContains(pr, "Trace");
    }

    @Test
    public void testTracerTimerNone1() throws Exception {
        connectContext.getSessionVariable().setBigQueryProfileThreshold("0s");
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMER", "xx");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertNotContains(pr, "--");
    }

    @Test
    public void testTracerTimerNone2() throws Exception {
        connectContext.getSessionVariable().setBigQueryProfileThreshold("0s");
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMER", "None");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertNotContains(pr, "--");
    }

    @Test
    public void testTracerTimerBase() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMER", "Base");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertContains(pr, "-- Planner");
    }

    @Test
    public void testTracerTimerOptimizer() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMER", "Optimizer");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printScopeTimer();
        Tracers.close();
        assertContains(pr, "--");
    }

    @Test
    public void testTracerTimingBase() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMING", "Base");
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        getFragmentPlan(sql);
        String pr = Tracers.printTiming();
        Tracers.close();
        assertContains(pr, "ms| watchScope");
    }

    @Test
    public void testTracerLogOptimizer() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(connectContext, "TIMING", "Optimizer");
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
        Tracers.init(connectContext, "LOGS", "Optimizer");
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
        Tracers.init(connectContext, "LOGS", "Analyze");
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

    @Test
    public void testTracerMulti() throws Exception {
        Tracers.register(connectContext);
        String sql = "select l_returnflag, sum(l_quantity) as sum_qty,\n" +
                "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge\n" +
                "from lineitem\n" +
                "where l_shipdate <= date '1998-12-01'\n" +
                "group by l_returnflag \n" +
                "order by l_returnflag;";
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.BASE, false, false);
        getFragmentPlan(sql);
        try (Timer t = Tracers.watchScope(Tracers.Module.BASE, "thisIsPrivilegeCheckCall")) {
            getFragmentPlan(sql);
        }
        String ss = Tracers.printScopeTimer();
        System.out.println(ss);
        Assertions.assertEquals(2, StringUtils.countMatches(ss, "Parser"));
        Assertions.assertEquals(2, StringUtils.countMatches(ss, "Planner"));
    }

    @Test
    public void testTracerForkConcurrency() throws Exception {
        Tracers.register(connectContext);
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.BASE, false, false);
        Tracers owner = Tracers.get();

        int numThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        try {
            // Run multiple iterations to expose any potential race conditions
            for (int iter = 0; iter < 100; iter++) {
                List<Tracers> forks = new ArrayList<>();
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < numThreads; i++) {
                    Tracers forked = owner.fork(true);
                    forks.add(forked);
                    futures.add(executor.submit(() -> {
                        try (Timer ignored = Tracers.watchScope(forked, Tracers.Module.BASE,
                                "forkedParallelWork")) {
                            // Simulate work
                        }
                        return null;
                    }));
                }
                for (Future<?> f : futures) {
                    f.get();
                }
                // Merge forks back on the owner thread after all tasks complete
                for (Tracers f : forks) {
                    owner.mergeFrom(f);
                }
            }
        } finally {
            executor.shutdown();
        }

        String output = Tracers.printScopeTimer();
        // Each iteration creates 4 scopes, 100 iterations = 400 total
        // The toString() format includes [count], e.g. "forkedParallelWork[400]"
        Assertions.assertTrue(output.contains("forkedParallelWork[400]"),
                "Expected forkedParallelWork[400] in output but got: " + output);
        Tracers.close();
    }

    @Test
    public void testTracerForkPrepareCollectMetaPattern() throws Exception {
        // Replicates the exact fork/merge orchestration used by PrepareCollectMetaTask.execute():
        // 1. outer scope on owner thread ("EXTERNAL.parallel_prepare_metadata")
        // 2. fork per parallel task → submit to executor
        // 3. join all futures
        // 4. merge all forks back
        // 5. shutdown executor in finally
        Tracers.register(connectContext);
        Tracers.init(Tracers.Mode.TIMER, Tracers.Module.BASE, false, false);
        Tracers owner = Tracers.get();

        int numTasks = 4;
        ExecutorService executor = Executors.newFixedThreadPool(numTasks);
        try {
            try (Timer ignored = Tracers.watchScope(Tracers.Module.BASE,
                    "BASE.parallel_prepare_metadata")) {
                List<Tracers> forks = new ArrayList<>(numTasks);
                Future<?>[] futures = new Future[numTasks];
                for (int i = 0; i < numTasks; i++) {
                    Tracers forked = owner.fork(true);
                    forks.add(forked);
                    String scopeName = "BASE.processSplit.table" + i;
                    futures[i] = executor.submit(() -> {
                        try (Timer t = Tracers.watchScope(forked, Tracers.Module.BASE,
                                scopeName)) {
                            // simulate metadata preparation work
                        }
                        return null;
                    });
                }
                for (Future<?> f : futures) {
                    f.get();
                }
                for (Tracers f : forks) {
                    owner.mergeFrom(f);
                }
            }
        } finally {
            executor.shutdown();
        }

        String output = Tracers.printScopeTimer();
        // Outer scope present and wrapping the parallel work
        Assertions.assertTrue(output.contains("BASE.parallel_prepare_metadata"),
                "Expected outer scope in output but got: " + output);
        // Each of the 4 forks has its own per-table scope in the merged output
        for (int i = 0; i < numTasks; i++) {
            Assertions.assertTrue(output.contains("BASE.processSplit.table" + i),
                    "Expected per-table scope table" + i + " in output but got: " + output);
        }
        Tracers.close();
    }
}
