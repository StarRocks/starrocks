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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.structure.Pair;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.List;

@Ignore
public class MVPartitionCompensateOptBench extends MvRewriteTestBase {

    private static final int MV_NUMS = 100;
    private static final int BENCHMARK_RUNS = 10;

    @Rule
    public TestRule mvPartitionCompensateBench = new BenchmarkRule();

    @BeforeClass
    public static void setup() throws Exception {
        MvRewriteTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "table_with_day_partition");
        starRocksAssert.withTable(cluster, "table_with_day_partition1");
        starRocksAssert.withTable(cluster, "table_with_day_partition2");
        connectContext.getSessionVariable().setCboMaterializedViewRewriteCandidateLimit(1000);
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRuleOutputLimit(1000);

        List<String> mvPartitionExprs = ImmutableList.of("id_date", "date_trunc('day', id_date)");
        List<Pair<String, String>> refreshPartitions = ImmutableList.of(
                Pair.create("1991-03-30", "1991-03-31"),
                Pair.create("1991-03-30", "1991-04-01"),
                Pair.create("1991-03-30", "1991-04-02"),
                Pair.create("1991-04-01", "1991-04-30")
        );

        starRocksAssert.getCtx().setDumpInfo(null);
        QueryDebugOptions debugOptions = new QueryDebugOptions();
        debugOptions.setEnableQueryTraceLog(true);
        connectContext.getSessionVariable().setQueryDebugOptions(debugOptions.toString());

        int i = 0;
        while (i < MV_NUMS) {
            for (String mvPartitionExpr : mvPartitionExprs) {
                String mvName = "mv_partition_compensate_" + i;
                System.out.println(mvName);
                String mvSQL = String.format("CREATE MATERIALIZED VIEW if not exists %s \n" +
                        "PARTITION BY %s \n" +
                        "REFRESH DEFERRED MANUAL " +
                        "AS " +
                        "select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) " +
                        "from table_with_day_partition a" +
                        " left join table_with_day_partition1 b on a.id_date=b.id_date " +
                        " left join table_with_day_partition2 c on a.id_date=c.id_date " +
                        "group by a.t1a,a.id_date;", mvName, mvPartitionExpr);
                starRocksAssert.withMaterializedView(mvSQL);

                Pair<String, String> refreshParts = refreshPartitions.get(i % refreshPartitions.size());
                cluster.runSql("test", String.format("refresh materialized view %s partition " +
                        "start('%s') end('%s') with sync mode;", mvName, refreshParts.first, refreshParts.second));
                i++;
            }
        }
    }

    private void testMVPartitionCompensatePerf(int i) {
        List<Pair<String, Boolean>> expects = ImmutableList.of(
                // no partition expressions
                Pair.create("a.id_date='1991-03-30'", true),
                Pair.create("a.id_date>='1991-03-30'", false),
                Pair.create("a.id_date!='1991-03-30'", false),
                // with partition expressions && partition expressions can be pruned
                Pair.create("date_format(a.id_date, '%Y%m%d')='19910330'", true),
                Pair.create("date_format(a.id_date, '%Y-%m-%d')='1991-03-30'", true),
                Pair.create("date_trunc('day', a.id_date)='1991-03-30'", true),
                Pair.create("date_trunc('day', a.id_date)>='1991-03-30'", false),
                Pair.create("subdate(a.id_date, interval 1 day)='1991-03-29'", true),
                Pair.create("adddate(a.id_date, interval 1 day)='1991-03-31'", true),
                // with partition expressions && partition expressions can be pruned
                Pair.create("cast(a.id_date as string)='1991-03-30'", false),
                Pair.create("cast(a.id_date as string) >='1991-03-30'", false)
        );
        Pair<String, Boolean> expect = expects.get(i);
        String query = String.format("select a.t1a, a.id_date, sum(a.t1b), sum(b.t1b) \n" +
                "from table_with_day_partition a\n" +
                " left join table_with_day_partition1 b on a.id_date=b.id_date \n" +
                " left join table_with_day_partition2 c on a.id_date=c.id_date \n" +
                " where %s " +
                " group by a.t1a,a.id_date;", expect.first);
        try {
            String plan = getFragmentPlan(query);
            if (expect.second) {
                PlanTestBase.assertContains(plan, "mv_partition_compensate_");
            } else {
                PlanTestBase.assertNotContains(plan, "mv_partition_compensate_");
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf0() {
        testMVPartitionCompensatePerf(0);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf1() {
        testMVPartitionCompensatePerf(1);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf2() {
        testMVPartitionCompensatePerf(2);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf3() {
        testMVPartitionCompensatePerf(3);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf4() {
        testMVPartitionCompensatePerf(4);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf5() {
        testMVPartitionCompensatePerf(5);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf6() {
        testMVPartitionCompensatePerf(6);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf7() {
        testMVPartitionCompensatePerf(7);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf8() {
        testMVPartitionCompensatePerf(8);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf9() {
        testMVPartitionCompensatePerf(9);
    }

    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = BENCHMARK_RUNS)
    public void testMVPartitionCompensatePerf10() {
        testMVPartitionCompensatePerf(10);
    }
}