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

package com.starrocks.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class ViewBasedMvRewritePerfTest extends MVTestBase {

    private static final int MV_NUM = 4;

    @Rule
    public TestRule benchRun = new BenchmarkRule();

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();

        // Env
        Config.mv_plan_cache_max_size = 1024;
        CachingMvPlanContextBuilder.getInstance().rebuildCache();
        starRocksAssert.getCtx().setDumpInfo(null);
        connectContext.getSessionVariable().setCboMaterializedViewRewriteCandidateLimit(1000);
        connectContext.getSessionVariable().setCboMaterializedViewRewriteRuleOutputLimit(1000);

        // Base tables
        starRocksAssert.withTable(cluster, "t0");
        starRocksAssert.withTable(cluster, "t1");
        starRocksAssert.withTable(cluster, "t2");
        cluster.runSql("test", "insert into t0 values(1, 1, 1), (2,2,2)");
        cluster.runSql("test", "insert into t1 values(1, 1, 1), (2,2,2)");

        starRocksAssert.withView("create view t0_view_1 as select v1, sum(v2) as total1 from t0 group by v1");
        starRocksAssert.withView("create view t0_view_2 as select v1, sum(v3) as total2 from t0 group by v1");
        starRocksAssert.withView("create view join_view_1 " +
                "as " +
                "select v1.v1, total1, total2 " +
                "from t0_view_1 v1 join t0_view_2 v2 " +
                "on v1.v1 = v2.v1");

        starRocksAssert.withRefreshedMaterializedView("create materialized view mv_agg_join_1 " +
                " refresh async as " +
                " select * from join_view_1");

        starRocksAssert.withRefreshedMaterializedView("create materialized view mv_agg_1 " +
                " refresh async as " +
                " select * from t0_view_1");

        // use another table
        starRocksAssert.withView("create view t1_view_1 as select k1, sum(v1) as total1 from t1 group by k1");
        starRocksAssert.withView("create view t1_view_2 as select k1, sum(v2) as total2 from t1 group by k1");
        starRocksAssert.withView("create view join_view_2 " +
                "as " +
                "select v1.k1, total1, total2 " +
                "from t1_view_1 v1 join t1_view_2 v2 " +
                "on v1.k1 = v2.k1");

        starRocksAssert.withView("create view t2_view_1 as select v1, sum(v2) as total1 from t2 group by v1");
        starRocksAssert.withView("create view t2_view_2 as select v1, sum(v3) as total2 from t2 group by v1");
        // 40 MV with same schema
        for (int i = 0; i < MV_NUM; i++) {
            // join MV
            String joinMV = "mv_candidate_join_" + i;
            starRocksAssert.withRefreshedMaterializedView("create materialized view " + joinMV +
                    " refresh async as " +
                    " select * from join_view_2");

            String viewJoinMv = "mv_candidate_view_join_" + i;
            starRocksAssert.withRefreshedMaterializedView("create materialized view " + viewJoinMv +
                    " refresh async as " +
                    " select v1.v1, total1, total2 " +
                    " from t2_view_1 v1 join t2_view_2 v2 " +
                    " on v1.v1 = v2.v1");
        }
    }

    @Before
    public void before() {
        super.before();
        starRocksAssert.getCtx().getSessionVariable().setEnableQueryDump(false);
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(true);
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    // round: 0.02 [+- 0.00], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 1, GC.time: 0.01,
    // time.total: 0.35, time.warmup: 0.05, time.bench: 0.30
    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = 20)
    public void testViewBaseRewrite_Basic() throws Exception {
        final String query = "select * from join_view_1";
        starRocksAssert.query(query).explainContains("mv_agg_join_1");
    }

    // round: 0.01 [+- 0.00], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 0, GC.time: 0.00,
    // time.total: 0.36, time.warmup: 0.06, time.bench: 0.30
    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = 20)
    public void testViewBaseRewrite_Basic_Disable() throws Exception {
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
        final String query = "select * from join_view_1";
        starRocksAssert.query(query).explainWithout("mv_agg_join_1");
    }

    // the following two tests test whether use view based mv rewrite or original SPJG mv rewrite
    // round: 0.01 [+- 0.00], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 0, GC.time: 0.00,
    // time.total: 0.29, time.warmup: 0.11, time.bench: 0.18
    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = 20)
    public void testViewBaseRewrite_ViewBased_VS_Spjg() throws Exception {
        final String query = "select * from t0_view_1";
        starRocksAssert.query(query).explainContains("mv_agg_1");
    }

    // round: 0.01 [+- 0.00], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 0, GC.time: 0.00,
    // time.total: 0.19, time.warmup: 0.03, time.bench: 0.16
    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = 20)
    public void testViewBaseRewrite_ViewBased_VS_Spjg_DisableView() throws Exception {
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
        final String query = "select * from t0_view_1";
        starRocksAssert.query(query).explainContains("mv_agg_1");
    }

    // round: 0.03 [+- 0.01], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 1, GC.time: 0.01,
    // time.total: 0.74, time.warmup: 0.12, time.bench: 0.62
    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = 20)
    public void testViewBaseRewrite_ViewBased_withManyMvs() throws Exception {
        final String query = "select * from join_view_2";
        starRocksAssert.query(query).explainContains("mv_candidate_join_");
    }

    // round: 0.02 [+- 0.01], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 1, GC.time: 0.02,
    // time.total: 1.05, time.warmup: 0.67, time.bench: 0.38
    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = 20)
    public void testViewBaseRewrite_ViewBased_withManyMvs_Disable() throws Exception {
        connectContext.getSessionVariable().setEnableViewBasedMvRewrite(false);
        final String query = "select * from join_view_2";
        starRocksAssert.query(query).explainWithout("mv_candidate_join_");
    }

    // round: 0.03 [+- 0.01], round.block: 0.00 [+- 0.00], round.gc: 0.00 [+- 0.00], GC.calls: 1, GC.time: 0.02,
    // time.total: 1.08, time.warmup: 0.52, time.bench: 0.56
    @Test
    @BenchmarkOptions(warmupRounds = 3, benchmarkRounds = 20)
    public void testViewBaseRewrite_ViewBased_withManyMvs_join() throws Exception {
        final String query = "select v1.v1, total1, total2 " +
                "from t2_view_1 v1 join t2_view_2 v2 " +
                "on v1.v1 = v2.v1";
        starRocksAssert.query(query).explainContains("mv_candidate_view_join_");
    }
}
