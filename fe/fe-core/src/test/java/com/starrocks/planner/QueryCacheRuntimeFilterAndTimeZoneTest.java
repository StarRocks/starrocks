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

package com.starrocks.planner;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TCacheParam;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

// Guards the two cache-key invariants that the query cache relies on:
// 1. nothing below the cache interpolation point may filter rows by something that is decided at
//    run time and is absent from the cache key (a runtime filter the fragment builds for itself,
//    whether by the AggregationNode or by the SortNode above it);
// 2. session variables that change how the BE evaluates the plan must be part of the digest.
public class QueryCacheRuntimeFilterAndTimeZoneTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnableQueryCache(true);
        ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
        ctx.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("qc_rf_db").useDatabase("qc_rf_db");
        // colocate, so that an aggregation carrying a LIMIT stays in the scan fragment and builds a
        // *local* AGG_IN_FILTER -- the shape the alien-GRF check does not catch.
        starRocksAssert.withTable("" +
                "CREATE TABLE t1(\n" +
                "dt DATE NOT NULL,\n" +
                "c1 INT NOT NULL,\n" +
                "ts BIGINT NOT NULL,\n" +
                "v1 BIGINT NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `c1`)\n" +
                "PARTITION BY RANGE(dt) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\", \"colocate_with\" = \"cg_qc_rf\");");
        // non-colocate, so that `group by ... order by ... limit` is planned as a two-phase aggregation
        // and PushDownTopNToPreAggRule can attach the TopN to the local (pre-cache) aggregation.
        starRocksAssert.withTable("" +
                "CREATE TABLE t2(\n" +
                "dt DATE NOT NULL,\n" +
                "c1 INT NOT NULL,\n" +
                "ts BIGINT NOT NULL,\n" +
                "v1 BIGINT NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `c1`)\n" +
                "PARTITION BY RANGE(dt) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\");");
        // Same as t2, but carrying row counts. PushDownTopNToPreAggRule only wins on cost once the
        // table is not empty, and it is that rule firing which moves the partial TopN down into the
        // scan fragment where it can build a filter against the cache point at all.
        starRocksAssert.withTable("" +
                "CREATE TABLE t3(\n" +
                "dt DATE NOT NULL,\n" +
                "c1 INT NOT NULL,\n" +
                "ts BIGINT NOT NULL,\n" +
                "v1 BIGINT NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `c1`)\n" +
                "PARTITION BY RANGE(dt) (\n" +
                "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\");");
        OlapTable t3 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("qc_rf_db").getTable("t3");
        for (Partition partition : t3.getAllPartitions()) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(40);
        }
        // A colocate pair for the join shapes. j1 is made huge and j2 tiny so that the aggregated j1
        // side is chosen as the probe (left) input, which is what puts the aggregation -- and hence the
        // cache interpolation point -- on the leftmost path below the join.
        for (String name : new String[] {"j1", "j2"}) {
            starRocksAssert.withTable("" +
                    "CREATE TABLE " + name + "(\n" +
                    "dt DATE NOT NULL,\n" +
                    "c1 INT NOT NULL,\n" +
                    "v1 BIGINT NOT NULL\n" +
                    ") ENGINE=OLAP\n" +
                    "DUPLICATE KEY(`dt`, `c1`)\n" +
                    "PARTITION BY RANGE(dt) (\n" +
                    "  START (\"2022-01-01\") END (\"2022-02-01\") EVERY (INTERVAL 1 day))\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 4\n" +
                    "PROPERTIES(\"replication_num\" = \"1\", \"colocate_with\" = \"cg_qc_join\");");
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getDb("qc_rf_db").getTable(name);
            long rows = name.equals("j1") ? 10000000L : 20L;
            for (Partition partition : table.getAllPartitions()) {
                partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(rows);
            }
        }
    }

    private Optional<TCacheParam> cacheParamOf(String sql) throws Exception {
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        return plan.getFragments().stream().map(PlanFragment::getCacheParam)
                .filter(java.util.Objects::nonNull).findFirst();
    }

    @Test
    public void testAggBuiltRuntimeFilterDisablesQueryCache() throws Exception {
        // baseline: the very same aggregation without a LIMIT builds no runtime filter and is cacheable.
        Assertions.assertTrue(cacheParamOf(
                        "select c1, sum(v1) from t1 where dt between '2022-01-01' and '2022-02-01' group by c1")
                .isPresent(), "an aggregation that builds no runtime filter must stay cacheable");

        // AGG_IN_FILTER: `group by ... limit n` makes the aggregation build an IN filter that probes
        // the scan feeding the cache point.
        Assertions.assertFalse(cacheParamOf(
                        "select c1, sum(v1) from t1 where dt between '2022-01-01' and '2022-02-01' " +
                                "group by c1 limit 10")
                .isPresent(), "an aggregation building an AGG_IN_FILTER must not be cached");

        Assertions.assertTrue(cacheParamOf(
                        "select c1, sum(v1) from t2 where dt between '2022-01-01' and '2022-02-01' group by c1")
                .isPresent(), "the two-phase baseline must stay cacheable");
    }

    @Test
    public void testTopNBuiltRuntimeFilterDisablesQueryCache() throws Exception {
        // TOPN_FILTER has two mutually exclusive builders, decided by whether PushDownTopNToPreAggRule
        // fired. Here it cannot: c1 is both the distribution key and the group-by key, so the plan is a
        // one-phase aggregation and the rule's TopN -> Agg(GLOBAL) -> Agg(LOCAL) pattern does not match.
        // The filter is then built by the SortNode sitting above the cache interpolation point (its
        // `perPipeline` is false, which is exactly the case SortNode.buildRuntimeFilters handles), and
        // it probes the scan feeding that cache point all the same.
        // This is the shape that escaped the first version of the check, which only looked at
        // AggregationNode; end-to-end coverage lives in
        // test/sql/test_query_cache/T/test_query_cache_topn_filter_stale_entry.
        Assertions.assertFalse(cacheParamOf(
                        "select c1, sum(v1) from t1 where dt between '2022-01-01' and '2022-02-01' " +
                                "group by c1 order by c1 limit 10")
                .isPresent(), "a SortNode building a TOPN_FILTER below the cache point must not be cached");

        // The very same query without the TopN keeps the same cache point and must stay cacheable, so
        // the assertion above cannot pass merely because this shape is uncacheable for other reasons.
        Assertions.assertTrue(cacheParamOf(
                        "select c1, sum(v1) from t1 where dt between '2022-01-01' and '2022-02-01' " +
                                "group by c1")
                .isPresent(), "the one-phase baseline without a TopN must stay cacheable");
    }

    @Test
    public void testTopNFilterLandingOutsideTheCachedSubtreeKeepsTheCache() throws Exception {
        String join = "select a.c1, a.s, j2.v1 from (select c1, sum(v1) s from j1 group by c1) a " +
                "join j2 on a.c1 = j2.c1 ";
        // Baseline: this shape is cacheable, the aggregation sits on the leftmost path below the join.
        Assertions.assertTrue(cacheParamOf(join).isPresent(), "the join baseline must be cacheable");

        // Ordering by a column only the right input has: the TOPN_FILTER can only be probed by the
        // right scan, so no row the cache point produces is affected and the cache must survive.
        Assertions.assertTrue(cacheParamOf(join + "order by j2.v1 limit 10").isPresent(),
                "a TopN filter that cannot probe the cached subtree must not disable the cache");

        // Ordering by the join column instead: equivalence propagates the filter onto the left scan
        // too, which does truncate what gets populated, so this one has to stay uncached.
        Assertions.assertFalse(cacheParamOf(join + "order by a.c1 limit 10").isPresent(),
                "a TopN filter probing the cached subtree must disable the cache");
    }

    @Test
    public void testPreAggTopNRuntimeFilterDisablesQueryCacheUnderEitherPushDownMode() throws Exception {
        // The shape the end-to-end case actually plans on a populated table: with real row counts
        // PushDownTopNToPreAggRule wins on cost and moves the partial TopN into the scan fragment,
        // right on top of the pre-aggregation that is the cache interpolation point. Which node then
        // builds the TOPN_FILTER is decided by topn_push_down_agg_mode:
        //   >= 1  the rule hands the sort info to the aggregation, which builds the filter, and the
        //         SortNode returns early -- this is what the default value plans;
        //   == 0  the rule leaves the aggregation without sort info, so the SortNode builds it instead.
        // Both probe the scan below the cache point and poison its per-tablet entries identically, so
        // neither may leave the fragment cacheable. Only the first was covered before.
        String sql = "select count(*), sum(s), min(s), max(s) from " +
                "(select c1, sum(v1) s from t3 group by c1 order by c1 limit 10) x";
        int originalMode = ctx.getSessionVariable().getTopNPushDownAggMode();
        try {
            for (int mode : new int[] {0, 1}) {
                ctx.getSessionVariable().setEnablePreAggTopNPushDown(mode);
                Assertions.assertFalse(cacheParamOf(sql).isPresent(),
                        "a pre-aggregation TopN filter must not be cached, topn_push_down_agg_mode=" + mode);
            }
        } finally {
            ctx.getSessionVariable().setEnablePreAggTopNPushDown(originalMode);
        }
    }


    @Test
    public void testTimeZoneIsPartOfTheDigest() throws Exception {
        String sql = "select from_unixtime(ts) h, count(*) from t1 " +
                "where dt between '2022-01-01' and '2022-02-01' group by h";
        String originalTimeZone = ctx.getSessionVariable().getTimeZone();
        try {
            ctx.getSessionVariable().setTimeZone("Asia/Shanghai");
            TCacheParam shanghai = cacheParamOf(sql).orElseThrow(() -> new AssertionError("expected a cached plan"));
            byte[] shanghaiDigest = shanghai.getDigest();

            ctx.getSessionVariable().setTimeZone("UTC");
            TCacheParam utc = cacheParamOf(sql).orElseThrow(() -> new AssertionError("expected a cached plan"));

            Assertions.assertFalse(java.util.Arrays.equals(shanghaiDigest, utc.getDigest()),
                    "plans evaluated under different time zones must not share a cache key");

            // ... and the digest is still stable for a fixed time zone.
            ctx.getSessionVariable().setTimeZone("Asia/Shanghai");
            TCacheParam shanghaiAgain = cacheParamOf(sql).orElseThrow(() -> new AssertionError("expected a plan"));
            Assertions.assertArrayEquals(shanghaiDigest, shanghaiAgain.getDigest(),
                    "the digest must stay stable for a fixed time zone");
        } finally {
            ctx.getSessionVariable().setTimeZone(originalTimeZone);
        }
    }
}
