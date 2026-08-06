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

import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
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
//    run time and is absent from the cache key (a runtime filter built by the aggregation itself);
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
        // The other filter an aggregation can build, TOPN_FILTER, goes through the very same branch.
        // It is not asserted here because PushDownTopNToPreAggRule needs table statistics this test
        // environment does not have; it is covered end-to-end by
        // test/sql/test_query_cache/T/test_query_cache_topn_filter_stale_entry.
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
