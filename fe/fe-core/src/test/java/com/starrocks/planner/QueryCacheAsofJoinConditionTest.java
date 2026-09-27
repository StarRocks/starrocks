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
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Objects;

// PlanFragmentBuilder splits an ASOF join's temporal condition out of the ON clause into its own
// field, so it is carried by neither eqJoinConjuncts nor otherJoinConjuncts and nothing normalizes
// it. Two ASOF joins differing only in that condition would share a cache key, and a join inside
// the digested subtree decides which rows the cached per-tablet aggregate is built from -- so a
// fragment containing one is not cached at all.
//
// The two positions behave differently, and this file pins both:
//   - on the leftmost path an ASOF join is rejected, because JoinOperator.isLeftTransform() lists
//     INNER/LEFT_SEMI/LEFT_OUTER/LEFT_ANTI and was never extended to the ASOF operators;
//   - nested in a build side isCacheable() alone decides, because isTransformJoin() is only
//     consulted for joins ON the leftmost path -- and it takes any HashJoinNode. That is the
//     position the guard exists for.
//
// End-to-end coverage lives in test/sql/test_query_cache/T/test_query_cache_asof_join_condition,
// which populates the entries with one variant and reads them back with the other.
public class QueryCacheAsofJoinConditionTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getSessionVariable().setEnableQueryCache(true);
        ctx.getSessionVariable().setOptimizerExecuteTimeout(30000);
        ctx.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
        FeConstants.runningUnitTest = true;
        StarRocksAssert sa = new StarRocksAssert(ctx);
        sa.withDatabase("qc_asof_db").useDatabase("qc_asof_db");
        // Colocate, so an INNER join below the aggregation is a transform join
        // (isLeftTransform() && !areBothSidesShuffled()) and stays cacheable.
        for (String name : new String[] {"c1t", "c2t"}) {
            sa.withTable("" +
                    "CREATE TABLE " + name + "(\n" +
                    "dt DATE NOT NULL,\n" +
                    "c1 INT NOT NULL,\n" +
                    "ts BIGINT NOT NULL,\n" +
                    "v1 BIGINT NOT NULL\n" +
                    ") ENGINE=OLAP\n" +
                    "DUPLICATE KEY(`dt`, `c1`)\n" +
                    "PARTITION BY RANGE(dt) (\n" +
                    "  START (\"2022-01-01\") END (\"2022-02-01\") EVERY (INTERVAL 1 day))\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 10\n" +
                    "PROPERTIES(\"replication_num\" = \"1\", \"colocate_with\" = \"cg_qc_asof\");");
        }
    }

    private boolean isCacheable(String sql) throws Exception {
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        return plan.getFragments().stream().map(PlanFragment::getCacheParam).anyMatch(Objects::nonNull);
    }

    private static final String AGG = "select a.c1, sum(a.v1) s from c1t a ";

    @Test
    public void testAsofJoinInABuildSideMakesTheFragmentUncacheable() throws Exception {
        // The reachable position: nested in the right subtree, where isTransformJoin() is not
        // consulted and isCacheable() alone decides. Without the guard this fragment is cacheable
        // and the temporal condition is absent from its digest, so `>=` and `>` share an entry.
        String outer = AGG + "join (select c.v1 as k from c2t x asof left join c2t c "
                + "on x.c1 = c.c1 and x.ts %s c.ts) b on a.ts > b.k group by a.c1";
        Assertions.assertFalse(isCacheable(String.format(outer, ">=")),
                "an ASOF join in a build side reaches the digested subtree, so the fragment must "
                        + "not be cached");
        Assertions.assertFalse(isCacheable(String.format(outer, ">")),
                "same for the strict operator");
    }

    @Test
    public void testAsofJoinOnTheLeftmostPathStaysUncacheable() throws Exception {
        // Pins the other half of the reachability story. Should isLeftTransform() ever be widened
        // to the ASOF operators -- the omission looks accidental, since isOuterJoin(),
        // isInnerJoin() and isLeftOuterJoin() were all made ASOF-aware -- this test fails and
        // points at the field that already makes the widening safe.
        String sql = AGG + "asof join c2t b on a.c1 = b.c1 and a.ts >= b.ts group by a.c1";
        Assertions.assertFalse(isCacheable(sql),
                "an ASOF join on the leftmost path is not a transform join, so the fragment must "
                        + "stay uncacheable; if this changed, verify the ASOF condition is in the digest");
    }
}
