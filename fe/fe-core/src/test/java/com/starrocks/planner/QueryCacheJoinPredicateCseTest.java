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

import java.util.Objects;
import java.util.Optional;

// A join predicate's common sub-expressions are factored out into JoinNode.commonSlotMap, and the
// join conjuncts then reference them by slot id alone. If the digest does not carry the definitions,
// two joins whose predicates differ only inside a factored-out subexpression share a cache key --
// and a JoinNode below the aggregation is inside the cached subtree, so it decides which rows the
// cached per-tablet aggregate is computed from. The result is that whichever query runs first
// populates the entries and the second one silently inherits its answer.
//
// Three things have to line up for this to be reachable, and getting any of them wrong makes the
// bug invisible rather than absent:
//   1. the join must sit BELOW the aggregation (above it, it is outside the digested subtree);
//   2. both bounds must reference the other side of the join. With literal bounds the whole
//      predicate is pushed into the right scan and never becomes a join predicate at all, so the
//      subexpression reaches the digest through the scan's conjuncts and the digests differ for a
//      reason that has nothing to do with commonSlotMap;
//   3. the subexpression must appear twice, or it is never factored out into commonSlotMap.
//
// End-to-end coverage lives in test/sql/test_query_cache/T/test_query_cache_join_predicate_cse.
public class QueryCacheJoinPredicateCseTest {
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
        sa.withDatabase("qc_cse_db").useDatabase("qc_cse_db");
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
                    "PROPERTIES(\"replication_num\" = \"1\", \"colocate_with\" = \"cg_qc_cse\");");
        }
    }

    private byte[] digestOf(String sql) throws Exception {
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(ctx, sql).second;
        Optional<TCacheParam> param = plan.getFragments().stream().map(PlanFragment::getCacheParam)
                .filter(Objects::nonNull).findFirst();
        return param.orElseThrow(() -> new AssertionError("expected a cacheable plan for: " + sql)).getDigest();
    }

    private static final String AGG = "select a.c1, sum(a.v1) s from c1t a ";

    @Test
    public void testNestLoopJoinPredicateCseIsPartOfTheDigest() throws Exception {
        // No equi conjunct -> NestLoopJoinNode, which isTransformJoin() accepts unconditionally,
        // so this is the shape with the widest reach.
        String mod7 = AGG + "join c2t b on (b.ts % 7) > a.ts and (b.ts % 7) < a.v1 group by a.c1";
        String mod9 = AGG + "join c2t b on (b.ts % 9) > a.ts and (b.ts % 9) < a.v1 group by a.c1";

        Assertions.assertFalse(java.util.Arrays.equals(digestOf(mod7), digestOf(mod9)),
                "two nest loop joins whose predicate CSE differs must not share a cache key");

        // Guard against the assertion passing for the wrong reason: the same shape with an
        // unchanged predicate must still be stable.
        Assertions.assertArrayEquals(digestOf(mod7), digestOf(mod7),
                "the digest must be stable for an unchanged plan");
    }

    @Test
    public void testHashJoinPredicateCseIsPartOfTheDigest() throws Exception {
        // Equi conjunct -> HashJoinNode. Bounds still reference the left side so the residual
        // predicate cannot be pushed into the right scan.
        String mod7 = AGG + "join c2t b on a.c1 = b.c1 and (b.ts % 7) > a.ts and (b.ts % 7) < a.v1 group by a.c1";
        String mod9 = AGG + "join c2t b on a.c1 = b.c1 and (b.ts % 9) > a.ts and (b.ts % 9) < a.v1 group by a.c1";

        Assertions.assertFalse(java.util.Arrays.equals(digestOf(mod7), digestOf(mod9)),
                "two hash joins whose predicate CSE differs must not share a cache key");
    }

}
