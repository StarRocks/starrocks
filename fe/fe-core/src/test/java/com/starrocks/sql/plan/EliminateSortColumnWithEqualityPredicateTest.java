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

import org.junit.jupiter.api.Test;

public class EliminateSortColumnWithEqualityPredicateTest extends PlanTestBase {

    @Test
    public void testEliminateSortColumns() throws Exception {
        String sql = "select v1, v2, v3  FROM t0 WHERE v2 = 111 \n" +
                "ORDER BY v2, v3 DESC \n" +
                "LIMIT 30;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, " 1:TOP-N\n" +
                "  |  order by: <slot 3> 3: v3 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 30\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: v2 = 111");

        sql = "select v1, v2, v3  FROM t0 WHERE v2 = 111 \n" +
                "ORDER BY v2 DESC \n" +
                "LIMIT 30;";
        plan = getFragmentPlan(sql);
        // The pinned sort column is eliminated, but a GLOBAL limit (gather exchange) must remain so
        // the limit is enforced after the merge instead of only per scan instance. Otherwise the
        // limit applied independently per tablet would inflate downstream consumers (e.g. count(*)).
        assertContains(plan, "  1:EXCHANGE\n" +
                "     limit: 30");
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: v2 = 111\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0");

        sql = "select v1, v2, v3  FROM t0 WHERE v2 = 111 and v3 > 0 \n" +
                "ORDER BY v2, v3 DESC \n" +
                "LIMIT 30;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "1:TOP-N\n" +
                "  |  order by: <slot 3> 3: v3 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 30");

        sql = "select v1, v2, v3  FROM t0 WHERE v2 = 111 or v2 > 0 \n" +
                "ORDER BY v2, v3 DESC \n" +
                "LIMIT 30;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC, <slot 3> 3: v3 DESC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 30\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: v2 = 111) OR (2: v2 > 0), 2: v2 > 0");

        sql = "select v1, v2, v3  FROM t0 WHERE v2 = 111 or v2 = 222 \n" +
                "ORDER BY v2, v3 DESC \n" +
                "LIMIT 30;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 ASC, <slot 3> 3: v3 DESC");

        sql = "select v1, v2, v3  FROM t0 WHERE v2 = 111 or v3 = 222 \n" +
                "ORDER BY v2 DESC \n" +
                "LIMIT 30;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 2> 2: v2 DESC");
    }

    @Test
    public void testCountOverEliminatedSortKeepsGlobalLimit() throws Exception {
        // Regression for the wrong-result bug: count(*) over a derived table whose only ORDER BY
        // column is pinned by an equality predicate. The sort is eliminated; without a GLOBAL limit
        // the limit becomes a per-tablet scan cap and the 2-phase count sums the per-instance counts
        // (e.g. returns 100 instead of 50). The plan must keep a global limit feeding the aggregate.
        String sql = "select count(*) from " +
                "(select v1, v2, v3 from t0 where v2 = 111 order by v2 limit 30) t;";
        String plan = getFragmentPlan(sql);
        // count(*) runs after a global limit gather, not over a per-tablet scan-only limit.
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     limit: 30");
        // the scan still feeds the global limit (no scan-local limit; the gather enforces the bound)
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: v2 = 111");

        // offset variant: the global limit must carry the offset; the gather enforces offset + limit.
        sql = "select count(*) from " +
                "(select v1, v2, v3 from t0 where v2 = 111 order by v2 limit 10, 30) t;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(*)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:EXCHANGE\n" +
                "     offset: 10\n" +
                "     limit: 30");
    }
}
