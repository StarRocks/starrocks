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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GlobalLateMaterializeCTEJoinTest extends PlanTestBase {

    private static double savedCteReuseRatio;
    private static boolean savedLateMaterialization;
    private static boolean savedCostBased;

    @BeforeAll
    public static void createFixture() throws Exception {
        StringBuilder columns = new StringBuilder();
        for (int i = 1; i <= 8; i++) {
            columns.append("  `v").append(i).append("` varchar(255) NULL,\n");
        }
        starRocksAssert.withTable("CREATE TABLE `lm_t` (\n"
                + "  `k1` bigint NULL,\n"
                + columns
                + "  `v9` varchar(255) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`)\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");

        savedCteReuseRatio = connectContext.getSessionVariable().getCboCTERuseRatio();
        savedLateMaterialization = connectContext.getSessionVariable().isEnableGlobalLateMaterialization();
        savedCostBased = connectContext.getSessionVariable().isEnableGlobalLateMaterializationCostBased();
        // The defect needs a materialized CTE (not an inlined one) and global late materialization,
        // both of which PlanTestBase turns off for every other test in this package.
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        connectContext.getSessionVariable().setEnableGlobalLateMaterializationCostBased(false);
    }

    @AfterAll
    public static void restoreSession() {
        connectContext.getSessionVariable().setCboCTERuseRatio(savedCteReuseRatio);
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(savedLateMaterialization);
        connectContext.getSessionVariable().setEnableGlobalLateMaterializationCostBased(savedCostBased);
    }

    /**
     * A CTE consumed on both sides of an outer join whose ON clause reads a late-materialized column
     * of the left side. Late materialization used to push the fetch of that column into whichever
     * child merely depended on the same scan -- here the right side, which consumes the same CTE --
     * and then dropped the fetch from the join itself, so the join's own predicate was left reading a
     * column no input produced: "Invalid plan: ... required cols {n} cannot obtain from input cols".
     */
    @Test
    public void testOuterJoinOnLateMaterializedColumnOfSharedCte() throws Exception {
        String sql = "with c1 as (select k1, v1, v2 from lm_t order by k1 limit 5), "
                + "c2 as (select count(*) as n from lm_t), "
                + "c3 as (select y.k1 as k, count(*) as cnt, max(x.v9) as m from lm_t x "
                + "       join c1 y on y.k1 = x.k1 group by y.k1) "
                + "select c2.n, a.k1, a.v1, a.v2, coalesce(z.cnt, 13) as cnt, z.m "
                + "from c1 a cross join c2 left outer join c3 z on a.v1 = a.k1 "
                + "order by a.k1 limit 14";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("NESTLOOP JOIN"), plan);
    }

    /**
     * A CTE consumer carries its own predicate, and nothing told late materialization that the
     * predicate's column has to exist by the time the consumer runs. The column stayed deferred and
     * the fragment builder then could not turn the predicate's column ref into an expression:
     * "Cannot convert ColumnRefOperator to Expr". The requirement has to reach the producer, in
     * producer column ids, because a consumer has no child of its own to fetch from.
     */
    @Test
    public void testCteConsumerPredicateOnLateMaterializedColumn() throws Exception {
        String sql = "with c as (select k1, v1, v2, v3 from lm_t), "
                + "d as (select count(*) as n from c where v2 = 'x') "
                + "select a.k1, a.v1, d.n from c a cross join d order by a.k1 limit 5";
        String plan = getFragmentPlan(sql);
        Assertions.assertTrue(plan.contains("MultiCastDataSinks") || plan.contains("CTE"), plan);
    }
}
