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

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// An aggregation on top of a REPEAT node must be multi-stage, so invalidOneStageAggCost gives the
// one-stage plan an infinite cost. When such an aggregation also consumes a shuffle it did not ask
// for itself (source type is not SHUFFLE_AGG), redundantTwoStageAggCost used to give the two-stage
// plan an infinite cost as well, assuming a one-stage alternative existed. With broadcast forbidden
// by the row count limit, the group ended up with no viable plan and planning failed.
public class GroupingSetsJoinAggPlanTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setBroadcastRowCountLimit(15000000);
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    // Plans the query with broadcast forbidden, and asserts the aggregation over REPEAT is planned
    // as a multi-stage aggregation.
    private void assertMultiStageAggOverRepeat(String sql) throws Exception {
        connectContext.getSessionVariable().setBroadcastRowCountLimit(1);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "REPEAT_NODE");
        assertContains(plan, "AGGREGATE (merge finalize)");
    }

    @Test
    public void testGroupingSetsAggUnderLeftJoin() throws Exception {
        assertMultiStageAggOverRepeat("select a.k1, a.k2, a.s1, b.s2 from "
                + " (select l_orderkey k1, coalesce(l_linenumber, 0) k2, sum(l_quantity) s1 from lineitem "
                + "  group by grouping sets((l_orderkey, l_linenumber), (l_orderkey))) a "
                + " left join (select o_orderkey k1, coalesce(o_custkey, 0) k2, sum(o_totalprice) s2 from orders "
                + "  group by grouping sets((o_orderkey, o_custkey), (o_orderkey))) b "
                + " on a.k1 = b.k1 and a.k2 = b.k2");
    }

    // The defect does not depend on the join type.
    @Test
    public void testGroupingSetsAggUnderInnerJoin() throws Exception {
        assertMultiStageAggOverRepeat("select a.k1, a.k2, a.s1, b.s2 from "
                + " (select l_orderkey k1, coalesce(l_linenumber, 0) k2, sum(l_quantity) s1 from lineitem "
                + "  group by grouping sets((l_orderkey, l_linenumber), (l_orderkey))) a "
                + " join (select o_orderkey k1, coalesce(o_custkey, 0) k2, sum(o_totalprice) s2 from orders "
                + "  group by grouping sets((o_orderkey, o_custkey), (o_orderkey))) b "
                + " on a.k1 = b.k1 and a.k2 = b.k2");
    }

    // Only one side aggregates: the aggregation is joined against a plain table.
    @Test
    public void testGroupingSetsAggJoinedWithTable() throws Exception {
        assertMultiStageAggOverRepeat("select a.k1, a.k2, a.s1, o.o_totalprice from "
                + " (select l_orderkey k1, coalesce(l_linenumber, 0) k2, sum(l_quantity) s1 from lineitem "
                + "  group by grouping sets((l_orderkey, l_linenumber), (l_orderkey))) a "
                + " left join orders o on a.k1 = o.o_orderkey and a.k2 = o.o_custkey");
    }

    // The aggregation's input distribution comes from a join below it, not from the table itself.
    @Test
    public void testGroupingSetsAggOverJoinInput() throws Exception {
        assertMultiStageAggOverRepeat("select a.k1, a.k2, a.s1, b.s2 from "
                + " (select k1, coalesce(k2, 0) k2, sum(v) s1 from "
                + "   (select l_orderkey k1, o_custkey k2, l_quantity v from lineitem join orders "
                + "     on l_orderkey = o_orderkey) t "
                + "  group by grouping sets((k1, k2), (k1))) a "
                + " left join (select o_orderkey k1, coalesce(o_custkey, 0) k2, sum(o_totalprice) s2 from orders "
                + "  group by grouping sets((o_orderkey, o_custkey), (o_orderkey))) b "
                + " on a.k1 = b.k1 and a.k2 = b.k2");
    }

    // Three aggregations joined together.
    @Test
    public void testGroupingSetsAggThreeWayJoin() throws Exception {
        assertMultiStageAggOverRepeat("select a.k1, a.k2, a.s1, b.s2, c.s3 from "
                + " (select l_orderkey k1, coalesce(l_linenumber, 0) k2, sum(l_quantity) s1 from lineitem "
                + "  group by grouping sets((l_orderkey, l_linenumber), (l_orderkey))) a "
                + " left join (select o_orderkey k1, coalesce(o_custkey, 0) k2, sum(o_totalprice) s2 from orders "
                + "  group by grouping sets((o_orderkey, o_custkey), (o_orderkey))) b "
                + " on a.k1 = b.k1 and a.k2 = b.k2 "
                + " left join (select ps_partkey k1, coalesce(ps_suppkey, 0) k2, sum(ps_availqty) s3 from partsupp "
                + "  group by grouping sets((ps_partkey, ps_suppkey), (ps_partkey))) c "
                + " on a.k1 = c.k1 and a.k2 = c.k2");
    }

    // Shape of the reported query: several joins and a window function below the aggregation, one
    // group-by key carried from the scan and one derived by an expression, and the outer join on
    // both of them.
    @Test
    public void testGroupingSetsAggWithDerivedKeyAndWindow() throws Exception {
        assertMultiStageAggOverRepeat("select t.k, t.seg, t.m, r.m2 from "
                + " (select k, seg, sum(amt) m from "
                + "   (select l_orderkey k, coalesce(substr(c_mktsegment, 1, 4), 'ALL') seg, "
                + "           sum(l_extendedprice) over (partition by l_orderkey) amt "
                + "    from lineitem join orders on l_orderkey = o_orderkey "
                + "                  join customer on o_custkey = c_custkey) s "
                + "  group by grouping sets((k, seg), (k))) t "
                + " left join "
                + " (select k, seg, sum(price) m2 from "
                + "   (select o_orderkey k, coalesce(substr(c_mktsegment, 1, 4), 'ALL') seg, o_totalprice price "
                + "    from orders join customer on o_custkey = c_custkey) s2 "
                + "  group by grouping sets((k, seg), (k))) r "
                + " on t.k = r.k and t.seg = r.seg");
    }

    // The join's distribution requirement reaches the aggregation through a window operator.
    @Test
    public void testGroupingSetsAggUnderWindowAndJoin() throws Exception {
        assertMultiStageAggOverRepeat("select x.k1, x.w, b.s2 from "
                + " (select k1, k2, sum(s1) over (partition by k1) w from "
                + "   (select l_orderkey k1, coalesce(l_linenumber, 0) k2, sum(l_quantity) s1 from lineitem "
                + "    group by grouping sets((l_orderkey, l_linenumber), (l_orderkey))) a) x "
                + " left join (select o_orderkey k1, coalesce(o_custkey, 0) k2, sum(o_totalprice) s2 from orders "
                + "  group by grouping sets((o_orderkey, o_custkey), (o_orderkey))) b "
                + " on x.k1 = b.k1 and x.k2 = b.k2");
    }

    // A semi join above the aggregation pins the property the same way an outer join does.
    @Test
    public void testGroupingSetsAggUnderSemiJoin() throws Exception {
        assertMultiStageAggOverRepeat("select k1, k2, s1 from "
                + " (select l_orderkey k1, coalesce(l_linenumber, 0) k2, sum(l_quantity) s1 from lineitem "
                + "  group by grouping sets((l_orderkey, l_linenumber), (l_orderkey))) a "
                + " where exists (select 1 from "
                + "   (select o_orderkey k1, coalesce(o_custkey, 0) k2 from orders "
                + "    group by grouping sets((o_orderkey, o_custkey), (o_orderkey))) b "
                + "  where a.k1 = b.k1 and a.k2 = b.k2)");
    }

    // The aggregation's input distribution is produced for a window operator below it, not by the
    // table's own bucketing: lineitem is bucketed by l_orderkey while this query groups by l_partkey.
    @Test
    public void testGroupingSetsAggOverWindowDistribution() throws Exception {
        assertMultiStageAggOverRepeat("select x.k1, x.s1, b.s2 from "
                + " (select k1, coalesce(k2, 0) k2, sum(v) s1 from "
                + "   (select l_partkey k1, l_suppkey k2, l_quantity v, "
                + "           sum(l_extendedprice) over (partition by l_partkey) w from lineitem) t "
                + "  group by grouping sets((k1, k2), (k1))) x "
                + " left join (select ps_partkey k1, coalesce(ps_suppkey, 0) k2, sum(ps_availqty) s2 from partsupp "
                + "  group by grouping sets((ps_partkey, ps_suppkey), (ps_partkey))) b "
                + " on x.k1 = b.k1 and x.k2 = b.k2");
    }

    // The aggregation is produced by a CTE and consumed by the join.
    @Test
    public void testGroupingSetsAggInCte() throws Exception {
        assertMultiStageAggOverRepeat("with c as "
                + " (select l_orderkey k1, coalesce(l_linenumber, 0) k2, sum(l_quantity) s1 from lineitem "
                + "  group by grouping sets((l_orderkey, l_linenumber), (l_orderkey))) "
                + "select c.k1, c.s1, b.s2 from c "
                + " left join (select o_orderkey k1, coalesce(o_custkey, 0) k2, sum(o_totalprice) s2 from orders "
                + "  group by grouping sets((o_orderkey, o_custkey), (o_orderkey))) b "
                + " on c.k1 = b.k1 and c.k2 = b.k2");
    }

    // An aggregation that can be one-stage keeps the redundant-two-stage penalty: its input is
    // already distributed by the group-by key, so the local aggregation would save nothing.
    @Test
    public void testOneStageAggStillPreferred() throws Exception {
        connectContext.getSessionVariable().setBroadcastRowCountLimit(1);
        String plan = getFragmentPlan("select a.k1, a.s1, b.s2 from "
                + " (select l_orderkey k1, sum(l_quantity) s1 from lineitem group by l_orderkey) a "
                + " left join (select o_orderkey k1, sum(o_totalprice) s2 from orders group by o_orderkey) b "
                + " on a.k1 = b.k1");
        assertContains(plan, "AGGREGATE (update finalize)");
        assertNotContains(plan, "AGGREGATE (merge finalize)");
    }
}
