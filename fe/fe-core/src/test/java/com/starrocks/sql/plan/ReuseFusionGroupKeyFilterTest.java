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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// ReuseFusionPlanRule over nullable columns: pieces that differ only by conjuncts shared by all of them or by
// conjuncts on group-by keys keep plain aggregates, the key conjuncts move to the piece's CTE consumer.
public class ReuseFusionGroupKeyFilterTest extends PlanTestBase {
    private static double cteReuseRatio;
    private static boolean ruleDebug;
    private static boolean extractCommonPlan;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        cteReuseRatio = connectContext.getSessionVariable().getCboCTERuseRatio();
        ruleDebug = connectContext.getSessionVariable().enableOptimizerRuleDebug();
        extractCommonPlan = connectContext.getSessionVariable().isCboExtractCommonPlan();
        connectContext.getSessionVariable().setCboExtractCommonPlan(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        // validates the input columns of every operator after each rule
        connectContext.getSessionVariable().setEnableOptimizerRuleDebug(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableOptimizerRuleDebug(ruleDebug);
        connectContext.getSessionVariable().setCboExtractCommonPlan(extractCommonPlan);
        connectContext.getSessionVariable().setCboCTERuseRatio(cteReuseRatio);
    }

    private static String unionOf(String... pieces) {
        return "select * from (" + String.join(" union all ", pieces) + ") t";
    }

    @Test
    public void testQuery65Shape() throws Exception {
        // TPC-DS Q65: the two copies of one subquery differ only by a NOT NULL on a group-by key
        String plan = getFragmentPlan("select a.v1, a.s from " +
                "(select v1, v2, sum(v3) s from t0 where v2 is not null group by v1, v2) a, " +
                "(select v1, avg(s) av from (select v1, v2, sum(v3) s from t0 group by v1, v2) x group by v1) b, t1 " +
                "where a.v1 = b.v1 and a.v2 = t1.v4 and a.s < b.av");
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(15: v3)\n" +
                "  |  group by: 13: v1, 14: v2");
        // the conjunct shared by both pieces is evaluated by the scan
        assertContains(plan, "PREDICATES: 13: v1 IS NOT NULL");
        assertContains(plan, "9:SELECT\n" +
                "  |  predicates: 2: v2 IS NOT NULL");
        assertNotContains(plan, "row_hit");
        assertNotContains(plan, "_if(");
    }

    @Test
    public void testGroupKeyPredicate() throws Exception {
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 group by v1",
                "select v1, sum(v3) from t0 where v1 > 100 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum(13: v3)");
        assertContains(plan, "predicates: 5: v1 > 100");
        assertNotContains(plan, "row_hit");
        assertNotContains(plan, "_if(");
    }

    @Test
    public void testGroupByOnlyPiecesWithKeyPredicateNotFused() throws Exception {
        // main does not fuse group-by-only pieces whose filters differ; taking the key-only conjunct out must not
        // start fusing them, or the second piece's v1 = 1 no longer reaches its own scan
        String plan = getFragmentPlan(unionOf(
                "select v1 from t0 group by v1",
                "select v1 from t0 where v1 = 1 group by v1"));
        assertNotContains(plan, "MultiCastDataSinks");
        assertContains(plan, "PREDICATES: 4: v1 = 1");
    }

    @Test
    public void testConstantLabelledPieces() throws Exception {
        // a constant column that tells the pieces apart sits above the aggregate and does not block the fusion
        String plan = getFragmentPlan(unionOf(
                "select 'a' p, v1, sum(v3) s from t0 group by v1",
                "select 'b', v1, sum(v3) from t0 where v1 > 100 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "predicates: 6: v1 > 100");
        assertNotContains(plan, "row_hit");
        assertNotContains(plan, "_if(");
    }

    @Test
    public void testGroupKeyNullPredicates() throws Exception {
        // NULL keys form their own group, IS NULL holds for all of its rows or none
        String plan = getFragmentPlan(unionOf(
                "select v1, count(*) from t0 group by v1",
                "select v1, count(*) from t0 where v1 is null group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: count(*)");
        assertContains(plan, "predicates: 5: v1 IS NULL");
        assertNotContains(plan, "row_hit");
        assertNotContains(plan, "_if(");

        // an expression over a key that can be UNKNOWN, combined with OR
        plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 group by v1",
                "select v1, sum(v3) from t0 where v1 + 1 > 100 or v1 is null group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum(13: v3)");
        assertContains(plan, "predicates: (5: v1 > 99) OR (5: v1 IS NULL)");
        assertNotContains(plan, "row_hit");
        assertNotContains(plan, "_if(");
    }

    @Test
    public void testGroupKeyOrderDiffers() throws Exception {
        String plan = getFragmentPlan(unionOf(
                "select v1, v2, sum(v3) from t0 group by v1, v2",
                "select v1, v2, sum(v3) from t0 where v2 > 100 group by v2, v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum(14: v3)");
        assertContains(plan, "predicates: 6: v2 > 100");
        assertNotContains(plan, "row_hit");
        assertNotContains(plan, "_if(");
    }

    @Test
    public void testConsumerDoesNotOutputGroupKey() throws Exception {
        // the consumer only selects the aggregate, the gate adds its own mapping for the key
        String plan = getFragmentPlan(unionOf(
                "select s from (select v1, sum(v3) s from t0 group by v1) a",
                "select s from (select v1, sum(v3) s from t0 where v1 > 100 group by v1) b"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum(12: v3)");
        assertContains(plan, "predicates: 5: v1 > 100\n");
        assertContains(plan, "  |  <slot 5> : 10: v1\n" +
                "  |  <slot 8> : 13: sum");
        assertNotContains(plan, "row_hit");
    }

    @Test
    public void testNonDeterministicGroupKeyPredicate() throws Exception {
        // rand() is not constant within a group, so it stays a per-row filter
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 group by v1",
                "select v1, sum(v3) from t0 where v1 > rand() group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum_if(16: v3, 15: expr), any_value_if(TRUE, 15: expr), sum(13: v3)");
        assertContains(plan, "<slot 15> : CAST(11: v1 AS DOUBLE) > rand()");
        assertContains(plan, "predicates: 19: row_hit IS NOT NULL");
    }

    @Test
    public void testUnboundedStateKeepsRowFilter() throws Exception {
        // array_agg's state grows with the rows: applying v1 = 1 on the consumer would build it for every group
        String plan = getFragmentPlan(unionOf(
                "select v1, max(v3), array_agg(v3) from t0 group by v1",
                "select v1, max(v3), array_agg(v3) from t0 where v1 = 1 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: max(16: v3), array_agg(16: v3), max_if(20: v3, 19: expr), " +
                "array_agg_if(20: v3, 19: expr), any_value_if(TRUE, 19: expr)");
        assertContains(plan, "<slot 19> : 14: v1 = 1");
        assertContains(plan, "predicates: 24: row_hit IS NOT NULL");
        assertNotContains(plan, "predicates: 6: v1 = 1");
    }

    @Test
    public void testVariableLengthValueStateKeepsRowFilter() throws Exception {
        // max(VARCHAR) keeps one string per group: applying v1 > 100 on the consumer would store it for every group
        String plan = getFragmentPlan(unionOf(
                "select v1, max(cast(v3 as varchar)) from t0 group by v1",
                "select v1, max(cast(v3 as varchar)) from t0 where v1 > 100 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: max(16: cast), max_if(20: cast, 19: expr), any_value_if(TRUE, 19: expr)");
        assertContains(plan, "<slot 19> : 13: v1 > 100");
        assertContains(plan, "predicates: 23: row_hit IS NOT NULL");

        // fixed-width min/max stay plain
        plan = getFragmentPlan(unionOf(
                "select v1, max(v3), min(v2) from t0 group by v1",
                "select v1, max(v3), min(v2) from t0 where v1 > 100 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: max(16: v3), min(15: v2)");
        assertContains(plan, "predicates: 6: v1 > 100");
        assertNotContains(plan, "row_hit");
    }

    @Test
    public void testNonDeterministicAggregateArgument() throws Exception {
        // each piece evaluates its own rand(): the two sums are not merged into one
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3 + rand()) from t0 group by v1",
                "select v1, sum(v3 + rand()) from t0 where v1 > 100 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum(16: expr), sum(17: expr)");
        assertContains(plan, "<slot 16> : 20: cast + rand()");
        assertContains(plan, "<slot 17> : 20: cast + rand()");
    }

    @Test
    public void testDuplicateNonDeterministicConjuncts() throws Exception {
        // rand() > 0.5 AND rand() > 0.5 are two independent evaluations and must both survive
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 where v1 > 100 group by v1",
                "select v1, sum(v3) from t0 where rand() > 0.5 and rand() > 0.5 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum_if(16: v3, 15: expr), any_value_if(TRUE, 15: expr), sum(13: v3)");
        assertContains(plan, "<slot 15> : (rand() > 0.5) AND (rand() > 0.5)");
        assertNotContains(plan, "PREDICATES");
    }

    @Test
    public void testNonGroupKeyPredicate() throws Exception {
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 group by v1",
                "select v1, sum(v3) from t0 where v3 > 10 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum_if(16: v3, 15: expr), any_value_if(TRUE, 15: expr), sum(13: v3)");
        assertContains(plan, "<slot 15> : 13: v3 > 10");
        assertContains(plan, "predicates: 19: row_hit IS NOT NULL");
    }

    @Test
    public void testMixedGroupKeyAndRowPredicate() throws Exception {
        // only the non-key part stays in the aggregate's filter, the key part gates the consumer
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 group by v1",
                "select v1, sum(v3) from t0 where v1 > 100 and v3 > 10 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum_if(16: v3, 15: expr), any_value_if(TRUE, 15: expr), sum(13: v3)");
        assertContains(plan, "<slot 15> : 13: v3 > 10");
        assertContains(plan, "predicates: 5: v1 > 100, 19: row_hit IS NOT NULL");
    }

    @Test
    public void testThreePiecesWithCommonPredicate() throws Exception {
        // no residual / key-only residual / row residual, all on top of a shared conjunct
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 where v2 = 5 group by v1",
                "select v1, sum(v3) from t0 where v2 = 5 and v1 > 100 group by v1",
                "select v1, sum(v3) from t0 where v2 = 5 and v3 > 10 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        // the first two pieces share one plain sum
        assertContains(plan, "output: sum(17: v3), sum_if(20: v3, 19: expr), any_value_if(TRUE, 19: expr)");
        assertContains(plan, "<slot 19> : 17: v3 > 10");
        assertContains(plan, "PREDICATES: 16: v2 = 5\n");
        assertContains(plan, "predicates: 5: v1 > 100\n");
        assertContains(plan, "predicates: 23: row_hit IS NOT NULL");
    }

    @Test
    public void testInnerJoinPieces() throws Exception {
        // the key conjuncts on t0 gate the consumers, the ones on t1 are not on a group-by key
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v5) from t0 join t1 on v1 = v4 group by v1",
                "select v1, sum(v5) from t0 join t1 on v1 = v4 where v1 > 100 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: sum_if(24: v5, 23: expr), any_value_if(TRUE, 23: expr), " +
                "sum_if(24: v5, 27: expr), any_value_if(TRUE, 27: expr)");
        assertContains(plan, "<slot 23> : 20: v4 IS NOT NULL");
        assertContains(plan, "<slot 27> : 20: v4 > 100");
        assertContains(plan, "predicates: 1: v1 IS NOT NULL, 30: row_hit IS NOT NULL");
        assertContains(plan, "predicates: 8: v1 > 100, 31: row_hit IS NOT NULL");
    }

    @Test
    public void testOuterJoinPiecesNotFused() throws Exception {
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v5) from t0 left join t1 on v1 = v4 group by v1",
                "select v1, sum(v5) from t0 left join t1 on v1 = v4 where v1 > 100 group by v1"));
        assertNotContains(plan, "MultiCastDataSinks");
    }

    @Test
    public void testNonDeterministicCommonPredicate() throws Exception {
        // a non-deterministic conjunct is neither hoisted into the scan as a shared conjunct nor evaluated by the
        // scan through the OR of the residuals: each piece evaluates its own copy once
        String plan = getFragmentPlan(unionOf(
                "select v1, sum(v3) from t0 where v3 > rand() group by v1",
                "select v1, sum(v3) from t0 where v3 > rand() and v1 > 100 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "output: any_value_if(TRUE, 14: expr), sum_if(15: v3, 18: expr), " +
                "any_value_if(TRUE, 18: expr), sum_if(15: v3, 14: expr)");
        assertContains(plan, "<slot 14> : 23: cast > rand()");
        assertContains(plan, "<slot 18> : 23: cast > rand()");
        assertNotContains(plan, "PREDICATES");
        assertContains(plan, "predicates: 5: v1 > 100, 22: row_hit IS NOT NULL");
    }

    // the key comes from the dimension side of an inner join inside each piece
    @Test
    public void testDimensionKeyPredicate() throws Exception {
        String plan = getFragmentPlan(unionOf(
                "select v6, sum(v3) from t0 join t1 on v1 = v4 where v6 = 2001 group by v6",
                "select v6, sum(v3) from t0 join t1 on v1 = v4 where v6 = 2002 group by v6"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "PREDICATES: 22: v6 IN (2002, 2001)");
        assertContains(plan, "predicates: 6: v6 = 2001");
        assertContains(plan, "predicates: 13: v6 = 2002");
        assertNotContains(plan, "_if(");
        assertNotContains(plan, "row_hit");
    }

    // a dictionary-encoded string key: the group gate on the consumer must still produce a valid plan
    @Test
    public void testLowCardinalityKeyPredicate() throws Exception {
        boolean mock = FeConstants.USE_MOCK_DICT_MANAGER;
        boolean lowCard = connectContext.getSessionVariable().isEnableLowCardinalityOptimize();
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        try {
            String plan = getVerboseExplain(unionOf(
                    "select t1a, sum(t1c) from test_all_type group by t1a",
                    "select t1a, sum(t1c) from test_all_type where t1a = 'a' group by t1a"));
            assertContains(plan, "MultiCastDataSinks");
            assertContains(plan, "predicates: 12: t1a = 'a'");
            assertNotContains(plan, "_if(");
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = mock;
            connectContext.getSessionVariable().setEnableLowCardinalityOptimize(lowCard);
        }
    }

    // group-by-only pieces whose filters differ only in conjunct order have the same filter and are fused
    @Test
    public void testGroupByOnlyPiecesWithReorderedConjuncts() throws Exception {
        String plan = getFragmentPlan(unionOf(
                "select v1 from t0 where v2 > 1 and v3 < 5 group by v1",
                "select v1 from t0 where v3 < 5 and v2 > 1 group by v1"));
        assertContains(plan, "MultiCastDataSinks");
        assertContains(plan, "PREDICATES: 9: v2 > 1, 10: v3 < 5");
        assertNotContains(plan, "row_hit");
    }

    // 0.0 and -0.0 compare equal but differ under 1 / k or a cast to string, and NaN never equals itself, so a
    // conjunct on a floating-point key is not constant within its group: keep it as a per-row filter
    @Test
    public void testFloatingPointKeyPredicateKeepsRowFilter() throws Exception {
        String plan = getFragmentPlan(unionOf(
                "select t1f, sum(t1c) from test_all_type group by t1f",
                "select t1f, sum(t1c) from test_all_type where t1f = 0 group by t1f"));
        assertContains(plan, "sum_if(");
        assertContains(plan, "row_hit IS NOT NULL");
    }
}
