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

import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.RangeDistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * F2 follow-up (colocate.md): mixed range-colocate × hash JOIN must not surface
 * as MySQL "Unknown error" — the optimizer should plan a shuffle/bucket join.
 *
 * <p>The load-bearing fix is in {@code ChildOutputPropertyGuarantor.convertRangeToHashShuffle},
 * which now publishes the range→hash enforcer back into {@code childrenBestExprList}
 * so downstream bucket/shuffle enforcement can read it.  The deriver throws a
 * descriptive {@code StarRocksPlannerException} if a mixed pair ever reaches it.
 */
public class RangeColocateMixedJoinPlanTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistributionConfig;
    private static boolean savedEnableRangeDistributionSessionVar;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        savedEnableRangeDistributionConfig = Config.enable_range_distribution;
        savedEnableRangeDistributionSessionVar = connectContext.getSessionVariable().isEnableRangeDistribution();
        Config.enable_range_distribution = true;
        connectContext.getSessionVariable().setEnableRangeDistribution(true);

        starRocksAssert.withDatabase("f2_mixed_join").useDatabase("f2_mixed_join");

        starRocksAssert.withTable(
                "create table cd (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'f2_grp:k1');");
        starRocksAssert.withTable(
                "create table cd2 (k1 int, k2 int)\n"
                        + "order by(k1, k2)\n"
                        + "properties('replication_num' = '1', 'colocate_with' = 'f2_grp:k1');");
        starRocksAssert.withTable(
                "create table dim (k1 int, x int)\n"
                        + "distributed by hash(k1) buckets 3\n"
                        + "properties('replication_num' = '1');");
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = savedEnableRangeDistributionConfig;
        if (connectContext != null) {
            connectContext.getSessionVariable().setEnableRangeDistribution(savedEnableRangeDistributionSessionVar);
        }
    }

    @BeforeEach
    public void resetSessionState() {
        Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", false);
    }

    // ---------- helpers ----------

    private String plan(String sql) throws Exception {
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }

    private void assertContainsMarkerNotColocate(String sql, String marker) throws Exception {
        String fragmentPlan = plan(sql);
        Assertions.assertTrue(fragmentPlan.contains(marker),
                () -> "expected plan to contain `" + marker + "` but got:\n" + fragmentPlan);
        Assertions.assertFalse(fragmentPlan.contains("colocate: true"),
                () -> "mixed range/hash join must not be a colocate join, plan was:\n" + fragmentPlan);
    }

    /** For join variants where the optimizer may commute the join direction. */
    private void assertPlansAsHashJoin(String sql) throws Exception {
        assertContainsMarkerNotColocate(sql, "HASH JOIN");
    }

    // ---------- T1–T12 ----------

    /** T1: F2 repro. cd (range-colocate) ⨝ dim (hash) on the colocate key. */
    @Test
    public void t1_rangeJoinHashOnColocateKey() throws Exception {
        // Optimizer chooses dim (LOCAL hash) on the left and cd (range→shuffle)
        // on the right, producing a bucket-shuffle join.
        assertContainsMarkerNotColocate(
                "select count(*) from cd join dim on cd.k1 = dim.k1",
                "INNER JOIN (BUCKET_SHUFFLE)");
    }

    /** T2: Hash ⨝ Range, explicit ordering matches the bucket-shuffle path. */
    @Test
    public void t2_hashJoinRangeBucketShuffle() throws Exception {
        assertContainsMarkerNotColocate(
                "select count(*) from dim join cd on dim.k1 = cd.k1",
                "INNER JOIN (BUCKET_SHUFFLE)");
    }

    /** T3: Range ⨝ Hash on non-colocate key — must still plan, no colocate. */
    @Test
    public void t3_rangeJoinHashOnNonColocateKey() throws Exception {
        assertContainsMarkerNotColocate(
                "select count(*) from cd join dim on cd.k2 = dim.k1",
                "INNER JOIN");
    }

    /** T4: Range ⨝ Range colocate (regression — must still pick colocate). */
    @Test
    public void t4_rangeJoinRangeStillColocate() throws Exception {
        String fragmentPlan = plan("select count(*) from cd a join cd2 b on a.k1 = b.k1");
        Assertions.assertTrue(fragmentPlan.contains("colocate: true"),
                () -> "range×range join on colocate key must remain colocate, plan was:\n" + fragmentPlan);
    }

    /** T5: LEFT OUTER variant of T1 — optimizer may commute, just assert it plans. */
    @Test
    public void t5_rangeLeftOuterHash() throws Exception {
        assertPlansAsHashJoin(
                "select count(*) from cd left outer join dim on cd.k1 = dim.k1");
    }

    /** T6: LEFT SEMI variant. IN-subquery may rewrite to INNER JOIN; markers loose. */
    @Test
    public void t6_rangeLeftSemiHash() throws Exception {
        assertPlansAsHashJoin(
                "select count(*) from cd where cd.k1 in (select dim.k1 from dim)");
    }

    /** T7: LEFT ANTI variant. */
    @Test
    public void t7_rangeLeftAntiHash() throws Exception {
        assertPlansAsHashJoin(
                "select count(*) from cd where cd.k1 not in (select dim.k1 from dim where dim.k1 is not null)");
    }

    /** T8: RIGHT OUTER variant — optimizer commutes freely. */
    @Test
    public void t8_rangeRightOuterHash() throws Exception {
        assertPlansAsHashJoin(
                "select count(*) from cd right outer join dim on cd.k1 = dim.k1");
    }

    /** T9: FULL OUTER variant. */
    @Test
    public void t9_rangeFullOuterHash() throws Exception {
        assertPlansAsHashJoin(
                "select count(*) from cd full outer join dim on cd.k1 = dim.k1");
    }

    /** T10: SHUFFLE hint forces shuffle join even when bucket-shuffle is feasible. */
    @Test
    public void t10_rangeJoinHashShuffleHint() throws Exception {
        assertContainsMarkerNotColocate(
                "select count(*) from cd join [shuffle] dim on cd.k1 = dim.k1",
                "INNER JOIN (PARTITIONED)");
    }

    /** T11: disable_colocate_join (session variable) doesn't break the mixed path. */
    @Test
    public void t11_disableColocateJoinSessionVar() throws Exception {
        Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", true);
        try {
            assertContainsMarkerNotColocate(
                    "select count(*) from cd join dim on cd.k1 = dim.k1",
                    "INNER JOIN");
        } finally {
            Deencapsulation.setField(connectContext.getSessionVariable(), "disableColocateJoin", false);
        }
    }

    /** T12: Hash(LOCAL) × Range with [shuffle] hint — Guarantor enforces shuffle on both sides. */
    @Test
    public void t12_hashLocalRangeShuffleHint() throws Exception {
        assertContainsMarkerNotColocate(
                "select count(*) from dim join [shuffle] cd on dim.k1 = cd.k1",
                "INNER JOIN (PARTITIONED)");
    }

    // ---------- Optimizer-level invariant ----------

    /**
     * Invariant: for non-colocate, non-broadcast joins, no child output property
     * may carry a {@code RangeDistributionSpec}.  Broadcast joins and range×range
     * colocate joins legitimately carry range specs and are excluded.
     */
    @Test
    public void invariantNoRangeSpecOnNonColocateMixedJoinChildren() throws Exception {
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(connectContext,
                "select count(*) from cd join dim on cd.k1 = dim.k1").second;
        OptExpression root = execPlan.getPhysicalPlan();
        Assertions.assertNotNull(root, "physical plan must be non-null");

        int inspectedJoins = checkNoRangeSpecOnJoinChildren(root, 0);
        Assertions.assertTrue(inspectedJoins >= 1, "expected at least one join in the F2 repro");
    }

    private static int checkNoRangeSpecOnJoinChildren(OptExpression node, int count) {
        Operator op = node.getOp();
        if (op instanceof PhysicalJoinOperator
                && !"BROADCAST".equalsIgnoreCase(((PhysicalJoinOperator) op).getJoinHint())) {
            for (int i = 0; i < node.arity(); i++) {
                OptExpression child = node.inputAt(i);
                if (child.getOutputProperty() != null) {
                    DistributionSpec spec = child.getOutputProperty().getDistributionProperty().getSpec();
                    Assertions.assertFalse(spec instanceof RangeDistributionSpec,
                            "non-colocate mixed join child " + i + " carries RangeDistributionSpec, spec=" + spec);
                }
            }
            count++;
        }
        for (int i = 0; i < node.arity(); i++) {
            count = checkNoRangeSpecOnJoinChildren(node.inputAt(i), count);
        }
        return count;
    }
}
