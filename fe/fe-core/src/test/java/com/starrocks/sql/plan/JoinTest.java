// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.SessionVariable;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TExplainLevel;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class JoinTest extends PlanTestBase {
    @Test
    public void testParallelism() throws Exception {
        int numCores = 8;
        int expectedParallelism = numCores / 2;
        new MockUp<BackendCoreStat>() {
            @Mock
            public int getAvgNumOfHardwareCoresOfBe() {
                return numCores;
            }
        };

        SessionVariable sessionVariable = connectContext.getSessionVariable();
        boolean enablePipeline = sessionVariable.isEnablePipelineEngine();
        int pipelineDop = sessionVariable.getPipelineDop();
        int parallelExecInstanceNum = sessionVariable.getParallelExecInstanceNum();

        try {
            // Enable DopAutoEstimate.
            sessionVariable.setEnablePipelineEngine(true);
            sessionVariable.setPipelineDop(0);
            sessionVariable.setParallelExecInstanceNum(1);
            FeConstants.runningUnitTest = true;

            // Case 1: local bucket shuffle join should use fragment instance parallel.
            String sql = "select a.v1 from t0 a join [bucket] t0 b on a.v1 = b.v2 and a.v2 = b.v1";
            ExecPlan plan = getExecPlan(sql);
            PlanFragment fragment = plan.getFragments().get(1);
            assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (BUCKET_SHUFFLE)");
            Assert.assertEquals(expectedParallelism, fragment.getParallelExecNum());
            Assert.assertEquals(1, fragment.getPipelineDop());

            // Case 2: colocate join should use fragment instance parallel.
            sql = "SELECT * from t0 join t0 as b on t0.v1 = b.v1;";
            plan = getExecPlan(sql);
            fragment = plan.getFragments().get(1);
            assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (COLOCATE)");
            Assert.assertEquals(expectedParallelism, fragment.getParallelExecNum());
            Assert.assertEquals(1, fragment.getPipelineDop());

            // Case 3: broadcast join should use pipeline parallel.
            sql = "select a.v1 from t0 a join [broadcast] t0 b on a.v1 = b.v2 and a.v2 = b.v1";
            plan = getExecPlan(sql);
            fragment = plan.getFragments().get(1);
            assertContains(fragment.getExplainString(TExplainLevel.NORMAL), "join op: INNER JOIN (BROADCAST)");
            Assert.assertEquals(1, fragment.getParallelExecNum());
            Assert.assertEquals(expectedParallelism, fragment.getPipelineDop());

            // Case 4: local bucket shuffle join succeeded by broadcast should use fragment instance parallel.
            sql = "select a.v1 from t0 a " +
                    "join [bucket] t0 b on a.v1 = b.v2 and a.v2 = b.v1 " +
                    "join [broadcast] t0 c on a.v1 = c.v2";
            plan = getExecPlan(sql);
            fragment = plan.getFragments().get(1);
            String fragmentString = fragment.getExplainString(TExplainLevel.NORMAL);
            assertContains(fragmentString, "join op: INNER JOIN (BROADCAST)");
            assertContains(fragmentString, "join op: INNER JOIN (BUCKET_SHUFFLE)");
            Assert.assertEquals(expectedParallelism, fragment.getParallelExecNum());
            Assert.assertEquals(1, fragment.getPipelineDop());
        } finally {
            sessionVariable.setEnablePipelineEngine(enablePipeline);
            sessionVariable.setPipelineDop(pipelineDop);
            sessionVariable.setParallelExecInstanceNum(parallelExecInstanceNum);
            FeConstants.runningUnitTest = false;
        }
    }

    @Test
    public void testColocateJoinWithProject() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select a.v1 from t0 as a join t0 b on a.v1 = b.v1 and a.v1 = b.v1 + 1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (COLOCATE)");
        FeConstants.runningUnitTest = false;
    }
<<<<<<< HEAD
=======

    @Test
    public void testShuffleJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 ) s1 join[shuffle] t2 on s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v2 = t2.v8 and s1.v6 = t2.v9";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                    "  |  equal join conjunct: 6: v6 = 9: v9\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v5 = t2.v8 and s1.v3 = t2.v9";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  equal join conjunct: 3: v3 = 9: v9\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v5 = t2.v8 and s1.v6 = t2.v9";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  equal join conjunct: 6: v6 = 9: v9\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6");
        }
        {
            // mismatch shuffle orders
            String sql =
                    "select * from ( select * from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v3 = t1.v6 ) s1 " +
                            "join[shuffle] t2 on s1.v6 = t2.v9 and s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 6: v6 = 9: v9\n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                    "  |  equal join conjunct: 3: v3 = 6: v6\n" +
                    "  |  \n" +
                    "  |----3:EXCHANGE\n" +
                    "  |    \n" +
                    "  1:EXCHANGE\n");
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 06\n" +
                    "    HASH_PARTITIONED: 9: v9, 8: v8");
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 03\n" +
                    "    HASH_PARTITIONED: 6: v6, 5: v5");
            assertContains(plan, "  STREAM DATA SINK\n" +
                    "    EXCHANGE ID: 01\n" +
                    "    HASH_PARTITIONED: 3: v3, 2: v2");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketSingleJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            // Change on predicate order
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t1.v4 = t0.v1 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            // Only output right table's attribute
            String sql =
                    "select * from ( select t1.v4 from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            // Bushy join
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 ) s1, " +
                            "( select * from t2 join[bucket] t3 on t2.v7 = t3.v10 ) s2 " +
                            "where s1.v4 = s2.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  9:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 10: v10");
        }
        {
            // Multi level joins
            String sql =
                    "select * from t0 join[bucket] t1 on t0.v1 = t1.v4 " +
                            "join[bucket] t2 on t1.v4 = t2.v7 " +
                            "join[bucket] t3 on t2.v7 = t3.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 7: v7 = 10: v10");
        }
        {
            // Multi level joins
            String sql =
                    "select * from t0 join[bucket] t1 on t0.v1 = t1.v4 " +
                            "join[bucket] t2 on t1.v4 = t2.v7 " +
                            "join[bucket] t3 on t2.v7 = t3.v10 " +
                            "join[bucket] t4 on t3.v10 = t4.v13";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 10: v10 = 13: v13");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketMultiJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v1 = t2.v7 and s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 7: v7\n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v4 = t2.v7 and s1.v2 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7\n" +
                    "  |  equal join conjunct: 2: v2 = 8: v8");
        }
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v4 = t2.v7 and s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7\n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testColocateSingleJoinEqEquivalentPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from colocate_t0 join[colocate] colocate_t1 on colocate_t0.v1 = colocate_t1.v4 ) s1 " +
                            "join[bucket] colocate_t2 on s1.v4 = colocate_t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            String sql =
                    "select * from ( select * from colocate_t0 join[colocate] colocate_t1 on colocate_t0.v1 = colocate_t1.v4 ) s1, " +
                            "( select * from colocate_t2 join[colocate] colocate_t3 on colocate_t2.v7 = colocate_t3.v10 ) s2 " +
                            "where s1.v4 = s2.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  7:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 10: v10");
        }
        {
            // anti colocate join
            String sql =
                    "select * from (select * from colocate_t0 join[bucket] colocate_t2 on colocate_t0.v1 = colocate_t2.v7) s1 " +
                            "join [colocate] colocate_t3 on s1.v7 = colocate_t3.v10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v7 = 7: v10\n" +
                    "  |  \n" +
                    "  |----5:EXCHANGE\n" +
                    "  |    \n" +
                    "  3:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 1: v1 = 4: v7");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinOnPredicateCommutativityNotInnerJoin() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 left join[bucket] t1 on t0.v1 = t1.v4 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        {
            String sql =
                    "select * from ( select * from t0 right join[bucket] t1 on t0.v1 = t1.v4 ) s1 join[bucket] t2 on s1.v4 = t2.v7";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 4: v4 = 7: v7");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testBucketJoinNotEqPropertyInfo() throws Exception {
        FeConstants.runningUnitTest = true;
        {
            String sql =
                    "select * from ( select * from t0 join[bucket] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) s1 " +
                            "join[bucket] t2 on s1.v5 = t2.v8";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  |  join op: INNER JOIN (PARTITIONED)\n" +
                    "  |  hash predicates:\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 5: v5 = 8: v8\n" +
                    "  |  \n" +
                    "  |----6:EXCHANGE\n" +
                    "  |    \n" +
                    "  4:EXCHANGE");
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testValueNodeJoin() throws Exception {
        String sql = "select count(*) from (select test_all_type.t1c as left_int, " +
                "test_all_type1.t1c as right_int from (select * from test_all_type limit 0) " +
                "test_all_type cross join (select * from test_all_type limit 0) test_all_type1 cross join (select * from test_all_type limit 0) test_all_type6) t;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:EMPTYSET");
        assertContains(plan, "2:EMPTYSET");
    }
>>>>>>> 84ece485 (Fix LogicValueOperator misjudgment of equality when columnRefSet and rows are empty (#5324))
}
