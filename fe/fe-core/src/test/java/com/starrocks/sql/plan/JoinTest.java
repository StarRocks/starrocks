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

    @Test
    public void testValueNodeJoin() throws Exception {
        String sql = "select count(*) from (select test_all_type.t1c as left_int, " +
                "test_all_type1.t1c as right_int from (select * from test_all_type limit 0) " +
                "test_all_type cross join (select * from test_all_type limit 0) test_all_type1 cross join (select * from test_all_type limit 0) test_all_type6) t;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:EMPTYSET");
        assertContains(plan, "1:EMPTYSET");
    }

    @Test
    public void testSemiJoinReorderProjections() throws Exception {
        String sql = "WITH with_t_0 as (\n" +
                "  SELECT \n" +
                "    t1_3.t1b, \n" +
                "    t1_3.t1d \n" +
                "  FROM \n" +
                "    test_all_type AS t1_3 \n" +
                "  WHERE \n" +
                "    (\n" +
                "      (\n" +
                "        SELECT \n" +
                "          t1_3.t1a \n" +
                "        FROM \n" +
                "          test_all_type AS t1_3\n" +
                "      )\n" +
                "    ) < (\n" +
                "      (\n" +
                "        SELECT \n" +
                "          11\n" +
                "      )\n" +
                "    )\n" +
                ") \n" +
                "SELECT \n" +
                "  SUM(count) \n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      CAST(false AS INT) as count \n" +
                "    FROM \n" +
                "      test_all_type AS t1_3 FULL \n" +
                "      JOIN (\n" +
                "        SELECT \n" +
                "          with_t_0.t1b \n" +
                "        FROM \n" +
                "          with_t_0 AS with_t_0 \n" +
                "        WHERE \n" +
                "          (with_t_0.t1d) IN (\n" +
                "            (\n" +
                "              SELECT \n" +
                "                t1_3.t1d \n" +
                "              FROM \n" +
                "                test_all_type AS t1_3\n" +
                "            )\n" +
                "          )\n" +
                "      ) subwith_t_0 ON t1_3.id_decimal = subwith_t_0.t1b\n" +
                "  ) t;";
        String plan = getFragmentPlan(sql);
        // check no error
        assertContains(plan, "17:ASSERT NUMBER OF ROWS");
    }

    @Test
    public void testSemiOuterJoin() throws Exception {
        String sql = "select * from t0 full outer join t2 on t0.v1 = t2.v7 and t0.v1 > t2.v7 " +
                "where t0.v2 in (select t1.v4 from t1 where false)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  7:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 7: v4\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (PARTITIONED)");
    }

    @Test
    public void testComplexExpr() throws Exception {
        String sql = "select count(v1) as c, if(v4 is not null, v4, '未知') as k2 from ( select A.v1, B.v4 from " +
                "( select v1 from t0 where v2 = 1 ) A left join ( select v4, if( v5 = 0, '未知', if( v5 = 1, '广东', " +
                "if( v5 = 2, '广西', if( v5 = 3, '北京', if( v5 = 4, '海南', if( v5 = 5, '福建', if( v5 = 6, '天津', " +
                "if( v5 = 7, '湖南', if( v5 = 8, '湖北', if( v5 = 9, '河南', if( v5 = 10, '河北', if( v5 = 11, '山东', " +
                "if( v5 = 12, '山西', if( v5 = 13, '黑龙江', if( v5 = 14, '辽宁', if( v5 = 15, '上海', if( v5 = 16, '甘肃', " +
                "if( v5 = 17, '青海', if( v5 = 18, '新疆', if( v5 = 19, '西藏', if( v5 = 20, '宁夏', if( v5 = 21, '四川', " +
                "if( v5 = 22, '云南', if( v5 = 23, '吉林', if( v5 = 24, '内蒙古', if( v5 = 25, '陕西', if( v5 = 26, '安徽', " +
                "if( v5 = 27, '贵州', if( v5 = 28, '江苏', if( v5 = 29, '重庆', if( v5 = 30, '浙江', if( v5 = 31, '江西', " +
                "if( v5 = 32, '国外', if( v5 = 33, '台湾', if(v5 = 34, '香港', if(v5 = 35, '澳门', 'Default')) ) ) ) ) ) ) " +
                ") ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) ) as k from t1 ) B on A.v1 = B.k ) C group by v4;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "if(5: v5 = 0, '未知'");
    }
}
