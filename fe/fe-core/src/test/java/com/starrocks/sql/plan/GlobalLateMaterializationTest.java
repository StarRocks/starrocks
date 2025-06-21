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
import org.junit.BeforeClass;
import org.junit.Test;

public class GlobalLateMaterializationTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
    }

    @Test
    public void testJoin() throws Exception {
        String sql;
        String plan;
        sql = "select * from t0 inner join t1 on t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |    <slot 3> => v3\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        sql = "select t1.* from t0 inner join t1 on t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:FETCH\n" +
                "  |  lookup node: 05\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |    <slot 3> => v3\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  4:EXCHANGE\n" +
                "     limit: 10");

        sql = "select * from t0, t1, t2 where t0.v1 = t1.v4 and t0.v2 = t2.v7";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  10:FETCH\n" +
                "  |  lookup node: 09\n" +
                "  |  table: t0\n" +
                "  |    <slot 3> => v3\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  table: t2\n" +
                "  |    <slot 8> => v8\n" +
                "  |    <slot 9> => v9\n" +
                "  |  \n" +
                "  8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 7: v7");

        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");

        sql = "select * from t0, t1, t2 where t0.v1 = t1.v4 and t1.v4 = t2.v7";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  8:FETCH\n" +
                "  |  lookup node: 07\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |    <slot 3> => v3\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  table: t2\n" +
                "  |    <slot 8> => v8\n" +
                "  |    <slot 9> => v9\n" +
                "  |  \n" +
                "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 7: v7");

        // outer join may change the nullable properties for late materialized columns
        sql = "select l_orderkey, p_name from lineitem join part on l_partkey = p_partkey";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: part\n" +
                "  |    <slot 28> => ROW_ID\n" +
                "  |    <slot 19> => [P_NAME, VARCHAR, false]\n" +
                "  |  table: lineitem\n" +
                "  |    <slot 29> => ROW_ID\n" +
                "  |    <slot 1> => [L_ORDERKEY, INT, false]\n" +
                "  |  cardinality: -1\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        sql = "select l_orderkey, p_name from lineitem left join part on l_partkey = p_partkey";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: part\n" +
                "  |    <slot 28> => ROW_ID\n" +
                "  |    <slot 19> => [P_NAME, VARCHAR, true]\n" +
                "  |  table: lineitem\n" +
                "  |    <slot 29> => ROW_ID\n" +
                "  |    <slot 1> => [L_ORDERKEY, INT, false]\n" +
                "  |  cardinality: -1\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        sql = "select l_orderkey, p_name from lineitem right join part on l_partkey = p_partkey";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: part\n" +
                "  |    <slot 28> => ROW_ID\n" +
                "  |    <slot 19> => [P_NAME, VARCHAR, false]\n" +
                "  |  table: lineitem\n" +
                "  |    <slot 29> => ROW_ID\n" +
                "  |    <slot 1> => [L_ORDERKEY, INT, true]\n" +
                "  |  cardinality: -1\n" +
                "  |  \n" +
                "  3:HASH JOIN");


        sql = "select l_orderkey, p_name from lineitem full join part on l_partkey = p_partkey";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: part\n" +
                "  |    <slot 28> => ROW_ID\n" +
                "  |    <slot 19> => [P_NAME, VARCHAR, true]\n" +
                "  |  table: lineitem\n" +
                "  |    <slot 29> => ROW_ID\n" +
                "  |    <slot 1> => [L_ORDERKEY, INT, true]\n" +
                "  |  cardinality: -1\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        // join with projection
        sql = "select t0.v2 + t1.v5, t0.v1 + t1.v6 from t0 join t1 on t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:Project\n" +
                "  |  <slot 7> : 2: v2 + 5: v5\n" +
                "  |  <slot 8> : 1: v1 + 6: v6\n" +
                "  |  \n" +
                "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |  \n" +
                "  3:HASH JOIN");
        // join with aggregation
        sql = "select count(t0.v2), count(t1.v5) from t0 join t1 on t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |  \n" +
                "  3:HASH JOIN");


    }

    @Test
    public void testTopN() throws Exception {
        String sql;
        String plan;
        sql = "select * from t0 order by v1 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:FETCH\n" +
                "  |  lookup node: 03\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |    <slot 3> => v3\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  2:MERGING-EXCHANGE\n" +
                "     limit: 10");

        sql = "select * from t0 order by v1 limit 1000, 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:FETCH\n" +
                "  |  lookup node: 03\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |    <slot 3> => v3\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  2:MERGING-EXCHANGE\n" +
                "     offset: 1000\n" +
                "     limit: 10");

        sql = "select v1 + v2 from t0 order by v1 limit 1000";
        plan = getFragmentPlan(sql);
        System.out.println(plan);

        sql = "select * from t0 order by v1 + v2 limit 1000, 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  3:MERGING-EXCHANGE\n" +
                "     offset: 1000\n" +
                "     limit: 10");

        sql = "select * from t0 join t1 on t0.v1 = t1.v4 order by t0.v1 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  7:FETCH\n" +
                "  |  lookup node: 06\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |    <slot 3> => v3\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  5:MERGING-EXCHANGE");

        sql = "select * from t0 join t1 on t0.v1 = t1.v4 order by t0.v2 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:TOP-N\n" +
                "  |  order by: <slot 2> 2: v2 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |  \n" +
                "  3:HASH JOIN");
        assertContains(plan, "  9:FETCH\n" +
                "  |  lookup node: 08\n" +
                "  |  table: t0\n" +
                "  |    <slot 3> => v3\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => v5\n" +
                "  |    <slot 6> => v6\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  7:MERGING-EXCHANGE");

    }

    // low cardinatlity test
    @Test
    public void testLowCardinality() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);

        // if late materialized columns are low-cardinality column, we only fetch dict code
        String sql;
        String plan;
        sql = "select * from t7 join t8 on t7.k1 = t8.k1";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  6:Decode\n" +
                "  |  <dict id 7> : <string id 2>\n" +
                "  |  <dict id 8> : <string id 3>\n" +
                "  |  <dict id 9> : <string id 5>\n" +
                "  |  <dict id 10> : <string id 6>\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t7\n" +
                "  |    <slot 11> => ROW_ID\n" +
                "  |    <slot 7> => [k2, INT, true]\n" +
                "  |    <slot 8> => [k3, INT, true]\n" +
                "  |  table: t8\n" +
                "  |    <slot 12> => ROW_ID\n" +
                "  |    <slot 9> => [k2, INT, true]\n" +
                "  |    <slot 10> => [k3, INT, true]\n" +
                "  |  cardinality: -1\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        sql = "select count(t7.k2), count(t8.k2) from t7 join t8 on t7.k1 > t8.k1";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t7\n" +
                "  |    <slot 11> => ROW_ID\n" +
                "  |    <slot 9> => [k2, INT, true]\n" +
                "  |  table: t8\n" +
                "  |    <slot 12> => ROW_ID\n" +
                "  |    <slot 10> => [k2, INT, true]\n" +
                "  |  cardinality: -1\n" +
                "  |  \n" +
                "  3:NESTLOOP JOIN");


        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
    }

    @Test
    public void testUnsupportedTables() throws Exception {
        String sql;
        String plan;
        // duplicate key table join mysql external table
        sql = "select * from t0 join mysql_table on t0.v1 = mysql_table.k1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:FETCH\n" +
                "  |  lookup node: 05\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |    <slot 3> => v3\n" +
                "  |  \n" +
                "  4:HASH JOIN");

        // duplicate key table join unique key table
        sql = "select t0.v2, t1.big_value from t0 join test_agg_group_single_unique_key as t1 on t0.v1 = t1.id";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  7:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 5> : 5: big_value\n" +
                "  |  \n" +
                "  6:FETCH\n" +
                "  |  lookup node: 05\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => v2\n" +
                "  |  \n" +
                "  4:HASH JOIN");
    }

}
