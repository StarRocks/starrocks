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

import org.junit.Test;

public class TableFunctionTest extends PlanTestBase {
    @Test
    public void testSql0() throws Exception {
        String sql = "SELECT * FROM TABLE(unnest(ARRAY<INT>[1, 2, 3]))";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: unnest\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : [1,2,3]\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testSql1() throws Exception {
        String sql = "SELECT x FROM TABLE(unnest(ARRAY<INT>[1, 2, 3])) t(x)";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: x\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : [1,2,3]\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "SELECT * FROM TABLE(unnest(ARRAY<INT>[1])) t0(x), TABLE(unnest(ARRAY<INT>[1, 2, 3])) t1(x)";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: x | 5: x\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  7:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : [1]\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  5:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 6> : [1,2,3]\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "SELECT * FROM TABLE(unnest(ARRAY<INT>[1])) t0(x) JOIN TABLE(unnest(ARRAY<INT>[1, 2, 3])) t1(x)" +
                " ON t0.x=t1 .x";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: x | 5: x\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: x = 5: x\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |    \n" +
                "  3:SELECT\n" +
                "  |  predicates: 2: x IS NOT NULL\n" +
                "  |  \n" +
                "  2:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : [1]\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  7:SELECT\n" +
                "  |  predicates: 5: x IS NOT NULL\n" +
                "  |  \n" +
                "  6:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 6> : [1,2,3]\n" +
                "  |  \n" +
                "  4:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testSql4() throws Exception {
        String sql = "SELECT * FROM TABLE(unnest(ARRAY<INT>[1])) t0(x) LEFT JOIN TABLE(unnest(ARRAY<INT>[1, 2, 3])) t1(x)" +
                " ON t0.x=t1 .x";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:2: x | 5: x\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  7:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: x = 5: x\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 3> : [1]\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  5:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 6> : [1,2,3]\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testTableFunctionReorder() throws Exception {
        String sql = "SELECT * FROM TABLE(unnest(ARRAY<INT>[1])) t0(x) LEFT JOIN TABLE(unnest(ARRAY<INT>[1, 2, 3])) t1(x)" +
                " ON t0.x=t1.x join t2 on t0.x = t2.v7";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v7 = 10: cast\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t2");
    }
}
