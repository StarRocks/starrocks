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

public class SelectConstTest extends PlanTestBase {
    @Test
    public void testSelectConst() throws Exception {
        assertPlanContains("select 1,2", "  1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 2\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select a from (select 1 as a, 2 as b) t", "  1:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 union all select 1,2", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 union select 1,2", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 except select 1,2", "EXCEPT", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2 from t0 intersect select 1,2", "INTERSECT", "  4:Project\n" +
                "  |  <slot 7> : 1\n" +
                "  |  <slot 8> : 2\n" +
                "  |  \n" +
                "  3:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select v1,v2,b from t0 inner join (select 1 as a,2 as b) t on v1 = a", "  2:Project\n" +
                "  |  <slot 6> : 2\n" +
                "  |  <slot 7> : 1\n" +
                "  |  \n" +
                "  1:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testValuesNodePredicate() throws Exception {
        assertPlanContains("select database()", "<slot 2> : 'test'");
        assertPlanContains("select schema()", "<slot 2> : 'test'");
        assertPlanContains("select user()", "<slot 2> : USER()");
        assertPlanContains("select current_user()", "<slot 2> : CURRENT_USER()");
        assertPlanContains("select connection_id()", "<slot 2> : CONNECTION_ID()");
    }

    @Test
    public void testFromUnixtime() throws Exception {
        assertPlanContains("select from_unixtime(10)", "'1970-01-01 08:00:10'");
        assertPlanContains("select from_unixtime(1024)", "'1970-01-01 08:17:04'");
        assertPlanContains("select from_unixtime(32678)", "'1970-01-01 17:04:38'");
        assertPlanContains("select from_unixtime(102400000)", "'1973-03-31 12:26:40'");
        assertPlanContains("select from_unixtime(253402243100)", "'9999-12-31 15:58:20'");
    }

    @Test
    public void testAggWithConstant() throws Exception {
        assertPlanContains("select case when c1=1 then 1 end from (select '1' c1  union  all select '2') a " +
                        "group by rollup(case  when c1=1 then 1 end, 1 + 1)",
                "<slot 4> : '2'",
                "<slot 2> : '1'",
                "  8:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[], [6], [6, 7]]\n" +
                        "  |  \n" +
                        "  7:Project\n" +
                        "  |  <slot 6> : if(5: expr = '1', 1, NULL)\n" +
                        "  |  <slot 7> : 2");
    }

    @Test
    public void testSubquery() throws Exception {
        assertPlanContains("select * from t0 where v3 in (select 2)", "LEFT SEMI JOIN", "<slot 7> : 2");
        assertPlanContains("select * from t0 where v3 not in (select 2)", "NULL AWARE LEFT ANTI JOIN",
                "<slot 7> : 2");
        assertPlanContains("select * from t0 where exists (select 9)", "  1:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select * from t0 where exists (select 9,10)", "  1:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select * from t0 where not exists (select 9)", ":UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select * from t0 where v3 = (select 6)", "  5:Project\n" +
                "  |  <slot 7> : CAST(5: expr AS BIGINT)", "equal join conjunct: 3: v3 = 7: cast");
        assertPlanContains("select case when (select max(v4) from t1) > 1 then 2 else 3 end", "  7:Project\n" +
                "  |  <slot 7> : if(5: max > 1, 2, 3)\n" +
                "  |  \n" +
                "  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
        assertPlanContains("select 1, 2, case when (select max(v4) from t1) > 1 then 4 else 5 end", "  7:Project\n" +
                "  |  <slot 2> : 1\n" +
                "  |  <slot 3> : 2\n" +
                "  |  <slot 9> : if(7: max > 1, 4, 5)\n" +
                "  |  \n" +
                "  6:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |    \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL");
    }

    @Test
    public void testDoubleCastWithoutScientificNotation() throws Exception {
        String sql = "SELECT * FROM t0 WHERE CAST(CAST(CASE WHEN TRUE THEN -1229625855 " +
                "WHEN false THEN 1 ELSE 2 / 3 END AS STRING ) AS BOOLEAN );";
        assertPlanContains(sql, "PREDICATES: CAST('-1229625855' AS BOOLEAN)");
    }
}
