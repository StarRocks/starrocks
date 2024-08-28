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

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class MaterializedViewTextBasedRewriteTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableMaterializedViewTextMatchRewrite(true);
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);
        starRocksAssert.withTable("create table user_tags (" +
                " time date, " +
                " user_id int, " +
                " user_name varchar(20), " +
                " tag_id int" +
                ")  partition by range (time) (partition p1 values less than MAXVALUE) " +
                "   distributed by hash(time)");
    }

    @Test
    public void testTextMatchRewrite0() {
        {
            String query= "select user_id, time, sum(tag_id) from user_tags group by user_id, time " +
                    "order by user_id, time;";
            testRewriteOK(query, query);
        }
        {
            String query= "select user_id, time, sum(tag_id) from user_tags group by user_id, time " +
                    "order by user_id, time limit 10;";
            testRewriteOK(query, query);
        }
        {
            String query= "select user_id, time, sum(tag_id) as alias from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteOK(query, query);
        }
        {
            String query= "select user_id, time, sum(tag_id) as alias1, sum(tag_id + 1) as alias2, sum(tag_id + 3)  as alias3 " +
                    "from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteOK(query, query);
        }
        {
            String mv = "select user_id, time, sum(tag_id) as alias from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            String query= "select user_id, time, sum(tag_id + 1) as alias from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteFail(mv, query);
        }
        // TODO: subset of mv
        {
            String mv = "select user_id, time, sum(tag_id) as alias1, sum(tag_id + 1) as alias2 from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            String query= "select user_id, time, sum(tag_id + 1) as alias1 from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteFail(mv, query);
        }
        {
            // with noisy space or \n
            String mv = "select user_id, time, sum(tag_id) as alias1 from user_tags \n" +
                    "group by user_id, time order by user_id, time limit 10;";
            String query= "select user_id, time, sum(tag_id) as alias1 from  user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteOK(mv, query);
        }
    }

    @Test
    public void testTextMatchRewrite1() {
        {
            String query= "select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time " +
                    "order by user_id, time;";
            testRewriteOK(query, query);
        }
        {
            String query= "select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time " +
                    "order by user_id, time limit 10;";
            testRewriteOK(query, query);
        }
        {
            String query= "select user_id, time, bitmap_union(to_bitmap(tag_id)) as alias from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteOK(query, query);
        }
        {
            // TODO: support different aliases
            String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as alias1 from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            String query= "select user_id, time, bitmap_union(to_bitmap(tag_id)) as alias2 from user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteFail(mv, query);
        }
        {
            // with noisy space or \n
            String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as alias1 from user_tags \n" +
                    "group by user_id, time order by user_id, time limit 10;";
            String query= "select user_id, time, bitmap_union(to_bitmap(tag_id)) as alias1 from  user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteOK(mv, query);
        }
        {
            // TODO: with different output order
            String mv = "select time, user_id, bitmap_union(to_bitmap(tag_id)) as alias1 from user_tags \n" +
                    "group by user_id, time order by user_id, time limit 10;";
            String query= "select user_id, time, bitmap_union(to_bitmap(tag_id)) as alias1 from  user_tags " +
                    "group by user_id, time order by user_id, time limit 10;";
            testRewriteFail(mv, query);
        }
    }

    @Test
    public void testTextMatchRewrite2() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time " +
                " order by user_id, time";
        // sub-query's order by will be squashed.
        testRewriteFail(mv, "select * from (" + mv + ") as a;");
        // TODO: support order by push-down
        testRewriteFail(mv, "select * from (select user_id, time, bitmap_union(to_bitmap(tag_id)) " +
                "from user_tags group by user_id, time) as a order by user_id, time;");
    }

    @Test
    public void testTextMatchRewrite3() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time";
        testRewriteOK(mv, "select * from (" + mv + ") as a;");
        testRewriteOK(mv, "select * from (" + mv + ") as a order by user_id, time;");
        testRewriteOK(mv, "select * from (" + mv + ") as a order by user_id, time limit 10;");
    }

    @Test
    public void testTextMatchRewrite4() {
        String query = "select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags where time = '2023-01-01' " +
                " group by user_id, time" +
                "  union all " +
                " select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags where time = '2023-01-01' " +
                " group by user_id, time;";
        testRewriteOK(query, query);
    }

    @Test
    public void testTextMatchRewrite5() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time";
        {
            String query = String.format("select * from (%s) a where time = '2023-01-01'", mv);
            testRewriteOK(mv, query);
        }
        {
            String query = String.format("select * from (%s) a where time = '2023-01-01' " +
                    "union all select * from (%s) b where time = '2023-01-02'", mv, mv);
            testRewriteOK(mv, query);
        }
        {
            String query = String.format("select count(*) from (" +
                    " select * from (%s) a where time = '2023-01-01' " +
                    " union all " +
                    " select * from (%s) b where time = '2023-01-02'" +
                    ") total", mv, mv);
            testRewriteOK(mv, query);
        }
        {
            String query = String.format("select time, bitmap_union(total.a) from (" +
                    " select * from (%s) a where time = '2023-01-01' " +
                    " union all " +
                    " select * from (%s) b where time = '2023-01-02'" +
                    ") total group by time", mv, mv);
            testRewriteOK(mv, query);
        }
    }

    @Test
    public void testTextMatchRewriteWithExtraTableInfo1() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time";
        String query = "select user_tags.user_id, user_tags.time, bitmap_union(to_bitmap(user_tags.tag_id)) as a " +
                "from user_tags group by user_id, time";
        testRewriteOK(mv, query);
    }

    @Test
    public void testTextMatchRewriteWithExtraTableInfo2() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time";
        String query = "select `user_tags`.user_id, `user_tags`.time, bitmap_union(to_bitmap(`user_tags`.tag_id)) as a " +
                "from user_tags group by user_id, time";
        testRewriteOK(mv, query);
    }

    @Test
    public void testTextMatchRewriteWithExtraDbTableInfo() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time";
        String query = "select test_mv.user_tags.user_id, test_mv.user_tags.time, " +
                "bitmap_union(to_bitmap(test_mv.user_tags.tag_id)) as a " +
                "from user_tags group by user_id, time";
        testRewriteOK(mv, query);
    }

    @Test
    public void testMvAstCache() {
        String query = "select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time " +
                "order by user_id, time;";
        ParseNode parseNode1 = MvUtils.getQueryAst(query, connectContext);
        ParseNode parseNode2 = MvUtils.getQueryAst(query, connectContext);
        Assert.assertFalse(parseNode2.equals(parseNode1));

        CachingMvPlanContextBuilder.AstKey astKey1 = new CachingMvPlanContextBuilder.AstKey(parseNode1);
        CachingMvPlanContextBuilder.AstKey astKey2 = new CachingMvPlanContextBuilder.AstKey(parseNode2);
        Assert.assertTrue(astKey2.equals(astKey1));
    }

    @Test
    public void testTextMatchRewriteWithOrder1() {
        String query = "select user_id, time, sum(tag_id) from user_tags group by user_id, time " +
                "order by user_id, time;";
        testRewriteOK(query, query)
                .contains("  2:SORT\n" +
                        "  |  order by: <slot 2> 2: user_id ASC, <slot 1> 1: time ASC\n" +
                        "  |  offset: 0")
                .contains("  1:Project\n" +
                        "  |  <slot 1> : 10: time\n" +
                        "  |  <slot 2> : 9: user_id\n" +
                        "  |  <slot 5> : 11: sum(tag_id)");
    }

    @Test
    public void testTextMatchRewriteWithOrder2() {
        String query = "select user_id, time, sum(tag_id) from user_tags group by user_id, time " +
                "order by user_id + 1, time;";
        testRewriteFail(query, query);
    }

    @Test
    public void testTextMatchRewriteWithOrder3() {
        {
            String query = "select user_id + 1, time, sum(tag_id) from user_tags group by user_id, time " +
                    "order by user_id + 1, time;";
            testRewriteOK(query, query);
        }
        {
            String query = "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time " +
                    "order by user_id + 1, time;";
            testRewriteOK(query, query);
        }
    }

    @Test
    public void testTextMatchRewriteWithUnionAll1() {
        {
            String query = "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time \n" +
                    "union all\n" +
                    "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;";
            testRewriteOK(query, query);
        }
        {
            String mv = "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time \n" +
                    "union all\n" +
                    "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;";
            String query = "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time " +
                    "union all " +
                    "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;";
            testRewriteOK(mv, query);
        }
        {
            String mv = "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time \n" +
                    "union all\n" +
                    "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;";
            String query = "select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time " +
                    "union all " +
                    "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t ;";
            testRewriteOK(mv, query);
        }
        {
            String mv = "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time \n" +
                    "union all\n" +
                    "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;";
            String query = "select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time " +
                    "union all " +
                    "select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t order by time;";
            testRewriteOK(mv, query);
        }
    }

    @Test
    public void testTextMatchRewriteWithSubQuery1() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time";
        String query = String.format("select user_id, count(time) from (%s) as t group by user_id limit 3;", mv);
        testRewriteOK(mv, query);
    }

    @Test
    public void testTextMatchRewriteWithSubQuery2() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time limit 3";
        String query = String.format("select user_id, count(time) from (%s) as t group by user_id limit 3;", mv);
        testRewriteOK(mv, query);
    }

    @Test
    public void testTextMatchRewriteWithSubQuery3() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time order by " +
                "user_id, time";
        String query = String.format("select user_id, count(time) from (%s) as t group by user_id limit 3;", mv);
        // TODO: support order by elimiation
        testRewriteFail(mv, query);
    }

    @Test
    public void testTextMatchRewriteWithSubQuery4() {
        String mv = "select * from (select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id," +
                " time order by user_id, time) s where user_id != 'xxxx'";
        String query = String.format("select user_id, count(time) from (%s) as t group by user_id limit 3;", mv);
        testRewriteOK(mv, query);
    }

    @Test
    public void testTextMatchRewriteWithExtraOrder1() {
        String mv = "select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id, time";
        String query = String.format("select user_id from (%s) t order by user_id, time;", mv);
        testRewriteOK(mv, query);
    }
    @Test
    public void testTextMatchRewriteWithExtraOrder2() {
        String mv = "select user_id, count(1) from (select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags " +
                "group by user_id, time) t group by user_id";
        String query = String.format("%s order by user_id;", mv);
        // TODO: support text based view for more patterns, now only rewrite the same query and subquery
        testRewriteFail(mv, query);
    }

    @Test
    public void testMVRewriteWithNonDeterministicFunctions() {
        starRocksAssert.withMaterializedView("create materialized view mv0" +
                " distributed by random" +
                " as select current_date(), t1a, t1b from test.test_all_type ;", () -> {
            {
                String query = " select current_date(), t1a, t1b from test.test_all_type";
                sql(query).nonMatch("mv0");
            }
        });
    }

    @Test
    public void testTextMatchRewriteWithSubQueryFilter() {
        starRocksAssert.withMaterializedView("create materialized view mv0" +
                " distributed by  random" +
                " as select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by user_id,time;",
                () -> {
                    String query = "select * from (select user_id, time, bitmap_union(to_bitmap(tag_id)) as a from user_tags group by " +
                            " user_id,time) s where user_id != 'xxxx'";
                    String plan = getQueryPlan(query);
                    PlanTestBase.assertContains(plan, "  0:OlapScanNode\n" +
                            "     TABLE: mv0\n" +
                            "     PREAGGREGATION: ON\n" +
                            "     PREDICATES: CAST(6: user_id AS VARCHAR(1048576)) != 'xxxx'");
                });
    }
}
