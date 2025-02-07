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

import com.starrocks.planner.TableFunctionNode;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.Test;

<<<<<<< HEAD
=======
import java.util.Optional;

>>>>>>> 4be4b5c34e ([Enhancement] Eliminate non-required unnest computation (#55431))
public class TableFunctionTest extends PlanTestBase {
    @Test
    public void testTableFunctionAlias() throws Exception {
        String sql = "select t.*, unnest from test_all_type t, unnest(split(t1a, ','))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        sql = "select table_function_unnest.*, unnest from test_all_type table_function_unnest, unnest(split(t1a, ','))";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        sql = "select t.*, unnest from test_all_type t, unnest(split(t1a, ',')) unnest";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        sql = "select t.*, unnest.v1, unnest.v2 from (select * from test_all_type join t0_not_null) t, " +
                "unnest(split(t1a, ','), v3) as unnest (v1, v2)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "TableValueFunction");

        Exception e = Assert.assertThrows(SemanticException.class, () -> {
            String sql1 = "select table_function_unnest.*, unnest.v1, unnest.v2 from " +
                    "(select * from test_all_type join t0_not_null) " +
                    "table_function_unnest, unnest(split(t1a, ','))";
            getFragmentPlan(sql1);
        });

        Assert.assertTrue(e.getMessage().contains("Not unique table/alias: 'table_function_unnest'"));
    }
<<<<<<< HEAD
=======

    @Test
    public void testRewrite() throws Exception {
        String sql = "SELECT k1,  unnest AS c3\n" +
                "    FROM test_agg,unnest(bitmap_to_array(b1)) ORDER BY k1 ASC, c3 ASC\n" +
                "LIMIT 5;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "tableFunctionName: unnest_bitmap");
        assertNotContains(plan, "bitmap_to_array");
    }

    @Test
    public void testUnnesetBitmapToArrayToUnnestBitmapRewrite() throws Exception {
        String sql = "with r1 as (select b1 as b2 from test_agg),\n" +
                "\t r2 as (select sub_bitmap(b1, 0, 10) as b2 from test_agg),\n" +
                "\t r3 as (select bitmap_and(t0.b2, t1.b2) as b2 from r1 t0 join r2 t1)\n" +
                "select unnest as r1 from r3, unnest(bitmap_to_array(b2)) order by r1;";
        String plan = getFragmentPlan(sql);
        PlanTestBase.assertContains(plan, "5:Project\n" +
                "  |  <slot 28> : bitmap_and(10: b1, 25: sub_bitmap)");
        PlanTestBase.assertContains(plan, "tableFunctionName: unnest_bitmap");
        PlanTestBase.assertNotContains(plan, "bitmap_to_array");
    }

    @Test
    public void testUnnesetFnResultNotRequired() throws Exception {
        Object[][] testCaseList = new Object[][] {
                {
                        "select t.* from test_all_type t, unnest(split(t1a, ','))",
                        false
                },
                {
                        "select t.*, unnest from test_all_type t, unnest(split(t1a, ','))",
                        true
                },
                {
                        "SELECT y FROM TABLE(generate_series(1, 2)) t(x), LATERAL generate_series(1, 5000) t2(y);",
                        true
                }
        };

        for (Object[] tc : testCaseList) {
            String sql = (String) tc[0];
            Boolean isRequired = (Boolean) tc[1];
            System.out.println(sql);
            ExecPlan plan = getExecPlan(sql);

            Optional<TableFunctionNode> optTableFuncNode = plan.getFragments()
                    .stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .filter(planNode -> planNode instanceof TableFunctionNode)
                    .map(planNode -> (TableFunctionNode) planNode)
                    .findFirst();
            Assert.assertTrue(optTableFuncNode.isPresent());
            Assert.assertEquals(optTableFuncNode.get().isFnResultRequired(), isRequired);
        }
    }
>>>>>>> 4be4b5c34e ([Enhancement] Eliminate non-required unnest computation (#55431))
}
