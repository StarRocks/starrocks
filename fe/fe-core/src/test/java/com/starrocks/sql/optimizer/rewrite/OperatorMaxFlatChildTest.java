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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.common.Config;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class OperatorMaxFlatChildTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testMaxCaseWhenChildren() {
        assertThrows(SemanticException.class, () -> {
            final int prev = Config.max_scalar_operator_flat_children;
            Config.max_scalar_operator_flat_children = 10;
            try {
                String sql;
                sql = "select\n" +
                        "    case\n" +
                        "        when cw1 = \"X11\" then concat(cw1, \"11\")\n" +
                        "        when cw1 = \"X111\" then concat(cw1, \"111\")\n" +
                        "        when cw1 = \"X1111\" then concat(cw1, \"1111\")\n" +
                        "    end cw1\n" +
                        "from\n" +
                        "    (\n" +
                        "        select\n" +
                        "            case\n" +
                        "                when cw1 = \"X11\" then concat(cw1, \"11\")\n" +
                        "                when cw1 = \"X111\" then concat(cw1, \"111\")\n" +
                        "                when cw1 = \"X1111\" then concat(cw1, \"1111\")\n" +
                        "            end cw1\n" +
                        "        from\n" +
                        "            (\n" +
                        "                select\n" +
                        "                    case\n" +
                        "                        when cw1 = \"X2\" then concat(cw1, \"1\")\n" +
                        "                        when cw1 = \"X1\" then concat(cw1, \"11\")\n" +
                        "                        when cw1 = \"X3\" then concat(cw1, \"11\")\n" +
                        "                    end cw1\n" +
                        "                from\n" +
                        "                    (\n" +
                        "                        select\n" +
                        "                            case\n" +
                        "                                when cw1 = 1 then upper(cw1)\n" +
                        "                                when cw1 = 2 then cw1\n" +
                        "                                when cw1 = 3 then lower(cw1)\n" +
                        "                            end cw1\n" +
                        "                        from\n" +
                        "                            (\n" +
                        "                                select\n" +
                        "                                    case\n" +
                        "                                        when t1a = 1 then t1a\n" +
                        "                                        when t1a = 2 then t1b\n" +
                        "                                        when t1a = 3 then t1c\n" +
                        "                                    end cw1\n" +
                        "                                from\n" +
                        "                                    test_all_type\n" +
                        "                            ) t\n" +
                        "                    ) t\n" +
                        "            ) t\n" +
                        "    ) t;\n";
                getFragmentPlan(sql);
            } finally {
                Config.max_scalar_operator_flat_children = prev;
            }
        });
    }

    // A 2-layer single-referenced CTE chain reproducing "Expression too complex": each layer's CASE
    // WHEN references the previous column 7 times, so inlining c1 into c2 multiplies the node count
    // (~20 -> ~153). Single-referenced CTEs are inlined, then MergeTwoProjectRule collapses the
    // projects and ReplaceColumnRefRewriter clones the expression per reference, tripping the limit.
    @Test
    public void testNestedCteExpressionBloat() throws Exception {
        final int prev = Config.max_scalar_operator_flat_children;
        Config.max_scalar_operator_flat_children = 100;
        try {
            String sql =
                    "WITH \n" +
                    "  c1 AS (SELECT CASE WHEN t1c = 1 THEN t1c + 1 WHEN t1c = 2 THEN t1c + 2 " +
                    "                     WHEN t1c = 3 THEN t1c + 3 ELSE t1c END AS x FROM test_all_type),\n" +
                    "  c2 AS (SELECT CASE WHEN x = 11 THEN x + 11 WHEN x = 22 THEN x + 22 " +
                    "                     WHEN x = 33 THEN x + 33 ELSE x END AS x FROM c1)\n" +
                    "SELECT x FROM c2";
            assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        } finally {
            Config.max_scalar_operator_flat_children = prev;
        }
    }

    // Same 2-layer chain, but c1 carries a [materialized] hint, so RelationTransformer materializes it
    // (buildCTEAnchorAndProducer + addForceCTE) instead of inlining. c2 then references c1's output
    // column (1 node) rather than inlining c1's CASE WHEN, so there is no project-merge bloat: at
    // limit = 100 it does not throw and the plan shows a MultiCastDataSinks producer -- also confirming
    // a single-referenced forced CTE survives the optimizer's inline rules.
    @Test
    public void testNestedCteMaterializedHintNoBloat() throws Exception {
        final int prev = Config.max_scalar_operator_flat_children;
        Config.max_scalar_operator_flat_children = 100;   // same tiny limit as the inline case
        try {
            String sql =
                    "WITH \n" +
                    "  c1 AS (SELECT CASE WHEN t1c = 1 THEN t1c + 1 WHEN t1c = 2 THEN t1c + 2 " +
                    "                     WHEN t1c = 3 THEN t1c + 3 ELSE t1c END AS x FROM test_all_type) [materialized],\n" +
                    "  c2 AS (SELECT CASE WHEN x = 11 THEN x + 11 WHEN x = 22 THEN x + 22 " +
                    "                     WHEN x = 33 THEN x + 33 ELSE x END AS x FROM c1)\n" +
                    "SELECT x FROM c2";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "MultiCastDataSinks");
        } finally {
            Config.max_scalar_operator_flat_children = prev;
        }
    }

    // C.2 auto fix (cascading): with cbo_cte_force_reuse_inlined_node_count on, the nested-CTE chain
    // that would blow up (each layer's CASE WHEN references the previous column 7 times, so the
    // inlined size cascades 20 -> 153 -> 1084) is auto-materialized in RelationTransformer -- no hint.
    // The estimator materializes c2 (its inlined size 153 > 100), which caps c3 back to ~20; nothing
    // exceeds the 200 flat-children limit and the plan shows MultiCastDataSinks.
    @Test
    public void testNestedCteAutoForceReuseByInlinedNodeCount() throws Exception {
        final int prevLimit = Config.max_scalar_operator_flat_children;
        final int prevThreshold = connectContext.getSessionVariable().getCboCTEForceReuseInlinedNodeCount();
        Config.max_scalar_operator_flat_children = 200;
        connectContext.getSessionVariable().setCboCTEForceReuseInlinedNodeCount(100);
        try {
            String sql =
                    "WITH \n" +
                    "  c1 AS (SELECT CASE WHEN t1c = 1 THEN t1c + 1 WHEN t1c = 2 THEN t1c + 2 " +
                    "                     WHEN t1c = 3 THEN t1c + 3 ELSE t1c END AS x FROM test_all_type),\n" +
                    "  c2 AS (SELECT CASE WHEN x = 11 THEN x + 11 WHEN x = 22 THEN x + 22 " +
                    "                     WHEN x = 33 THEN x + 33 ELSE x END AS x FROM c1),\n" +
                    "  c3 AS (SELECT CASE WHEN x = 44 THEN x + 44 WHEN x = 55 THEN x + 55 " +
                    "                     WHEN x = 66 THEN x + 66 ELSE x END AS x FROM c2)\n" +
                    "SELECT x FROM c3";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "MultiCastDataSinks");
        } finally {
            Config.max_scalar_operator_flat_children = prevLimit;
            connectContext.getSessionVariable().setCboCTEForceReuseInlinedNodeCount(prevThreshold);
        }
    }

}
