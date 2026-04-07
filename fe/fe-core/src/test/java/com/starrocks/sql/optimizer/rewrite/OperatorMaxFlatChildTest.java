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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

    @Test
    public void testMergeProjectCTEFallbackAvoidsTooComplexError() {
        final int prevFlat = Config.max_scalar_operator_flat_children;
        final int prevMerge = Config.merge_project_cte_rewrite_max_flat_children;
        try {
            String sql =
                    "WITH cte1 AS (\n" +
                            "  SELECT\n" +
                            "    t1a,\n" +
                            "    (CASE\n" +
                            "      WHEN t1b < 10 THEN 'small'\n" +
                            "      WHEN t1b >= 10 AND t1b < 50 THEN 'medium'\n" +
                            "      WHEN t1b >= 50 AND t1b < 100 THEN 'large'\n" +
                            "      WHEN t1b >= 100 AND t1b < 500 THEN 'xlarge'\n" +
                            "      ELSE NULL\n" +
                            "    END) AS size_label\n" +
                            "  FROM test_all_type\n" +
                            "),\n" +
                            "cte2 AS (\n" +
                            "  SELECT\n" +
                            "    t1a,\n" +
                            "    (CASE\n" +
                            "      WHEN size_label = 'small' AND t1a = 'X1' THEN concat(t1a, '_s')\n" +
                            "      WHEN size_label = 'medium' AND t1a = 'X1' THEN concat(t1a, '_m')\n" +
                            "      WHEN size_label = 'large' AND t1a = 'X1' THEN concat(t1a, '_l')\n" +
                            "      WHEN size_label = 'xlarge' AND t1a = 'X1' THEN concat(t1a, '_x')\n" +
                            "      ELSE NULL\n" +
                            "    END) AS category\n" +
                            "  FROM cte1\n" +
                            ")\n" +
                            "SELECT t1a, category FROM cte2";

            // Individual CASE WHEN expressions have ~20 flat children each (passes 50).
            // Merging two levels inlines the inner into the outer, creating ~120+ flat children (exceeds 50).
            Config.max_scalar_operator_flat_children = 50;
            Config.merge_project_cte_rewrite_max_flat_children = 0;

            // Without CTE fallback, the merge causes the expression to exceed max_scalar_operator_flat_children
            assertThrows(SemanticException.class, () -> getFragmentPlan(sql));

            // With CTE fallback enabled, the lower project is materialized as a CTE instead of inlining
            Config.merge_project_cte_rewrite_max_flat_children = 30;
            String plan = assertDoesNotThrow(() -> getFragmentPlan(sql));
            assertContains(plan, "MultiCastDataSinks");
        } finally {
            Config.max_scalar_operator_flat_children = prevFlat;
            Config.merge_project_cte_rewrite_max_flat_children = prevMerge;
        }
    }
}
