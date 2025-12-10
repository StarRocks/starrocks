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

import com.starrocks.sql.Explain;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExplainTest extends PlanTestBase {
    @Test
    public void testExplain() throws Exception {
        String sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4";
        ExecPlan execPlan = getExecPlan(sql);
        String plan1 = Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns());
        System.out.println("plan1" + plan1);
        assertContains(plan1, "- Output => [1:v1]\n"
                + "    - AGGREGATE(GLOBAL) [1:v1]\n"
                + "            Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 27.6}\n"
                + "        - EXCHANGE(SHUFFLE) [1]\n"
                + "                Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 7.6}\n"
                + "            - AGGREGATE(LOCAL) [1:v1]\n"
                + "                    Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 6.0}\n"
                + "                - SCAN [t0] => [1:v1]\n"
                + "                        Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 4.0}\n"
                + "                        partitionRatio: 0/1, tabletRatio: 0/0");

        Explain explain = new Explain(true, true, " ", " |");
        String plan2 = explain.print(execPlan.getPhysicalPlan(), execPlan.getOutputColumns());
        System.out.println("plan2" + plan2);
        assertContains(plan2, "- Output => [1:v1]\n"
                + " - AGGREGATE(GLOBAL) [1:v1] {rows: 1}\n"
                + "  - EXCHANGE(SHUFFLE) [1] {rows: 1}\n"
                + "   - AGGREGATE(LOCAL) [1:v1] {rows: 1}\n"
                + "    - SCAN [t0] => [1:v1] {rows: 1}\n"
                + "      |partitionRatio: 0/1, tabletRatio: 0/0");
    }

    @Test
    public void testDesensitizeExplain() throws Exception {
        connectContext.getSessionVariable().setEnableDesensitizeExplain(true);
        String sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 WHERE t0.v1 > 1000";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 1: v1 > ?\n");

        sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 and t0.v2 + t1.v5 > 1000";
        plan = getFragmentPlan(sql);
        assertContains(plan, "other join predicates: 2: v2 + 5: v5 > ?");

        sql = "SELECT MIN(pow(t0.v1, 2)) FROM t0";
        plan = getFragmentPlan(sql);
        assertContains(plan, "min(pow(CAST(1: v1 AS DOUBLE), ?))");

    }

    @Test
    public void testExplainCosts() throws Exception {
        String sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4";
        String plan = getCostExplain(sql);
        Assertions.assertTrue(plan.contains("3:AGGREGATE (merge finalize)\n" +
                "  |  group by: [1: v1, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * v1-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                "  |  \n" +
                "  2:EXCHANGE\n" +
                "     distribution type: SHUFFLE\n" +
                "     partition exprs: [1: v1, BIGINT, true]\n" +
                "     cardinality: 1"), plan);
        Assertions.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: [1: v1, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * v1-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: t0, rollup: t0\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=1.0\n" +
                "     cardinality: 1\n" +
                "     column statistics: \n" +
                "     * v1-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"), plan);
    }

    @Test
    public void testExplainCostsWithLabels() throws Exception {
        String sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4";
        String plan = getCostExplainWithLabels(sql);
        Assertions.assertTrue(plan.contains("3:AGGREGATE (merge finalize)\n" +
                "  |  group by: [1: v1, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * v1-->[MIN: -Infinity, MAX: Infinity, NULLS: 0.0, ROS: 1.0, NDV: 1.0] UNKNOWN\n" +
                "  |  \n" +
                "  2:EXCHANGE\n" +
                "     distribution type: SHUFFLE\n" +
                "     partition exprs: [1: v1, BIGINT, true]\n" +
                "     cardinality: 1"), plan);
        Assertions.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: [1: v1, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * v1-->[MIN: -Infinity, MAX: Infinity, NULLS: 0.0, ROS: 1.0, NDV: 1.0] UNKNOWN\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: t0, rollup: t0\n" +
                "     preAggregation: on\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=1.0\n" +
                "     cardinality: 1\n" +
                "     column statistics: \n" +
                "     * v1-->[MIN: -Infinity, MAX: Infinity, NULLS: 0.0, ROS: 1.0, NDV: 1.0] UNKNOWN"), plan);
    }
}
