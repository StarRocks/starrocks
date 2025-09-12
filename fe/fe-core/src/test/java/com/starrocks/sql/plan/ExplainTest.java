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
}
