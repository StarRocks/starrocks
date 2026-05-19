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

import com.starrocks.common.Config;
import com.starrocks.sql.Explain;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
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
    public void testExplainCostsMockSyntax() throws Exception {
        String sql = "EXPLAIN COSTS MOCK SELECT t0.v1 FROM t0";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertTrue(stmt.isExplain());
        Assertions.assertEquals(StatementBase.ExplainLevel.COSTS, stmt.getExplainLevel());
        Assertions.assertTrue(stmt.isMockColumnNames());

        sql = "EXPLAIN COSTS SELECT t0.v1 FROM t0";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertTrue(stmt.isExplain());
        Assertions.assertEquals(StatementBase.ExplainLevel.COSTS, stmt.getExplainLevel());
        Assertions.assertFalse(stmt.isMockColumnNames());

        sql = "EXPLAIN MOCK SELECT t0.v1 FROM t0";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertTrue(stmt.isExplain());
        Assertions.assertTrue(stmt.isMockColumnNames());
    }

    @Test
    public void testExplainCostsMockRewriterPlan() throws Exception {
        // Rewriter substitutes every column reference in the rendered cost plan
        // with a stable `mock_col_<N>` and leaves table/keyword names alone.
        String sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4";
        ExecPlan execPlan = getExecPlan(sql);
        String plan = execPlan.getExplainString(com.starrocks.thrift.TExplainLevel.COSTS);
        ExplainMockRewriter rewriter = new ExplainMockRewriter(execPlan.getColumnRefFactory());

        String rewritten = rewriter.rewrite(plan);
        Assertions.assertTrue(rewritten.contains("mock_col_"), rewritten);
        // Column names from t0 and t1 should be gone from the substituted output.
        Assertions.assertFalse(java.util.regex.Pattern.compile("\\bv1\\b").matcher(rewritten).find(),
                rewritten);
        Assertions.assertFalse(java.util.regex.Pattern.compile("\\bv4\\b").matcher(rewritten).find(),
                rewritten);
        // Table names must still be present.
        Assertions.assertTrue(rewritten.contains("table: t0"), rewritten);
    }

    @Test
    public void testExplainCostsMockRewriterSql() throws Exception {
        // Running the rewriter against AstToSQLBuilder output yields a SQL where
        // every column reference matches the names used in the mocked plan.
        String sql = "SELECT t0.v1 FROM t0 WHERE t0.v2 > 1";
        StatementBase parsedStmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        ExecPlan execPlan = getExecPlan(sql);
        ExplainMockRewriter rewriter = new ExplainMockRewriter(execPlan.getColumnRefFactory());

        String mockedSql = rewriter.rewrite(
                com.starrocks.sql.analyzer.AstToSQLBuilder.toSQL(parsedStmt));
        Assertions.assertTrue(mockedSql.contains("mock_col_"), mockedSql);
        Assertions.assertFalse(java.util.regex.Pattern.compile("\\bv1\\b").matcher(mockedSql).find(),
                mockedSql);
        Assertions.assertFalse(java.util.regex.Pattern.compile("\\bv2\\b").matcher(mockedSql).find(),
                mockedSql);
        // Same column gets the same mock name in the plan as in the SQL, so
        // intersecting both rewrites must yield matching identifiers.
        String mockedPlan = rewriter.rewrite(
                execPlan.getExplainString(com.starrocks.thrift.TExplainLevel.COSTS));
        for (String mock : rewriter.getMapping().values()) {
            if (mockedSql.contains(mock)) {
                Assertions.assertTrue(mockedPlan.contains(mock),
                        "expected " + mock + " in plan:\n" + mockedPlan);
            }
        }
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

    @Test
    public void testExplainUsesConfiguredDefaultLevel() throws Exception {
        String originalLevel = Config.query_explain_level;
        Config.query_explain_level = "LOGICAL";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(
                    "EXPLAIN SELECT * FROM t0", connectContext);
            Assertions.assertTrue(statementBase instanceof QueryStatement);
            QueryStatement queryStatement = (QueryStatement) statementBase;
            Assertions.assertTrue(queryStatement.isExplain());
            Assertions.assertEquals(StatementBase.ExplainLevel.LOGICAL, queryStatement.getExplainLevel());
        } finally {
            Config.query_explain_level = originalLevel;
        }

        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(
                "EXPLAIN SELECT * FROM t0", connectContext);
        Assertions.assertTrue(statementBase instanceof QueryStatement);
        QueryStatement queryStatement = (QueryStatement) statementBase;
        Assertions.assertTrue(queryStatement.isExplain());
        Assertions.assertEquals(StatementBase.ExplainLevel.NORMAL, queryStatement.getExplainLevel());

    }
}
