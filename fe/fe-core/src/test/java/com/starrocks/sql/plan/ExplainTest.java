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

<<<<<<< HEAD
import com.starrocks.common.Config;
import com.starrocks.sql.Explain;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
=======
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.Explain;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
>>>>>>> 9049219645b... [BugFix][RB-727] Fix StackOverflowError in EXPLAIN LOGICAL for operators without a dedicated printer (#66835)
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

<<<<<<< HEAD
=======
import java.util.List;
import java.util.Map;

>>>>>>> 9049219645b... [BugFix][RB-727] Fix StackOverflowError in EXPLAIN LOGICAL for operators without a dedicated printer (#66835)
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
        try {
            String sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 WHERE t0.v1 > 1000";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 > ?\n");

            sql = "SELECT DISTINCT t0.v1 FROM t0 LEFT JOIN t1 ON t0.v1 = t1.v4 and t0.v2 + t1.v5 > 1000";
            plan = getFragmentPlan(sql);
            assertContains(plan, "other join predicates: 2: v2 + 5: v5 > ?");

            sql = "SELECT MIN(pow(t0.v1, 2)) FROM t0";
            plan = getFragmentPlan(sql);
            assertContains(plan, "min(pow(CAST(1: v1 AS DOUBLE), ?))");
        } finally {
            connectContext.getSessionVariable().setEnableDesensitizeExplain(false);
        }
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
<<<<<<< HEAD
=======

    @Test
    public void testExplainTypesIsQueryOnly() throws Exception {
        for (String sql : List.of(
                "EXPLAIN TYPES SELECT 1",
                "DESC TYPES SELECT 1",
                "DESCRIBE TYPES SELECT 1",
                "EXPLAIN TYPES WITH t AS (SELECT 1 AS c) SELECT c FROM t",
                "EXPLAIN TYPES SELECT 1 UNION ALL SELECT 2")) {
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.assertTrue(statement.isExplain());
            Assertions.assertEquals(StatementBase.ExplainLevel.TYPES, statement.getExplainLevel());
        }

        for (String sql : List.of(
                "EXPLAIN TYPES INSERT INTO t0 SELECT 1, 2, 3",
                "INSERT INTO t0 EXPLAIN TYPES SELECT 1, 2, 3",
                "DESC TYPES UPDATE t0 SET v1 = 1",
                "DESCRIBE TYPES DELETE FROM t0",
                "EXPLAIN TYPES MERGE INTO t0 AS t USING t1 AS s ON t.v1 = s.v4 WHEN MATCHED THEN UPDATE SET v2 = s.v5",
                "EXPLAIN TYPES REFRESH MATERIALIZED VIEW mv")) {
            AnalysisException e = Assertions.assertThrows(
                    AnalysisException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
            Assertions.assertTrue(e.getMessage().contains("TYPES is only supported for queries"), sql + ": " + e.getMessage());
        }
    }

    @Test
    public void testTypesCannotBeConfiguredAsDefaultExplainLevel() throws Exception {
        String originalLevel = Config.query_explain_level;
        Config.query_explain_level = "TYPES";
        try {
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "EXPLAIN SELECT 1", connectContext);
            Assertions.assertEquals(StatementBase.ExplainLevel.NORMAL, statement.getExplainLevel());
        } finally {
            Config.query_explain_level = originalLevel;
        }
    }

    @Test
    public void testExplainTypesReturnsFinalOutputSchemaWithoutExecution() throws Exception {
        for (String prefix : List.of("EXPLAIN", "DESC", "DESCRIBE")) {
            connectContext.getState().reset();
            connectContext.setQueryId(UUIDUtil.genUUID());
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(
                    prefix + " TYPES SELECT CAST(1 AS DECIMAL(38, 12)) AS duplicate_name, "
                            + "NULL AS duplicate_name",
                    connectContext);
            StmtExecutor executor = new StmtExecutor(connectContext, statement);
            executor.setProxy();

            executor.execute();

            ShowResultSet resultSet = executor.getProxyResultSet();
            Assertions.assertNotNull(resultSet);
            Assertions.assertEquals(List.of(
                    List.of("duplicate_name", "DECIMAL128(38,12)", "0"),
                    List.of("duplicate_name", "BOOLEAN", "1")), resultSet.getResultRows());
            Assertions.assertEquals(VarcharType.VARCHAR, resultSet.getMetaData().getColumn(0).getType());
            Assertions.assertEquals(VarcharType.VARCHAR, resultSet.getMetaData().getColumn(1).getType());
            Assertions.assertEquals(BooleanType.BOOLEAN, resultSet.getMetaData().getColumn(2).getType());
            Assertions.assertNull(executor.getCoordinator());
        }
    }

    @Test
    public void testExplainTypesReturnsCopyableComplexTypes() throws Exception {
        connectContext.getState().reset();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(
                "EXPLAIN TYPES SELECT "
                        + "CAST(NULL AS ARRAY<DECIMAL(20, 4)>) AS amounts, "
                        + "CAST(NULL AS MAP<VARCHAR, ARRAY<BIGINT>>) AS attributes, "
                        + "CAST(NULL AS STRUCT<`order id` BIGINT>) AS record",
                connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, statement);
        executor.setProxy();

        executor.execute();

        Assertions.assertEquals(List.of(
                List.of("amounts", "ARRAY<DECIMAL128(20,4)>", "1"),
                List.of("attributes", "MAP<VARCHAR,ARRAY<BIGINT>>", "1"),
                List.of("record", "STRUCT<`order id` BIGINT>", "1")),
                executor.getProxyResultSet().getResultRows());
        Assertions.assertNull(executor.getCoordinator());
    }

    @Test
    public void testExplainTypesPreservesExactDecimalTypes() throws Exception {
        connectContext.getState().reset();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(
                "EXPLAIN TYPES SELECT "
                        + "CAST(NULL AS DECIMALV2(9, 2)) AS d2, "
                        + "CAST(NULL AS DECIMAL32(4, 1)) AS d32, "
                        + "CAST(NULL AS DECIMAL64(10, 2)) AS d64, "
                        + "CAST(NULL AS DECIMAL128(20, 4)) AS d128, "
                        + "CAST(NULL AS DECIMAL256(40, 6)) AS d256",
                connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, statement);
        executor.setProxy();

        executor.execute();

        Assertions.assertEquals(List.of(
                List.of("d2", "DECIMALV2(9,2)", "1"),
                List.of("d32", "DECIMAL32(4,1)", "1"),
                List.of("d64", "DECIMAL64(10,2)", "1"),
                List.of("d128", "DECIMAL128(20,4)", "1"),
                List.of("d256", "DECIMAL256(40,6)", "1")),
                executor.getProxyResultSet().getResultRows());
        Assertions.assertNull(executor.getCoordinator());
    }

    @Test
    public void testExplainTypesDoesNotReturnPartialSchemaOnPlanningFailure() throws Exception {
        connectContext.getState().reset();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StatementBase statement = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "EXPLAIN TYPES SELECT missing_column FROM t0",
                connectContext);
        StmtExecutor executor = new StmtExecutor(connectContext, statement);
        executor.setProxy();

        executor.execute();

        Assertions.assertTrue(connectContext.getState().isError());
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("missing_column"));
        Assertions.assertNull(executor.getProxyResultSet());
        Assertions.assertNull(executor.getCoordinator());
    }

    @Test
    public void testExplainLogicalIcebergEqualityDeleteScan() {
        IcebergTable table = Mockito.mock(IcebergTable.class);
        Mockito.when(table.getName()).thenReturn("eqd_repro");
        Column column = new Column("id", IntegerType.INT);
        ColumnRefOperator ref = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        LogicalIcebergEqualityDeleteScanOperator logicalScan = new LogicalIcebergEqualityDeleteScanOperator(
                table, Map.of(ref, column), Map.of(column, ref), -1, null);
        OptExpression root = OptExpression.create(new PhysicalIcebergEqualityDeleteScanOperator(logicalScan));

        String plan = new Explain(false, false, "    ", "    ").print(root, List.of(ref));
        assertContains(plan, "- ICEBERG-EQUALITY-DELETE-SCAN [eqd_repro] => [1:id]");
    }

    @Test
    public void testExplainLogicalRawValues() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        int threshold = sv.getLargeInPredicateThreshold();
        sv.setLargeInPredicateThreshold(3);
        try {
            String plan = getExecPlan("select * from t0 where v1 in (1, 2, 3, 4)")
                    .getExplainString(StatementBase.ExplainLevel.LOGICAL);
            assertContains(plan, "LEFT SEMI JOIN");
            assertContains(plan, "- RAW-VALUES");
        } finally {
            sv.setLargeInPredicateThreshold(threshold);
        }
    }

    @Test
    public void testExplainLogicalFetch() throws Exception {
        SessionVariable sv = connectContext.getSessionVariable();
        boolean glm = sv.isEnableGlobalLateMaterialization();
        boolean costBased = sv.isEnableGlobalLateMaterializationCostBased();
        sv.setEnableGlobalLateMaterialization(true);
        sv.setEnableGlobalLateMaterializationCostBased(false);
        try {
            String plan = getExecPlan("select * from supplier where S_SUPPKEY < 10 order by 1,2 limit 10")
                    .getExplainString(StatementBase.ExplainLevel.LOGICAL);
            assertContains(plan, "- FETCH");
            assertContains(plan, "- LOOKUP");
        } finally {
            sv.setEnableGlobalLateMaterialization(glm);
            sv.setEnableGlobalLateMaterializationCostBased(costBased);
        }
    }
>>>>>>> 9049219645b... [BugFix][RB-727] Fix StackOverflowError in EXPLAIN LOGICAL for operators without a dedicated printer (#66835)
}
