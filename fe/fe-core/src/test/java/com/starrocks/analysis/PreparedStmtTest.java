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

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeInPredicate;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PreparedStmtTest{
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    private static String createTable = "CREATE TABLE `prepare_stmt` (\n" +
            "  `c0` varchar(24) NOT NULL COMMENT \"\",\n" +
            "  `c1` decimal128(24, 5) NOT NULL COMMENT \"\",\n" +
            "  `c2` decimal128(24, 2) NOT NULL COMMENT \"\"\n" +
            ") ENGINE=OLAP \n" +
            "DUPLICATE KEY(`c0`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`c0`) BUCKETS 1 \n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"DEFAULT\",\n" +
            "\"enable_persistent_index\" = \"true\",\n" +
            "\"replicated_storage\" = \"true\",\n" +
            "\"compression\" = \"LZ4\"\n" +
            "); ";


    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("demo").useDatabase("demo");
        starRocksAssert.withTable(createTable);
        starRocksAssert.withTable(createTable.replace("`prepare_stmt`", "`prepared_policy_source`"));
    }

    @Test
    public void testParser() throws Exception {
        String sql1 = "PREPARE stmt2 FROM select * from demo.prepare_stmt where c1 = ? and c2 = ?;";
        String sql2 = "PREPARE stmt3 FROM 'select * from demo.prepare_stmt';";
        String sql3 = "execute stmt3;";
        String sql4 = "execute stmt2 using @i;";

        PrepareStmt stmt1 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        PrepareStmt stmt2 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Assertions.assertEquals(2, stmt1.getParameters().size());
        Assertions.assertEquals(0, stmt2.getParameters().size());
        Assertions.assertThrows(StarRocksPlannerException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql3, ctx));

        ctx.putPreparedStmt("stmt2", new PrepareStmtContext(stmt2, ctx, null));
        Assertions.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql4, ctx));
    }

    @Test
    public void testIsQuery() throws Exception {
        String selectSql = "select * from demo.prepare_stmt";
        QueryStatement queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(selectSql, ctx);
        Assertions.assertEquals(true, ctx.isQueryStmt(queryStatement));

        String prepareSql = "PREPARE stmt FROM select * from demo.prepare_stmt";
        PrepareStmt prepareStmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(prepareSql, ctx);
        Assertions.assertEquals(false, ctx.isQueryStmt(prepareStmt));

        ctx.putPreparedStmt("stmt", new PrepareStmtContext(prepareStmt, ctx, null));
        Assertions.assertEquals(true, ctx.isQueryStmt(new ExecuteStmt("stmt", null)));
        Assertions.assertEquals(false, ctx.isQueryStmt(new ExecuteStmt("stmt1", null)));
    }

    @Test
    public void testPrepareEnable() {
        ctx.getSessionVariable().setEnablePrepareStmt(false);
        String prepareSql = "PREPARE stmt1 FROM insert into demo.prepare_stmt values (?, ?, ?, ?);";
        String executeSql = "execute stmt1 using @i, @i;";
        Assertions.assertThrows(StarRocksPlannerException.class, () -> starRocksAssert.query(prepareSql).explainQuery());
        Assertions.assertThrows(StarRocksPlannerException.class, () -> starRocksAssert.query(executeSql).explainQuery());
        ctx.getSessionVariable().setEnablePrepareStmt(true);
        assertDoesNotThrow(() -> starRocksAssert.query(prepareSql));

        // TODO support forward leader for fe
        StatementBase statement = SqlParser.parse(prepareSql, ctx.getSessionVariable()).get(0);
        StmtExecutor executor = new StmtExecutor(ctx, statement);
        Assertions.assertFalse(executor.isForwardToLeader());
    }

    @Test
    public void testPrepareWithSelectConst() throws Exception {
        String sql = "PREPARE stmt1 FROM select ?, ?, ?;";
        PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assertions.assertEquals(3, stmt.getParameters().size());

        HashSet<Integer> idSet = new HashSet<Integer>();
        for (Expr expr : stmt.getParameters()) {
            Assertions.assertEquals(true, idSet.add(expr.hashCode()));
        }

        Assertions.assertEquals(false, stmt.getParameters().get(0).equals(stmt.getParameters().get(1)));
        Assertions.assertEquals(false, stmt.getParameters().get(1).equals(stmt.getParameters().get(2)));
        Assertions.assertEquals(false, stmt.getParameters().get(0).equals(stmt.getParameters().get(2)));
    }

    @Test
    public void testExecutionAstIsFreshAndMetadataAstIsImmutable() throws Exception {
        String sql = "PREPARE fresh_stmt FROM 'select c0 from prepare_stmt where c1 = ?'";
        PrepareStmt metadataStmt = (PrepareStmt) SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        PrepareStmtContext prepareContext = new PrepareStmtContext(metadataStmt, ctx, null);

        String preparedDatabase = ctx.getDatabase();
        boolean originalAliasMode = ctx.isRelationAliasCaseInsensitive();
        boolean callerAliasMode = !originalAliasMode;
        Object catalogModification = ctx.getModifiedSessionVariablesMap().get("catalog");
        ctx.setDatabase("a_different_database");
        ctx.setRelationAliasCaseInSensitive(callerAliasMode);
        try {
            PrepareStmt first = prepareContext.instantiate(List.of(new IntLiteral(11)));
            PrepareStmt second = prepareContext.instantiate(List.of(new IntLiteral(22)));

            Assertions.assertNotSame(metadataStmt, first);
            Assertions.assertNotSame(first, second);
            Assertions.assertNotSame(metadataStmt.getInnerStmt(), first.getInnerStmt());
            Assertions.assertNotSame(first.getInnerStmt(), second.getInnerStmt());
            Assertions.assertNotSame(first.getParameters().get(0), second.getParameters().get(0));
            Assertions.assertEquals(11, ((IntLiteral) first.getParameters().get(0).getExpr()).getValue());
            Assertions.assertEquals(22, ((IntLiteral) second.getParameters().get(0).getExpr()).getValue());
            Assertions.assertNull(metadataStmt.getParameters().get(0).getExpr());
            Assertions.assertTrue(prepareContext.getBoundSqlForAudit(List.of(new IntLiteral(33))).contains("33"));
            Assertions.assertNull(metadataStmt.getParameters().get(0).getExpr());

            ctx.putPreparedStmt("fresh_stmt", prepareContext);
            GeneratedPreparedPlan generated = generatePreparedPlan("fresh_stmt", new IntLiteral(44));
            SelectRelation plannedSelect = (SelectRelation) ((QueryStatement) generated.executor.getParsedStmt())
                    .getQueryRelation();
            TableRelation plannedTable = (TableRelation) plannedSelect.getRelation();
            Assertions.assertEquals(preparedDatabase, plannedTable.getName().getDb());
            Assertions.assertEquals("a_different_database", ctx.getDatabase(),
                    "EXECUTE must restore the caller's current database after planning");
            Assertions.assertEquals(callerAliasMode, ctx.isRelationAliasCaseInsensitive());
            Assertions.assertSame(catalogModification, ctx.getModifiedSessionVariablesMap().get("catalog"),
                    "Temporary prepared catalog changes must not become forwarded session state");
        } finally {
            ctx.removePreparedStmt("fresh_stmt");
            ctx.setDatabase(preparedDatabase);
            ctx.setRelationAliasCaseInSensitive(originalAliasMode);
        }
    }

    @Test
    public void testExecutionReparseUsesPreparedParserSnapshotWithoutLeakingState() {
        boolean originalLargeIn = ctx.getSessionVariable().enableLargeInPredicate();
        int originalThreshold = ctx.getSessionVariable().getLargeInPredicateThreshold();
        boolean originalAliasMode = ctx.isRelationAliasCaseInsensitive();
        String originalDatabase = ctx.getDatabase();
        try {
            ctx.getSessionVariable().setEnableLargeInPredicate(true);
            ctx.getSessionVariable().setLargeInPredicateThreshold(2);
            String sql = "select c0 from prepare_stmt where c0 in ('a', 'b')";
            StatementBase query = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            PrepareStmt metadataStmt = new PrepareStmt("parser_snapshot_stmt", query, Collections.emptyList());
            PrepareStmtContext prepareContext = new PrepareStmtContext(
                    metadataStmt, ctx, null, new OriginStatement(sql, 0));

            ctx.getSessionVariable().setEnableLargeInPredicate(false);
            ctx.setDatabase("a_different_database");
            ctx.setRelationAliasCaseInSensitive(!originalAliasMode);
            PrepareStmt executable = prepareContext.instantiate(Collections.emptyList());
            SelectRelation select = (SelectRelation) ((QueryStatement) executable.getInnerStmt()).getQueryRelation();

            Assertions.assertInstanceOf(LargeInPredicate.class, select.getPredicate());
            Assertions.assertFalse(ctx.getSessionVariable().enableLargeInPredicate());
            Assertions.assertEquals("a_different_database", ctx.getDatabase());
            Assertions.assertEquals(!originalAliasMode, ctx.isRelationAliasCaseInsensitive());
        } finally {
            ctx.getSessionVariable().setEnableLargeInPredicate(originalLargeIn);
            ctx.getSessionVariable().setLargeInPredicateThreshold(originalThreshold);
            ctx.setDatabase(originalDatabase);
            ctx.setRelationAliasCaseInSensitive(originalAliasMode);
        }
    }

    @Test
    public void testExecutionReparseUsesOriginalStatementWithoutRedactingCredentials() {
        String sql = "select 0; select * from files(\"path\"=\"s3://bucket/file\", "
                + "\"aws.s3.secret_key\"=\"secret_value\")";
        List<StatementBase> statements = SqlParser.parse(sql, ctx.getSessionVariable());
        PrepareStmt metadataStmt = new PrepareStmt("credential_stmt", statements.get(1), Collections.emptyList());
        PrepareStmtContext prepareContext = new PrepareStmtContext(
                metadataStmt, ctx, null, new OriginStatement(sql, 1));

        PrepareStmt executable = prepareContext.instantiate(Collections.emptyList());
        SelectRelation select = (SelectRelation) ((QueryStatement) executable.getInnerStmt()).getQueryRelation();
        FileTableFunctionRelation files = (FileTableFunctionRelation) select.getRelation();
        Assertions.assertEquals("secret_value", files.getProperties().get("aws.s3.secret_key"));
    }

    @Test
    public void testPreparedExecutionReevaluatesChangedPolicy() throws Exception {
        String name = "dynamic_policy_stmt";
        AtomicReference<String> currentPolicy = new AtomicReference<>("prepare_policy");

        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
            authorizer.when(() -> Authorizer.getColumnMaskingPolicy(
                            Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> Map.of("c0", new StringLiteral(currentPolicy.get())));
            authorizer.when(() -> Authorizer.getRowAccessPolicy(Mockito.any(), Mockito.any()))
                    .thenReturn(null);

            // Exercise the real PREPARE lifecycle: metadata analysis is allowed to rewrite its
            // working AST, while StmtExecutor must retain an untouched source for EXECUTE.
            PrepareStmt metadataStmt = (PrepareStmt) SqlParser.parse(
                    "PREPARE " + name + " FROM 'select c0 from prepare_stmt where c0 = ?'",
                    ctx.getSessionVariable()).get(0);
            new StmtExecutor(ctx, metadataStmt).execute();
            PrepareStmtContext prepareContext = ctx.getPreparedStmt(name);
            Assertions.assertNotNull(prepareContext);
            Assertions.assertTrue(AstToSQLBuilder.toSQL(metadataStmt.getInnerStmt()).contains("prepare_policy"));

            currentPolicy.set("execute_policy_1");
            GeneratedPreparedPlan firstPlan = generatePreparedPlan(name, new StringLiteral("first"));
            StatementBase first = firstPlan.executor.getParsedStmt();
            String firstSql = AstToSQLBuilder.toSQL(first);

            currentPolicy.set("execute_policy_2");
            GeneratedPreparedPlan secondPlan = generatePreparedPlan(name, new StringLiteral("second"));
            StatementBase second = secondPlan.executor.getParsedStmt();
            String secondSql = AstToSQLBuilder.toSQL(second);

            Assertions.assertNotSame(first, second);
            Assertions.assertTrue(firstSql.contains("execute_policy_1"), firstSql);
            Assertions.assertTrue(secondSql.contains("execute_policy_2"), secondSql);
            String firstLogicalPlan = logicalPlan(firstPlan.execPlan);
            String secondLogicalPlan = logicalPlan(secondPlan.execPlan);
            Assertions.assertTrue(firstLogicalPlan.contains("execute_policy_1"), firstLogicalPlan);
            Assertions.assertTrue(secondLogicalPlan.contains("execute_policy_2"), secondLogicalPlan);
            Assertions.assertFalse(prepareContext.isCached());
            Assertions.assertNull(metadataStmt.getParameters().get(0).getExpr());
        } finally {
            ctx.removePreparedStmt(name);
        }
    }

    @Test
    public void testPreparedPointQueryKeepsNoPolicyFastPath() throws Exception {
        String name = "cached_policy_free_stmt";
        PrepareStmt metadataStmt = (PrepareStmt) SqlParser.parse(
                "PREPARE " + name + " FROM 'select c0 from prepare_stmt where c0 = ?'",
                ctx.getSessionVariable()).get(0);
        PrepareStmtContext prepareContext = new PrepareStmtContext(metadataStmt, ctx, null);
        ctx.putPreparedStmt(name, prepareContext);
        AtomicBoolean denyCachedExecution = new AtomicBoolean(false);

        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
            authorizer.when(() -> Authorizer.getColumnMaskingPolicy(
                            Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.emptyMap());
            authorizer.when(() -> Authorizer.getRowAccessPolicy(Mockito.any(), Mockito.any()))
                    .thenReturn(null);
            authorizer.when(() -> Authorizer.check(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        if (denyCachedExecution.get()) {
                            throw new SemanticException("prepared privilege was revoked");
                        }
                        return null;
                    });

            GeneratedPreparedPlan firstPlan = generatePreparedPlan(name, new StringLiteral("first"));
            Assertions.assertTrue(prepareContext.isCached());
            ExecPlan cachedExecPlan = prepareContext.getExecPlan();
            StatementBase first = firstPlan.executor.getParsedStmt();

            GeneratedPreparedPlan secondPlan = generatePreparedPlan(name, new StringLiteral("second"));
            Assertions.assertSame(first, secondPlan.executor.getParsedStmt());
            Assertions.assertSame(cachedExecPlan, prepareContext.getExecPlan());
            Assertions.assertTrue(secondPlan.execPlan.getExplainString(TExplainLevel.NORMAL).contains("second"));
            Assertions.assertTrue(prepareContext.isCached());
            authorizer.verify(() -> Authorizer.check(Mockito.any(), Mockito.any()), Mockito.times(2));

            denyCachedExecution.set(true);
            AnalysisException denied = Assertions.assertThrows(AnalysisException.class,
                    () -> generatePreparedPlan(name, new StringLiteral("denied")));
            Assertions.assertTrue(denied.getMessage().contains("prepared privilege was revoked"));
        } finally {
            ctx.removePreparedStmt(name);
        }
    }

    @Test
    public void testCachedPreparedPlanIsInvalidatedWhenPolicyAppears() throws Exception {
        String name = "policy_added_after_cache_stmt";
        PrepareStmt metadataStmt = (PrepareStmt) SqlParser.parse(
                "PREPARE " + name + " FROM 'select c0 from prepare_stmt where c0 = ?'",
                ctx.getSessionVariable()).get(0);
        PrepareStmtContext prepareContext = new PrepareStmtContext(metadataStmt, ctx, null);
        ctx.putPreparedStmt(name, prepareContext);
        AtomicBoolean policyEnabled = new AtomicBoolean(false);

        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
            authorizer.when(() -> Authorizer.getColumnMaskingPolicy(
                            Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(Collections.emptyMap());
            authorizer.when(() -> Authorizer.getRowAccessPolicy(Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> policyEnabled.get() ? new BoolLiteral(false) : null);

            GeneratedPreparedPlan firstPlan = generatePreparedPlan(name, new StringLiteral("first"));
            Assertions.assertTrue(prepareContext.isCached());

            policyEnabled.set(true);
            GeneratedPreparedPlan secondPlan = generatePreparedPlan(name, new StringLiteral("second"));
            Assertions.assertNotSame(firstPlan.executor.getParsedStmt(), secondPlan.executor.getParsedStmt());
            Assertions.assertTrue(AstToSQLBuilder.toSQL(secondPlan.executor.getParsedStmt()).contains("WHERE FALSE"));
            String secondLogicalPlan = logicalPlan(secondPlan.execPlan);
            Assertions.assertTrue(secondLogicalPlan.toLowerCase().contains("false"), secondLogicalPlan);
            Assertions.assertFalse(prepareContext.isCached());
        } finally {
            ctx.removePreparedStmt(name);
        }
    }

    @Test
    public void testPreparedPointQueryWithScalarSubqueryIsNotCached() throws Exception {
        String name = "nested_policy_added_after_cache_stmt";
        PrepareStmt metadataStmt = (PrepareStmt) SqlParser.parse(
                "PREPARE " + name + " FROM 'select (select max(c0) from prepared_policy_source) "
                        + "from prepare_stmt where c0 = ?'",
                ctx.getSessionVariable()).get(0);
        PrepareStmtContext prepareContext = new PrepareStmtContext(metadataStmt, ctx, null);
        ctx.putPreparedStmt(name, prepareContext);
        AtomicBoolean policyEnabled = new AtomicBoolean(false);

        try (MockedStatic<Authorizer> authorizer = Mockito.mockStatic(Authorizer.class)) {
            authorizer.when(() -> Authorizer.getColumnMaskingPolicy(
                            Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> policyEnabled.get()
                            && "prepared_policy_source".equals(invocation.getArgument(1, com.starrocks.catalog.TableName.class)
                            .getTbl()) ? Map.of("c0", new StringLiteral("nested_policy")) : Collections.emptyMap());
            authorizer.when(() -> Authorizer.getRowAccessPolicy(Mockito.any(), Mockito.any()))
                    .thenReturn(null);

            generatePreparedPlan(name, new StringLiteral("first"));
            Assertions.assertFalse(prepareContext.isCached(),
                    "The point-query cache cannot safely replan scans inside a scalar subquery");

            policyEnabled.set(true);
            GeneratedPreparedPlan secondPlan = generatePreparedPlan(name, new StringLiteral("second"));
            String secondSql = AstToSQLBuilder.toSQL(secondPlan.executor.getParsedStmt());
            Assertions.assertTrue(secondSql.contains("nested_policy"), secondSql);
            Assertions.assertFalse(prepareContext.isCached());
        } finally {
            ctx.removePreparedStmt(name);
        }
    }

    private GeneratedPreparedPlan generatePreparedPlan(String name, Expr value) throws Exception {
        ExecuteStmt executeStmt = new ExecuteStmt(name, List.of(value));
        executeStmt.setOrigStmt(new OriginStatement("EXECUTE " + name, 0));
        StmtExecutor executor = new StmtExecutor(ctx, executeStmt);
        ExecPlan execPlan = Deencapsulation.invoke(executor, "generateExecPlan");
        return new GeneratedPreparedPlan(executor, execPlan);
    }

    private static String logicalPlan(ExecPlan execPlan) {
        return LogicalPlanPrinter.print(execPlan.getLogicalPlan().getRoot(), true, true);
    }

    private static class GeneratedPreparedPlan {
        private final StmtExecutor executor;
        private final ExecPlan execPlan;

        private GeneratedPreparedPlan(StmtExecutor executor, ExecPlan execPlan) {
            this.executor = executor;
            this.execPlan = execPlan;
        }
    }

    @Test
    public void testPrepareStatementParser() {
        String sql = "PREPARE stmt1 FROM insert into demo.prepare_stmt values (?, ?, ?, ?);";
        Exception e = assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        assertEquals("Getting analyzing error. Detail message: This command is not supported in the " +
                "prepared statement protocol yet.", e.getMessage());
    }

    @Test
    public void testPrepareStatementParserWithHavingClause() {
        String sql = "PREPARE stmt1 FROM SELECT prepare_stmt.c0 from prepare_stmt GROUP BY prepare_stmt.c0 HAVING COUNT(*) = ?";
        try {
            PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            Assertions.fail("should not reach here");
        }

        sql = "PREPARE stmt1 FROM SELECT prepare_stmt.c0 from prepare_stmt GROUP BY prepare_stmt.c0 HAVING c0 > ?";
        try {
            PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            Assertions.fail("should not reach here");
        }
    }

    @Test
    public void testPrepareStmtWithCte() throws Exception {
        String sql = "PREPARE stmt FROM with cte as (select * from prepare_stmt where c0 = ?) select * from cte where c1 = ?";
        PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        QueryStatement queryStmt = (QueryStatement) stmt.getInnerStmt();
        Assertions.assertTrue(stmt.getParameters().get(1) ==
                ((SelectRelation) queryStmt.getQueryRelation()).getPredicate().getChild(1));

        sql = "PREPARE stmt FROM select *, ? from (with cte as " +
                "(select * from prepare_stmt where c0 = ?) select * from cte where c1 = ?) t where c2 = ?";
        stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        queryStmt = (QueryStatement) stmt.getInnerStmt();
        Assertions.assertTrue(stmt.getParameters().get(0) ==
                ((SelectRelation) queryStmt.getQueryRelation()).getSelectList().getItems().get(1).getExpr());
        Assertions.assertTrue(stmt.getParameters().get(3) ==
                ((SelectRelation) queryStmt.getQueryRelation()).getPredicate().getChild(1));
    }

}
