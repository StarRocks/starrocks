// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

@TestMethodOrder(MethodName.class)
public class SetExecutorTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        String createUserSql = "CREATE USER 'testUser' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);
        UserRef user = createUserStmt.getUser();
        testUser = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
    }

    private static void ctxToTestUser() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setQualifiedUser(testUser.getUser());
    }

    private static void ctxToRoot() {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getUser());
    }

    @Test
    public void testNormal() throws StarRocksException {
        List<SetListItem> vars = Lists.newArrayList();

        new SetPassVar(new UserRef("testUser", "%"),
                new UserAuthOption(null, "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B", false, NodePosition.ZERO),
                NodePosition.ZERO);
        vars.add(new SetNamesVar("utf8"));
        vars.add(new SystemVariable("query_timeout", new IntLiteral(10L)));

        SetStmt stmt = new SetStmt(vars);
        ctxToTestUser();
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, starRocksAssert.getCtx());
        SetExecutor executor = new SetExecutor(starRocksAssert.getCtx(), stmt);

        executor.execute();
    }

    @Test
    public void test1SetSessionAndGlobal() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant operate on system to testUser", ctx), ctx);
        ctxToTestUser();

        String globalSQL = "set global query_timeout = 10";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(globalSQL, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assertions.assertEquals(null, ctx.getModifiedSessionVariables());
        Assertions.assertEquals(10, ctx.sessionVariable.getQueryTimeoutS());

        String sessionSQL = "set query_timeout = 9";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sessionSQL, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assertions.assertEquals(1, ctx.getModifiedSessionVariables().getSetListItems().size());
        Assertions.assertEquals(9, ctx.sessionVariable.getQueryTimeoutS());

        ctx.modifyUserVariable(new UserVariable("test_b", new IntLiteral(1), true, NodePosition.ZERO));
        String userVarSql = "set @a = 10";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(userVarSql, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assertions.assertEquals(2, ctx.getModifiedSessionVariables().getSetListItems().size());
        Assertions.assertEquals("10", ctx.getModifiedSessionVariables().getSetListItems().get(1).toSql());
        ctx.getUserVariables().remove("test_b");
    }

    public void testUserVariableImp(LiteralExpr value, Type type) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = String.format("set @var = cast(%s as %s)", value.toSql(), type.toSql());
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        UserVariable userVariable = ctx.getUserVariable("var");
        Assertions.assertTrue(userVariable.getEvaluatedExpression().getType().matchesType(type));
        LiteralExpr literalExpr = (LiteralExpr) userVariable.getEvaluatedExpression();
        Assertions.assertEquals(value.getStringValue(), literalExpr.getStringValue());
        String planFragment = UtFrameUtils.getPlanAndFragment(ctx, "select @var").second.
                getExplainString(TExplainLevel.NORMAL);
        Assertions.assertTrue(planFragment.contains(value.getStringValue()));
    }

    @Test
    public void testUserDefineVariable() throws Exception {
        testUserVariableImp(new IntLiteral(1, Type.TINYINT), Type.TINYINT);
        testUserVariableImp(new IntLiteral(1, Type.TINYINT), Type.SMALLINT);
        testUserVariableImp(new IntLiteral(1, Type.INT), Type.INT);
        testUserVariableImp(new IntLiteral(1, Type.BIGINT), Type.BIGINT);
        testUserVariableImp(new LargeIntLiteral("1"), Type.LARGEINT);
        testUserVariableImp(new FloatLiteral(1D, Type.FLOAT), Type.FLOAT);
        testUserVariableImp(new FloatLiteral(1D, Type.DOUBLE), Type.DOUBLE);
        testUserVariableImp(new DateLiteral("2020-01-01", Type.DATE), Type.DATE);
        testUserVariableImp(new DateLiteral("2020-01-01 00:00:00", Type.DATETIME), Type.DATETIME);
        testUserVariableImp(new DecimalLiteral("1", Type.DECIMAL32_INT), Type.DECIMAL32_INT);
        testUserVariableImp(new DecimalLiteral("1", Type.DECIMAL64_INT), Type.DECIMAL64_INT);
        testUserVariableImp(new DecimalLiteral("1", Type.DECIMAL128_INT), Type.DECIMAL128_INT);
        testUserVariableImp(new StringLiteral("xxx"), ScalarType.createVarcharType(10));
    }

    @Test
    public void testUserDefineVariable2() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "set @var = cast(10 as decimal)";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        UserVariable userVariable = ctx.getUserVariable("var");
        Assertions.assertTrue(userVariable.getEvaluatedExpression().getType().isDecimalV3());
        LiteralExpr literalExpr = (LiteralExpr) userVariable.getEvaluatedExpression();
        Assertions.assertEquals("10", literalExpr.getStringValue());

        sql = "set @var = cast(1 as boolean)";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        userVariable = ctx.getUserVariable("var");
        Assertions.assertTrue(userVariable.getEvaluatedExpression().getType().isBoolean());
        BoolLiteral literal = (BoolLiteral) userVariable.getEvaluatedExpression();
        Assertions.assertTrue(literal.getValue());

        sql = "set @var = cast(0 as boolean)";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        userVariable = ctx.getUserVariable("var");
        Assertions.assertTrue(userVariable.getEvaluatedExpression().getType().isBoolean());
        literal = (BoolLiteral) userVariable.getEvaluatedExpression();
        Assertions.assertFalse(literal.getValue());
    }

    @Test
    public void testUserDefineVariable3() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "set @aVar = 5, @bVar = @aVar + 1, @cVar = @bVar + 1";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        UserVariable userVariableA = ctx.getUserVariable("aVar");
        UserVariable userVariableB = ctx.getUserVariable("bVar");
        UserVariable userVariableC = ctx.getUserVariable("cVar");
        Assertions.assertTrue(userVariableA.getEvaluatedExpression().getType().matchesType(Type.TINYINT));
        Assertions.assertTrue(userVariableB.getEvaluatedExpression().getType().matchesType(Type.SMALLINT));
        Assertions.assertTrue(userVariableC.getEvaluatedExpression().getType().matchesType(Type.INT));

        LiteralExpr literalExprA = (LiteralExpr) userVariableA.getEvaluatedExpression();
        Assertions.assertEquals("5", literalExprA.getStringValue());

        LiteralExpr literalExprB = (LiteralExpr) userVariableB.getEvaluatedExpression();
        Assertions.assertEquals("6", literalExprB.getStringValue());

        LiteralExpr literalExprC = (LiteralExpr) userVariableC.getEvaluatedExpression();
        Assertions.assertEquals("7", literalExprC.getStringValue());

        sql = "set @aVar = 6, @bVar = @aVar + 1, @cVar = @bVar + 1";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        userVariableA = ctx.getUserVariable("aVar");
        userVariableB = ctx.getUserVariable("bVar");
        userVariableC = ctx.getUserVariable("cVar");
        Assertions.assertTrue(userVariableA.getEvaluatedExpression().getType().matchesType(Type.TINYINT));
        Assertions.assertTrue(userVariableB.getEvaluatedExpression().getType().matchesType(Type.SMALLINT));
        Assertions.assertTrue(userVariableC.getEvaluatedExpression().getType().matchesType(Type.INT));
        literalExprA = (LiteralExpr) userVariableA.getEvaluatedExpression();
        Assertions.assertEquals("6", literalExprA.getStringValue());

        literalExprB = (LiteralExpr) userVariableB.getEvaluatedExpression();
        Assertions.assertEquals("7", literalExprB.getStringValue());

        literalExprC = (LiteralExpr) userVariableC.getEvaluatedExpression();
        Assertions.assertEquals("8", literalExprC.getStringValue());


        sql = "set @aVar = 5, @bVar = @aVar + 1, @cVar = @eVar + 1";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        userVariableA = ctx.getUserVariable("aVar");
        userVariableB = ctx.getUserVariable("bVar");
        userVariableC = ctx.getUserVariable("cVar");
        Assertions.assertTrue(userVariableA.getEvaluatedExpression().getType().matchesType(Type.TINYINT));
        Assertions.assertTrue(userVariableB.getEvaluatedExpression().getType().matchesType(Type.SMALLINT));
        Assertions.assertTrue(userVariableC.getEvaluatedExpression() instanceof NullLiteral);

        literalExprA = (LiteralExpr) userVariableA.getEvaluatedExpression();
        Assertions.assertEquals("5", literalExprA.getStringValue());

        literalExprB = (LiteralExpr) userVariableB.getEvaluatedExpression();
        Assertions.assertEquals("6", literalExprB.getStringValue());

        literalExprC = (LiteralExpr) userVariableC.getEvaluatedExpression();
        Assertions.assertEquals("NULL", literalExprC.getStringValue());

        try {
            sql = "set @fVar = 1, " +
                    "@abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz=2";
            stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            executor = new SetExecutor(ctx, stmt);
            executor.execute();
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getMessage().contains("User variable name " +
                    "'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz' is illegal"));
        }
        Assertions.assertTrue(ctx.getUserVariable("fVar") == null);

        ctx.getUserVariables().clear();
        for (int i = 0; i < 1023; ++i) {
            ctx.getUserVariables().put(String.valueOf(i), new UserVariable(null, null, null));
        }
        try {
            sql = "set @aVar = 6, @bVar = @aVar + 1, @cVar = @bVar + 1";
            stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            executor = new SetExecutor(ctx, stmt);
            executor.execute();
        } catch (SemanticException e) {
            Assertions.assertTrue(e.getMessage().contains("User variable exceeds the maximum limit of 1024"));
        }
        Assertions.assertFalse(ctx.getUserVariables().containsKey("aVar"));
        Assertions.assertFalse(ctx.getUserVariables().containsKey("bVar"));
        Assertions.assertFalse(ctx.getUserVariables().containsKey("cVar"));
        Assertions.assertTrue(ctx.getUserVariables().size() == 1023);
    }

    @Test
    public void testJSONVariable() throws Exception {
        String json = "'{\"xxx\" : 1}'";
        Type type = Type.JSON;
        String sql = String.format("set @var = cast(%s as %s)", json, type.toSql());
        UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
    }
}