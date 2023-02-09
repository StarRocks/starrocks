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
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetNamesVar;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetVar;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SetExecutorTest {
    private ConnectContext ctx;

    @Mocked
    private Auth auth;

    @Before
    public void setUp() throws DdlException {
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setQualifiedUser("root");
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUser = new UserIdentity("root", "192.168.1.1");
        currentUser.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setThreadLocalInfo();

        new Expectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.setPassword((SetPassVar) any);
                minTimes = 0;
            }
        };
    }

    @Test
    public void testNormal() throws UserException {
        List<SetVar> vars = Lists.newArrayList();
        vars.add(new SetPassVar(new UserIdentity("testUser", "%"), "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B"));
        vars.add(new SetNamesVar("utf8"));
        vars.add(new SetVar("query_timeout", new IntLiteral(10L)));

        SetStmt stmt = new SetStmt(vars);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);

        executor.execute();
    }

    @Test
    public void testSetSessionAndGlobal(@Mocked EditLog editLog) throws Exception {
        new Expectations(editLog) {
            {
                editLog.logGlobalVariableV2((GlobalVarPersistInfo) any);
                minTimes = 1;
            }
        };
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        String globalSQL = "set global query_timeout = 10";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(globalSQL, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(null, ctx.getModifiedSessionVariables());
        Assert.assertEquals(10, ctx.sessionVariable.getQueryTimeoutS());

        String sessionSQL = "set query_timeout = 9";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sessionSQL, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(1, ctx.getModifiedSessionVariables().getSetVars().size());
        Assert.assertEquals(9, ctx.sessionVariable.getQueryTimeoutS());
    }

    public void testUserVariableImp(LiteralExpr value, Type type) throws Exception {
        String sql = String.format("set @var = cast(%s as %s)", value.toSql(), type.toSql());
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        UserVariable userVariable = ctx.getUserVariables("var");
        Assert.assertTrue(userVariable.getResolvedExpression().getType().matchesType(type));
        Assert.assertEquals(value.getStringValue(), userVariable.getResolvedExpression().getStringValue());
        String planFragment = UtFrameUtils.getPlanAndFragment(ctx, "select @var").second.
                getExplainString(TExplainLevel.NORMAL);
        Assert.assertTrue(planFragment.contains(value.getStringValue()));
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
        String sql = "set @var = cast(10 as decimal)";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        UserVariable userVariable = ctx.getUserVariables("var");
        Assert.assertTrue(userVariable.getResolvedExpression().getType().isDecimalV3());
        Assert.assertEquals("10", userVariable.getResolvedExpression().getStringValue());

        sql = "set @var = cast(1 as boolean)";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        userVariable = ctx.getUserVariables("var");
        Assert.assertTrue(userVariable.getResolvedExpression().getType().isBoolean());
        BoolLiteral literal = (BoolLiteral) userVariable.getResolvedExpression();
        Assert.assertTrue(literal.getValue());

        sql = "set @var = cast(0 as boolean)";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        userVariable = ctx.getUserVariables("var");
        Assert.assertTrue(userVariable.getResolvedExpression().getType().isBoolean());
        literal = (BoolLiteral) userVariable.getResolvedExpression();
        Assert.assertFalse(literal.getValue());
    }

    @Test
    public void testJSONVariable() throws Exception {
        String json = "'{\"xxx\" : 1}'";
        Type type = Type.JSON;
        String sql = String.format("set @var = cast(%s as %s)", json, type.toSql());
        UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

    @Test
    public void testSetDefault() throws Exception {
        String sql = "set query_timeout=DEFAULT";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetStmtAnalyzer.analyze(stmt, ctx);
        Assert.assertEquals("SET `query_timeout` = 300", AstToSQLBuilder.toSQL(stmt));
    }
}