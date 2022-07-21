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
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SetNamesVar;
import com.starrocks.analysis.SetPassVar;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class SetExecutorTest {
    private Analyzer analyzer;
    private ConnectContext ctx;

    @Mocked
    private Auth auth;

    @Before
    public void setUp() throws DdlException {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
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
    public void testNormal() throws UserException, AnalysisException, DdlException {
        List<SetVar> vars = Lists.newArrayList();
        vars.add(new SetPassVar(new UserIdentity("testUser", "%"), "*88EEBA7D913688E7278E2AD071FDB5E76D76D34B"));
        vars.add(new SetNamesVar("utf8"));
        vars.add(new SetVar("query_timeout", new IntLiteral(10L)));

        SetStmt stmt = new SetStmt(vars);
        stmt.analyze(analyzer);
        SetExecutor executor = new SetExecutor(ctx, stmt);

        executor.execute();
    }

    @Test
    public void testSetSessionAndGlobal() throws Exception {
        String globalSQL = "set global query_timeout = 10";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(globalSQL, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(null, ctx.getModifiedSessionVariables());
        Assert.assertEquals(10, ctx.sessionVariable.getQueryTimeoutS());


        String sessionSQL = "set query_timeout = 9";
        stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(globalSQL, ctx);
        executor = new SetExecutor(ctx, stmt);
        executor.execute();
        Assert.assertEquals(1, ctx.getModifiedSessionVariables().getSetVars().size());
        Assert.assertEquals(9, ctx.sessionVariable.getQueryTimeoutS());
    }

    @Test
    public void testSetSessionVars() throws Exception {
        String sql = "set global query_timeout = 10, time_zone = 'Asia/Chongqing'";
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        SetExecutor executor = new SetExecutor(ctx, stmt);
        executor.setSessionVars();
        Assert.assertEquals(1, ctx.getModifiedSessionVariables().getSetVars().size());
        Assert.assertEquals(10, ctx.sessionVariable.getQueryTimeoutS());
        Assert.assertEquals("Asia/Chongqing", ctx.sessionVariable.getTimeZone());

     }
}