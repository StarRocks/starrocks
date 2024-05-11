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

package com.starrocks.qe;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.rest.ExecuteSqlAction;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConnectionLimitTest {
    private static StarRocksAssert starRocksAssert;
    private static int connectionId = 100;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);

        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);

        createUserSql = "CREATE USER 'test01' IDENTIFIED BY ''";
        createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);
    }

    private ConnectContext createConnectContextForUser(String qualifiedName) {
        ConnectContext context = new ConnectContext();
        context.setQualifiedUser(qualifiedName);
        context.setCurrentUserIdentity(new UserIdentity(qualifiedName, "%"));
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setConnectionId(connectionId++);

        return context;
    }

    @Test
    public void testConnectionReachSystemLimit() {
        Config.qe_max_connection = 1;
        ExecuteEnv.setup();
        Pair<Boolean, String> result =
                ExecuteEnv.getInstance().getScheduler()
                        .registerConnection(createConnectContextForUser("test"));
        Assert.assertTrue(result.first);
        result = ExecuteEnv.getInstance().getScheduler()
                .registerConnection(createConnectContextForUser("test"));
        Assert.assertFalse(result.first);
        Assert.assertTrue(result.second.contains("Reach cluster-wide connection limit"));
    }


    @Test
    public void testConnectionReachUserLimit() throws Exception {
        Config.qe_max_connection = 100;
        ExecuteEnv.setup();
        String sql = "set property for 'test' 'max_user_connections' = '1'";
        SetUserPropertyStmt setUserPropertyStmt =
                (SetUserPropertyStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                .updateUserProperty("test", setUserPropertyStmt.getPropertyPairList());

        Pair<Boolean, String> result =
                ExecuteEnv.getInstance().getScheduler()
                        .registerConnection(createConnectContextForUser("test"));
        Assert.assertTrue(result.first);
        result = ExecuteEnv.getInstance().getScheduler()
                .registerConnection(createConnectContextForUser("test"));
        Assert.assertFalse(result.first);
        System.out.println(result.second);
        Assert.assertTrue(result.second.contains("Reach user-level"));
    }

    @Test
    public void testShowProcessListForUser() {
        String sql = "show processlist for 'test'";
        ExecuteEnv.setup();
        ExecuteEnv.getInstance().getScheduler().registerConnection(createConnectContextForUser("test"));
        ExecuteEnv.getInstance().getScheduler().registerConnection(createConnectContextForUser("test01"));
        ShowProcesslistStmt stmt = (ShowProcesslistStmt) com.starrocks.sql.parser.SqlParser.parse(sql,
                starRocksAssert.getCtx().getSessionVariable()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, starRocksAssert.getCtx());
        starRocksAssert.getCtx().setConnectScheduler(ExecuteEnv.getInstance().getScheduler());
        ShowResultSet resultSet = ShowExecutor.execute(stmt, starRocksAssert.getCtx());
        System.out.println(resultSet.getResultRows());
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertTrue(resultSet.getResultRows().get(0).contains("test"));

        ShowProcesslistStmt showProcesslistStmt = new ShowProcesslistStmt(true);
        Assert.assertNull(showProcesslistStmt.getForUser());
    }

    private HttpConnectContext createHttpConnectContextForUser(String qualifiedName) {
        HttpConnectContext context = new HttpConnectContext();
        context.setQualifiedUser(qualifiedName);
        context.setCurrentUserIdentity(new UserIdentity(qualifiedName, "%"));
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setConnectionId(connectionId++);

        return context;
    }

    @Test
    public void testHttpConnectContextReachLimit() {
        ExecuteSqlAction executeSqlAction = new ExecuteSqlAction(null);
        Config.qe_max_connection = 1;
        ExecuteEnv.setup();

        Deencapsulation.invoke(executeSqlAction,
                "registerContext",
                "select 1",
                createHttpConnectContextForUser("test"));
        try {
            Deencapsulation.invoke(executeSqlAction,
                    "registerContext",
                    "select 1",
                    createHttpConnectContextForUser("test"));
        } catch (StarRocksHttpException e) {
            Assert.assertTrue(e.getMessage().contains("Reach cluster-wide connection limit"));
        }

        // reset
        Config.qe_max_connection = 4096;
        ExecuteEnv.setup();
    }
}
