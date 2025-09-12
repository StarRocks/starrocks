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
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.NativeAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.rest.ExecuteSqlAction;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TListConnectionRequest;
import com.starrocks.thrift.TListConnectionResponse;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ConnectionTest {
    private static StarRocksAssert starRocksAssert;
    private static int connectionId = 100;

    @BeforeAll
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
        Assertions.assertTrue(result.first);
        result = ExecuteEnv.getInstance().getScheduler()
                .registerConnection(createConnectContextForUser("test"));
        Assertions.assertFalse(result.first);
        Assertions.assertTrue(result.second.contains("Reach cluster-wide connection limit"));
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
        Assertions.assertTrue(result.first);
        result = ExecuteEnv.getInstance().getScheduler()
                .registerConnection(createConnectContextForUser("test"));
        Assertions.assertFalse(result.first);
        Assertions.assertTrue(result.second.contains("Reach user-level"));
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
            Assertions.assertTrue(e.getMessage().contains("Reach cluster-wide connection limit"));
        }

        // reset
        Config.qe_max_connection = 4096;
        ExecuteEnv.setup();
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
        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity("test", "%"));
        ShowResultSet resultSet = ShowExecutor.execute(stmt, starRocksAssert.getCtx());
        Assertions.assertEquals(1, resultSet.getResultRows().size());
        Assertions.assertTrue(resultSet.getResultRows().get(0).contains("test"));

        ShowProcesslistStmt showProcesslistStmt = new ShowProcesslistStmt(true);
        Assertions.assertNull(showProcesslistStmt.getForUser());
    }

    @Test
    public void testListConnections() throws Exception {
        Config.qe_max_connection = 1000;
        ExecuteEnv.setup();
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();

        connectScheduler.registerConnection(createConnectContextForUser("u1"));
        connectScheduler.registerConnection(createConnectContextForUser("u2"));

        FrontendServiceImpl impl = new FrontendServiceImpl(ExecuteEnv.getInstance());
        TListConnectionRequest request = new TListConnectionRequest();

        TAuthInfo tAuthInfo = new TAuthInfo();
        tAuthInfo.setCurrent_user_ident(UserIdentityUtils.toThrift(UserIdentity.ROOT));
        request.setAuth_info(tAuthInfo);
        request.setFor_user(null);
        request.setShow_full(false);

        TListConnectionResponse response = impl.listConnections(request);
        Assertions.assertEquals(2, response.getConnections().size());

        tAuthInfo = new TAuthInfo();
        tAuthInfo.setCurrent_user_ident(UserIdentityUtils.toThrift(UserIdentity.ROOT));
        request.setAuth_info(tAuthInfo);
        request.setFor_user("u2");
        request.setShow_full(false);
        response = impl.listConnections(request);
        Assertions.assertEquals(1, response.getConnections().size());

        tAuthInfo = new TAuthInfo();
        tAuthInfo.setCurrent_user_ident(UserIdentityUtils.toThrift(UserIdentity.ROOT));
        request.setAuth_info(tAuthInfo);
        request.setFor_user("u3");
        request.setShow_full(false);
        response = impl.listConnections(request);
        Assertions.assertEquals(0, response.getConnectionsSize());

        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithDomain("u2", "test");
        tAuthInfo = new TAuthInfo();
        tAuthInfo.setCurrent_user_ident(UserIdentityUtils.toThrift(userIdentity));
        request.setAuth_info(tAuthInfo);
        request.setFor_user("u2");
        request.setShow_full(false);
        response = impl.listConnections(request);
        Assertions.assertEquals(1, response.getConnections().size());

        new MockUp<NativeAccessController>() {
            @Mock
            public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
                    throws AccessDeniedException {
                throw new AccessDeniedException();
            }
        };

        userIdentity = UserIdentity.createAnalyzedUserIdentWithDomain("u1", "test");
        tAuthInfo = new TAuthInfo();
        tAuthInfo.setCurrent_user_ident(UserIdentityUtils.toThrift(userIdentity));
        request.setAuth_info(tAuthInfo);
        request.setFor_user("u2");
        request.setShow_full(false);
        response = impl.listConnections(request);
        Assertions.assertEquals(0, response.getConnectionsSize());
    }

    @Test
    public void testConnectionSetAuthFromThrift() throws Exception {
        ConnectContext context = new ConnectContext();
        TAuthInfo tAuthInfo = new TAuthInfo();
        tAuthInfo.setUser("user");
        tAuthInfo.setUser_ip("%");
        UserIdentityUtils.setAuthInfoFromThrift(context, tAuthInfo);
        Assertions.assertEquals("user", context.getCurrentUserIdentity().getUser());
    }

    @Test
    public void testGetCurrentConnectionMap() {
        Config.qe_max_connection = 1000;
        ExecuteEnv.setup();
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        ConnectContext conn1 = createConnectContextForUser("u1");
        ConnectContext conn2 = createConnectContextForUser("u2");
        connectScheduler.registerConnection(conn1);
        connectScheduler.registerConnection(conn2);
        Map<Long, ConnectContext> currentConnectionMap = connectScheduler.getCurrentConnectionMap();
        Assertions.assertEquals(2, currentConnectionMap.size());
        connectScheduler.unregisterConnection(conn1);
        Assertions.assertEquals(1, currentConnectionMap.size());
        connectScheduler.unregisterConnection(conn2);
        Assertions.assertEquals(0, currentConnectionMap.size());
    }
}
