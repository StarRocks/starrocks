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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionTest.class);
    private static StarRocksAssert starRocksAssert;
    private static int connectionId = 100;
    private static int uniqueUserId = 0;

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

    private static String nextUserName(String prefix) {
        return prefix + "_" + (++uniqueUserId);
    }

    private void ensureUserExists(String userName) throws Exception {
        String createUserSql = String.format("CREATE USER '%s' IDENTIFIED BY ''", userName);
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().createUser(createUserStmt);
    }

    private void setUserMaxConnections(String userName, long maxConn) throws Exception {
        String sql = String.format("set property for '%s' 'max_user_connections' = '%d'", userName, maxConn);
        SetUserPropertyStmt setUserPropertyStmt =
                (SetUserPropertyStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                .updateUserProperty(userName, setUserPropertyStmt.getPropertyPairList());
    }

    private int getUserConnCount(ConnectScheduler scheduler, String userName) {
        AtomicInteger counter = scheduler.getUserConnectionMap().get(userName);
        return counter == null ? 0 : counter.get();
    }

    private int totalTrackedConnections(ConnectScheduler scheduler) {
        return scheduler.getUserConnectionMap().values().stream().mapToInt(AtomicInteger::get).sum();
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

    @Test
    public void testOnUserChangedUpdatesCounters() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("change_user_a");
        String userB = nextUserName("change_user_b");
        ensureUserExists(userA);
        ensureUserExists(userB);

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx = createConnectContextForUser(userA);

        Pair<Boolean, String> registerResult = scheduler.registerConnection(ctx);
        Assertions.assertTrue(registerResult.first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(0, getUserConnCount(scheduler, userB));
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());
        Assertions.assertEquals(1, totalTrackedConnections(scheduler));

        ctx.setQualifiedUser(userB);
        ctx.setCurrentUserIdentity(new UserIdentity(userB, "%"));
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(ctx, userA, userB);
        Assertions.assertTrue(changeResult.first);
        Assertions.assertEquals(0, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB));
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());
        Assertions.assertEquals(1, totalTrackedConnections(scheduler));

        ctx.setQualifiedUser(userA);
        ctx.setCurrentUserIdentity(new UserIdentity(userA, "%"));
        changeResult = scheduler.onUserChanged(ctx, userB, userA);
        Assertions.assertTrue(changeResult.first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(0, getUserConnCount(scheduler, userB));
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());
        Assertions.assertEquals(1, totalTrackedConnections(scheduler));

        scheduler.unregisterConnection(ctx);
        Assertions.assertEquals(0, scheduler.getCurrentConnectionMap().size());
        Assertions.assertEquals(0, totalTrackedConnections(scheduler));
        Assertions.assertTrue(scheduler.getUserConnectionMap().values().stream().allMatch(counter -> counter.get() >= 0));
    }

    @Test
    public void testOnUserChangedRespectsUserLimit() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("limit_user_a");
        String userB = nextUserName("limit_user_b");
        ensureUserExists(userA);
        ensureUserExists(userB);
        setUserMaxConnections(userB, 1);

        ConnectScheduler scheduler = new ConnectScheduler(100);

        ConnectContext primaryCtx = createConnectContextForUser(userA);
        Assertions.assertTrue(scheduler.registerConnection(primaryCtx).first);

        ConnectContext occupiedCtx = createConnectContextForUser(userB);
        Assertions.assertTrue(scheduler.registerConnection(occupiedCtx).first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB));

        primaryCtx.setQualifiedUser(userB);
        primaryCtx.setCurrentUserIdentity(new UserIdentity(userB, "%"));
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(primaryCtx, userA, userB);
        Assertions.assertFalse(changeResult.first);
        Assertions.assertNotNull(changeResult.second);
        Assertions.assertTrue(changeResult.second.contains("Reach user-level"));
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB));
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(2, totalTrackedConnections(scheduler));
        Assertions.assertEquals(2, scheduler.getCurrentConnectionMap().size());

        // revert context state to original user, as MysqlProto.changeUser would do on failure
        primaryCtx.setQualifiedUser(userA);
        primaryCtx.setCurrentUserIdentity(new UserIdentity(userA, "%"));

        scheduler.unregisterConnection(primaryCtx);
        scheduler.unregisterConnection(occupiedCtx);
        Assertions.assertEquals(0, scheduler.getCurrentConnectionMap().size());
        Assertions.assertEquals(0, totalTrackedConnections(scheduler));
    }

    @Test
    public void testOnUserChangedWithNullNewUser() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("null_user_a");
        ensureUserExists(userA);

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx = createConnectContextForUser(userA);

        Pair<Boolean, String> registerResult = scheduler.registerConnection(ctx);
        Assertions.assertTrue(registerResult.first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));

        // Test with null new user
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(ctx, userA, null);
        Assertions.assertFalse(changeResult.first);
        Assertions.assertNotNull(changeResult.second);
        Assertions.assertTrue(changeResult.second.contains("new qualifiedUser is null"));
        
        // Verify original state is preserved
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(1, totalTrackedConnections(scheduler));
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());

        scheduler.unregisterConnection(ctx);
    }

    @Test
    public void testOnUserChangedWithSameUser() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("same_user_a");
        ensureUserExists(userA);

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx = createConnectContextForUser(userA);

        Pair<Boolean, String> registerResult = scheduler.registerConnection(ctx);
        Assertions.assertTrue(registerResult.first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));

        // Test with same user (should succeed but no state change)
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(ctx, userA, userA);
        Assertions.assertTrue(changeResult.first);
        Assertions.assertNull(changeResult.second);
        
        // Verify state remains unchanged
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(1, totalTrackedConnections(scheduler));
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());

        scheduler.unregisterConnection(ctx);
    }

    @Test
    public void testOnUserChangedWithNonExistentOldUser() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("non_existent_old_a");
        String userB = nextUserName("non_existent_old_b");
        ensureUserExists(userA);
        ensureUserExists(userB);

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx = createConnectContextForUser(userA);

        Pair<Boolean, String> registerResult = scheduler.registerConnection(ctx);
        Assertions.assertTrue(registerResult.first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));

        // Test with non-existent old user (should still succeed)
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(ctx, "non_existent_user", userB);
        Assertions.assertTrue(changeResult.first);
        
        // Verify new user counter is incremented, old user counter remains unchanged
        // because the old user didn't exist in the counter map
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA)); // Still 1 because old user didn't exist
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB)); // New user counter incremented
        Assertions.assertEquals(2, totalTrackedConnections(scheduler)); // Both users have connections
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());

        scheduler.unregisterConnection(ctx);
    }

    @Test
    public void testOnUserChangedWithNegativeCounterRecovery() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("negative_counter_a");
        String userB = nextUserName("negative_counter_b");
        ensureUserExists(userA);
        ensureUserExists(userB);

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx = createConnectContextForUser(userA);

        Pair<Boolean, String> registerResult = scheduler.registerConnection(ctx);
        Assertions.assertTrue(registerResult.first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));

        // Manually create a negative counter scenario by manipulating the internal state
        // This simulates a race condition or bug where counter goes negative
        AtomicInteger userACounter = scheduler.getUserConnectionMap().get(userA);
        userACounter.set(-1); // Force negative counter

        // Now try to change user - should handle negative counter gracefully
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(ctx, userA, userB);
        Assertions.assertTrue(changeResult.first);
        
        // Verify counters are correct after recovery
        Assertions.assertEquals(0, getUserConnCount(scheduler, userA)); // Should be reset to 0
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB));
        Assertions.assertEquals(1, totalTrackedConnections(scheduler));
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());

        scheduler.unregisterConnection(ctx);
    }

    @Test
    public void testOnUserChangedWithMissingOldUserCounter() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("missing_counter_a");
        String userB = nextUserName("missing_counter_b");
        ensureUserExists(userA);
        ensureUserExists(userB);

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx = createConnectContextForUser(userA);

        Pair<Boolean, String> registerResult = scheduler.registerConnection(ctx);
        Assertions.assertTrue(registerResult.first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));

        // Manually remove the counter to simulate missing counter scenario
        scheduler.getUserConnectionMap().remove(userA);

        // Now try to change user - should handle missing counter gracefully
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(ctx, userA, userB);
        Assertions.assertTrue(changeResult.first);
        
        // Verify new user counter is still incremented correctly
        Assertions.assertEquals(0, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB));
        Assertions.assertEquals(1, totalTrackedConnections(scheduler));
        Assertions.assertEquals(1, scheduler.getCurrentConnectionMap().size());

        scheduler.unregisterConnection(ctx);
    }

    @Test
    public void testOnUserChangedConcurrentModification() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("concurrent_a");
        String userB = nextUserName("concurrent_b");
        String userC = nextUserName("concurrent_c");
        ensureUserExists(userA);
        ensureUserExists(userB);
        ensureUserExists(userC);
        setUserMaxConnections(userB, 1); // Set userB limit to 1

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx1 = createConnectContextForUser(userA);
        ConnectContext ctx2 = createConnectContextForUser(userB);

        // Register both connections
        Assertions.assertTrue(scheduler.registerConnection(ctx1).first);
        Assertions.assertTrue(scheduler.registerConnection(ctx2).first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB));

        // Simulate concurrent user changes
        // ctx1 changes from userA to userB (should fail due to userB limit)
        ctx1.setQualifiedUser(userB);
        ctx1.setCurrentUserIdentity(new UserIdentity(userB, "%"));
        Pair<Boolean, String> changeResult1 = scheduler.onUserChanged(ctx1, userA, userB);
        
        // ctx2 changes from userB to userC (should succeed)
        ctx2.setQualifiedUser(userC);
        ctx2.setCurrentUserIdentity(new UserIdentity(userC, "%"));
        Pair<Boolean, String> changeResult2 = scheduler.onUserChanged(ctx2, userB, userC);

        // Verify results
        Assertions.assertFalse(changeResult1.first); // Should fail due to userB limit
        Assertions.assertTrue(changeResult2.first);  // Should succeed

        // Verify final state
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA)); // ctx1 reverted
        Assertions.assertEquals(0, getUserConnCount(scheduler, userB)); // ctx2 moved to userC
        Assertions.assertEquals(1, getUserConnCount(scheduler, userC)); // ctx2 succeeded
        Assertions.assertEquals(2, totalTrackedConnections(scheduler)); // Both connections tracked
        Assertions.assertEquals(2, scheduler.getCurrentConnectionMap().size());

        scheduler.unregisterConnection(ctx1);
        scheduler.unregisterConnection(ctx2);
    }

    @Test
    public void testOnUserChangedStateConsistencyAfterFailure() throws Exception {
        ExecuteEnv.setup();
        String userA = nextUserName("consistency_a");
        String userB = nextUserName("consistency_b");
        ensureUserExists(userA);
        ensureUserExists(userB);
        setUserMaxConnections(userB, 1);

        ConnectScheduler scheduler = new ConnectScheduler(100);
        ConnectContext ctx1 = createConnectContextForUser(userA);
        ConnectContext ctx2 = createConnectContextForUser(userB);

        // Register both connections
        Assertions.assertTrue(scheduler.registerConnection(ctx1).first);
        Assertions.assertTrue(scheduler.registerConnection(ctx2).first);
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA));
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB));

        // Try to change ctx1 from userA to userB (should fail due to userB limit)
        ctx1.setQualifiedUser(userB);
        ctx1.setCurrentUserIdentity(new UserIdentity(userB, "%"));
        Pair<Boolean, String> changeResult = scheduler.onUserChanged(ctx1, userA, userB);
        
        Assertions.assertFalse(changeResult.first);
        Assertions.assertNotNull(changeResult.second);
        Assertions.assertTrue(changeResult.second.contains("Reach user-level"));

        // Verify state consistency after failure
        // The onUserChanged method checks the limit before incrementing the counter
        // So userB counter should remain 1, and the change should be considered failed
        Assertions.assertEquals(1, getUserConnCount(scheduler, userA)); // Should remain unchanged
        Assertions.assertEquals(1, getUserConnCount(scheduler, userB)); // Should remain unchanged (limit check before increment)
        Assertions.assertEquals(2, totalTrackedConnections(scheduler)); // Both connections still tracked
        Assertions.assertEquals(2, scheduler.getCurrentConnectionMap().size());

        // Verify that the context state needs to be manually reverted by caller
        // (This is the responsibility of MysqlProto.changeUser)
        Assertions.assertEquals(userB, ctx1.getQualifiedUser()); // Context still has new user
        Assertions.assertEquals(userB, ctx1.getCurrentUserIdentity().getUser());

        scheduler.unregisterConnection(ctx1);
        scheduler.unregisterConnection(ctx2);
    }


}
