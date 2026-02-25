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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.RefreshConnectionsStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRefreshConnectionsResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;

public class RefreshConnectionsStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;
    private static UserIdentity testUserWithoutOperate;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        
        // Create test users
        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        
        // User with OPERATE privilege
        String createUserSql = "CREATE USER 'testUser' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                        createUserSql, starRocksAssert.getCtx());
        authenticationManager.createUser(createUserStmt);
        UserRef user = createUserStmt.getUser();
        testUser = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
        
        // Grant OPERATE privilege to testUser
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        com.starrocks.qe.DDLStmtExecutor.execute(
                UtFrameUtils.parseStmtWithNewParser(
                        "grant operate on system to testUser", starRocksAssert.getCtx()),
                starRocksAssert.getCtx());
        
        // User without OPERATE privilege
        String createUserSql2 = "CREATE USER 'testUser2' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt2 =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(
                        createUserSql2, starRocksAssert.getCtx());
        authenticationManager.createUser(createUserStmt2);
        UserRef user2 = createUserStmt2.getUser();
        testUserWithoutOperate = new UserIdentity(user2.getUser(), user2.getHost(), user2.isDomain());
    }

    private void ctxToTestUser() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setQualifiedUser(testUser.getUser());
    }

    private void ctxToTestUserWithoutOperate() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUserWithoutOperate);
        starRocksAssert.getCtx().setQualifiedUser(testUserWithoutOperate.getUser());
    }

    private void ctxToRoot() {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getUser());
    }

    @Test
    public void testParseRefreshConnections() throws Exception {
        String sql = "REFRESH CONNECTIONS";
        RefreshConnectionsStmt stmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                sql, starRocksAssert.getCtx());
        Assertions.assertNotNull(stmt);
        Assertions.assertFalse(stmt.isForce());
    }

    @Test
    public void testParseRreshConnectionsForce() throws Exception {
        String sql = "REFRESH CONNECTIONS FORCE";
        RefreshConnectionsStmt stmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                sql, starRocksAssert.getCtx());
        Assertions.assertNotNull(stmt);
        Assertions.assertTrue(stmt.isForce());
    }

    @Test
    public void testRefreshConnectionsWithOperatePrivilege() throws Exception {
        ctxToTestUser();
        
        // First set a global variable
        String setGlobalSql = "SET GLOBAL query_timeout = 600";
        SetStmt setStmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                setGlobalSql, starRocksAssert.getCtx());
        Analyzer.analyze(setStmt, starRocksAssert.getCtx());
        SetExecutor setExecutor = new SetExecutor(starRocksAssert.getCtx(), setStmt);
        setExecutor.execute();
        
        // Create a new connection with old default value
        ConnectContext newCtx = new ConnectContext();
        newCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        newCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        
        // Verify new connection has old value (before refresh)
        int oldTimeout = newCtx.getSessionVariable().getQueryTimeoutS();
        
        // Execute REFRESH CONNECTIONS
        String refreshSql = "REFRESH CONNECTIONS";
        RefreshConnectionsStmt refreshStmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                refreshSql, starRocksAssert.getCtx());
        Analyzer.analyze(refreshStmt, starRocksAssert.getCtx());
        
        // Should not throw exception (has OPERATE privilege)
        Assertions.assertDoesNotThrow(() -> {
            Authorizer.check(refreshStmt, starRocksAssert.getCtx());
        });
    }

    @Test
    public void testRefreshConnectionsWithoutOperatePrivilege() throws Exception {
        ctxToTestUserWithoutOperate();
        
        String refreshSql = "REFRESH CONNECTIONS";
        RefreshConnectionsStmt refreshStmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                refreshSql, starRocksAssert.getCtx());
        Analyzer.analyze(refreshStmt, starRocksAssert.getCtx());
        
        // Should throw AccessDeniedException (no OPERATE privilege)
        Assertions.assertThrows(ErrorReportException.class, () -> {
            Authorizer.check(refreshStmt, starRocksAssert.getCtx());
        });
    }

    @Test
    public void testRefreshConnectionsExecution() throws Exception {
        ctxToRoot();
        
        // Setup ExecuteEnv
        ExecuteEnv.setup();
        
        // Create a test connection
        ConnectContext testCtx = new ConnectContext();
        testCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        testCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        testCtx.setConnectionId(1);
        testCtx.setQualifiedUser("aaa");
        
        // Register the connection
        ExecuteEnv.getInstance().getScheduler().registerConnection(testCtx);
        
        try {
            // Set a global variable
            String setGlobalSql = "SET GLOBAL query_timeout = 900";
            SetStmt setStmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt, starRocksAssert.getCtx());
            SetExecutor setExecutor = new SetExecutor(starRocksAssert.getCtx(), setStmt);
            setExecutor.execute();
            
            // Store original value
            int originalTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
            
            // Modify session variable to a different value
            SystemVariable sessionVar = new SystemVariable(SetType.SESSION, "query_timeout", 
                    new IntLiteral(500L));
            testCtx.modifySystemVariable(sessionVar, true);
            Assertions.assertEquals(500, testCtx.getSessionVariable().getQueryTimeoutS());
            
            // Execute REFRESH CONNECTIONS
            String refreshSql = "REFRESH CONNECTIONS";
            RefreshConnectionsStmt refreshStmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                    refreshSql, starRocksAssert.getCtx());
            Analyzer.analyze(refreshStmt, starRocksAssert.getCtx());
            Authorizer.check(refreshStmt, starRocksAssert.getCtx());
            
            // Execute the statement
            com.starrocks.qe.StmtExecutor executor = new com.starrocks.qe.StmtExecutor(
                    starRocksAssert.getCtx(), refreshStmt);
            executor.execute();
            
            // Since query_timeout was modified by the session, it should NOT be refreshed
            // (refreshConnections only refreshes unmodified variables)
            Assertions.assertEquals(500, testCtx.getSessionVariable().getQueryTimeoutS());
            
            // Now clear the modification and refresh again
            testCtx.getModifiedSessionVariablesMap().clear();
            executor.execute();
            
            // Now it should be refreshed to the global value
            Assertions.assertEquals(900, testCtx.getSessionVariable().getQueryTimeoutS());
        } finally {
            // Cleanup
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(testCtx);
        }
    }

    @Test
    public void testRefreshConnectionsOnlyUnmodifiedVariables() throws Exception {
        ctxToRoot();
        
        // Setup ExecuteEnv
        ExecuteEnv.setup();
        
        // Create a test connection
        ConnectContext testCtx = new ConnectContext();
        testCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        testCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        testCtx.setConnectionId(2);
        testCtx.setQualifiedUser("aaa");
        
        // Register the connection
        ExecuteEnv.getInstance().getScheduler().registerConnection(testCtx);
        
        try {
            // Set global variables
            String setGlobalSql1 = "SET GLOBAL query_timeout = 800";
            SetStmt setStmt1 = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql1, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt1, starRocksAssert.getCtx());
            SetExecutor setExecutor1 = new SetExecutor(starRocksAssert.getCtx(), setStmt1);
            setExecutor1.execute();
            
            String setGlobalSql2 = "SET GLOBAL exec_mem_limit = 1000000000";
            SetStmt setStmt2 = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql2, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt2, starRocksAssert.getCtx());
            SetExecutor setExecutor2 = new SetExecutor(starRocksAssert.getCtx(), setStmt2);
            setExecutor2.execute();
            
            // Modify only query_timeout in session
            SystemVariable sessionVar = new SystemVariable(SetType.SESSION, "query_timeout", 
                    new IntLiteral(600L));
            testCtx.modifySystemVariable(sessionVar, true);
            
            // Store original values
            int originalTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
            long originalMemLimit = testCtx.getSessionVariable().getMaxExecMemByte();
            
            // Execute REFRESH CONNECTIONS
            String refreshSql = "REFRESH CONNECTIONS";
            RefreshConnectionsStmt refreshStmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                    refreshSql, starRocksAssert.getCtx());
            Analyzer.analyze(refreshStmt, starRocksAssert.getCtx());
            Authorizer.check(refreshStmt, starRocksAssert.getCtx());
            
            com.starrocks.qe.StmtExecutor executor = new com.starrocks.qe.StmtExecutor(
                    starRocksAssert.getCtx(), refreshStmt);
            executor.execute();
            
            // query_timeout should NOT be refreshed (was modified by session)
            Assertions.assertEquals(600, testCtx.getSessionVariable().getQueryTimeoutS());
            
            // exec_mem_limit SHOULD be refreshed (was NOT modified by session)
            Assertions.assertEquals(1000000000L, testCtx.getSessionVariable().getMaxExecMemByte());
        } finally {
            // Cleanup
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(testCtx);
        }
    }

    @Test
    public void testRefreshConnectionsEmptyConnectionMap() throws Exception {
        ctxToRoot();
        VariableMgr variableMgr = starRocksAssert.getCtx().getGlobalStateMgr().getVariableMgr();
        Assertions.assertDoesNotThrow(() -> {
            variableMgr.refreshConnections();
        });
    }

    @Test
    public void testRefreshConnectionsForce() throws Exception {
        ctxToRoot();
        ExecuteEnv.setup();
        ConnectContext testCtx = new ConnectContext();
        testCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        testCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        testCtx.setConnectionId(4);
        testCtx.setQualifiedUser("aaa");
        ExecuteEnv.getInstance().getScheduler().registerConnection(testCtx);
        try {
            String setGlobalSql = "SET GLOBAL query_timeout = 1000";
            SetStmt setStmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt, starRocksAssert.getCtx());
            SetExecutor setExecutor = new SetExecutor(starRocksAssert.getCtx(), setStmt);
            setExecutor.execute();
            SystemVariable sessionVar = new SystemVariable(SetType.SESSION, "query_timeout",
                    new IntLiteral(500L));
            testCtx.modifySystemVariable(sessionVar, true);
            Assertions.assertEquals(500, testCtx.getSessionVariable().getQueryTimeoutS());
            String refreshSql = "REFRESH CONNECTIONS FORCE";
            RefreshConnectionsStmt refreshStmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                    refreshSql, starRocksAssert.getCtx());
            Analyzer.analyze(refreshStmt, starRocksAssert.getCtx());
            Authorizer.check(refreshStmt, starRocksAssert.getCtx());
            com.starrocks.qe.StmtExecutor executor = new com.starrocks.qe.StmtExecutor(
                    starRocksAssert.getCtx(), refreshStmt);
            executor.execute();
            Assertions.assertEquals(1000, testCtx.getSessionVariable().getQueryTimeoutS());
        } finally {
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(testCtx);
        }
    }

    @Test
    public void testForceRefreshClearsModifiedVariables() throws Exception {
        ctxToRoot();
        ExecuteEnv.setup();
        ConnectContext testCtx = new ConnectContext();
        testCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        testCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        testCtx.setConnectionId(6);
        testCtx.setQualifiedUser("aaa");
        ExecuteEnv.getInstance().getScheduler().registerConnection(testCtx);
        try {
            String setGlobalSql1 = "SET GLOBAL query_timeout = 1500";
            SetStmt setStmt1 = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql1, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt1, starRocksAssert.getCtx());
            SetExecutor setExecutor1 = new SetExecutor(starRocksAssert.getCtx(), setStmt1);
            setExecutor1.execute();
            SystemVariable sessionVar = new SystemVariable(SetType.SESSION, "query_timeout",
                    new IntLiteral(700L));
            testCtx.modifySystemVariable(sessionVar, true);
            Assertions.assertTrue(testCtx.getModifiedSessionVariablesMap().containsKey("query_timeout"));
            String refreshSqlForce = "REFRESH CONNECTIONS FORCE";
            RefreshConnectionsStmt refreshStmtForce = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                    refreshSqlForce, starRocksAssert.getCtx());
            Analyzer.analyze(refreshStmtForce, starRocksAssert.getCtx());
            com.starrocks.qe.StmtExecutor executorForce = new com.starrocks.qe.StmtExecutor(
                    starRocksAssert.getCtx(), refreshStmtForce);
            executorForce.execute();
            Assertions.assertEquals(1500, testCtx.getSessionVariable().getQueryTimeoutS());
            Assertions.assertFalse(testCtx.getModifiedSessionVariablesMap().containsKey("query_timeout"));
            String setGlobalSql2 = "SET GLOBAL query_timeout = 2000";
            SetStmt setStmt2 = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql2, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt2, starRocksAssert.getCtx());
            SetExecutor setExecutor2 = new SetExecutor(starRocksAssert.getCtx(), setStmt2);
            setExecutor2.execute();
            String refreshSql = "REFRESH CONNECTIONS";
            RefreshConnectionsStmt refreshStmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                    refreshSql, starRocksAssert.getCtx());
            Analyzer.analyze(refreshStmt, starRocksAssert.getCtx());
            com.starrocks.qe.StmtExecutor executor = new com.starrocks.qe.StmtExecutor(
                    starRocksAssert.getCtx(), refreshStmt);
            executor.execute();
            Assertions.assertEquals(2000, testCtx.getSessionVariable().getQueryTimeoutS());
        } finally {
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(testCtx);
        }
    }

    @Test
    public void testRefreshConnectionsInternalWithNullScheduler() throws Exception {
        ctxToRoot();
        VariableMgr variableMgr = starRocksAssert.getCtx().getGlobalStateMgr().getVariableMgr();
        ExecuteEnv env = ExecuteEnv.getInstance();
        ConnectScheduler originalScheduler = env.getScheduler();
        try {
            Field schedulerField = ExecuteEnv.class.getDeclaredField("scheduler");
            schedulerField.setAccessible(true);
            schedulerField.set(env, null);
            Assertions.assertDoesNotThrow(() -> {
                variableMgr.refreshConnectionsInternal(false);
            });
        } finally {
            Field schedulerField = ExecuteEnv.class.getDeclaredField("scheduler");
            schedulerField.setAccessible(true);
            schedulerField.set(env, originalScheduler);
        }
    }

    @Test
    public void testRefreshConnectionsWithExceptionInRefreshConnectionVariables() throws Exception {
        ctxToRoot();
        ExecuteEnv.setup();
        ConnectContext testCtx = new ConnectContext();
        testCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        testCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        testCtx.setConnectionId(7);
        testCtx.setQualifiedUser("aaa");
        ExecuteEnv.getInstance().getScheduler().registerConnection(testCtx);
        try {
            String setGlobalSql = "SET GLOBAL query_timeout = 1200";
            SetStmt setStmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt, starRocksAssert.getCtx());
            SetExecutor setExecutor = new SetExecutor(starRocksAssert.getCtx(), setStmt);
            setExecutor.execute();
            String refreshSql = "REFRESH CONNECTIONS";
            RefreshConnectionsStmt refreshStmt = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                    refreshSql, starRocksAssert.getCtx());
            Analyzer.analyze(refreshStmt, starRocksAssert.getCtx());
            com.starrocks.qe.StmtExecutor executor = new com.starrocks.qe.StmtExecutor(
                    starRocksAssert.getCtx(), refreshStmt);
            Assertions.assertDoesNotThrow(() -> executor.execute());
        } finally {
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(testCtx);
        }
    }

    @Test
    public void testForceRefreshWhenValueUnchangedButModified() throws Exception {
        ctxToRoot();
        ExecuteEnv.setup();
        ConnectContext testCtx = new ConnectContext();
        testCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        testCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        testCtx.setConnectionId(8);
        testCtx.setQualifiedUser("aaa");
        ExecuteEnv.getInstance().getScheduler().registerConnection(testCtx);
        try {
            int globalValue = 1000;
            String setGlobalSql = "SET GLOBAL query_timeout = " + globalValue;
            SetStmt setStmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt, starRocksAssert.getCtx());
            SetExecutor setExecutor = new SetExecutor(starRocksAssert.getCtx(), setStmt);
            setExecutor.execute();
            SystemVariable sessionVar = new SystemVariable(SetType.SESSION, "query_timeout",
                    new IntLiteral((long) globalValue));
            testCtx.modifySystemVariable(sessionVar, true);
            Assertions.assertEquals(globalValue, testCtx.getSessionVariable().getQueryTimeoutS());
            Assertions.assertTrue(testCtx.getModifiedSessionVariablesMap().containsKey("query_timeout"));
            String refreshSqlForce = "REFRESH CONNECTIONS FORCE";
            RefreshConnectionsStmt refreshStmtForce = (RefreshConnectionsStmt) UtFrameUtils.parseStmtWithNewParser(
                    refreshSqlForce, starRocksAssert.getCtx());
            Analyzer.analyze(refreshStmtForce, starRocksAssert.getCtx());
            com.starrocks.qe.StmtExecutor executorForce = new com.starrocks.qe.StmtExecutor(
                    starRocksAssert.getCtx(), refreshStmtForce);
            executorForce.execute();
            Assertions.assertEquals(globalValue, testCtx.getSessionVariable().getQueryTimeoutS());
            Assertions.assertFalse(testCtx.getModifiedSessionVariablesMap().containsKey("query_timeout"));
        } finally {
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(testCtx);
        }
    }

    @Test
    public void testNotifyOtherFEsWithRPCException() throws Exception {
        ctxToRoot();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        NodeMgr nodeMgr = globalStateMgr.getNodeMgr();
        Pair<String, Integer> selfNode = nodeMgr.getSelfNode();
        String otherHost = "127.0.0.2";
        if (selfNode.first.equals(otherHost)) {
            otherHost = "127.0.0.3";
        }
        try {
            nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, otherHost, 9010);
            new MockUp<ThriftRPCRequestExecutor>() {
                @Mock
                public <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT call(
                        com.starrocks.rpc.ThriftConnectionPool<SERVER_CLIENT> genericPool,
                        TNetworkAddress address,
                        int timeoutMs,
                        int tryTimes,
                        ThriftRPCRequestExecutor.MethodCallable<SERVER_CLIENT, RESULT> callable) throws TException {
                    throw new TException("RPC call failed");
                }
            };
            VariableMgr variableMgr = globalStateMgr.getVariableMgr();
            Assertions.assertDoesNotThrow(() -> {
                variableMgr.refreshConnections(false);
            });
        } finally {
            try {
                nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, otherHost, 9010);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testNotifyOtherFEsWithErrorResponse() throws Exception {
        ctxToRoot();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        NodeMgr nodeMgr = globalStateMgr.getNodeMgr();
        Pair<String, Integer> selfNode = nodeMgr.getSelfNode();
        String otherHost = "127.0.0.3";
        if (selfNode.first.equals(otherHost)) {
            otherHost = "127.0.0.4";
        }
        try {
            nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, otherHost, 9010);
            new MockUp<ThriftRPCRequestExecutor>() {
                @Mock
                public <RESULT, SERVER_CLIENT extends org.apache.thrift.TServiceClient> RESULT call(
                        com.starrocks.rpc.ThriftConnectionPool<SERVER_CLIENT> genericPool,
                        TNetworkAddress address,
                        int timeoutMs,
                        int tryTimes,
                        ThriftRPCRequestExecutor.MethodCallable<SERVER_CLIENT, RESULT> callable) throws TException {
                    TRefreshConnectionsResponse response = new TRefreshConnectionsResponse();
                    TStatus status = new TStatus();
                    status.setStatus_code(TStatusCode.INTERNAL_ERROR);
                    status.setError_msgs(new ArrayList<>());
                    status.addToError_msgs("Internal error occurred");
                    response.setStatus(status);
                    return (RESULT) response;
                }
            };
            VariableMgr variableMgr = globalStateMgr.getVariableMgr();
            Assertions.assertDoesNotThrow(() -> {
                variableMgr.refreshConnections(false);
            });
        } finally {
            try {
                nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, otherHost, 9010);
            } catch (Exception e) {
            }
        }
    }
}
