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

import com.google.common.collect.Lists;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.ErrorReportException;
import com.starrocks.persist.GlobalVarPersistInfo;
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
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
    public void testReplayRefreshConnections() throws Exception {
        ctxToRoot();
        
        // Setup ExecuteEnv
        ExecuteEnv.setup();
        
        // Create a test connection
        ConnectContext testCtx = new ConnectContext();
        testCtx.setGlobalStateMgr(starRocksAssert.getCtx().getGlobalStateMgr());
        testCtx.setSessionVariable(starRocksAssert.getCtx().getGlobalStateMgr()
                .getVariableMgr().newSessionVariable());
        testCtx.setConnectionId(3);
        testCtx.setQualifiedUser("aaa");
        
        // Register the connection
        ExecuteEnv.getInstance().getScheduler().registerConnection(testCtx);
        
        try {
            // Set a global variable
            String setGlobalSql = "SET GLOBAL query_timeout = 700";
            SetStmt setStmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    setGlobalSql, starRocksAssert.getCtx());
            Analyzer.analyze(setStmt, starRocksAssert.getCtx());
            SetExecutor setExecutor = new SetExecutor(starRocksAssert.getCtx(), setStmt);
            setExecutor.execute();
            
            // Create a GlobalVarPersistInfo for refresh connections (empty varNames)
            VariableMgr variableMgr = starRocksAssert.getCtx().getGlobalStateMgr().getVariableMgr();
            GlobalVarPersistInfo info = new GlobalVarPersistInfo(
                    variableMgr.getDefaultSessionVariable(), Lists.newArrayList());
            info.setPersistJsonString("{}");
            
            // Store original value
            int originalTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
            
            // Replay the refresh connections command
            variableMgr.replayGlobalVariableV2(info);
            
            // Connection should be refreshed
            Assertions.assertEquals(700, testCtx.getSessionVariable().getQueryTimeoutS());
        } finally {
            // Cleanup
            ExecuteEnv.getInstance().getScheduler().unregisterConnection(testCtx);
        }
    }

    @Test
    public void testRefreshConnectionsEmptyConnectionMap() throws Exception {
        ctxToRoot();
        
        VariableMgr variableMgr = starRocksAssert.getCtx().getGlobalStateMgr().getVariableMgr();
        
        // Should not throw exception even with no connections
        Assertions.assertDoesNotThrow(() -> {
            variableMgr.refreshConnections();
        });
    }
}
