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
import com.starrocks.authentication.UserProperty;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TUserIdentity;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xnio.StreamConnection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class VariablePriorityTest {

    @Mocked
    private StreamConnection connection;

    private VariableMgr variableMgr = new VariableMgr();
    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() throws Exception {
        globalStateMgr = GlobalStateMgr.getCurrentState();
    }

    @Test
    public void testVariablePriorityWithExplicitSet() throws Exception {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        // First set user property
        UserProperty userProperty = new UserProperty();
        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("query_timeout", "300");
        userProperty.setSessionVariables(sessionVars);
        ctx.updateByUserProperty(userProperty);

        // Verify that user property variables are applied
        Assertions.assertEquals(300, ctx.getSessionVariable().getQueryTimeoutS());

        // Then explicitly set a variable (should have higher priority)
        SystemVariable explicitVar = new SystemVariable("query_timeout", new StringLiteral("600"));
        ctx.modifySystemVariable(explicitVar, true);

        // Verify that explicit set overrides user property
        Assertions.assertEquals(600, ctx.getSessionVariable().getQueryTimeoutS());
    }

    @Test
    public void testVariablePriorityWithGlobalSet() throws Exception {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        // Set global variable (lowest priority)
        SystemVariable globalVar = new SystemVariable("query_timeout", new StringLiteral("120"));
        ctx.modifySystemVariable(globalVar, false);

        // Set user property (medium priority)
        UserProperty userProperty = new UserProperty();
        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("query_timeout", "300");
        userProperty.setSessionVariables(sessionVars);
        ctx.updateByUserProperty(userProperty);

        // Verify that user property overrides global
        Assertions.assertEquals(300, ctx.getSessionVariable().getQueryTimeoutS());

        // Set explicit variable (highest priority)
        SystemVariable explicitVar = new SystemVariable("query_timeout", new StringLiteral("600"));
        ctx.modifySystemVariable(explicitVar, true);

        // Verify that explicit set overrides both global and user property
        Assertions.assertEquals(600, ctx.getSessionVariable().getQueryTimeoutS());
    }

    @Test
    public void testModifiedSessionVariablesTracking() throws Exception {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        UserProperty userProperty = new UserProperty();
        Map<String, String> sessionVars = new HashMap<>();
        sessionVars.put("query_timeout", "300");
        userProperty.setSessionVariables(sessionVars);
        ctx.updateByUserProperty(userProperty);

        // Set some session variables
        SystemVariable var = new SystemVariable("sql_mode", new StringLiteral("2097152")); // STRICT_TRANS_TABLES = 1L << 21
        ctx.modifySystemVariable(var, true);

        // Get modified session variables
        SetStmt modifiedVars = ctx.getModifiedSessionVariables();
        Assertions.assertNotNull(modifiedVars);
        Assertions.assertEquals(1, modifiedVars.getSetListItems().size());

        // Verify the variables are tracked
        List<SetListItem> items = modifiedVars.getSetListItems();
        Set<String> varNames = items.stream()
                .map(item -> ((SystemVariable) item).getVariable())
                .collect(Collectors.toSet());
        Assertions.assertTrue(varNames.contains("sql_mode"));
    }

    // ==================== ExecuteAsExecutor 相关测试 ====================

    @Test
    public void testExecuteAsVariablePriority() throws Exception {
        // Create users with different user properties
        UserProperty user1Property = new UserProperty();
        Map<String, String> user1SessionVars = new HashMap<>();
        user1SessionVars.put("query_timeout", "300");
        user1Property.setSessionVariables(user1SessionVars);

        UserProperty user2Property = new UserProperty();
        Map<String, String> user2SessionVars = new HashMap<>();
        user2SessionVars.put("query_timeout", "600");
        user2Property.setSessionVariables(user2SessionVars);

        UserProperty user3Property = new UserProperty();
        Map<String, String> user3SessionVars = new HashMap<>();
        user3SessionVars.put("query_timeout", "900");
        user3Property.setSessionVariables(user3SessionVars);

        // Mock authentication manager to return user properties
        new MockUp<AuthenticationMgr>() {
            @Mock
            public UserProperty getUserProperty(String userName) {
                if ("u1".equals(userName)) {
                    return user1Property;
                } else if ("u2".equals(userName)) {
                    return user2Property;
                } else if ("impersonate_user".equals(userName)) {
                    return user3Property;
                }
                return new UserProperty();
            }
        };

        // Create context and set initial user
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(globalStateMgr);
        context.setCurrentUserIdentity(new UserIdentity("impersonate_user", "%", false));
        context.updateByUserProperty(user3Property);

        // Verify initial state
        Assertions.assertEquals(900, context.getSessionVariable().getQueryTimeoutS());

        // Execute AS u1
        ExecuteAsStmt executeAsStmt1 = new ExecuteAsStmt(new UserRef("u1", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt1, context);
        // Verify that user1's properties are applied
        Assertions.assertEquals(300, context.getSessionVariable().getQueryTimeoutS());

        // Execute AS u2
        ExecuteAsStmt executeAsStmt2 = new ExecuteAsStmt(new UserRef("u2", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt2, context);
        // Verify that user2's properties are applied (overriding user1's)
        Assertions.assertEquals(600, context.getSessionVariable().getQueryTimeoutS());
    }

    @Test
    public void testExecuteAsVariablePrioritySessionFirst() throws Exception {
        // Create users with different user properties
        UserProperty user1Property = new UserProperty();
        Map<String, String> user1SessionVars = new HashMap<>();
        user1SessionVars.put("query_timeout", "300");
        user1Property.setSessionVariables(user1SessionVars);

        UserProperty user2Property = new UserProperty();
        Map<String, String> user2SessionVars = new HashMap<>();
        user2SessionVars.put("query_timeout", "600");
        user2Property.setSessionVariables(user2SessionVars);

        UserProperty user3Property = new UserProperty();
        Map<String, String> user3SessionVars = new HashMap<>();
        user3SessionVars.put("query_timeout", "900");
        user3Property.setSessionVariables(user3SessionVars);

        // Mock authentication manager to return user properties
        new MockUp<AuthenticationMgr>() {
            @Mock
            public UserProperty getUserProperty(String userName) {
                if ("u1".equals(userName)) {
                    return user1Property;
                } else if ("u2".equals(userName)) {
                    return user2Property;
                } else if ("impersonate_user".equals(userName)) {
                    return user3Property;
                }
                return new UserProperty();
            }
        };

        // Create context and set initial user
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(globalStateMgr);
        context.setCurrentUserIdentity(new UserIdentity("impersonate_user", "%", false));
        context.updateByUserProperty(user3Property);

        // Verify initial state
        Assertions.assertEquals(900, context.getSessionVariable().getQueryTimeoutS());

        // Set some initial session variables (simulating explicit SET)
        SystemVariable initialVar = new SystemVariable("query_timeout", new StringLiteral("120"));
        context.modifySystemVariable(initialVar, true);

        // Verify initial state
        Assertions.assertEquals(120, context.getSessionVariable().getQueryTimeoutS());

        // Execute AS u1
        ExecuteAsStmt executeAsStmt1 = new ExecuteAsStmt(new UserRef("u1", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt1, context);
        // Verify that user1's properties are applied
        Assertions.assertEquals(120, context.getSessionVariable().getQueryTimeoutS());

        // Execute AS u2
        ExecuteAsStmt executeAsStmt2 = new ExecuteAsStmt(new UserRef("u2", "%"), false);
        ExecuteAsExecutor.execute(executeAsStmt2, context);
        // Verify that user2's properties are applied (overriding user1's)
        Assertions.assertEquals(120, context.getSessionVariable().getQueryTimeoutS());
    }

    @Test
    public void testExecuteAsWithEphemeralUser() throws Exception {
        // Create context with ephemeral user
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(globalStateMgr);
        UserIdentity ephemeralUser = UserIdentity.createEphemeralUserIdent("external_user", "%");
        context.setCurrentUserIdentity(ephemeralUser);

        // Set some session variables
        SystemVariable var = new SystemVariable("query_timeout", new StringLiteral("120"));
        context.modifySystemVariable(var, true);

        // Execute AS with ephemeral user (should not apply user properties)
        ExecuteAsStmt executeAsStmt = new ExecuteAsStmt(new UserRef("external_user", "%", false, true, NodePosition.ZERO), false);
        ExecuteAsExecutor.execute(executeAsStmt, context);

        // Verify that session variables are not modified by user properties
        // (ephemeral users don't have user properties)
        Assertions.assertEquals(120, context.getSessionVariable().getQueryTimeoutS());
    }

    @Test
    public void testForward(@Mocked LeaderOpExecutor leaderOpExecutor) throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return null;
            }

            @Mock
            public LeaderOpExecutor getLeaderOpExecutor() {
                return leaderOpExecutor;
            }
        };

        new MockUp<LeaderOpExecutor>() {
            @Mock
            public TMasterOpResult getResult() {
                return new TMasterOpResult();
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 1L;
            }
        };

        // Create users with different user properties
        UserProperty user1Property = new UserProperty();
        Map<String, String> user1SessionVars = new HashMap<>();
        user1SessionVars.put("query_timeout", "300");
        user1Property.setSessionVariables(user1SessionVars);

        // Mock authentication manager to return user properties
        new MockUp<AuthenticationMgr>() {
            @Mock
            public UserProperty getUserProperty(String userName) {
                return user1Property;
            }
        };

        TMasterOpRequest request = new TMasterOpRequest();
        request.setCatalog("default");
        request.setDb("testDb1");
        request.setUser("u1");
        request.setSql("select 1");
        request.setIsInternalStmt(true);
        //request.setModified_variables_sql("set query_timeout = 10");
        request.setCurrent_user_ident(new TUserIdentity().setUsername("root").setHost("127.0.0.1"));
        request.setQueryId(UUIDUtil.genTUniqueId());
        request.setSession_id(UUID.randomUUID().toString());
        request.setIsLastStmt(true);

        // mock context
        ConnectContext ctx = new ConnectContext();
        ConnectProcessor processor = new ConnectProcessor(ctx);
        TMasterOpResult result = processor.proxyExecute(request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(300, ctx.getSessionVariable().getQueryTimeoutS());

        request = new TMasterOpRequest();
        request.setCatalog("default");
        request.setDb("testDb1");
        request.setUser("u1");
        request.setSql("select 1");
        request.setIsInternalStmt(true);
        request.setModified_variables_sql("set query_timeout = 10");
        request.setCurrent_user_ident(new TUserIdentity().setUsername("root").setHost("127.0.0.1"));
        request.setQueryId(UUIDUtil.genTUniqueId());
        request.setSession_id(UUID.randomUUID().toString());
        request.setIsLastStmt(true);

        // mock context
        ctx = new ConnectContext();
        processor = new ConnectProcessor(ctx);
        result = processor.proxyExecute(request);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(10, ctx.getSessionVariable().getQueryTimeoutS());
    }
}
