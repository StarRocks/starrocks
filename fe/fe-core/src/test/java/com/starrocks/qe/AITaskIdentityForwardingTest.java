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

import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.TaskExecutionIdentity;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AITaskIdentityForwardingTest {
    private final org.apache.logging.log4j.core.Logger identityLogger =
            (org.apache.logging.log4j.core.Logger) LogManager.getLogger(TaskExecutionIdentity.class);
    private Level previousLogLevel;
    private IdentityLogAppender appender;
    private int executedStatements;

    @BeforeEach
    public void setUp() {
        executedStatements = 0;
        UtFrameUtils.mockInitWarehouseEnv();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public Long getMaxJournalId() {
                return 0L;
            }
        };
        // Only the forwarded execution is stubbed; request creation and proxy identity installation are real.
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
                executedStatements++;
            }
        };
        previousLogLevel = identityLogger.getLevel();
        appender = new IdentityLogAppender();
        appender.start();
        identityLogger.addAppender(appender);
        Configurator.setLevel(TaskExecutionIdentity.class.getName(), Level.WARN);
    }

    @AfterEach
    public void tearDown() {
        Configurator.setLevel(TaskExecutionIdentity.class.getName(), previousLogLevel);
        identityLogger.removeAppender(appender);
        appender.stop();
        ConnectContext.remove();
    }

    @Test
    public void testAuthenticatedIdentityIsForwardedAndCanOwnAITask() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot();
        assertNotNull(TMasterOpRequest._Fields.findByName("task_execution_identity"));
        ConnectContext receiver = receive(request);
        assertEquals("root", receiver.getDistinguishedName());
        assertEquals("native", receiver.getSecurityIntegration());
        String taskJson = GsonUtils.GSON.toJson(TaskBuilder.buildTask(resolvedAISubmit(receiver), receiver));
        assertTrue(taskJson.contains("\"executionIdentity\""));
        assertEquals(1, executedStatements);
        assertTrue(appender.messages.isEmpty());
    }

    @Test
    public void testMissingForwardedMetadataCannotBecomeNativeCreator() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot();
        request.unsetQuery_source();
        TMasterOpRequest._Fields field = TMasterOpRequest._Fields.findByName("task_execution_identity");
        if (field != null) {
            request.setFieldValue(field, null);
        }
        ConnectContext receiver = receive(request);
        assertFalse(receiver.getState().isError());
        assertEquals(1, executedStatements);
        assertNull(receiver.getAuthenticatedTaskIdentity());
        SemanticException exception = assertThrows(SemanticException.class,
                () -> TaskBuilder.buildTask(resolvedAISubmit(receiver), receiver));
        assertTrue(exception.getMessage().contains("reconnect"));
        assertTrue(exception.getMessage().contains("EXECUTE AS is not supported"));
        assertTrue(exception.getMessage().contains("for forwarded requests, verify that all Frontends are upgraded"));
        assertTrue(appender.messages.isEmpty());
    }

    @Test
    public void testEphemeralMismatchCannotReuseNativeForwardedMetadata() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot();
        request.getCurrent_user_ident().setIs_ephemeral(true);
        ConnectContext receiver = receive(request);
        assertNull(receiver.getAuthenticatedTaskIdentity());
        assertThrows(SemanticException.class, () -> TaskBuilder.buildTask(resolvedAISubmit(receiver), receiver));
        assertEquals(List.of("WARN: Ignoring forwarded AI task identity: principal mismatch"), appender.messages);
    }

    @Test
    public void testNewIdentityEnvelopeRequiresItsQuerySource() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot(QueryDetail.QuerySource.TASK);
        request.unsetQuery_source();
        ConnectContext receiver = receive(request);
        assertTrue(receiver.getState().isError());
        assertTrue(receiver.getState().getErrorMessage().contains("forwarded execution identity"));
        assertNull(receiver.getCurrentUserIdentity());
        assertNull(receiver.getAuthenticatedTaskIdentity());
        assertEquals(0, executedStatements);
    }

    @Test
    public void testUnknownQuerySourceCannotDefaultToExternal() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot(QueryDetail.QuerySource.TASK);
        request.setQuery_source("UNKNOWN_FUTURE_SOURCE");
        ConnectContext receiver = receive(request);
        assertTrue(receiver.getState().isError());
        assertTrue(receiver.getState().getErrorMessage().contains("forwarded execution identity"));
        assertNull(receiver.getCurrentUserIdentity());
        assertNull(receiver.getAuthenticatedTaskIdentity());
        assertEquals(0, executedStatements);
    }

    @Test
    public void testIncompleteIdentityCannotUseDefaultNativePrincipal() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot();
        request.getTask_execution_identity().unsetIs_ephemeral();
        ConnectContext receiver = receive(request);
        assertNull(receiver.getAuthenticatedTaskIdentity());
        assertThrows(SemanticException.class, () -> TaskBuilder.buildTask(resolvedAISubmit(receiver), receiver));
        assertEquals(List.of("WARN: Ignoring forwarded AI task identity: incomplete envelope"), appender.messages);
    }

    @Test
    public void testUnknownIdentityVersionIsUntrusted() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot();
        request.getTask_execution_identity().setVersion(99);
        ConnectContext receiver = receive(request);
        assertNull(receiver.getAuthenticatedTaskIdentity());
        assertThrows(SemanticException.class, () -> TaskBuilder.buildTask(resolvedAISubmit(receiver), receiver));
        assertEquals(List.of("WARN: Ignoring forwarded AI task identity: unsupported version"), appender.messages);
    }

    @Test
    public void testInvalidIdentityAttributesAreRejectedWithoutExposingSecrets() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot();
        request.getTask_execution_identity().setSecurity_integration("sensitive-mechanism-sentinel");
        request.getTask_execution_identity().setDistinguished_name("CN=sensitive-dn-sentinel,token=sensitive-token-sentinel");
        ConnectContext receiver = receive(request);
        assertNull(receiver.getAuthenticatedTaskIdentity());
        SemanticException exception = assertThrows(SemanticException.class,
                () -> TaskBuilder.buildTask(resolvedAISubmit(receiver), receiver));
        assertFalse(exception.getMessage().contains("sensitive"));
        assertEquals(List.of("WARN: Ignoring forwarded AI task identity: invalid attributes"), appender.messages);
        assertFalse(appender.hasException);
    }

    @Test
    public void testTaskSourceSurvivesForwardingWithoutIdentityMetadata() throws Exception {
        TMasterOpRequest request = requestFromAuthenticatedRoot(QueryDetail.QuerySource.TASK);
        TMasterOpRequest._Fields field = TMasterOpRequest._Fields.findByName("task_execution_identity");
        if (field != null) {
            request.setFieldValue(field, null);
        }
        ConnectContext receiver = receive(request);
        assertEquals(QueryDetail.QuerySource.TASK, receiver.getQuerySource());
        assertThrows(SemanticException.class,
                () -> Authorizer.checkAIFunctionPrivileges(resolvedAISubmit(receiver), receiver));
    }

    private static TMasterOpRequest requestFromAuthenticatedRoot() throws Exception {
        return requestFromAuthenticatedRoot(QueryDetail.QuerySource.EXTERNAL);
    }

    private static TMasterOpRequest requestFromAuthenticatedRoot(QueryDetail.QuerySource source) throws Exception {
        ConnectContext context = new ConnectContext();
        AuthenticationHandler.authenticate(context, "root", "127.0.0.1", new byte[0]);
        context.setQueryId(UUID.randomUUID());
        context.setDatabase("test");
        context.setQuerySource(source);
        context.setThreadLocalInfo();
        return new LeaderOpExecutor(Pair.create("127.0.0.1", 9020), null,
                new OriginStatement("select 1", 0), context, RedirectStatus.FORWARD_NO_SYNC, false)
                .createTMasterOpRequest(context, 0);
    }

    private static ConnectContext receive(TMasterOpRequest request) {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        new ConnectProcessor(context).proxyExecute(request, null);
        return context;
    }

    private static SubmitTaskStmt resolvedAISubmit(ConnectContext context) {
        SubmitTaskStmt statement = (SubmitTaskStmt) SqlParser.parse(
                "submit task forwarded_ai as create table task_output as select ai_complete('prompt') as answer",
                context.getSessionVariable()).get(0);
        SelectRelation select = (SelectRelation) statement.getCreateTableAsSelectStmt()
                .getQueryStatement().getQueryRelation();
        FunctionCallExpr call = (FunctionCallExpr) select.getSelectList().getItems().get(0).getExpr();
        Function function = new Function(new FunctionName("ai_complete"),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        function.setBinaryType(TFunctionBinaryType.AI);
        call.setFn(function);
        select.setOutputExpr(List.of(call));
        return statement;
    }

    private static class IdentityLogAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();
        private boolean hasException;

        IdentityLogAppender() {
            super("ai-task-identity-diagnostics", null, null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getLevel() + ": " + event.getMessage().getFormattedMessage());
            hasException |= event.getThrown() != null;
        }
    }
}
