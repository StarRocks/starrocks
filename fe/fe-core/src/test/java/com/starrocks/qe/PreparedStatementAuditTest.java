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

import com.starrocks.authentication.AccessControlContext;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for audit logging and metrics in prepared statement execution (COM_STMT_EXECUTE)
 */
public class PreparedStatementAuditTest extends DDLTestBase {
    @Mocked
    private static StreamConnection connection;

    private static PQueryStatistics statistics = new PQueryStatistics();
    private static ConnectContext myContext;

    @BeforeAll
    public static void setUpClass() throws Exception {
        FeConstants.runningUnitTest = false;
        statistics.scanBytes = 0L;
        statistics.scanRows = 0L;
        DDLTestBase.beforeAll();
        MetricRepo.init();
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Mock
        MysqlChannel channel = new MysqlChannel(connection);
        new Expectations(channel) {
            {
                channel.getRemoteHostPortString();
                minTimes = 0;
                result = "127.0.0.1:12345";
            }
        };
        myContext = new ConnectContext(connection);
        Deencapsulation.setField(myContext, "mysqlChannel", channel);
        super.setUp();
    }

    private static MysqlChannel mockChannel(ByteBuffer packet) {
        try {
            MysqlChannel channel = new MysqlChannel(connection);
            new Expectations(channel) {
                {
                    // Mock receive
                    channel.fetchOnePacket();
                    minTimes = 0;
                    result = packet;

                    // Mock reset
                    channel.setSequenceId(0);
                    times = 1;

                    // Mock send
                    channel.sendAndFlush((ByteBuffer) any);
                    minTimes = 0;

                    channel.getRemoteHostPortString();
                    minTimes = 0;
                    result = "127.0.0.1:12345";
                }
            };
            return channel;
        } catch (IOException e) {
            return null;
        }
    }

    private static ConnectContext initMockContext(MysqlChannel channel, GlobalStateMgr globalStateMgr) {
        ConnectContext context = new ConnectContext(connection) {
            @Override
            public void setKilled() {
                myContext.setKilled();
            }

            @Override
            public MysqlSerializer getSerializer() {
                return myContext.getSerializer();
            }

            @Override
            public QueryState getState() {
                return myContext.getState();
            }

            @Override
            public void setStartTime() {
                myContext.setStartTime();
            }

            @Override
            public String getDatabase() {
                return myContext.getDatabase();
            }

            @Override
            public void setDatabase(String db) {
                myContext.setDatabase(db);
            }

            @Override
            public long getStmtId() {
                return myContext.getStmtId();
            }

            @Override
            public TUniqueId getExecutionId() {
                return myContext.getExecutionId();
            }

            @Override
            public MysqlCapability getCapability() {
                return myContext.getCapability();
            }

            @Override
            public AccessControlContext getAccessControlContext() {
                return myContext.getAccessControlContext();
            }

            @Override
            public PlainPasswordAuthenticationProvider getAuthenticationProvider() {
                return myContext.getAuthenticationProvider();
            }
        };

        new Expectations(context) {
            {
                context.getMysqlChannel();
                minTimes = 0;
                result = channel;

                context.getGlobalStateMgr();
                minTimes = 0;
                result = globalStateMgr;

                context.getQualifiedUser();
                minTimes = 0;
                result = "testUser";

                context.getDatabase();
                minTimes = 0;
                result = "testDb1";

                context.getStmtId();
                minTimes = 0;
                result = 1L;

                context.getExecutionId();
                minTimes = 0;
                result = new TUniqueId();

                context.getCapability();
                minTimes = 0;
                result = MysqlCapability.DEFAULT_CAPABILITY;

                context.getAccessControlContext();
                minTimes = 0;
                result = new AccessControlContext();

                context.getAuthenticationProvider();
                minTimes = 0;
                result = new PlainPasswordAuthenticationProvider(MysqlPassword.EMPTY_PASSWORD);
            }
        };

        return context;
    }

    /**
     * Helper method to create a COM_STMT_EXECUTE packet
     */
    private ByteBuffer createExecutePacket(int stmtId, List<Object> params) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();

        // Command type COM_STMT_EXECUTE (0x17 = 23)
        serializer.writeInt1(23);

        // Statement ID
        serializer.writeInt4(stmtId);

        // Flags (0 = CURSOR_TYPE_NO_CURSOR)
        serializer.writeInt1(0);

        // Iteration count (always 1)
        serializer.writeInt4(1);

        // NULL bitmap (empty for no parameters)
        int nullBitmapLength = (params.size() + 7) / 8;
        if (nullBitmapLength > 0) {
            byte[] nullBitmap = new byte[nullBitmapLength];
            serializer.writeBytes(nullBitmap);
        }

        // new_params_bind_flag (0 = types not included)
        if (params.size() > 0) {
            serializer.writeInt1(0);
        }

        return serializer.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Helper method to create a mock PrepareStmt
     */
    private PrepareStmt createMockPrepareStmt(String sql) {
        try {
            // Create a simple statement for testing
            StatementBase innerStmt = UtFrameUtils.parseStmtWithNewParser(sql,
                    UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
            return new PrepareStmt("test_stmt", innerStmt, null);
        } catch (Exception e) {
            // Return a simple mock for test purposes
            return new PrepareStmt("test_stmt", null, null);
        }
    }

    /**
     * Test that audit event is created for successful prepared statement execution
     */
    @Test
    public void testAuditEventForSuccessfulExecution() throws Exception {
        int stmtId = 1;
        ByteBuffer executePacket = createExecutePacket(stmtId, new ArrayList<>());
        ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

        // Create a prepared statement context with a query statement
        PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 AS col1");
        PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
        ctx.putPreparedStmt(String.valueOf(stmtId), prepareCtx);

        // Track audit event
        AtomicReference<AuditEvent> capturedAuditEvent = new AtomicReference<>();

        // Mock AuditEventProcessor to capture the audit event
        new MockUp<AuditEventProcessor>() {
            @Mock
            public void handleAuditEvent(AuditEvent auditEvent) {
                capturedAuditEvent.set(auditEvent);
            }
        };

        // Mock StmtExecutor
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() throws Exception {
                // Simulate successful execution
                ctx.getState().setOk();
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return statistics;
            }

            @Mock
            public StatementBase getParsedStmt() {
                return prepareStmt;
            }
        };

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();

        // Verify that audit event was created
        AuditEvent auditEvent = capturedAuditEvent.get();
        Assertions.assertNotNull(auditEvent, "Audit event should be created for prepared statement execution");
        Assertions.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assertions.assertTrue(auditEvent.isQuery, "Audit event should be marked as query");
        Assertions.assertNotNull(auditEvent.stmt, "Audit event should contain the statement");
    }

    /**
     * Test that audit event is created even when prepared statement is not found
     */
    @Test
    public void testAuditEventForMissingPreparedStatement() throws Exception {
        int stmtId = 999; // Non-existent statement ID
        ByteBuffer executePacket = createExecutePacket(stmtId, new ArrayList<>());
        ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

        // Track audit event
        AtomicReference<AuditEvent> capturedAuditEvent = new AtomicReference<>();

        // Mock AuditEventProcessor to capture the audit event
        new MockUp<AuditEventProcessor>() {
            @Mock
            public void handleAuditEvent(AuditEvent auditEvent) {
                capturedAuditEvent.set(auditEvent);
            }
        };

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();

        // Verify that audit event was created even for error case
        AuditEvent auditEvent = capturedAuditEvent.get();
        Assertions.assertNotNull(auditEvent, "Audit event should be created even when prepared statement is not found");
        Assertions.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assertions.assertTrue(ctx.getState().isError(), "State should be error");
        Assertions.assertNotNull(auditEvent.stmt, "Audit event should contain the statement");
    }

    /**
     * Test that audit event is created when prepared statement execution throws exception
     */
    @Test
    public void testAuditEventForExecutionException() throws Exception {
        int stmtId = 1;
        ByteBuffer executePacket = createExecutePacket(stmtId, new ArrayList<>());
        ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

        // Create a prepared statement context with a query statement
        PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 AS col1");
        PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
        ctx.putPreparedStmt(String.valueOf(stmtId), prepareCtx);

        // Track audit event
        AtomicReference<AuditEvent> capturedAuditEvent = new AtomicReference<>();

        // Mock AuditEventProcessor to capture the audit event
        new MockUp<AuditEventProcessor>() {
            @Mock
            public void handleAuditEvent(AuditEvent auditEvent) {
                capturedAuditEvent.set(auditEvent);
            }
        };

        // Mock StmtExecutor to throw exception
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() throws Exception {
                throw new RuntimeException("Test execution error");
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return statistics;
            }

            @Mock
            public StatementBase getParsedStmt() {
                return prepareStmt;
            }
        };

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();

        // Verify that audit event was created even for exception case
        AuditEvent auditEvent = capturedAuditEvent.get();
        Assertions.assertNotNull(auditEvent, "Audit event should be created even when execution throws exception");
        Assertions.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assertions.assertTrue(ctx.getState().isError(), "State should be error");
        Assertions.assertNotNull(auditEvent.errorCode, "Audit event should contain error code");
    }

    /**
     * Test that metrics are updated for successful prepared statement execution
     */
    @Test
    public void testMetricsForSuccessfulExecution() throws Exception {
        int stmtId = 1;
        ByteBuffer executePacket = createExecutePacket(stmtId, new ArrayList<>());
        ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

        // Create a prepared statement context with a query statement
        PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 AS col1");
        PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
        ctx.putPreparedStmt(String.valueOf(stmtId), prepareCtx);

        // Get initial metric values
        long initialQueryCount = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long initialSuccessCount = MetricRepo.COUNTER_QUERY_SUCCESS.getValue();

        // Mock StmtExecutor
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() throws Exception {
                // Simulate successful execution
                ctx.getState().setOk();
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return statistics;
            }

            @Mock
            public StatementBase getParsedStmt() {
                return prepareStmt;
            }
        };

        // Mock AuditEventProcessor to avoid processing
        new MockUp<AuditEventProcessor>() {
            @Mock
            public void handleAuditEvent(AuditEvent auditEvent) {
                // Do nothing
            }
        };

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();

        // Verify metrics were updated
        Assertions.assertEquals(initialQueryCount + 1, MetricRepo.COUNTER_QUERY_ALL.getValue(),
                "Query count should be incremented");
        Assertions.assertEquals(initialSuccessCount + 1, MetricRepo.COUNTER_QUERY_SUCCESS.getValue(),
                "Success count should be incremented");
    }

    /**
     * Test that error metrics are updated when prepared statement execution fails
     */
    @Test
    public void testMetricsForFailedExecution() throws Exception {
        int stmtId = 1;
        ByteBuffer executePacket = createExecutePacket(stmtId, new ArrayList<>());
        ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

        // Create a prepared statement context with a query statement
        PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 AS col1");
        PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
        ctx.putPreparedStmt(String.valueOf(stmtId), prepareCtx);

        // Get initial metric values
        long initialQueryCount = MetricRepo.COUNTER_QUERY_ALL.getValue();
        long initialErrCount = MetricRepo.COUNTER_QUERY_ERR.getValue();
        long initialInternalErrCount = MetricRepo.COUNTER_QUERY_INTERNAL_ERR.getValue();

        // Mock StmtExecutor to throw exception
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() throws Exception {
                throw new RuntimeException("Test execution error");
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return statistics;
            }

            @Mock
            public StatementBase getParsedStmt() {
                return prepareStmt;
            }
        };

        // Mock AuditEventProcessor to avoid processing
        new MockUp<AuditEventProcessor>() {
            @Mock
            public void handleAuditEvent(AuditEvent auditEvent) {
                // Do nothing
            }
        };

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();

        // Verify error metrics were updated
        Assertions.assertEquals(initialQueryCount + 1, MetricRepo.COUNTER_QUERY_ALL.getValue(),
                "Query count should be incremented even for errors");
        Assertions.assertEquals(initialErrCount + 1, MetricRepo.COUNTER_QUERY_ERR.getValue(),
                "Error count should be incremented");
        Assertions.assertEquals(initialInternalErrCount + 1, MetricRepo.COUNTER_QUERY_INTERNAL_ERR.getValue(),
                "Internal error count should be incremented");
    }

    /**
     * Test that analysis error metrics are updated when prepared statement is not found
     */
    @Test
    public void testMetricsForAnalysisError() throws Exception {
        int stmtId = 999; // Non-existent statement ID
        ByteBuffer executePacket = createExecutePacket(stmtId, new ArrayList<>());
        ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

        // Get initial metric values
        long initialAnalysisErrCount = MetricRepo.COUNTER_QUERY_ANALYSIS_ERR.getValue();

        // Mock AuditEventProcessor to avoid processing
        new MockUp<AuditEventProcessor>() {
            @Mock
            public void handleAuditEvent(AuditEvent auditEvent) {
                // Do nothing
            }
        };

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();

        // Verify analysis error metrics were updated
        Assertions.assertEquals(initialAnalysisErrCount + 1, MetricRepo.COUNTER_QUERY_ANALYSIS_ERR.getValue(),
                "Analysis error count should be incremented when prepared statement is not found");
    }
}
