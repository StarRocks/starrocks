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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/ConnectProcessorTest.java

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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.authentication.AccessControlContext;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlErrPacket;
import com.starrocks.mysql.MysqlOkPacket;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.plugin.AuditEvent.AuditEventBuilder;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.transaction.ExplicitTxnStatementValidator;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectProcessorTest extends DDLTestBase {
    private static ByteBuffer initDbPacket;
    private static ByteBuffer initWarehousePacket;
    private static ByteBuffer changeUserPacket;
    private static ByteBuffer resetConnectionPacket;
    private static ByteBuffer pingPacket;
    private static ByteBuffer quitPacket;
    private static ByteBuffer queryPacket;
    private static ByteBuffer fieldListPacket;
    private static AuditEventBuilder auditBuilder = new AuditEventBuilder();
    private static ConnectContext myContext;

    private static StreamConnection connection;

    static {
        connection = Mockito.mock(StreamConnection.class);
        java.net.InetSocketAddress mockAddr = new java.net.InetSocketAddress("127.0.0.1", 12345);
        Mockito.when(connection.getPeerAddress()).thenReturn(mockAddr);
    }

    private static PQueryStatistics statistics = new PQueryStatistics();

    @BeforeAll
    public static void setUpClass() {
        FeConstants.runningUnitTest = false;
        // Init Database packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(2);
            serializer.writeEofString("testDb1");
            initDbPacket = serializer.toByteBuffer();
        }

        // Init Warehouse packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(2);
            serializer.writeEofString("'warehouse aaa'");
            initWarehousePacket = serializer.toByteBuffer();
        }

        // Change user packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(17);
            // code
            serializer.writeInt1(17);
            // user name
            serializer.writeNulTerminateString("starrocks-user");
            // plugin data
            serializer.writeInt1(20);
            byte[] buf = new byte[20];
            for (int i = 0; i < 20; ++i) {
                buf[i] = (byte) ('a' + i);
            }
            serializer.writeBytes(buf);
            // database
            serializer.writeNulTerminateString("testDb");
            // character set
            serializer.writeInt2(33);
            changeUserPacket = serializer.toByteBuffer();
        }

        // Reset connection packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(31);
            resetConnectionPacket = serializer.toByteBuffer();
        }

        // Ping packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(14);
            pingPacket = serializer.toByteBuffer();
        }

        // Quit packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(1);
            quitPacket = serializer.toByteBuffer();
        }

        // Query packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(3);
            serializer.writeEofString("select * from a");
            queryPacket = serializer.toByteBuffer();
        }

        // Field list packet
        {
            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(4);
            serializer.writeNulTerminateString("testTable1");
            serializer.writeEofString("");
            fieldListPacket = serializer.toByteBuffer();
        }

        statistics.scanBytes = 0L;
        statistics.scanRows = 0L;
        DDLTestBase.beforeAll();
    }

    @BeforeEach
    public void setUp() throws Exception {
        initDbPacket.clear();
        initWarehousePacket.clear();
        pingPacket.clear();
        quitPacket.clear();
        queryPacket.clear();
        fieldListPacket.clear();
        changeUserPacket.clear();
        resetConnectionPacket.clear();
        // Mock
        MysqlChannel channel = Mockito.mock(MysqlChannel.class);
        Mockito.when(channel.getRemoteHostPortString()).thenReturn("127.0.0.1:12345");
        myContext = new ConnectContext(connection);
        Deencapsulation.setField(myContext, "mysqlChannel", channel);
        super.setUp();
    }

    private static MysqlChannel mockChannel(ByteBuffer packet) {
        try {
            MysqlChannel channel = Mockito.mock(MysqlChannel.class);
            Mockito.when(channel.fetchOnePacket()).thenReturn(packet);
            Mockito.when(channel.getRemoteHostPortString()).thenReturn("127.0.0.1:12345");
            return channel;
        } catch (IOException e) {
            return null;
        }
    }

    private static ConnectContext initMockContext(MysqlChannel channel, GlobalStateMgr globalStateMgr) {
        ConnectContext context = Mockito.spy(new ConnectContext(connection) {
            private boolean firstTimeToSetCommand = true;

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
            public void setCommand(MysqlCommand command) {
                if (firstTimeToSetCommand) {
                    myContext.setCommand(command);
                    firstTimeToSetCommand = false;
                } else {
                    super.setCommand(command);
                }
            }
        });

        Mockito.doReturn(channel).when(context).getMysqlChannel();
        Mockito.doReturn(false).doReturn(true).doReturn(false).when(context).isKilled();
        Mockito.doReturn(globalStateMgr).when(context).getGlobalStateMgr();
        Mockito.doReturn(auditBuilder).when(context).getAuditEventBuilder();
        Mockito.doReturn("testCluster:user").when(context).getQualifiedUser();
        Mockito.doReturn(UserIdentity.ROOT).when(context).getCurrentUserIdentity();
        Mockito.doReturn(0L).when(context).getStartTime();
        Mockito.doReturn(1L).when(context).getReturnRows();
        Mockito.doNothing().when(context).setStmtId(Mockito.anyLong());
        Mockito.doReturn(1L).when(context).getStmtId();
        Mockito.doReturn(new TUniqueId()).when(context).getExecutionId();
        Mockito.doReturn(MysqlCapability.DEFAULT_CAPABILITY).when(context).getCapability();
        Mockito.doReturn(new AccessControlContext()).when(context).getAccessControlContext();
        Mockito.doReturn(new PlainPasswordAuthenticationProvider(MysqlPassword.EMPTY_PASSWORD))
                .when(context).getAuthenticationProvider();

        return context;
    }

    @Test
    public void testQuit() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(quitPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_QUIT, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assertions.assertTrue(myContext.isKilled());
    }

    @Test
    public void testInitDb() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initDbPacket), GlobalStateMgr.getCurrentState());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
    }

    @Test
    public void testInitDbFail() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initDbPacket), GlobalStateMgr.getCurrentState());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assertions.assertFalse(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
    }

    @Test
    public void testInitWarehouse() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initWarehousePacket), GlobalStateMgr.getCurrentState());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
    }

    @Test
    public void testChangeUser() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(changeUserPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_CHANGE_USER, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assertions.assertFalse(myContext.isKilled());
    }

    @Test
    public void testResetConnection() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(resetConnectionPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_RESET_CONNECTION, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assertions.assertFalse(myContext.isKilled());
    }

    @Test
    public void testPing() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(pingPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_PING, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assertions.assertFalse(myContext.isKilled());
    }

    @Test
    public void testPingLoop() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(pingPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.loopForTest();
        Assertions.assertEquals(MysqlCommand.COM_PING, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assertions.assertFalse(myContext.isKilled());
    }

    @Test
    public void testQuery() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        // Mock statement executor
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (mock, mockCtx) -> {
                    Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(statistics);
                })) {
            processor.processOnce();
            Assertions.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
        }
    }

    // Verify multi-statement query emits one after-audit per stmt when all stmts succeed.
    @Test
    public void testMultiStatementAuditPerStatement() throws Exception {
        ByteBuffer packet = createQueryPacket("select 1; select 2;");
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
        List<String> auditedSqls = new ArrayList<>();
        ConnectProcessor processor = new ConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                       String digestFromLeader) {
                auditedSqls.add(origStmt);
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return null;
            }
        };

        processor.processOnce();
        Assertions.assertEquals(2, auditedSqls.size());
        Assertions.assertFalse(auditedSqls.get(0).contains(";"));
        Assertions.assertFalse(auditedSqls.get(1).contains(";"));
    }

    // Verify multi-statement query stops on the first failed stmt and only audits executed stmts.
    @Test
    public void testMultiStatementAuditStopsAfterFailure() throws Exception {
        ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
        List<String> auditRecords = new ArrayList<>();
        ConnectProcessor processor = new ConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                       String digestFromLeader) {
                auditRecords.add(ctx.getState().toString() + ":" + origStmt);
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute(Invocation invocation) throws Exception {
                StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                int stmtIdx = stmtExecutor.getParsedStmt().getOrigStmt().idx;
                if (stmtIdx == 1) {
                    throw new IOException("mock stmt failure");
                }
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return null;
            }
        };

        processor.processOnce();
        Assertions.assertEquals(2, auditRecords.size());
        Assertions.assertTrue(auditRecords.get(0).startsWith("OK:"));
        Assertions.assertTrue(auditRecords.get(1).startsWith("ERR:"));
        Assertions.assertFalse(auditRecords.get(0).contains(";"));
        Assertions.assertFalse(auditRecords.get(1).contains("; select 3"));
    }

    // Verify parse failure still records a single audit entry with the original raw SQL.
    @Test
    public void testParseFailureAuditsOriginalSql() throws Exception {
        ByteBuffer packet = createQueryPacket("select from");
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
        List<String> auditRecords = new ArrayList<>();
        ConnectProcessor processor = new ConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                       String digestFromLeader) {
                auditRecords.add(ctx.getState().toString() + ":" + origStmt);
            }
        };

        processor.processOnce();
        Assertions.assertEquals(1, auditRecords.size());
        Assertions.assertTrue(auditRecords.get(0).startsWith("ERR:"));
        Assertions.assertEquals("ERR:select from", auditRecords.get(0));
    }

    // Verify LargeInPredicate retry is scoped to the failing stmt instead of replaying previous stmts.
    @Test
    public void testMultiStatementLargeInPredicateRetriesCurrentStmtOnly() throws Exception {
        ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
        List<String> auditRecords = new ArrayList<>();
        int[] executeCounts = new int[3];
        ConnectProcessor processor = new ConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                       String digestFromLeader) {
                auditRecords.add(ctx.getState().toString() + ":" + origStmt);
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute(Invocation invocation) throws Exception {
                StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                int stmtIdx = stmtExecutor.getParsedStmt().getOrigStmt().idx;
                executeCounts[stmtIdx]++;
                if (stmtIdx == 1 && executeCounts[stmtIdx] == 1) {
                    throw new LargeInPredicateException("mock large in predicate retry");
                }
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return null;
            }
        };

        processor.processOnce();
        Assertions.assertArrayEquals(new int[] {1, 2, 1}, executeCounts);
        Assertions.assertEquals(3, auditRecords.size());
        Assertions.assertTrue(auditRecords.get(0).startsWith("OK:"));
        Assertions.assertTrue(auditRecords.get(1).startsWith("OK:"));
        Assertions.assertTrue(auditRecords.get(2).startsWith("OK:"));
        Assertions.assertFalse(auditRecords.get(0).contains(";"));
        Assertions.assertFalse(auditRecords.get(1).contains(";"));
        Assertions.assertFalse(auditRecords.get(2).contains(";"));
    }

    // Verify a stmt that still fails after LargeInPredicate retry is audited once and stops later stmts.
    @Test
    public void testMultiStatementLargeInPredicateRetryStopsAfterRetriedStmtFailure() throws Exception {
        ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
        List<String> auditRecords = new ArrayList<>();
        int[] executeCounts = new int[3];
        ConnectProcessor processor = new ConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                       String digestFromLeader) {
                auditRecords.add(ctx.getState().toString() + ":" + origStmt);
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute(Invocation invocation) throws Exception {
                StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                int stmtIdx = stmtExecutor.getParsedStmt().getOrigStmt().idx;
                executeCounts[stmtIdx]++;
                if (stmtIdx == 1) {
                    if (executeCounts[stmtIdx] == 1) {
                        throw new LargeInPredicateException("mock large in predicate retry");
                    }
                    throw new IOException("mock stmt failure after retry");
                }
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return null;
            }
        };

        processor.processOnce();
        Assertions.assertArrayEquals(new int[] {1, 2, 0}, executeCounts);
        Assertions.assertEquals(2, auditRecords.size());
        Assertions.assertTrue(auditRecords.get(0).startsWith("OK:"));
        Assertions.assertTrue(auditRecords.get(1).startsWith("ERR:"));
        Assertions.assertFalse(auditRecords.get(0).contains(";"));
        Assertions.assertFalse(auditRecords.get(1).contains("; select 3"));
    }

    // Verify before/after audit hooks both fire once per stmt for successful multi-statement execution.
    @Test
    public void testMultiStatementAuditBeforeAndAfterPerStatement() throws Exception {
        boolean oldAuditStmtBeforeExecute = Config.audit_stmt_before_execute;
        Config.audit_stmt_before_execute = true;
        try {
            ByteBuffer packet = createQueryPacket("select 1; select 2;");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            List<String> auditRecords = new ArrayList<>();
            ConnectProcessor processor = new ConnectProcessor(ctx) {
                @Override
                public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
                    auditRecords.add("BEFORE:" + parsedStmt.getOrigStmt().idx + ":" + origStmt);
                }

                @Override
                public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                           String digestFromLeader) {
                    auditRecords.add("AFTER:" + parsedStmt.getOrigStmt().idx + ":" + ctx.getState().toString()
                            + ":" + origStmt);
                }
            };
            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() {
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();
            Assertions.assertEquals(4, auditRecords.size());
            Assertions.assertTrue(auditRecords.get(0).startsWith("BEFORE:0:"));
            Assertions.assertTrue(auditRecords.get(1).startsWith("AFTER:0:OK:"));
            Assertions.assertTrue(auditRecords.get(2).startsWith("BEFORE:1:"));
            Assertions.assertTrue(auditRecords.get(3).startsWith("AFTER:1:OK:"));
            Assertions.assertFalse(auditRecords.get(0).contains(";"));
            Assertions.assertFalse(auditRecords.get(1).contains(";"));
            Assertions.assertFalse(auditRecords.get(2).contains(";"));
            Assertions.assertFalse(auditRecords.get(3).contains(";"));
        } finally {
            Config.audit_stmt_before_execute = oldAuditStmtBeforeExecute;
        }
    }

    // Verify before/after audit hooks stop after the first failing stmt in a multi-statement request.
    @Test
    public void testMultiStatementAuditBeforeAndAfterStopsAfterFailure() throws Exception {
        boolean oldAuditStmtBeforeExecute = Config.audit_stmt_before_execute;
        Config.audit_stmt_before_execute = true;
        try {
            ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            List<String> auditRecords = new ArrayList<>();
            ConnectProcessor processor = new ConnectProcessor(ctx) {
                @Override
                public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
                    auditRecords.add("BEFORE:" + parsedStmt.getOrigStmt().idx + ":" + origStmt);
                }

                @Override
                public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                           String digestFromLeader) {
                    auditRecords.add("AFTER:" + parsedStmt.getOrigStmt().idx + ":" + ctx.getState().toString()
                            + ":" + origStmt);
                }
            };
            new MockUp<StmtExecutor>() {
                @Mock
                public void execute(Invocation invocation) throws Exception {
                    StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                    if (stmtExecutor.getParsedStmt().getOrigStmt().idx == 1) {
                        throw new IOException("mock stmt failure");
                    }
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();
            Assertions.assertEquals(4, auditRecords.size());
            Assertions.assertTrue(auditRecords.get(0).startsWith("BEFORE:0:"));
            Assertions.assertTrue(auditRecords.get(1).startsWith("AFTER:0:OK:"));
            Assertions.assertTrue(auditRecords.get(2).startsWith("BEFORE:1:"));
            Assertions.assertTrue(auditRecords.get(3).startsWith("AFTER:1:ERR:"));
            Assertions.assertFalse(auditRecords.get(0).contains(";"));
            Assertions.assertFalse(auditRecords.get(1).contains(";"));
            Assertions.assertFalse(auditRecords.get(2).contains(";"));
            Assertions.assertFalse(auditRecords.get(3).contains("; select 3"));
        } finally {
            Config.audit_stmt_before_execute = oldAuditStmtBeforeExecute;
        }
    }

    // Verify stmt-level retry does not emit duplicated BEFORE audit entries for the retried stmt.
    @Test
    public void testMultiStatementAuditBeforeExecOnlyOnceWhenStmtRetries() throws Exception {
        boolean oldAuditStmtBeforeExecute = Config.audit_stmt_before_execute;
        Config.audit_stmt_before_execute = true;
        try {
            ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            List<String> auditRecords = new ArrayList<>();
            int[] executeCounts = new int[3];
            ConnectProcessor processor = new ConnectProcessor(ctx) {
                @Override
                public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
                    auditRecords.add("BEFORE:" + parsedStmt.getOrigStmt().idx + ":" + origStmt);
                }

                @Override
                public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                           String digestFromLeader) {
                    auditRecords.add("AFTER:" + parsedStmt.getOrigStmt().idx + ":" + ctx.getState().toString()
                            + ":" + origStmt);
                }
            };
            new MockUp<StmtExecutor>() {
                @Mock
                public void execute(Invocation invocation) throws Exception {
                    StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                    int stmtIdx = stmtExecutor.getParsedStmt().getOrigStmt().idx;
                    executeCounts[stmtIdx]++;
                    if (stmtIdx == 1 && executeCounts[stmtIdx] == 1) {
                        throw new LargeInPredicateException("mock large in predicate retry");
                    }
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();
            Assertions.assertArrayEquals(new int[] {1, 2, 1}, executeCounts);
            Assertions.assertEquals(6, auditRecords.size());
            Assertions.assertTrue(auditRecords.get(0).startsWith("BEFORE:0:"));
            Assertions.assertTrue(auditRecords.get(1).startsWith("AFTER:0:OK:"));
            Assertions.assertTrue(auditRecords.get(2).startsWith("BEFORE:1:"));
            Assertions.assertTrue(auditRecords.get(3).startsWith("AFTER:1:OK:"));
            Assertions.assertTrue(auditRecords.get(4).startsWith("BEFORE:2:"));
            Assertions.assertTrue(auditRecords.get(5).startsWith("AFTER:2:OK:"));
        } finally {
            Config.audit_stmt_before_execute = oldAuditStmtBeforeExecute;
        }
    }

    // Verify validation failure before executor.execute still audits the failing stmt.
    @Test
    public void testMultiStatementPreExecutionFailureAuditsFailedStmt() throws Exception {
        ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
        ctx.setTxnId(1);
        List<String> auditRecords = new ArrayList<>();
        int[] executeCounts = new int[3];
        ConnectProcessor processor = new ConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                       String digestFromLeader) {
                auditRecords.add(ctx.getState().toString() + ":" + origStmt);
            }
        };
        new MockUp<ExplicitTxnStatementValidator>() {
            @Mock
            public void validate(StatementBase statement, ConnectContext context) {
                if (statement.getOrigStmt().idx == 1) {
                    throw new SemanticException("mock pre execution failure");
                }
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute(Invocation invocation) {
                StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                executeCounts[stmtExecutor.getParsedStmt().getOrigStmt().idx]++;
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return null;
            }
        };

        processor.processOnce();
        Assertions.assertArrayEquals(new int[] {1, 0, 0}, executeCounts);
        Assertions.assertEquals(2, auditRecords.size());
        Assertions.assertTrue(auditRecords.get(0).startsWith("OK:"));
        Assertions.assertTrue(auditRecords.get(1).startsWith("ERR:"));
        Assertions.assertFalse(auditRecords.get(0).contains(";"));
        Assertions.assertFalse(auditRecords.get(1).contains("; select 3"));
    }

    // Verify validation failure before execution still gets both BEFORE and AFTER audit records.
    @Test
    public void testMultiStatementPreExecutionFailureHasBeforeAndAfterAudit() throws Exception {
        boolean oldAuditStmtBeforeExecute = Config.audit_stmt_before_execute;
        Config.audit_stmt_before_execute = true;
        try {
            ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            ctx.setTxnId(1);
            List<String> auditRecords = new ArrayList<>();
            ConnectProcessor processor = new ConnectProcessor(ctx) {
                @Override
                public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
                    auditRecords.add("BEFORE:" + parsedStmt.getOrigStmt().idx + ":" + origStmt);
                }

                @Override
                public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                           String digestFromLeader) {
                    auditRecords.add("AFTER:" + parsedStmt.getOrigStmt().idx + ":" + ctx.getState().toString()
                            + ":" + origStmt);
                }
            };
            new MockUp<ExplicitTxnStatementValidator>() {
                @Mock
                public void validate(StatementBase statement, ConnectContext context) {
                    if (statement.getOrigStmt().idx == 1) {
                        throw new SemanticException("mock pre execution failure");
                    }
                }
            };
            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() {
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();
            Assertions.assertEquals(4, auditRecords.size());
            Assertions.assertTrue(auditRecords.get(0).startsWith("BEFORE:0:"));
            Assertions.assertTrue(auditRecords.get(1).startsWith("AFTER:0:OK:"));
            Assertions.assertTrue(auditRecords.get(2).startsWith("BEFORE:1:"));
            Assertions.assertTrue(auditRecords.get(3).startsWith("AFTER:1:ERR:"));
        } finally {
            Config.audit_stmt_before_execute = oldAuditStmtBeforeExecute;
        }
    }

    // Verify non-Exception Throwable from stmt execution still produces one failure audit for the current stmt.
    @Test
    public void testMultiStatementThrowableFailureStillAuditsFailedStmt() throws Exception {
        ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
        List<String> auditRecords = new ArrayList<>();
        int[] executeCounts = new int[3];
        ConnectProcessor processor = new ConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                       String digestFromLeader) {
                auditRecords.add(ctx.getState().toString() + ":" + origStmt);
            }
        };
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute(Invocation invocation) {
                StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                int stmtIdx = stmtExecutor.getParsedStmt().getOrigStmt().idx;
                executeCounts[stmtIdx]++;
                if (stmtIdx == 1) {
                    throw new AssertionError("mock throwable failure");
                }
            }

            @Mock
            public PQueryStatistics getQueryStatisticsForAuditLog() {
                return null;
            }
        };

        processor.processOnce();
        Assertions.assertArrayEquals(new int[] {1, 1, 0}, executeCounts);
        Assertions.assertEquals(2, auditRecords.size());
        Assertions.assertTrue(auditRecords.get(0).startsWith("OK:"));
        Assertions.assertTrue(auditRecords.get(1).startsWith("ERR:"));
        Assertions.assertFalse(auditRecords.get(0).contains(";"));
        Assertions.assertFalse(auditRecords.get(1).contains("; select 3"));
    }

    // Verify query detail records one running and one finished entry per stmt in successful multi-statement execution.
    @Test
    public void testQueryDetailForMultiStatement() throws Exception {
        boolean enableCollectQueryDetail = Config.enable_collect_query_detail_info;
        Config.enable_collect_query_detail_info = true;
        QueryDetailQueue.TOTAL_QUERIES.clear();
        try {
            ByteBuffer packet = createQueryPacket("select 1; select 2;");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            ConnectProcessor processor = new ConnectProcessor(ctx);

            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() {
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();

            List<QueryDetail> details = QueryDetailQueue.getQueryDetailsAfterTime(0);
            int runningCount = 0;
            int finishedCount = 0;
            for (QueryDetail detail : details) {
                if (detail.getState() == QueryDetail.QueryMemState.RUNNING) {
                    runningCount++;
                    Assertions.assertFalse(detail.getSql().contains(";"));
                } else if (detail.getState() == QueryDetail.QueryMemState.FINISHED) {
                    finishedCount++;
                    Assertions.assertFalse(detail.getSql().contains(";"));
                }
            }
            Assertions.assertEquals(4, details.size());
            Assertions.assertEquals(2, runningCount);
            Assertions.assertEquals(2, finishedCount);
        } finally {
            Config.enable_collect_query_detail_info = enableCollectQueryDetail;
            QueryDetailQueue.TOTAL_QUERIES.clear();
        }
    }

    // Verify query detail only contains executed stmts and marks the failed stmt as FAILED.
    @Test
    public void testQueryDetailForMultiStatementStopsAfterFailure() throws Exception {
        boolean enableCollectQueryDetail = Config.enable_collect_query_detail_info;
        Config.enable_collect_query_detail_info = true;
        QueryDetailQueue.TOTAL_QUERIES.clear();
        try {
            ByteBuffer packet = createQueryPacket("select 1; select 2; select 3;");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            ConnectProcessor processor = new ConnectProcessor(ctx);

            new MockUp<StmtExecutor>() {
                @Mock
                public void execute(Invocation invocation) throws Exception {
                    StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                    if (stmtExecutor.getParsedStmt().getOrigStmt().idx == 1) {
                        throw new IOException("mock stmt failure");
                    }
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();

            List<QueryDetail> details = QueryDetailQueue.getQueryDetailsAfterTime(0);
            int runningCount = 0;
            int finishedCount = 0;
            int failedCount = 0;
            for (QueryDetail detail : details) {
                if (detail.getState() == QueryDetail.QueryMemState.RUNNING) {
                    runningCount++;
                } else if (detail.getState() == QueryDetail.QueryMemState.FINISHED) {
                    finishedCount++;
                } else if (detail.getState() == QueryDetail.QueryMemState.FAILED) {
                    failedCount++;
                }
                Assertions.assertFalse(detail.getSql().contains(";"));
                Assertions.assertFalse(detail.getSql().contains("select 3"));
            }
            Assertions.assertEquals(4, details.size());
            Assertions.assertEquals(2, runningCount);
            Assertions.assertEquals(1, finishedCount);
            Assertions.assertEquals(1, failedCount);
        } finally {
            Config.enable_collect_query_detail_info = enableCollectQueryDetail;
            QueryDetailQueue.TOTAL_QUERIES.clear();
        }
    }

    // Verify stmt retry reuses the same stmt identity so query detail does not duplicate running entries.
    @Test
    public void testQueryDetailForMultiStatementRetryDoesNotDuplicateRunning() throws Exception {
        boolean enableCollectQueryDetail = Config.enable_collect_query_detail_info;
        Config.enable_collect_query_detail_info = true;
        QueryDetailQueue.TOTAL_QUERIES.clear();
        try {
            ByteBuffer packet = createQueryPacket("select 1; select 2;");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            ConnectProcessor processor = new ConnectProcessor(ctx);
            int[] executeCounts = new int[2];
            List<String> retriedStmtQueryIds = new ArrayList<>();

            new MockUp<StmtExecutor>() {
                @Mock
                public void execute(Invocation invocation) throws Exception {
                    StmtExecutor stmtExecutor = (StmtExecutor) invocation.getInvokedInstance();
                    int stmtIdx = stmtExecutor.getParsedStmt().getOrigStmt().idx;
                    executeCounts[stmtIdx]++;
                    if (stmtIdx == 1) {
                        retriedStmtQueryIds.add(ctx.getQueryId().toString());
                    }
                    if (stmtIdx == 1 && executeCounts[stmtIdx] == 1) {
                        throw new LargeInPredicateException("mock large in predicate retry");
                    }
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();

            List<QueryDetail> details = QueryDetailQueue.getQueryDetailsAfterTime(0);
            int runningCount = 0;
            int finishedCount = 0;
            for (QueryDetail detail : details) {
                if (detail.getState() == QueryDetail.QueryMemState.RUNNING) {
                    runningCount++;
                } else if (detail.getState() == QueryDetail.QueryMemState.FINISHED) {
                    finishedCount++;
                }
                Assertions.assertFalse(detail.getSql().contains(";"));
            }
            Assertions.assertArrayEquals(new int[] {1, 2}, executeCounts);
            Assertions.assertEquals(2, retriedStmtQueryIds.size());
            Assertions.assertEquals(retriedStmtQueryIds.get(0), retriedStmtQueryIds.get(1));
            Assertions.assertEquals(4, details.size());
            Assertions.assertEquals(2, runningCount);
            Assertions.assertEquals(2, finishedCount);
        } finally {
            Config.enable_collect_query_detail_info = enableCollectQueryDetail;
            QueryDetailQueue.TOTAL_QUERIES.clear();
        }
    }

    // Verify single-statement query detail keeps the original SQL text instead of formatted stmt SQL.
    @Test
    public void testSingleStatementQueryDetailKeepsOriginalSql() throws Exception {
        boolean enableCollectQueryDetail = Config.enable_collect_query_detail_info;
        Config.enable_collect_query_detail_info = true;
        QueryDetailQueue.TOTAL_QUERIES.clear();
        try {
            ByteBuffer packet = createQueryPacket("select 1 /*keep*/");
            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            ConnectProcessor processor = new ConnectProcessor(ctx);

            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() {
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();

            List<QueryDetail> details = QueryDetailQueue.getQueryDetailsAfterTime(0);
            Assertions.assertFalse(details.isEmpty());
            Assertions.assertTrue(details.get(0).getSql().contains("/*keep*/"));
        } finally {
            Config.enable_collect_query_detail_info = enableCollectQueryDetail;
            QueryDetailQueue.TOTAL_QUERIES.clear();
        }
    }

    @Test
    public void testQueryWithInlineWarehouse() throws Exception {
        Config.run_mode = RunMode.SHARED_DATA.getName();
        RunMode.detectRunMode();
        Config.enable_collect_query_detail_info = true;

        WarehouseComputeResourceProvider originalProvider =
                Deencapsulation.getField(GlobalStateMgr.getCurrentState().getWarehouseMgr(), "computeResourceProvider");
        WarehouseComputeResourceProvider spyProvider = Mockito.spy(originalProvider);
        Mockito.doReturn(true).when(spyProvider).isResourceAvailable(Mockito.any());
        Deencapsulation.setField(GlobalStateMgr.getCurrentState().getWarehouseMgr(), "computeResourceProvider", spyProvider);

        try {
            GlobalStateMgr.getCurrentState().getWarehouseMgr().addWarehouse(new DefaultWarehouse(2, "wh2"));
            GlobalStateMgr.getCurrentState().getWarehouseMgr().addWarehouse(new DefaultWarehouse(3, "wh3"));

            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(3);
            serializer.writeEofString("select /*+SET_VAR(enable_constant_execute_in_fe=false,warehouse='wh2')*/ 1");
            ByteBuffer packet = serializer.toByteBuffer();

            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            ctx.setCurrentUserIdentity(UserIdentity.ROOT);
            ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            ctx.setQualifiedUser(UserIdentity.ROOT.getUser());
            ctx.setQueryId(UUIDUtil.genUUID());

            ConnectProcessor processor = new ConnectProcessor(ctx);

            processor.processOnce();
            Assertions.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());

            QueryDetail queryDetail = ctx.getQueryDetail();
            Assertions.assertEquals("wh2", queryDetail.getWarehouse());

            DefaultCoordinator coordinator = (DefaultCoordinator) ctx.getExecutor().getCoordinator();
            Assertions.assertEquals(2, coordinator.getJobSpec().getComputeResource().getWarehouseId());

            AuditEvent auditEvent = ctx.getAuditEventBuilder().build();
            Assertions.assertEquals("wh2", auditEvent.warehouse);
        } finally {
            Deencapsulation.setField(GlobalStateMgr.getCurrentState().getWarehouseMgr(),
                    "computeResourceProvider", originalProvider);
            Config.enable_collect_query_detail_info = false;
            Config.run_mode = RunMode.SHARED_NOTHING.getName();
            RunMode.detectRunMode();
        }
    }

    @Test
    public void testQueryWithSetWarehouse() throws Exception {
        Config.run_mode = RunMode.SHARED_DATA.getName();
        RunMode.detectRunMode();
        Config.enable_collect_query_detail_info = true;

        WarehouseComputeResourceProvider originalProvider =
                Deencapsulation.getField(GlobalStateMgr.getCurrentState().getWarehouseMgr(), "computeResourceProvider");
        WarehouseComputeResourceProvider spyProvider = Mockito.spy(originalProvider);
        Mockito.doReturn(true).when(spyProvider).isResourceAvailable(Mockito.any());
        Deencapsulation.setField(GlobalStateMgr.getCurrentState().getWarehouseMgr(), "computeResourceProvider", spyProvider);

        try {
            GlobalStateMgr.getCurrentState().getWarehouseMgr().addWarehouse(new DefaultWarehouse(2, "wh2"));
            GlobalStateMgr.getCurrentState().getWarehouseMgr().addWarehouse(new DefaultWarehouse(3, "wh3"));

            MysqlSerializer serializer = MysqlSerializer.newInstance();
            serializer.writeInt1(3);
            serializer.writeEofString("select /*+SET_VAR(enable_constant_execute_in_fe=false)*/ 1");
            ByteBuffer packet = serializer.toByteBuffer();

            ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());
            ctx.getSessionVariable().setWarehouseName("wh3");

            ConnectProcessor processor = new ConnectProcessor(ctx);

            processor.processOnce();
            Assertions.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());

            QueryDetail queryDetail = ctx.getQueryDetail();
            Assertions.assertEquals("wh3", queryDetail.getWarehouse());

            DefaultCoordinator coordinator = (DefaultCoordinator) ctx.getExecutor().getCoordinator();
            Assertions.assertEquals(3, coordinator.getJobSpec().getComputeResource().getWarehouseId());

            AuditEvent auditEvent = ctx.getAuditEventBuilder().build();
            Assertions.assertEquals("wh3", auditEvent.warehouse);
        } finally {
            Deencapsulation.setField(GlobalStateMgr.getCurrentState().getWarehouseMgr(),
                    "computeResourceProvider", originalProvider);
            Config.enable_collect_query_detail_info = false;
            Config.run_mode = RunMode.SHARED_NOTHING.getName();
            RunMode.detectRunMode();
        }
    }

    @Test
    public void testQueryFail() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (mock, mockCtx) -> {
                    Mockito.doThrow(new IOException("Fail")).when(mock).execute();
                    Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(statistics);
                })) {
            processor.processOnce();
            Assertions.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
        }
    }

    @Test
    public void testQueryFail2() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (mock, mockCtx) -> {
                    Mockito.doThrow(new NullPointerException("Fail")).when(mock).execute();
                    Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(statistics);
                })) {
            processor.processOnce();
            Assertions.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
            Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        }
    }

    @Test
    public void testQueryWithCustomQueryId() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());
        ctx.getSessionVariable().setCustomQueryId("a_custom_query_id");

        ConnectProcessor processor = new ConnectProcessor(ctx);

        AtomicReference<String> customQueryId = new AtomicReference<>();
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (mock, mockCtx) -> {
                    Mockito.doAnswer(invocation -> {
                        customQueryId.set(ctx.getCustomQueryId());
                        return null;
                    }).when(mock).execute();
                    Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(null);
                })) {
            processor.processOnce();
            Assertions.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
            // verify customQueryId is set during query execution
            Assertions.assertEquals("a_custom_query_id", customQueryId.get());
            // customQueryId is cleared after query finished
            Assertions.assertEquals("", ctx.getCustomQueryId());
            Assertions.assertEquals("", ctx.getSessionVariable().getCustomQueryId());
        }
    }

    @Test
    public void testQueryWithCustomSessionName() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());
        ctx.getSessionVariable().setCustomSessionName("session_name");

        ConnectProcessor processor = new ConnectProcessor(ctx);

        AtomicReference<String> customSessionName = new AtomicReference<>();
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (mock, mockCtx) -> {
                    Mockito.doAnswer(invocation -> {
                        customSessionName.set(ctx.getCustomSessionName());
                        return null;
                    }).when(mock).execute();
                    Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(null);
                })) {
            processor.processOnce();
            Assertions.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
            // verify customSessionName is set during query execution
            Assertions.assertEquals("session_name", customSessionName.get());
            // customSessionName is NOT cleared after query finished
            Assertions.assertEquals("session_name", ctx.getCustomSessionName());
            Assertions.assertEquals("session_name", ctx.getSessionVariable().getCustomSessionName());
        }
    }

    @Test
    public void testFieldList() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(fieldListPacket), GlobalStateMgr.getCurrentState());

        myContext.setDatabase("testDb1");
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlEofPacket);
    }

    @Test
    public void testFieldListFailEmptyTable() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(4);
        serializer.writeNulTerminateString("");
        serializer.writeEofString("");

        ConnectContext ctx = initMockContext(mockChannel(serializer.toByteBuffer()),
                GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assertions.assertEquals("Empty tableName", myContext.getState().getErrorMessage());
    }

    @Test
    public void testFieldListFailNoDb() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(4);
        serializer.writeNulTerminateString("testTable");
        serializer.writeEofString("");

        myContext.setDatabase("testCluster:emptyDb");
        ConnectContext ctx = initMockContext(mockChannel(serializer.toByteBuffer()),
                AccessTestUtil.fetchAdminCatalog());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assertions.assertEquals("Unknown database(testCluster:emptyDb)", myContext.getState().getErrorMessage());
    }

    @Test
    public void testFieldListFailNoTable() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(4);
        serializer.writeNulTerminateString("emptyTable");
        serializer.writeEofString("");

        myContext.setDatabase("testDb1");
        ConnectContext ctx = initMockContext(mockChannel(serializer.toByteBuffer()),
                GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assertions.assertEquals("Unknown table(emptyTable)", myContext.getState().getErrorMessage());
    }

    @Test
    public void testUnsupportedCommand() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(5);
        ByteBuffer packet = serializer.toByteBuffer();
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_CREATE_DB, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assertions.assertFalse(myContext.isKilled());
    }

    @Test
    public void testUnknownCommand() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(101);
        ByteBuffer packet = serializer.toByteBuffer();
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assertions.assertEquals(MysqlCommand.COM_SLEEP, myContext.getCommand());
        Assertions.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assertions.assertFalse(myContext.isKilled());
    }

    @Test
    public void testNullPacket() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(null), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.loopForTest();
        Assertions.assertTrue(myContext.isKilled());
    }

    @Test
    public void testAddRunningQueryDetail() throws Exception {
        com.starrocks.common.Config.enable_collect_query_detail_info = true;
        ConnectContext ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.setQueryId(UUIDUtil.genUUID());
        ConnectProcessor processor = new ConnectProcessor(ctx);
        String sql = "CREATE ROUTINE LOAD example_db.example_tbl2_ordertest1 ON example_tbl2\n" +
                "COLUMNS TERMINATED BY \",\",\n" +
                "COLUMNS (order_id, pay_dt, customer_name, nationality, temp_gender, price)\n" +
                "FROM KAFKA\n" +
                "(\n" +
                "    \"kafka_broker_list\" =\"127.0.0.1:9000\",\n" +
                "    \"kafka_topic\" = \"ordertest1\"\n" +
                ");";
        StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        processor.executor = new StmtExecutor(ctx, statementBase);
        processor.executor.addRunningQueryDetail(statementBase);

        Assertions.assertFalse(Strings.isNullOrEmpty(QueryDetailQueue.getQueryDetailsAfterTime(0).get(0).getSql()));
    }

    @Test
    public void testProxyExecute() throws Exception {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setCatalog("default");
        request.setDb("testDb1");
        request.setUser("root");
        request.setSql("select 1");
        request.setIsInternalStmt(true);
        request.setModified_variables_sql("set query_timeout = 10");
        request.setCurrent_user_ident(new TUserIdentity().setUsername("root").setHost("127.0.0.1"));
        request.setQueryId(UUIDUtil.genTUniqueId());
        request.setSession_id(UUID.randomUUID().toString());
        request.setIsLastStmt(true);

        // mock context
        ConnectContext ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ctx.setCurrentCatalog("default");
        ctx.setDatabase("testDb1");
        ctx.setQualifiedUser("root");
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(UserIdentity.ROOT);
        ctx.setSessionId(java.util.UUID.randomUUID());
        ctx.setThreadLocalInfo();

        ConnectProcessor processor = new ConnectProcessor(ctx);
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (mock, mockCtx) -> {
                    Mockito.doNothing().when(mock).execute();
                    Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(null);
                })) {
            TMasterOpResult result = processor.proxyExecute(request, null);
            Assertions.assertNotNull(result);
        }
    }

    // Verify leader-side proxy execution preserves multi-stmt context so query detail records the current stmt SQL.
    // Verify leader-side proxy execution preserves multi-stmt context so query detail records the current stmt SQL.
    @Test
    public void testProxyExecuteMultiStmtUsesStmtScopedQueryDetail() throws Exception {
        boolean enableCollectQueryDetail = Config.enable_collect_query_detail_info;
        Config.enable_collect_query_detail_info = true;
        QueryDetailQueue.TOTAL_QUERIES.clear();
        try {
            TMasterOpRequest request = new TMasterOpRequest();
            request.setCatalog("default");
            request.setDb("testDb1");
            request.setUser("root");
            request.setSql("select 1; select 2");
            request.setStmtIdx(1);
            request.setIsInternalStmt(true);
            request.setCurrent_user_ident(new TUserIdentity().setUsername("root").setHost("127.0.0.1"));
            request.setQueryId(UUIDUtil.genTUniqueId());
            request.setSession_id(UUID.randomUUID().toString());
            request.setIsLastStmt(true);

            ConnectContext ctx = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
            ctx.setCurrentCatalog("default");
            ctx.setDatabase("testDb1");
            ctx.setQualifiedUser("root");
            ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            ctx.setCurrentUserIdentity(UserIdentity.ROOT);
            ctx.setCurrentRoleIds(UserIdentity.ROOT);
            ctx.setSessionId(UUID.randomUUID());
            ctx.setThreadLocalInfo();

            ConnectProcessor processor = new ConnectProcessor(ctx);
            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() {
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            TMasterOpResult result = processor.proxyExecute(request, null);
            Assertions.assertNotNull(result);
            Assertions.assertTrue(ctx.isMultiStmt());

            List<QueryDetail> details = QueryDetailQueue.getQueryDetailsAfterTime(0);
            Assertions.assertFalse(details.isEmpty());
            Assertions.assertFalse(details.get(0).getSql().contains(";"));
        } finally {
            Config.enable_collect_query_detail_info = enableCollectQueryDetail;
            QueryDetailQueue.TOTAL_QUERIES.clear();
        }
    }

    @Test
    public void testProxyExecuteUserIdentityIsNull() throws Exception {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setCatalog("default");
        request.setDb("testDb1");
        request.setUser("root");
        request.setSql("select 1");
        request.setIsInternalStmt(true);
        request.setModified_variables_sql("set query_timeout = 10");
        request.setQueryId(UUIDUtil.genTUniqueId());
        request.setSession_id(UUID.randomUUID().toString());
        request.setIsLastStmt(true);

        ConnectContext context = new ConnectContext();
        ConnectProcessor processor = new ConnectProcessor(context);
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                (mock, mockCtx) -> {
                    Mockito.doNothing().when(mock).execute();
                    Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(null);
                })) {
            TMasterOpResult result = processor.proxyExecute(request, null);
            Assertions.assertNotNull(result);
            Assertions.assertTrue(context.getState().isError());
        }
    }

    /**
     * Verify COM_STMT_EXECUTE query path still registers and initializes tracing correctly.
     */
    @Test
    public void testHandleExecuteTracingForQuery() throws Exception {
        // Create a prepared statement packet for COM_STMT_EXECUTE
        ByteBuffer executePacket = createExecutePacket(1, new ArrayList<>());
        ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

        // Create a prepared statement context with a query statement
        PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 + 2");
        PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
        ctx.putPreparedStmt("1", prepareCtx);

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Track method calls
        AtomicReference<Boolean> tracersRegistered = new AtomicReference<>(false);
        AtomicReference<Boolean> tracersInitialized = new AtomicReference<>(false);

        // Mock Tracers
        try (MockedStatic<Tracers> tracersMock = Mockito.mockStatic(Tracers.class, Mockito.CALLS_REAL_METHODS)) {
            tracersMock.when(() -> Tracers.register(Mockito.any(ConnectContext.class)))
                    .thenAnswer(invocation -> {
                        tracersRegistered.set(true);
                        return null;
                    });
            tracersMock.when(() -> Tracers.init(Mockito.any(ConnectContext.class), Mockito.any(), Mockito.any()))
                    .thenAnswer(invocation -> {
                        tracersInitialized.set(true);
                        return null;
                    });

            // Mock StmtExecutor
            try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class,
                    (mock, mockCtx) -> {
                        Mockito.doNothing().when(mock).execute();
                        Mockito.when(mock.getQueryStatisticsForAuditLog()).thenReturn(statistics);
                        Mockito.when(mock.getParsedStmt()).thenReturn(prepareStmt);
                        // mockConstruction skips real constructor; replicate originStmt setup
                        if (mockCtx.arguments().size() >= 2
                                && mockCtx.arguments().get(1) instanceof StatementBase) {
                            StatementBase stmt = (StatementBase) mockCtx.arguments().get(1);
                            Deencapsulation.setField(mock, "originStmt", stmt.getOrigStmt());
                        }
                        Mockito.when(mock.getOriginStmtInString()).thenCallRealMethod();
                    })) {
                processor.processOnce();

                // Verify that tracers are properly initialized for query statements
                Assertions.assertTrue(tracersRegistered.get(), "Tracers should be registered for query statements");
                Assertions.assertTrue(tracersInitialized.get(), "Tracers should be initialized for query statements");
                Assertions.assertEquals(MysqlCommand.COM_STMT_EXECUTE, myContext.getCommand());
                Assertions.assertEquals("SELECT 1 + 2 AS `1 + 2`", processor.executor.getOriginStmtInString());
            }
        }
    }

    // Verify COM_STMT_EXECUTE emits both BEFORE and AFTER audit records on success.
    @Test
    public void testHandleExecuteAuditBeforeAndAfter() throws Exception {
        boolean oldAuditStmtBeforeExecute = Config.audit_stmt_before_execute;
        Config.audit_stmt_before_execute = true;
        try {
            ByteBuffer executePacket = createExecutePacket(1, new ArrayList<>());
            ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

            PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 + 2");
            PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
            ctx.putPreparedStmt("1", prepareCtx);

            List<String> auditRecords = new ArrayList<>();
            ConnectProcessor processor = new ConnectProcessor(ctx) {
                @Override
                public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
                    auditRecords.add("BEFORE:" + origStmt);
                }

                @Override
                public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                           String digestFromLeader) {
                    auditRecords.add("AFTER:" + ctx.getState().toString() + ":" + origStmt);
                }
            };

            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() throws Exception {
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();
            Assertions.assertEquals(2, auditRecords.size());
            Assertions.assertTrue(auditRecords.get(0).startsWith("BEFORE:SELECT 1 + 2"));
            Assertions.assertTrue(auditRecords.get(1).startsWith("AFTER:OK:SELECT 1 + 2"));
        } finally {
            Config.audit_stmt_before_execute = oldAuditStmtBeforeExecute;
        }
    }

    // Verify COM_STMT_EXECUTE still emits AFTER audit when execution fails after BEFORE audit.
    @Test
    public void testHandleExecuteAuditBeforeAndAfterOnFailure() throws Exception {
        boolean oldAuditStmtBeforeExecute = Config.audit_stmt_before_execute;
        Config.audit_stmt_before_execute = true;
        try {
            ByteBuffer executePacket = createExecutePacket(1, new ArrayList<>());
            ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());

            PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 + 2");
            PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
            ctx.putPreparedStmt("1", prepareCtx);

            List<String> auditRecords = new ArrayList<>();
            ConnectProcessor processor = new ConnectProcessor(ctx) {
                @Override
                public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
                    auditRecords.add("BEFORE:" + origStmt);
                }

                @Override
                public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics,
                                           String digestFromLeader) {
                    auditRecords.add("AFTER:" + ctx.getState().toString() + ":" + origStmt);
                }
            };

            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() throws Exception {
                    throw new IOException("mock execute failure");
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();
            Assertions.assertEquals(2, auditRecords.size());
            Assertions.assertTrue(auditRecords.get(0).startsWith("BEFORE:SELECT 1 + 2"));
            Assertions.assertTrue(auditRecords.get(1).startsWith("AFTER:ERR:SELECT 1 + 2"));
        } finally {
            Config.audit_stmt_before_execute = oldAuditStmtBeforeExecute;
        }
    }

    // Verify COM_STMT_EXECUTE before-audit starts from a reset builder instead of reusing stale base fields.
    @Test
    public void testHandleExecuteAuditBeforeUsesFreshBuilderBaseFields() throws Exception {
        boolean oldAuditStmtBeforeExecute = Config.audit_stmt_before_execute;
        Config.audit_stmt_before_execute = true;
        try {
            ByteBuffer executePacket = createExecutePacket(1, new ArrayList<>());
            ConnectContext ctx = initMockContext(mockChannel(executePacket), GlobalStateMgr.getCurrentState());
            myContext.setDatabase("testDb1");

            PrepareStmt prepareStmt = createMockPrepareStmt("SELECT 1 + 2");
            PrepareStmtContext prepareCtx = new PrepareStmtContext(prepareStmt, ctx, null);
            ctx.putPreparedStmt("1", prepareCtx);

            ctx.getAuditEventBuilder().reset();
            ctx.getAuditEventBuilder().setUser("stale-user").setDb("stale-db");

            AtomicReference<String> beforeAuditUser = new AtomicReference<>();
            AtomicReference<String> beforeAuditDb = new AtomicReference<>();
            ConnectProcessor processor = new ConnectProcessor(ctx) {
                @Override
                public void auditBeforeExec(String origStmt, StatementBase parsedStmt) {
                    AuditEvent snapshot = ctx.getAuditEventBuilder().buildSnapshot();
                    beforeAuditUser.set(snapshot.user);
                    beforeAuditDb.set(snapshot.db);
                    super.auditBeforeExec(origStmt, parsedStmt);
                }
            };

            new MockUp<StmtExecutor>() {
                @Mock
                public void execute() {
                }

                @Mock
                public PQueryStatistics getQueryStatisticsForAuditLog() {
                    return null;
                }
            };

            processor.processOnce();
            Assertions.assertEquals("testCluster:user", beforeAuditUser.get());
            Assertions.assertEquals("testDb1", beforeAuditDb.get());
        } finally {
            Config.audit_stmt_before_execute = oldAuditStmtBeforeExecute;
        }
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

    private ByteBuffer createQueryPacket(String sql) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(3);
        serializer.writeEofString(sql);
        return serializer.toByteBuffer();
    }

    private ByteBuffer createPreparePacket(String sql) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(MysqlCommand.COM_STMT_PREPARE.getCommandCode());
        serializer.writeEofString(sql);
        return serializer.toByteBuffer();
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
}
