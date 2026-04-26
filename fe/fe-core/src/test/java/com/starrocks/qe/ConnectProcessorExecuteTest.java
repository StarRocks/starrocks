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

import com.starrocks.authentication.AccessControlContext;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.plugin.AuditEvent.AuditEventBuilder;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.xnio.StreamConnection;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConnectProcessorExecuteTest extends DDLTestBase {
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
        connection = mock(StreamConnection.class);
        java.net.InetSocketAddress mockAddr = new java.net.InetSocketAddress("127.0.0.1", 12345);
        when(connection.getPeerAddress()).thenReturn(mockAddr);
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
        MysqlChannel channel = mock(MysqlChannel.class);
        when(channel.getRemoteHostPortString()).thenReturn("127.0.0.1:12345");
        myContext = new ConnectContext(connection);
        Deencapsulation.setField(myContext, "mysqlChannel", channel);
        super.setUp();
    }

    private static MysqlChannel mockChannel(ByteBuffer packet) {
        try {
            MysqlChannel channel = mock(MysqlChannel.class);
            when(channel.fetchOnePacket()).thenReturn(packet);
            when(channel.getRemoteHostPortString()).thenReturn("127.0.0.1:12345");
            return channel;
        } catch (Exception e) {
            return null;
        }
    }

    private static ConnectContext initMockContext(MysqlChannel channel, GlobalStateMgr globalStateMgr) {
        ConnectContext context = spy(new ConnectContext(connection) {
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

        doReturn(channel).when(context).getMysqlChannel();
        doReturn(false).doReturn(true).doReturn(false).when(context).isKilled();
        doReturn(globalStateMgr).when(context).getGlobalStateMgr();
        doReturn(auditBuilder).when(context).getAuditEventBuilder();
        doReturn("testCluster:user").when(context).getQualifiedUser();
        doReturn(UserIdentity.ROOT).when(context).getCurrentUserIdentity();
        doReturn(0L).when(context).getStartTime();
        doReturn(1L).when(context).getReturnRows();
        doNothing().when(context).setStmtId(anyLong());
        doReturn(1L).when(context).getStmtId();
        doReturn(new TUniqueId()).when(context).getExecutionId();
        doReturn(MysqlCapability.DEFAULT_CAPABILITY).when(context).getCapability();
        doReturn(new AccessControlContext()).when(context).getAccessControlContext();
        doReturn(new PlainPasswordAuthenticationProvider(MysqlPassword.EMPTY_PASSWORD))
                .when(context).getAuthenticationProvider();

        return context;
    }

    @Test
    public void testStmtExecute() throws Exception {
        int stmtId = 1;
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(MysqlCommand.COM_STMT_EXECUTE.getCommandCode());
        serializer.writeInt4(stmtId);
        serializer.writeInt1(0); // flags
        serializer.writeInt4(0); // flags
        ByteBuffer packet = serializer.toByteBuffer();

        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());

        String sql = "PREPARE stmt1 FROM select * from testDb1.testTable1";
        PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ctx.putPreparedStmt(String.valueOf(stmtId), new PrepareStmtContext(stmt, ctx, null));

        ConnectProcessor processor = new ConnectProcessor(ctx);
        // Mock statement executor
        try (MockedConstruction<StmtExecutor> mocked = Mockito.mockConstruction(StmtExecutor.class,
                (mock, context) -> {
                    when(mock.getQueryStatisticsForAuditLog()).thenReturn(statistics);
                })) {

            processor.processOnce();

            Assertions.assertEquals(MysqlCommand.COM_STMT_EXECUTE, myContext.getCommand());
            Assertions.assertTrue(ctx.getState().isQuery());
        }
    }
}
