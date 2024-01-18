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
import com.starrocks.authz.authentication.AuthenticationMgr;
import com.starrocks.authz.authorization.PrivilegeBuiltinConstants;
import com.starrocks.common.conf.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlErrPacket;
import com.starrocks.mysql.MysqlOkPacket;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.plugin.AuditEvent.AuditEventBuilder;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

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

    @Mocked
    private static SocketChannel socketChannel;

    private static PQueryStatistics statistics = new PQueryStatistics();

    @BeforeClass
    public static void setUpClass() {
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
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        initDbPacket.clear();
        initWarehousePacket.clear();
        pingPacket.clear();
        quitPacket.clear();
        queryPacket.clear();
        fieldListPacket.clear();
        changeUserPacket.clear();
        resetConnectionPacket.clear();
        // Mock
        MysqlChannel channel = new MysqlChannel(socketChannel);
        new Expectations(channel) {
            {
                channel.getRemoteHostPortString();
                minTimes = 0;
                result = "127.0.0.1:12345";
            }
        };
        myContext = new ConnectContext(socketChannel);
        Deencapsulation.setField(myContext, "mysqlChannel", channel);
    }

    private static MysqlChannel mockChannel(ByteBuffer packet) {
        try {
            MysqlChannel channel = new MysqlChannel(socketChannel);
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
        ConnectContext context = new ConnectContext(socketChannel) {
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
        };

        new Expectations(context) {
            {
                context.getMysqlChannel();
                minTimes = 0;
                result = channel;

                context.isKilled();
                minTimes = 0;
                maxTimes = 3;
                returns(false, true, false);

                context.getGlobalStateMgr();
                minTimes = 0;
                result = globalStateMgr;

                context.getAuditEventBuilder();
                minTimes = 0;
                result = auditBuilder;

                context.getQualifiedUser();
                minTimes = 0;
                result = "testCluster:user";

                context.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.ROOT;

                context.getStartTime();
                minTimes = 0;
                result = 0L;

                context.getReturnRows();
                minTimes = 0;
                result = 1L;

                context.setStmtId(anyLong);
                minTimes = 0;

                context.getStmtId();
                minTimes = 0;
                result = 1L;

                context.getExecutionId();
                minTimes = 0;
                result = new TUniqueId();

                context.getCapability();
                minTimes = 0;
                result = MysqlCapability.DEFAULT_CAPABILITY;
            }
        };

        return context;
    }

    @Test
    public void testQuit() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(quitPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUIT, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertTrue(myContext.isKilled());
    }

    @Test
    public void testInitDb() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initDbPacket), GlobalStateMgr.getCurrentState());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
    }

    @Test
    public void testInitDbFail() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initDbPacket), GlobalStateMgr.getCurrentState());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assert.assertFalse(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
    }

    @Test
    public void testInitWarehouse() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(initWarehousePacket), GlobalStateMgr.getCurrentState());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_INIT_DB, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
    }

    @Test
    public void testChangeUser() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(changeUserPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_CHANGE_USER, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testResetConnection() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(resetConnectionPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_RESET_CONNECTION, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testPing() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(pingPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_PING, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testPingLoop() throws IOException {
        ConnectContext ctx = initMockContext(mockChannel(pingPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.loop();
        Assert.assertEquals(MysqlCommand.COM_PING, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlOkPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testQuery(@Mocked StmtExecutor executor) throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        new Expectations() {
            {
                executor.getQueryStatisticsForAuditLog();
                minTimes = 0;
                result = statistics;
            }
        };

        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
    }

    @Test
    public void testQueryFail(@Mocked StmtExecutor executor) throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        new Expectations() {
            {
                executor.execute();
                minTimes = 0;
                result = new IOException("Fail");

                executor.getQueryStatisticsForAuditLog();
                minTimes = 0;
                result = statistics;
            }
        };
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
    }

    @Test
    public void testQueryFail2(@Mocked StmtExecutor executor) throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(queryPacket), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);

        // Mock statement executor
        new Expectations() {
            {
                executor.execute();
                minTimes = 0;
                result = new NullPointerException("Fail");

                executor.getQueryStatisticsForAuditLog();
                minTimes = 0;
                result = statistics;
            }
        };
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_QUERY, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
    }

    @Test
    public void testFieldList() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(fieldListPacket), GlobalStateMgr.getCurrentState());

        myContext.setDatabase("testDb1");
        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlEofPacket);
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
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertEquals("Empty tableName", myContext.getState().getErrorMessage());
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
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertEquals("Unknown database(testCluster:emptyDb)", myContext.getState().getErrorMessage());
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
        Assert.assertEquals(MysqlCommand.COM_FIELD_LIST, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertEquals("Unknown table(emptyTable)", myContext.getState().getErrorMessage());
    }

    @Test
    public void testUnsupportedCommand() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(5);
        ByteBuffer packet = serializer.toByteBuffer();
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_CREATE_DB, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testUnknownCommand() throws Exception {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        serializer.writeInt1(101);
        ByteBuffer packet = serializer.toByteBuffer();
        ConnectContext ctx = initMockContext(mockChannel(packet), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.processOnce();
        Assert.assertEquals(MysqlCommand.COM_SLEEP, myContext.getCommand());
        Assert.assertTrue(myContext.getState().toResponsePacket() instanceof MysqlErrPacket);
        Assert.assertFalse(myContext.isKilled());
    }

    @Test
    public void testNullPacket() throws Exception {
        ConnectContext ctx = initMockContext(mockChannel(null), GlobalStateMgr.getCurrentState());

        ConnectProcessor processor = new ConnectProcessor(ctx);
        processor.loop();
        Assert.assertTrue(myContext.isKilled());
    }

    @Test
    public void testAddRunningQueryDetail() throws Exception {
        Config.enable_collect_query_detail_info = true;
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

        processor.addRunningQueryDetail(statementBase);

        Assert.assertFalse(Strings.isNullOrEmpty(QueryDetailQueue.getQueryDetailsAfterTime(0).get(0).getSql()));
    }
}
