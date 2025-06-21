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

import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCodec;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link StmtExecutor#sendStmtPrepareOK(PrepareStmt)}.
 *
 * <p>This test focuses on the logic that converts {@link PrepareStmt#getName()} into the statement_id
 * returned to the client. 3 scenarios are covered:
 * 1. name is a numeric string within 4-byte range.
 * 2. name is a numeric string beyond 4-byte range.
 * 3. name is not a numeric string.</p>
 */
public class StmtExecutorPrepareStmtIdTest {

    // Minimal dummy MysqlChannel to capture packets without touching real network.
    private static class DummyMysqlChannel extends MysqlChannel {
        private final List<ByteBuffer> packets = new ArrayList<>();
        DummyMysqlChannel() {
            super(null);
        }
        @Override
        public void sendOnePacket(ByteBuffer packet) throws IOException {
            // Keep an independent copy for inspection
            ByteBuffer dup = ByteBuffer.allocate(packet.remaining());
            dup.put(packet);
            dup.flip();
            packets.add(dup);
        }
        @Override
        public void flush() {
            // no-op
        }
        List<ByteBuffer> getPackets() {
            return packets;
        }
    }

    @BeforeClass
    public static void beforeClass() {
        UtFrameUtils.createMinStarRocksCluster();
    }

    private static int executePrepareAndGetStmtId(String stmtName) throws Exception {
        // Build context and inject dummy channel
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        DummyMysqlChannel channel = new DummyMysqlChannel();
        Field channelField = ConnectContext.class.getDeclaredField("mysqlChannel");
        channelField.setAccessible(true);
        channelField.set(ctx, channel);
        // Mark this request as coming from binary protocol so subsequent logic (if any) knows.
        ctx.setCommand(MysqlCommand.COM_STMT_PREPARE);

        // Build a simple query statement: SELECT 1
        QueryStatement query = (QueryStatement) UtFrameUtils.parseStmtWithNewParser("select 1", ctx);
        PrepareStmt prepareStmt = new PrepareStmt(stmtName, query, null);

        // Create executor and invoke private method via reflection
        StmtExecutor executor = new StmtExecutor(ctx, prepareStmt);
        Method m = StmtExecutor.class.getDeclaredMethod("sendStmtPrepareOK", PrepareStmt.class);
        m.setAccessible(true);
        m.invoke(executor, prepareStmt);

        // First packet is OK packet containing statement_id at bytes 1-4 (little-endian)
        ByteBuffer first = channel.getPackets().get(0).duplicate();
        // Read OK header byte
        MysqlCodec.readByte(first);
        return MysqlCodec.readInt4(first);
    }

    @Test
    public void testNumericWithinRange() throws Exception {
        int id = executePrepareAndGetStmtId("123");
        Assert.assertEquals(123, id);
    }

    @Test
    public void testNumericOutOfRange() throws Exception {
        String largeNum = "5000000000"; // > 0xFFFFFFFF
        int expected = largeNum.hashCode() & 0x7FFFFFFF;
        int id = executePrepareAndGetStmtId(largeNum);
        Assert.assertEquals(expected, id);
    }

    @Test
    public void testNonNumericName() throws Exception {
        String name = "notNumber";
        int expected = name.hashCode() & 0x7FFFFFFF;
        int id = executePrepareAndGetStmtId(name);
        Assert.assertEquals(expected, id);
    }
} 