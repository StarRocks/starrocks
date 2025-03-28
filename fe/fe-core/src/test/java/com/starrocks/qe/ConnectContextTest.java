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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/ConnectContextTest.java

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

import com.starrocks.analysis.TableName;
import com.starrocks.common.Status;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.DefaultWarehouse;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xnio.StreamConnection;

import java.util.List;

public class ConnectContextTest {
    @Mocked
    private MysqlChannel channel;
    @Mocked
    private StreamConnection connection;
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private ConnectScheduler connectScheduler;

    private VariableMgr variableMgr = new VariableMgr();

    @Before
    public void setUp() throws Exception {
        new Expectations() {
            {
                channel.getRemoteHostPortString();
                minTimes = 0;
                result = "127.0.0.1:12345";

                channel.close();
                minTimes = 0;

                channel.getRemoteIp();
                minTimes = 0;
                result = "192.168.1.1";

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };
    }

    @Test
    public void testNormal() {
        ConnectContext ctx = new ConnectContext(connection);

        // State
        Assert.assertNotNull(ctx.getState());

        // Capability
        Assert.assertEquals(MysqlCapability.DEFAULT_CAPABILITY, ctx.getServerCapability());
        ctx.setCapability(new MysqlCapability(10));
        Assert.assertEquals(new MysqlCapability(10), ctx.getCapability());

        // Kill flag
        Assert.assertFalse(ctx.isKilled());
        ctx.setKilled();
        Assert.assertTrue(ctx.isKilled());

        // Current db
        Assert.assertEquals("", ctx.getDatabase());
        ctx.setDatabase("testCluster:testDb");
        Assert.assertEquals("testCluster:testDb", ctx.getDatabase());

        // User
        ctx.setQualifiedUser("testCluster:testUser");
        Assert.assertEquals("testCluster:testUser", ctx.getQualifiedUser());

        // Serializer
        Assert.assertNotNull(ctx.getSerializer());

        // Session variable
        Assert.assertNotNull(ctx.getSessionVariable());

        // connect scheduler
        Assert.assertNull(ctx.getConnectScheduler());
        ctx.setConnectScheduler(connectScheduler);
        Assert.assertNotNull(ctx.getConnectScheduler());

        // connection id
        ctx.setConnectionId(101);
        Assert.assertEquals(101, ctx.getConnectionId());

        // set connect start time to now
        ctx.resetConnectionStartTime();

        // command
        ctx.setCommand(MysqlCommand.COM_PING);
        Assert.assertEquals(MysqlCommand.COM_PING, ctx.getCommand());

        // Thread info
        Assert.assertNotNull(ctx.toThreadInfo());
        long currentTimeMillis = System.currentTimeMillis();
        List<String> row = ctx.toThreadInfo().toRow(currentTimeMillis, false);
        Assert.assertEquals(10, row.size());
        Assert.assertEquals("101", row.get(0));
        Assert.assertEquals("testUser", row.get(1));
        Assert.assertEquals("127.0.0.1:12345", row.get(2));
        Assert.assertEquals("testDb", row.get(3));
        Assert.assertEquals("Ping", row.get(4));
        Assert.assertEquals(TimeUtils.longToTimeString(ctx.getConnectionStartTime()), row.get(5));
        Assert.assertEquals(Long.toString((currentTimeMillis - ctx.getConnectionStartTime()) / 1000), row.get(6));
        Assert.assertEquals("OK", row.get(7));
        Assert.assertEquals("", row.get(8));
        Assert.assertEquals("false", row.get(9));

        // Start time
        ctx.setStartTime();
        Assert.assertNotSame(0, ctx.getStartTime());

        // query id
        ctx.setExecutionId(new TUniqueId(100, 200));
        Assert.assertEquals(new TUniqueId(100, 200), ctx.getExecutionId());

        // GlobalStateMgr
        Assert.assertNull(ctx.getGlobalStateMgr());
        ctx.setGlobalStateMgr(globalStateMgr);
        Assert.assertNotNull(ctx.getGlobalStateMgr());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testSleepTimeout() {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        // sleep no time out
        ctx.setStartTime();
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assert.assertTrue(ctx.isKilled());

        // Kill
        ctx.kill(true, "sleep time out");
        Assert.assertTrue(ctx.isKilled());
        ctx.kill(false, "sleep time out");
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testQueryTimeout() {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setThreadLocalInfo();

        StmtExecutor executor = new StmtExecutor(ctx, new QueryStatement(ValuesRelation.newDualRelation()));
        ctx.setExecutor(executor);

        // query no time out
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true, "query timeout");
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testInsertTimeout() {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setThreadLocalInfo();

        StmtExecutor executor = new StmtExecutor(
                ctx, new InsertStmt(new TableName("db", "tbl"), new QueryStatement(ValuesRelation.newDualRelation())));
        ctx.setExecutor(executor);

        // insert no time out
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getInsertTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        now = ctx.getStartTime() + ctx.getSessionVariable().getInsertTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true, "insert timeout");
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testThreadLocal() {
        ConnectContext ctx = new ConnectContext(connection);
        Assert.assertNull(ConnectContext.get());
        ctx.setThreadLocalInfo();
        Assert.assertNotNull(ConnectContext.get());
        Assert.assertEquals(ctx, ConnectContext.get());
    }

    @Test
    public void testWarehouse(@Mocked WarehouseManager warehouseManager) {
        new Expectations() {
            {
                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouse(anyLong);
                minTimes = 0;
                result = new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setCurrentWarehouse("wh1");
        Assert.assertEquals("wh1", ctx.getCurrentWarehouseName());

        ctx.setCurrentWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, ctx.getCurrentWarehouseId());
    }

    @Test
    public void testGetNormalizedErrorCode() {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setState(new QueryState());
        Status status = new Status(new TStatus(TStatusCode.MEM_LIMIT_EXCEEDED));

        {
            ctx.setErrorCodeOnce(status.getErrorCodeString());
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            Assert.assertEquals("MEM_LIMIT_EXCEEDED", ctx.getNormalizedErrorCode());
        }

        {
            ctx.resetErrorCode();
            Assert.assertEquals("ANALYSIS_ERR", ctx.getNormalizedErrorCode());
        }
    }

    @Test
    public void testIsIdleLastFor() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setCommand(MysqlCommand.COM_SLEEP);
        context.setEndTime();

        Thread.sleep(100);

        Assert.assertTrue(context.isIdleLastFor(99));

        context.setCommand(MysqlCommand.COM_QUERY);
        context.setStartTime();
        Assert.assertFalse(context.isIdleLastFor(99));
    }
}
