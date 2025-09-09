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

import com.starrocks.common.Config;
import com.starrocks.common.Status;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.MultipleWarehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

    @BeforeEach
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
        Assertions.assertNotNull(ctx.getState());

        // Capability
        Assertions.assertEquals(MysqlCapability.DEFAULT_CAPABILITY, ctx.getServerCapability());
        ctx.setCapability(new MysqlCapability(10));
        Assertions.assertEquals(new MysqlCapability(10), ctx.getCapability());

        // Kill flag
        Assertions.assertFalse(ctx.isKilled());
        ctx.setKilled();
        Assertions.assertTrue(ctx.isKilled());

        // Current db
        Assertions.assertEquals("", ctx.getDatabase());
        ctx.setDatabase("testCluster:testDb");
        Assertions.assertEquals("testCluster:testDb", ctx.getDatabase());

        // User
        ctx.setQualifiedUser("testCluster:testUser");
        Assertions.assertEquals("testCluster:testUser", ctx.getQualifiedUser());

        // Serializer
        Assertions.assertNotNull(ctx.getSerializer());

        // Session variable
        Assertions.assertNotNull(ctx.getSessionVariable());

        // connection id
        ctx.setConnectionId(101);
        Assertions.assertEquals(101, ctx.getConnectionId());

        // set connect start time to now
        ctx.resetConnectionStartTime();

        // command
        ctx.setCommand(MysqlCommand.COM_PING);
        Assertions.assertEquals(MysqlCommand.COM_PING, ctx.getCommand());

        // Thread info
        Assertions.assertNotNull(ctx.toThreadInfo());
        long currentTimeMillis = System.currentTimeMillis();
        List<String> row = ctx.toThreadInfo().toRow(currentTimeMillis, false);
        Assertions.assertEquals(12, row.size());
        Assertions.assertEquals("101", row.get(0));
        Assertions.assertEquals("testUser", row.get(1));
        Assertions.assertEquals("127.0.0.1:12345", row.get(2));
        Assertions.assertEquals("testDb", row.get(3));
        Assertions.assertEquals("Ping", row.get(4));
        Assertions.assertEquals(TimeUtils.longToTimeString(ctx.getConnectionStartTime()), row.get(5));
        Assertions.assertEquals(Long.toString((currentTimeMillis - ctx.getConnectionStartTime()) / 1000), row.get(6));
        Assertions.assertEquals("OK", row.get(7));
        Assertions.assertEquals("", row.get(8));
        Assertions.assertEquals("false", row.get(9));
        Assertions.assertEquals("default_warehouse", row.get(10));
        Assertions.assertEquals("", row.get(11));

        // Start time
        ctx.setStartTime();
        Assertions.assertNotSame(0, ctx.getStartTime());

        // query id
        ctx.setExecutionId(new TUniqueId(100, 200));
        Assertions.assertEquals(new TUniqueId(100, 200), ctx.getExecutionId());

        // GlobalStateMgr
        Assertions.assertNotNull(ctx.getGlobalStateMgr());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testSleepTimeout() {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        // sleep no time out
        ctx.setStartTime();
        Assertions.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assertions.assertFalse(ctx.isKilled());

        // Timeout
        ctx.setStartTime();
        now = ctx.getStartTime() + ctx.getSessionVariable().getWaitTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assertions.assertTrue(ctx.isKilled());

        // Kill
        ctx.kill(true, "sleep time out");
        Assertions.assertTrue(ctx.isKilled());
        ctx.kill(false, "sleep time out");
        Assertions.assertTrue(ctx.isKilled());

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
        Assertions.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assertions.assertFalse(ctx.isKilled());

        // Timeout
        now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assertions.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true, "query timeout");
        Assertions.assertTrue(ctx.isKilled());

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
        Assertions.assertFalse(ctx.isKilled());
        long now = ctx.getStartTime() + ctx.getSessionVariable().getInsertTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assertions.assertFalse(ctx.isKilled());

        // Timeout
        now = ctx.getStartTime() + ctx.getSessionVariable().getInsertTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assertions.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true, "insert timeout");
        Assertions.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testThreadLocal() {
        ConnectContext ctx = new ConnectContext(connection);
        Assertions.assertNull(ConnectContext.get());
        ctx.setThreadLocalInfo();
        Assertions.assertNotNull(ConnectContext.get());
        Assertions.assertEquals(ctx, ConnectContext.get());
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
        Assertions.assertEquals("wh1", ctx.getCurrentWarehouseName());

        ctx.setCurrentWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, ctx.getCurrentWarehouseId());
    }
    @Test
    public void testWarehouseWithRollback(@Mocked WarehouseManager warehouseManager) {
        new Expectations() {
            {
                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getWarehouse(anyLong);
                minTimes = 0;
                result = new MultipleWarehouse(10, "wh2", 10);

                warehouseManager.getWarehouseAllowNull(anyString);
                minTimes = 0;
                result = null;
            }
        };

        Config.enable_rollback_default_warehouse = true;
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setCurrentWarehouse("wh2");
        Assertions.assertEquals("wh2", ctx.getCurrentWarehouseName());

        ctx.setCurrentWarehouseId(10);
        Assertions.assertEquals(10, ctx.getCurrentWarehouseId());
    }

    @Test
    public void testGetNormalizedErrorCode() {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setState(new QueryState());
        Status status = new Status(new TStatus(TStatusCode.MEM_LIMIT_EXCEEDED));

        {
            ctx.setErrorCodeOnce(status.getErrorCodeString());
            ctx.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            Assertions.assertEquals("MEM_LIMIT_EXCEEDED", ctx.getNormalizedErrorCode());
        }

        {
            ctx.resetErrorCode();
            Assertions.assertEquals("ANALYSIS_ERR", ctx.getNormalizedErrorCode());
        }
    }

    @Test
    public void testIsIdleLastFor() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setCommand(MysqlCommand.COM_SLEEP);
        context.setEndTime();

        Thread.sleep(100);

        Assertions.assertTrue(context.isIdleLastFor(99));

        context.setCommand(MysqlCommand.COM_QUERY);
        context.setStartTime();
        Assertions.assertFalse(context.isIdleLastFor(99));
    }

    @Test
    public void testQueryTimeoutWithPendingTime() {
        ConnectContext ctx = new ConnectContext(connection);
        ctx.setCommand(MysqlCommand.COM_QUERY);
        ctx.setThreadLocalInfo();

        StmtExecutor executor = new StmtExecutor(ctx, new QueryStatement(ValuesRelation.newDualRelation()));
        ctx.setExecutor(executor);

        // query no time out
        Assertions.assertFalse(ctx.isKilled());

        long now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 - 1;
        Assertions.assertFalse(ctx.checkTimeout(now));
        Assertions.assertFalse(ctx.isKilled());

        // Timeout without pending time
        now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 + 1;
        Assertions.assertTrue(ctx.checkTimeout(now));
        Assertions.assertFalse(ctx.isKilled());

        // Timeout with pending time
        int pendingTimeS = 300;
        ctx.setPendingTimeSecond(pendingTimeS);
        Assertions.assertTrue(ctx.getExecTimeout() == ctx.getSessionVariable().getQueryTimeoutS() + pendingTimeS);

        now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 + 1;
        Assertions.assertFalse(ctx.checkTimeout(now));
        now = ctx.getStartTime() + ctx.getSessionVariable().getQueryTimeoutS() * 1000 + pendingTimeS * 1000 + 1;
        Assertions.assertTrue(ctx.checkTimeout(now));

        // Kill
        ctx.kill(true, "query timeout");
        Assertions.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void getCurrentComputeResourceName_returnsEmptyStringWhenNotSharedDataMode() {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return false;
            }
        };
        ConnectContext ctx = new ConnectContext();
        Assertions.assertEquals("", ctx.getCurrentComputeResourceName());
    }

    @Test
    public void getCurrentComputeResourceName_returnsEmptyStringWhenComputeResourceIsNull() {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentComputeResource(null);
        Assertions.assertEquals("", ctx.getCurrentComputeResourceName());
    }

    @Test
    public void getCurrentComputeResourceName_returnsResourceNameWhenComputeResourceIsSet(
            @Mocked WarehouseManager warehouseManager) {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;

                warehouseManager.getComputeResourceName((ComputeResource) any);
                minTimes = 0;
                result = "testResource";
            }
        };
        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setCurrentComputeResource(WarehouseComputeResource.of(0L));
        Assertions.assertEquals("testResource", ctx.getCurrentComputeResourceName());
    }

    @Test
    public void getCurrentComputeResourceNoAcquire_returnsDefaultResourceWhenNotSharedDataMode() {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return false;
            }
        };
        ConnectContext ctx = new ConnectContext();
        Assertions.assertEquals(WarehouseManager.DEFAULT_RESOURCE, ctx.getCurrentComputeResourceNoAcquire());
    }

    @Test
    public void getCurrentComputeResourceNoAcquire_returnsNullWhenComputeResourceIsNotSet() {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentComputeResource(null);
        Assertions.assertNull(ctx.getCurrentComputeResourceNoAcquire());
    }

    @Test
    public void getCurrentComputeResourceNoAcquire_returnsComputeResourceWhenSet() {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };
        ConnectContext ctx = new ConnectContext();
        ComputeResource resource = WarehouseComputeResource.of(1L);
        ctx.setCurrentComputeResource(resource);
        Assertions.assertEquals(resource, ctx.getCurrentComputeResourceNoAcquire());
    }

    @Test
    public void testConnectContextNoGlobalStateMgrNPE() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            connectContext = new ConnectContext();
            // not set globalStateMgr
            connectContext.setThreadLocalInfo();
        }
        // ConnectContext.get() should have non-nullable globalStateMgr even if forget to manually create the context
        // without setting globalStateMgr explicitly
        Assertions.assertNotNull(ConnectContext.get().getGlobalStateMgr());

        connectContext = ConnectContext.get();
        // set globalStateMgr explicitly
        connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        Assertions.assertNotNull(ConnectContext.get().getGlobalStateMgr());
    }
}
