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

<<<<<<< HEAD
=======
import com.starrocks.analysis.TableName;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
>>>>>>> cff8cb61f0 ([BugFix] Wait for journal replay in changeCatalogDb on follower FE (backport #69834) (#69901))
import com.starrocks.common.Status;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
import com.starrocks.server.WarehouseManager;
=======
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ValuesRelation;
>>>>>>> cff8cb61f0 ([BugFix] Wait for journal replay in changeCatalogDb on follower FE (backport #69834) (#69901))
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.DefaultWarehouse;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.channels.SocketChannel;
import java.util.List;

public class ConnectContextTest {
    @Mocked
    private MysqlChannel channel;
    @Mocked
    private StmtExecutor executor;
    @Mocked
    private SocketChannel socketChannel;
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

                executor.cancel("set up");
                minTimes = 0;

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;
            }
        };
    }

    @Test
    public void testNormal() {
        ConnectContext ctx = new ConnectContext(socketChannel);

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
        Assert.assertNotNull(ctx.getGlobalStateMgr());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testSleepTimeout() {
        ConnectContext ctx = new ConnectContext(socketChannel);
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
        ctx.setExecutor(executor);
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
    public void testOtherTimeout() {
        ConnectContext ctx = new ConnectContext(socketChannel);
        ctx.setCommand(MysqlCommand.COM_QUERY);

        // sleep no time out
        Assert.assertFalse(ctx.isKilled());
        long now = ctx.getSessionVariable().getQueryTimeoutS() * 1000 - 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Timeout
        now = ctx.getSessionVariable().getQueryTimeoutS() * 1000 + 1;
        ctx.checkTimeout(now);
        Assert.assertFalse(ctx.isKilled());

        // Kill
        ctx.kill(true, "query timeout");
        Assert.assertTrue(ctx.isKilled());

        // clean up
        ctx.cleanup();
    }

    @Test
    public void testThreadLocal() {
        ConnectContext ctx = new ConnectContext(socketChannel);
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

        ConnectContext ctx = new ConnectContext(socketChannel);
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setCurrentWarehouse("wh1");
        Assert.assertEquals("wh1", ctx.getCurrentWarehouseName());

        ctx.setCurrentWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID, ctx.getCurrentWarehouseId());
    }

    @Test
    public void testGetNormalizedErrorCode() {
        ConnectContext ctx = new ConnectContext(socketChannel);
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
    public void testConnectContextNoGlobalStateMgrNPE() {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            connectContext = new ConnectContext();
            // not set globalStateMgr
            connectContext.setThreadLocalInfo();
        }
        // ConnectContext.get() should have non-nullable globalStateMgr even if forget to manually create the context
        // without setting globalStateMgr explicitly
        Assert.assertNotNull(ConnectContext.get().getGlobalStateMgr());

        connectContext = ConnectContext.get();
        // set globalStateMgr explicitly
        connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        Assert.assertNotNull(ConnectContext.get().getGlobalStateMgr());
    }

    // -----------------------------------------------------------------------
    // Tests for changeCatalogDb() journal-replay wait on follower FE
    // -----------------------------------------------------------------------

    /**
     * On a follower FE, if the database is not found locally on the first check
     * (journal not yet replayed), the follower fetches the leader's max journal ID,
     * waits for local replay, and retries. If the database exists after the wait,
     * changeCatalogDb() should succeed.
     */
    @Test
    public void testChangeCatalogDb_followerWaitsForJournalAndSucceeds(
            @Mocked MetadataMgr metadataMgr,
            @Mocked JournalObservable journalObservable) throws Exception {
        Database mockDb = new Database(1L, "testdb");

        new MockUp<LeaderOpExecutor>() {
            @Mock
            public static long fetchLeaderMaxJournalId(ConnectContext ctx) {
                return 100L;
            }
        };

        new Expectations() {
            {
                globalStateMgr.isLeader();
                result = false;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                globalStateMgr.getJournalObservable();
                result = journalObservable;
                minTimes = 0;

                // First call (outer condition): null  → enter wait block
                // Second call (inner condition): mockDb → no error thrown
                metadataMgr.getDb((ConnectContext) any, anyString, anyString);
                returns(null, mockDb);

                // Simulate a successful journal replay (no-op)
                journalObservable.waitOn(100L, anyInt);
            }
        };

        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnOrInDb(ConnectContext ctx, String catalog, String db)
                    throws AccessDeniedException {
            }
        };

        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        Assertions.assertDoesNotThrow(() -> ctx.changeCatalogDb("testdb"));
        Assertions.assertEquals("testdb", ctx.getDatabase());
    }

    /**
     * On a follower FE, if the database is still missing after journal replay wait
     * (the database genuinely does not exist), changeCatalogDb() should throw ERR_BAD_DB_ERROR.
     */
    @Test
    public void testChangeCatalogDb_followerThrowsWhenDbStillMissingAfterWait(
            @Mocked MetadataMgr metadataMgr,
            @Mocked JournalObservable journalObservable) throws Exception {
        new MockUp<LeaderOpExecutor>() {
            @Mock
            public static long fetchLeaderMaxJournalId(ConnectContext ctx) {
                return 100L;
            }
        };

        new Expectations() {
            {
                globalStateMgr.isLeader();
                result = false;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                globalStateMgr.getJournalObservable();
                result = journalObservable;
                minTimes = 0;

                // Both calls return null: db genuinely does not exist
                metadataMgr.getDb((ConnectContext) any, anyString, anyString);
                result = null;

                journalObservable.waitOn(100L, anyInt);
            }
        };

        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        Assertions.assertThrows(DdlException.class, () -> ctx.changeCatalogDb("nonexistent"));
    }

    /**
     * On a follower FE, if fetchLeaderMaxJournalId returns -1 (leader unavailable),
     * the waitOn call is skipped entirely. The inner db check still runs and throws
     * ERR_BAD_DB_ERROR if the database is not found.
     */
    @Test
    public void testChangeCatalogDb_followerSkipsWaitWhenLeaderJournalIdNegative(
            @Mocked MetadataMgr metadataMgr,
            @Mocked JournalObservable journalObservable) throws Exception {
        new MockUp<LeaderOpExecutor>() {
            @Mock
            public static long fetchLeaderMaxJournalId(ConnectContext ctx) {
                return -1L;
            }
        };

        new Expectations() {
            {
                globalStateMgr.isLeader();
                result = false;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                globalStateMgr.getJournalObservable();
                result = journalObservable;
                minTimes = 0;

                // Both calls return null: db not found
                metadataMgr.getDb((ConnectContext) any, anyString, anyString);
                result = null;

                // waitOn must never be called when journalId <= 0
                journalObservable.waitOn(anyLong, anyInt);
                times = 0;
            }
        };

        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        Assertions.assertThrows(DdlException.class, () -> ctx.changeCatalogDb("missingdb"));
    }

    /**
     * On a follower FE, if fetchLeaderMaxJournalId returns -1 but the database
     * actually exists on the second check (e.g. replayed in the meantime),
     * changeCatalogDb() should succeed without calling waitOn.
     */
    @Test
    public void testChangeCatalogDb_followerSucceedsWithoutWaitWhenDbAppearsAfterFetchFails(
            @Mocked MetadataMgr metadataMgr,
            @Mocked JournalObservable journalObservable) throws Exception {
        Database mockDb = new Database(2L, "latedb");

        new MockUp<LeaderOpExecutor>() {
            @Mock
            public static long fetchLeaderMaxJournalId(ConnectContext ctx) {
                return -1L;
            }
        };

        new MockUp<Authorizer>() {
            @Mock
            public void checkAnyActionOnOrInDb(ConnectContext ctx, String catalog, String db)
                    throws AccessDeniedException {
            }
        };

        new Expectations() {
            {
                globalStateMgr.isLeader();
                result = false;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                globalStateMgr.getJournalObservable();
                result = journalObservable;
                minTimes = 0;

                // First call: null (enter wait block), second call: found
                metadataMgr.getDb((ConnectContext) any, anyString, anyString);
                returns(null, mockDb);

                journalObservable.waitOn(anyLong, anyInt);
                times = 0;
            }
        };

        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        Assertions.assertDoesNotThrow(() -> ctx.changeCatalogDb("latedb"));
        Assertions.assertEquals("latedb", ctx.getDatabase());
    }

    /**
     * On the leader FE, changeCatalogDb() should throw immediately without fetching
     * the leader journal ID or waiting for journal replay.
     */
    @Test
    public void testChangeCatalogDb_leaderDoesNotWaitForJournal(
            @Mocked MetadataMgr metadataMgr) {
        new Expectations() {
            {
                globalStateMgr.isLeader();
                result = true;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getDb((ConnectContext) any, anyString, anyString);
                result = null;

                // getJournalObservable must never be called on the leader
                globalStateMgr.getJournalObservable();
                times = 0;
            }
        };

        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        Assertions.assertThrows(DdlException.class, () -> ctx.changeCatalogDb("somedb"));
    }

    /**
     * On a follower FE, if the journal replay wait times out (DdlException from waitOn),
     * the exception is swallowed and the inner db check still runs, throwing ERR_BAD_DB_ERROR.
     */
    @Test
    public void testChangeCatalogDb_followerThrowsDbErrorAfterJournalWaitTimeout(
            @Mocked MetadataMgr metadataMgr,
            @Mocked JournalObservable journalObservable) throws DdlException {
        new MockUp<LeaderOpExecutor>() {
            @Mock
            public static long fetchLeaderMaxJournalId(ConnectContext ctx) {
                return 100L;
            }
        };

        new Expectations() {
            {
                globalStateMgr.isLeader();
                result = false;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                globalStateMgr.getJournalObservable();
                result = journalObservable;
                minTimes = 0;

                // Both calls return null: db does not appear even after wait
                metadataMgr.getDb((ConnectContext) any, anyString, anyString);
                result = null;

                // Simulate journal replay timeout
                journalObservable.waitOn(100L, anyInt);
                result = new DdlException("journal replay timeout");
            }
        };

        ConnectContext ctx = new ConnectContext(connection);
        ctx.setGlobalStateMgr(globalStateMgr);

        // The timeout exception is caught internally; ERR_BAD_DB_ERROR is still thrown
        Assertions.assertThrows(DdlException.class, () -> ctx.changeCatalogDb("nonexistent"));
    }
}
