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

package com.starrocks.sql;

import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.LogUtil;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.connector.jdbc.MockedJDBCMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test for mixed queries (internal + JDBC) where connector metadata fetch can block.
 * The analyzer must not hold PlannerMetaLock while doing connector metadata requests.
 */
public class StatementPlannerExternalTablesLockTest extends ConnectorPlanTestBase {

    private static class BlockingJDBCMetadata extends MockedJDBCMetadata {
        private final CountDownLatch started;
        private final CountDownLatch allowReturn;
        private final AtomicInteger getTableCalls;

        public BlockingJDBCMetadata(Map<String, String> properties,
                                   CountDownLatch started,
                                   CountDownLatch allowReturn,
                                   AtomicInteger getTableCalls) {
            super(properties);
            this.started = started;
            this.allowReturn = allowReturn;
            this.getTableCalls = getTableCalls;
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            getTableCalls.incrementAndGet();
            started.countDown();
            try {
                // Simulate connector metadata request blocking
                allowReturn.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return super.getTable(context, dbName, tblName);
        }
    }

    private static class BlockingJDBCQueryMetadata extends MockedJDBCMetadata {
        private final CountDownLatch started;
        private final CountDownLatch allowReturn;
        private final AtomicInteger getTableFromQueryCalls;

        public BlockingJDBCQueryMetadata(Map<String, String> properties,
                                         CountDownLatch started,
                                         CountDownLatch allowReturn,
                                         AtomicInteger getTableFromQueryCalls) {
            super(properties);
            this.started = started;
            this.allowReturn = allowReturn;
            this.getTableFromQueryCalls = getTableFromQueryCalls;
        }

        @Override
        public Table getTableFromQuery(ConnectContext context, String dbName, String query) {
            getTableFromQueryCalls.incrementAndGet();
            started.countDown();
            try {
                allowReturn.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return super.getTableFromQuery(context, dbName, query);
        }
    }

    private static class BlockingRefreshHiveMetadata extends MockedHiveMetadata {
        private final CountDownLatch refreshStarted;
        private final CountDownLatch allowRefresh;
        private final AtomicInteger getTableCalls;
        private final AtomicInteger refreshCalls;

        private BlockingRefreshHiveMetadata(CountDownLatch refreshStarted,
                                            CountDownLatch allowRefresh,
                                            AtomicInteger getTableCalls,
                                            AtomicInteger refreshCalls) {
            this.refreshStarted = refreshStarted;
            this.allowRefresh = allowRefresh;
            this.getTableCalls = getTableCalls;
            this.refreshCalls = refreshCalls;
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            getTableCalls.incrementAndGet();
            return super.getTable(context, dbName, tblName);
        }

        @Override
        public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
            refreshCalls.incrementAndGet();
            refreshStarted.countDown();
            try {
                allowRefresh.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class FailingRefreshHiveMetadata extends MockedHiveMetadata {
        private final AtomicInteger getTableCalls;
        private final AtomicInteger refreshCalls;
        private final RuntimeException refreshFailure;

        private FailingRefreshHiveMetadata(AtomicInteger getTableCalls, AtomicInteger refreshCalls,
                                           RuntimeException refreshFailure) {
            this.getTableCalls = getTableCalls;
            this.refreshCalls = refreshCalls;
            this.refreshFailure = refreshFailure;
        }

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            getTableCalls.incrementAndGet();
            return super.getTable(context, dbName, tblName);
        }

        @Override
        public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
            refreshCalls.incrementAndGet();
            throw refreshFailure;
        }
    }

    @Test
    public void testCTEWithInternalTable() throws Exception {
        // Test that CTE with internal table works correctly
        String sql = "with cte as (select * from t0) select * from cte";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            StatementPlanner.plan(stmt, connectContext);
        } catch (Exception e) {
            throw new RuntimeException("CTE with internal table test failed: " + e.getMessage(), e);
        }
    }

    @Test
    public void testCTEWithExternalTable() throws Exception {
        // Test that CTE with external table works correctly
        String sql = "with cte as (select * from jdbc0.partitioned_db0.tbl0) select * from cte";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            StatementPlanner.plan(stmt, connectContext);
        } catch (Exception e) {
            throw new RuntimeException("CTE with external table test failed: " + e.getMessage(), e);
        }
    }

    @Test
    public void testCTEJoinInternalTable() throws Exception {
        // Test CTE joined with internal table
        String sql = "with cte as (select * from jdbc0.partitioned_db0.tbl0) " +
                     "select * from t0 join cte on t0.v1 = cte.a";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            StatementPlanner.plan(stmt, connectContext);
        } catch (Exception e) {
            throw new RuntimeException("CTE join with internal table test failed: " + e.getMessage(), e);
        }
    }

    @Test
    public void testInternalTableCTEJoinExternalTable() throws Exception {
        // Test external table joined with CTE (based on internal table)
        // This is the key scenario: CTE with internal table, joined with external table
        // Should pre-parse external table before acquiring lock on internal tables
        String sql = "with cte as (select * from t0) " +
                     "select * from jdbc0.partitioned_db0.tbl0 " +
                     "join cte on jdbc0.partitioned_db0.tbl0.a = cte.v1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            StatementPlanner.plan(stmt, connectContext);
        } catch (Exception e) {
            throw new RuntimeException("Internal table CTE join external table test failed: "
                    + e.getMessage(), e);
        }
    }

    @Test
    public void testMixedQueryExternalMetadataNotUnderLock() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch allowReturn = new CountDownLatch(1);
        AtomicInteger getTableCalls = new AtomicInteger();

        // Replace jdbc0 metadata with a blocking one
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
        Map<String, String> props = new HashMap<>();
        props.put(JDBCResource.TYPE, "jdbc");
        props.put(JDBCResource.DRIVER_CLASS, "org.mariadb.jdbc.Driver");
        props.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
        props.put(JDBCResource.USER, "root");
        props.put(JDBCResource.PASSWORD, "123456");
        props.put(JDBCResource.CHECK_SUM, "xxxx");
        props.put(JDBCResource.DRIVER_URL, "xxxx");
        BlockingJDBCMetadata blocking = new BlockingJDBCMetadata(props, started, allowReturn, getTableCalls);
        metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, blocking);

        String sql = "select * from t0 join jdbc0.partitioned_db0.tbl0 on true";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

        AtomicBoolean lockCalled = new AtomicBoolean(false);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
            @Override
            public void lock() {
                // Don't take real locks in UT; just record timing.
                lockCalled.set(true);
            }

            @Override
            public void unlock() {
                // no-op
            }
        };

        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                StatementPlanner.analyzeStatement(stmt, connectContext, locker);
                finished.set(true);
            } catch (Throwable t0) {
                error.set(t0);
            }
        });
        t.start();

        // Wait for getTable to be called and blocking
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        // While connector metadata is blocked, we must not take PlannerMetaLock
        Assertions.assertFalse(lockCalled.get());

        allowReturn.countDown();
        t.join(TimeUnit.SECONDS.toMillis(20));

        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
        Assertions.assertTrue(finished.get());
        Assertions.assertTrue(lockCalled.get());
        // Analyzer should reuse pre-resolved external table; metadata getTable must not be called twice.
        Assertions.assertEquals(1, getTableCalls.get());
    }

    @Test
    public void testMixedQueryNativeQueryMetadataNotUnderLock() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch allowReturn = new CountDownLatch(1);
        AtomicInteger getTableFromQueryCalls = new AtomicInteger();

        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
        Map<String, String> props = new HashMap<>();
        props.put(JDBCResource.TYPE, "jdbc");
        props.put(JDBCResource.DRIVER_CLASS, "org.mariadb.jdbc.Driver");
        props.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
        props.put(JDBCResource.USER, "root");
        props.put(JDBCResource.PASSWORD, "123456");
        props.put(JDBCResource.CHECK_SUM, "xxxx");
        props.put(JDBCResource.DRIVER_URL, "xxxx");
        BlockingJDBCQueryMetadata blocking =
                new BlockingJDBCQueryMetadata(props, started, allowReturn, getTableFromQueryCalls);
        metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, blocking);

        String sql = "select * from t0 join table(jdbc0.native_query('select * from remote_table')) q on true";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

        AtomicBoolean lockCalled = new AtomicBoolean(false);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
            @Override
            public void lock() {
                lockCalled.set(true);
            }

            @Override
            public void unlock() {
                // no-op
            }
        };

        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                StatementPlanner.analyzeStatement(stmt, connectContext, locker);
                finished.set(true);
            } catch (Throwable t0) {
                error.set(t0);
            }
        });
        t.start();

        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        Assertions.assertFalse(lockCalled.get(),
                "Meta lock was acquired while JDBC native_query metadata was blocked.");

        allowReturn.countDown();
        t.join(TimeUnit.SECONDS.toMillis(20));

        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
        Assertions.assertTrue(finished.get());
        Assertions.assertTrue(lockCalled.get());
        Assertions.assertEquals(1, getTableFromQueryCalls.get(),
                "getTableFromQuery was called " + getTableFromQueryCalls.get() + " times, expected 1.");
    }

    @Test
    public void testInsertSelectMixedTablesWithBlockingExternal() throws Exception {
        // Test INSERT ... SELECT from external table to internal table
        // Verify: external metadata is fetched BEFORE acquiring meta lock (analyzeExternalTablesOnly path)
        // Verify: getTable is called only once (no duplicate calls)
        //
        // Note: Set runningUnitTest=false to test analyzeExternalTablesOnly path instead of deferredLock path
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch allowReturn = new CountDownLatch(1);
            AtomicInteger getTableCalls = new AtomicInteger();

            // Replace jdbc0 metadata with a blocking one
            GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
            Map<String, String> props = new HashMap<>();
            props.put(JDBCResource.TYPE, "jdbc");
            props.put(JDBCResource.DRIVER_CLASS, "org.mariadb.jdbc.Driver");
            props.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
            props.put(JDBCResource.USER, "root");
            props.put(JDBCResource.PASSWORD, "123456");
            props.put(JDBCResource.CHECK_SUM, "xxxx");
            props.put(JDBCResource.DRIVER_URL, "xxxx");
            BlockingJDBCMetadata blocking =
                    new BlockingJDBCMetadata(props, started, allowReturn, getTableCalls);
            metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, blocking);

            // Use INSERT with specific column names to avoid schema mismatch
            String sql = "insert into t0 (v1, v2) select a, b from jdbc0.partitioned_db0.tbl0";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

            AtomicBoolean lockCalled = new AtomicBoolean(false);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
                @Override
                public void lock() {
                    lockCalled.set(true);
                }

                @Override
                public void unlock() {
                    // no-op
                }
            };

            AtomicBoolean finished = new AtomicBoolean(false);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    StatementPlanner.analyzeStatement(stmt, connectContext, locker);
                    finished.set(true);
                } catch (Throwable t0) {
                    error.set(t0);
                }
            });
            t.start();

            // Wait for getTable to be called (external metadata fetch starts)
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            // CRITICAL: While external metadata is blocked, we must NOT take meta lock
            Assertions.assertFalse(lockCalled.get(),
                    "Meta lock was acquired while external metadata was blocked! " +
                            "This indicates the fix is not working for INSERT ... SELECT.");

            allowReturn.countDown();
            t.join(TimeUnit.SECONDS.toMillis(20));

            if (error.get() != null) {
                throw new RuntimeException("INSERT ... SELECT failed: " + error.get().getMessage(), error.get());
            }
            Assertions.assertTrue(finished.get(), "INSERT ... SELECT did not finish");

            // CRITICAL: getTable must be called only once (pre-resolved, not called again during analysis)
            Assertions.assertEquals(1, getTableCalls.get(),
                    "getTable was called " + getTableCalls.get() + " times, expected 1. " +
                            "This indicates duplicate external metadata fetch.");
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testInsertSelectFilesystemRefreshNotUnderLock() throws Exception {
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch allowRefresh = new CountDownLatch(1);
        AtomicInteger getTableCalls = new AtomicInteger();
        AtomicInteger refreshCalls = new AtomicInteger();

        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME,
                new BlockingRefreshHiveMetadata(refreshStarted, allowRefresh, getTableCalls, refreshCalls));

        String sql = "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

        AtomicBoolean lockCalled = new AtomicBoolean(false);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
            @Override
            public void lock() {
                lockCalled.set(true);
            }

            @Override
            public void unlock() {
                // no-op
            }
        };

        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                StatementPlanner.analyzeStatement(stmt, connectContext, locker);
                finished.set(true);
            } catch (Throwable t0) {
                error.set(t0);
            }
        });
        t.start();
        try {
            Assertions.assertTrue(refreshStarted.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(lockCalled.get(),
                    "Meta lock was acquired while filesystem external refresh was blocked.");
        } finally {
            allowRefresh.countDown();
            t.join(TimeUnit.SECONDS.toMillis(20));
            if (t.isAlive()) {
                t.interrupt();
                t.join(TimeUnit.SECONDS.toMillis(5));
            }
        }

        if (error.get() != null) {
            throw new RuntimeException("INSERT ... SELECT failed: " + error.get().getMessage(), error.get());
        }

        Assertions.assertFalse(t.isAlive(), "background analyze thread should exit after refresh is released");
        Assertions.assertTrue(finished.get(), "INSERT ... SELECT did not finish");
        Assertions.assertTrue(lockCalled.get(), "Meta lock was never acquired after refresh completed");
        Assertions.assertEquals(1, refreshCalls.get(), "filesystem external refresh should run exactly once");
        Assertions.assertEquals(2, getTableCalls.get(),
                "filesystem external table should be resolved before and after refresh, but not during locked analysis");
    }

    /**
     * Registers a Hive mock whose refreshTable throws {@code failure}, plans {@code sql}, and
     * returns the thrown exception. Callers build {@code failure} in the shape the production
     * connector produces; the mock only decides what refreshTable throws.
     */
    private Exception planWithFailingRefresh(String sql, RuntimeException failure,
                                             Class<? extends Exception> expectedType) throws Exception {
        AtomicInteger getTableCalls = new AtomicInteger();
        AtomicInteger refreshCalls = new AtomicInteger();

        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME,
                new FailingRefreshHiveMetadata(getTableCalls, refreshCalls, failure));

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        Exception e = Assertions.assertThrows(expectedType, () -> StatementPlanner.plan(stmt, connectContext));
        Assertions.assertEquals(1, refreshCalls.get(), "filesystem external refresh should be attempted once");
        Assertions.assertEquals(1, getTableCalls.get(),
                "planner should stop after the first pre-lock resolution when refresh fails");
        return e;
    }

    private void assertActionableRefreshError(String sql) throws Exception {
        // Production shape for a metastore denial: HiveMetaClient.callRPC wraps the reflective
        // InvocationTargetException (target: the metastore error) in a StarRocksConnectorException.
        StarRocksConnectorException connectorFailure = new StarRocksConnectorException(
                "Failed to get table [tpch.lineitem]",
                new InvocationTargetException(new MetaException("mock metastore denial")));
        Exception e = planWithFailingRefresh(sql, connectorFailure, StarRocksConnectorException.class);
        Assertions.assertTrue(e.getMessage().contains("hive0.tpch.lineitem"),
                "error should name the table: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("SET enable_insert_select_external_auto_refresh = false"),
                "error should tell the user how to disable the refresh: " + e.getMessage());
        Assertions.assertSame(connectorFailure, e.getCause(),
                "the connector exception should be kept as the cause, with its message and root cause");
        // What the client sees: our line first, then the connector message and the metastore's reason once each.
        String clientMessage = LogUtil.getUnwoundExceptionMessage(e);
        Assertions.assertTrue(clientMessage.startsWith("Auto refresh of external table hive0.tpch.lineitem"),
                clientMessage);
        Assertions.assertTrue(clientMessage.contains("MetaException: mock metastore denial"), clientMessage);
        Assertions.assertEquals(clientMessage.indexOf("Failed to get table"), clientMessage.lastIndexOf("Failed to get table"),
                "connector message should appear exactly once: " + clientMessage);
    }

    @Test
    public void testInsertSelectFilesystemRefreshFailurePropagatesActionableError() throws Exception {
        assertActionableRefreshError("insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem");
    }

    @Test
    public void testSubmitTaskInsertRefreshFailurePropagatesActionableError() throws Exception {
        assertActionableRefreshError("submit task refresh_failure_task as " +
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem");
    }

    @Test
    public void testInsertSelectFilesystemRefreshDroppedTableGetsNoDisableAdvice() throws Exception {
        // Production shape for a dropped table: HiveMetaClient.callRPC's reflective invoke wraps
        // NoSuchObjectException in InvocationTargetException, and CachingHiveMetastore rethrows it
        // wrapped in one more StarRocksConnectorException after invalidating the cache.
        Exception e = planWithFailingRefresh(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem",
                new StarRocksConnectorException("Failed to get table [tpch.lineitem], invalidated cache.",
                        new StarRocksConnectorException("Failed to get table [tpch.lineitem]",
                                new InvocationTargetException(new NoSuchObjectException("table not found")))),
                StarRocksConnectorException.class);
        Assertions.assertTrue(e.getMessage().contains("hive0.tpch.lineitem"),
                "error should name the table: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("no longer exists"),
                "error should say the table is gone: " + e.getMessage());
        Assertions.assertFalse(e.getMessage().contains("enable_insert_select_external_auto_refresh"),
                "a dropped table must not be answered with disable-the-refresh advice: " + e.getMessage());
        String clientMessage = LogUtil.getUnwoundExceptionMessage(e);
        Assertions.assertTrue(clientMessage.contains("NoSuchObjectException: table not found"), clientMessage);
    }

    @Test
    public void testInsertSelectFilesystemRefreshNotFoundWithoutCacheInvalidationGetsDisableAdvice() throws Exception {
        // A NoSuchObjectException from a later RPC of the same refresh (partition names, statistics) arrives
        // straight from callRPC, without the caching metastore's rewrap, and the cached table was not
        // invalidated. That is not a dropped table, so the user must still get the disable advice.
        Exception e = planWithFailingRefresh(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem",
                new StarRocksConnectorException("Failed to get partitionKeys on [tpch.lineitem]",
                        new InvocationTargetException(new NoSuchObjectException("stats not found"))),
                StarRocksConnectorException.class);
        Assertions.assertFalse(e.getMessage().contains("no longer exists"),
                "a live table must not be reported as dropped: " + e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("SET enable_insert_select_external_auto_refresh = false"),
                "error should tell the user how to disable the refresh: " + e.getMessage());
    }

    @Test
    public void testInsertSelectFilesystemRefreshNonConnectorFailureKeepsType() throws Exception {
        // Only connector exceptions are rewritten; anything else (an FE bug, an NPE) must propagate untouched.
        RuntimeException failure = new RuntimeException("mock refresh failure");
        Exception e = planWithFailingRefresh(
                "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem",
                failure, RuntimeException.class);
        Assertions.assertSame(failure, e, "non-connector failure should propagate untouched");
    }

    @Test
    public void testInsertSelectFilesystemRefreshDisabledSkipsRefresh() throws Exception {
        boolean originalValue = connectContext.getSessionVariable().isEnableInsertSelectExternalAutoRefresh();
        connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(false);
        try {
            CountDownLatch refreshStarted = new CountDownLatch(1);
            CountDownLatch allowRefresh = new CountDownLatch(1);
            AtomicInteger getTableCalls = new AtomicInteger();
            AtomicInteger refreshCalls = new AtomicInteger();

            GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME,
                    new BlockingRefreshHiveMetadata(refreshStarted, allowRefresh, getTableCalls, refreshCalls));

            String sql = "insert into t0 (v1, v2) select l_orderkey, l_partkey from hive0.tpch.lineitem";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

            AtomicBoolean lockCalled = new AtomicBoolean(false);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
                @Override
                public void lock() {
                    lockCalled.set(true);
                }

                @Override
                public void unlock() {
                    // no-op
                }
            };

            Assertions.assertDoesNotThrow(() -> StatementPlanner.analyzeStatement(stmt, connectContext, locker));
            Assertions.assertEquals(0, refreshCalls.get(), "refresh should be skipped when auto refresh is disabled");
            Assertions.assertEquals(1, getTableCalls.get(), "external table should still be pre-resolved once");
            Assertions.assertFalse(refreshStarted.await(200, TimeUnit.MILLISECONDS),
                    "refresh should not have been triggered");
            Assertions.assertTrue(lockCalled.get(), "analyze should still acquire meta lock");
            allowRefresh.countDown();
        } finally {
            connectContext.getSessionVariable().setEnableInsertSelectExternalAutoRefresh(originalValue);
        }
    }

    @Test
    public void testInsertSelectFilesystemRefreshWithUnqualifiedExternalTable() throws Exception {
        String originalCatalog = connectContext.getCurrentCatalog();
        String originalDb = connectContext.getDatabase();
        try {
            connectContext.setCurrentCatalog(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME);
            connectContext.setDatabase(MockedHiveMetadata.MOCKED_TPCH_DB_NAME);

            CountDownLatch refreshStarted = new CountDownLatch(1);
            CountDownLatch allowRefresh = new CountDownLatch(1);
            AtomicInteger getTableCalls = new AtomicInteger();
            AtomicInteger refreshCalls = new AtomicInteger();

            GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME,
                    new BlockingRefreshHiveMetadata(refreshStarted, allowRefresh, getTableCalls, refreshCalls));

            String sql = "insert into default_catalog.test.t0 (v1, v2) select l_orderkey, l_partkey from lineitem";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

            AtomicBoolean lockCalled = new AtomicBoolean(false);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
                @Override
                public void lock() {
                    lockCalled.set(true);
                }

                @Override
                public void unlock() {
                    // no-op
                }
            };

            AtomicBoolean finished = new AtomicBoolean(false);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    StatementPlanner.analyzeStatement(stmt, connectContext, locker);
                    finished.set(true);
                } catch (Throwable t0) {
                    error.set(t0);
                }
            });
            t.start();
            try {
                Assertions.assertTrue(refreshStarted.await(10, TimeUnit.SECONDS));
                Assertions.assertFalse(lockCalled.get(),
                        "Meta lock was acquired while refreshing an unqualified external table.");
            } finally {
                allowRefresh.countDown();
                t.join(TimeUnit.SECONDS.toMillis(20));
                if (t.isAlive()) {
                    t.interrupt();
                    t.join(TimeUnit.SECONDS.toMillis(5));
                }
            }

            if (error.get() != null) {
                throw new RuntimeException("INSERT ... SELECT failed: " + error.get().getMessage(), error.get());
            }

            Assertions.assertFalse(t.isAlive(), "background analyze thread should exit after refresh is released");
            Assertions.assertTrue(finished.get(), "INSERT ... SELECT did not finish");
            Assertions.assertEquals(1, refreshCalls.get(), "unqualified external table should still refresh once");
            Assertions.assertEquals(2, getTableCalls.get(),
                    "unqualified external table should be resolved before and after refresh");
        } finally {
            connectContext.setCurrentCatalog(originalCatalog);
            connectContext.setDatabase(originalDb);
        }
    }

    @Test
    public void testNestedSubqueryWithSameNameCTE() throws Exception {
        // Test that CTE in nested subquery doesn't affect external table pre-resolution in outer query.
        // Scenario: Outer query uses external table "tbl0", nested subquery has CTE with same name "tbl0".
        // The external table should still be pre-resolved (not skipped due to CTE name collision).
        String sql = "select * from jdbc0.partitioned_db0.tbl0 t " +
                     "where exists (with tbl0 as (select * from t0) select * from tbl0)";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            StatementPlanner.plan(stmt, connectContext);
            // Success: External table was correctly pre-resolved despite nested CTE having same name
        } catch (Exception e) {
            throw new RuntimeException("Nested subquery with same-name CTE test failed: " + e.getMessage(), e);
        }
    }

    @Test
    public void testNestedSubqueryCTEDoesNotPolluteOuterScope() throws Exception {
        // Test that CTE defined in WHERE clause subquery doesn't pollute outer scope.
        // This verifies the session-based approach works correctly.
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch allowReturn = new CountDownLatch(1);
        AtomicInteger getTableCalls = new AtomicInteger();

        // Replace jdbc0 metadata with a blocking one
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
        Map<String, String> props = new HashMap<>();
        props.put(JDBCResource.TYPE, "jdbc");
        props.put(JDBCResource.DRIVER_CLASS, "org.mariadb.jdbc.Driver");
        props.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
        props.put(JDBCResource.USER, "root");
        props.put(JDBCResource.PASSWORD, "123456");
        props.put(JDBCResource.CHECK_SUM, "xxxx");
        props.put(JDBCResource.DRIVER_URL, "xxxx");
        BlockingJDBCMetadata blocking = new BlockingJDBCMetadata(props, started, allowReturn, getTableCalls);
        metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, blocking);

        // Outer query uses external table "tbl0", nested subquery has CTE "tbl0"
        String sql = "select * from t0 join jdbc0.partitioned_db0.tbl0 on true " +
                     "where exists (with tbl0 as (select * from t0) select * from tbl0)";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

        AtomicBoolean lockCalled = new AtomicBoolean(false);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
            @Override
            public void lock() {
                lockCalled.set(true);
            }

            @Override
            public void unlock() {
                // no-op
            }
        };

        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                StatementPlanner.analyzeStatement(stmt, connectContext, locker);
                finished.set(true);
            } catch (Throwable t0) {
                error.set(t0);
            }
        });
        t.start();

        // Wait for getTable to be called
        Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        // CRITICAL: While external metadata is blocked, we must NOT take meta lock
        Assertions.assertFalse(lockCalled.get(),
                "Meta lock was acquired while external metadata was blocked! " +
                        "This indicates CTE in nested subquery incorrectly prevented external table pre-resolution.");

        allowReturn.countDown();
        t.join(TimeUnit.SECONDS.toMillis(20));

        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
        Assertions.assertTrue(finished.get());
        Assertions.assertTrue(lockCalled.get());
        // Verify getTable was called only once (pre-resolved, not called again during analysis)
        Assertions.assertEquals(1, getTableCalls.get(),
                "getTable was called " + getTableCalls.get() + " times, expected 1. " +
                        "This indicates CTE scoping issue prevented proper pre-resolution.");
    }

    @Test
    public void testCteReferenceNotResolvedAsExternalTableInExternalCatalog() throws Exception {
        String originalCatalog = connectContext.getCurrentCatalog();
        String originalDb = connectContext.getDatabase();
        try {
            connectContext.setCurrentCatalog(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME);
            connectContext.setDatabase(MockedJDBCMetadata.MOCKED_PARTITIONED_DB_NAME);

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch allowReturn = new CountDownLatch(1);
            AtomicInteger getTableCalls = new AtomicInteger();

            // Replace jdbc0 metadata with a blocking one
            GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) gsm.getMetadataMgr();
            Map<String, String> props = new HashMap<>();
            props.put(JDBCResource.TYPE, "jdbc");
            props.put(JDBCResource.DRIVER_CLASS, "org.mariadb.jdbc.Driver");
            props.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
            props.put(JDBCResource.USER, "root");
            props.put(JDBCResource.PASSWORD, "123456");
            props.put(JDBCResource.CHECK_SUM, "xxxx");
            props.put(JDBCResource.DRIVER_URL, "xxxx");
            BlockingJDBCMetadata blocking = new BlockingJDBCMetadata(props, started, allowReturn, getTableCalls);
            metadataMgr.registerMockedMetadata(MockedJDBCMetadata.MOCKED_JDBC_CATALOG_NAME, blocking);

            String sql = "with tbl0 as (select * from jdbc0.partitioned_db0.tbl0) " +
                         "select * from default_catalog.test.t0 join tbl0 on true";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);

            AtomicBoolean lockCalled = new AtomicBoolean(false);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt) {
                @Override
                public void lock() {
                    lockCalled.set(true);
                }

                @Override
                public void unlock() {
                    // no-op
                }
            };

            AtomicBoolean finished = new AtomicBoolean(false);
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try {
                    StatementPlanner.analyzeStatement(stmt, connectContext, locker);
                    finished.set(true);
                } catch (Throwable t0) {
                    error.set(t0);
                }
            });
            t.start();

            // getTable should be called only for the CTE definition table, not for the CTE reference
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(lockCalled.get(), "Meta lock was acquired while external metadata was blocked.");

            allowReturn.countDown();
            t.join(TimeUnit.SECONDS.toMillis(20));

            if (error.get() != null) {
                throw new RuntimeException(error.get());
            }
            Assertions.assertTrue(finished.get());
            Assertions.assertTrue(lockCalled.get());
            Assertions.assertEquals(1, getTableCalls.get(),
                    "CTE reference was incorrectly resolved as external table.");
        } finally {
            connectContext.setCurrentCatalog(originalCatalog);
            connectContext.setDatabase(originalDb);
        }
    }
}
