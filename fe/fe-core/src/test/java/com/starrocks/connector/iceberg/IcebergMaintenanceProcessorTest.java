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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.procedure.IcebergMaintenanceTaskStats;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class IcebergMaintenanceProcessorTest extends TableTestBase {

    @BeforeAll
    public static void setUpMetrics() {
        // initialize the metric registry against the real GlobalStateMgr before any
        // test mocks it: MetricRepo.addMetric() lazily calls init(), which dereferences
        // GlobalStateMgr managers eagerly and would NPE on a bare mock
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testIsRecentlyWrittenTableWithinWindow() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        Mockito.when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        Mockito.when(snapshot.summary()).thenReturn(Collections.singletonMap("operation", "append"));

        long nowMillis = System.currentTimeMillis();
        Mockito.when(snapshot.timestampMillis()).thenReturn(nowMillis - 1000L);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "isRecentlyWrittenTable", IcebergCatalog.class, ConnectContext.class,
                String.class, String.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(processor, catalog, null, "db", "tbl", nowMillis);
        Assertions.assertTrue(result);
    }

    @Test
    public void testIsRecentlyWrittenTableOutsideWindow() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        Mockito.when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        Mockito.when(snapshot.summary()).thenReturn(Collections.singletonMap("operation", "delete"));

        long nowMillis = System.currentTimeMillis();
        long twentyFiveHoursMillis = 25L * 3600L * 1000L;
        Mockito.when(snapshot.timestampMillis()).thenReturn(nowMillis - twentyFiveHoursMillis);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "isRecentlyWrittenTable", IcebergCatalog.class, ConnectContext.class,
                String.class, String.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(processor, catalog, null, "db", "tbl", nowMillis);
        Assertions.assertFalse(result);
    }

    @Test
    public void testIsRecentlyWrittenTableNullTable() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(null);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "isRecentlyWrittenTable", IcebergCatalog.class, ConnectContext.class,
                String.class, String.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(processor, catalog, null, "db", "tbl", System.currentTimeMillis());
        Assertions.assertFalse(result);
    }

    @Test
    public void testIsRecentlyWrittenTableSummaryNull() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        Mockito.when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        Mockito.when(snapshot.summary()).thenReturn(null);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "isRecentlyWrittenTable", IcebergCatalog.class, ConnectContext.class,
                String.class, String.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(processor, catalog, null, "db", "tbl", System.currentTimeMillis());
        Assertions.assertFalse(result);
    }

    @Test
    public void testIsRecentlyWrittenTableNonWriteOperation() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        Mockito.when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        Mockito.when(snapshot.summary()).thenReturn(Collections.singletonMap("operation", "replace"));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "isRecentlyWrittenTable", IcebergCatalog.class, ConnectContext.class,
                String.class, String.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(processor, catalog, null, "db", "tbl", System.currentTimeMillis());
        Assertions.assertFalse(result);
    }

    @Test
    public void testIsRecentlyWrittenTableOverwriteOperation() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table table = Mockito.mock(Table.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);

        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl"))).thenReturn(table);
        Mockito.when(table.snapshots()).thenReturn(Collections.singletonList(snapshot));
        Mockito.when(snapshot.summary()).thenReturn(Collections.singletonMap("operation", "overwrite"));

        long nowMillis = System.currentTimeMillis();
        Mockito.when(snapshot.timestampMillis()).thenReturn(nowMillis - 1000L);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "isRecentlyWrittenTable", IcebergCatalog.class, ConnectContext.class,
                String.class, String.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(processor, catalog, null, "db", "tbl", nowMillis);
        Assertions.assertTrue(result);
    }

    @Test
    public void testIsRecentlyWrittenTableGetTableThrows() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("tbl")))
                .thenThrow(new RuntimeException("boom"));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "isRecentlyWrittenTable", IcebergCatalog.class, ConnectContext.class,
                String.class, String.class, long.class);
        method.setAccessible(true);

        boolean result = (Boolean) method.invoke(processor, catalog, null, "db", "tbl", System.currentTimeMillis());
        Assertions.assertFalse(result);
    }

    @Test
    public void testIsWriteOperation() throws Exception {
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod("isWriteOperation", String.class);
        method.setAccessible(true);

        Assertions.assertFalse((Boolean) method.invoke(processor, (Object) null));
        Assertions.assertTrue((Boolean) method.invoke(processor, "append"));
        Assertions.assertTrue((Boolean) method.invoke(processor, "DELETE"));
        Assertions.assertTrue((Boolean) method.invoke(processor, "Overwrite"));
        Assertions.assertFalse((Boolean) method.invoke(processor, "replace"));
    }

    @Test
    public void testListTablesForMaintenance() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Mockito.when(catalog.listAllDatabases(Mockito.any())).thenThrow(new RuntimeException("no dbs"));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "listTablesForMaintenance", IcebergCatalog.class, ConnectContext.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<Pair<String, String>> r =
                (List<Pair<String, String>>) method.invoke(processor, catalog, new ConnectContext());
        Assertions.assertTrue(r.isEmpty());

        Mockito.reset(catalog);
        Mockito.when(catalog.listAllDatabases(Mockito.any())).thenReturn(Lists.newArrayList("db1"));
        Mockito.when(catalog.listTables(Mockito.any(), Mockito.eq("db1"))).thenThrow(new RuntimeException("no tables"));

        @SuppressWarnings("unchecked")
        List<Pair<String, String>> r2 =
                (List<Pair<String, String>>) method.invoke(processor, catalog, new ConnectContext());
        Assertions.assertTrue(r2.isEmpty());
    }

    @Test
    public void testShutdownExecutorPaths() throws Exception {
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        ExecutorService mockExec = Mockito.mock(ExecutorService.class);
        Field f = IcebergMaintenanceProcessor.class.getDeclaredField("maintenanceExecutor");
        f.setAccessible(true);
        f.set(processor, mockExec);

        Mockito.when(mockExec.awaitTermination(60, TimeUnit.SECONDS)).thenReturn(true);
        processor.shutdown();
        Mockito.verify(mockExec).shutdown();
        Mockito.verify(mockExec).awaitTermination(60, TimeUnit.SECONDS);

        Mockito.reset(mockExec);
        Mockito.when(mockExec.awaitTermination(60, TimeUnit.SECONDS)).thenReturn(false).thenReturn(true);
        processor.shutdown();
        Mockito.verify(mockExec).shutdownNow();

        Mockito.reset(mockExec);
        Mockito.when(mockExec.awaitTermination(60, TimeUnit.SECONDS)).thenReturn(false).thenReturn(false);
        processor.shutdown();

        Mockito.reset(mockExec);
        Mockito.when(mockExec.awaitTermination(60, TimeUnit.SECONDS)).thenThrow(new InterruptedException());
        processor.shutdown();
        Mockito.verify(mockExec).shutdownNow();
        Assertions.assertTrue(Thread.interrupted());
    }

    @Test
    public void testRunAfterCatalogReadyNotLeader() {
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr mockGsm = Mockito.mock(GlobalStateMgr.class);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            Mockito.when(mockGsm.isLeader()).thenReturn(false);
            processor.runAfterCatalogReady();
        }
    }

    @Test
    public void testRunAfterCatalogReadyEmptyMap() {
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
            GlobalStateMgr mockGsm = Mockito.mock(GlobalStateMgr.class);
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            Mockito.when(mockGsm.isLeader()).thenReturn(true);
            processor.runAfterCatalogReady();
        }
    }

    @Test
    public void testRunAfterCatalogReadyUpdatesInterval() {
        int old = Config.iceberg_background_check_maintenance_interval_seconds;
        Config.iceberg_background_check_maintenance_interval_seconds = old + 999;
        MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class);
        try {
            IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
            GlobalStateMgr mockGsm = mockGlobalStateMgrForLeaderFe();
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            processor.runAfterCatalogReady();
            long expectedMs = Config.iceberg_background_check_maintenance_interval_seconds * 1000L;
            Assertions.assertEquals(expectedMs, processor.getInterval());
        } finally {
            gsm.close();
            Config.iceberg_background_check_maintenance_interval_seconds = old;
        }
    }

    @Test
    public void testRunAfterCatalogReadyCleanupAndRewrite() throws Exception {
        int oldInterval = Config.iceberg_background_check_maintenance_interval_seconds;
        try {
            Config.iceberg_background_check_maintenance_interval_seconds = 3600;

            TestTables.TestTable icebergTable = create(SCHEMA_A, SPEC_A, "maint_cr", 2);
            icebergTable.newFastAppend().appendFile(FILE_A).commit();

            IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
            HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
            Mockito.when(hdfs.getConfiguration()).thenReturn(new Configuration());
            Mockito.when(catalog.listAllDatabases(Mockito.any())).thenReturn(Lists.newArrayList("db"));
            Mockito.when(catalog.listTables(Mockito.any(), Mockito.eq("db"))).thenReturn(Lists.newArrayList("t1"));
            Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("t1"))).thenReturn(icebergTable);

            IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
            processor.registerIcebergCatalogForMaintenance("cat", catalog, hdfs, 1, 1);
            setLastMaintenanceTimes(processor, 0L, 0L);

            try (MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class)) {
                GlobalStateMgr mockGsm = mockGlobalStateMgrForLeaderFe();
                gsm.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
                processor.runAfterCatalogReady();
            }
        } finally {
            Config.iceberg_background_check_maintenance_interval_seconds = oldInterval;
        }
    }

    @Test
    public void testRunAfterCatalogReadyNeitherDue() throws Exception {
        TestTables.TestTable icebergTable = create(SCHEMA_A, SPEC_A, "maint_neither", 2);
        icebergTable.newFastAppend().appendFile(FILE_A).commit();

        IcebergCatalog mockCatalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Mockito.when(mockCatalog.listAllDatabases(Mockito.any())).thenReturn(Lists.newArrayList("db"));
        Mockito.when(mockCatalog.listTables(Mockito.any(), Mockito.eq("db"))).thenReturn(Lists.newArrayList("t1"));
        Mockito.when(mockCatalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("t1"))).thenReturn(icebergTable);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        processor.registerIcebergCatalogForMaintenance("cat", mockCatalog, hdfs, 1, 1);
        setLastMaintenanceTimes(processor, System.currentTimeMillis(), System.currentTimeMillis());

        MockedStatic<GlobalStateMgr> gsm = Mockito.mockStatic(GlobalStateMgr.class);
        try {
            GlobalStateMgr mockGsm = mockGlobalStateMgrForLeaderFe();
            gsm.when(GlobalStateMgr::getCurrentState).thenReturn(mockGsm);
            processor.runAfterCatalogReady();
        } finally {
            gsm.close();
        }
        Mockito.verify(mockCatalog, Mockito.atLeastOnce()).listAllDatabases(Mockito.any());
        Mockito.verify(mockCatalog, Mockito.atLeastOnce()).getTable(Mockito.any(ConnectContext.class),
                Mockito.eq("db"), Mockito.eq("t1"));
    }

    @Test
    public void testRunCleanupSkipsNullSnapshotTable() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table tableNoSnap = Mockito.mock(Table.class);
        Mockito.when(tableNoSnap.currentSnapshot()).thenReturn(null);

        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("bad")))
                .thenReturn(tableNoSnap);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runCleanupForCatalog", getMaintenanceInfoClass(), List.class);
        method.setAccessible(true);

        Object info = buildMaintenanceInfo("c", catalog, Mockito.mock(HdfsEnvironment.class), 1, 1);
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> names = Lists.newArrayList(Pair.create("db", "bad"));
        method.invoke(processor, info, names);
    }

    @Test
    public void testRunCleanupTaskThrows() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("x")))
                .thenThrow(new RuntimeException("get table failed"));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runCleanupForCatalog", getMaintenanceInfoClass(), List.class);
        method.setAccessible(true);

        Object info = buildMaintenanceInfo("c", catalog, Mockito.mock(HdfsEnvironment.class), 1, 1);
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> names = Lists.newArrayList(Pair.create("db", "x"));
        method.invoke(processor, info, names);
    }

    @Test
    public void testRunRewriteTaskThrows() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("x")))
                .thenThrow(new RuntimeException("get table failed"));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRewriteForCatalog", getMaintenanceInfoClass(), List.class);
        method.setAccessible(true);

        Object info = buildMaintenanceInfo("c", catalog, Mockito.mock(HdfsEnvironment.class), 1, 1);
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> names = Lists.newArrayList(Pair.create("db", "x"));
        method.invoke(processor, info, names);
    }

    @Test
    public void testRunRewriteSkipsNullSnapshot() throws Exception {
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table tableNoSnap = Mockito.mock(Table.class);
        Mockito.when(tableNoSnap.currentSnapshot()).thenReturn(null);
        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("bad"))).thenReturn(tableNoSnap);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRewriteForCatalog", getMaintenanceInfoClass(), List.class);
        method.setAccessible(true);

        Object info = buildMaintenanceInfo("c", catalog, Mockito.mock(HdfsEnvironment.class), 1, 1);
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> names = Lists.newArrayList(Pair.create("db", "bad"));
        method.invoke(processor, info, names);
    }

    @Test
    public void testRunProcedureHelpers() throws Exception {
        TestTables.TestTable icebergTable = create(SCHEMA_A, SPEC_A, "maint_proc", 2);
        icebergTable.newFastAppend().appendFile(FILE_A).commit();
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Mockito.when(hdfs.getConfiguration()).thenReturn(new Configuration());

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method expire = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runExpireSnapshots", IcebergCatalog.class, Table.class, HdfsEnvironment.class,
                IcebergMaintenanceTaskStats.class);
        expire.setAccessible(true);
        IcebergMaintenanceTaskStats expireStats = new IcebergMaintenanceTaskStats();
        expire.invoke(processor, catalog, icebergTable, hdfs, expireStats);
        Assertions.assertEquals(IcebergTableOperation.EXPIRE_SNAPSHOTS, expireStats.getOperation());
        Assertions.assertEquals(1, expireStats.getSnapshotCountInput());
        Assertions.assertTrue(expireStats.isExecuted());
        Assertions.assertEquals(1, expireStats.getSnapshotCountOutput());

        Method orphan = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRemoveOrphanFiles", IcebergCatalog.class, Table.class, HdfsEnvironment.class,
                IcebergMaintenanceTaskStats.class);
        orphan.setAccessible(true);
        IcebergMaintenanceTaskStats orphanStats = new IcebergMaintenanceTaskStats();
        orphan.invoke(processor, catalog, icebergTable, hdfs, orphanStats);
        Assertions.assertEquals(IcebergTableOperation.REMOVE_ORPHAN_FILES, orphanStats.getOperation());
        Assertions.assertTrue(orphanStats.isExecuted());

        Method rewrite = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRewriteManifests", IcebergCatalog.class, Table.class, HdfsEnvironment.class,
                IcebergMaintenanceTaskStats.class);
        rewrite.setAccessible(true);
        IcebergMaintenanceTaskStats rewriteStats = new IcebergMaintenanceTaskStats();
        rewrite.invoke(processor, catalog, icebergTable, hdfs, rewriteStats);
        Assertions.assertEquals(IcebergTableOperation.REWRITE_MANIFESTS, rewriteStats.getOperation());
    }

    @Test
    public void testRunRewriteManifestsCommitsTransaction() throws Exception {
        TestTables.TestTable icebergTable = create(SCHEMA_A, SPEC_A, "maint_commit_rewrite", 2);
        // two separate appends produce two manifests, making rewrite_manifests do real work
        icebergTable.newFastAppend().appendFile(FILE_A).commit();
        icebergTable.newFastAppend().appendFile(FILE_A_1).commit();
        Assertions.assertEquals(2, icebergTable.currentSnapshot().allManifests(icebergTable.io()).size());

        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Mockito.when(hdfs.getConfiguration()).thenReturn(new Configuration());

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method rewrite = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRewriteManifests", IcebergCatalog.class, Table.class, HdfsEnvironment.class,
                IcebergMaintenanceTaskStats.class);
        rewrite.setAccessible(true);
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        rewrite.invoke(processor, catalog, icebergTable, hdfs, stats);

        Assertions.assertTrue(stats.isExecuted());
        Assertions.assertEquals(2, stats.getManifestCountInput());
        // the rewritten manifest list must be published (commitTransaction), so the
        // refreshed table now has a single compacted manifest
        icebergTable.refresh();
        Assertions.assertEquals(1, icebergTable.currentSnapshot().allManifests(icebergTable.io()).size());
        Assertions.assertEquals(1, stats.getManifestCountOutput());
        Assertions.assertTrue(stats.getManifestBytesOutput() > 0);
    }

    @Test
    public void testRunRewriteManifestsNoopDoesNotCommit() throws Exception {
        TestTables.TestTable icebergTable = create(SCHEMA_A, SPEC_A, "maint_noop_rewrite", 2);
        icebergTable.newFastAppend().appendFile(FILE_A).commit();
        long snapshotId = icebergTable.currentSnapshot().snapshotId();

        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Mockito.when(hdfs.getConfiguration()).thenReturn(new Configuration());

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method rewrite = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRewriteManifests", IcebergCatalog.class, Table.class, HdfsEnvironment.class,
                IcebergMaintenanceTaskStats.class);
        rewrite.setAccessible(true);
        IcebergMaintenanceTaskStats stats = new IcebergMaintenanceTaskStats();
        rewrite.invoke(processor, catalog, icebergTable, hdfs, stats);

        // single small manifest: the procedure early-returns, no transaction commit,
        // so no new snapshot is created
        Assertions.assertFalse(stats.isExecuted());
        icebergTable.refresh();
        Assertions.assertEquals(snapshotId, icebergTable.currentSnapshot().snapshotId());
    }

    @Test
    public void testWaitForFuturesDeadlineExceededBeforeGet() throws Exception {
        int old = Config.iceberg_background_check_maintenance_interval_seconds;
        try {
            Config.iceberg_background_check_maintenance_interval_seconds = 1;

            Future<?> f1 = Mockito.mock(Future.class);
            Mockito.when(f1.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS))).thenAnswer(invocation -> {
                Thread.sleep(1100);
                return null;
            });
            Future<?> f2 = Mockito.mock(Future.class);

            IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
            Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                    "waitForFutures", List.class, List.class, String.class, String.class);
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            List<Future<?>> futures = Lists.newArrayList(f1, f2);
            @SuppressWarnings("unchecked")
            List<Pair<String, String>> names = Lists.newArrayList(
                    Pair.create("d", "t1"), Pair.create("d", "t2"));
            method.invoke(processor, futures, names, "cat", "cleanup");
            Mockito.verify(f2).cancel(true);
        } finally {
            Config.iceberg_background_check_maintenance_interval_seconds = old;
        }
    }

    @Test
    public void testWaitForFuturesPerTableTimeout() throws Exception {
        int old = Config.iceberg_background_check_maintenance_interval_seconds;
        try {
            Config.iceberg_background_check_maintenance_interval_seconds = 3600;

            Future<?> f1 = Mockito.mock(Future.class);
            Mockito.when(f1.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS)))
                    .thenThrow(new TimeoutException());

            IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
            Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                    "waitForFutures", List.class, List.class, String.class, String.class);
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            List<Future<?>> futures = Lists.newArrayList(f1);
            @SuppressWarnings("unchecked")
            List<Pair<String, String>> names = Lists.newArrayList(Pair.create("d", "t1"));
            method.invoke(processor, futures, names, "cat", "cleanup");
            Mockito.verify(f1).cancel(true);
        } finally {
            Config.iceberg_background_check_maintenance_interval_seconds = old;
        }
    }

    @Test
    public void testWaitForFuturesTimeoutWhenTotalDeadlineExceeded() throws Exception {
        int old = Config.iceberg_background_check_maintenance_interval_seconds;
        try {
            Config.iceberg_background_check_maintenance_interval_seconds = 1;

            Future<?> f1 = Mockito.mock(Future.class);
            Mockito.when(f1.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS))).thenAnswer(invocation -> {
                Thread.sleep(1100);
                throw new TimeoutException();
            });
            Future<?> f2 = Mockito.mock(Future.class);

            IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
            Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                    "waitForFutures", List.class, List.class, String.class, String.class);
            method.setAccessible(true);

            @SuppressWarnings("unchecked")
            List<Future<?>> futures = Lists.newArrayList(f1, f2);
            @SuppressWarnings("unchecked")
            List<Pair<String, String>> names = Lists.newArrayList(
                    Pair.create("d", "t1"), Pair.create("d", "t2"));
            method.invoke(processor, futures, names, "cat", "cleanup");
            Mockito.verify(f1).cancel(true);
            Mockito.verify(f2).cancel(true);
        } finally {
            Config.iceberg_background_check_maintenance_interval_seconds = old;
        }
    }

    @Test
    public void testWaitForFuturesUnexpectedException() throws Exception {
        Future<?> f1 = Mockito.mock(Future.class);
        Mockito.when(f1.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new RuntimeException("unexpected"));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "waitForFutures", List.class, List.class, String.class, String.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<Future<?>> futures = Lists.newArrayList(f1);
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> names = Lists.newArrayList(Pair.create("d", "t1"));
        method.invoke(processor, futures, names, "cat", "cleanup");
    }

    @Test
    public void testRunCleanupRejectedExecution() throws Exception {
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        ExecutorService mockExec = Mockito.mock(ExecutorService.class);
        Field f = IcebergMaintenanceProcessor.class.getDeclaredField("maintenanceExecutor");
        f.setAccessible(true);
        f.set(processor, mockExec);

        AtomicInteger submits = new AtomicInteger();
        Mockito.when(mockExec.submit(Mockito.any(Runnable.class))).thenAnswer(invocation -> {
            if (submits.incrementAndGet() == 1) {
                Runnable r = invocation.getArgument(0);
                r.run();
                return CompletableFuture.completedFuture(null);
            }
            throw new RejectedExecutionException();
        });

        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        Table tableNoSnap = Mockito.mock(Table.class);
        Mockito.when(tableNoSnap.currentSnapshot()).thenReturn(null);
        Mockito.when(catalog.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("t1"))).thenReturn(tableNoSnap);

        Method method = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runCleanupForCatalog", getMaintenanceInfoClass(), List.class);
        method.setAccessible(true);

        Object info = buildMaintenanceInfo("c", catalog, Mockito.mock(HdfsEnvironment.class), 1, 1);
        @SuppressWarnings("unchecked")
        List<Pair<String, String>> names = Lists.newArrayList(
                Pair.create("db", "t1"), Pair.create("db", "t2"));
        method.invoke(processor, info, names);
    }

    /**
     * {@link ConnectContext} constructor calls {@link GlobalStateMgr#getVariableMgr()}#newSessionVariable();
     * stub both when mocking leader FE for {@code runAfterCatalogReady()}.
     */
    private static GlobalStateMgr mockGlobalStateMgrForLeaderFe() {
        GlobalStateMgr mockGsm = Mockito.mock(GlobalStateMgr.class);
        VariableMgr variableMgr = Mockito.mock(VariableMgr.class);
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(mockGsm.isLeader()).thenReturn(true);
        Mockito.when(mockGsm.getVariableMgr()).thenReturn(variableMgr);
        Mockito.when(variableMgr.newSessionVariable()).thenReturn(sessionVariable);
        return mockGsm;
    }

    private static Class<?> getMaintenanceInfoClass() throws Exception {
        for (Class<?> c : IcebergMaintenanceProcessor.class.getDeclaredClasses()) {
            if (c.getSimpleName().equals("IcebergMaintenanceInfo")) {
                return c;
            }
        }
        throw new IllegalStateException("IcebergMaintenanceInfo not found");
    }

    private static Object buildMaintenanceInfo(String catalogName, IcebergCatalog icebergCatalog,
                                               HdfsEnvironment hdfs, int cleanupHours, int rewriteHours) throws Exception {
        Class<?> clazz = getMaintenanceInfoClass();
        Constructor<?> ctor = clazz.getDeclaredConstructor(
                String.class, IcebergCatalog.class, HdfsEnvironment.class, int.class, int.class);
        ctor.setAccessible(true);
        return ctor.newInstance(catalogName, icebergCatalog, hdfs, cleanupHours, rewriteHours);
    }

    @Test
    public void testIcebergPlanWorkerExecutorIsNotNull() throws Exception {
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Field f = IcebergMaintenanceProcessor.class.getDeclaredField("icebergPlanWorkerExecutor");
        f.setAccessible(true);
        ExecutorService executor = (ExecutorService) f.get(processor);
        Assertions.assertNotNull(executor);
        Assertions.assertFalse(executor.isShutdown());
    }

    @Test
    public void testShutdownClosesIcebergPlanWorkerExecutor() throws Exception {
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Field f = IcebergMaintenanceProcessor.class.getDeclaredField("icebergPlanWorkerExecutor");
        f.setAccessible(true);
        ExecutorService planExecutor = (ExecutorService) f.get(processor);

        processor.shutdown();
        Assertions.assertTrue(planExecutor.isShutdown());
    }

    @Test
    public void testRunExpireSnapshotsPassesExecutorViaContext() throws Exception {
        TestTables.TestTable icebergTable = create(SCHEMA_A, SPEC_A, "maint_exec_expire", 2);
        icebergTable.newFastAppend().appendFile(FILE_A).commit();
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Mockito.when(hdfs.getConfiguration()).thenReturn(new Configuration());

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Field f = IcebergMaintenanceProcessor.class.getDeclaredField("icebergPlanWorkerExecutor");
        f.setAccessible(true);
        ExecutorService planExecutor = (ExecutorService) f.get(processor);
        Assertions.assertNotNull(planExecutor);

        Method expire = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runExpireSnapshots", IcebergCatalog.class, Table.class, HdfsEnvironment.class,
                IcebergMaintenanceTaskStats.class);
        expire.setAccessible(true);
        expire.invoke(processor, catalog, icebergTable, hdfs, new IcebergMaintenanceTaskStats());
    }

    @Test
    public void testRunRewriteManifestsPassesExecutorViaContext() throws Exception {
        TestTables.TestTable icebergTable = create(SCHEMA_A, SPEC_A, "maint_exec_rewrite", 2);
        icebergTable.newFastAppend().appendFile(FILE_A).commit();
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Mockito.when(hdfs.getConfiguration()).thenReturn(new Configuration());

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method rewrite = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRewriteManifests", IcebergCatalog.class, Table.class, HdfsEnvironment.class,
                IcebergMaintenanceTaskStats.class);
        rewrite.setAccessible(true);
        rewrite.invoke(processor, catalog, icebergTable, hdfs, new IcebergMaintenanceTaskStats());
    }

    @Test
    public void testRunMaintenanceTaskReassertsInterruptFlagOnCancellation() throws Exception {
        Thread.interrupted(); // start from a clean interrupt state
        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method run = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runMaintenanceTask", String.class, String.class, String.class, String.class, Consumer.class);
        run.setAccessible(true);

        // a cancellation surfaces as an interrupt-derived exception; catching it clears the
        // interrupt flag, so runMaintenanceTask must re-assert it (it never rethrows)
        Consumer<IcebergMaintenanceTaskStats> cancelled = stats -> {
            throw new RuntimeException(new InterruptedException("cancelled"));
        };
        run.invoke(processor, "c", "db", "t", "expire_snapshots", cancelled);
        Assertions.assertTrue(Thread.interrupted(), "interrupt flag must be re-asserted after cancellation");

        // a normal failure must NOT set the interrupt flag, so the next action still runs
        Consumer<IcebergMaintenanceTaskStats> normalFailure = stats -> {
            throw new RuntimeException("boom");
        };
        run.invoke(processor, "c", "db", "t", "expire_snapshots", normalFailure);
        Assertions.assertFalse(Thread.currentThread().isInterrupted(),
                "a normal failure must not leave the thread interrupted");
    }

    @Test
    public void testCleanupSkipsOrphanWhenExpireCancelled() throws Exception {
        Thread.interrupted(); // start from a clean interrupt state
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Table table = Mockito.mock(Table.class);
        // expire's first table call; make it surface a cancellation (interrupt-derived)
        Mockito.when(table.newTransaction()).thenThrow(new RuntimeException(new InterruptedException("cancelled")));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method cleanup = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runTableCleanup", getMaintenanceInfoClass(), Table.class, String.class, String.class);
        cleanup.setAccessible(true);
        Object info = buildMaintenanceInfo("c", catalog, hdfs, 1, 1);

        try {
            cleanup.invoke(processor, info, table, "db", "t");
            // remove_orphan_files must not have started — it would call table.currentSnapshot() first
            Mockito.verify(table, Mockito.never()).currentSnapshot();
        } finally {
            Thread.interrupted(); // clear the flag we deliberately set; don't leak to other tests
        }
    }

    @Test
    public void testCleanupSkipsOrphanOnInterruptedIOException() throws Exception {
        Thread.interrupted(); // start from a clean interrupt state
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Table table = Mockito.mock(Table.class);
        // a Hadoop/classic-IO call interrupted by cancel(true) can surface as InterruptedIOException
        // (flag already cleared); this must be recognized as cancellation
        Mockito.when(table.newTransaction()).thenThrow(new RuntimeException(new InterruptedIOException("interrupted")));

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method cleanup = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runTableCleanup", getMaintenanceInfoClass(), Table.class, String.class, String.class);
        cleanup.setAccessible(true);
        Object info = buildMaintenanceInfo("c", catalog, hdfs, 1, 1);

        try {
            cleanup.invoke(processor, info, table, "db", "t");
            Mockito.verify(table, Mockito.never()).currentSnapshot();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testCleanupRunsOrphanOnSocketTimeout() throws Exception {
        Thread.interrupted(); // start from a clean interrupt state
        IcebergCatalog catalog = Mockito.mock(IcebergCatalog.class);
        HdfsEnvironment hdfs = Mockito.mock(HdfsEnvironment.class);
        Table table = Mockito.mock(Table.class);
        // a transient SocketTimeoutException (extends InterruptedIOException) is NOT a cancellation:
        // orphan removal must still run. currentSnapshot()==null lets orphan early-return cleanly.
        Mockito.when(table.newTransaction()).thenThrow(new RuntimeException(new SocketTimeoutException("read timed out")));
        Mockito.when(table.currentSnapshot()).thenReturn(null);

        IcebergMaintenanceProcessor processor = new IcebergMaintenanceProcessor();
        Method cleanup = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runTableCleanup", getMaintenanceInfoClass(), Table.class, String.class, String.class);
        cleanup.setAccessible(true);
        Object info = buildMaintenanceInfo("c", catalog, hdfs, 1, 1);

        cleanup.invoke(processor, info, table, "db", "t");
        // orphan removal was attempted (it reads currentSnapshot() first), and the thread is not interrupted
        Mockito.verify(table, Mockito.atLeastOnce()).currentSnapshot();
        Assertions.assertFalse(Thread.currentThread().isInterrupted());
    }

    @SuppressWarnings("unchecked")
    private static void setLastMaintenanceTimes(IcebergMaintenanceProcessor p, long cleanup, long rewrite)
            throws Exception {
        Field mapField = IcebergMaintenanceProcessor.class.getDeclaredField("maintenanceInfoMap");
        mapField.setAccessible(true);
        ConcurrentHashMap<String, ?> map = (ConcurrentHashMap<String, ?>) mapField.get(p);
        for (Object info : map.values()) {
            Field lc = info.getClass().getDeclaredField("lastCleanupTimeMillis");
            lc.setAccessible(true);
            lc.setLong(info, cleanup);
            Field lr = info.getClass().getDeclaredField("lastRewriteTimeMillis");
            lr.setAccessible(true);
            lr.setLong(info, rewrite);
        }
    }
}
