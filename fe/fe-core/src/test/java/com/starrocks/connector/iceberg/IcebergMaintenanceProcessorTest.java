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
import com.starrocks.common.Pair;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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

public class IcebergMaintenanceProcessorTest extends TableTestBase {

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
                "runExpireSnapshots", IcebergCatalog.class, Table.class, HdfsEnvironment.class);
        expire.setAccessible(true);
        expire.invoke(processor, catalog, icebergTable, hdfs);

        Method orphan = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRemoveOrphanFiles", IcebergCatalog.class, Table.class, HdfsEnvironment.class);
        orphan.setAccessible(true);
        orphan.invoke(processor, catalog, icebergTable, hdfs);

        Method rewrite = IcebergMaintenanceProcessor.class.getDeclaredMethod(
                "runRewriteManifests", IcebergCatalog.class, Table.class, HdfsEnvironment.class);
        rewrite.setAccessible(true);
        rewrite.invoke(processor, catalog, icebergTable, hdfs);
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
