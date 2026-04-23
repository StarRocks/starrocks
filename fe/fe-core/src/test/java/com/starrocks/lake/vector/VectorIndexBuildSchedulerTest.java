// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.lake.vector;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.proto.BuildVectorIndexRequest;
import com.starrocks.proto.BuildVectorIndexResponse;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class VectorIndexBuildSchedulerTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private LocalMetastore localMetastore;
    @Mocked
    private WarehouseManager warehouseManager;

    private VectorIndexBuildScheduler scheduler;

    @BeforeEach
    public void setUp() {
        scheduler = new VectorIndexBuildScheduler();
    }

    // ========== Helper methods ==========

    private Map<Long, VectorIndexBuildTask> getRunningTasks() {
        return scheduler.getRunningTasksForTest();
    }

    private ConcurrentHashMap<Long, Long> getPendingTablets() {
        return scheduler.getPendingTabletsForTest();
    }

    private LakeTable mockLakeTable(boolean asyncVectorIndex) {
        LakeTable table = Mockito.mock(LakeTable.class);
        Mockito.when(table.isCloudNativeTableOrMaterializedView()).thenReturn(true);
        if (asyncVectorIndex) {
            Index vectorIndex = Mockito.mock(Index.class);
            Mockito.when(vectorIndex.getIndexType()).thenReturn(IndexDef.IndexType.VECTOR);
            Map<String, String> props = new HashMap<>();
            props.put("index_build_mode", "async");
            Mockito.when(vectorIndex.getProperties()).thenReturn(props);
            Mockito.when(table.getIndexes()).thenReturn(Lists.newArrayList(vectorIndex));
        } else {
            Mockito.when(table.getIndexes()).thenReturn(Collections.emptyList());
        }
        return table;
    }

    private Database mockDatabase(LakeTable... tables) {
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getTables()).thenReturn(Lists.newArrayList(tables));
        return db;
    }

    private void setupRecoveryScanExpectations(Database db) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                localMetastore.getDbIds();
                result = Lists.newArrayList(10001L);
                localMetastore.getDb(10001L);
                result = db;
            }
        };
    }

    private VectorIndexBuildTask createTaskWithFuture(long tabletId, long version,
                                                       CompletableFuture<BuildVectorIndexResponse> future) {
        ComputeNode node = Mockito.mock(ComputeNode.class);
        VectorIndexBuildTask task = new VectorIndexBuildTask(node, tabletId, version, 0);
        try {
            java.lang.reflect.Field futureField = VectorIndexBuildTask.class.getDeclaredField("future");
            futureField.setAccessible(true);
            futureField.set(task, future);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return task;
    }

    private VectorIndexBuildTask createTaskWithStartTime(long tabletId, long version, long startTimeMs) {
        ComputeNode node = Mockito.mock(ComputeNode.class);
        VectorIndexBuildTask task = new VectorIndexBuildTask(node, tabletId, version, 0);
        try {
            java.lang.reflect.Field startField = VectorIndexBuildTask.class.getDeclaredField("startTimeMs");
            startField.setAccessible(true);
            startField.set(task, startTimeMs);
            java.lang.reflect.Field futureField = VectorIndexBuildTask.class.getDeclaredField("future");
            futureField.setAccessible(true);
            futureField.set(task, new CompletableFuture<>());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return task;
    }

    private void setupFindLakeTabletExpectations(long tabletId, LakeTablet tablet,
                                                  long dbId, long tableId, long partitionId, long indexId) {
        TabletInvertedIndex invertedIndex = Mockito.mock(TabletInvertedIndex.class);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, null, true);
        Mockito.when(invertedIndex.getTabletMeta(tabletId)).thenReturn(tabletMeta);

        Database db = Mockito.mock(Database.class);
        LakeTable table = Mockito.mock(LakeTable.class);
        Mockito.when(db.getTable(tableId)).thenReturn(table);
        PhysicalPartition partition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(table.getPhysicalPartition(partitionId)).thenReturn(partition);
        MaterializedIndex matIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(partition.getIndex(indexId)).thenReturn(matIndex);
        Mockito.when(matIndex.getTablet(tabletId)).thenReturn(tablet);

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                globalStateMgr.getTabletInvertedIndex();
                result = invertedIndex;
                minTimes = 0;
                globalStateMgr.getLocalMetastore();
                result = localMetastore;
                minTimes = 0;
                localMetastore.getDb(dbId);
                result = db;
                minTimes = 0;
            }
        };
    }

    // ========== addPendingTablet tests ==========

    @Test
    public void testAddPendingTablet() {
        scheduler.addPendingTablet(1001L, 5L);
        Assertions.assertEquals(5L, getPendingTablets().get(1001L));
    }

    @Test
    public void testAddPendingTabletMergeMax() {
        scheduler.addPendingTablet(1001L, 5L);
        scheduler.addPendingTablet(1001L, 8L);
        Assertions.assertEquals(8L, getPendingTablets().get(1001L));
    }

    @Test
    public void testAddPendingTabletNoRegression() {
        scheduler.addPendingTablet(1001L, 8L);
        scheduler.addPendingTablet(1001L, 5L);
        Assertions.assertEquals(8L, getPendingTablets().get(1001L));
    }

    // ========== Recovery scan tests ==========

    @Test
    public void testRecoveryScanFindsLaggingTablets() {
        LakeTable table = mockLakeTable(true);
        Database db = mockDatabase(table);

        LakeTablet tablet1 = new LakeTablet(2001L);
        tablet1.setVectorIndexBuiltVersion(3L);
        LakeTablet tablet2 = new LakeTablet(2002L);

        PhysicalPartition partition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(partition.getVisibleVersion()).thenReturn(5L);
        MaterializedIndex matIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(matIndex.getTablets()).thenReturn(Lists.newArrayList(tablet1, tablet2));
        Mockito.when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL))
                .thenReturn(Lists.newArrayList(matIndex));
        Mockito.when(table.getPhysicalPartitions()).thenReturn(Lists.newArrayList(partition));

        setupRecoveryScanExpectations(db);
        scheduler.recoveryScan();

        ConcurrentHashMap<Long, Long> pending = getPendingTablets();
        Assertions.assertEquals(5L, pending.get(2001L));
        Assertions.assertEquals(5L, pending.get(2002L));
    }

    @Test
    public void testRecoveryScanSkipsBuiltTablets() {
        LakeTable table = mockLakeTable(true);
        Database db = mockDatabase(table);

        LakeTablet tablet = new LakeTablet(2001L);
        tablet.setVectorIndexBuiltVersion(5L);

        PhysicalPartition partition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(partition.getVisibleVersion()).thenReturn(5L);
        MaterializedIndex matIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(matIndex.getTablets()).thenReturn(Lists.newArrayList(tablet));
        Mockito.when(partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL))
                .thenReturn(Lists.newArrayList(matIndex));
        Mockito.when(table.getPhysicalPartitions()).thenReturn(Lists.newArrayList(partition));

        setupRecoveryScanExpectations(db);
        scheduler.recoveryScan();

        Assertions.assertTrue(getPendingTablets().isEmpty());
    }

    @Test
    public void testRecoveryScanSkipsNonAsyncTables() {
        LakeTable table = mockLakeTable(false);
        Database db = mockDatabase(table);

        setupRecoveryScanExpectations(db);
        scheduler.recoveryScan();

        Assertions.assertTrue(getPendingTablets().isEmpty());
    }

    @Test
    public void testRecoveryScanSkipsVersion1() {
        LakeTable table = mockLakeTable(true);
        Database db = mockDatabase(table);

        PhysicalPartition partition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(partition.getVisibleVersion()).thenReturn(1L);
        Mockito.when(table.getPhysicalPartitions()).thenReturn(Lists.newArrayList(partition));

        setupRecoveryScanExpectations(db);
        scheduler.recoveryScan();

        Assertions.assertTrue(getPendingTablets().isEmpty());
    }

    // ========== checkRunningTasks tests ==========

    @Test
    public void testCheckRunningTasksSuccess() {
        long tabletId = 3001L;
        LakeTablet tablet = new LakeTablet(tabletId);

        BuildVectorIndexResponse response = new BuildVectorIndexResponse();
        response.newBuiltVersion = 7L;
        CompletableFuture<BuildVectorIndexResponse> future = CompletableFuture.completedFuture(response);
        VectorIndexBuildTask task = createTaskWithFuture(tabletId, 7L, future);
        getRunningTasks().put(tabletId, task);

        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        scheduler.checkRunningTasks();

        Assertions.assertTrue(getRunningTasks().isEmpty());
        Assertions.assertEquals(7L, tablet.getVectorIndexBuiltVersion());
    }

    @Test
    public void testCheckRunningTasksFailure() {
        long tabletId = 3001L;
        CompletableFuture<BuildVectorIndexResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("RPC failed"));
        VectorIndexBuildTask task = createTaskWithFuture(tabletId, 7L, future);
        getRunningTasks().put(tabletId, task);

        scheduler.checkRunningTasks();

        Assertions.assertTrue(getRunningTasks().isEmpty());
    }

    @Test
    public void testCheckRunningTasksSkipsInProgress() {
        long tabletId = 3001L;
        VectorIndexBuildTask task = createTaskWithStartTime(tabletId, 7L, System.currentTimeMillis());
        getRunningTasks().put(tabletId, task);

        scheduler.checkRunningTasks();

        Assertions.assertEquals(1, getRunningTasks().size());
    }

    @Test
    public void testCheckRunningTasksNullBuiltVersion() {
        long tabletId = 3001L;
        LakeTablet tablet = new LakeTablet(tabletId);

        BuildVectorIndexResponse response = new BuildVectorIndexResponse();
        response.newBuiltVersion = null;
        CompletableFuture<BuildVectorIndexResponse> future = CompletableFuture.completedFuture(response);
        VectorIndexBuildTask task = createTaskWithFuture(tabletId, 7L, future);
        getRunningTasks().put(tabletId, task);

        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        scheduler.checkRunningTasks();

        Assertions.assertTrue(getRunningTasks().isEmpty());
        Assertions.assertEquals(0L, tablet.getVectorIndexBuiltVersion());
    }

    // ========== checkRunningTaskTimeout tests ==========

    @Test
    public void testCheckRunningTaskTimeout() {
        long tabletId = 3001L;
        long oldStartTime = System.currentTimeMillis() - VectorIndexBuildScheduler.BUILD_TIMEOUT_MS - 1000;
        VectorIndexBuildTask task = createTaskWithStartTime(tabletId, 7L, oldStartTime);
        getRunningTasks().put(tabletId, task);

        scheduler.checkRunningTaskTimeout();

        Assertions.assertTrue(getRunningTasks().isEmpty());
        Assertions.assertEquals(7L, getPendingTablets().get(tabletId));
    }

    @Test
    public void testCheckRunningTaskTimeoutKeepsRecent() {
        long tabletId = 3001L;
        VectorIndexBuildTask task = createTaskWithStartTime(tabletId, 7L, System.currentTimeMillis());
        getRunningTasks().put(tabletId, task);

        scheduler.checkRunningTaskTimeout();

        Assertions.assertEquals(1, getRunningTasks().size());
        Assertions.assertTrue(getPendingTablets().isEmpty());
    }

    // ========== scheduleFromPending tests ==========

    @Test
    public void testScheduleFromPendingSkipsAlreadyBuilt() {
        long tabletId = 3001L;
        scheduler.addPendingTablet(tabletId, 5L);

        LakeTablet tablet = new LakeTablet(tabletId);
        tablet.setVectorIndexBuiltVersion(5L);

        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        scheduler.scheduleFromPending();

        Assertions.assertTrue(getPendingTablets().isEmpty());
        Assertions.assertTrue(getRunningTasks().isEmpty());
    }

    @Test
    public void testScheduleFromPendingSkipsAlreadyRunning() {
        long tabletId = 3001L;
        scheduler.addPendingTablet(tabletId, 5L);

        VectorIndexBuildTask existing = createTaskWithStartTime(tabletId, 5L, System.currentTimeMillis());
        getRunningTasks().put(tabletId, existing);

        scheduler.scheduleFromPending();

        Assertions.assertEquals(5L, getPendingTablets().get(tabletId));
    }

    // ========== hasAsyncVectorIndex tests ==========

    @Test
    public void testHasAsyncVectorIndexTrue() {
        LakeTable table = mockLakeTable(true);
        Assertions.assertTrue(VectorIndexBuildScheduler.hasAsyncVectorIndex(table));
    }

    @Test
    public void testHasAsyncVectorIndexFalse() {
        LakeTable table = mockLakeTable(false);
        Assertions.assertFalse(VectorIndexBuildScheduler.hasAsyncVectorIndex(table));
    }

    @Test
    public void testHasAsyncVectorIndexNullIndexes() {
        LakeTable table = Mockito.mock(LakeTable.class);
        Mockito.when(table.getIndexes()).thenReturn(null);
        Assertions.assertFalse(VectorIndexBuildScheduler.hasAsyncVectorIndex(table));
    }

    // ========== LakeTablet builtVersion tests ==========

    @Test
    public void testLakeTabletBuiltVersionDefault() {
        LakeTablet tablet = new LakeTablet(1001L);
        Assertions.assertEquals(0L, tablet.getVectorIndexBuiltVersion());
    }

    @Test
    public void testLakeTabletBuiltVersionSet() {
        LakeTablet tablet = new LakeTablet(1001L);
        tablet.setVectorIndexBuiltVersion(5L);
        Assertions.assertEquals(5L, tablet.getVectorIndexBuiltVersion());
    }

    @Test
    public void testLakeTabletBuiltVersionMaxSemantics() {
        LakeTablet tablet = new LakeTablet(1001L);
        tablet.setVectorIndexBuiltVersion(5L);
        tablet.setVectorIndexBuiltVersion(3L);
        Assertions.assertEquals(5L, tablet.getVectorIndexBuiltVersion());
        tablet.setVectorIndexBuiltVersion(8L);
        Assertions.assertEquals(8L, tablet.getVectorIndexBuiltVersion());
    }

    @Test
    public void testLakeTabletBuiltVersionSerialization() {
        LakeTablet tablet = new LakeTablet(1001L);
        tablet.setVectorIndexBuiltVersion(7L);

        com.google.gson.Gson gson = new com.google.gson.Gson();
        String json = gson.toJson(tablet);
        LakeTablet deserialized = gson.fromJson(json, LakeTablet.class);

        Assertions.assertEquals(7L, deserialized.getVectorIndexBuiltVersion());
    }

    @Test
    public void testLakeTabletBuiltVersionDeserializeOldFormat() {
        String json = "{\"id\":1001,\"dataSize\":100,\"rowCount\":10}";
        com.google.gson.Gson gson = new com.google.gson.Gson();
        LakeTablet deserialized = gson.fromJson(json, LakeTablet.class);

        Assertions.assertEquals(0L, deserialized.getVectorIndexBuiltVersion());
    }

    // ========== onPublishComplete tests ==========

    private static VectorIndexBuildInfoPB infoOf(long tabletId, long version) {
        VectorIndexBuildInfoPB info = new VectorIndexBuildInfoPB();
        info.tabletId = tabletId;
        info.version = version;
        return info;
    }

    // GlobalStateMgr.getVectorIndexBuildScheduler() returns a Thread subclass which JMockit cannot
    // auto-mock from an Expectations block. MockUp lets us override just that one method.
    private void mockGetScheduler(VectorIndexBuildScheduler returnValue) {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public VectorIndexBuildScheduler getVectorIndexBuildScheduler() {
                return returnValue;
            }
        };
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testOnPublishCompleteEnqueuesEachInfo() {
        mockGetScheduler(scheduler);
        List<VectorIndexBuildInfoPB> infos = new ArrayList<>();
        infos.add(infoOf(4001L, 5L));
        infos.add(infoOf(4002L, 7L));

        VectorIndexBuildScheduler.onPublishComplete(infos);

        Assertions.assertEquals(5L, getPendingTablets().get(4001L));
        Assertions.assertEquals(7L, getPendingTablets().get(4002L));
    }

    @Test
    public void testOnPublishCompleteSkipsNullAndEmpty() {
        // No GlobalStateMgr expectation needed — early return on null/empty must not touch state.
        VectorIndexBuildScheduler.onPublishComplete(null);
        VectorIndexBuildScheduler.onPublishComplete(Collections.emptyList());
        Assertions.assertTrue(getPendingTablets().isEmpty());
    }

    @Test
    public void testOnPublishCompleteSkipsInfoWithNullFields() {
        mockGetScheduler(scheduler);
        VectorIndexBuildInfoPB missingTablet = new VectorIndexBuildInfoPB();
        missingTablet.version = 1L;
        VectorIndexBuildInfoPB missingVersion = new VectorIndexBuildInfoPB();
        missingVersion.tabletId = 9L;

        VectorIndexBuildScheduler.onPublishComplete(Lists.newArrayList(missingTablet, missingVersion));

        Assertions.assertTrue(getPendingTablets().isEmpty());
    }

    @Test
    public void testOnPublishCompleteHandlesNullScheduler() {
        mockGetScheduler(null);
        // Must silently no-op when scheduler is not initialized (FE startup race window).
        Assertions.assertDoesNotThrow(
                () -> VectorIndexBuildScheduler.onPublishComplete(Lists.newArrayList(infoOf(1L, 1L))));
    }

    // ========== getBuiltVersion tests ==========

    @Test
    public void testGetBuiltVersionReturnsValueWhenSet() {
        long tabletId = 5001L;
        LakeTablet tablet = new LakeTablet(tabletId);
        tablet.setVectorIndexBuiltVersion(9L);
        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        Assertions.assertEquals(9L, scheduler.getBuiltVersion(tabletId));
    }

    @Test
    public void testGetBuiltVersionReturnsNullWhenZero() {
        long tabletId = 5002L;
        LakeTablet tablet = new LakeTablet(tabletId);
        // builtVersion stays 0 — should map to null (publish path uses null as "no entry")
        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        Assertions.assertNull(scheduler.getBuiltVersion(tabletId));
    }

    @Test
    public void testGetBuiltVersionReturnsNullWhenTabletMissing() {
        // No expectations: TabletInvertedIndex/metastore mocks return null,
        // findLakeTablet returns null, getBuiltVersion returns null.
        Assertions.assertNull(scheduler.getBuiltVersion(9999L));
    }

    // ========== cleanupStaleEntries tests ==========

    @Test
    public void testCleanupStaleEntriesRemovesExpiredCooldownAndOrphanPreferred() throws Exception {
        // Stale cooldown (already expired) + preferred node not referenced anywhere — both must go.
        Map<Long, Long> cooldownUntil = getMap("cooldownUntil");
        Map<Long, ComputeNode> preferredNodes = getMap("preferredNodes");

        cooldownUntil.put(6001L, System.currentTimeMillis() - 1000);
        long futureTs = System.currentTimeMillis() + 60_000;
        cooldownUntil.put(6002L, futureTs);

        ComputeNode orphanNode = Mockito.mock(ComputeNode.class);
        ComputeNode keepNode = Mockito.mock(ComputeNode.class);
        preferredNodes.put(7001L, orphanNode);
        preferredNodes.put(7002L, keepNode);
        scheduler.addPendingTablet(7002L, 1L); // 7002 still pending → keep

        scheduler.cleanupStaleEntries();

        Assertions.assertFalse(cooldownUntil.containsKey(6001L), "expired cooldown removed");
        Assertions.assertEquals(futureTs, cooldownUntil.get(6002L), "future cooldown kept");
        Assertions.assertFalse(preferredNodes.containsKey(7001L), "orphan preferred removed");
        Assertions.assertTrue(preferredNodes.containsKey(7002L), "preferred for pending tablet kept");
    }

    @SuppressWarnings("unchecked")
    private <V> Map<Long, V> getMap(String fieldName) throws Exception {
        java.lang.reflect.Field f = VectorIndexBuildScheduler.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return (Map<Long, V>) f.get(scheduler);
    }

    // ========== scheduleFromPending — full-path / branch tests ==========

    @Test
    public void testScheduleFromPendingDispatchesTask(@Mocked BrpcProxy brpcProxy,
                                                      @Mocked LakeService lakeService) throws Exception {
        long tabletId = 8001L;
        long version = 12L;
        LakeTablet tablet = new LakeTablet(tabletId);
        scheduler.addPendingTablet(tabletId, version);
        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        ComputeNode node = Mockito.mock(ComputeNode.class);
        Mockito.when(node.getId()).thenReturn(101L);
        Mockito.when(node.getHost()).thenReturn("127.0.0.1");
        Mockito.when(node.getBrpcPort()).thenReturn(8060);
        ComputeResource resource = Mockito.mock(ComputeResource.class);
        new Expectations() {
            {
                globalStateMgr.getWarehouseMgr();
                result = warehouseManager;
                minTimes = 0;
                warehouseManager.getVectorIndexBuildComputeResource(anyLong);
                result = resource;
                warehouseManager.getComputeNodeAssignedToTablet(resource, tabletId);
                result = node;
                BrpcProxy.getLakeService("127.0.0.1", 8060);
                result = lakeService;
                lakeService.buildVectorIndex((BuildVectorIndexRequest) any);
                result = new CompletableFuture<BuildVectorIndexResponse>(); // pending future, task remains running
            }
        };

        scheduler.scheduleFromPending();

        Assertions.assertTrue(getPendingTablets().isEmpty(), "tablet moved out of pending");
        Assertions.assertTrue(getRunningTasks().containsKey(tabletId), "task added to runningTasks");
    }

    @Test
    public void testScheduleFromPendingPrefersAliveNode(@Mocked BrpcProxy brpcProxy,
                                                        @Mocked LakeService lakeService) throws Exception {
        long tabletId = 8002L;
        long version = 5L;
        LakeTablet tablet = new LakeTablet(tabletId);
        scheduler.addPendingTablet(tabletId, version);
        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        ComputeNode preferred = Mockito.mock(ComputeNode.class);
        Mockito.when(preferred.isAlive()).thenReturn(true);
        Mockito.when(preferred.getId()).thenReturn(202L);
        Mockito.when(preferred.getHost()).thenReturn("10.0.0.5");
        Mockito.when(preferred.getBrpcPort()).thenReturn(8060);
        getMap("preferredNodes").put(tabletId, preferred);

        new Expectations() {
            {
                BrpcProxy.getLakeService("10.0.0.5", 8060);
                result = lakeService;
                lakeService.buildVectorIndex((BuildVectorIndexRequest) any);
                result = new CompletableFuture<BuildVectorIndexResponse>();
                // pickComputeNode must NOT be invoked — preferred node is alive.
                warehouseManager.getComputeNodeAssignedToTablet((ComputeResource) any, anyLong);
                times = 0;
            }
        };

        scheduler.scheduleFromPending();

        Assertions.assertTrue(getRunningTasks().containsKey(tabletId));
        Assertions.assertFalse(getMap("preferredNodes").containsKey(tabletId), "preferred consumed");
    }

    @Test
    public void testScheduleFromPendingSkipsTabletInActiveCooldown() throws Exception {
        long tabletId = 8003L;
        scheduler.addPendingTablet(tabletId, 5L);
        long futureTs = System.currentTimeMillis() + 60_000;
        getMap("cooldownUntil").put(tabletId, futureTs);

        scheduler.scheduleFromPending();

        Assertions.assertEquals(5L, getPendingTablets().get(tabletId), "still pending");
        Assertions.assertTrue(getRunningTasks().isEmpty(), "no task dispatched");
    }

    @Test
    public void testScheduleFromPendingClearsExpiredCooldownAndProceeds() throws Exception {
        long tabletId = 8004L;
        scheduler.addPendingTablet(tabletId, 5L);
        getMap("cooldownUntil").put(tabletId, System.currentTimeMillis() - 1000);
        // No findLakeTablet mock → tablet null → orphan-removed; this drives the "expired cooldown
        // is cleared, then proceed with normal logic" path which currently sees tablet missing.

        scheduler.scheduleFromPending();

        Assertions.assertFalse(getPendingTablets().containsKey(tabletId), "orphan tablet removed");
        Assertions.assertFalse(getMap("cooldownUntil").containsKey(tabletId), "expired cooldown cleared");
    }

    // ========== runAfterCatalogReady tests ==========

    @Test
    public void testRunAfterCatalogReadyOnFollowerResetsRecoveryFlag() throws Exception {
        // Force the leader check to return false; daemon should bail out and reset recoveryScanDone.
        java.lang.reflect.Field f = VectorIndexBuildScheduler.class.getDeclaredField("recoveryScanDone");
        f.setAccessible(true);
        f.setBoolean(scheduler, true);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }
        };
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
            }
        };

        java.lang.reflect.Method m =
                VectorIndexBuildScheduler.class.getDeclaredMethod("runAfterCatalogReady");
        m.setAccessible(true);
        m.invoke(scheduler);

        Assertions.assertFalse(f.getBoolean(scheduler), "recoveryScanDone reset on non-leader");
    }

    @Test
    public void testRunAfterCatalogReadyOnLeaderRunsRecoveryThenSkips() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }
            @Mock
            public LocalMetastore getLocalMetastore() {
                return localMetastore;
            }
        };
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;
                localMetastore.getDbIds();
                result = Collections.emptyList();
            }
        };

        java.lang.reflect.Method m =
                VectorIndexBuildScheduler.class.getDeclaredMethod("runAfterCatalogReady");
        m.setAccessible(true);

        java.lang.reflect.Field f = VectorIndexBuildScheduler.class.getDeclaredField("recoveryScanDone");
        f.setAccessible(true);

        m.invoke(scheduler);
        Assertions.assertTrue(f.getBoolean(scheduler), "recoveryScan ran on first leader tick");

        // Second invocation must NOT re-scan; flag stays true.
        m.invoke(scheduler);
        Assertions.assertTrue(f.getBoolean(scheduler));
    }

    // ========== checkRunningTasks — partial-build branch ==========

    @Test
    public void testCheckRunningTasksPartialReEnqueues() {
        long tabletId = 9101L;
        LakeTablet tablet = new LakeTablet(tabletId);
        tablet.setVectorIndexBuiltVersion(2L);

        BuildVectorIndexResponse response = new BuildVectorIndexResponse();
        response.newBuiltVersion = 5L; // partial — target was 10
        CompletableFuture<BuildVectorIndexResponse> future = CompletableFuture.completedFuture(response);
        VectorIndexBuildTask task = createTaskWithFuture(tabletId, 10L, future);
        getRunningTasks().put(tabletId, task);

        setupFindLakeTabletExpectations(tabletId, tablet, 1L, 2L, 3L, 4L);

        scheduler.checkRunningTasks();

        Assertions.assertTrue(getRunningTasks().isEmpty(), "task removed");
        Assertions.assertEquals(5L, tablet.getVectorIndexBuiltVersion(), "builtVersion updated to partial");
        Assertions.assertEquals(10L, getPendingTablets().get(tabletId), "tablet re-enqueued for next round");
    }

    @Test
    public void testCheckRunningTasksDedupRejection() {
        long tabletId = 9102L;
        BuildVectorIndexResponse response = new BuildVectorIndexResponse();
        com.starrocks.proto.StatusPB status = new com.starrocks.proto.StatusPB();
        status.statusCode = com.starrocks.thrift.TStatusCode.RESOURCE_BUSY.getValue();
        status.errorMsgs = Lists.newArrayList("busy");
        response.status = status;
        CompletableFuture<BuildVectorIndexResponse> future = CompletableFuture.completedFuture(response);
        VectorIndexBuildTask task = createTaskWithFuture(tabletId, 7L, future);
        getRunningTasks().put(tabletId, task);

        scheduler.checkRunningTasks();

        Assertions.assertTrue(getRunningTasks().isEmpty(), "task removed");
        Assertions.assertEquals(7L, getPendingTablets().get(tabletId), "tablet re-enqueued (cooldown)");
    }

    @Test
    public void testScheduleFromPendingStopsAtMaxConcurrent() {
        // Pre-fill runningTasks to MAX_CONCURRENT_TASKS so the loop breaks immediately.
        for (int i = 0; i < VectorIndexBuildScheduler.MAX_CONCURRENT_TASKS; i++) {
            ComputeNode node = Mockito.mock(ComputeNode.class);
            getRunningTasks().put((long) (10_000 + i),
                    createTaskWithStartTime(10_000 + i, 1L, System.currentTimeMillis()));
        }
        scheduler.addPendingTablet(9001L, 5L);

        scheduler.scheduleFromPending();

        // Pending tablet must NOT be promoted to running because we're at the cap.
        Assertions.assertEquals(5L, getPendingTablets().get(9001L));
    }
}
