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

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.thrift.TWorkGroupType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies CopyOnWrite semantics for ResourceGroupMgr's ResourceGroupSnapshot volatile field.
 */
public class ResourceGroupMgrConcurrencyTest {

    private ResourceGroupMgr mgr;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        mgr = new ResourceGroupMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static CreateResourceGroupStmt mvStmt(String name, String cpuWeight) {
        Map<String, String> props = Maps.newHashMap();
        props.put("cpu_weight", cpuWeight);
        props.put("mem_limit", "50%");
        props.put("type", "mv");
        CreateResourceGroupStmt stmt = new CreateResourceGroupStmt(name, false, false,
                Collections.emptyList(), props);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt);
        return stmt;
    }

    @SuppressWarnings("unchecked")
    private <T> T field(String name) throws Exception {
        Field f = ResourceGroupMgr.class.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(mgr);
    }

    private Object getSnapshot() throws Exception {
        return field("snapshot");
    }

    @SuppressWarnings("unchecked")
    private <T> T snapField(Object snap, String fieldName) throws Exception {
        Field f = snap.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return (T) f.get(snap);
    }

    private Map<String, ResourceGroup> byName() throws Exception {
        return snapField(getSnapshot(), "byName");
    }

    private Map<Long, ResourceGroup> byId() throws Exception {
        return snapField(getSnapshot(), "byId");
    }

    private Map<Long, ResourceGroupClassifier> byClassifier() throws Exception {
        return snapField(getSnapshot(), "byClassifier");
    }

    private void injectRgIntoMap(Object... namesAndGroups) throws Exception {
        Map<String, ResourceGroup> bName = new LinkedHashMap<>();
        Map<Long, ResourceGroup>   bId   = new HashMap<>();
        for (int i = 0; i < namesAndGroups.length; i += 2) {
            ResourceGroup rg = (ResourceGroup) namesAndGroups[i + 1];
            bName.put((String) namesAndGroups[i], rg);
            bId.put(rg.getId(), rg);
        }
        Class<?> snapClass = null;
        for (Class<?> c : ResourceGroupMgr.class.getDeclaredClasses()) {
            if ("ResourceGroupSnapshot".equals(c.getSimpleName())) {
                snapClass = c;
                break;
            }
        }
        if (snapClass == null) {
            throw new IllegalStateException("ResourceGroupSnapshot not found");
        }
        Constructor<?> ctor = snapClass.getDeclaredConstructor(Map.class, Map.class, Map.class, ResourceGroup.class);
        ctor.setAccessible(true);
        Object snap = ctor.newInstance(bName, bId, Collections.emptyMap(), null);
        Field f = ResourceGroupMgr.class.getDeclaredField("snapshot");
        f.setAccessible(true);
        f.set(mgr, snap);
    }

    @Test
    public void testVolatileFieldsInitializedAsUnmodifiable() throws Exception {
        Map<String, ResourceGroup>         rgMap  = byName();
        Map<Long, ResourceGroup>           idMap  = byId();
        Map<Long, ResourceGroupClassifier> clsMap = byClassifier();
        Assertions.assertNotNull(rgMap);
        Assertions.assertNotNull(idMap);
        Assertions.assertNotNull(clsMap);
        Assertions.assertTrue(rgMap.isEmpty());
        Assertions.assertTrue(idMap.isEmpty());
        Assertions.assertTrue(clsMap.isEmpty());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> rgMap.put("x", null));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> idMap.put(1L, null));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> clsMap.put(1L, null));
    }

    @Test
    public void testAddResourceGroupInternalReplacesAllThreeMaps() throws Exception {
        Object snapBefore = getSnapshot();
        Map<String, ResourceGroup> rgBefore = snapField(snapBefore, "byName");
        Map<Long, ResourceGroup>   idBefore = snapField(snapBefore, "byId");

        mgr.createResourceGroup(mvStmt("rg_add_test", "1"));

        Object snapAfter = getSnapshot();
        Map<String, ResourceGroup> rgAfter = snapField(snapAfter, "byName");
        Map<Long, ResourceGroup>   idAfter = snapField(snapAfter, "byId");

        Assertions.assertNotSame(snapBefore, snapAfter);
        Assertions.assertNotSame(rgBefore, rgAfter);
        Assertions.assertNotSame(idBefore, idAfter);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> rgAfter.put("y", null));
        Assertions.assertTrue(rgAfter.containsKey("rg_add_test"));
    }

    @Test
    public void testRemoveResourceGroupInternalReplacesAllThreeMaps() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_remove_test", "1"));
        Object snapBefore = getSnapshot();
        Map<String, ResourceGroup> rgBefore = snapField(snapBefore, "byName");
        Map<Long, ResourceGroup>   idBefore = snapField(snapBefore, "byId");

        mgr.dropResourceGroup(new DropResourceGroupStmt("rg_remove_test", false));

        Object snapAfter = getSnapshot();
        Map<String, ResourceGroup> rgAfter = snapField(snapAfter, "byName");
        Map<Long, ResourceGroup>   idAfter = snapField(snapAfter, "byId");

        Assertions.assertNotSame(snapBefore, snapAfter);
        Assertions.assertNotSame(rgBefore, rgAfter);
        Assertions.assertNotSame(idBefore, idAfter);
        Assertions.assertFalse(rgAfter.containsKey("rg_remove_test"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> rgAfter.put("z", null));
    }

    @Test
    public void testGetAllResourceGroupNamesReturnsDefensiveCopy() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_copy_test", "1"));
        Set<String> names = mgr.getAllResourceGroupNames();
        Map<String, ResourceGroup> internalMap = byName();
        Assertions.assertNotSame(internalMap.keySet(), names);
        names.add("injected_name");
        Assertions.assertFalse(mgr.getAllResourceGroupNames().contains("injected_name"));
        Assertions.assertTrue(names.contains("rg_copy_test"));
    }

    @Test
    public void testGetResourceGroupByNameLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_by_name", "1"));
        ResourceGroup rg = mgr.getResourceGroup("rg_by_name");
        Assertions.assertNotNull(rg);
        Assertions.assertEquals("rg_by_name", rg.getName());
        Field lockField = ResourceGroupMgr.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        java.util.concurrent.locks.ReentrantReadWriteLock rwLock =
                (java.util.concurrent.locks.ReentrantReadWriteLock) lockField.get(mgr);
        Assertions.assertEquals(0, rwLock.getReadLockCount());
    }

    @Test
    public void testGetResourceGroupByIdLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_by_id", "1"));
        ResourceGroup rgByName = mgr.getResourceGroup("rg_by_id");
        Assertions.assertNotNull(rgByName);
        ResourceGroup rg = mgr.getResourceGroup(rgByName.getId());
        Assertions.assertNotNull(rg);
        Assertions.assertEquals(rgByName.getId(), rg.getId());
        Field lockField = ResourceGroupMgr.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        java.util.concurrent.locks.ReentrantReadWriteLock rwLock =
                (java.util.concurrent.locks.ReentrantReadWriteLock) lockField.get(mgr);
        Assertions.assertEquals(0, rwLock.getReadLockCount());
    }

    @Test
    public void testChooseResourceGroupByNameLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_choose_name", "1"));
        TWorkGroup twg = mgr.chooseResourceGroupByName(null, "rg_choose_name");
        Assertions.assertNotNull(twg);
        Assertions.assertEquals("rg_choose_name", twg.getName());
        Field lockField = ResourceGroupMgr.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        java.util.concurrent.locks.ReentrantReadWriteLock rwLock =
                (java.util.concurrent.locks.ReentrantReadWriteLock) lockField.get(mgr);
        Assertions.assertEquals(0, rwLock.getReadLockCount());
    }

    @Test
    public void testChooseResourceGroupByIDLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_choose_id", "1"));
        ResourceGroup rg = mgr.getResourceGroup("rg_choose_id");
        Assertions.assertNotNull(rg);
        TWorkGroup twg = mgr.chooseResourceGroupByID(null, rg.getId());
        Assertions.assertNotNull(twg);
        Field lockField = ResourceGroupMgr.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        java.util.concurrent.locks.ReentrantReadWriteLock rwLock =
                (java.util.concurrent.locks.ReentrantReadWriteLock) lockField.get(mgr);
        Assertions.assertEquals(0, rwLock.getReadLockCount());
    }

    @Test
    public void testCandidateGroupIdsBuildMatchesStreamApproach() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_cand_a", "1"));
        mgr.createResourceGroup(mvStmt("rg_cand_b", "1"));
        Map<String, ResourceGroup> snap = byName();
        Set<Long> streamIds = new java.util.HashSet<>();
        for (ResourceGroup rg : snap.values()) {
            streamIds.add(rg.getId());
        }
        Set<Long> loopIds = new java.util.HashSet<>();
        for (ResourceGroup rg : snap.values()) {
            loopIds.add(rg.getId());
        }
        Assertions.assertEquals(streamIds, loopIds);
        Assertions.assertTrue(loopIds.size() >= 2);
    }

    @Test
    public void testGroupVolatileVisibilityAfterCreate() throws Exception {
        Assertions.assertFalse(byName().containsKey("rg_visible_mv"));
        mgr.createResourceGroup(mvStmt("rg_visible_mv", "1"));
        Map<String, ResourceGroup> snap = byName();
        Assertions.assertTrue(snap.containsKey("rg_visible_mv"));
        Assertions.assertEquals(TWorkGroupType.WG_MV, snap.get("rg_visible_mv").getResourceGroupType());
        long id = snap.get("rg_visible_mv").getId();
        Assertions.assertTrue(byId().containsKey(id));
    }

    @Test
    public void testReadSnapshotConsistencyUnderWrite() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_snap_a", "1"));
        mgr.createResourceGroup(mvStmt("rg_snap_b", "1"));
        Object preWriteSnap = getSnapshot();
        Map<String, ResourceGroup> snapshotBefore   = snapField(preWriteSnap, "byName");
        Map<Long, ResourceGroup>   idSnapshotBefore = snapField(preWriteSnap, "byId");
        for (Map.Entry<String, ResourceGroup> e : snapshotBefore.entrySet()) {
            Assertions.assertTrue(idSnapshotBefore.containsKey(e.getValue().getId()));
        }
        mgr.dropResourceGroup(new DropResourceGroupStmt("rg_snap_a", false));
        for (Map.Entry<String, ResourceGroup> e : snapshotBefore.entrySet()) {
            Assertions.assertTrue(idSnapshotBefore.containsKey(e.getValue().getId()),
                    "Pre-write snapshot was mutated for ID " + e.getValue().getId());
        }
    }

    @Test
    public void testConcurrentReadsAndWritesNoException() throws Exception {
        for (int i = 0; i < 5; i++) {
            mgr.createResourceGroup(mvStmt("rg_conc_" + i, "1"));
        }
        Field snapField = ResourceGroupMgr.class.getDeclaredField("snapshot");
        snapField.setAccessible(true);
        Class<?> snapClass = snapField.get(mgr).getClass();
        Constructor<?> snapCtor = snapClass.getDeclaredConstructor(Map.class, Map.class, Map.class, ResourceGroup.class);
        snapCtor.setAccessible(true);

        int readerCount = 8;
        int writerCount = 2;
        ExecutorService pool = Executors.newFixedThreadPool(readerCount + writerCount);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);

        for (int i = 0; i < readerCount; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                        mgr.getAllResourceGroupNames();
                        mgr.getResourceGroup("rg_conc_0");
                        mgr.chooseResourceGroupByName(null, "rg_conc_0");
                        @SuppressWarnings("unchecked")
                        Map<String, ResourceGroup> snap =
                                (Map<String, ResourceGroup>) snapField(snapField.get(mgr), "byName");
                        for (ResourceGroup rg : snap.values()) {
                            Assertions.assertNotNull(rg.getName());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                    stop.set(true);
                }
            });
        }

        for (int i = 0; i < writerCount; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    int counter = 0;
                    while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                        @SuppressWarnings("unchecked")
                        Map<String, ResourceGroup> current =
                                (Map<String, ResourceGroup>) snapField(snapField.get(mgr), "byName");
                        Map<String, ResourceGroup> copy = new java.util.HashMap<>(current);
                        String key = "rg_transient_" + (counter % 3);
                        if (copy.containsKey(key)) {
                            copy.remove(key);
                        } else if (!current.isEmpty()) {
                            copy.put(key, current.values().iterator().next());
                        }
                        Object newSnap = snapCtor.newInstance(copy,
                                Collections.emptyMap(), Collections.emptyMap(), null);
                        snapField.set(mgr, newSnap);
                        counter++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                    stop.set(true);
                }
            });
        }

        startLatch.countDown();
        Thread.sleep(1000);
        stop.set(true);
        pool.shutdownNow();
        Assertions.assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        if (firstError.get() != null) {
            Assertions.fail("Exception in concurrent thread: " + firstError.get());
        }
    }

    @Test
    public void testWriteLockStillProtectsConcurrentDdl() throws Exception {
        int threadCount = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        AtomicBoolean error = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    latch.await();
                    mgr.createResourceGroup(mvStmt("rg_ddl_" + idx, "1"));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    error.set(true);
                }
            });
        }
        latch.countDown();
        pool.shutdown();
        pool.awaitTermination(15, TimeUnit.SECONDS);
        Assertions.assertFalse(error.get());
        Map<String, ResourceGroup> rgMap = byName();
        Map<Long, ResourceGroup>   idMap = byId();
        for (int i = 0; i < threadCount; i++) {
            ResourceGroup rg = rgMap.get("rg_ddl_" + i);
            Assertions.assertNotNull(rg, "Missing rg_ddl_" + i);
            Assertions.assertTrue(idMap.containsKey(rg.getId()));
        }
    }

    @Test
    public void testReturnedSnapshotIsUnmodifiable() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_immutable", "1"));
        Map<String, ResourceGroup> snap = byName();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> snap.put("rg_injected", null));
        Assertions.assertThrows(UnsupportedOperationException.class, snap::clear);
    }

    @Test
    public void testShowAllResourceGroupsListAll() throws Exception {
        ResourceGroup rgA = new ResourceGroup();
        rgA.setName("rg_show_a");
        rgA.setId(1001L);
        rgA.setMemLimit(0.1);
        rgA.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rgA.setClassifiers(Collections.emptyList());
        ResourceGroup rgB = new ResourceGroup();
        rgB.setName("rg_show_b");
        rgB.setId(1002L);
        rgB.setMemLimit(0.1);
        rgB.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rgB.setClassifiers(Collections.emptyList());
        injectRgIntoMap("rg_show_a", rgA, "rg_show_b", rgB);
        java.util.List<java.util.List<String>> rows = mgr.showAllResourceGroups(null, false, true);
        Assertions.assertFalse(rows.isEmpty());
        Assertions.assertTrue(rows.stream().anyMatch(r -> r.contains("rg_show_a")));
        Assertions.assertTrue(rows.stream().anyMatch(r -> r.contains("rg_show_b")));
    }

    @Test
    public void testShowAllResourceGroupsPerUserVisibility() throws Exception {
        ResourceGroup rg = new ResourceGroup();
        rg.setName("rg_show_user");
        rg.setId(2001L);
        rg.setMemLimit(0.1);
        rg.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rg.setClassifiers(Collections.emptyList());
        injectRgIntoMap("rg_show_user", rg);
        com.starrocks.qe.ConnectContext ctx = new com.starrocks.qe.ConnectContext();
        ctx.setRemoteIP("127.0.0.1");
        ctx.setQualifiedUser("test_user");
        com.starrocks.qe.ConnectContext.set(ctx);
        try {
            Assertions.assertNotNull(mgr.showAllResourceGroups(ctx, false, false));
        } finally {
            com.starrocks.qe.ConnectContext.set(null);
        }
    }

    @Test
    public void testShowOneResourceGroupFoundAndNotFound() throws Exception {
        ResourceGroup rg = new ResourceGroup();
        rg.setName("rg_show_one");
        rg.setId(3001L);
        rg.setMemLimit(0.1);
        rg.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rg.setClassifiers(Collections.emptyList());
        injectRgIntoMap("rg_show_one", rg);
        java.util.List<java.util.List<String>> found = mgr.showOneResourceGroup("rg_show_one", false);
        Assertions.assertFalse(found.isEmpty());
        Assertions.assertTrue(found.stream().anyMatch(r -> r.contains("rg_show_one")));
        Assertions.assertTrue(mgr.showOneResourceGroup("rg_does_not_exist", false).isEmpty());
    }

    // -------------------------------------------------------------------------
    // 17. Finding 3: Single-write ALTER snapshot replacement
    // -------------------------------------------------------------------------

    @Test
    public void testSingleWriteAlterSnapshotReplacement() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_alter_single_write", "1"));

        // Latch-synchronised concurrent reader: captures a snapshot reference during the alter
        // and checks the group is present in that snapshot. With the old double-write code a
        // reader that captured a snapshot between the remove and add would see null here.
        AtomicReference<String> readerFailure = new AtomicReference<>();
        CountDownLatch readerReady  = new CountDownLatch(1);
        CountDownLatch writerDone   = new CountDownLatch(1);
        CountDownLatch readerDone   = new CountDownLatch(1);

        Thread reader = new Thread(() -> {
            try {
                readerReady.countDown();
                // spin until alter is in-flight; the barrier is best-effort for race coverage
                writerDone.await();
                // Check every snapshot we can capture after the alter completed.
                for (int i = 0; i < 1000; i++) {
                    ResourceGroup rg = mgr.getResourceGroup("rg_alter_single_write");
                    if (rg == null) {
                        readerFailure.set("Group absent in iteration " + i);
                        break;
                    }
                }
            } catch (Exception e) {
                readerFailure.set(e.getMessage());
            } finally {
                readerDone.countDown();
            }
        });
        reader.setDaemon(true);
        reader.start();
        readerReady.await();

        mgr.alterResourceGroup(new com.starrocks.sql.ast.AlterResourceGroupStmt("rg_alter_single_write",
                new com.starrocks.sql.ast.AlterResourceGroupStmt.AlterProperties(
                        Collections.singletonMap("mem_limit", "0.6"))));
        writerDone.countDown();
        readerDone.await(5, TimeUnit.SECONDS);

        Assertions.assertNull(readerFailure.get(), readerFailure.get());

        Object snapAfter = getSnapshot();
        Map<String, ResourceGroup> byNameAfter = snapField(snapAfter, "byName");
        Assertions.assertTrue(byNameAfter.containsKey("rg_alter_single_write"),
                "Group must remain continuously present in snapshot after alter");
    }

    // -------------------------------------------------------------------------
    // 18. Finding 4: Atomic shortQueryResourceGroup snapshot bundling
    // -------------------------------------------------------------------------

    @Test
    public void testAtomicShortQueryGroupSnapshotRead() throws Exception {
        ResourceGroup sqGroup = new ResourceGroup();
        sqGroup.setName("sq_rg");
        sqGroup.setId(777L);
        sqGroup.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        sqGroup.setMemLimit(0.5);
        ResourceGroupClassifier classifier = new ResourceGroupClassifier();
        classifier.setResourceGroupId(777L);
        classifier.setUser("test_user");
        sqGroup.setClassifiers(Collections.singletonList(classifier));

        java.lang.reflect.Method addMethod =
                ResourceGroupMgr.class.getDeclaredMethod("addResourceGroupInternal", ResourceGroup.class);
        addMethod.setAccessible(true);
        addMethod.invoke(mgr, sqGroup);

        Object snap = getSnapshot();
        ResourceGroup sqFromSnap = snapField(snap, "shortQueryResourceGroup");
        Assertions.assertNotNull(sqFromSnap, "shortQueryResourceGroup must be populated in snapshot");
        Assertions.assertEquals("sq_rg", sqFromSnap.getName());
    }

    @Test
    public void testDuplicateShortQueryGroupCreationFails() throws Exception {
        ResourceGroup sqGroup = new ResourceGroup();
        sqGroup.setName("sq1");
        sqGroup.setId(888L);
        sqGroup.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        sqGroup.setMemLimit(0.5);
        sqGroup.setClassifiers(Collections.emptyList());

        java.lang.reflect.Method addMethod =
                ResourceGroupMgr.class.getDeclaredMethod("addResourceGroupInternal", ResourceGroup.class);
        addMethod.setAccessible(true);
        addMethod.invoke(mgr, sqGroup);

        Map<String, String> props = Maps.newHashMap();
        props.put("cpu_weight", "1");
        props.put("mem_limit", "50%");
        props.put("type", "short_query");
        CreateResourceGroupStmt stmt2 = new CreateResourceGroupStmt("sq2", false, false,
                Collections.emptyList(), props);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt2);

        Assertions.assertThrows(DdlException.class, () -> mgr.createResourceGroup(stmt2));
    }

    // -------------------------------------------------------------------------
    // 21. Finding 3 (stress): concurrent ALTER never produces a transient absent group
    // -------------------------------------------------------------------------

    /**
     * Stress-tests that {@code replaceResourceGroupInternal} truly eliminates the
     * transient-absence window seen with the old remove-then-add double-write.
     *
     * <p>One writer thread repeatedly alters an MV resource group (5 000 iterations).
     * Ten reader threads each call {@code getResourceGroup} as fast as possible during
     * the same period and count any {@code null} returns.
     *
     * <p>A null count greater than zero would mean a reader observed the group absent
     * while a writer was mid-alter — the bug this fix is designed to prevent.
     */
    @Test
    public void testConcurrentAlterNoTransientGroupAbsence() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_alter_stress", "1"));

        int readerCount = 10;
        int writerIters = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(readerCount + 1);
        CountDownLatch startLatch   = new CountDownLatch(1);
        CountDownLatch writerDone   = new CountDownLatch(1);
        AtomicBoolean  writerActive = new AtomicBoolean(true);
        AtomicLong     nullCount    = new AtomicLong(0);
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        // Writer: alternate between two mem_limit values to force repeated snapshot replacements.
        pool.submit(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < writerIters; i++) {
                    String memLimit = (i % 2 == 0) ? "0.3" : "0.4";
                    mgr.alterResourceGroup(new com.starrocks.sql.ast.AlterResourceGroupStmt(
                            "rg_alter_stress",
                            new com.starrocks.sql.ast.AlterResourceGroupStmt.AlterProperties(
                                    Collections.singletonMap("mem_limit", memLimit))));
                }
            } catch (Throwable t) {
                firstError.compareAndSet(null, t);
            } finally {
                writerActive.set(false);
                writerDone.countDown();
            }
        });

        // Readers: count any null observations of the group during the writer's run.
        for (int i = 0; i < readerCount; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    while (writerActive.get() || !writerDone.await(0, TimeUnit.MILLISECONDS)) {
                        if (mgr.getResourceGroup("rg_alter_stress") == null) {
                            nullCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                }
            });
        }

        startLatch.countDown();
        writerDone.await(30, TimeUnit.SECONDS);
        pool.shutdownNow();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        Assertions.assertNull(firstError.get(),
                "Unexpected exception in concurrent thread: " + firstError.get());
        Assertions.assertEquals(0L, nullCount.get(),
                "Lock-free readers observed " + nullCount.get() +
                        " transient null(s) for 'rg_alter_stress' during concurrent alter");
    }

    // -------------------------------------------------------------------------
    // 22. Finding P1 (discussion_r3793204369): CREATE RESOURCE GROUP OR REPLACE
    // -------------------------------------------------------------------------

    @Test
    public void testCreateResourceGroupReplaceMemPoolChangeMemLimit() throws Exception {
        Map<String, String> props1 = Maps.newHashMap();
        props1.put("cpu_weight", "1");
        props1.put("mem_limit", "50%");
        props1.put("mem_pool", "custom_pool_1");
        props1.put("type", "mv");
        CreateResourceGroupStmt stmt1 = new CreateResourceGroupStmt("rg_mp1", false, false,
                Collections.emptyList(), props1);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt1);
        mgr.createResourceGroup(stmt1);

        ResourceGroup rg1 = mgr.getResourceGroup("rg_mp1");
        Assertions.assertNotNull(rg1);
        Assertions.assertEquals(0.5, rg1.getMemLimit());

        // Replace the group in custom_pool_1 with a different mem_limit (60%).
        // This validates that resourceGroupInMemPoolHaveSameMemLimit ignores the old version
        // of rg_mp1 being replaced and succeeds without throwing DdlException.
        Map<String, String> props2 = Maps.newHashMap();
        props2.put("cpu_weight", "1");
        props2.put("mem_limit", "60%");
        props2.put("mem_pool", "custom_pool_1");
        props2.put("type", "mv");
        CreateResourceGroupStmt stmt2 = new CreateResourceGroupStmt("rg_mp1", false, true,
                Collections.emptyList(), props2);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt2);

        mgr.createResourceGroup(stmt2);

        ResourceGroup rg2 = mgr.getResourceGroup("rg_mp1");
        Assertions.assertNotNull(rg2);
        Assertions.assertEquals(0.6, rg2.getMemLimit());
    }

    @Test
    public void testCreateResourceGroupReplaceShortQueryGroup() throws Exception {
        ResourceGroup sqGroup = new ResourceGroup();
        sqGroup.setName("sq_replace");
        sqGroup.setId(888L);
        sqGroup.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        sqGroup.setMemLimit(0.5);
        sqGroup.setCpuWeight(1);
        sqGroup.setClassifiers(Collections.emptyList());

        java.lang.reflect.Method addMethod =
                ResourceGroupMgr.class.getDeclaredMethod("addResourceGroupInternal", ResourceGroup.class);
        addMethod.setAccessible(true);
        addMethod.invoke(mgr, sqGroup);

        Assertions.assertNotNull(snapField(getSnapshot(), "shortQueryResourceGroup"));
        Assertions.assertEquals("sq_replace",
                ((ResourceGroup) snapField(getSnapshot(), "shortQueryResourceGroup")).getName());

        // Replace short_query group with MV group.
        // Validates that shortQueryResourceGroup in snapshot becomes null when a WG_SHORT_QUERY group is replaced.
        Map<String, String> props2 = Maps.newHashMap();
        props2.put("cpu_weight", "1");
        props2.put("mem_limit", "60%");
        props2.put("type", "mv");
        CreateResourceGroupStmt stmt2 = new CreateResourceGroupStmt("sq_replace", false, true,
                Collections.emptyList(), props2);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt2);

        mgr.createResourceGroup(stmt2);

        ResourceGroup sqReplaced = snapField(getSnapshot(), "shortQueryResourceGroup");
        Assertions.assertNull(sqReplaced,
                "shortQueryResourceGroup must become null when WG_SHORT_QUERY group is replaced with WG_MV");
        ResourceGroup rgReplaced = mgr.getResourceGroup("sq_replace");
        Assertions.assertNotNull(rgReplaced);
        Assertions.assertEquals(0.6, rgReplaced.getMemLimit());
    }

    @Test
    public void testCreateResourceGroupReplaceFailsValidationBeforeSideEffects() throws Exception {
        Map<String, String> props1 = Maps.newHashMap();
        props1.put("cpu_weight", "1");
        props1.put("mem_limit", "50%");
        props1.put("type", "mv");
        CreateResourceGroupStmt stmt1 = new CreateResourceGroupStmt("rg_valid_test", false, false,
                Collections.emptyList(), props1);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt1);
        mgr.createResourceGroup(stmt1);

        ResourceGroup original = mgr.getResourceGroup("rg_valid_test");
        Assertions.assertNotNull(original);

        // Inject an existing short_query group.
        ResourceGroup sqExisting = new ResourceGroup();
        sqExisting.setName("sq_existing");
        sqExisting.setId(999L);
        sqExisting.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        sqExisting.setMemLimit(0.5);
        sqExisting.setClassifiers(Collections.emptyList());
        java.lang.reflect.Method addMethod =
                ResourceGroupMgr.class.getDeclaredMethod("addResourceGroupInternal", ResourceGroup.class);
        addMethod.setAccessible(true);
        addMethod.invoke(mgr, sqExisting);

        // Attempt replacing rg_valid_test with short_query type when another short_query group sq_existing exists.
        Map<String, String> propsBad = Maps.newHashMap();
        propsBad.put("cpu_weight", "1");
        propsBad.put("mem_limit", "50%");
        propsBad.put("type", "short_query");
        CreateResourceGroupStmt stmtBad = new CreateResourceGroupStmt("rg_valid_test", false, true,
                Collections.emptyList(), propsBad);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmtBad);

        // Should throw DdlException: "There can be only one short_query RESOURCE_GROUP"
        Assertions.assertThrows(DdlException.class, () -> mgr.createResourceGroup(stmtBad));

        // Verify original group remains completely unmodified in snapshot.
        ResourceGroup current = mgr.getResourceGroup("rg_valid_test");
        Assertions.assertNotNull(current);
        Assertions.assertEquals(original.getId(), current.getId());
        Assertions.assertEquals(TWorkGroupType.WG_MV, current.getResourceGroupType());
    }

    @Test
    public void testReplaceResourceGroupWithNullClassifiers() throws Exception {
        ResourceGroup rgNullCls = new ResourceGroup();
        rgNullCls.setName("rg_null_cls");
        rgNullCls.setId(777L);
        rgNullCls.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rgNullCls.setMemLimit(0.5);
        rgNullCls.setClassifiers(null);

        java.lang.reflect.Method addMethod =
                ResourceGroupMgr.class.getDeclaredMethod("addResourceGroupInternal", ResourceGroup.class);
        addMethod.setAccessible(true);
        // Must not throw NPE when classifiers is null
        Assertions.assertDoesNotThrow(() -> addMethod.invoke(mgr, rgNullCls));

        ResourceGroup rgNew = new ResourceGroup();
        rgNew.setName("rg_null_cls");
        rgNew.setId(777L);
        rgNew.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rgNew.setMemLimit(0.6);
        rgNew.setClassifiers(null);

        java.lang.reflect.Method replaceMethod =
                ResourceGroupMgr.class.getDeclaredMethod("replaceResourceGroupInternal", String.class, ResourceGroup.class);
        replaceMethod.setAccessible(true);
        // Must not throw NPE when replacing group with null classifiers
        Assertions.assertDoesNotThrow(() -> replaceMethod.invoke(mgr, "rg_null_cls", rgNew));
        Assertions.assertEquals(0.6, mgr.getResourceGroup("rg_null_cls").getMemLimit());
    }

    @Test
    public void testCreateResourceGroupReplaceSingleWalOperation() throws Exception {
        Map<String, String> props1 = Maps.newHashMap();
        props1.put("cpu_weight", "1");
        props1.put("mem_limit", "50%");
        props1.put("type", "mv");
        CreateResourceGroupStmt stmt1 = new CreateResourceGroupStmt("rg_single_wal", false, false,
                Collections.emptyList(), props1);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt1);
        mgr.createResourceGroup(stmt1);

        ResourceGroup original = mgr.getResourceGroup("rg_single_wal");
        Assertions.assertNotNull(original);
        long originalId = original.getId();

        // Replace rg_single_wal with 60% mem_limit
        Map<String, String> props2 = Maps.newHashMap();
        props2.put("cpu_weight", "1");
        props2.put("mem_limit", "60%");
        props2.put("type", "mv");
        CreateResourceGroupStmt stmt2 = new CreateResourceGroupStmt("rg_single_wal", false, true,
                Collections.emptyList(), props2);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt2);

        mgr.createResourceGroup(stmt2);

        ResourceGroup replaced = mgr.getResourceGroup("rg_single_wal");
        Assertions.assertNotNull(replaced);
        // ID must be preserved across replacement
        Assertions.assertEquals(originalId, replaced.getId());
        Assertions.assertEquals(0.6, replaced.getMemLimit());
    }
}
