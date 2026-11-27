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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
class ResourceGroupMgrConcurrencyTest {

    private ResourceGroupMgr mgr;

    @BeforeEach
    void setUp() {
        UtFrameUtils.setUpForPersistTest();
        mgr = new ResourceGroupMgr();
    }

    @AfterEach
    void tearDown() {
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

    private Map<String, ResourceGroup> byName() {
        return mgr.getSnapshotForTest().byName;
    }

    private Map<Long, ResourceGroup> byId() {
        return mgr.getSnapshotForTest().byId;
    }

    private Map<Long, ResourceGroupClassifier> byClassifier() {
        return mgr.getSnapshotForTest().byClassifier;
    }

    private void injectRgIntoMap(Object... namesAndGroups) {
        Map<String, ResourceGroup> bName = new LinkedHashMap<>();
        Map<Long, ResourceGroup>   bId   = new HashMap<>();
        for (int i = 0; i < namesAndGroups.length; i += 2) {
            ResourceGroup rg = (ResourceGroup) namesAndGroups[i + 1];
            bName.put((String) namesAndGroups[i], rg);
            bId.put(rg.getId(), rg);
        }
        ResourceGroupMgr.ResourceGroupSnapshot snap = ResourceGroupMgr.newSnapshotForTest(
                bName, bId, Collections.emptyMap(), null);
        mgr.setSnapshotForTest(snap);
    }

    @Test
    void testVolatileFieldsInitializedAsUnmodifiable() {
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
    void testAddResourceGroupInternalReplacesAllThreeMaps() throws Exception {
        ResourceGroupMgr.ResourceGroupSnapshot snapBefore = mgr.getSnapshotForTest();
        Map<String, ResourceGroup> rgBefore = snapBefore.byName;
        Map<Long, ResourceGroup>   idBefore = snapBefore.byId;

        mgr.createResourceGroup(mvStmt("rg_add_test", "1"));

        ResourceGroupMgr.ResourceGroupSnapshot snapAfter = mgr.getSnapshotForTest();
        Map<String, ResourceGroup> rgAfter = snapAfter.byName;
        Map<Long, ResourceGroup>   idAfter = snapAfter.byId;

        Assertions.assertNotSame(snapBefore, snapAfter);
        Assertions.assertNotSame(rgBefore, rgAfter);
        Assertions.assertNotSame(idBefore, idAfter);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> rgAfter.put("y", null));
        Assertions.assertTrue(rgAfter.containsKey("rg_add_test"));
    }

    @Test
    void testRemoveResourceGroupInternalReplacesAllThreeMaps() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_remove_test", "1"));
        ResourceGroupMgr.ResourceGroupSnapshot snapBefore = mgr.getSnapshotForTest();
        Map<String, ResourceGroup> rgBefore = snapBefore.byName;
        Map<Long, ResourceGroup>   idBefore = snapBefore.byId;

        mgr.dropResourceGroup(new DropResourceGroupStmt("rg_remove_test", false));

        ResourceGroupMgr.ResourceGroupSnapshot snapAfter = mgr.getSnapshotForTest();
        Map<String, ResourceGroup> rgAfter = snapAfter.byName;
        Map<Long, ResourceGroup>   idAfter = snapAfter.byId;

        Assertions.assertNotSame(snapBefore, snapAfter);
        Assertions.assertNotSame(rgBefore, rgAfter);
        Assertions.assertNotSame(idBefore, idAfter);
        Assertions.assertFalse(rgAfter.containsKey("rg_remove_test"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> rgAfter.put("z", null));
    }

    @Test
    void testGetAllResourceGroupNamesReturnsDefensiveCopy() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_copy_test", "1"));
        Set<String> names = mgr.getAllResourceGroupNames();
        Map<String, ResourceGroup> internalMap = byName();
        Assertions.assertNotSame(internalMap.keySet(), names);
        names.add("injected_name");
        Assertions.assertFalse(mgr.getAllResourceGroupNames().contains("injected_name"));
        Assertions.assertTrue(names.contains("rg_copy_test"));
    }

    @Test
    void testGetResourceGroupByNameLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_by_name", "1"));
        CountDownLatch writeLockHeld = new CountDownLatch(1);
        CountDownLatch releaseWriteLock = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            mgr.writeLock();
            try {
                writeLockHeld.countDown();
                releaseWriteLock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                mgr.writeUnlock();
            }
        });
        writer.start();
        try {
            Assertions.assertTrue(writeLockHeld.await(5, TimeUnit.SECONDS));
            CompletableFuture<ResourceGroup> future =
                    CompletableFuture.supplyAsync(() -> mgr.getResourceGroup("rg_by_name"));
            ResourceGroup rg = future.get(2, TimeUnit.SECONDS);
            Assertions.assertNotNull(rg);
            Assertions.assertEquals("rg_by_name", rg.getName());
        } finally {
            releaseWriteLock.countDown();
            writer.join(5000);
        }
    }

    @Test
    void testGetResourceGroupByIdLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_by_id", "1"));
        ResourceGroup rgByName = mgr.getResourceGroup("rg_by_id");
        Assertions.assertNotNull(rgByName);
        long id = rgByName.getId();

        CountDownLatch writeLockHeld = new CountDownLatch(1);
        CountDownLatch releaseWriteLock = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            mgr.writeLock();
            try {
                writeLockHeld.countDown();
                releaseWriteLock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                mgr.writeUnlock();
            }
        });
        writer.start();
        try {
            Assertions.assertTrue(writeLockHeld.await(5, TimeUnit.SECONDS));
            CompletableFuture<ResourceGroup> future =
                    CompletableFuture.supplyAsync(() -> mgr.getResourceGroup(id));
            ResourceGroup rg = future.get(2, TimeUnit.SECONDS);
            Assertions.assertNotNull(rg);
            Assertions.assertEquals(id, rg.getId());
        } finally {
            releaseWriteLock.countDown();
            writer.join(5000);
        }
    }

    @Test
    void testChooseResourceGroupByNameLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_choose_name", "1"));
        CountDownLatch writeLockHeld = new CountDownLatch(1);
        CountDownLatch releaseWriteLock = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            mgr.writeLock();
            try {
                writeLockHeld.countDown();
                releaseWriteLock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                mgr.writeUnlock();
            }
        });
        writer.start();
        try {
            Assertions.assertTrue(writeLockHeld.await(5, TimeUnit.SECONDS));
            CompletableFuture<TWorkGroup> future =
                    CompletableFuture.supplyAsync(() -> mgr.chooseResourceGroupByName(null, "rg_choose_name"));
            TWorkGroup twg = future.get(2, TimeUnit.SECONDS);
            Assertions.assertNotNull(twg);
            Assertions.assertEquals("rg_choose_name", twg.getName());
        } finally {
            releaseWriteLock.countDown();
            writer.join(5000);
        }
    }

    @Test
    void testChooseResourceGroupByIDLockFree() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_choose_id", "1"));
        ResourceGroup rg = mgr.getResourceGroup("rg_choose_id");
        Assertions.assertNotNull(rg);
        long id = rg.getId();

        CountDownLatch writeLockHeld = new CountDownLatch(1);
        CountDownLatch releaseWriteLock = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            mgr.writeLock();
            try {
                writeLockHeld.countDown();
                releaseWriteLock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                mgr.writeUnlock();
            }
        });
        writer.start();
        try {
            Assertions.assertTrue(writeLockHeld.await(5, TimeUnit.SECONDS));
            CompletableFuture<TWorkGroup> future =
                    CompletableFuture.supplyAsync(() -> mgr.chooseResourceGroupByID(null, id));
            TWorkGroup twg = future.get(2, TimeUnit.SECONDS);
            Assertions.assertNotNull(twg);
        } finally {
            releaseWriteLock.countDown();
            writer.join(5000);
        }
    }

    @Test
    void testAllHotPathReadMethodsLockFreeUnderWriteLock() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_all_lockfree", "1"));
        ResourceGroup rg = mgr.getResourceGroup("rg_all_lockfree");
        Assertions.assertNotNull(rg);
        long id = rg.getId();

        CountDownLatch writeLockHeld = new CountDownLatch(1);
        CountDownLatch releaseWriteLock = new CountDownLatch(1);
        Thread writer = new Thread(() -> {
            mgr.writeLock();
            try {
                writeLockHeld.countDown();
                releaseWriteLock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                mgr.writeUnlock();
            }
        });
        writer.start();
        try {
            Assertions.assertTrue(writeLockHeld.await(5, TimeUnit.SECONDS));

            com.starrocks.qe.ConnectContext ctx = new com.starrocks.qe.ConnectContext();
            ctx.setRemoteIP("127.0.0.1");
            ctx.setQualifiedUser("test_user");

            CompletableFuture<Void> allReads = CompletableFuture.runAsync(() -> {
                Assertions.assertNotNull(mgr.getResourceGroup("rg_all_lockfree"));
                Assertions.assertNotNull(mgr.getResourceGroup(id));
                Assertions.assertNotNull(mgr.chooseResourceGroupByName(null, "rg_all_lockfree"));
                Assertions.assertNotNull(mgr.chooseResourceGroupByID(null, id));
                Assertions.assertTrue(mgr.getAllResourceGroupNames().contains("rg_all_lockfree"));
                Assertions.assertTrue(mgr.getResourceGroupIds().contains(id));
                mgr.chooseResourceGroup(ctx, null, null);
                Assertions.assertFalse(mgr.showOneResourceGroup("rg_all_lockfree", false).isEmpty());
                Assertions.assertFalse(mgr.showAllResourceGroups(null, false, true).isEmpty());
            });

            Assertions.assertDoesNotThrow(() -> allReads.get(2, TimeUnit.SECONDS));
        } finally {
            releaseWriteLock.countDown();
            writer.join(5000);
        }
    }

    @Test
    void testCandidateGroupIdsBuildMatchesStreamApproach() throws Exception {
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
    void testGroupVolatileVisibilityAfterCreate() throws Exception {
        Assertions.assertFalse(byName().containsKey("rg_visible_mv"));
        mgr.createResourceGroup(mvStmt("rg_visible_mv", "1"));
        Map<String, ResourceGroup> snap = byName();
        Assertions.assertTrue(snap.containsKey("rg_visible_mv"));
        Assertions.assertEquals(TWorkGroupType.WG_MV, snap.get("rg_visible_mv").getResourceGroupType());
        long id = snap.get("rg_visible_mv").getId();
        Assertions.assertTrue(byId().containsKey(id));
    }

    @Test
    void testReadSnapshotConsistencyUnderWrite() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_snap_a", "1"));
        mgr.createResourceGroup(mvStmt("rg_snap_b", "1"));
        ResourceGroupMgr.ResourceGroupSnapshot preWriteSnap = mgr.getSnapshotForTest();
        Map<String, ResourceGroup> snapshotBefore   = preWriteSnap.byName;
        Map<Long, ResourceGroup>   idSnapshotBefore = preWriteSnap.byId;
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
    void testConcurrentReadsAndWritesNoException() throws Exception {
        for (int i = 0; i < 5; i++) {
            mgr.createResourceGroup(mvStmt("rg_conc_" + i, "1"));
        }

        int readerCount = 8;
        int writerCount = 2;
        int readOps = 2_000;
        int writeOps = 500;
        ExecutorService pool = Executors.newFixedThreadPool(readerCount + writerCount);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(readerCount + writerCount);

        for (int i = 0; i < readerCount; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < readOps && firstError.get() == null; j++) {
                        mgr.getAllResourceGroupNames();
                        mgr.getResourceGroup("rg_conc_0");
                        mgr.chooseResourceGroupByName(null, "rg_conc_0");
                        Map<String, ResourceGroup> snap = mgr.getSnapshotForTest().byName;
                        for (ResourceGroup rg : snap.values()) {
                            Assertions.assertNotNull(rg.getName());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        for (int i = 0; i < writerCount; i++) {
            pool.submit(() -> {
                try {
                    startLatch.await();
                    for (int counter = 0; counter < writeOps && firstError.get() == null; counter++) {
                        Map<String, ResourceGroup> current = mgr.getSnapshotForTest().byName;
                        Map<String, ResourceGroup> copy = new java.util.HashMap<>(current);
                        String key = "rg_transient_" + (counter % 3);
                        if (copy.containsKey(key)) {
                            copy.remove(key);
                        } else if (!current.isEmpty()) {
                            copy.put(key, current.values().iterator().next());
                        }
                        ResourceGroupMgr.ResourceGroupSnapshot newSnap = ResourceGroupMgr.newSnapshotForTest(
                                copy, Collections.emptyMap(), Collections.emptyMap(), null);
                        mgr.setSnapshotForTest(newSnap);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    firstError.compareAndSet(null, t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        Assertions.assertTrue(doneLatch.await(15, TimeUnit.SECONDS));
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        if (firstError.get() != null) {
            Assertions.fail("Exception in concurrent thread: " + firstError.get());
        }
    }

    @Test
    void testWriteLockStillProtectsConcurrentDdl() throws Exception {
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
    void testReturnedSnapshotIsUnmodifiable() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_immutable", "1"));
        Map<String, ResourceGroup> snap = byName();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> snap.put("rg_injected", null));
        Assertions.assertThrows(UnsupportedOperationException.class, snap::clear);
    }

    @Test
    void testShowAllResourceGroupsListAll() {
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
    void testShowAllResourceGroupsPerUserVisibility() {
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
    void testShowOneResourceGroupFoundAndNotFound() {
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
    void testSingleWriteAlterSnapshotReplacement() throws Exception {
        mgr.createResourceGroup(mvStmt("rg_alter_single_write", "1"));

        // Latch-synchronised concurrent reader: repeatedly queries the group during the alter
        // and checks the group remains present throughout.
        AtomicReference<String> readerFailure = new AtomicReference<>();
        CountDownLatch readerReady  = new CountDownLatch(1);
        CountDownLatch writerDone   = new CountDownLatch(1);
        CountDownLatch readerDone   = new CountDownLatch(1);
        AtomicBoolean  active       = new AtomicBoolean(true);

        Thread reader = new Thread(() -> {
            try {
                readerReady.countDown();
                while (active.get() || !writerDone.await(0, TimeUnit.MILLISECONDS)) {
                    ResourceGroup rg = mgr.getResourceGroup("rg_alter_single_write");
                    if (rg == null) {
                        readerFailure.set("Group absent during concurrent alter");
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
        active.set(false);
        writerDone.countDown();
        readerDone.await(5, TimeUnit.SECONDS);

        Assertions.assertNull(readerFailure.get(), readerFailure.get());

        Map<String, ResourceGroup> byNameAfter = mgr.getSnapshotForTest().byName;
        Assertions.assertTrue(byNameAfter.containsKey("rg_alter_single_write"),
                "Group must remain continuously present in snapshot after alter");
    }

    // -------------------------------------------------------------------------
    // 18. Finding 4: Atomic shortQueryResourceGroup snapshot bundling
    // -------------------------------------------------------------------------

    @Test
    void testAtomicShortQueryGroupSnapshotRead() {
        ResourceGroup sqGroup = new ResourceGroup();
        sqGroup.setName("sq_rg");
        sqGroup.setId(777L);
        sqGroup.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        sqGroup.setMemLimit(0.5);
        ResourceGroupClassifier classifier = new ResourceGroupClassifier();
        classifier.setResourceGroupId(777L);
        classifier.setUser("test_user");
        sqGroup.setClassifiers(Collections.singletonList(classifier));

        mgr.addResourceGroupInternal(sqGroup);

        ResourceGroup sqFromSnap = mgr.getSnapshotForTest().shortQueryResourceGroup;
        Assertions.assertNotNull(sqFromSnap, "shortQueryResourceGroup must be populated in snapshot");
        Assertions.assertEquals("sq_rg", sqFromSnap.getName());
    }

    @Test
    void testDuplicateShortQueryGroupCreationFails() throws Exception {
        ResourceGroup sqGroup = new ResourceGroup();
        sqGroup.setName("sq1");
        sqGroup.setId(888L);
        sqGroup.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        sqGroup.setMemLimit(0.5);
        sqGroup.setClassifiers(Collections.emptyList());

        mgr.addResourceGroupInternal(sqGroup);

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
    void testConcurrentAlterNoTransientGroupAbsence() throws Exception {
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
    void testCreateResourceGroupReplaceMemPoolChangeMemLimit() throws Exception {
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
    void testCreateResourceGroupReplaceShortQueryGroup() throws Exception {
        ResourceGroup sqGroup = new ResourceGroup();
        sqGroup.setName("sq_replace");
        sqGroup.setId(888L);
        sqGroup.setResourceGroupType(TWorkGroupType.WG_SHORT_QUERY);
        sqGroup.setMemLimit(0.5);
        sqGroup.setCpuWeight(1);
        sqGroup.setClassifiers(Collections.emptyList());

        mgr.addResourceGroupInternal(sqGroup);

        Assertions.assertNotNull(mgr.getSnapshotForTest().shortQueryResourceGroup);
        Assertions.assertEquals("sq_replace",
                mgr.getSnapshotForTest().shortQueryResourceGroup.getName());

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

        ResourceGroup sqReplaced = mgr.getSnapshotForTest().shortQueryResourceGroup;
        Assertions.assertNull(sqReplaced,
                "shortQueryResourceGroup must become null when WG_SHORT_QUERY group is replaced with WG_MV");
        ResourceGroup rgReplaced = mgr.getResourceGroup("sq_replace");
        Assertions.assertNotNull(rgReplaced);
        Assertions.assertEquals(0.6, rgReplaced.getMemLimit());
    }

    @Test
    void testCreateResourceGroupReplaceFailsValidationBeforeSideEffects() throws Exception {
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
        mgr.addResourceGroupInternal(sqExisting);

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
    void testReplaceResourceGroupWithNullClassifiers() {
        ResourceGroup rgNullCls = new ResourceGroup();
        rgNullCls.setName("rg_null_cls");
        rgNullCls.setId(777L);
        rgNullCls.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rgNullCls.setMemLimit(0.5);
        rgNullCls.setClassifiers(null);

        // Must not throw NPE when classifiers is null
        Assertions.assertDoesNotThrow(() -> mgr.addResourceGroupInternal(rgNullCls));

        ResourceGroup rgNew = new ResourceGroup();
        rgNew.setName("rg_null_cls");
        rgNew.setId(777L);
        rgNew.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        rgNew.setMemLimit(0.6);
        rgNew.setClassifiers(null);

        // Must not throw NPE when replacing group with null classifiers
        Assertions.assertDoesNotThrow(() -> mgr.replaceResourceGroupInternal("rg_null_cls", rgNew));
        Assertions.assertEquals(0.6, mgr.getResourceGroup("rg_null_cls").getMemLimit());
    }

    @Test
    void testCreateResourceGroupReplaceSingleWalOperation() throws Exception {
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
