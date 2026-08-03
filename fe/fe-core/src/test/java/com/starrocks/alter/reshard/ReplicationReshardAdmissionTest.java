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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockInterruptException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.replication.ReplicationMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TTableReplicationRequest;
import com.starrocks.thrift.TTableType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicationReshardAdmissionTest {
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        Config.enable_range_distribution = true;
        starRocksAssert.withDatabase("replication_reshard_admission_test")
                .useDatabase("replication_reshard_admission_test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("replication_reshard_admission_test");

        String sql = "create table admission_table (key1 int, key2 int)\n"
                + "partition by range(key1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "order by(key1)\n"
                + "properties('replication_num' = '1');";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "admission_table");
    }

    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        setLeaderWorkAdmissionOpen(true);
        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    @AfterEach
    public void tearDown() throws Exception {
        setLeaderWorkAdmissionOpen(true);
        restoreTableRegistration();
        Thread.interrupted();
    }

    @Test
    public void testSplitResolvesTableAfterAcquiringAdmissionLock() throws Exception {
        assertCatalogDisappearanceRejected(
                new SplitTabletJob(101, db.getId(), table.getId(), Map.of()));
    }

    @Test
    public void testMergeResolvesTableAfterAcquiringAdmissionLock() throws Exception {
        assertCatalogDisappearanceRejected(
                new MergeTabletJob(102, db.getId(), table.getId(), Map.of()));
    }

    @Test
    public void testReplicationConstructionReservationBlocksSplitAndMerge() throws Exception {
        CountDownLatch constructionEntered = new CountDownLatch(1);
        CountDownLatch releaseConstruction = new CountDownLatch(1);
        ReplicationMgr replicationMgr = blockingReplicationMgr(constructionEntered, releaseConstruction);
        mockReplicationMgr(replicationMgr);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread producer = new Thread(() -> {
            try {
                replicationMgr.addReplicationJob(newRequest());
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        producer.start();

        Assertions.assertTrue(constructionEntered.await(5, TimeUnit.SECONDS));
        Assertions.assertTrue(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));

        SplitTabletJob splitJob = new SplitTabletJob(1, db.getId(), table.getId(), Map.of());
        MergeTabletJob mergeJob = new MergeTabletJob(2, db.getId(), table.getId(), Map.of());
        Assertions.assertThrows(StarRocksException.class, splitJob::init);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertThrows(StarRocksException.class, mergeJob::init);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        releaseConstruction.countDown();
        producer.join(5000L);
        Assertions.assertFalse(producer.isAlive());
        Assertions.assertNull(failure.get());
        Assertions.assertEquals(1, replicationMgr.getRunningJobs().size());
    }

    @Test
    public void testTabletReshardStateBlocksNewReplication() throws Exception {
        ReplicationMgr replicationMgr = new ReplicationMgr();
        table.setState(OlapTable.OlapTableState.TABLET_RESHARD);

        Assertions.assertThrows(StarRocksException.class,
                () -> replicationMgr.addReplicationJob(newReplicationJob()));

        Assertions.assertTrue(replicationMgr.getRunningJobs().isEmpty());
        Assertions.assertFalse(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));
    }

    @Test
    public void testReplicationRejectsClosedAdmissionBeforeConstruction() throws Exception {
        AtomicInteger constructionCount = new AtomicInteger();
        ReplicationMgr replicationMgr = new ReplicationMgr() {
            @Override
            protected ReplicationJob createReplicationJob(TTableReplicationRequest request) throws StarRocksException {
                constructionCount.incrementAndGet();
                return newReplicationJob();
            }
        };
        setLeaderWorkAdmissionOpen(false);

        Assertions.assertThrows(StarRocksException.class,
                () -> replicationMgr.addReplicationJob(newRequest()));

        Assertions.assertEquals(0, constructionCount.get());
        Assertions.assertFalse(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));
        Assertions.assertTrue(replicationMgr.getRunningJobs().isEmpty());
    }

    @Test
    public void testDirectReplicationRejectsClosedLeaderWorkAdmission() throws Exception {
        ReplicationMgr replicationMgr = new ReplicationMgr();
        ReplicationJob replicationJob = newReplicationJob();
        setLeaderWorkAdmissionOpen(false);

        Assertions.assertThrows(StarRocksException.class,
                () -> replicationMgr.addReplicationJob(replicationJob));

        Assertions.assertTrue(replicationMgr.getRunningJobs().isEmpty());
        Assertions.assertFalse(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));
    }

    @Test
    public void testReplicationRejectsClosedAdmissionAtFinalPublicationWithoutLeak() throws Exception {
        CountDownLatch constructionEntered = new CountDownLatch(1);
        CountDownLatch releaseConstruction = new CountDownLatch(1);
        ReplicationMgr replicationMgr = blockingReplicationMgr(constructionEntered, releaseConstruction);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                replicationMgr.addReplicationJob(newRequest());
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        producer.start();

        Assertions.assertTrue(constructionEntered.await(5, TimeUnit.SECONDS));
        setLeaderWorkAdmissionOpen(false);
        releaseConstruction.countDown();
        producer.join(5000L);

        Assertions.assertFalse(producer.isAlive());
        Assertions.assertInstanceOf(StarRocksException.class, failure.get());
        Assertions.assertTrue(replicationMgr.getRunningJobs().isEmpty());
        Assertions.assertFalse(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));
    }

    @Test
    public void testReplicationConstructorFailureDoesNotLeakReservation() {
        ReplicationMgr replicationMgr = new ReplicationMgr() {
            @Override
            protected ReplicationJob createReplicationJob(TTableReplicationRequest request) throws StarRocksException {
                throw new StarRocksException("construction failed");
            }
        };

        Assertions.assertThrows(StarRocksException.class,
                () -> replicationMgr.addReplicationJob(newRequest()));

        Assertions.assertFalse(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));
        Assertions.assertTrue(replicationMgr.getRunningJobs().isEmpty());
    }

    @Test
    public void testInterruptedConstructionCleanupWaitsForContendedTableLock() throws Exception {
        CountDownLatch constructionEntered = new CountDownLatch(1);
        CountDownLatch tableLockHeld = new CountDownLatch(1);
        CountDownLatch failureInjected = new CountDownLatch(1);
        CountDownLatch releaseTableLock = new CountDownLatch(1);
        AtomicReference<StarRocksException> constructionFailure = new AtomicReference<>();
        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        AtomicReference<Throwable> holderFailure = new AtomicReference<>();
        AtomicBoolean producerInterruptedOnExit = new AtomicBoolean();
        ReplicationMgr replicationMgr = new ReplicationMgr() {
            @Override
            protected ReplicationJob createReplicationJob(TTableReplicationRequest request) throws StarRocksException {
                constructionEntered.countDown();
                try {
                    if (!tableLockHeld.await(5, TimeUnit.SECONDS)) {
                        throw new StarRocksException("timed out waiting for the contended table lock");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StarRocksException("interrupted before injecting the construction failure", e);
                }

                StarRocksException failure = new StarRocksException(
                        "replication construction interrupted", new InterruptedException("construction interrupted"));
                constructionFailure.set(failure);
                Thread.currentThread().interrupt();
                failureInjected.countDown();
                throw failure;
            }
        };

        Thread lockHolder = new Thread(() -> {
            try {
                if (!constructionEntered.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("replication construction did not start");
                }
                Locker locker = new Locker();
                locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
                try {
                    tableLockHeld.countDown();
                    if (!releaseTableLock.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release the table lock");
                    }
                } finally {
                    locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
                }
            } catch (Throwable t) {
                holderFailure.set(t);
                tableLockHeld.countDown();
            }
        });
        Thread producer = new Thread(() -> {
            try {
                replicationMgr.addReplicationJob(newRequest());
            } catch (Throwable t) {
                producerFailure.set(t);
            } finally {
                producerInterruptedOnExit.set(Thread.currentThread().isInterrupted());
            }
        });

        lockHolder.start();
        producer.start();
        try {
            Assertions.assertTrue(tableLockHeld.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(failureInjected.await(5, TimeUnit.SECONDS));
            awaitReplicationCleanupBlockedOrFinished(producer);
        } finally {
            releaseTableLock.countDown();
            lockHolder.join(5000L);
            producer.join(5000L);
        }

        Assertions.assertFalse(lockHolder.isAlive());
        Assertions.assertFalse(producer.isAlive());
        Assertions.assertNull(holderFailure.get());
        Assertions.assertSame(constructionFailure.get(), producerFailure.get());
        Assertions.assertTrue(producerInterruptedOnExit.get());
        Assertions.assertTrue(replicationMgr.getRunningJobs().isEmpty());
        Assertions.assertFalse(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));
    }

    @Test
    public void testCleanupTracksDbLockGrantedBeforeInterrupt() throws Exception {
        assertCleanupTracksGrantedLock(LockType.INTENTION_EXCLUSIVE);
    }

    @Test
    public void testCleanupTracksTableLockGrantedBeforeInterrupt() throws Exception {
        assertCleanupTracksGrantedLock(LockType.WRITE);
    }

    @Test
    public void testLiveInitChecksReplicationButReplayBypassesAdmission() throws Exception {
        ReplicationMgr replicationMgr = new ReplicationMgr();
        replicationMgr.replayReplicationJob(newReplicationJob());
        mockReplicationMgr(replicationMgr);
        SplitTabletJob splitJob = new SplitTabletJob(3, db.getId(), table.getId(), Map.of());
        MergeTabletJob mergeJob = new MergeTabletJob(4, db.getId(), table.getId(), Map.of());

        Assertions.assertThrows(StarRocksException.class, splitJob::init);
        Assertions.assertThrows(StarRocksException.class, mergeJob::init);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        splitJob.replayPendingJob();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        table.setState(OlapTable.OlapTableState.NORMAL);
        mergeJob.replayPendingJob();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
    }

    @Test
    public void testExternalSnapshotRejectsStaleTouchedGroupBeforeMutation() {
        List<SplitTabletJob.ExternalAdmissionGroup> groups = currentGroups();
        SplitTabletJob.ExternalAdmissionGroup touched = groups.get(0);
        Set<SplitTabletJob.ExternalAdmissionGroup> staleGroups = new HashSet<>(groups);
        staleGroups.remove(touched);
        staleGroups.add(new SplitTabletJob.ExternalAdmissionGroup(
                touched.physicalPartitionId(), touched.indexMetaId(), touched.currentIndexId() + 1));
        Map<Long, ReshardingPhysicalPartition> touchedOnly = new HashMap<>();
        touchedOnly.put(touched.physicalPartitionId(), null);
        SplitTabletJob job = new SplitTabletJob(5, db.getId(), table.getId(), touchedOnly);
        job.setExternalAdmissionSnapshot(new SplitTabletJob.ExternalAdmissionSnapshot(
                db.getId(), table.getId(), staleGroups));

        Assertions.assertThrows(StarRocksException.class, job::init);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testExternalSnapshotRejectsStaleSeparatelyAlignedGroupBeforeMutation() {
        List<SplitTabletJob.ExternalAdmissionGroup> groups = currentGroups();
        Assertions.assertTrue(groups.size() >= 2);
        SplitTabletJob.ExternalAdmissionGroup touched = groups.get(0);
        SplitTabletJob.ExternalAdmissionGroup aligned = groups.get(1);
        Set<SplitTabletJob.ExternalAdmissionGroup> staleGroups = new HashSet<>(groups);
        staleGroups.remove(aligned);
        staleGroups.add(new SplitTabletJob.ExternalAdmissionGroup(
                aligned.physicalPartitionId(), aligned.indexMetaId(), aligned.currentIndexId() + 1));
        Map<Long, ReshardingPhysicalPartition> touchedOnly = new HashMap<>();
        touchedOnly.put(touched.physicalPartitionId(), null);
        SplitTabletJob job = new SplitTabletJob(6, db.getId(), table.getId(), touchedOnly);
        job.setExternalAdmissionSnapshot(new SplitTabletJob.ExternalAdmissionSnapshot(
                db.getId(), table.getId(), staleGroups));

        Assertions.assertThrows(StarRocksException.class, job::init);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testExternalSnapshotAcceptsCompleteFreshGroupSet() throws Exception {
        SplitTabletJob job = new SplitTabletJob(7, db.getId(), table.getId(), Map.of());
        job.setExternalAdmissionSnapshot(new SplitTabletJob.ExternalAdmissionSnapshot(
                db.getId(), table.getId(), new HashSet<>(currentGroups())));

        job.init();
        Assertions.assertEquals(OlapTable.OlapTableState.TABLET_RESHARD, table.getState());
        job.rollbackInit();
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
    }

    @Test
    public void testExternalIdentityFieldsRoundTripWithNullDefaults() {
        SplitTabletJob job = new SplitTabletJob(8, db.getId(), table.getId(), Map.of());
        Assertions.assertNull(job.getExternalRequestId());
        Assertions.assertNull(job.getExternalFinalDigest());
        Assertions.assertNull(job.getExternalStepDigest());

        job.setExternalIdentity("request-1", "final-digest", "step-digest");
        String json = GsonUtils.GSON.toJson(job);
        SplitTabletJob restored = GsonUtils.GSON.fromJson(json, SplitTabletJob.class);

        Assertions.assertEquals("request-1", restored.getExternalRequestId());
        Assertions.assertEquals("final-digest", restored.getExternalFinalDigest());
        Assertions.assertEquals("step-digest", restored.getExternalStepDigest());
        Assertions.assertFalse(json.contains("externalAdmissionSnapshot"));

        SplitTabletJob oldJob = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(new SplitTabletJob(9, db.getId(), table.getId(), Map.of())),
                SplitTabletJob.class);
        Assertions.assertNull(oldJob.getExternalRequestId());
        Assertions.assertNull(oldJob.getExternalFinalDigest());
        Assertions.assertNull(oldJob.getExternalStepDigest());
    }

    private ReplicationMgr blockingReplicationMgr(
            CountDownLatch constructionEntered, CountDownLatch releaseConstruction) {
        return new ReplicationMgr() {
            @Override
            protected ReplicationJob createReplicationJob(TTableReplicationRequest request) throws StarRocksException {
                constructionEntered.countDown();
                try {
                    if (!releaseConstruction.await(5, TimeUnit.SECONDS)) {
                        throw new StarRocksException("timed out waiting to release replication construction");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StarRocksException("replication construction interrupted", e);
                }
                return newReplicationJob();
            }
        };
    }

    private void assertCleanupTracksGrantedLock(LockType promotedLockType) throws Exception {
        long promotedResourceId = promotedLockType == LockType.INTENTION_EXCLUSIVE ? db.getId() : table.getId();
        CountDownLatch constructionEntered = new CountDownLatch(1);
        CountDownLatch blockerLockHeld = new CountDownLatch(1);
        CountDownLatch failureInjected = new CountDownLatch(1);
        CountDownLatch releaseBlockerLock = new CountDownLatch(1);
        AtomicReference<Thread> producerThread = new AtomicReference<>();
        AtomicReference<Locker> promotedLocker = new AtomicReference<>();
        AtomicReference<StarRocksException> constructionFailure = new AtomicReference<>();
        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        AtomicReference<Throwable> holderFailure = new AtomicReference<>();
        AtomicBoolean interruptAfterGrantPending = new AtomicBoolean(true);
        AtomicBoolean producerInterruptedOnExit = new AtomicBoolean();
        new MockUp<Locker>() {
            @Mock
            public void lock(Invocation invocation, long rid, LockType lockType) throws LockException {
                boolean injectAfterGrant = Thread.currentThread() == producerThread.get()
                        && rid == promotedResourceId && lockType == promotedLockType
                        && interruptAfterGrantPending.compareAndSet(true, false);
                if (injectAfterGrant) {
                    promotedLocker.set(invocation.getInvokedInstance());
                }
                invocation.proceed(rid, lockType);
                if (injectAfterGrant) {
                    throw new LockInterruptException(new InterruptedException("interrupted after lock grant"));
                }
            }
        };
        ReplicationMgr replicationMgr = new ReplicationMgr() {
            @Override
            protected ReplicationJob createReplicationJob(TTableReplicationRequest request) throws StarRocksException {
                constructionEntered.countDown();
                try {
                    if (!blockerLockHeld.await(5, TimeUnit.SECONDS)) {
                        throw new StarRocksException("timed out waiting for the promotion blocker lock");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StarRocksException("interrupted before injecting the construction failure", e);
                }

                StarRocksException failure = new StarRocksException(
                        "replication construction interrupted", new InterruptedException("construction interrupted"));
                constructionFailure.set(failure);
                Thread.currentThread().interrupt();
                failureInjected.countDown();
                throw failure;
            }
        };
        Thread lockHolder = new Thread(() -> {
            try {
                if (!constructionEntered.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("replication construction did not start");
                }
                Locker locker = new Locker();
                acquirePromotionBlockerLock(locker, promotedLockType);
                try {
                    blockerLockHeld.countDown();
                    if (!releaseBlockerLock.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release the promotion blocker lock");
                    }
                } finally {
                    releasePromotionBlockerLock(locker, promotedLockType);
                }
            } catch (Throwable t) {
                holderFailure.set(t);
                blockerLockHeld.countDown();
            }
        });
        Thread producer = new Thread(() -> {
            producerThread.set(Thread.currentThread());
            try {
                replicationMgr.addReplicationJob(newRequest());
            } catch (Throwable t) {
                producerFailure.set(t);
            } finally {
                producerInterruptedOnExit.set(Thread.currentThread().isInterrupted());
            }
        });

        lockHolder.start();
        producer.start();
        try {
            try {
                Assertions.assertTrue(blockerLockHeld.await(5, TimeUnit.SECONDS));
                Assertions.assertTrue(failureInjected.await(5, TimeUnit.SECONDS));
                awaitReplicationCleanupBlockedOrFinished(producer);
                Assertions.assertTrue(producer.isAlive());
            } finally {
                releaseBlockerLock.countDown();
                lockHolder.join(5000L);
                producer.join(5000L);
            }

            Assertions.assertFalse(lockHolder.isAlive());
            Assertions.assertFalse(producer.isAlive());
            Assertions.assertNull(holderFailure.get());
            Assertions.assertFalse(interruptAfterGrantPending.get());
            Assertions.assertSame(constructionFailure.get(), producerFailure.get());
            Assertions.assertTrue(producerInterruptedOnExit.get());
            Assertions.assertTrue(replicationMgr.getRunningJobs().isEmpty());
            Assertions.assertFalse(replicationMgr.isTableUnderReplication(db.getId(), table.getId()));
            assertIndependentDbAndTableLocksAvailable();
        } finally {
            releaseBlockerLock.countDown();
            lockHolder.join(5000L);
            producer.join(5000L);
            releasePromotedLockLeak(promotedLocker.get());
        }
    }

    private void acquirePromotionBlockerLock(Locker locker, LockType promotedLockType) {
        if (promotedLockType == LockType.INTENTION_EXCLUSIVE) {
            locker.lockDatabase(db.getId(), LockType.WRITE);
        } else {
            locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
        }
    }

    private void releasePromotionBlockerLock(Locker locker, LockType promotedLockType) {
        if (promotedLockType == LockType.INTENTION_EXCLUSIVE) {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        } else {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
        }
    }

    private void assertIndependentDbAndTableLocksAvailable() {
        Locker locker = new Locker();
        boolean dbLockAcquired = locker.tryLockDatabase(db.getId(), LockType.WRITE, 1, TimeUnit.SECONDS);
        try {
            Assertions.assertTrue(dbLockAcquired, "cleanup leaked the database intention lock");
        } finally {
            if (dbLockAcquired) {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
        }

        boolean tableLockAcquired = locker.tryLockTableWithIntensiveDbLock(
                db.getId(), table.getId(), LockType.WRITE, 1, TimeUnit.SECONDS);
        try {
            Assertions.assertTrue(tableLockAcquired, "cleanup leaked the table write lock");
        } finally {
            if (tableLockAcquired) {
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
            }
        }
    }

    private void releasePromotedLockLeak(Locker locker) {
        if (locker == null) {
            return;
        }
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        while (lockManager.isOwner(table.getId(), locker, LockType.WRITE)) {
            locker.release(table.getId(), LockType.WRITE);
        }
        while (lockManager.isOwner(db.getId(), locker, LockType.INTENTION_EXCLUSIVE)) {
            locker.release(db.getId(), LockType.INTENTION_EXCLUSIVE);
        }
    }

    private void mockReplicationMgr(ReplicationMgr replicationMgr) {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public ReplicationMgr getReplicationMgr() {
                return replicationMgr;
            }
        };
    }

    private ReplicationJob newReplicationJob() {
        OlapTable sourceTable = DeepCopy.copyWithGson(table, OlapTable.class);
        for (PhysicalPartition sourcePartition : sourceTable.getAllPhysicalPartitions()) {
            sourcePartition.updateVersionForRestore(sourcePartition.getVisibleVersion() + 10);
        }
        return new ReplicationJob(null, "token", db.getId(), table, sourceTable,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
    }

    private TTableReplicationRequest newRequest() {
        TTableReplicationRequest request = new TTableReplicationRequest();
        request.setDatabase_id(db.getId());
        request.setTable_id(table.getId());
        request.setSrc_table_type(TTableType.OLAP_TABLE);
        return request;
    }

    private List<SplitTabletJob.ExternalAdmissionGroup> currentGroups() {
        List<SplitTabletJob.ExternalAdmissionGroup> groups = new ArrayList<>();
        for (PhysicalPartition physicalPartition : table.getAllPhysicalPartitions()) {
            for (MaterializedIndex index : physicalPartition
                    .getLatestMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                groups.add(new SplitTabletJob.ExternalAdmissionGroup(
                        physicalPartition.getId(), index.getMetaId(), index.getId()));
            }
        }
        return groups;
    }

    private void assertCatalogDisappearanceRejected(TabletReshardJob job) throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        AtomicInteger journalCount = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObjectOrThrow(short op, Object object,
                                             com.starrocks.persist.WALApplier applier) {
                journalCount.incrementAndGet();
                applier.apply(object);
            }
        };

        Thread producer = new Thread(() -> {
            try {
                jobMgr.addTabletReshardJob(job);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
        try {
            producer.start();
            awaitBlockedOnTableLock(producer);
            db.unRegisterTableUnlocked(table);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
        }

        producer.join(5000L);
        try {
            Assertions.assertFalse(producer.isAlive());
            Assertions.assertInstanceOf(StarRocksException.class, failure.get());
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            Assertions.assertEquals(0, journalCount.get());
            Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
        } finally {
            restoreTableRegistration();
        }
    }

    private void awaitBlockedOnTableLock(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            Thread.State state = thread.getState();
            if (state == Thread.State.BLOCKED || state == Thread.State.WAITING
                    || state == Thread.State.TIMED_WAITING) {
                return;
            }
            if (!thread.isAlive()) {
                Assertions.fail("reshard admission completed before the table lock was released");
            }
            Thread.sleep(10L);
        }
        Assertions.fail("reshard admission did not block on the table lock");
    }

    private void awaitReplicationCleanupBlockedOrFinished(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            if (!thread.isAlive()) {
                return;
            }
            Thread.State state = thread.getState();
            boolean inReservationCleanup = false;
            for (StackTraceElement frame : thread.getStackTrace()) {
                if (frame.getMethodName().startsWith("releaseConstructionReservation")) {
                    inReservationCleanup = true;
                    break;
                }
            }
            if (inReservationCleanup && (state == Thread.State.BLOCKED || state == Thread.State.WAITING
                    || state == Thread.State.TIMED_WAITING)) {
                return;
            }
            Thread.sleep(10L);
        }
        Assertions.fail("replication cleanup neither blocked on the table lock nor completed");
    }

    private void restoreTableRegistration() {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
        try {
            if (db.getTable(table.getId()) == null) {
                Assertions.assertTrue(db.registerTableUnlocked(table));
            }
            table.setState(OlapTable.OlapTableState.NORMAL);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
        }
    }

    private void setLeaderWorkAdmissionOpen(boolean open) throws Exception {
        Field field = GlobalStateMgr.class.getDeclaredField("leaderWorkAdmissionOpen");
        field.setAccessible(true);
        AtomicBoolean admissionOpen = (AtomicBoolean) field.get(GlobalStateMgr.getCurrentState());
        admissionOpen.set(open);
    }
}
