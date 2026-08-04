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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalWriteException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.thrift.TTabletReshardJobsItem;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TabletReshardJobMgrTest {
    public static class TestNormalTabletReshardJob extends TabletReshardJob {

        private long tableId = 0;
        private int initCount;
        private int rollbackCount;

        public TestNormalTabletReshardJob(long jobId, TabletReshardJob.JobType jobType) {
            super(jobId, jobType);
        }

        public void setTableId(long tableId) {
            this.tableId = tableId;
        }

        public void markFinished(long finishedTimeMs) {
            this.jobState = JobState.FINISHED;
            this.finishedTimeMs = finishedTimeMs;
        }

        @Override
        public long getTableId() {
            return tableId;
        }

        @Override
        public long getParallelTablets() {
            return 10;
        }

        @Override
        public void init() throws StarRocksException {
            initCount++;
        }

        @Override
        public void rollbackInit() throws StarRocksException {
            rollbackCount++;
        }

        @Override
        protected void runPendingJob() {
            setJobState(JobState.PREPARING);
        }

        @Override
        protected void runPreparingJob() {
            setJobState(JobState.RUNNING);
        }

        @Override
        protected void runRunningJob() {
            setJobState(JobState.CLEANING);
        }

        @Override
        protected void runCleaningJob() {
            setJobState(JobState.FINISHED);
        }

        @Override
        protected void runFinishedJob() {

        }

        @Override
        protected void runAbortingJob() {
            setJobState(JobState.ABORTED);
        }

        @Override
        protected void runAbortedJob() {

        }

        @Override
        protected boolean canAbort() {
            return true;
        }

        @Override
        public void replay() {
            return;
        }

        @Override
        protected void replayPendingJob() {
            throw new UnsupportedOperationException("Unimplemented method 'replayPendingJob'");
        }

        @Override
        protected void replayPreparingJob() {
            throw new UnsupportedOperationException("Unimplemented method 'replayPreparingJob'");
        }

        @Override
        protected void replayRunningJob() {
            throw new UnsupportedOperationException("Unimplemented method 'replayRunningJob'");
        }

        @Override
        protected void replayCleaningJob() {
            throw new UnsupportedOperationException("Unimplemented method 'replayCleaningJob'");
        }

        @Override
        protected void replayFinishedJob() {
            throw new UnsupportedOperationException("Unimplemented method 'replayFinishedJob'");
        }

        @Override
        protected void replayAbortingJob() {
            throw new UnsupportedOperationException("Unimplemented method 'replayAbortingJob'");
        }

        @Override
        protected void replayAbortedJob() {
            throw new UnsupportedOperationException("Unimplemented method 'replayAbortedJob'");
        }

        @Override
        protected void registerReshardingTabletsOnRestart() {
            throw new UnsupportedOperationException("Unimplemented method 'registerReshardingTabletsOnRestart'");
        }

        @Override
        public TTabletReshardJobsItem getInfo() {
            return new TTabletReshardJobsItem();
        }
    }

    public static class TestAbnormalTabletReshardJob extends TestNormalTabletReshardJob {

        public TestAbnormalTabletReshardJob(long jobId, TabletReshardJob.JobType jobType) {
            super(jobId, jobType);
        }

        @Override
        protected void runPreparingJob() {
            abort("Abort");
        }

        @Override
        public long getParallelTablets() {
            return Config.tablet_reshard_max_parallel_tablets / 2;
        }
    }

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database reshardDb;
    private static OlapTable reshardTable;
    private static LakeTablet oversizedTablet;

    @BeforeEach
    public void clearSharedMgr() throws Exception {
        // Tests that use the singleton TabletReshardJobMgr share its state. Clear it before each
        // test so a job created in one test does not block table-reservation checks in the next.
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().tabletReshardJobs.clear();
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().clearSizeSplitLatchForTest();
        // Reset the table state: a split job created by a previous test transitions the table to
        // TABLET_RESHARD via init(), and triggerTabletReshard short-circuits on non-NORMAL.
        if (reshardTable != null) {
            reshardTable.setState(OlapTable.OlapTableState.NORMAL);
        }
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        setLeaderWorkAdmissionOpen(true);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("reshard_test_db").useDatabase("reshard_test_db");
        reshardDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("reshard_test_db");

        String sql = "create table reshard_trigger_table (key1 int, key2 varchar(10))\n" +
                "order by(key1)\n" +
                "properties('replication_num' = '1');";
        starRocksAssert.withTable(sql);
        reshardTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(reshardDb.getFullName(), "reshard_trigger_table");

        // Grab the single tablet from the default partition.
        Tablet t = reshardTable.getPartitions().iterator().next()
                .getDefaultPhysicalPartition().getLatestBaseIndex().getTablets().get(0);
        oversizedTablet = (LakeTablet) t;
    }

    @Test
    public void testTabletReshardJobMgr() throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();

        TestNormalTabletReshardJob normalJob = new TestNormalTabletReshardJob(1, null);
        jobMgr.addTabletReshardJob(normalJob);

        TestAbnormalTabletReshardJob abnormalJob = new TestAbnormalTabletReshardJob(2, null);
        jobMgr.addTabletReshardJob(abnormalJob);

        TestAbnormalTabletReshardJob abnormalJob2 = new TestAbnormalTabletReshardJob(3, null);
        Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addTabletReshardJob(abnormalJob2));

        Assertions.assertEquals(2, jobMgr.getTabletReshardJobs().size());
        Assertions.assertEquals(normalJob.getParallelTablets() + abnormalJob.getParallelTablets(),
                jobMgr.getTotalParallelTablets());

        jobMgr.runAfterLeaseValid();

        Assertions.assertEquals(TabletReshardJob.JobState.FINISHED, normalJob.getJobState());
        Assertions.assertEquals(TabletReshardJob.JobState.ABORTED, abnormalJob.getJobState());
        Assertions.assertTrue(normalJob.isDone());
        Assertions.assertTrue(abnormalJob.isDone());
        Assertions.assertFalse(normalJob.isExpired());
        Assertions.assertFalse(abnormalJob.isExpired());
        Assertions.assertEquals(2, jobMgr.getTabletReshardJobs().size());
        Assertions.assertEquals(0, jobMgr.getTotalParallelTablets());

        abnormalJob.finishedTimeMs = 0;
        Assertions.assertTrue(abnormalJob.isExpired());

        jobMgr.runAfterLeaseValid();

        Assertions.assertEquals(1, jobMgr.getTabletReshardJobs().size());
    }

    @Test
    public void testAddJobRejectedWhenInitFails() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();

        // A job whose init() (admission-time table reservation) fails must not be queued.
        TabletReshardJob job = new TestNormalTabletReshardJob(1, null) {
            @Override
            public void init() throws StarRocksException {
                throw new StarRocksException("table not reservable");
            }
        };

        Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addTabletReshardJob(job));
        Assertions.assertNull(jobMgr.getTabletReshardJob(1));
        Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
    }

    @Test
    public void testAddJobRejectsClosedLeaderWorkBeforeInit() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        TestNormalTabletReshardJob job =
                new TestNormalTabletReshardJob(11, TabletReshardJob.JobType.SPLIT_TABLET);
        mockLeaderAdmissionClosed();

        Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addTabletReshardJob(job));

        Assertions.assertEquals(0, job.initCount);
        Assertions.assertEquals(0, job.rollbackCount);
        Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
    }

    @Test
    public void testAddJobPublishesOnlyInWalApplier() throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        TestNormalTabletReshardJob job =
                new TestNormalTabletReshardJob(12, TabletReshardJob.JobType.SPLIT_TABLET);
        AtomicReference<WALApplier> capturedApplier = new AtomicReference<>();
        AtomicReference<Object> capturedObject = new AtomicReference<>();
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObjectOrThrow(short op, Object object, WALApplier applier) {
                Assertions.assertEquals(OperationType.OP_UPDATE_TABLET_RESHARD_JOB_LOG, op);
                capturedObject.set(object);
                capturedApplier.set(applier);
            }
        };

        jobMgr.addTabletReshardJob(job);

        Assertions.assertEquals(1, job.initCount);
        Assertions.assertEquals(0, job.rollbackCount);
        Assertions.assertNull(jobMgr.getTabletReshardJob(job.getJobId()));
        Assertions.assertSame(job, capturedObject.get());
        Assertions.assertNotNull(capturedApplier.get());

        capturedApplier.get().apply(job);
        Assertions.assertSame(job, jobMgr.getTabletReshardJob(job.getJobId()));
    }

    @Test
    public void testAddJobRollsBackInitOnDefiniteJournalAbort() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        TestNormalTabletReshardJob job =
                new TestNormalTabletReshardJob(13, TabletReshardJob.JobType.MERGE_TABLET);
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObjectOrThrow(short op, Object object, WALApplier applier)
                    throws JournalWriteException {
                throw new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED, "writer aborted");
            }
        };

        StarRocksException exception = Assertions.assertThrows(
                StarRocksException.class, () -> jobMgr.addTabletReshardJob(job));

        Assertions.assertTrue(exception.getMessage().contains("writer aborted"));
        Assertions.assertEquals(1, job.initCount);
        Assertions.assertEquals(1, job.rollbackCount);
        Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
    }

    @Test
    public void testAddJobRollsBackInitOnPreEnqueueInterrupt() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        TestNormalTabletReshardJob job =
                new TestNormalTabletReshardJob(14, TabletReshardJob.JobType.SPLIT_TABLET);
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObjectOrThrow(short op, Object object, WALApplier applier)
                    throws InterruptedException {
                throw new InterruptedException("before enqueue");
            }
        };

        try {
            Assertions.assertThrows(StarRocksException.class, () -> jobMgr.addTabletReshardJob(job));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        Assertions.assertEquals(1, job.initCount);
        Assertions.assertEquals(1, job.rollbackCount);
        Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
    }

    @Test
    public void testPreEnqueueInterruptRollsBackRealSplitWhileTableLockContended() throws Exception {
        assertInterruptedJournalFailureRollsBack(
                new SplitTabletJob(140, reshardDb.getId(), reshardTable.getId(), Map.of()), true);
    }

    @Test
    public void testPostEnqueueAbortRollsBackRealMergeWhileTableLockContended() throws Exception {
        assertInterruptedJournalFailureRollsBack(
                new MergeTabletJob(141, reshardDb.getId(), reshardTable.getId(), Map.of()), false);
    }

    @Test
    public void testRollbackFailureIsSuppressedUnderOriginalJournalFailure() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        TestNormalTabletReshardJob job = new TestNormalTabletReshardJob(
                142, TabletReshardJob.JobType.SPLIT_TABLET) {
            @Override
            public void rollbackInit() {
                throw new IllegalStateException("rollback lock failed");
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObjectOrThrow(short op, Object object, WALApplier applier)
                    throws JournalWriteException {
                throw new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED, "writer aborted");
            }
        };

        StarRocksException failure = Assertions.assertThrows(
                StarRocksException.class, () -> jobMgr.addTabletReshardJob(job));

        Assertions.assertInstanceOf(JournalWriteException.class, failure.getCause());
        Assertions.assertEquals(1, failure.getSuppressed().length);
        Assertions.assertInstanceOf(IllegalStateException.class, failure.getSuppressed()[0]);
        Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
    }

    @Test
    public void testCapacityReservationIncludesWalPendingJob() throws Exception {
        long originalLimit = Config.tablet_reshard_max_parallel_tablets;
        Config.tablet_reshard_max_parallel_tablets = 15;
        try {
            TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
            TestNormalTabletReshardJob first = new TestNormalTabletReshardJob(
                    150, TabletReshardJob.JobType.SPLIT_TABLET);
            TestNormalTabletReshardJob second = new TestNormalTabletReshardJob(
                    151, TabletReshardJob.JobType.MERGE_TABLET);
            CountDownLatch firstJournalEntered = new CountDownLatch(1);
            CountDownLatch releaseFirstJournal = new CountDownLatch(1);
            AtomicInteger journalCount = new AtomicInteger();
            AtomicReference<Throwable> firstFailure = new AtomicReference<>();
            new MockUp<EditLog>() {
                @Mock
                public void logJsonObjectOrThrow(short op, Object object, WALApplier applier)
                        throws InterruptedException {
                    int call = journalCount.incrementAndGet();
                    if (call == 1) {
                        firstJournalEntered.countDown();
                        if (!releaseFirstJournal.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to release first journal append");
                        }
                    }
                    applier.apply(object);
                }
            };

            Thread firstProducer = new Thread(() -> {
                try {
                    jobMgr.addTabletReshardJob(first);
                } catch (Throwable t) {
                    firstFailure.set(t);
                }
            });
            firstProducer.start();
            Assertions.assertTrue(firstJournalEntered.await(5, TimeUnit.SECONDS));
            try {
                Assertions.assertThrows(StarRocksException.class,
                        () -> jobMgr.addTabletReshardJob(second));
            } finally {
                releaseFirstJournal.countDown();
                firstProducer.join(5000L);
            }

            Assertions.assertFalse(firstProducer.isAlive());
            Assertions.assertNull(firstFailure.get());
            Assertions.assertEquals(1, journalCount.get());
            Assertions.assertEquals(0, second.initCount);
            Assertions.assertEquals(1, jobMgr.getTabletReshardJobs().size());
            Assertions.assertSame(first, jobMgr.getTabletReshardJob(first.getJobId()));
        } finally {
            Config.tablet_reshard_max_parallel_tablets = originalLimit;
        }
    }

    @Test
    public void testFailedJournalReleasesCapacityReservation() throws Exception {
        long originalLimit = Config.tablet_reshard_max_parallel_tablets;
        Config.tablet_reshard_max_parallel_tablets = 15;
        try {
            TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
            TestNormalTabletReshardJob failed = new TestNormalTabletReshardJob(
                    152, TabletReshardJob.JobType.SPLIT_TABLET);
            TestNormalTabletReshardJob admitted = new TestNormalTabletReshardJob(
                    153, TabletReshardJob.JobType.MERGE_TABLET);
            AtomicInteger journalCount = new AtomicInteger();
            new MockUp<EditLog>() {
                @Mock
                public void logJsonObjectOrThrow(short op, Object object, WALApplier applier)
                        throws JournalWriteException {
                    if (journalCount.incrementAndGet() == 1) {
                        throw new JournalWriteException(
                                JournalWriteException.Reason.WRITER_ABORTED, "writer aborted");
                    }
                    applier.apply(object);
                }
            };

            Assertions.assertThrows(StarRocksException.class,
                    () -> jobMgr.addTabletReshardJob(failed));
            jobMgr.addTabletReshardJob(admitted);

            Assertions.assertEquals(2, journalCount.get());
            Assertions.assertEquals(1, failed.rollbackCount);
            Assertions.assertEquals(1, jobMgr.getTabletReshardJobs().size());
            Assertions.assertSame(admitted, jobMgr.getTabletReshardJob(admitted.getJobId()));
        } finally {
            Config.tablet_reshard_max_parallel_tablets = originalLimit;
        }
    }

    @Test
    public void testGetTabletReshardJobsInfo() throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();

        TestNormalTabletReshardJob job1 = new TestNormalTabletReshardJob(1, null);
        TestNormalTabletReshardJob job2 = new TestNormalTabletReshardJob(2, null);
        jobMgr.addTabletReshardJob(job1);
        jobMgr.addTabletReshardJob(job2);

        Assertions.assertEquals(2, jobMgr.getAllJobsInfo().getItems().size());
    }

    @Test
    public void testIsTableSafeToDeleteTabletWithReshardJob() {
        long tableId = 918273L;

        // Use a local mgr injected via GlobalStateMgr so the shared singleton's background daemon
        // does not reap the job under test.
        TabletReshardJobMgr localMgr = new TabletReshardJobMgr();
        TestNormalTabletReshardJob job = new TestNormalTabletReshardJob(1, TabletReshardJob.JobType.SPLIT_TABLET);
        job.setTableId(tableId);
        job.jobState = TabletReshardJob.JobState.FINISHED;
        job.finishedTimeMs = 1000L;
        localMgr.tabletReshardJobs.put(job.getJobId(), job);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public TabletReshardJobMgr getTabletReshardJobMgr() {
                return localMgr;
            }
        };

        final boolean[] automatedOn = {false};
        final long[] boundary = {0L};
        new MockUp<ClusterSnapshotMgr>() {
            @Mock
            public boolean isAutomatedSnapshotOn() {
                return automatedOn[0];
            }

            @Mock
            public long getSafeDeletionTimeMs() {
                return boundary[0];
            }
        };

        ClusterSnapshotMgr csMgr = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();

        // Automated snapshot off: no behavior change, always safe.
        automatedOn[0] = false;
        Assertions.assertTrue(csMgr.isTableSafeToDeleteTablet(tableId));

        // Automated on, reshard finished at/after the safe-deletion boundary (a snapshot covers the
        // pre-reshard state): the table's tablets must not be reclaimed.
        automatedOn[0] = true;
        boundary[0] = 1000L;
        Assertions.assertFalse(csMgr.isTableSafeToDeleteTablet(tableId));
        // A different table with no reshard job is unaffected.
        Assertions.assertTrue(csMgr.isTableSafeToDeleteTablet(tableId + 1));

        // Automated on, reshard finished before the boundary (every covering snapshot expired): safe.
        boundary[0] = 2000L;
        Assertions.assertTrue(csMgr.isTableSafeToDeleteTablet(tableId));

        // A still-running reshard job keeps the table unsafe while automated snapshot is on.
        job.jobState = TabletReshardJob.JobState.RUNNING;
        Assertions.assertFalse(csMgr.isTableSafeToDeleteTablet(tableId));

        // An ABORTED reshard removed no tablets, so it must NOT pin the table even when it finished
        // at/after the safe-deletion boundary (its partial orphan shards stay reclaimable).
        job.jobState = TabletReshardJob.JobState.ABORTED;
        boundary[0] = 1000L; // == job.finishedTimeMs, so only the aborted-state guard prevents pinning
        Assertions.assertTrue(csMgr.isTableSafeToDeleteTablet(tableId));
    }

    @Test
    public void testExpiredReshardJobRetainedUntilSnapshotSafe() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();

        TestNormalTabletReshardJob job = new TestNormalTabletReshardJob(1, TabletReshardJob.JobType.SPLIT_TABLET);
        job.setTableId(100L);
        job.jobState = TabletReshardJob.JobState.FINISHED;
        job.finishedTimeMs = 1000L; // deep in the past -> isExpired() is true
        jobMgr.tabletReshardJobs.put(job.getJobId(), job);
        Assertions.assertTrue(job.isExpired());

        final long[] boundary = {0L};
        new MockUp<ClusterSnapshotMgr>() {
            @Mock
            public long getSafeDeletionTimeMs() {
                return boundary[0];
            }
        };

        // A covering snapshot (safe-deletion boundary <= finishedTimeMs): the expired job is retained
        // so isTableSafeToDeleteTablet() keeps the pre-reshard tablets alive.
        boundary[0] = 500L;
        jobMgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, jobMgr.getTabletReshardJobs().size());

        // No covering snapshot (boundary > finishedTimeMs): the expired job is reaped (no leak).
        boundary[0] = 2000L;
        jobMgr.runAfterCatalogReadyForTest();
        Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
    }

    private void assertInterruptedJournalFailureRollsBack(
            TabletReshardJob job, boolean preEnqueueInterrupt) throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        CountDownLatch journalEntered = new CountDownLatch(1);
        CountDownLatch tableLockHeld = new CountDownLatch(1);
        CountDownLatch journalFailureInjected = new CountDownLatch(1);
        CountDownLatch releaseTableLock = new CountDownLatch(1);
        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        AtomicReference<Throwable> holderFailure = new AtomicReference<>();
        AtomicBoolean interruptedOnExit = new AtomicBoolean();
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObjectOrThrow(short op, Object object, WALApplier applier)
                    throws InterruptedException, JournalWriteException {
                journalEntered.countDown();
                if (!tableLockHeld.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting for contended table lock");
                }
                journalFailureInjected.countDown();
                if (preEnqueueInterrupt) {
                    throw new InterruptedException("before enqueue");
                }
                Thread.currentThread().interrupt();
                throw new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED, "writer aborted");
            }
        };

        Thread lockHolder = new Thread(() -> {
            Locker locker = new Locker();
            boolean locked = false;
            try {
                if (!journalEntered.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("journal append was not reached");
                }
                locker.lockTableWithIntensiveDbLock(
                        reshardDb.getId(), reshardTable.getId(), LockType.WRITE);
                locked = true;
                tableLockHeld.countDown();
                if (!releaseTableLock.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting to release table lock");
                }
            } catch (Throwable t) {
                holderFailure.set(t);
            } finally {
                if (locked) {
                    locker.unLockTableWithIntensiveDbLock(
                            reshardDb.getId(), reshardTable.getId(), LockType.WRITE);
                }
            }
        });
        Thread producer = new Thread(() -> {
            try {
                jobMgr.addTabletReshardJob(job);
            } catch (Throwable t) {
                producerFailure.set(t);
            } finally {
                interruptedOnExit.set(Thread.currentThread().isInterrupted());
            }
        });

        lockHolder.start();
        producer.start();
        try {
            Assertions.assertTrue(tableLockHeld.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(journalFailureInjected.await(5, TimeUnit.SECONDS));
            awaitRollbackBlockedOrFinished(producer);
        } finally {
            releaseTableLock.countDown();
            producer.join(5000L);
            lockHolder.join(5000L);
        }

        Assertions.assertFalse(producer.isAlive());
        Assertions.assertFalse(lockHolder.isAlive());
        Assertions.assertNull(holderFailure.get());
        StarRocksException failure = Assertions.assertInstanceOf(
                StarRocksException.class, producerFailure.get());
        if (preEnqueueInterrupt) {
            Assertions.assertInstanceOf(InterruptedException.class, failure.getCause());
        } else {
            Assertions.assertInstanceOf(JournalWriteException.class, failure.getCause());
        }
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, reshardTable.getState());
        Assertions.assertTrue(jobMgr.getTabletReshardJobs().isEmpty());
        Assertions.assertTrue(interruptedOnExit.get());
    }

    private void awaitRollbackBlockedOrFinished(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            Thread.State state = thread.getState();
            if (!thread.isAlive()) {
                return;
            }
            boolean inRollbackLock = false;
            for (StackTraceElement frame : thread.getStackTrace()) {
                if (frame.getMethodName().equals("rollbackInit")
                        || frame.getClassName().endsWith("AutoCloseableLock")) {
                    inRollbackLock = true;
                    break;
                }
            }
            if (inRollbackLock && (state == Thread.State.BLOCKED || state == Thread.State.WAITING
                    || state == Thread.State.TIMED_WAITING)) {
                return;
            }
            Thread.sleep(10L);
        }
        Assertions.fail("producer neither blocked on the rollback lock nor completed");
    }

    private void mockLeaderAdmissionOpen() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public boolean isLeaderWorkAdmissionOpen() {
                return true;
            }
        };
    }

    private void mockLeaderAdmissionClosed() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return false;
            }

            @Mock
            public boolean isLeaderWorkAdmissionOpen() {
                return false;
            }
        };
    }

    private void setLeaderWorkAdmissionOpen(boolean open) throws Exception {
        Field field = GlobalStateMgr.class.getDeclaredField("leaderWorkAdmissionOpen");
        field.setAccessible(true);
        AtomicBoolean admissionOpen = (AtomicBoolean) field.get(GlobalStateMgr.getCurrentState());
        admissionOpen.set(open);
    }

    @Test
    public void testAddAndDrainReshardCandidate() throws Exception {
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);

        mockLeaderAdmissionOpen();

        Assertions.assertTrue(reshardTable.isRangeDistribution(),
                "test table must be range-distributed; update DDL or Config if this fires");

        int before = mgr.getTabletReshardJobs().size();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(before + 1, mgr.getTabletReshardJobs().size());
    }

    @Test
    public void testTickSkippedWhenAdmissionClosed() throws Exception {
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);

        // Phase 1: admission open — pre-seed one candidate so the set is non-empty.
        mockLeaderAdmissionOpen();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        Assertions.assertEquals(1, mgr.getReshardCandidateCount(),
                "pre-seeded candidate should be present while admission is open");

        // Phase 2: flip admission closed — runAfterCatalogReady must clear the candidate set
        // without creating any new reshard job.
        mockLeaderAdmissionClosed();
        int before = mgr.getTabletReshardJobs().size();
        mgr.runAfterCatalogReadyForTest();                                    // gated: drain + jobs skipped, candidates cleared
        Assertions.assertEquals(before, mgr.getTabletReshardJobs().size(),
                "no new reshard job should be created when admission is closed");
        Assertions.assertEquals(0, mgr.getReshardCandidateCount(),
                "candidate set must be cleared when admission is closed");
    }

    @Test
    public void testAddReshardCandidateDropsNonActionableSignal() throws Exception {
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        mockLeaderAdmissionOpen();
        // A tablet below the split threshold with no merge signal is not actionable: even with
        // admission open, addReshardCandidate must drop it rather than queue a no-op candidate.
        long belowSplit = TabletReshardUtils.splitThreshold(Config.tablet_reshard_target_size) - 1;
        int before = mgr.getReshardCandidateCount();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), belowSplit, Long.MAX_VALUE);
        Assertions.assertEquals(before, mgr.getReshardCandidateCount(),
                "non-actionable signal (below split threshold, no merge) must not be queued");
    }

    @Test
    public void testAddReshardCandidateCoalescesPerTable() throws Exception {
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        mockLeaderAdmissionOpen();
        long target = Config.tablet_reshard_target_size;
        // First actionable mark seeds the table's candidate.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), target * 2, Long.MAX_VALUE);
        int afterFirst = mgr.getReshardCandidateCount();
        // A second mark for the same table must coalesce (exercise the merge remap), not add a new entry.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), target * 4, Long.MAX_VALUE);
        Assertions.assertEquals(afterFirst, mgr.getReshardCandidateCount(),
                "repeated marks for one table coalesce into a single candidate");
    }

    @Test
    public void testAddAndDrainMergeCandidate() throws Exception {
        // The unified candidate queue carries the merge signal too: a sub-threshold adjacent-pair sum
        // (no split signal) marked via addReshardCandidate must be routed to a merge job by the drain.
        boolean[] mergeCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause) {
                mergeCalled[0] = true;
            }
        };

        mockLeaderAdmissionOpen();
        Assertions.assertTrue(reshardTable.isRangeDistribution(),
                "test table must be range-distributed; update DDL or Config if this fires");

        // pair sum strictly below mergePairThreshold = ceil(0.8 * target) -> needMerge, needSplit(0) false
        long mergePairBelowThreshold =
                TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, mergePairBelowThreshold);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertTrue(mergeCalled[0],
                "drain must route a sub-threshold merge candidate to a merge job");
    }

    @Test
    public void testAutoSplitSuppressedAfterNoProgress() throws Exception {
        int[] splitCalls = {0};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                splitCalls[0]++;
                TestNormalTabletReshardJob job =
                        new TestNormalTabletReshardJob(1000L + splitCalls[0], TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        mockLeaderAdmissionOpen();
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        // Round 1: first over-threshold observation -> one split job fired, table latched.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "first over-threshold scan must fire exactly one split");
        Assertions.assertTrue(mgr.hasSizeSplitLatch(reshardTable.getId()), "table must be latched after firing");

        // Round 2: same size -> same signature (identical fallback leaves layout + requested count unchanged) -> suppressed.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "an unchanged split request must not re-fire (loop broken)");
    }

    @Test
    public void testAutoSplitReArmsOnDataChange() throws Exception {
        int[] splitCalls = {0};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                splitCalls[0]++;
                TestNormalTabletReshardJob job =
                        new TestNormalTabletReshardJob(2000L + splitCalls[0], TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        mockLeaderAdmissionOpen();
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "suppressed while nothing changed");

        // Simulate a load: advance dataVersion -> signature changes -> latch re-arms.
        com.starrocks.catalog.PhysicalPartition pp = reshardTable.getPartitions().iterator().next()
                .getDefaultPhysicalPartition();
        pp.setDataVersion(pp.getDataVersion() + 1);

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(2, splitCalls[0], "a data change (dataVersion bump) must re-arm the split");
    }

    @Test
    public void testAutoSplitReArmsOnSplitCountChange() throws Exception {
        int[] splitCalls = {0};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                splitCalls[0]++;
                TestNormalTabletReshardJob job =
                        new TestNormalTabletReshardJob(2500L + splitCalls[0], TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };
        mockLeaderAdmissionOpen();
        long originalTarget = Config.tablet_reshard_target_size;
        try {
            TabletReshardJobMgr mgr = new TabletReshardJobMgr();
            long big = originalTarget * 8; // calcSplitCount ~ 8
            oversizedTablet.setDataSize(big);
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), big, Long.MAX_VALUE);
            mgr.runAfterCatalogReadyForTest();
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), big, Long.MAX_VALUE);
            mgr.runAfterCatalogReadyForTest();
            Assertions.assertEquals(1, splitCalls[0], "suppressed at unchanged split count");

            // Raise the target so calcSplitCount(big, target) drops -> requested split count changes -> re-arm.
            Config.tablet_reshard_target_size = big / 2; // calcSplitCount ~ 2, different from ~8
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), big, Long.MAX_VALUE);
            mgr.runAfterCatalogReadyForTest();
            Assertions.assertEquals(2, splitCalls[0], "a changed requested split count must re-arm the split");
        } finally {
            Config.tablet_reshard_target_size = originalTarget;
        }
    }

    @Test
    public void testAutoSplitReArmsOnConfigChangeWhenCountCapped() throws Exception {
        int[] splitCalls = {0};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                splitCalls[0]++;
                TestNormalTabletReshardJob job =
                        new TestNormalTabletReshardJob(2700L + splitCalls[0], TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };
        mockLeaderAdmissionOpen();
        long originalTarget = Config.tablet_reshard_target_size;
        int originalMax = Config.tablet_reshard_max_split_count;
        try {
            TabletReshardJobMgr mgr = new TabletReshardJobMgr();
            // Cap the count at 2; huge tablet so calcSplitCount is pinned at the cap regardless of target.
            Config.tablet_reshard_max_split_count = 2;
            long huge = originalTarget * 100;
            oversizedTablet.setDataSize(huge);
            // sanity: count is capped at 2 for both target values used below
            Assertions.assertEquals(2, TabletReshardUtils.calcSplitCount(huge, originalTarget));
            Assertions.assertEquals(2, TabletReshardUtils.calcSplitCount(huge, originalTarget * 3));

            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), huge, Long.MAX_VALUE);
            mgr.runAfterCatalogReadyForTest();
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), huge, Long.MAX_VALUE);
            mgr.runAfterCatalogReadyForTest();
            Assertions.assertEquals(1, splitCalls[0], "suppressed while config + layout unchanged (count capped)");

            // Change target: the max count stays capped at 2, but the raw target config in the fingerprint changes.
            Config.tablet_reshard_target_size = originalTarget * 3;
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), huge, Long.MAX_VALUE);
            mgr.runAfterCatalogReadyForTest();
            Assertions.assertEquals(2, splitCalls[0],
                    "a config change must re-arm even when the max tablet's computed count is capped/unchanged");
        } finally {
            Config.tablet_reshard_target_size = originalTarget;
            Config.tablet_reshard_max_split_count = originalMax;
        }
    }

    @Test
    public void testSplitPlanSignatureAvoids32BitCollision() {
        long originalTarget = Config.tablet_reshard_target_size;
        int originalMax = Config.tablet_reshard_max_split_count;
        try {
            Config.tablet_reshard_max_split_count = 2; // pin computed count at the cap for a huge tablet
            long huge = 1024L * 1024 * 1024 * 1024; // 1 TiB, far over any target's split threshold

            // Two DISTINCT targets that collide under Long.hashCode (hence under Objects.hash):
            //   Long.hashCode(10737418240L) == Long.hashCode(2147483650L) == -2147483646
            long targetA = 10737418240L; // 10 GiB
            long targetB = 2147483650L;  // ~2 GiB
            Assertions.assertEquals(Long.hashCode(targetA), Long.hashCode(targetB),
                    "precondition: targets collide under Long.hashCode");
            Assertions.assertEquals(2, TabletReshardUtils.calcSplitCount(huge, targetA));
            Assertions.assertEquals(2, TabletReshardUtils.calcSplitCount(huge, targetB));
            Assertions.assertEquals(java.util.Objects.hash(targetA, 2, 2), java.util.Objects.hash(targetB, 2, 2),
                    "the old 32-bit Objects.hash fingerprint would have collided on these inputs");

            Config.tablet_reshard_target_size = targetA;
            long sigA = TabletReshardJobMgr.splitPlanSignature(huge);
            Config.tablet_reshard_target_size = targetB;
            long sigB = TabletReshardJobMgr.splitPlanSignature(huge);
            Assertions.assertNotEquals(sigA, sigB,
                    "the 64-bit murmur3 fingerprint must distinguish targets that collide under Objects.hash");
        } finally {
            Config.tablet_reshard_target_size = originalTarget;
            Config.tablet_reshard_max_split_count = originalMax;
        }
    }

    @Test
    public void testAutoSplitLatchForgottenBelowThreshold() throws Exception {
        boolean[] splitFired = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                splitFired[0] = true;
                TestNormalTabletReshardJob job = new TestNormalTabletReshardJob(3000L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause) {
                // no-op: keep the table state unchanged so the drain reaches triggerTabletReshard
            }
        };

        mockLeaderAdmissionOpen();
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertTrue(splitFired[0], "initial split must fire");
        Assertions.assertTrue(mgr.hasSizeSplitLatch(reshardTable.getId()), "latched after firing");

        // A drain where needSplit is false (only a merge signal) must forget the size-split latch entry.
        long mergePairBelowThreshold = TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, mergePairBelowThreshold);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertFalse(mgr.hasSizeSplitLatch(reshardTable.getId()),
                "dropping below the split threshold must forget the latch so future growth re-arms");
    }

    @Test
    public void testAutoSplitCreationFailureRetries() throws Exception {
        int[] attempts = {0};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) throws StarRocksException {
                attempts[0]++;
                throw new StarRocksException("transient create failure");
            }
        };
        mockLeaderAdmissionOpen();
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();

        Assertions.assertEquals(2, attempts[0], "a creation failure records nothing and must retry, not suppress");
        Assertions.assertFalse(mgr.hasSizeSplitLatch(reshardTable.getId()), "no latch entry on a failed creation");
    }

    @Test
    public void testAdmissionClosedClearsSizeSplitLatch() throws Exception {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                TestNormalTabletReshardJob job = new TestNormalTabletReshardJob(3500L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        mockLeaderAdmissionOpen();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertTrue(mgr.hasSizeSplitLatch(reshardTable.getId()), "latched after firing");

        // Demotion: the admission-closed tick must clear the latch (bounding it to a leader tenure).
        mockLeaderAdmissionClosed();
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertFalse(mgr.hasSizeSplitLatch(reshardTable.getId()),
                "leader demotion must clear the size-split latch");
    }
}
