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
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.proto.ParentTabletPublishInfoPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.thrift.TTabletReshardJobsItem;
import com.starrocks.thrift.TTabletReshardJobsResponse;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TabletReshardJobMgrTest {
    // A JMockit @Mock for a static method must itself be static, so it cannot capture a local of the
    // enclosing test method; the counters it writes live here instead. Reset before each test.
    private static final AtomicInteger NODE_COUNT_RESOLUTIONS = new AtomicInteger();

    private static class WarnCounterAppender extends AbstractAppender {
        private final AtomicInteger warnCount = new AtomicInteger();

        WarnCounterAppender() {
            super("tablet-reshard-warn-counter", null, null);
        }

        @Override
        public void append(LogEvent event) {
            if (event.getLevel() == Level.WARN) {
                warnCount.incrementAndGet();
            }
        }

        int getWarnCount() {
            return warnCount.get();
        }
    }

    private static void stubCountingNodeCount() {
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int adaptiveSplitBoundForTable(long tableId) {
                NODE_COUNT_RESOLUTIONS.incrementAndGet();
                return TabletReshardUtils.adaptiveSplitBound(8);
            }
        };
    }

    public static class TestNormalTabletReshardJob extends TabletReshardJob {

        private long tableId = 0;

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

        @Override
        protected String anyPublishFailureReason() {
            return null;
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
    public void clearSharedMgr() {
        // Tests that use the singleton TabletReshardJobMgr share its state. Clear it before each
        // test so a job created in one test does not block table-reservation checks in the next.
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().tabletReshardJobs.clear();
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().clearSizeSplitLatchForTest();
        // Reset the table state: a split job created by a previous test transitions the table to
        // TABLET_RESHARD via init(), and triggerTabletReshard short-circuits on non-NORMAL.
        if (reshardTable != null) {
            reshardTable.setState(OlapTable.OlapTableState.NORMAL);
        }
        NODE_COUNT_RESOLUTIONS.set(0);
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
    public void testGetTabletReshardJobsInfo() throws Exception {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();

        TestNormalTabletReshardJob job1 = new TestNormalTabletReshardJob(1, null);
        TestNormalTabletReshardJob job2 = new TestNormalTabletReshardJob(2, null);
        jobMgr.addTabletReshardJob(job1);
        jobMgr.addTabletReshardJob(job2);

        Assertions.assertEquals(2, jobMgr.getAllJobsInfo().getItems().size());
    }

    @Test
    public void testGetTabletReshardJobsInfoDoesNotWarnForDroppedMetadata() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        long splitJobId = 101L;
        long mergeJobId = 102L;
        SplitTabletJob splitJob = new SplitTabletJob(splitJobId, -1L, -1L, Map.of());
        splitJob.jobState = TabletReshardJob.JobState.FINISHED;
        jobMgr.tabletReshardJobs.put(splitJobId, splitJob);
        MergeTabletJob mergeJob = new MergeTabletJob(mergeJobId, reshardDb.getId(), -1L, Map.of());
        mergeJob.jobState = TabletReshardJob.JobState.FINISHED;
        jobMgr.tabletReshardJobs.put(mergeJobId, mergeJob);

        WarnCounterAppender appender = new WarnCounterAppender();
        org.apache.logging.log4j.core.Logger splitLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(SplitTabletJob.class);
        org.apache.logging.log4j.core.Logger mergeLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(MergeTabletJob.class);
        appender.start();
        splitLogger.addAppender(appender);
        mergeLogger.addAppender(appender);

        TTabletReshardJobsResponse response;
        try {
            response = jobMgr.getAllJobsInfo();
        } finally {
            splitLogger.removeAppender(appender);
            mergeLogger.removeAppender(appender);
            appender.stop();
        }

        Assertions.assertEquals(0, appender.getWarnCount(),
                "dropped metadata is expected for retained history jobs and must not produce warnings");
        Assertions.assertEquals(2, response.getItemsSize());
        TTabletReshardJobsItem splitItem = response.getItems().stream()
                .filter(item -> item.getJob_id() == splitJobId).findFirst().orElseThrow();
        Assertions.assertEquals("", splitItem.getDb_name());
        Assertions.assertEquals("", splitItem.getTable_name());
        TTabletReshardJobsItem mergeItem = response.getItems().stream()
                .filter(item -> item.getJob_id() == mergeJobId).findFirst().orElseThrow();
        Assertions.assertEquals(reshardDb.getFullName(), mergeItem.getDb_name());
        Assertions.assertEquals("", mergeItem.getTable_name());
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

    @Test
    public void testAddAndDrainReshardCandidate() throws Exception {
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        oversizedTablet.setDataSize(Config.tablet_reshard_target_size * 2);

        mockLeaderAdmissionOpen();

        Assertions.assertTrue(reshardTable.isRangeDistribution(),
                "test table must be range-distributed; update DDL or Config if this fires");

        int before = mgr.getTabletReshardJobs().size();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
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
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
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
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), belowSplit, Long.MAX_VALUE, 0L, 0);
        Assertions.assertEquals(before, mgr.getReshardCandidateCount(),
                "non-actionable signal (below split threshold, no merge) must not be queued");
    }

    @Test
    public void testAddReshardCandidateCoalescesPerTable() throws Exception {
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        mockLeaderAdmissionOpen();
        long target = Config.tablet_reshard_target_size;
        // First actionable mark seeds the table's candidate.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), target * 2, Long.MAX_VALUE, 0L, 0);
        int afterFirst = mgr.getReshardCandidateCount();
        // A second mark for the same table must coalesce (exercise the merge remap), not add a new entry.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), target * 4, Long.MAX_VALUE, 0L, 0);
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
        // The trigger now consults tablet_reshard_enable_tablet_merge before planning, so that this
        // path does not pay a read-locked signature walk for a job that cannot run. The check used to
        // live inside createTabletReshardJob, which this test mocks out -- so the precondition it
        // always depended on has to be stated explicitly now.
        boolean savedMergeFlag = Config.tablet_reshard_enable_tablet_merge;
        try {
            Config.tablet_reshard_enable_tablet_merge = true;
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, mergePairBelowThreshold, 0L, 0);
            mgr.runAfterCatalogReadyForTest();
        } finally {
            Config.tablet_reshard_enable_tablet_merge = savedMergeFlag;
        }
        Assertions.assertTrue(mergeCalled[0],
                "drain must route a sub-threshold merge candidate to a merge job");
    }

    /**
     * The merge feature gate must be consulted BEFORE the latch signature is computed. That signature
     * takes the table READ lock and hashes every tablet of every visible index, while the gate itself
     * lives inside createTabletReshardJob -- and `needMerge` does not consult the flag, so a table
     * carrying only a merge signal is still queued while merge is off. Getting the order wrong pays
     * that walk on every statistics pass, forever, for a job that cannot run; before this feature the
     * same path threw immediately with no walk at all. Asserting "no job was created" would NOT catch
     * that, so this counts the signature computation itself.
     */
    @Test
    public void testMergeDisabledSkipsTheLatchSignatureWalk() throws Exception {
        int[] signatureWalks = {0};
        new MockUp<ColocateChecker>() {
            @Mock
            public long tableConvergenceSignature(Database db, OlapTable table, long expectedRangesSig) {
                signatureWalks[0]++;
                return 1L;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause)
                    throws StarRocksException {
                throw new StarRocksException("Tablet merge is disabled.");
            }
        };

        mockLeaderAdmissionOpen();
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        long pairSize = TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
        boolean savedFlag = Config.tablet_reshard_enable_tablet_merge;

        try {
            Config.tablet_reshard_enable_tablet_merge = false;
            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    0L, pairSize, 0L, 0);
            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    0L, pairSize, 0L, 0);
            Assertions.assertEquals(0, signatureWalks[0],
                    "merge disabled must skip the read-locked signature walk entirely");

            // Control: with the flag on, the very same signal does compute it -- so the assertion
            // above is about the gate and not about the signal being unactionable.
            Config.tablet_reshard_enable_tablet_merge = true;
            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    0L, pairSize, 0L, 0);
            Assertions.assertEquals(1, signatureWalks[0],
                    "merge enabled must evaluate the latch for an actionable signal");
        } finally {
            Config.tablet_reshard_enable_tablet_merge = savedFlag;
        }
    }

    /**
     * The merge signal is derived from adjacent tablet PAIRS and knows nothing about colocate, while
     * the planner refuses a pair that straddles a ColocateRange. A range-colocate table settles at one
     * tablet per range, so the signal stays permanently actionable against a permanently empty plan --
     * and without a latch the table re-walks every partition and index under the read lock on every
     * stat pass, forever. An EmptyReshardPlanException must therefore suppress the retry, and must
     * re-arm once the signal moves.
     */
    @Test
    public void testEmptyMergePlanSuppressedUntilTheLayoutChanges() throws Exception {
        int[] mergeAttempts = {0};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause)
                    throws StarRocksException {
                mergeAttempts[0]++;
                throw new EmptyReshardPlanException("No tablets need to merge in table t");
            }
        };

        mockLeaderAdmissionOpen();
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        long pairSize = TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
        boolean savedMergeFlag = Config.tablet_reshard_enable_tablet_merge;
        try {
            // The trigger consults the feature gate before planning, so an automatic merge only gets
            // this far when merge is enabled.
            Config.tablet_reshard_enable_tablet_merge = true;

            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    0L, pairSize, 0L, 0);
            Assertions.assertEquals(1, mergeAttempts[0], "the first actionable signal must be planned");

            // Same signal, same layout: deterministic empty plan, so it must not be re-planned.
            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    0L, pairSize, 0L, 0);
            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    0L, pairSize, 0L, 0);
            Assertions.assertEquals(1, mergeAttempts[0],
                    "an unchanged layout and signal must not re-walk the table");

            // A moved signal means the tablet mix (or the parallelism floor) changed, so the plan
            // could now be non-empty: the latch must re-arm rather than suppress forever.
            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    0L, pairSize - 1, 0L, 0);
            Assertions.assertEquals(2, mergeAttempts[0], "a changed signal must re-arm the latch");
        } finally {
            Config.tablet_reshard_enable_tablet_merge = savedMergeFlag;
        }
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
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "first over-threshold scan must fire exactly one split");
        Assertions.assertTrue(mgr.hasSizeSplitLatch(reshardTable.getId()), "table must be latched after firing");

        // Round 2: same size -> same signature (identical fallback leaves layout + requested count unchanged) -> suppressed.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
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
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
        mgr.runAfterCatalogReadyForTest();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "suppressed while nothing changed");

        // Simulate a load: advance dataVersion -> signature changes -> latch re-arms.
        com.starrocks.catalog.PhysicalPartition pp = reshardTable.getPartitions().iterator().next()
                .getDefaultPhysicalPartition();
        pp.setDataVersion(pp.getDataVersion() + 1);

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
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
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), big, Long.MAX_VALUE, 0L, 0);
            mgr.runAfterCatalogReadyForTest();
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), big, Long.MAX_VALUE, 0L, 0);
            mgr.runAfterCatalogReadyForTest();
            Assertions.assertEquals(1, splitCalls[0], "suppressed at unchanged split count");

            // Raise the target so calcSplitCount(big, target) drops -> requested split count changes -> re-arm.
            Config.tablet_reshard_target_size = big / 2; // calcSplitCount ~ 2, different from ~8
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), big, Long.MAX_VALUE, 0L, 0);
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

            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), huge, Long.MAX_VALUE, 0L, 0);
            mgr.runAfterCatalogReadyForTest();
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), huge, Long.MAX_VALUE, 0L, 0);
            mgr.runAfterCatalogReadyForTest();
            Assertions.assertEquals(1, splitCalls[0], "suppressed while config + layout unchanged (count capped)");

            // Change target: the max count stays capped at 2, but the raw target config in the fingerprint changes.
            Config.tablet_reshard_target_size = originalTarget * 3;
            mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), huge, Long.MAX_VALUE, 0L, 0);
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
            long sigA = TabletReshardJobMgr.splitPlanSignature(huge, 0L, 8, Config.tablet_reshard_max_split_count);
            Config.tablet_reshard_target_size = targetB;
            long sigB = TabletReshardJobMgr.splitPlanSignature(huge, 0L, 8, Config.tablet_reshard_max_split_count);
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
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertTrue(splitFired[0], "initial split must fire");
        Assertions.assertTrue(mgr.hasSizeSplitLatch(reshardTable.getId()), "latched after firing");

        // A drain where needSplit is false (only a merge signal) must forget the size-split latch entry.
        long mergePairBelowThreshold = TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, mergePairBelowThreshold, 0L, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertFalse(mgr.hasSizeSplitLatch(reshardTable.getId()),
                "dropping below the split threshold must forget the latch so future growth re-arms");
    }

    @Test
    public void anEmptyPlanLatchesWhereATransientFailureDoesNot() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) throws StarRocksException {
                throw new EmptyReshardPlanException("nothing to split");
            }
        };
        mockLeaderAdmissionOpen();
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        // Adaptive-only, because a live size signal rethrows rather than classifying. An empty plan is
        // deterministic -- the same layout and configuration produce it again -- so it must latch, which
        // is what stops the drain re-walking the table under its read lock on every unchanged scan.
        // The sibling case, testAutoSplitCreationFailureRetries, pins that a transient failure must not.
        Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                0L, Long.MAX_VALUE, Config.tablet_reshard_min_split_size * 4, 8);
        Assertions.assertTrue(mgr.hasSizeSplitLatch(reshardTable.getId()),
                "a deterministic empty plan must latch");
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
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
        mgr.runAfterCatalogReadyForTest();
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(),
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
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
                Config.tablet_reshard_target_size * 2, Long.MAX_VALUE, 0L, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertTrue(mgr.hasSizeSplitLatch(reshardTable.getId()), "latched after firing");

        // Demotion: the admission-closed tick must clear the latch (bounding it to a leader tenure).
        mockLeaderAdmissionClosed();
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertFalse(mgr.hasSizeSplitLatch(reshardTable.getId()),
                "leader demotion must clear the size-split latch");
    }

    @Test
    public void earlySignalCoalescesByMax() {
        mockLeaderAdmissionOpen();
        // A local manager: the singleton's scheduler thread ticks every 10 ms and would drain the
        // candidate out from under the assertions below.
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();
        long small = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
        // Three marks with the largest neither first nor last: with only an ascending pair, a remap
        // mutated to keep the incoming value would return the same answer as Math.max and the
        // assertion could not tell the two apart.
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, Long.MAX_VALUE, small, 0);
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, Long.MAX_VALUE, small * 4, 0);
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, Long.MAX_VALUE, small * 2, 0);
        Assertions.assertEquals(1, mgr.getReshardCandidateCount());
        // The coalesced candidate must carry the larger early signal.
        Assertions.assertEquals(small * 4, mgr.peekMaxUnderProvisionedTabletSize(reshardTable.getId()));
    }

    @Test
    public void earlySignalReachesTheTriggerThroughTheDrain() {
        // The only test that carries a live early signal all the way from the queue to a job: every
        // other early-path test invokes triggerTabletReshard directly and so cannot see the drain
        // dropping the signal on its way through.
        int[] splitCalls = {0};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                splitCalls[0]++;
                TestNormalTabletReshardJob job =
                        new TestNormalTabletReshardJob(4000L + splitCalls[0], TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };
        mockLeaderAdmissionOpen();
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        // Early-only: no split signal (0L) and no merge signal (Long.MAX_VALUE), so nothing but the
        // third field can produce a job.
        long earlySize = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, Long.MAX_VALUE, earlySize, 0);
        Assertions.assertEquals(1, mgr.getReshardCandidateCount(), "the early-only candidate must be queued");
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "the drain must hand the early signal to the trigger");
    }

    @Test
    public void aChangedEarlySignalReArmsTheSuppressedSplit() {
        // The early signal is an input to the no-progress fingerprint, so a tablet growing into a
        // different early size band must re-arm a split the latch is suppressing. Fails if the trigger
        // stops feeding maxUnderProvisionedTabletSize into splitPlanSignature.
        int[] splitCalls = {0};
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int safeComputeNodeCountForTable(long tableId) {
                return 8;   // fixed, so the node count cannot be what moves the fingerprint
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    com.starrocks.sql.ast.SplitTabletClause clause) {
                splitCalls[0]++;
                TestNormalTabletReshardJob job =
                        new TestNormalTabletReshardJob(4500L + splitCalls[0], TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };
        mockLeaderAdmissionOpen();
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();

        // calcSplitCount(3 GiB, earlyTarget 2 GiB) = 2; calcSplitCount(12 GiB, 2 GiB) = 6.
        long small = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
        long large = small * 4;

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, Long.MAX_VALUE, small, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "first early observation must fire");

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, Long.MAX_VALUE, small, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(1, splitCalls[0], "an unchanged early signal must stay suppressed");

        mgr.addReshardCandidate(reshardDb.getId(), reshardTable.getId(), 0L, Long.MAX_VALUE, large, 0);
        mgr.runAfterCatalogReadyForTest();
        Assertions.assertEquals(2, splitCalls[0], "a changed early child count must re-arm the split");
    }

    @Test
    public void splitPlanSignatureChangesForEachNewInput() {
        long savedMin = Config.tablet_reshard_min_split_size;
        try {
            long max = Config.tablet_reshard_target_size * 4;
            long adaptive = Config.tablet_reshard_min_split_size * 4;
            int cap = Config.tablet_reshard_max_split_count;
            long base = TabletReshardJobMgr.splitPlanSignature(max, adaptive, 8, cap);

            Assertions.assertNotEquals(base, TabletReshardJobMgr.splitPlanSignature(max * 4, adaptive, 8, cap),
                    "a larger max tablet changes the size rule's answer");

            Config.tablet_reshard_min_split_size = savedMin * 2;
            Assertions.assertNotEquals(base, TabletReshardJobMgr.splitPlanSignature(max, adaptive, 8, cap),
                    "min_split_size moves the adaptive target");
            Config.tablet_reshard_min_split_size = savedMin;

            Assertions.assertNotEquals(base, TabletReshardJobMgr.splitPlanSignature(max, adaptive * 8, 8, cap),
                    "a different adaptive-split tablet size");

            // The bound is a plan input, not just a scan input: an index the adaptive rule declined
            // to widen at four nodes may be worth widening at eight, so a resized warehouse has to
            // re-arm a suppressed table rather than leave it latched on the old bound's answer.
            Assertions.assertNotEquals(base, TabletReshardJobMgr.splitPlanSignature(max, adaptive, 4, cap),
                    "a resized warehouse moves the bound");
        } finally {
            Config.tablet_reshard_min_split_size = savedMin;
        }
    }

    @Test
    public void mergePlanSignatureChangesForEachNewInput() {
        long savedTarget = Config.tablet_reshard_target_size;
        try {
            // Literal, not derived from the config another test in this class may have moved: two
            // pair sums that happen to be equal would make the first assertion vacuous.
            long base = TabletReshardJobMgr.mergePlanSignature(1024L);

            Assertions.assertNotEquals(base, TabletReshardJobMgr.mergePlanSignature(4096L),
                    "a different adjacent-pair sum changes the size rule's answer");

            Config.tablet_reshard_target_size = savedTarget * 2 + 1;
            Assertions.assertNotEquals(base, TabletReshardJobMgr.mergePlanSignature(1024L),
                    "target_size moves every merge threshold, so a suppressed table must re-arm");
        } finally {
            Config.tablet_reshard_target_size = savedTarget;
        }
    }

    @Test
    public void theTriggerReusesTheBoundItsProducerAlreadyResolved() {
        mockLeaderAdmissionOpen();
        stubCountingNodeCount();
        // A local manager, and the counter zeroed as late as possible: the singleton's scheduler
        // thread ticks every 10 ms, and a candidate an earlier test left queued would resolve too.
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();
        // Leave the table too small to split: a real job here would replace the shared tablet this
        // class hands to every other test, and the resolution under test happens before planning.
        oversizedTablet.setDataSize(0);

        // Resolving a warehouse probes StarMgr, and whoever raised the candidate already paid for it,
        // so the trigger resolves nothing of its own -- on either path. The adaptive path is the one
        // that would hurt: a skewed index parked at the bound keeps signalling and keeps being
        // suppressed, so it would pay that round trip on every scan, forever. The single resolution
        // left is the factory's, for the plan it builds.
        NODE_COUNT_RESOLUTIONS.set(0);
        Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                Config.tablet_reshard_target_size * 4, Long.MAX_VALUE, 0L, 0);
        Assertions.assertEquals(1, NODE_COUNT_RESOLUTIONS.get(), "size-only split resolves only in the factory");

        NODE_COUNT_RESOLUTIONS.set(0);
        Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                0L, Long.MAX_VALUE, Config.tablet_reshard_min_split_size * 4, 8);
        Assertions.assertEquals(1, NODE_COUNT_RESOLUTIONS.get(), "adaptive split resolves only in the factory");
    }

    @Test
    public void noNodeCountResolutionWhenTheTableIsNotNormal() {
        mockLeaderAdmissionOpen();
        stubCountingNodeCount();
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();
        reshardTable.setState(OlapTable.OlapTableState.TABLET_RESHARD);
        NODE_COUNT_RESOLUTIONS.set(0);
        try {
            Deencapsulation.invoke(mgr, "triggerTabletReshard", reshardDb, reshardTable,
                    Config.tablet_reshard_target_size * 4, Long.MAX_VALUE, 0L, 0);
        } finally {
            reshardTable.setState(OlapTable.OlapTableState.NORMAL);
        }
        Assertions.assertEquals(0, NODE_COUNT_RESOLUTIONS.get(), "the NORMAL guard must run before the probe");
    }

    /**
     * The ORDER BY split cadence gate. A split whose children must be UNSHARE-rewritten holds the
     * partition's only compaction slot for the whole rewrite, during which size-tiered compaction cannot
     * run, so the next split waits out tablet_reshard_orderby_split_interval_second. The previous finish
     * time is read straight off the finished jobs still in the map, which is what lets it survive a
     * leader switch.
     */
    @Test
    public void quietPeriodCountsOnlyThisTablesFinishedSplits() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        long tableId = 770001L;
        int savedInterval = Config.tablet_reshard_orderby_split_interval_second;
        try {
            // Disabled: nothing is ever held back.
            Config.tablet_reshard_orderby_split_interval_second = 0;
            Assertions.assertTrue(quietPeriodElapsed(jobMgr, tableId));

            Config.tablet_reshard_orderby_split_interval_second = 3600;
            // No previous job at all, so there is no clock to wait on.
            Assertions.assertTrue(quietPeriodElapsed(jobMgr, tableId));

            // A job still running carries a finish time of 0 and must not start one either.
            SplitTabletJob running = new SplitTabletJob(770101L, reshardDb.getId(), tableId, Map.of());
            running.jobState = TabletReshardJob.JobState.RUNNING;
            jobMgr.tabletReshardJobs.put(770101L, running);
            Assertions.assertTrue(quietPeriodElapsed(jobMgr, tableId));

            // One that finished just now does.
            SplitTabletJob justFinished = new SplitTabletJob(770102L, reshardDb.getId(), tableId, Map.of());
            justFinished.jobState = TabletReshardJob.JobState.FINISHED;
            justFinished.finishedTimeMs = System.currentTimeMillis();
            jobMgr.tabletReshardJobs.put(770102L, justFinished);
            Assertions.assertFalse(quietPeriodElapsed(jobMgr, tableId));
            // ... and only for its own table.
            Assertions.assertTrue(quietPeriodElapsed(jobMgr, tableId + 1));

            // The newest finish is the one that counts: an older sibling must not talk the gate open.
            SplitTabletJob longDone = new SplitTabletJob(770103L, reshardDb.getId(), tableId, Map.of());
            longDone.jobState = TabletReshardJob.JobState.FINISHED;
            longDone.finishedTimeMs = System.currentTimeMillis() - 7200_000L;
            jobMgr.tabletReshardJobs.put(770103L, longDone);
            Assertions.assertFalse(quietPeriodElapsed(jobMgr, tableId));

            jobMgr.tabletReshardJobs.remove(770102L);
            Assertions.assertTrue(quietPeriodElapsed(jobMgr, tableId));
        } finally {
            Config.tablet_reshard_orderby_split_interval_second = savedInterval;
        }
    }

    private static boolean quietPeriodElapsed(TabletReshardJobMgr jobMgr, long tableId) {
        return Deencapsulation.invoke(jobMgr, "reshardQuietPeriodElapsed", tableId);
    }

    /**
     * While a split's children are being UNSHARE-rewritten the parent stays queryable, so every ordinary
     * publish of those children has to carry a query-parent metadata page for it. This is what collects
     * them, and the queryable-index pin is what says a parent still needs one.
     */
    @Test
    public void parentPublishInfosTrackThePinnedParentUntilItsFamilyLands() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        PhysicalPartition physicalPartition =
                reshardTable.getPartitions().iterator().next().getDefaultPhysicalPartition();
        long indexMetaId = reshardTable.getBaseIndexMetaId();
        physicalPartition.pinQueryableIndex(indexMetaId, physicalPartition.getLatestBaseIndex().getId());
        try {
            long parentTabletId = 780001L;
            List<Long> childTabletIds = List.of(780002L, 780003L);
            ReshardingTablet family = mock(ReshardingTablet.class);
            when(family.getOldTabletIds()).thenReturn(List.of(parentTabletId));
            when(family.getNewTabletIds()).thenReturn(childTabletIds);
            when(family.getFirstOldTabletId()).thenReturn(parentTabletId);

            MaterializedIndex newIndex = mock(MaterializedIndex.class);
            when(newIndex.getMetaId()).thenReturn(indexMetaId);
            ReshardingMaterializedIndex reshardingIndex = mock(ReshardingMaterializedIndex.class);
            when(reshardingIndex.getMaterializedIndex()).thenReturn(newIndex);
            when(reshardingIndex.getReshardingTablets()).thenReturn(List.of(family));
            ReshardingPhysicalPartition reshardingPartition = mock(ReshardingPhysicalPartition.class);
            when(reshardingPartition.getPhysicalPartitionId()).thenReturn(physicalPartition.getId());
            when(reshardingPartition.getReshardingIndexes()).thenReturn(Map.of(indexMetaId, reshardingIndex));

            SplitTabletJob split = new SplitTabletJob(780101L, reshardDb.getId(), reshardTable.getId(),
                    Map.of(physicalPartition.getId(), reshardingPartition));
            split.jobState = TabletReshardJob.JobState.RUNNING;
            jobMgr.tabletReshardJobs.put(780101L, split);
            // A merge job keeps no parent view alive; the collector must skip it rather than ask it.
            jobMgr.tabletReshardJobs.put(780102L,
                    new MergeTabletJob(780102L, reshardDb.getId(), reshardTable.getId(), Map.of()));

            List<ParentTabletPublishInfoPB> infos =
                    jobMgr.collectParentPublishInfos(new HashSet<>(childTabletIds));
            Assertions.assertEquals(1, infos.size());
            Assertions.assertEquals(parentTabletId, (long) infos.get(0).getParentTabletId());

            // Half a family is not a family: the page is owed only once every child has published.
            Assertions.assertTrue(jobMgr.collectParentPublishInfos(Set.of(childTabletIds.get(0))).isEmpty());

            // A job in a final state has handed its children over and stops reporting.
            split.jobState = TabletReshardJob.JobState.FINISHED;
            Assertions.assertTrue(jobMgr.collectParentPublishInfos(new HashSet<>(childTabletIds)).isEmpty());
            split.jobState = TabletReshardJob.JobState.RUNNING;

            // So does unpinning: with the parent view gone there is nothing left to serve from it.
            Assertions.assertTrue(physicalPartition.finishUnshare());
            Assertions.assertTrue(jobMgr.collectParentPublishInfos(new HashSet<>(childTabletIds)).isEmpty());

            // hasLiveSplitJob() is table-agnostic, so a publish for an unrelated table walks this job
            // too. It must be turned away from the job's own state, without reading -- or locking --
            // a table that has nothing to do with the batch.
            AtomicInteger tableLookups = new AtomicInteger();
            new MockUp<LocalMetastore>() {
                @Mock
                public Table getTable(Long dbId, Long tableId) {
                    tableLookups.incrementAndGet();
                    return reshardTable;
                }
            };
            Assertions.assertTrue(jobMgr.collectParentPublishInfos(Set.of(990001L, 990002L)).isEmpty(),
                    "a batch holding none of this job's tablets owes no parent page");
            Assertions.assertEquals(0, tableLookups.get(),
                    "an unrelated batch must be answered without touching the table");
        } finally {
            physicalPartition.finishUnshare();
        }
    }


    /**
     * The publish path consults this before doing any per-batch work, so it has to be true exactly when
     * collectParentPublishInfos could still answer something. That method early-returns on a final job
     * state, so "a SplitTabletJob that is not in a final state" is precisely the condition -- getting it
     * wrong in the false direction stops the parent-view pages without failing anything loudly.
     */
    @Test
    public void hasLiveSplitJobMatchesWhatTheCollectorCanAnswer() {
        TabletReshardJobMgr jobMgr = new TabletReshardJobMgr();
        Assertions.assertFalse(jobMgr.hasLiveSplitJob(), "an empty job map owes nothing");

        // A merge job is not a split: it keeps no parent view alive.
        jobMgr.tabletReshardJobs.put(9001L, new MergeTabletJob(9001L, reshardDb.getId(), -1L, Map.of()));
        Assertions.assertFalse(jobMgr.hasLiveSplitJob(), "a merge job must not keep the publish path busy");

        SplitTabletJob split = new SplitTabletJob(9002L, reshardDb.getId(), reshardTable.getId(), Map.of());
        split.jobState = TabletReshardJob.JobState.RUNNING;
        jobMgr.tabletReshardJobs.put(9002L, split);
        Assertions.assertTrue(jobMgr.hasLiveSplitJob(), "a running split job may still owe a parent page");

        // Finished jobs linger for tablet_reshard_history_job_keep_max_ms; they must not keep it true,
        // which is the whole point of asking.
        for (TabletReshardJob.JobState finalState :
                List.of(TabletReshardJob.JobState.FINISHED, TabletReshardJob.JobState.ABORTED)) {
            split.jobState = finalState;
            Assertions.assertFalse(jobMgr.hasLiveSplitJob(), "a " + finalState + " split job owes nothing");
            Assertions.assertTrue(jobMgr.collectParentPublishInfos(Set.of(1L, 2L)).isEmpty(),
                    "and the collector agrees for " + finalState);
        }
    }
}
