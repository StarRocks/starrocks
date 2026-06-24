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
import com.starrocks.lake.LakeTablet;
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

public class TabletReshardJobMgrTest {
    public static class TestNormalTabletReshardJob extends TabletReshardJob {

        public TestNormalTabletReshardJob(long jobId, TabletReshardJob.JobType jobType) {
            super(jobId, jobType);
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
        // Reset the table state: a split job created by a previous test transitions the table to
        // TABLET_RESHARD via init(), and triggerTabletReshard short-circuits on non-NORMAL.
        if (reshardTable != null) {
            reshardTable.setState(OlapTable.OlapTableState.NORMAL);
        }
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

        jobMgr.runAfterCatalogReady();

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

        jobMgr.runAfterCatalogReady();

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
}
