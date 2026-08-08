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
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AutomaticTabletReshardTest {
    // A JMockit @Mock for a static method must itself be static, so it cannot capture a local of the
    // enclosing test method; what those mocks record lives here instead. Reset before each test.
    private static final AtomicInteger NODE_COUNT_RESOLUTIONS = new AtomicInteger();
    private static final AtomicLong RECORDED_RANGES_SIG = new AtomicLong();

    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "order by(key1)\n" +
                "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");
    }

    @BeforeEach
    public void setUp() {
        // triggerTabletReshard is leader-admission gated; open the gate so each test exercises
        // the split/merge decision instead of short-circuiting.
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
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().clearSizeSplitLatchForTest();
        NODE_COUNT_RESOLUTIONS.set(0);
        RECORDED_RANGES_SIG.set(0L);
    }

    @Test
    void testTriggerTabletReshardFailed() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause splitTabletClause, int computeNodeCount) throws StarRocksException {
                throw new StarRocksException("Create tablet reshard job failed");
            }
        };

        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                Config.tablet_reshard_target_size * 4, Long.MAX_VALUE, 0L);
    }

    @Test
    void testTriggerTabletReshardSuccess() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause splitTabletClause, int computeNodeCount) throws StarRocksException {
                TabletReshardJobMgrTest.TestNormalTabletReshardJob job =
                        new TabletReshardJobMgrTest.TestNormalTabletReshardJob(1L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                Config.tablet_reshard_target_size * 4, Long.MAX_VALUE, 0L);
    }

    @Test
    void testTriggerTabletMergeSuccess() {
        boolean[] mergeCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause mergeTabletClause)
                    throws StarRocksException {
                mergeCalled[0] = true;
            }
        };

        // pair sum strictly below mergePairThreshold = ceil(0.8 * target) → triggers merge
        long t = Config.tablet_reshard_target_size;
        long pairSumBelowThreshold = TabletReshardUtils.mergePairThreshold(t) - 1;
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                0L, pairSumBelowThreshold, 0L);
        org.junit.jupiter.api.Assertions.assertTrue(mergeCalled[0],
                "merge job should be created when minAdjacentPair < mergePairThreshold");
    }

    @Test
    void testTriggerTabletMergeBoundaryNotTriggered() {
        boolean[] mergeCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause mergeTabletClause)
                    throws StarRocksException {
                mergeCalled[0] = true;
            }
        };

        // pair sum exactly at mergePairThreshold → strict-less-than means NOT triggered
        long t = Config.tablet_reshard_target_size;
        long atThreshold = TabletReshardUtils.mergePairThreshold(t);
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                0L, atThreshold, 0L);
        org.junit.jupiter.api.Assertions.assertFalse(mergeCalled[0],
                "merge must not trigger at the exact threshold (strict <)");
    }

    @Test
    void testTriggerTabletSplitBoundaryNotTriggered() {
        boolean[] splitCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause splitTabletClause, int computeNodeCount) throws StarRocksException {
                splitCalled[0] = true;
                return new TabletReshardJobMgrTest.TestNormalTabletReshardJob(2L, TabletReshardJob.JobType.SPLIT_TABLET);
            }
        };

        // maxTabletSize one byte below splitThreshold = ceil(1.5 * target) → NOT triggered
        long t = Config.tablet_reshard_target_size;
        long justBelow = TabletReshardUtils.splitThreshold(t) - 1;
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                justBelow, Long.MAX_VALUE, 0L);
        org.junit.jupiter.api.Assertions.assertFalse(splitCalled[0],
                "split must not trigger one byte below splitThreshold");
    }

    @Test
    void mergeOnlyCandidateStillProducesAMergeJob() {
        // The admission gate gained a disjunct; it must not have replaced the merge one.
        boolean[] mergeCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause) {
                mergeCalled[0] = true;
            }
        };

        long pairSumBelowThreshold =
                TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
        // A local manager: the singleton's scheduler thread ticks every 10 ms and would drain the
        // candidate out from under the assertion below.
        TabletReshardJobMgr mgr = new TabletReshardJobMgr();
        mgr.addReshardCandidate(db.getId(), table.getId(), 0L, pairSumBelowThreshold, 0L);
        assertEquals(1, mgr.getReshardCandidateCount(), "a merge-only candidate must still be queued");
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, pairSumBelowThreshold, 0L);
        assertTrue(mergeCalled[0]);
    }

    @Test
    void unactionableEarlySplitFallsThroughToMerge() {
        boolean[] mergeCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause clause, int computeNodeCount) throws StarRocksException {
                throw new StarRocksException("No tablets need to split");
            }

            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause) {
                mergeCalled[0] = true;
            }
        };

        long earlySize = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
        long pairSumBelowThreshold =
                TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, pairSumBelowThreshold, earlySize);
        assertTrue(mergeCalled[0], "merge must still run when the early split produced nothing");
    }

    @Test
    void fallThroughKeepsTheLatchWhileAnEarlySignalIsLive() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause clause, int computeNodeCount) {
                TabletReshardJobMgrTest.TestNormalTabletReshardJob job =
                        new TabletReshardJobMgrTest.TestNormalTabletReshardJob(
                                1L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        long earlySize = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, Long.MAX_VALUE, earlySize);
        assertTrue(mgr.hasSizeSplitLatch(table.getId()));

        // Same signals again: the latch suppresses, control falls through, the tombstone survives.
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, Long.MAX_VALUE, earlySize);
        assertTrue(mgr.hasSizeSplitLatch(table.getId()),
                "falling through to merge must not erase a live early signal's suppression");
    }

    @Test
    void earlyOnlyTriggerIsSkippedWhenTheParallelCapIsBelowTwo() {
        // Carries BOTH an actionable early signal and an actionable merge signal: an implementation
        // that `return`s instead of falling through when earlyCapacityPossible is false would swallow
        // the merge, and every other test in this task would still pass.
        int[] created = {0};
        boolean[] mergeCalled = {false};
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int safeComputeNodeCountForTable(long tableId) {
                NODE_COUNT_RESOLUTIONS.incrementAndGet();
                return 8;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause clause, int computeNodeCount) {
                created[0]++;
                return null;
            }

            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause) {
                mergeCalled[0] = true;
            }
        };

        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        long savedCap = Config.tablet_reshard_max_parallel_tablets;
        Config.tablet_reshard_max_parallel_tablets = 1L;
        try {
            long earlySize = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
            long mergeSignal =
                    TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
            Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, mergeSignal, earlySize);
        } finally {
            Config.tablet_reshard_max_parallel_tablets = savedCap;
        }
        assertEquals(0, created[0], "cap < 2 cannot fit any early split");
        assertEquals(0, NODE_COUNT_RESOLUTIONS.get(), "and the short-circuit runs before the node-count probe");
        assertTrue(mergeCalled[0], "the capacity short-circuit must fall through to merge, not return");
        assertFalse(mgr.hasSizeSplitLatch(table.getId()), "a capacity short-circuit records no latch entry");
    }

    @Test
    void aRunningJobLeavesTheAutomaticPlanIdenticalToTodays() throws Exception {
        // With another job running the early contribution is dropped, so the job the factory builds
        // must carry exactly the split_count the size rule alone produces.
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public long getTotalParallelTablets() {
                return 4L;
            }
        };

        // Exercise the FACTORY directly, not the manager: TabletReshardJobMgr.createTabletReshardJob
        // admits the job, calls init() (moving the shared table to TABLET_RESHARD) and inserts it into
        // the manager, none of which this assertion needs and none of which the test would undo.
        try {
            setTabletDataSizes(3L << 30, 100L << 30);
            TabletReshardJob job = new SplitTabletJobFactory(db, table, new SplitTabletClause(), 8)
                    .createTabletReshardJob();
            assertEquals(10L, job.getParallelTablets(), "only the 100 GiB tablet splits, exactly as today");
        } finally {
            setTabletDataSizes();
        }
    }

    @Test
    void theSampledNodeCountReachesBothTheSignatureAndTheFactory() {
        // Property: ONE resolution feeds both the latch fingerprint and the factory. Prove it by
        // returning a DIFFERENT count on any second resolution: if the code resolved twice, the
        // fingerprint and the factory would disagree.
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int safeComputeNodeCountForTable(long tableId) {
                return NODE_COUNT_RESOLUTIONS.getAndIncrement() == 0 ? 8 : 2;
            }
        };
        new MockUp<ColocateChecker>() {
            @Mock
            public static long tableConvergenceSignature(Database db, OlapTable table, long expectedRangesSig) {
                RECORDED_RANGES_SIG.set(expectedRangesSig);
                return expectedRangesSig;
            }
        };
        int[] factoryNodeCount = {-1};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause clause, int computeNodeCount) {
                factoryNodeCount[0] = computeNodeCount;
                TabletReshardJobMgrTest.TestNormalTabletReshardJob job =
                        new TabletReshardJobMgrTest.TestNormalTabletReshardJob(
                                4L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        long maxTabletSize = Config.tablet_reshard_target_size * 4;
        Deencapsulation.invoke(GlobalStateMgr.getCurrentState().getTabletReshardJobMgr(),
                "triggerTabletReshard", db, table, maxTabletSize, Long.MAX_VALUE, 0L);
        assertEquals(8, factoryNodeCount[0], "the factory must be handed the sampled count");
        assertEquals(TabletReshardJobMgr.splitPlanSignature(maxTabletSize, 0L, 8), RECORDED_RANGES_SIG.get(),
                "the fingerprint must describe the plan that ran");
        assertEquals(1, NODE_COUNT_RESOLUTIONS.get(), "one resolution per firing decision");
    }

    @Test
    void earlyOnlyTriggerIsSkippedWhileAnotherJobRunsAndFiresAfterItFinishes() {
        int[] created = {0};
        long[] running = {4L};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public long getTotalParallelTablets() {
                return running[0];
            }

            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause clause, int computeNodeCount) {
                created[0]++;
                TabletReshardJobMgrTest.TestNormalTabletReshardJob job =
                        new TabletReshardJobMgrTest.TestNormalTabletReshardJob(
                                2L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        long earlySize = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, Long.MAX_VALUE, earlySize);
        assertEquals(0, created[0], "no early job while another reshard job is running");

        running[0] = 0L;
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, Long.MAX_VALUE, earlySize);
        assertEquals(1, created[0], "capacity release lets the early split fire");
    }

    /**
     * Replaces the shared table's base index with exactly one LakeTablet per given size, in the order
     * given; called with no size it just empties the index. Every other test in this class mocks job
     * creation and asserts on signals it passes in explicitly, so none of them reads the tablet list.
     */
    private static void setTabletDataSizes(long... sizes) {
        PhysicalPartition partition = table.getAllPhysicalPartitions().iterator().next();
        MaterializedIndex index = partition.getLatestBaseIndex();
        for (Tablet existing : new ArrayList<>(index.getTablets())) {
            index.removeTablet(existing.getId());
        }
        for (long size : sizes) {
            LakeTablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            tablet.setDataSize(size);
            index.addTablet(tablet, new TabletMeta(db.getId(), table.getId(), partition.getId(),
                    index.getId(), TStorageMedium.HDD, true));
        }
    }
}
