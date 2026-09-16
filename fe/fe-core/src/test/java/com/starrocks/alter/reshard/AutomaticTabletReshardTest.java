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
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AutomaticTabletReshardTest {
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
    }

    @Test
    void testTriggerTabletReshardFailed() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
                    throws StarRocksException {
                throw new StarRocksException("Create tablet reshard job failed");
            }
        };

        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                Config.tablet_reshard_target_size * 4, Long.MAX_VALUE, 0L, 0);
    }

    @Test
    void testTriggerTabletReshardSuccess() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
                    throws StarRocksException {
                TabletReshardJobMgrTest.TestNormalTabletReshardJob job =
                        new TabletReshardJobMgrTest.TestNormalTabletReshardJob(1L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                Config.tablet_reshard_target_size * 4, Long.MAX_VALUE, 0L, 0);
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

        // triggerTabletReshard consults tablet_reshard_enable_tablet_merge before planning; the gate
        // used to sit inside createTabletReshardJob, which this test mocks out. State the precondition
        // explicitly, or the default-off flag decides the outcome instead of the rule under test.
        boolean savedMergeFlag = Config.tablet_reshard_enable_tablet_merge;
        try {
            Config.tablet_reshard_enable_tablet_merge = true;
            // pair sum strictly below mergePairThreshold = ceil(0.8 * target) → triggers merge
            long t = Config.tablet_reshard_target_size;
            long pairSumBelowThreshold = TabletReshardUtils.mergePairThreshold(t) - 1;
            TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
            Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                    0L, pairSumBelowThreshold, 0L, 0);
            org.junit.jupiter.api.Assertions.assertTrue(mergeCalled[0],
                    "merge job should be created when minAdjacentPair < mergePairThreshold");
        } finally {
            Config.tablet_reshard_enable_tablet_merge = savedMergeFlag;
        }
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

        // triggerTabletReshard consults tablet_reshard_enable_tablet_merge before planning; the gate
        // used to sit inside createTabletReshardJob, which this test mocks out. State the precondition
        // explicitly, or the default-off flag decides the outcome instead of the rule under test.
        boolean savedMergeFlag = Config.tablet_reshard_enable_tablet_merge;
        try {
            Config.tablet_reshard_enable_tablet_merge = true;
            // pair sum exactly at mergePairThreshold → strict-less-than means NOT triggered
            long t = Config.tablet_reshard_target_size;
            long atThreshold = TabletReshardUtils.mergePairThreshold(t);
            TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
            Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                    0L, atThreshold, 0L, 0);
            org.junit.jupiter.api.Assertions.assertFalse(mergeCalled[0],
                    "merge must not trigger at the exact threshold (strict <)");
        } finally {
            Config.tablet_reshard_enable_tablet_merge = savedMergeFlag;
        }
    }

    @Test
    void testTriggerTabletSplitBoundaryNotTriggered() {
        boolean[] splitCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table, SplitTabletClause splitTabletClause)
                    throws StarRocksException {
                splitCalled[0] = true;
                return new TabletReshardJobMgrTest.TestNormalTabletReshardJob(2L, TabletReshardJob.JobType.SPLIT_TABLET);
            }
        };

        // maxTabletSize one byte below splitThreshold = ceil(1.5 * target) → NOT triggered
        long t = Config.tablet_reshard_target_size;
        long justBelow = TabletReshardUtils.splitThreshold(t) - 1;
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table,
                justBelow, Long.MAX_VALUE, 0L, 0);
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

        // triggerTabletReshard consults tablet_reshard_enable_tablet_merge before planning; the gate
        // used to sit inside createTabletReshardJob, which this test mocks out. State the precondition
        // explicitly, or the default-off flag decides the outcome instead of the rule under test.
        boolean savedMergeFlag = Config.tablet_reshard_enable_tablet_merge;
        try {
            Config.tablet_reshard_enable_tablet_merge = true;
            long pairSumBelowThreshold =
                    TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
            // A local manager: the singleton's scheduler thread ticks every 10 ms and would drain the
            // candidate out from under the assertion below.
            TabletReshardJobMgr mgr = new TabletReshardJobMgr();
            mgr.addReshardCandidate(db.getId(), table.getId(), 0L, pairSumBelowThreshold, 0L, 0);
            assertEquals(1, mgr.getReshardCandidateCount(), "a merge-only candidate must still be queued");
            Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, pairSumBelowThreshold, 0L, 0);
            assertTrue(mergeCalled[0]);
        } finally {
            Config.tablet_reshard_enable_tablet_merge = savedMergeFlag;
        }
    }

    @Test
    void unactionableEarlySplitFallsThroughToMerge() {
        boolean[] mergeCalled = {false};
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause clause) throws StarRocksException {
                throw new StarRocksException("No tablets need to split");
            }

            @Mock
            public void createTabletReshardJob(Database db, OlapTable table, MergeTabletClause clause) {
                mergeCalled[0] = true;
            }
        };

        // triggerTabletReshard consults tablet_reshard_enable_tablet_merge before planning; the gate
        // used to sit inside createTabletReshardJob, which this test mocks out. State the precondition
        // explicitly, or the default-off flag decides the outcome instead of the rule under test.
        boolean savedMergeFlag = Config.tablet_reshard_enable_tablet_merge;
        try {
            Config.tablet_reshard_enable_tablet_merge = true;
            long earlySize = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
            long pairSumBelowThreshold =
                    TabletReshardUtils.mergePairThreshold(Config.tablet_reshard_target_size) - 1;
            TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
            Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, pairSumBelowThreshold, earlySize, 0);
            assertTrue(mergeCalled[0], "merge must still run when the early split produced nothing");
        } finally {
            Config.tablet_reshard_enable_tablet_merge = savedMergeFlag;
        }
    }

    @Test
    void fallThroughKeepsTheLatchWhileAnEarlySignalIsLive() {
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public TabletReshardJob createTabletReshardJob(Database db, OlapTable table,
                    SplitTabletClause clause) {
                TabletReshardJobMgrTest.TestNormalTabletReshardJob job =
                        new TabletReshardJobMgrTest.TestNormalTabletReshardJob(
                                1L, TabletReshardJob.JobType.SPLIT_TABLET);
                job.setTableId(table.getId());
                return job;
            }
        };

        long earlySize = TabletReshardUtils.splitThreshold(Config.tablet_reshard_min_split_size);
        TabletReshardJobMgr mgr = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr();
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, Long.MAX_VALUE, earlySize, 0);
        assertTrue(mgr.hasSizeSplitLatch(table.getId()));

        // Same signals again: the latch suppresses, control falls through, the tombstone survives.
        Deencapsulation.invoke(mgr, "triggerTabletReshard", db, table, 0L, Long.MAX_VALUE, earlySize, 0);
        assertTrue(mgr.hasSizeSplitLatch(table.getId()),
                "falling through to merge must not erase a live early signal's suppression");
    }
}
