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

package com.starrocks.leader;

import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.Journal;
import com.starrocks.journal.JournalType;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.utframe.MockJournal;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CheckpointControllerTest {
    private MockedStatic<GlobalStateMgr> globalStateMgrStatic;
    private GlobalStateMgr globalStateMgr;
    private NodeMgr nodeMgr;
    private CheckpointController controller;
    private Frontend leader;
    private Frontend follower1;
    private Frontend follower2;

    @BeforeEach
    public void setUp() {
        // Mock GlobalStateMgr and NodeMgr
        globalStateMgrStatic = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        globalStateMgr = Mockito.mock(GlobalStateMgr.class, Mockito.RETURNS_DEEP_STUBS);
        nodeMgr = new NodeMgr();
        globalStateMgrStatic.when(GlobalStateMgr::getServingState).thenReturn(globalStateMgr);
        // createImage() resolves the selected worker through getCurrentState(); point both at the
        // same mock so the node selectWorker() picks is the node that is then looked up.
        globalStateMgrStatic.when(GlobalStateMgr::getCurrentState).thenReturn(globalStateMgr);
        Mockito.when(globalStateMgr.getNodeMgr()).thenReturn(nodeMgr);
        // doCheckpoint() compares this against the candidate to take the local-worker branch
        Deencapsulation.setField(nodeMgr, "nodeName", "leader");

        controller = new CheckpointController("test", new MockJournal(), "");
        // leader and followers
        leader = new Frontend(FrontendNodeType.LEADER, "leader", "127.0.0.1", 9010);
        leader.setAlive(true);
        leader.setFid(1);
        leader.setRpcPort(9020);
        leader.setHeapUsedPercent(10.0f);
        follower1 = new Frontend(FrontendNodeType.FOLLOWER, "follower1", "127.0.0.2", 9011);
        follower1.setAlive(true);
        follower1.setFid(2);
        follower1.setRpcPort(9021);
        follower1.setHeapUsedPercent(20.0f);
        follower2 = new Frontend(FrontendNodeType.FOLLOWER, "follower2", "127.0.0.3", 9012);
        follower2.setAlive(true);
        follower2.setFid(3);
        follower2.setRpcPort(9022);
        follower2.setHeapUsedPercent(30.0f);
        nodeMgr.setMySelf(leader);
        nodeMgr.replayAddFrontend(leader);
        nodeMgr.replayAddFrontend(follower1);
        nodeMgr.replayAddFrontend(follower2);
    }

    @AfterEach
    public void tearDown() {
        globalStateMgrStatic.close();
    }

    @Test
    public void testGetWorkers_checkpointOnlyOnLeader_true() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = true;
        List<Frontend> workers = controller.getWorkers(false);
        assertEquals(1, workers.size());
        assertEquals("leader", workers.get(0).getNodeName());
        Config.checkpoint_only_on_leader = oldValue;
    }

    @Test
    public void testGetWorkers_needClusterSnapshotInfo_true() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = false;
        List<Frontend> workers = controller.getWorkers(true);
        assertEquals(1, workers.size());
        assertEquals("leader", workers.get(0).getNodeName());
        Config.checkpoint_only_on_leader = oldValue;
    }

    @Test
    public void testGetWorkers_sortByLastFailedTime() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = false;
        controller.setLastFailedTime("follower2", System.currentTimeMillis());
        controller.setLastFailedTime("follower1", System.currentTimeMillis() - 10000);
        List<Frontend> workers = controller.getWorkers(false);
        int idx1 = workers.indexOf(follower1);
        int idx2 = workers.indexOf(follower2);
        assertTrue(idx1 < idx2);
        Config.checkpoint_only_on_leader = oldValue;
    }

    @Test
    public void testOnStoppedClearsLeaderSessionBookkeeping() {
        // onStopped must drop the previous leader's pending-push set and last-failed-worker
        // map so a re-elected leader does not (a) try to push to nodes that already have
        // the new image and (b) inherit stale worker-selection bias. Per-round volatile
        // fields (workerNodeName / journalId / result / clusterSnapshotInfo) are naturally
        // overwritten by the next createImage() call, so onStopped does not touch them.
        controller.nodesToPushImage.add("follower1");
        controller.nodesToPushImage.add("follower2");
        controller.setLastFailedTime("follower1", System.currentTimeMillis());

        controller.onStopped();

        Assertions.assertTrue(controller.nodesToPushImage.isEmpty(),
                "nodesToPushImage must be cleared on demotion");
        Assertions.assertTrue(controller.lastFailedTime.isEmpty(),
                "lastFailedTime must be cleared on demotion");
    }

    @Test
    public void testOnStopRequestedWakesCheckpointWait() throws Exception {
        BlockingQueue<CheckpointController.CheckpointCompletionStatus> result = new ArrayBlockingQueue<>(1);
        java.lang.reflect.Field resultField = CheckpointController.class.getDeclaredField("result");
        resultField.setAccessible(true);
        resultField.set(controller, result);

        controller.onStopRequested();

        Assertions.assertNotNull(result.poll(1, TimeUnit.SECONDS),
                "stop request must wake createImage result wait");
    }

    @Test
    public void testOnStopRequestedDisconnectsOwnInFlightConnection() {
        // The controller holds its own in-flight HTTP connection; onStopRequested() must disconnect
        // exactly that connection (never a global registry) to break out of an uninterruptible read.
        HttpURLConnection conn = Mockito.mock(HttpURLConnection.class);
        controller.inFlightConnection = conn;

        controller.onStopRequested();

        Mockito.verify(conn).disconnect();
    }

    @Test
    public void testOnStopRequestedNoConnectionIsNoop() {
        // With no in-flight connection, onStopRequested() must not fail (nothing to disconnect).
        controller.inFlightConnection = null;
        controller.onStopRequested();
    }

    @Test
    public void testInterruptOnStopOptedOut() {
        // The controller's worker calls BDBJE directly (getFinalizedJournalId / deleteJournals),
        // where an interrupt can invalidate the environment - it must opt out of the default
        // interrupt-based stop and rely on cooperative isStopRequested()/onStopRequested().
        Assertions.assertFalse(controller.interruptOnStop());
    }

    @Test
    public void testJournalDatabaseDeleted() {
        // oldest database dropped -> a real cleanup happened
        Assertions.assertTrue(CheckpointController.journalDatabaseDeleted(
                List.of(1L, 101L, 201L), List.of(101L, 201L)));
        Assertions.assertTrue(CheckpointController.journalDatabaseDeleted(
                List.of(1L), List.of()));
        // a new database rolled in but nothing was reclaimed
        Assertions.assertFalse(CheckpointController.journalDatabaseDeleted(
                List.of(1L, 101L), List.of(1L, 101L, 201L)));
        Assertions.assertFalse(CheckpointController.journalDatabaseDeleted(
                List.of(1L), List.of(1L)));
        // bdb environment closing / nothing to start with
        Assertions.assertFalse(CheckpointController.journalDatabaseDeleted(null, List.of(1L)));
        Assertions.assertFalse(CheckpointController.journalDatabaseDeleted(List.of(), List.of()));
        Assertions.assertFalse(CheckpointController.journalDatabaseDeleted(List.of(1L), null));
    }

    /**
     * The scenario this PR is about: a registered FE that cannot be reached pins
     * getMinReplayedJournalId() at 0, so deleteVersion collapses to 0 and the round reclaims
     * nothing. Both followers point at loopback ports nothing is listening on, so the probe fails
     * exactly the way an unreachable node fails in production.
     */
    @Test
    public void testDeleteOldJournalsSkippedWhenPeerUnreachable() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("");
        // same databases before and after -> nothing was reclaimed
        Mockito.when(journal.getDatabaseNames()).thenReturn(List.of(1L, 101L));
        CheckpointController controller = new CheckpointController("test-skip", journal, "");

        controller.deleteOldJournals(500L);

        // minReplayedJournalId fell back to 0, so deleteVersion is 0 and only journals < 1 go away
        Mockito.verify(journal).deleteJournals(1L);
    }

    @Test
    public void testDeleteOldJournalsReclaimsOldestDatabase() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("");
        Mockito.when(journal.getDatabaseNames())
                .thenReturn(List.of(1L, 101L, 201L))
                .thenReturn(List.of(101L, 201L));
        CheckpointController controller = new CheckpointController("test-reclaim", journal, "");

        controller.deleteOldJournals(500L);

        Mockito.verify(journal).deleteJournals(1L);
        Mockito.verify(journal, Mockito.times(2)).getDatabaseNames();
    }

    @Test
    public void testRetainedMetricsUpdatedWhenCleanupLeavesCurrentDatabase() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("");
        Mockito.when(journal.getDatabaseNames())
                .thenReturn(List.of(1L, 101L, 201L))
                .thenReturn(List.of(201L));
        CheckpointController controller = new CheckpointController("test-rebaseline", journal, "");

        withRetainedMetrics(() -> {
            MetricRepo.initializeEditLogRetained(
                    JournalType.FE_META, 1L, 250L);
            MetricRepo.recordEditLogBatch(JournalType.FE_META, 250L, 1L, 100L);
            MetricRepo.initializeEditLogRetained(JournalType.STAR_MGR, 1L, 50L);
            MetricRepo.recordEditLogBatch(JournalType.STAR_MGR, 50L, 1L, 1800L);

            controller.deleteOldJournals(500L);

            assertEquals(50L, MetricRepo.getEditLogRetainedCount(JournalType.FE_META));
            assertEquals(5000L, MetricRepo.getEditLogRetainedBytesEstimate(JournalType.FE_META));
            assertEquals(50L, MetricRepo.getEditLogRetainedCount(JournalType.STAR_MGR));
            assertEquals(90_000L, MetricRepo.getEditLogRetainedBytesEstimate(JournalType.STAR_MGR));
        });
    }

    @Test
    public void testRetainedMetricsReducedOnPartialCleanup() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("");
        Mockito.when(journal.getDatabaseNames())
                .thenReturn(List.of(1L, 11L, 21L, 31L))
                .thenReturn(List.of(21L, 31L));
        CheckpointController controller = new CheckpointController("test-partial", journal, "");

        withRetainedMetrics(() -> {
            MetricRepo.initializeEditLogRetained(
                    JournalType.FE_META, 1L, 35L);
            MetricRepo.recordEditLogBatch(JournalType.FE_META, 35L, 1L, 100L);

            controller.deleteOldJournals(500L);

            assertEquals(15L, MetricRepo.getEditLogRetainedCount(JournalType.FE_META));
            assertEquals(1500L, MetricRepo.getEditLogRetainedBytesEstimate(JournalType.FE_META));
        });
    }

    @Test
    public void testRetainedMetricsKeptWhenNothingReclaimed() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("");
        Mockito.when(journal.getDatabaseNames()).thenReturn(List.of(1L, 101L));
        CheckpointController controller = new CheckpointController("test-keep", journal, "");

        withRetainedMetrics(() -> {
            MetricRepo.initializeEditLogRetained(JournalType.FE_META, 1L, 150L);
            MetricRepo.recordEditLogBatch(JournalType.FE_META, 150L, 1L, 100L);

            controller.deleteOldJournals(500L);

            assertEquals(150L, MetricRepo.getEditLogRetainedCount(JournalType.FE_META));
            assertEquals(15_000L, MetricRepo.getEditLogRetainedBytesEstimate(JournalType.FE_META));
        });
    }

    @Test
    public void testStarMgrCleanupUpdatesOnlyStarMgrMetrics() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("starmgr_");
        Mockito.when(journal.getDatabaseNames())
                .thenReturn(List.of(1L, 101L))
                .thenReturn(List.of(101L));
        CheckpointController controller = new CheckpointController(
                "test-starmgr", journal, "starmgr", JournalType.STAR_MGR);

        withRetainedMetrics(() -> {
            MetricRepo.initializeEditLogRetained(JournalType.FE_META, 1L, 5000L);
            MetricRepo.recordEditLogBatch(JournalType.FE_META, 5000L, 1L, 1800L);
            MetricRepo.initializeEditLogRetained(JournalType.STAR_MGR, 1L, 150L);
            MetricRepo.recordEditLogBatch(JournalType.STAR_MGR, 150L, 1L, 1800L);

            controller.deleteOldJournals(500L);

            assertEquals(5000L, MetricRepo.getEditLogRetainedCount(JournalType.FE_META));
            assertEquals(9_000_000L, MetricRepo.getEditLogRetainedBytesEstimate(JournalType.FE_META));
            assertEquals(50L, MetricRepo.getEditLogRetainedCount(JournalType.STAR_MGR));
            assertEquals(90_000L, MetricRepo.getEditLogRetainedBytesEstimate(JournalType.STAR_MGR));
        });
    }

    private void withRetainedMetrics(Runnable body) {
        MetricRepo.resetEditLogRetained(JournalType.FE_META);
        MetricRepo.resetEditLogRetained(JournalType.STAR_MGR);
        try {
            body.run();
        } finally {
            MetricRepo.resetEditLogRetained(JournalType.FE_META);
            MetricRepo.resetEditLogRetained(JournalType.STAR_MGR);
        }
    }

    /**
     * A failed removeDatabase() must not be swallowed: the caller logs it under the
     * "Delete old edit log failed:" prefix and rethrows so the daemon round aborts.
     */
    @Test
    public void testDeleteOldJournalsPropagatesFailure() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("");
        Mockito.when(journal.getDatabaseNames()).thenReturn(List.of(1L, 101L));
        Mockito.doThrow(new IllegalStateException("bdb is closing"))
                .when(journal).deleteJournals(Mockito.anyLong());
        CheckpointController controller = new CheckpointController("test-fail", journal, "");

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> controller.deleteOldJournals(500L));
        assertEquals("bdb is closing", e.getMessage());
    }

    @Test
    public void testImageWriteFailureIsCounted() {
        withImageMetrics(() -> {
            long successBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, true);
            long failedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
            boolean oldValue = Config.checkpoint_only_on_leader;
            Config.checkpoint_only_on_leader = true;
            // no live worker to hand the checkpoint to -> createImage() bails out
            leader.setAlive(false);
            try {
                controller.runCheckpointControllerWithIds(1L, 2L, false);
            } finally {
                leader.setAlive(true);
                Config.checkpoint_only_on_leader = oldValue;
            }

            assertEquals(successBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, true));
            assertEquals(failedBefore + 1L, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
        });
    }

    @Test
    public void testStarMgrImageWriteFailureIsCountedSeparately() {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn(JournalType.STAR_MGR.getPrefix());
        CheckpointController starMgrController = new CheckpointController(
                "test-starmgr-image", journal, "starmgr", JournalType.STAR_MGR);

        withImageMetrics(() -> {
            long feFailedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
            long starMgrFailedBefore = MetricRepo.getImageWriteCount(JournalType.STAR_MGR, false);
            boolean oldValue = Config.checkpoint_only_on_leader;
            Config.checkpoint_only_on_leader = true;
            leader.setAlive(false);
            try {
                starMgrController.runCheckpointControllerWithIds(1L, 2L, false);
            } finally {
                leader.setAlive(true);
                Config.checkpoint_only_on_leader = oldValue;
            }

            assertEquals(feFailedBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
            assertEquals(starMgrFailedBefore + 1L,
                    MetricRepo.getImageWriteCount(JournalType.STAR_MGR, false));
        });
    }

    /**
     * When the image is already current no checkpoint is attempted, so neither series may move -
     * otherwise every idle daemon tick would look like a checkpoint failure.
     */
    @Test
    public void testImageWriteNotCountedWhenImageIsCurrent() {
        withImageMetrics(() -> {
            long successBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, true);
            long failedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
            controller.runCheckpointControllerWithIds(5L, 5L, false);

            assertEquals(successBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, true));
            assertEquals(failedBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
        });
    }

    /**
     * A worker that accepts the checkpoint but then reports failure must be counted as a failed
     * image write, not silently dropped.
     */
    @Test
    public void testCreateImageWorkerReportsFailureIsCounted() throws Exception {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = true;
        Thread canceller = new Thread(() -> {
            try {
                for (int i = 0; i < 100 && controller.getWorkerNodeName() == null; i++) {
                    Thread.sleep(50);
                }
                controller.cancelCheckpoint("leader", "worker restarted");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            withImageMetrics(() -> {
                long successBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, true);
                long failedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
                canceller.start();
                controller.runCheckpointControllerWithIds(1L, 2L, false);

                assertEquals(successBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, true));
                assertEquals(failedBefore + 1L, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
            });
        } finally {
            canceller.join(10_000);
            Config.checkpoint_only_on_leader = oldValue;
        }
    }

    /**
     * The success side of the same branch: a completed checkpoint counts once as a successful image
     * write, and every other FE gets queued for the image push.
     */
    @Test
    public void testCreateImageSuccessIsCounted() throws Exception {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = true;
        Thread finisher = new Thread(() -> {
            try {
                for (int i = 0; i < 100 && controller.getWorkerNodeName() == null; i++) {
                    Thread.sleep(50);
                }
                // journalId must match the round's maxJournalId, node must be the selected worker
                controller.finishCheckpoint(2L, "leader", null);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            withImageMetrics(() -> {
                long successBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, true);
                long failedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
                finisher.start();
                controller.runCheckpointControllerWithIds(1L, 2L, false);

                assertEquals(successBefore + 1L, MetricRepo.getImageWriteCount(JournalType.FE_META, true));
                assertEquals(failedBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
            });
        } finally {
            finisher.join(10_000);
            Config.checkpoint_only_on_leader = oldValue;
        }
    }

    /**
     * An interrupt while waiting on the worker must be reported as a failed image write, never
     * mistaken for a completed checkpoint.
     */
    @Test
    public void testCreateImageInterruptedIsCounted() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = true;
        try {
            withImageMetrics(() -> {
                long successBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, true);
                long failedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
                // makes the very first result.poll() throw InterruptedException
                Thread.currentThread().interrupt();
                controller.runCheckpointControllerWithIds(1L, 2L, false);

                assertEquals(successBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, true));
                assertEquals(failedBefore + 1L, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
            });
        } finally {
            // poll() clears the flag when it throws; clear again in case it never got there
            Thread.interrupted();
            Config.checkpoint_only_on_leader = oldValue;
        }
    }

    @Test
    public void testCreateImageTimesOutIsCounted() {
        boolean oldOnLeader = Config.checkpoint_only_on_leader;
        long oldTimeout = Config.checkpoint_timeout_seconds;
        Config.checkpoint_only_on_leader = true;
        // nothing ever reports back, so the wait loop must give up and report a failed image
        Config.checkpoint_timeout_seconds = 1;
        try {
            withImageMetrics(() -> {
                long successBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, true);
                long failedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
                controller.runCheckpointControllerWithIds(1L, 2L, false);

                assertEquals(successBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, true));
                assertEquals(failedBefore + 1L, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
            });
        } finally {
            Config.checkpoint_timeout_seconds = oldTimeout;
            Config.checkpoint_only_on_leader = oldOnLeader;
        }
    }

    /**
     * The worker can drop out of the cluster between being selected and being looked up; that has
     * to abort the round rather than NPE.
     */
    @Test
    public void testCreateImageWorkerVanishedIsCounted() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = true;
        NodeMgr spyNodeMgr = Mockito.spy(nodeMgr);
        Mockito.doReturn(null).when(spyNodeMgr).getFeByName(Mockito.anyString());
        Mockito.when(globalStateMgr.getNodeMgr()).thenReturn(spyNodeMgr);
        try {
            withImageMetrics(() -> {
                long successBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, true);
                long failedBefore = MetricRepo.getImageWriteCount(JournalType.FE_META, false);
                controller.runCheckpointControllerWithIds(1L, 2L, false);

                assertEquals(successBefore, MetricRepo.getImageWriteCount(JournalType.FE_META, true));
                assertEquals(failedBefore + 1L, MetricRepo.getImageWriteCount(JournalType.FE_META, false));
            });
        } finally {
            Mockito.when(globalStateMgr.getNodeMgr()).thenReturn(nodeMgr);
            Config.checkpoint_only_on_leader = oldValue;
        }
    }

    /**
     * Each unreachable node gets its own failure series and stays queued for the next round. The
     * per-node label identifies which registered FE is preventing deleteOldJournals() from running.
     */
    @Test
    public void testPushImageFailureIsCountedByNode() {
        withImageMetrics(() -> {
            controller.nodesToPushImage.add("follower1");
            controller.nodesToPushImage.add("follower2");

            long follower1Before = MetricRepo.getImagePushCount(JournalType.FE_META, "follower1", false);
            long follower2Before = MetricRepo.getImagePushCount(JournalType.FE_META, "follower2", false);
            long follower1SuccessBefore = MetricRepo.getImagePushCount(JournalType.FE_META, "follower1", true);
            long follower2SuccessBefore = MetricRepo.getImagePushCount(JournalType.FE_META, "follower2", true);

            controller.pushImage(100L);

            assertEquals(follower1Before + 1L,
                    MetricRepo.getImagePushCount(JournalType.FE_META, "follower1", false));
            assertEquals(follower2Before + 1L,
                    MetricRepo.getImagePushCount(JournalType.FE_META, "follower2", false));
            assertEquals(follower1SuccessBefore,
                    MetricRepo.getImagePushCount(JournalType.FE_META, "follower1", true));
            assertEquals(follower2SuccessBefore,
                    MetricRepo.getImagePushCount(JournalType.FE_META, "follower2", true));
            assertTrue(controller.nodesToPushImage.contains("follower1"),
                    "a node that failed to receive the image must stay queued for retry");
            assertTrue(controller.nodesToPushImage.contains("follower2"),
                    "a node that failed to receive the image must stay queued for retry");
        });
    }

    /**
     * A node that disappeared from the cluster is dropped from the queue instead of being retried
     * forever.
     */
    @Test
    public void testPushImageDropsUnknownNode() {
        withImageMetrics(() -> {
            controller.nodesToPushImage.add("gone");

            controller.pushImage(100L);

            assertTrue(controller.nodesToPushImage.isEmpty());
            assertEquals(0L, MetricRepo.getImagePushCount(JournalType.FE_META, "gone", false));
        });
    }

    /**
     * The reachable case, which the widened catch clause must not have swallowed: every peer
     * answers with its replayed journal id, the lowest one wins, and journals are deleted up to it
     * rather than falling back to the "delete nothing" version 0.
     */
    @Test
    public void testMinReplayedJournalIdIsHonouredWhenPeersAnswer() throws Exception {
        Journal journal = Mockito.mock(Journal.class);
        Mockito.when(journal.getPrefix()).thenReturn("");
        Mockito.when(journal.getDatabaseNames()).thenReturn(List.of(1L, 101L));
        CheckpointController controller = new CheckpointController("test-reachable", journal, "");

        int oldPort = Config.http_port;
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        // follower1 lags behind follower2, so 150 is the safe delete point
        AtomicInteger call = new AtomicInteger();
        server.createContext("/journal_id", exchange -> {
            exchange.getResponseHeaders().add("id", call.getAndIncrement() == 0 ? "150" : "900");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            Config.http_port = server.getAddress().getPort();

            controller.deleteOldJournals(500L);

            // min(imageVersion=500, minReplayed=150) = 150, so journals <= 150 go away
            Mockito.verify(journal).deleteJournals(151L);
        } finally {
            server.stop(0);
            Config.http_port = oldPort;
        }
    }

    @Test
    public void testPushImageSuccessDequeuesNode() throws Exception {
        int oldPort = Config.http_port;
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/put", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            Config.http_port = server.getAddress().getPort();
            withImageMetrics(() -> {
                controller.nodesToPushImage.add("follower1");
                long successBefore = MetricRepo.getImagePushCount(JournalType.FE_META, "follower1", true);

                controller.pushImage(100L);

                assertEquals(successBefore + 1L,
                        MetricRepo.getImagePushCount(JournalType.FE_META, "follower1", true));
                assertTrue(controller.nodesToPushImage.isEmpty(),
                        "a node that received the image must leave the queue");
            });
        } finally {
            server.stop(0);
            Config.http_port = oldPort;
        }
    }

    /** Enables metric recording without standing up the whole repo. */
    private void withImageMetrics(Runnable body) {
        boolean oldHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            body.run();
        } finally {
            MetricRepo.hasInit = oldHasInit;
        }
    }

    @Test
    public void testGetWorkers_sortByHeapUsedPercent() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = false;
        List<Frontend> workers = controller.getWorkers(false);
        // follower1 heapUsedPercent=20, follower2=30, leader=10(MAX)
        int idx1 = workers.indexOf(follower1);
        int idx2 = workers.indexOf(follower2);
        int idxLeader = workers.indexOf(leader);
        assertTrue(idx1 < idx2);
        assertEquals(2, idxLeader);
        Config.checkpoint_only_on_leader = oldValue;
    }
}
