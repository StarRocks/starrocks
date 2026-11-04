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

package com.starrocks.server;

import com.starrocks.catalog.MetaReplayState;
import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalCursor;
import com.starrocks.journal.JournalEntity;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The point of the meta freshness checker is that the canRead/isReady verdict no longer waits for the
 * replayer: {@link #testStaleMetaIsReportedWhileOneJournalIsStuckInTheApplier} is the case that used
 * to be unreportable, the rest pin down the decision table around it.
 */
public class MetaFreshnessCheckerTest {
    private int savedTolerationSecond;
    private boolean savedIgnoreMetaCheck;
    private long savedCheckIntervalMs;
    private long savedStuckThresholdSecond;

    private String savedMetaDir;
    private String savedPluginDir;
    private String testMetaDir;
    private String testPluginDir;
    private GlobalStateMgr globalStateMgr;
    private GlobalStateMgr.MetaFreshnessChecker checker;

    @BeforeEach
    public void setUp() {
        testMetaDir = UUID.randomUUID().toString();
        testPluginDir = UUID.randomUUID().toString();
        savedMetaDir = Config.meta_dir;
        savedPluginDir = Config.plugin_dir;
        Config.meta_dir = testMetaDir;
        Config.plugin_dir = testPluginDir;

        savedTolerationSecond = Config.meta_delay_toleration_second;
        savedIgnoreMetaCheck = Config.ignore_meta_check;
        savedCheckIntervalMs = Config.meta_freshness_check_interval_ms;
        savedStuckThresholdSecond = Config.metadata_replay_stuck_warn_threshold_second;

        Config.meta_delay_toleration_second = 1;
        Config.ignore_meta_check = false;

        globalStateMgr = new GlobalStateMgr(new NodeMgr());
        globalStateMgr.setFrontendNodeType(FrontendNodeType.FOLLOWER);
        checker = new GlobalStateMgr.MetaFreshnessChecker(globalStateMgr, globalStateMgr.getMetaReplayProgress());
    }

    @AfterEach
    public void tearDown() {
        FileUtils.deleteQuietly(new File(testMetaDir));
        FileUtils.deleteQuietly(new File(testPluginDir));

        // the directories above are gone now, so leaving these pointed at them would hand any later
        // test in this JVM a meta_dir that does not exist
        Config.meta_dir = savedMetaDir;
        Config.plugin_dir = savedPluginDir;

        Config.meta_delay_toleration_second = savedTolerationSecond;
        Config.ignore_meta_check = savedIgnoreMetaCheck;
        Config.meta_freshness_check_interval_ms = savedCheckIntervalMs;
        Config.metadata_replay_stuck_warn_threshold_second = savedStuckThresholdSecond;
    }

    /**
     * Regression test for the defect this class was added for: one journal entry blocks in the applier
     * for longer than meta_delay_toleration_second. The replayer cannot report that - it is inside
     * loadJournal and its flow control is only checked between entries - so the verdict has to come
     * from the checker, while the applier is still blocked.
     */
    @Test
    public void testStaleMetaIsReportedWhileOneJournalIsStuckInTheApplier() throws Exception {
        CountDownLatch applierEntered = new CountDownLatch(1);
        CountDownLatch releaseApplier = new CountDownLatch(1);
        globalStateMgr.setEditLog(new BlockingEditLog(applierEntered, releaseApplier));

        // a node that is up to date and serving reads
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());
        assertTrue(globalStateMgr.isReady());

        JournalCursor cursor = new SingleEntryCursor(new JournalEntity(OperationType.OP_TIMESTAMP_V2, null));
        Thread replayer = new Thread(() -> {
            try {
                globalStateMgr.replayJournalInner(cursor, true);
            } catch (Exception e) {
                // the applier is released by the test, nothing here should throw
                throw new IllegalStateException(e);
            }
        }, "test-replayer");
        replayer.setDaemon(true);
        replayer.start();
        assertTrue(applierEntered.await(30, TimeUnit.SECONDS), "replay never reached the applier");

        // the leader keeps writing, but this node's clock stopped at the entry it is stuck on
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();

        assertFalse(globalStateMgr.canRead(), "a node stuck mid-journal must stop serving stale reads");
        assertFalse(globalStateMgr.isReady());
        assertEquals(MetaReplayState.MetaState.OUT_OF_DATE, globalStateMgr.getMetaReplayState().state);
        // the whole point: the verdict landed without waiting for the applier to finish
        assertTrue(replayer.isAlive());
        assertEquals(1L, releaseApplier.getCount());

        releaseApplier.countDown();
        replayer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse(replayer.isAlive());

        // once the entry lands and the clock catches up, the node serves again
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());
        assertTrue(globalStateMgr.isReady());
    }

    @Test
    public void testFreshMetaServesReads() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();

        assertTrue(globalStateMgr.canRead());
        assertTrue(globalStateMgr.isReady());
    }

    @Test
    public void testReplayFailureStopsServing() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        globalStateMgr.getMetaReplayProgress().recordFailure(new IllegalStateException("replay blew up"));
        checker.evaluate();
        assertFalse(globalStateMgr.canRead());
        assertFalse(globalStateMgr.isReady());
        assertEquals(MetaReplayState.MetaState.REPLAY_EXCEPTION, globalStateMgr.getMetaReplayState().state);

        // still failing at the next check, with no new exception recorded: reads stay off
        checker.evaluate();
        assertFalse(globalStateMgr.canRead());
        assertEquals(MetaReplayState.MetaState.REPLAY_EXCEPTION, globalStateMgr.getMetaReplayState().state);

        globalStateMgr.getMetaReplayProgress().clearFailure();
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());
        assertTrue(globalStateMgr.isReady());
        assertEquals(MetaReplayState.MetaState.OK, globalStateMgr.getMetaReplayState().state);
    }

    /**
     * A replay cycle that fails and a later one that succeeds can both land between two evaluations:
     * the replayer backs off for 5s after an exception, which is the same order as the largest check
     * interval the knob allows. The checker never has to reconstruct that after the fact, because the
     * replayer takes the node out of service in the failing cycle itself; by the time a check runs,
     * replay has recovered and putting the node back is the right answer.
     */
    @Test
    public void testFailureBetweenTwoChecksStopsServingWhenItHappens() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        // the whole failure/recovery cycle happens while the checker is asleep
        globalStateMgr.publishReplayFailure(new IllegalStateException("transient replay failure"));
        assertFalse(globalStateMgr.canRead(), "reads must stop in the failing cycle, not at the next check");
        assertFalse(globalStateMgr.isReady());
        assertEquals(MetaReplayState.MetaState.REPLAY_EXCEPTION, globalStateMgr.getMetaReplayState().state);
        assertEquals("transient replay failure", globalStateMgr.getMetaReplayState().throwable.getMessage());

        globalStateMgr.getMetaReplayProgress().clearFailure();

        // replay has recovered by the time the checker wakes, so it serves again at the first check
        checker.evaluate();
        assertTrue(globalStateMgr.canRead(), "a recovered replayer must not cost an extra check interval");
        assertTrue(globalStateMgr.isReady());
        assertEquals(MetaReplayState.MetaState.OK, globalStateMgr.getMetaReplayState().state);
    }

    /**
     * A replay failure that has since been recovered from must not stay visible once the verdict moves
     * on to staleness. The old replayer-local check called setOk() after every successful cycle, which
     * cleared the throwable before setCanRead() could mark the node OUT_OF_DATE; the checker reaches
     * setOutOfDate() without passing through setOk(), so /api/_meta_replay_state would otherwise report
     * a resolved exception beside OUT_OF_DATE for the whole catch-up.
     */
    @Test
    public void testRecoveredExceptionIsNotReportedWithStaleness() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();

        globalStateMgr.getMetaReplayProgress().recordFailure(new IllegalStateException("replay blew up"));
        checker.evaluate();
        assertEquals(MetaReplayState.MetaState.REPLAY_EXCEPTION, globalStateMgr.getMetaReplayState().state);
        assertNotNull(globalStateMgr.getMetaReplayState().throwable);

        // replay recovers but is still catching up: behind the leader's clock, and applying entries
        globalStateMgr.getMetaReplayProgress().clearFailure();
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        globalStateMgr.getMetaReplayProgress().beginEntry(9L, OperationType.OP_TIMESTAMP_V2);
        checker.evaluate();

        assertFalse(globalStateMgr.canRead());
        assertEquals(MetaReplayState.MetaState.OUT_OF_DATE, globalStateMgr.getMetaReplayState().state);
        assertNull(globalStateMgr.getMetaReplayState().throwable,
                "a recovered failure must not be reported alongside OUT_OF_DATE");
        assertEquals("NULL", globalStateMgr.getMetaReplayState().getInfo().get("exception"));
    }

    @Test
    public void testIgnoreMetaCheckKeepsReadingButNotReady() {
        Config.ignore_meta_check = true;
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        globalStateMgr.getMetaReplayProgress().beginEntry(7L, OperationType.OP_TIMESTAMP_V2);

        checker.evaluate();

        assertTrue(globalStateMgr.canRead());
        assertFalse(globalStateMgr.isReady());
    }

    /**
     * ignore_meta_check waives the delay verdict only. A replay that threw leaves an incomplete image
     * rather than an old one, so the node stops serving however it is configured, and comes back only
     * once replay succeeds again.
     */
    @Test
    public void testIgnoreMetaCheckDoesNotWaiveAReplayFailure() {
        Config.ignore_meta_check = true;
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        globalStateMgr.getMetaReplayProgress().recordFailure(new IllegalStateException("replay blew up"));
        checker.evaluate();

        assertFalse(globalStateMgr.canRead(), "ignore_meta_check must not keep a failed replay in service");
        assertFalse(globalStateMgr.isReady());
        assertEquals(MetaReplayState.MetaState.REPLAY_EXCEPTION, globalStateMgr.getMetaReplayState().state);

        // recovery is driven by replay succeeding, not by the flag
        globalStateMgr.getMetaReplayProgress().clearFailure();
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());
        assertFalse(globalStateMgr.isReady(), "ignore_meta_check still means not ready");
    }

    /**
     * Staleness alone is not enough: on an idle cluster the leader writes nothing, so falling behind its
     * clock says nothing about this node. Kept from the original setCanRead() behaviour.
     */
    @Test
    public void testStaleButIdleFollowerKeepsServing() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();

        assertTrue(globalStateMgr.canRead(), "an idle cluster must not take a follower out of service");
    }

    @Test
    public void testStaleAndIdleUnknownNodeStopsServing() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        // disconnected from the leader: no journal to read, and no leader to tell us we are behind
        globalStateMgr.setFrontendNodeType(FrontendNodeType.UNKNOWN);
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();

        assertFalse(globalStateMgr.canRead());
        assertFalse(globalStateMgr.isReady());
    }

    @Test
    public void testStaleFollowerThatRecentlyAppliedStopsServing() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        // the leader is alive and we are applying its journals, we are simply behind
        GlobalStateMgr.MetaReplayProgress progress = globalStateMgr.getMetaReplayProgress();
        progress.beginEntry(11L, OperationType.OP_TIMESTAMP_V2);
        progress.endEntry();
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();

        assertFalse(globalStateMgr.canRead());
        assertFalse(globalStateMgr.isReady());
    }

    @Test
    public void testLeaderVerdictIsNotTouched() throws Exception {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        // stale by every measure, but on a leader the activation path owns canRead/isReady
        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        globalStateMgr.getMetaReplayProgress().beginEntry(3L, OperationType.OP_TIMESTAMP_V2);
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());
        assertTrue(globalStateMgr.isReady());

        // and the same while the node is on its way to becoming one
        globalStateMgr.setFrontendNodeType(FrontendNodeType.FOLLOWER);
        setInTransferringToLeader(true);
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());
        assertTrue(globalStateMgr.isReady());

        setInTransferringToLeader(false);
        checker.evaluate();
        assertFalse(globalStateMgr.canRead());
    }

    @Test
    public void testMetaReplayLagIsReportedOnlyOffTheLeader() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 30_000L);
        assertTrue(globalStateMgr.getMetaReplayLagSecond() >= 30);

        globalStateMgr.setFrontendNodeType(FrontendNodeType.LEADER);
        assertEquals(0L, globalStateMgr.getMetaReplayLagSecond());

        // nothing replayed yet: there is no lag to report, not an infinite one
        globalStateMgr.setFrontendNodeType(FrontendNodeType.FOLLOWER);
        globalStateMgr.setSynchronizedTime(0);
        assertEquals(0L, globalStateMgr.getMetaReplayLagSecond());
    }

    @Test
    public void testInflightTrackingFollowsTheApplier() {
        GlobalStateMgr.MetaReplayProgress progress = new GlobalStateMgr.MetaReplayProgress();
        long now = System.currentTimeMillis();

        assertEquals(0L, progress.inflightElapsedMs(now));
        assertFalse(progress.isApplying());
        assertEquals(0L, progress.appliedSequence());

        progress.beginEntry(42L, OperationType.OP_CREATE_TABLE_V2);
        assertTrue(progress.isApplying());
        assertEquals(42L, progress.getInflightJournalId());
        assertEquals(OperationType.OP_CREATE_TABLE_V2, progress.getInflightOpCode());
        // a clock five minutes ahead stands in for an entry that has been applying for five minutes
        // (minus whatever real time passed between `now` and beginEntry())
        assertTrue(progress.inflightElapsedMs(now + 300_000L) >= 299_000L);
        // an entry is not counted as applied until it lands
        assertEquals(0L, progress.appliedSequence());

        progress.endEntry();
        assertFalse(progress.isApplying());
        assertEquals(0L, progress.inflightElapsedMs(System.currentTimeMillis()));
        assertEquals(1L, progress.appliedSequence());

        // endEntry() with nothing in flight (EOF, or a cursor that threw) counts nothing
        progress.endEntry();
        assertEquals(1L, progress.appliedSequence());
    }

    /**
     * A journal that lands and finishes between two evaluations must still take the node out of service.
     * The first cut of this class asked "did an entry land within the last checkInterval ms", but the
     * checker sleeps for its interval plus its own cycle time, so an entry applied in that sliver was
     * seen by neither evaluation and a stale follower kept serving. The sequence is consumed once, so
     * whatever the scheduling, the entry is accounted for by exactly one evaluation.
     */
    @Test
    public void testJournalAppliedBetweenTwoChecksIsNotMissed() {
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());

        // one entry lands and completes entirely between two evaluations, however long ago
        GlobalStateMgr.MetaReplayProgress progress = globalStateMgr.getMetaReplayProgress();
        progress.beginEntry(9L, OperationType.OP_TIMESTAMP_V2);
        progress.endEntry();
        assertFalse(progress.isApplying());

        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();
        assertFalse(globalStateMgr.canRead(), "an entry applied between checks must not go unnoticed");
        assertFalse(globalStateMgr.isReady());

        // and it counts once: with no further progress the verdict is not re-derived from it
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        checker.evaluate();
        assertTrue(globalStateMgr.canRead());
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();
        assertTrue(globalStateMgr.canRead(), "consumed progress must not be counted twice");
    }

    /**
     * metaReplayState is what `/api/show_meta_info` reports. The replayer polls roughly every
     * millisecond and the checker runs roughly once a second, so if the replayer published OK on every
     * successful poll it would erase the checker's OUT_OF_DATE almost as soon as it was set, and the
     * diagnostic would read OK for nearly the whole time the node is refusing reads.
     */
    @Test
    public void testOutOfDateStateSurvivesIdleReplayPolls() throws Exception {
        globalStateMgr.setEditLog(new NoOpEditLog());

        // the replayer applies an entry, so the checker sees a leader that is alive and writing
        globalStateMgr.replayJournalInner(
                new SingleEntryCursor(new JournalEntity(OperationType.OP_TIMESTAMP_V2, null)), true);
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        checker.evaluate();
        assertEquals(MetaReplayState.MetaState.OUT_OF_DATE, globalStateMgr.getMetaReplayState().state);
        assertFalse(globalStateMgr.canRead());

        // an idle replay poll: the cursor is at EOF, so nothing is applied and nothing is published
        globalStateMgr.replayJournalInner(new SingleEntryCursor(null), true);

        assertEquals(MetaReplayState.MetaState.OUT_OF_DATE, globalStateMgr.getMetaReplayState().state);
        assertFalse(globalStateMgr.canRead());
    }

    @Test
    public void testCheckIntervalIsClamped() {
        Config.meta_freshness_check_interval_ms = 0;
        assertEquals(10L, GlobalStateMgr.MetaFreshnessChecker.checkIntervalMs());

        Config.meta_freshness_check_interval_ms = 1_000_000L;
        assertEquals(5000L, GlobalStateMgr.MetaFreshnessChecker.checkIntervalMs());

        Config.meta_freshness_check_interval_ms = 1000L;
        assertEquals(1000L, GlobalStateMgr.MetaFreshnessChecker.checkIntervalMs());
    }

    @Test
    public void testTheCheckerIsWiredToTheProgressTheReplayerWritesTo() {
        globalStateMgr.createMetaFreshnessChecker();
        GlobalStateMgr.MetaFreshnessChecker wired = globalStateMgr.getMetaFreshnessChecker();

        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());
        wired.evaluate();
        assertTrue(globalStateMgr.canRead());

        // written by the replayer thread in replayJournalInner, read by the checker
        globalStateMgr.getMetaReplayProgress().beginEntry(5L, OperationType.OP_TIMESTAMP_V2);
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        wired.evaluate();
        assertFalse(globalStateMgr.canRead());
    }

    /**
     * runOneCycle() is what the daemon thread actually runs: it re-reads the interval knob before
     * publishing a verdict, so a runtime change of meta_freshness_check_interval_ms paces the sleep
     * that follows rather than taking effect a cycle late.
     */
    @Test
    public void testRunOneCycleAppliesTheClampedIntervalAndPublishesTheVerdict() {
        Config.meta_freshness_check_interval_ms = 1_000_000L;
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis());

        checker.runOneCycle();

        assertEquals(5000L, checker.getInterval(), "the interval knob is clamped on every cycle");
        assertTrue(globalStateMgr.canRead(), "runOneCycle must publish the verdict, not just pace itself");

        // a stale node, and a knob lowered at runtime: both land on the next cycle
        Config.meta_freshness_check_interval_ms = 50L;
        globalStateMgr.setSynchronizedTime(System.currentTimeMillis() - 60_000L);
        globalStateMgr.getMetaReplayProgress().beginEntry(3L, OperationType.OP_TIMESTAMP_V2);

        checker.runOneCycle();

        assertEquals(50L, checker.getInterval());
        assertFalse(globalStateMgr.canRead());
    }

    /**
     * The stuck-replay report is the diagnostic that names the applier holding up metadata replay. It
     * is rate limited so a stall lasting hours does not fill fe.log, and it is emitted regardless of
     * staleness, so a stall well inside meta_delay_toleration_second is still visible.
     */
    @Test
    public void testStuckReplayIsReportedOncePerWindow() {
        Config.metadata_replay_stuck_warn_threshold_second = 30;
        long now = System.currentTimeMillis();

        // nothing in flight: there is no stuck entry to report
        assertFalse(checker.reportStuckReplay(now));

        globalStateMgr.getMetaReplayProgress().beginEntry(11L, OperationType.OP_CREATE_TABLE_V2);

        // in flight, but not yet past the threshold
        assertFalse(checker.reportStuckReplay(now + 29_000L));

        // past the threshold: reported, with no replayer thread to dump yet
        assertTrue(checker.reportStuckReplay(now + 31_000L));

        // still stuck, but inside the log interval: suppressed
        assertFalse(checker.reportStuckReplay(now + 41_000L));

        // once the log interval has elapsed it is reported again, now with the replayer stack
        globalStateMgr.createReplayer();
        assertTrue(checker.reportStuckReplay(now + 62_000L));
    }

    @Test
    public void testStuckReplayReportCanBeDisabled() {
        Config.metadata_replay_stuck_warn_threshold_second = 0;
        globalStateMgr.getMetaReplayProgress().beginEntry(12L, OperationType.OP_CREATE_TABLE_V2);

        assertFalse(checker.reportStuckReplay(System.currentTimeMillis() + 600_000L));
    }

    /**
     * An entry that finishes stops being reported: endEntry() clears the in-flight marker, so a node
     * that recovered is not still described as frozen.
     */
    @Test
    public void testFinishedEntryIsNoLongerReportedAsStuck() {
        Config.metadata_replay_stuck_warn_threshold_second = 1;
        GlobalStateMgr.MetaReplayProgress progress = globalStateMgr.getMetaReplayProgress();
        long now = System.currentTimeMillis();

        progress.beginEntry(13L, OperationType.OP_TIMESTAMP_V2);
        assertTrue(checker.reportStuckReplay(now + 5_000L));

        progress.endEntry();
        assertFalse(checker.reportStuckReplay(now + 120_000L));
    }

    private void setInTransferringToLeader(boolean value) throws Exception {
        Field field = GlobalStateMgr.class.getDeclaredField("isInTransferringToLeader");
        field.setAccessible(true);
        field.setBoolean(globalStateMgr, value);
    }

    /**
     * Stands in for an applier that cannot make progress, e.g. one waiting on a database lock held by a
     * long running read on the same follower.
     */
    private static class BlockingEditLog extends EditLog {
        private final CountDownLatch entered;
        private final CountDownLatch release;

        BlockingEditLog(CountDownLatch entered, CountDownLatch release) {
            super(new ArrayBlockingQueue<>(1));
            this.entered = entered;
            this.release = release;
        }

        @Override
        public void loadJournal(GlobalStateMgr globalStateMgr, JournalEntity journal) {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class NoOpEditLog extends EditLog {
        NoOpEditLog() {
            super(new ArrayBlockingQueue<>(1));
        }

        @Override
        public void loadJournal(GlobalStateMgr globalStateMgr, JournalEntity journal) {
        }
    }

    private static class SingleEntryCursor implements JournalCursor {
        private final JournalEntity entity;
        private boolean served = false;

        SingleEntryCursor(JournalEntity entity) {
            this.entity = entity;
        }

        @Override
        public void refresh() {
        }

        @Override
        public JournalEntity next() {
            if (served) {
                return null;
            }
            served = true;
            return entity;
        }

        @Override
        public void close() {
        }

        @Override
        public void skipNext() {
        }
    }
}
