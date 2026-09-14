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

package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GtidGeneratorTest {

    private TestableGtidGenerator gtidGenerator;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        gtidGenerator = new TestableGtidGenerator();
        gtidGenerator.clock = 1_700_000_000_000L;
    }

    @Test
    public void testNextGtidRequiresLeader() {
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);
        // Every FE builds a generator at startup, so construction alone must not generate a gtid.
        TestableGtidGenerator generator = new TestableGtidGenerator();
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, generator::nextGtid);
        Assertions.assertTrue(e.getMessage().contains("leader"), e.getMessage());

        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        generator.clock = System.currentTimeMillis();
        Assertions.assertTrue(generator.nextGtid() > 0);
    }

    @Test
    public void testNextGtidIncrementsSequenceOnSameMillisecond() {
        long firstGtid = gtidGenerator.nextGtid();
        long secondGtid = gtidGenerator.nextGtid();

        Assertions.assertNotEquals(firstGtid, secondGtid, "GTIDs should be unique");
        Assertions.assertEquals(firstGtid >> GtidGenerator.TIMESTAMP_SHIFT, secondGtid >> GtidGenerator.TIMESTAMP_SHIFT);
        Assertions.assertEquals((firstGtid & GtidGenerator.MAX_SEQUENCE) + 1, secondGtid & GtidGenerator.MAX_SEQUENCE,
                "Sequence should increment by 1 on the same millisecond");
    }

    @Test
    public void testNextGtidAdvancesTimestampOnSequenceOverflow() {
        gtidGenerator.setLastGtid((GtidGenerator.MAX_SEQUENCE << GtidGenerator.CLUSTER_ID_SHIFT)
                | GtidGenerator.MAX_SEQUENCE);
        long overflowGtid = gtidGenerator.nextGtid();

        Assertions.assertTrue((overflowGtid >> GtidGenerator.TIMESTAMP_SHIFT) > 0,
                "Timestamp should advance when sequence overflows");
    }

    @Test
    public void testSetLastGtidCorrectlyUpdatesState() {
        long expectedTimestamp = System.currentTimeMillis();
        long expectedSequence = 123L;
        long customGtid = GtidGenerator.encode(expectedTimestamp, expectedSequence);

        gtidGenerator.setLastGtid(customGtid);
        long actualGtid = gtidGenerator.lastGtid();

        Assertions.assertEquals(customGtid, actualGtid, "The GTID after setting last GTID should match the custom GTID");
    }

    @Test
    public void testNextGtidResetsSequenceOnNewMillisecond() {
        gtidGenerator.nextGtid();
        gtidGenerator.clock += 1;
        long nextGtid = gtidGenerator.nextGtid();

        Assertions.assertEquals(0, nextGtid & GtidGenerator.MAX_SEQUENCE,
                "Sequence should reset on a new millisecond");
    }

    @Test
    public void testNextGtidWhenSystemClockGoesBackwards() {
        gtidGenerator.setLastGtid(Long.MAX_VALUE);
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> gtidGenerator.nextGtid(),
                "Should throw an IllegalStateException when the system clock goes backwards");
        Assertions.assertTrue(e.getMessage().contains("Timestamp overflow"), e.getMessage());
    }

    @Test
    public void testNextGtidWhenSystemClockGoesBackwardsLittle() {
        long gtid = GtidGenerator.getGtid(gtidGenerator.clock + 5000);
        gtidGenerator.setLastGtid(gtid);
        long nextGtid = gtidGenerator.nextGtid();
        Assertions.assertEquals(nextGtid >> GtidGenerator.TIMESTAMP_SHIFT, gtid >> GtidGenerator.TIMESTAMP_SHIFT,
                "GTID should be positive");
        Assertions.assertTrue(nextGtid > gtid, "GTID should be positive");
    }

    @Test
    public void testClusterIdInGtid() {
        long gtid = gtidGenerator.nextGtid();
        long clusterId = (gtid >> GtidGenerator.CLUSTER_ID_SHIFT) & GtidGenerator.MAX_CLUSTER_ID;
        Assertions.assertEquals(GtidGenerator.CLUSTER_ID, clusterId, "Cluster ID should be correctly set in GTID");
    }

    @Test
    public void testGtidTimestampAdvancement() {
        long firstGtid = gtidGenerator.nextGtid();
        long secondGtid = gtidGenerator.nextGtid();
        long firstTimestamp = firstGtid >> GtidGenerator.TIMESTAMP_SHIFT;
        long secondTimestamp = secondGtid >> GtidGenerator.TIMESTAMP_SHIFT;

        Assertions.assertTrue(secondTimestamp >= firstTimestamp,
                "Timestamp should advance or stay the same on subsequent GTIDs");
    }

    @Test
    public void testFirstIssuePersistsOnceAndStaysInsideWindow() {
        long first = gtidGenerator.nextGtid();
        Assertions.assertEquals(1, gtidGenerator.persistCount);
        long timestamp = (first >> GtidGenerator.TIMESTAMP_SHIFT) + GtidGenerator.EPOCH;
        long expectedBound = GtidGenerator.nextBatchEndGtid(timestamp);
        Assertions.assertEquals(expectedBound, gtidGenerator.getBatchEndGtid());
        Assertions.assertEquals(GtidGenerator.encode(timestamp + Config.gtid_batch_window_ms - 1,
                GtidGenerator.MAX_SEQUENCE), expectedBound);

        gtidGenerator.clock = timestamp + 1;
        gtidGenerator.nextGtid();
        Assertions.assertEquals(1, gtidGenerator.persistCount);
    }

    @Test
    public void testCrossingBoundaryPersistsAgain() {
        long window = 10L;
        long oldWindow = Config.gtid_batch_window_ms;
        Config.gtid_batch_window_ms = window;
        try {
            long first = gtidGenerator.nextGtid();
            Assertions.assertEquals(1, gtidGenerator.persistCount);
            long firstTs = (first >> GtidGenerator.TIMESTAMP_SHIFT) + GtidGenerator.EPOCH;
            long bound = gtidGenerator.getBatchEndGtid();
            gtidGenerator.clock = firstTs + window;
            gtidGenerator.nextGtid();
            Assertions.assertEquals(2, gtidGenerator.persistCount);
            Assertions.assertEquals(GtidGenerator.nextBatchEndGtid(firstTs + window),
                    gtidGenerator.getBatchEndGtid());
            Assertions.assertTrue(gtidGenerator.getBatchEndGtid() > bound);
        } finally {
            Config.gtid_batch_window_ms = oldWindow;
        }
    }

    @Test
    public void testSequenceOverflowCrossingBoundaryPersists() {
        long window = 10L;
        long oldWindow = Config.gtid_batch_window_ms;
        Config.gtid_batch_window_ms = window;
        try {
            long first = gtidGenerator.nextGtid();
            long firstTs = (first >> GtidGenerator.TIMESTAMP_SHIFT) + GtidGenerator.EPOCH;
            long bound = gtidGenerator.getBatchEndGtid();
            gtidGenerator.setLastGtid(bound);
            gtidGenerator.clock = firstTs + window - 1;
            int persistsBefore = gtidGenerator.persistCount;
            long overflowGtid = gtidGenerator.nextGtid();
            Assertions.assertEquals(persistsBefore + 1, gtidGenerator.persistCount);
            Assertions.assertEquals(GtidGenerator.getGtid(firstTs + window), overflowGtid);
            Assertions.assertTrue(overflowGtid > bound);
        } finally {
            Config.gtid_batch_window_ms = oldWindow;
        }
    }

    @Test
    public void testPersistFailureDoesNotIssueOrMutateState() {
        gtidGenerator.persistShouldFail = true;
        Assertions.assertThrows(RuntimeException.class, () -> gtidGenerator.nextGtid());
        Assertions.assertEquals(0L, gtidGenerator.getBatchEndGtid());
        Assertions.assertEquals(-1L, gtidGenerator.getLastTimestamp());
        Assertions.assertEquals(0L, gtidGenerator.getLastSequence());
        Assertions.assertEquals(1, gtidGenerator.persistCount);

        gtidGenerator.persistShouldFail = false;
        long gtid = gtidGenerator.nextGtid();
        Assertions.assertTrue(gtid > 0);
        Assertions.assertEquals(2, gtidGenerator.persistCount);
        Assertions.assertTrue(gtidGenerator.getBatchEndGtid() > 0);
    }

    @Test
    public void testInitOnlyMovesForwardAndRecoversClock() {
        long t1 = 1_700_000_000_000L;
        long bound1 = GtidGenerator.encode(t1, GtidGenerator.MAX_SEQUENCE);
        gtidGenerator.init(bound1);
        Assertions.assertEquals(bound1, gtidGenerator.getBatchEndGtid());
        Assertions.assertEquals(t1, gtidGenerator.getLastTimestamp());
        Assertions.assertEquals(GtidGenerator.MAX_SEQUENCE, gtidGenerator.getLastSequence());

        gtidGenerator.init(bound1 - 1000);
        Assertions.assertEquals(bound1, gtidGenerator.getBatchEndGtid());
        Assertions.assertEquals(t1, gtidGenerator.getLastTimestamp());

        long t2 = t1 + 5000;
        long bound2 = GtidGenerator.encode(t2, GtidGenerator.MAX_SEQUENCE);
        gtidGenerator.init(bound2);
        Assertions.assertEquals(bound2, gtidGenerator.getBatchEndGtid());
        Assertions.assertEquals(t2, gtidGenerator.getLastTimestamp());
        Assertions.assertEquals(GtidGenerator.MAX_SEQUENCE, gtidGenerator.getLastSequence());
    }

    @Test
    public void testRecoveredGeneratorFirstGtidIsNextAfterBatchEnd() {
        long bound = GtidGenerator.nextBatchEndGtid(gtidGenerator.clock);
        gtidGenerator.init(bound);
        gtidGenerator.clock = gtidGenerator.getLastTimestamp() - 3000;
        long expected = GtidGenerator.getGtid(gtidGenerator.getLastTimestamp() + 1);
        long gtid = gtidGenerator.nextGtid();
        Assertions.assertEquals(expected, gtid);
        Assertions.assertTrue(gtid > bound);
    }

    @Test
    public void testBatchWindowClampsBelowOne() {
        long oldWindow = Config.gtid_batch_window_ms;
        Config.gtid_batch_window_ms = 0;
        try {
            long first = gtidGenerator.nextGtid();
            long timestamp = (first >> GtidGenerator.TIMESTAMP_SHIFT) + GtidGenerator.EPOCH;
            Assertions.assertEquals(GtidGenerator.encode(timestamp, GtidGenerator.MAX_SEQUENCE),
                    gtidGenerator.getBatchEndGtid());
        } finally {
            Config.gtid_batch_window_ms = oldWindow;
        }
    }

    @Test
    public void testTimestampAheadOfClockMsBeforeAndAfterIssue() {
        Assertions.assertEquals(0L, gtidGenerator.getTimestampAheadOfClockMs());
        gtidGenerator.setLastGtid(GtidGenerator.getGtid(gtidGenerator.clock + 10_000));
        Assertions.assertEquals(10_000L, gtidGenerator.getTimestampAheadOfClockMs());
        gtidGenerator.clock += 10_000;
        Assertions.assertEquals(0L, gtidGenerator.getTimestampAheadOfClockMs());
        gtidGenerator.clock += 1;
        Assertions.assertEquals(0L, gtidGenerator.getTimestampAheadOfClockMs());
    }

    @Test
    public void testFailoverFromSlowerClockStaysAbovePreviousIds() {
        long window = 10L;
        long oldWindow = Config.gtid_batch_window_ms;
        Config.gtid_batch_window_ms = window;
        try {
            TestableGtidGenerator leaderA = new TestableGtidGenerator();
            leaderA.clock = 1_700_000_000_000L;
            long lastFromA = 0;
            for (int i = 0; i < 5; i++) {
                lastFromA = leaderA.nextGtid();
                leaderA.clock += 1;
            }

            TestableGtidGenerator leaderB = new TestableGtidGenerator();
            leaderB.init(leaderA.getBatchEndGtid());
            leaderB.clock = leaderA.clock - 50_000;
            long firstFromB = leaderB.nextGtid();
            Assertions.assertTrue(firstFromB > lastFromA);
        } finally {
            Config.gtid_batch_window_ms = oldWindow;
        }
    }

    @Test
    public void testLeaderApplyDoesNotResetLastSequence() {
        long first = gtidGenerator.nextGtid();
        long second = gtidGenerator.nextGtid();
        Assertions.assertEquals(1, gtidGenerator.persistCount);
        Assertions.assertEquals(0L, first & GtidGenerator.MAX_SEQUENCE);
        Assertions.assertEquals(1L, second & GtidGenerator.MAX_SEQUENCE);
        Assertions.assertEquals(1L, gtidGenerator.getLastSequence());
    }

    @Test
    public void testImageRoundTripRestoresBatchEnd() throws Exception {
        gtidGenerator.nextGtid();
        long batchEnd = gtidGenerator.getBatchEndGtid();
        String json = GsonUtils.GSON.toJson(gtidGenerator);
        Assertions.assertTrue(json.contains("\"bi\":"));
        Assertions.assertFalse(json.contains("lastTimestamp"));
        Assertions.assertFalse(json.contains("lastSequence"));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        gtidGenerator.save(image.getImageWriter());

        GtidGenerator loaded = new GtidGenerator();
        loaded.load(image.getMetaBlockReader());
        Assertions.assertEquals(batchEnd, loaded.getBatchEndGtid());
        Assertions.assertEquals((batchEnd >> GtidGenerator.TIMESTAMP_SHIFT) + GtidGenerator.EPOCH,
                loaded.getLastTimestamp());
        Assertions.assertEquals(GtidGenerator.MAX_SEQUENCE, loaded.getLastSequence());

        TestableGtidGenerator recovered = new TestableGtidGenerator();
        recovered.init(loaded.getBatchEndGtid());
        recovered.clock = recovered.getLastTimestamp() - 1000;
        long expected = GtidGenerator.getGtid(recovered.getLastTimestamp() + 1);
        Assertions.assertEquals(expected, recovered.nextGtid());
    }

    @Test
    public void testMissingImageBlockLeavesZeroWatermark() {
        GtidGenerator generator = new GtidGenerator();
        Assertions.assertEquals(0L, generator.getBatchEndGtid());
        Assertions.assertEquals(-1L, generator.getLastTimestamp());
        Assertions.assertEquals(0L, generator.getLastSequence());
    }

    @Test
    public void testFirstGtidAfterInitLogsOnlyWhenTimestampAheadOfNow() {
        long start = gtidGenerator.clock;
        gtidGenerator.init(GtidGenerator.encode(start + 10, GtidGenerator.MAX_SEQUENCE));

        gtidGenerator.clock = start + 11;
        gtidGenerator.nextGtid();
        Assertions.assertEquals(0, gtidGenerator.firstGtidAfterInitLogCount);

        gtidGenerator.init(GtidGenerator.encode(start + 100_000, GtidGenerator.MAX_SEQUENCE));
        gtidGenerator.clock = start;
        gtidGenerator.nextGtid();
        Assertions.assertEquals(1, gtidGenerator.firstGtidAfterInitLogCount);

        gtidGenerator.nextGtid();
        Assertions.assertEquals(1, gtidGenerator.firstGtidAfterInitLogCount);
    }

    @Test
    public void testLeaderApplyDoesNotArmFirstGtidAfterInit() {
        gtidGenerator.nextGtid();
        Assertions.assertEquals(0, gtidGenerator.firstGtidAfterInitLogCount);
        gtidGenerator.clock = gtidGenerator.getLastTimestamp() - 1000;
        gtidGenerator.nextGtid();
        Assertions.assertEquals(0, gtidGenerator.firstGtidAfterInitLogCount);
    }

    @Test
    public void testHugeWindowDoesNotWrapOrIssue() {
        long oldWindow = Config.gtid_batch_window_ms;
        Config.gtid_batch_window_ms = Long.MAX_VALUE;
        try {
            IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                    () -> GtidGenerator.nextBatchEndGtid(gtidGenerator.clock));
            Assertions.assertTrue(e.getMessage().contains("gtid_batch_window_ms"), e.getMessage());

            e = Assertions.assertThrows(IllegalStateException.class, () -> gtidGenerator.nextGtid());
            Assertions.assertTrue(e.getMessage().contains("gtid_batch_window_ms"), e.getMessage());
            Assertions.assertEquals(0, gtidGenerator.persistCount);
            Assertions.assertEquals(0L, gtidGenerator.getBatchEndGtid());
            Assertions.assertEquals(-1L, gtidGenerator.getLastTimestamp());
        } finally {
            Config.gtid_batch_window_ms = oldWindow;
        }
    }

    @Test
    public void testWindowExceedingRemainingTimestampRangeThrows() {
        long oldWindow = Config.gtid_batch_window_ms;
        try {
            Config.gtid_batch_window_ms = 1L;
            Assertions.assertEquals(
                    GtidGenerator.encode(GtidGenerator.MAX_TIMESTAMP, GtidGenerator.MAX_SEQUENCE),
                    GtidGenerator.nextBatchEndGtid(GtidGenerator.MAX_TIMESTAMP));

            Config.gtid_batch_window_ms = 2L;
            Assertions.assertEquals(
                    GtidGenerator.encode(GtidGenerator.MAX_TIMESTAMP, GtidGenerator.MAX_SEQUENCE),
                    GtidGenerator.nextBatchEndGtid(GtidGenerator.MAX_TIMESTAMP - 1));
            IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                    () -> GtidGenerator.nextBatchEndGtid(GtidGenerator.MAX_TIMESTAMP));
            Assertions.assertTrue(e.getMessage().contains("remaining timestamp range"), e.getMessage());
        } finally {
            Config.gtid_batch_window_ms = oldWindow;
        }
    }

    private static final class TestableGtidGenerator extends GtidGenerator {
        long clock = System.currentTimeMillis();
        boolean persistShouldFail;
        int persistCount;
        int firstGtidAfterInitLogCount;

        @Override
        protected long timeGen() {
            return clock;
        }

        @Override
        protected void persistBatchEndGtid(long newBatchEndGtid) {
            persistCount++;
            if (persistShouldFail) {
                throw new RuntimeException("persist failed");
            }
            applyBatchEndGtid(newBatchEndGtid);
        }

        @Override
        protected void logFirstGtidAfterInit(long timestamp, long now) {
            firstGtidAfterInitLogCount++;
        }
    }
}
