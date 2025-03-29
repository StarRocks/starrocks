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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GtidGeneratorTest {

    private GtidGenerator gtidGenerator;

    @BeforeEach
    public void setUp() {
        gtidGenerator = new GtidGenerator();
    }

    @Test
    public void testNextGtidIncrementsSequenceOnSameMillisecond() {
        long firstGtid = gtidGenerator.nextGtid();
        long secondGtid = gtidGenerator.nextGtid();

        Assertions.assertNotEquals(firstGtid, secondGtid, "GTIDs should be unique");
        if (firstGtid >> GtidGenerator.TIMESTAMP_SHIFT == secondGtid >> GtidGenerator.TIMESTAMP_SHIFT) {
            Assertions.assertEquals((firstGtid & GtidGenerator.MAX_SEQUENCE) + 1, secondGtid & GtidGenerator.MAX_SEQUENCE,
                    "Sequence should increment by 1 on the same millisecond");
        }
    }

    @Test
    public void testNextGtidAdvancesTimestampOnSequenceOverflow() {
        // Simulate sequence overflow
        gtidGenerator.setLastGtid((GtidGenerator.MAX_SEQUENCE << GtidGenerator.CLUSTER_ID_SHIFT) | GtidGenerator.MAX_SEQUENCE);
        long overflowGtid = gtidGenerator.nextGtid();

        Assertions.assertTrue((overflowGtid >> GtidGenerator.TIMESTAMP_SHIFT) > 0,
                "Timestamp should advance when sequence overflows");
    }

    @Test
    public void testSetLastGtidCorrectlyUpdatesState() {
        long expectedTimestamp = System.currentTimeMillis();
        long expectedSequence = 123L;
        long customGtid = ((expectedTimestamp - GtidGenerator.EPOCH) << GtidGenerator.TIMESTAMP_SHIFT)
                | (GtidGenerator.CLUSTER_ID << GtidGenerator.CLUSTER_ID_SHIFT) | expectedSequence;

        gtidGenerator.setLastGtid(customGtid);
        long actualGtid = gtidGenerator.lastGtid();

        Assertions.assertEquals(customGtid, actualGtid, "The GTID after setting last GTID should match the custom GTID");
    }

    @Test
    public void testNextGtidResetsSequenceOnNewMillisecond() throws InterruptedException {
        long firstGtid = gtidGenerator.nextGtid();
        Thread.sleep(1); // Ensure the next GTID is in a new millisecond
        long nextGtid = gtidGenerator.nextGtid();

        Assertions.assertTrue((nextGtid & GtidGenerator.MAX_SEQUENCE) == 0, "Sequence should reset on a new millisecond");
    }

    @Test
    public void testNextGtidWhenSystemClockGoesBackwards() {
        gtidGenerator.setLastGtid(Long.MAX_VALUE); // Simulate a GTID with a far future timestamp
        Assertions.assertThrows(IllegalStateException.class, () -> {
            gtidGenerator.nextGtid();
        }, "Should throw an IllegalStateException when the system clock goes backwards");
    }

    @Test
    public void testNextGtidWhenSystemClockGoesBackwardsLittle() {
        long gtid = gtidGenerator.getGtid(System.currentTimeMillis() + 5000);
        gtidGenerator.setLastGtid(gtid); // Simulate a GTID with a far future timestamp
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

        Assertions.assertTrue(secondTimestamp >= firstTimestamp, "Timestamp should advance or stay the same on subsequent GTIDs");
    }
}
