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

package com.starrocks.qe.scheduler.slot;

import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WarehouseInFlightTrackerTest {
    private static final long WH = 100L;

    @Test
    public void testRegisterAndSumRaw() {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        TUniqueId q1 = new TUniqueId(1, 1);
        TUniqueId q2 = new TUniqueId(2, 2);
        tracker.onEnterPending(WH, q1, /*raw=*/ 20, /*clamped=*/ 8, /*totalSlots=*/ 8);
        tracker.onEnterPending(WH, q2, /*raw=*/ 4, /*clamped=*/ 4, /*totalSlots=*/ 8);
        assertEquals(24L, tracker.getSumRawSlots(WH));
        assertNotNull(tracker.getEntry(WH, q1));
        tracker.onExitPending(WH, q1);
        assertEquals(4L, tracker.getSumRawSlots(WH));
        assertNull(tracker.getEntry(WH, q1));
    }

    @Test
    public void testIsolationAcrossWarehouses() {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        tracker.onEnterPending(WH, new TUniqueId(1, 1), 10, 8, 8);
        tracker.onEnterPending(WH + 1, new TUniqueId(2, 2), 50, 8, 8);
        assertEquals(10L, tracker.getSumRawSlots(WH));
        assertEquals(50L, tracker.getSumRawSlots(WH + 1));
    }

    @Test
    public void testEmptyWarehouseReturnsZero() {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        assertEquals(0L, tracker.getSumRawSlots(WH));
    }

    @Test
    public void testAwaitCapacityReturnsImmediatelyWhenAlreadyEnough() throws Exception {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        long start = System.currentTimeMillis();
        boolean result = tracker.awaitCapacity(
                /*rawSlots=*/ 8,
                /*supplyTotalSlots=*/ () -> 16,
                /*thresholdRatio=*/ 1.0,
                /*maxWaitMs=*/ 1000L);
        assertTrue(result);
        assertTrue(System.currentTimeMillis() - start < 200, "should return without waiting");
    }

    @Test
    public void testAwaitCapacityTimesOut() throws Exception {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        long start = System.currentTimeMillis();
        boolean result = tracker.awaitCapacity(
                /*rawSlots=*/ 100,
                /*supplyTotalSlots=*/ () -> 8,
                /*thresholdRatio=*/ 1.0,
                /*maxWaitMs=*/ 500L);
        assertFalse(result);
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed >= 500 && elapsed < 2000,
                "should wait full timeout (was " + elapsed + "ms)");
    }

    @Test
    public void testAwaitCapacityUnblocksWhenSupplyGrows() throws Exception {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        AtomicInteger total = new AtomicInteger(8);
        Thread grower = new Thread(() -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException ignored) {
                // ignore
            }
            total.set(64);
        });
        grower.start();
        long start = System.currentTimeMillis();
        boolean result = tracker.awaitCapacity(50, total::get, 1.0, 2000L);
        grower.join();
        assertTrue(result);
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed >= 300 && elapsed < 2000,
                "should unblock soon after supply grows (elapsed " + elapsed + "ms)");
    }
}
