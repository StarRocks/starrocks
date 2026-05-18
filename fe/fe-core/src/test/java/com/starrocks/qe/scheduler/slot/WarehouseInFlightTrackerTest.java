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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WarehouseInFlightTrackerTest {
    private static final long WH = 100L;

    @Test
    public void testRegisterAndMaxRaw() {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        TUniqueId q1 = new TUniqueId(1, 1);
        TUniqueId q2 = new TUniqueId(2, 2);
        tracker.onEnterPending(WH, q1, /*raw=*/ 20, /*clamped=*/ 8, /*totalSlots=*/ 8, /*isBig=*/ true);
        tracker.onEnterPending(WH, q2, /*raw=*/ 4, /*clamped=*/ 4, /*totalSlots=*/ 8, /*isBig=*/ false);
        assertEquals(20, tracker.getMaxRawSlots(WH));
        assertEquals(24, tracker.getSumRawSlots(WH));
        assertEquals(1, tracker.getBigQueryCount(WH));
        tracker.onExitPending(WH, q1);
        assertEquals(4, tracker.getMaxRawSlots(WH));
        assertEquals(0, tracker.getBigQueryCount(WH));
    }

    @Test
    public void testIsolationAcrossWarehouses() {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        tracker.onEnterPending(WH, new TUniqueId(1, 1), 10, 8, 8, true);
        tracker.onEnterPending(WH + 1, new TUniqueId(2, 2), 50, 8, 8, true);
        assertEquals(10, tracker.getMaxRawSlots(WH));
        assertEquals(50, tracker.getMaxRawSlots(WH + 1));
    }

    @Test
    public void testEmptyWarehouseReturnsZero() {
        WarehouseInFlightTracker tracker = new WarehouseInFlightTracker();
        assertEquals(0, tracker.getMaxRawSlots(WH));
        assertEquals(0, tracker.getSumRawSlots(WH));
        assertEquals(0, tracker.getBigQueryCount(WH));
    }
}
