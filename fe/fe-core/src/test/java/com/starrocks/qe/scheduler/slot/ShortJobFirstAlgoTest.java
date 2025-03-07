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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.starrocks.qe.scheduler.slot.ShortJobFirstAlgo.AGING_WEIGHT;

public class ShortJobFirstAlgoTest {

    private LogicalSlot createLogicalSlot(int numSlots) {
        return new LogicalSlot(new TUniqueId(1L, 1L), "fe1", 1, numSlots,
                1000L, 1, 1, 1, 1);
    }

    @Test
    public void testScore() {
        LogicalSlot logicalSlot = createLogicalSlot(16);
        SlotSelectionStrategyV2.SlotContext slot = new SlotSelectionStrategyV2.SlotContext(logicalSlot);
        slot.setCreateTime(100);

        ShortJobFirstAlgo algo = new ShortJobFirstAlgo();
        algo.updateBaseTime(100);
        Assertions.assertEquals(16.0, algo.calculateScore(slot));

        algo.updateBaseTime(1000);
        Assertions.assertEquals(13.0, algo.calculateScore(slot));

        algo.updateBaseTime(10000);
        Assertions.assertEquals(10.0, algo.calculateScore(slot));
    }

    @Test
    public void testAgingScores() {
        long startTime = 10_000_000L;
        TestShortJobFirstAlgo algo = new TestShortJobFirstAlgo(startTime);

        // create slots with different waiting time
        SlotSelectionStrategyV2.SlotContext[] slots = new SlotSelectionStrategyV2.SlotContext[6];
        long[] waitTimes = {
                100,    // 100ms -> log2(1) = 0
                200,    // 200ms -> log2(2) = 1
                500,    // 500ms -> log2(5) = 2
                1000,   // 1s -> log2(10) = 3
                5000,   // 5s -> log2(50) = 5
                10000   // 10s -> log2(100) = 6
        };

        for (int i = 0; i < slots.length; i++) {
            slots[i] = createMockSlot(4);  // all tasks have 4 slots
            Mockito.when(slots[i].getCreateTime()).thenReturn(startTime - waitTimes[i]);
        }

        algo.add(slots[3]);  // 1s
        algo.add(slots[0]);  // 100ms
        algo.add(slots[5]);  // 10s
        algo.add(slots[1]);  // 200ms
        algo.add(slots[4]);  // 5s
        algo.add(slots[2]);  // 500ms
        Assertions.assertEquals(6, algo.size());

        algo.updateBaseTime(startTime);

        double[] expectedScores = {
                4 - 0 * AGING_WEIGHT,  // 100ms
                4 - 1 * AGING_WEIGHT,  // 200ms
                4 - 2 * AGING_WEIGHT,  // 500ms
                4 - 3 * AGING_WEIGHT,  // 1s
                4 - 5 * AGING_WEIGHT,  // 5s
                4 - 6 * AGING_WEIGHT   // 10s
        };

        // Verify the scores
        for (int i = 0; i < slots.length; i++) {
            double actualScore = algo.calculateScore(slots[i]);
            Assertions.assertEquals(expectedScores[i], actualScore, 0.01,
                    "Score mismatch for wait time " + waitTimes[i] + "ms");
        }

        Assertions.assertEquals(slots[5], algo.peak(), "Should poll 10s wait task first");
        Assertions.assertEquals(slots[5], algo.poll(), "Should poll 10s wait task first");
        Assertions.assertEquals(slots[4], algo.poll(), "Should poll 5s wait task second");
        Assertions.assertEquals(slots[3], algo.poll(), "Should poll 1s wait task third");
        Assertions.assertEquals(slots[2], algo.poll(), "Should poll 500ms wait task fourth");
        Assertions.assertEquals(slots[1], algo.poll(), "Should poll 200ms wait task fifth");
        Assertions.assertEquals(slots[0], algo.poll(), "Should poll 100ms wait task last");
    }

    private static class TestShortJobFirstAlgo extends ShortJobFirstAlgo {
        private final long mockCurrentTime;

        public TestShortJobFirstAlgo(long initialTime) {
            super();
            this.mockCurrentTime = initialTime;
        }

        @Override
        public long currentTime() {
            return mockCurrentTime;
        }
    }

    private SlotSelectionStrategyV2.SlotContext createMockSlot(int numPhysicalSlots) {
        SlotSelectionStrategyV2.SlotContext context = Mockito.mock(SlotSelectionStrategyV2.SlotContext.class);
        LogicalSlot slot = Mockito.mock(LogicalSlot.class);
        Mockito.when(slot.getNumPhysicalSlots()).thenReturn(numPhysicalSlots);
        Mockito.when(context.getSlot()).thenReturn(slot);
        return context;
    }
}