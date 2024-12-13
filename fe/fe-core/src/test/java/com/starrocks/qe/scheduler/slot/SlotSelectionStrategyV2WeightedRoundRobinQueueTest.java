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

import com.google.common.collect.ImmutableList;
import com.starrocks.common.util.UUIDUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.starrocks.qe.scheduler.slot.SlotSelectionStrategyV2.SlotContext;
import static com.starrocks.qe.scheduler.slot.SlotSelectionStrategyV2.WeightedRoundRobinQueue;
import static org.assertj.core.api.Assertions.assertThat;

public class SlotSelectionStrategyV2WeightedRoundRobinQueueTest {
    @Test
    public void testInit() {
        class TestCase {
            final int numSlotsPerWorker;
            final int expectedQueueIndex;

            public TestCase(int numSlotsPerWorker, int expectedQueueIndex) {
                this.numSlotsPerWorker = numSlotsPerWorker;
                this.expectedQueueIndex = expectedQueueIndex;
            }

            @Override
            public String toString() {
                return "TestCase{" +
                        "numSlotsPerWorker=" + numSlotsPerWorker +
                        ", expectedQueueIndex=" + expectedQueueIndex +
                        '}';
            }
        }

        final int numWorkers = 3;

        {
            // 7: [64, inf)
            // 6: [32, 64)
            // 5: [16, 8)
            // 4: [8, 4)
            // 3: [4, 1)
            // 2: [2, 1)
            // 1: [1, 1]
            // 0: [0, 0]
            WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(64 * numWorkers, numWorkers);
            List<TestCase> cases = ImmutableList.of(
                    new TestCase(65, 7),
                    new TestCase(64, 7),

                    new TestCase(63, 6),
                    new TestCase(33, 6),
                    new TestCase(32, 6),

                    new TestCase(31, 5),
                    new TestCase(17, 5),
                    new TestCase(16, 5),

                    new TestCase(15, 4),
                    new TestCase(9, 4),
                    new TestCase(8, 4),

                    new TestCase(7, 3),
                    new TestCase(5, 3),
                    new TestCase(4, 3),

                    new TestCase(3, 2),
                    new TestCase(2, 2),

                    new TestCase(1, 1),
                    new TestCase(0, 0)
            );
            for (TestCase tc : cases) {
                validateQueueIndexOfSlot(queue, tc.numSlotsPerWorker * numWorkers, tc.expectedQueueIndex, tc.toString());
            }
        }

        {
            // 7: [1024, inf)
            // 6: [512, 1024)
            // 5: [256, 512)
            // 4: [128, 256)
            // 3: [64, 128)
            // 2: [32, 64)
            // 1: [16, 32]
            // 0: [0, 16)
            WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024 * numWorkers, numWorkers);
            List<TestCase> cases = ImmutableList.of(
                    new TestCase(1025, 7),
                    new TestCase(1024, 7),

                    new TestCase(1023, 6),
                    new TestCase(513, 6),
                    new TestCase(512, 6),

                    new TestCase(511, 5),
                    new TestCase(257, 5),
                    new TestCase(256, 5),

                    new TestCase(255, 4),
                    new TestCase(129, 4),
                    new TestCase(128, 4),

                    new TestCase(127, 3),
                    new TestCase(65, 3),
                    new TestCase(64, 3),

                    new TestCase(63, 2),
                    new TestCase(33, 2),
                    new TestCase(32, 2),

                    new TestCase(31, 1),
                    new TestCase(17, 1),
                    new TestCase(16, 1),

                    new TestCase(15, 0),
                    new TestCase(8, 0),
                    new TestCase(1, 0),
                    new TestCase(0, 0)
            );
            for (TestCase tc : cases) {
                validateQueueIndexOfSlot(queue, tc.numSlotsPerWorker * numWorkers, tc.expectedQueueIndex, tc.toString());
            }
        }
    }

    private static SlotContext generateSlotContext(int numSlots) {
        LogicalSlot slot = new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0, 0, 0);
        return new SlotContext(slot);
    }

    private static void validateQueueIndexOfSlot(WeightedRoundRobinQueue queue, int numSlots, int expectedQueueIndex,
                                                 String failMsg) {
        SlotContext context = generateSlotContext(numSlots);
        queue.add(context);
        assertThat(context.getSubQueueIndex()).describedAs(failMsg).isEqualTo(expectedQueueIndex);

        SlotContext curContext = queue.peak();
        assertThat(curContext).describedAs(failMsg).isSameAs(context);

        curContext = queue.poll();
        assertThat(curContext).describedAs(failMsg).isSameAs(context);

        assertThat(queue.isEmpty()).describedAs(failMsg).isTrue();
    }

    @Test
    public void testAddAndPeakByPriority() {
        WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024, 1);

        int numSlots = 1024;
        for (int i = 7; i >= 0; i--) {
            for (int j = 0; j < 16384; j++) {
                queue.add(generateSlotContext(numSlots));
            }
            numSlots = numSlots >>> 1;
        }

        final int[] expectedPeakTimes = new int[] {0, 0, 0, 0, 0, 0, 0, 0};
        int curWeight = 1;
        for (int i = 7; i >= 0; i--) {
            expectedPeakTimes[i] = curWeight;
            curWeight = (int) (curWeight * 2.5);
        }

        final int totalPeakTimes = Arrays.stream(expectedPeakTimes).sum();
        int[] peakTimes = new int[] {0, 0, 0, 0, 0, 0, 0, 0};
        for (int i = 0; i < totalPeakTimes; i++) {
            SlotContext context = queue.peak();
            peakTimes[context.getSubQueueIndex()]++;

            // Return the same one when peaking multiple times.
            for (int j = 0; j < 3; j++) {
                SlotContext curContext = queue.peak();
                assertThat(curContext).isSameAs(context);
            }

            SlotContext curContext = queue.poll();
            assertThat(curContext).isSameAs(context);
        }

        assertThat(peakTimes).containsExactly(expectedPeakTimes);
    }

    @Test
    public void testRemovePeaked() {
        WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024, 1);

        SlotContext context1 = generateSlotContext(1024);
        SlotContext context2 = generateSlotContext(1);
        queue.add(context1);
        queue.add(context2);
        assertThat(queue.size()).isEqualTo(2);

        SlotContext peak = queue.peak();
        assertThat(peak).isSameAs(context2);

        queue.remove(context2);
        peak = queue.peak();
        assertThat(peak).isSameAs(context1);
        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.size()).isOne();
    }

    @Test
    public void testRemoveToPeak() {
        WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024, 1);

        SlotContext context1 = generateSlotContext(1024);
        SlotContext context2 = generateSlotContext(1);
        queue.add(context1);
        queue.add(context2);
        assertThat(queue.size()).isEqualTo(2);

        queue.remove(context2);
        SlotContext peak = queue.peak();
        assertThat(peak).isSameAs(context1);
        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.size()).isOne();
    }

    @Test
    public void testRemoveNonExist() {
        WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024, 1);

        SlotContext context1 = generateSlotContext(1024);
        SlotContext context2 = generateSlotContext(1);
        queue.add(context1);
        queue.add(context2);
        assertThat(queue.size()).isEqualTo(2);

        SlotContext context3 = generateSlotContext(256);
        queue.remove(context3);
        assertThat(queue.size()).isEqualTo(2);

        SlotContext peak = queue.peak();
        assertThat(peak).isSameAs(context2);
    }

    @Test
    public void testPoll() {
        WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024, 1);

        final int numSlots = 100;
        Random rand = new Random();

        // Add `numSlots` slots.
        for (int i = 0; i < numSlots; i++) {
            queue.add(generateSlotContext(rand.nextInt()));
        }
        assertThat(queue.size()).isEqualTo(numSlots);

        // Poll all slots.
        for (int i = 0; i < numSlots; i++) {
            SlotContext context = queue.poll();
            assertThat(context).isNotNull();
        }
        assertThat(queue.size()).isZero();
        assertThat(queue.isEmpty()).isTrue();

        // Poll an empty queue.
        SlotContext context = queue.poll();
        assertThat(context).isNull();
    }

    @Test
    public void testEmptyQueueComeback() {
        WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024, 1);

        SlotContext slot1 = generateSlotContext(1024);
        SlotContext slot2 = generateSlotContext(512);
        SlotContext slot3 = generateSlotContext(256);
        SlotContext slot4 = generateSlotContext(1);

        queue.add(slot1);
        assertThat(queue.peak()).isSameAs(slot1);

        // The queue of slot2 comebacks and slot2 > slot1 (peak)
        queue.add(slot2);
        assertThat(queue.peak()).isSameAs(slot2);

        // The queue of slot4 comebacks and slot4 > slot2 (peak)
        queue.add(slot4);
        assertThat(queue.peak()).isSameAs(slot4);

        // The queue of slot3 comebacks but slot3 < slot4 (peak)
        queue.add(slot3);
        assertThat(queue.peak()).isSameAs(slot4);

        // Poll all the slots.
        assertThat(queue.poll()).isSameAs(slot4);
        assertThat(queue.poll()).isSameAs(slot3);
        assertThat(queue.poll()).isSameAs(slot2);
        assertThat(queue.poll()).isSameAs(slot1);
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    public void testSkipEmptyQueue() {
        SlotContext slot1 = generateSlotContext(1024);
        SlotContext slot2 = generateSlotContext(512);
        SlotContext slot3 = generateSlotContext(256);
        SlotContext slot4 = generateSlotContext(1);

        WeightedRoundRobinQueue queue = new WeightedRoundRobinQueue(1024, 1);

        queue.add(slot1);
        assertThat(queue.peak()).isSameAs(slot1);

        assertThat(queue.getSubQueues()[7].getSkipTimes()).isZero();
        int skipTimes = 1;
        for (int i = 6; i >= 0; i--) {
            assertThat(queue.getSubQueues()[i].getSkipTimes())
                    .describedAs("subQueue#" + i)
                    .isBetween(skipTimes - 1L, skipTimes + 1L);
            skipTimes = (int) ((double) skipTimes * 2.5);
        }
    }

}
