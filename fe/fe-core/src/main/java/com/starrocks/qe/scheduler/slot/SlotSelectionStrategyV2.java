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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.compress.utils.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The slot resource is divided into small extra slots and normal slots.
 *
 * <p> Small slots are tried to allocate slots from the small extra slots. If it fails, will try to allocate them from the normal
 * slots. The small slot is defined as the query that requires only one slot.
 *
 * <p> Selecting slots to allocate from the normal slots is based on the smooth weighted round-robin algorithm.
 * The slots are divided into multiple sub-queues according to the number of slots required by the query.
 * Each sub-queue has a different weight. See {@link WeightedRoundRobinQueue} for details.
 */
public class SlotSelectionStrategyV2 implements SlotSelectionStrategy {

    private static final Logger LOG = LogManager.getLogger(SlotSelectionStrategyV2.class);

    private static final long UPDATE_OPTIONS_INTERVAL_MS = 1000;

    /**
     * These three members are initialized in the first call of {@link #updateOptionsPeriodically};
     */
    private long lastUpdateOptionsTime = 0;
    private QueryQueueOptions opts = null;
    private SlotScheduleAlgorithm requiringQueue = null;

    private final Map<TUniqueId, SlotContext> slotContexts = Maps.newHashMap();

    private final LinkedHashMap<TUniqueId, SlotContext> requiringSmallSlots = new LinkedHashMap<>();
    private int numAllocatedSmallSlots = 0;

    @Override
    public void onRequireSlot(LogicalSlot slot) {
        updateOptionsPeriodically();

        SlotContext slotContext = new SlotContext(slot);

        slotContexts.put(slot.getSlotId(), slotContext);
        if (isSmallSlot(slot)) {
            requiringSmallSlots.put(slot.getSlotId(), slotContext);
        }
        requiringQueue.add(slotContext);

        MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                .increase((long) slot.getNumPhysicalSlots());
    }

    @Override
    public void onAllocateSlot(LogicalSlot slot) {
        updateOptionsPeriodically();

        SlotContext slotContext = slotContexts.get(slot.getSlotId());
        if (isSmallSlot(slot)) {
            requiringSmallSlots.remove(slot.getSlotId());
            if (slotContext.isAllocatedAsSmallSlot()) {
                numAllocatedSmallSlots += slot.getNumPhysicalSlots();
            }
        }
        requiringQueue.remove(slotContext);

        String category = String.valueOf(slotContext.getSubQueueIndex());
        MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(category).increase((long) -slot.getNumPhysicalSlots());
        MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_RUNNING.getMetric(category).increase((long) slot.getNumPhysicalSlots());
        MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_ALLOCATED_TOTAL.getMetric(category)
                .increase((long) slot.getNumPhysicalSlots());
    }

    @Override
    public void onReleaseSlot(LogicalSlot slot) {
        updateOptionsPeriodically();

        SlotContext slotContext = slotContexts.remove(slot.getSlotId());
        if (isSmallSlot(slot)) {
            requiringSmallSlots.remove(slot.getSlotId());
            if (slotContext.isAllocatedAsSmallSlot()) {
                numAllocatedSmallSlots -= slot.getNumPhysicalSlots();
            }
        }
        if (requiringQueue.remove(slotContext)) {
            MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                    .increase((long) -slot.getNumPhysicalSlots());
        } else {
            MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_RUNNING.getMetric(String.valueOf(slotContext.getSubQueueIndex()))
                    .increase((long) -slot.getNumPhysicalSlots());
        }
    }

    @Override
    public List<LogicalSlot> peakSlotsToAllocate(SlotTracker slotTracker) {
        updateOptionsPeriodically();

        List<LogicalSlot> slotsToAllocate = Lists.newArrayList();

        int curNumAllocatedSmallSlots = numAllocatedSmallSlots;
        for (SlotContext slotContext : requiringSmallSlots.values()) {
            LogicalSlot slot = slotContext.getSlot();
            if (curNumAllocatedSmallSlots + slot.getNumPhysicalSlots() > opts.v2().getTotalSmallSlots()) {
                break;
            }

            requiringQueue.remove(slotContext);

            slotsToAllocate.add(slot);
            slotContext.setAllocateAsSmallSlot();
            curNumAllocatedSmallSlots += slot.getNumPhysicalSlots();
        }

        int numAllocatedSlots = slotTracker.getNumAllocatedSlots() - numAllocatedSmallSlots;
        while (!requiringQueue.isEmpty()) {
            SlotContext slotContext = requiringQueue.peak();
            if (!isGlobalSlotAvailable(numAllocatedSlots, slotContext.getSlot())) {
                break;
            }

            requiringQueue.poll();

            slotsToAllocate.add(slotContext.getSlot());
            numAllocatedSlots += slotContext.getSlot().getNumPhysicalSlots();
        }

        return slotsToAllocate;
    }

    @VisibleForTesting
    protected QueryQueueOptions getCurrentOptions() {
        return opts;
    }

    private void updateOptionsPeriodically() {
        long now = System.currentTimeMillis();
        if (now - lastUpdateOptionsTime < UPDATE_OPTIONS_INTERVAL_MS) {
            return;
        }

        lastUpdateOptionsTime = now;
        QueryQueueOptions newOpts = QueryQueueOptions.createFromEnv();
        if (!newOpts.equals(opts)) {
            opts = newOpts;

            // Change the schedule algorithm, move pending slots to new queue
            SlotScheduleAlgorithm newQueue = null;
            if (opts.getPolicy().equals(QueryQueueOptions.SchedulePolicy.SWRR)) {
                newQueue = new WeightedRoundRobinQueue(opts.v2().getTotalSlots(), opts.v2().getNumWorkers());
            } else if (opts.getPolicy().equals(QueryQueueOptions.SchedulePolicy.SJF)) {
                newQueue = new ShortJobFirstAlgo();
            } else {
                Preconditions.checkState(false, "unknown schedule policy: " + opts.getPolicy());
            }
            if (requiringQueue != null) {
                while (!requiringQueue.isEmpty()) {
                    newQueue.add(requiringQueue.poll());
                }
            }
            requiringQueue = newQueue;

            slotContexts.values().stream()
                    .filter(slotContext -> slotContext.getSlot().getState() == LogicalSlot.State.REQUIRING)
                    .forEach(slotContext -> requiringQueue.add(slotContext));

            LOG.info("updated SlotSelectionStrategy to {}", newOpts.toString());

        }
    }

    private boolean isGlobalSlotAvailable(int numAllocatedSlots, LogicalSlot slot) {
        final int numTotalSlots = opts.v2().getTotalSlots();
        return numAllocatedSlots == 0 || numAllocatedSlots + slot.getNumPhysicalSlots() <= numTotalSlots;
    }

    private static boolean isSmallSlot(LogicalSlot slot) {
        return slot.getNumPhysicalSlots() <= 1;
    }

    /**
     * The implementation of the smooth weighted round-robin algorithm.
     *
     * <p> If the peaking sub-queue is empty, will update {@link SlotSubQueue#state} as usual and record the skip
     * times of this sub-queue. Repeat this process until the peaking sub-queue is not empty.
     * When this sub-queue is not empty anymore, will give it some compensations according to the skip times.
     *
     * <p> The rules to divide sub-queues are as follows:
     * <ul>
     * <li> The minimum slots of {@code subQueues[i]} is {@code 2^(lastSubQueueLog2Bound-(NUM_SUB_QUEUES-1-i) )},
     * that is, the
     * minimum slots of 0~7 sub-queues are 2^(b-7), 2^(b-6), 2^(b-5), 2^(b-4), 2^(b-3), 2^(b-2), 2^(b-1), 2^b,
     * respectively.
     * where {@code b} denotes {@code lastSubQueueLog2Bound}.
     * <li> The weight of {@code subQueues[i]} is {@code 2.5^(NUM_SUB_QUEUES-1-i)}, that is, the weights of 0~7
     * sub-queues
     * are 2.5^7, 2.5^6, 2.5^5, 2.5^4, 2.5^3, 2.5^2, 2.5^1, 2.5^0 respectively.
     * </ul>
     */
    @VisibleForTesting
    static class WeightedRoundRobinQueue implements SlotScheduleAlgorithm {
        private static final int NUM_SUB_QUEUES = 8;

        private final int lastSubQueueLog2Bound;
        private final int numWorkers;

        private final SlotSubQueue[] subQueues;
        private int size = 0;
        private final int totalWeight;

        private SlotContext nextSlotToPeak = null;

        public WeightedRoundRobinQueue(int totalSlots, int numWorkers) {
            this.numWorkers = numWorkers;

            final int numSlotsPerWorker = totalSlots / numWorkers;
            this.lastSubQueueLog2Bound = Utils.log2(numSlotsPerWorker);

            int curMinSlots = 1 << lastSubQueueLog2Bound;
            int curWeight = 1;
            int totalWeight = 0;
            this.subQueues = new SlotSubQueue[NUM_SUB_QUEUES];
            for (int i = subQueues.length - 1; i >= 0; i--) {
                subQueues[i] = new SlotSubQueue(i, curWeight);
                totalWeight += curWeight;

                String category = String.valueOf(i);
                LongCounterMetric m = MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_PENDING.getMetric(category);
                m.increase(-m.getValue());  // Reset.
                m = MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_STATE.getMetric(category);
                m.increase(-m.getValue());  // Reset.
                MetricRepo.GAUGE_QUERY_QUEUE_CATEGORY_WEIGHT.getMetric(category).setValue(curWeight);
                MetricRepo.GAUGE_QUERY_QUEUE_CATEGORY_SLOT_MIN_SLOTS.getMetric(category)
                        .setValue(curMinSlots * numWorkers);

                curWeight = (curWeight >>> 1) + (curWeight << 1); // curWeight*=2.5
                curMinSlots = curMinSlots >>> 1;
            }
            this.totalWeight = totalWeight;
        }

        public int size() {
            return size;
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void add(SlotContext slotContext) {
            int queueIndex = getSubQueueIndex(slotContext);
            SlotSubQueue subQueue = subQueues[queueIndex];
            if (subQueue.add(slotContext) != null) {
                return;
            }

            slotContext.setSubQueueIndex(queueIndex);
            size++;

            if (subQueue.size() != 1) {
                return;
            }

            // This sub-queue is empty before adding this slot, so give it some compensations.
            subQueue.comeback();

            if (nextSlotToPeak != null) {
                // If the previous peaked slot is in a lower priority sub-queue due to this higher priority sub-queue
                // is empty,
                // peak the slot in this higher priority sub-queue instead.
                SlotSubQueue prevPeakSubQueue = subQueues[nextSlotToPeak.getSubQueueIndex()];
                if (prevPeakSubQueue.state + totalWeight < subQueue.state) {
                    nextSlotToPeak = slotContext;
                    subQueue.incrState(-totalWeight);
                    prevPeakSubQueue.incrState(totalWeight);
                }
            }
        }

        public boolean remove(SlotContext slotContext) {
            int queueIndex = getSubQueueIndex(slotContext);
            TUniqueId slotId = slotContext.getSlotId();
            boolean contains = subQueues[queueIndex].remove(slotId) != null;
            if (contains) {
                size--;
                if (nextSlotToPeak != null && slotId.equals(nextSlotToPeak.getSlotId())) {
                    nextSlotToPeak = null;
                }
            }
            return contains;
        }

        public SlotContext peak() {
            if (size == 0) {
                return null;
            }

            if (nextSlotToPeak != null) {
                return nextSlotToPeak;
            }

            int queueIndex = nextSubQueueIndex();
            nextSlotToPeak = subQueues[queueIndex].peak();
            return nextSlotToPeak;
        }

        public SlotContext poll() {
            SlotContext slotContext = peak();
            if (slotContext != null) {
                remove(slotContext);
            }
            return slotContext;
        }

        @VisibleForTesting
        SlotSubQueue[] getSubQueues() {
            return subQueues;
        }

        private int nextSubQueueIndex() {
            if (isEmpty()) {
                return -1;
            }

            int maxQueueIndex;
            while (true) {
                // Find the sub-queue with the maximum state.
                maxQueueIndex = 0;
                for (int i = 0; i < subQueues.length; i++) {
                    SlotSubQueue queue = subQueues[i];
                    queue.incrState(queue.weight);
                    if (queue.state > subQueues[maxQueueIndex].state) {
                        maxQueueIndex = i;
                    }
                }
                SlotSubQueue maxSubQueue = subQueues[maxQueueIndex];
                maxSubQueue.incrState(-totalWeight);

                if (!maxSubQueue.isEmpty()) {
                    break;
                }

                // If the highest state sub-queue is empty, skip it

                maxSubQueue.skip(1);

                final int selectTimes = calculateContinuousSelectTimesForQueue(maxQueueIndex);
                if (selectTimes > 0) {
                    maxSubQueue.skip(selectTimes);
                    maxSubQueue.incrState(-(totalWeight - maxSubQueue.weight) * selectTimes);
                    for (int i = 0; i < subQueues.length; i++) {
                        SlotSubQueue queue = subQueues[i];
                        if (i != maxQueueIndex) {
                            queue.incrState(queue.weight * selectTimes);
                        }
                    }
                }
            }

            final int minState = Arrays.stream(subQueues).mapToInt(queue -> queue.state).min().getAsInt();
            if (minState > 1000) {
                for (SlotSubQueue queue : subQueues) {
                    queue.incrState(-minState);
                }
            }

            return maxQueueIndex;
        }

        /**
         * Calculate the times of selecting the queue with the given index continuously.
         *
         * @param queueIndex The index of the queue to calculate.
         * @return The times of selecting the queue with the given index continuously.
         */
        private int calculateContinuousSelectTimesForQueue(int queueIndex) {
            SlotSubQueue selectQueue = subQueues[queueIndex];
            int minSelectTimes = Integer.MAX_VALUE;
            for (int i = 0; i < subQueues.length; i++) {
                if (i == queueIndex) {
                    continue;
                }

                SlotSubQueue queue = subQueues[i];

                // The queue_s is in a higher priority than queue_i, until `state_i + weight_i >= state_s + weight_s`,
                // that is `deltaState >= deltaWeight`.
                // When peaking queue_s one time, state_s decreases `totalWeight - weight_s`, and state_i increases
                // `weight_i`.
                // Therefore, queue_i increases `weight_i + (totalWeight - weight_s)` = `totalWeight - deltaWeight`
                // each time
                // compared to queue_s.
                // Thus, the times of peaking queue_s before queue_i is `deltaWeight - deltaState / (totalWeight -
                // deltaWeight)`.
                int deltaWeight = selectQueue.weight - queue.weight;
                int deltaState = queue.state - selectQueue.state;
                int incrStatePerTime = totalWeight - deltaWeight;

                if (deltaState >= deltaWeight) {
                    return 0;
                }

                int curSkipTimes = (deltaWeight - deltaState + incrStatePerTime - 1) / incrStatePerTime;
                minSelectTimes = Math.min(minSelectTimes, curSkipTimes);
            }

            return minSelectTimes == Integer.MAX_VALUE ? 0 : minSelectTimes;
        }

        private int getSubQueueIndex(SlotContext slotContext) {
            int numSlotsPerWorker = slotContext.getSlot().getNumPhysicalSlots() / numWorkers;
            int index = (NUM_SUB_QUEUES - 1) - (lastSubQueueLog2Bound - Utils.log2(numSlotsPerWorker));
            return Math.max(0, Math.min(index, NUM_SUB_QUEUES - 1));
        }

        class SlotSubQueue {
            private final int queueIndex;

            private final LinkedHashMap<TUniqueId, SlotContext> slots = Maps.newLinkedHashMap();
            private final int weight;
            private int state;
            private long skipTimes;

            public SlotSubQueue(int queueIndex, int weight) {
                this.queueIndex = queueIndex;
                this.weight = weight;
                this.state = 0;
                this.skipTimes = 0;
            }

            public void skip(int incr) {
                skipTimes += incr;
            }

            public void comeback() {
                // Give it some compensations according to the skip times.
                incrState((int) Math.min((totalWeight - weight) * skipTimes, totalWeight * 2L));
                skipTimes = 0;
            }

            public void incrState(int incr) {
                this.state += incr;
                MetricRepo.COUNTER_QUERY_QUEUE_CATEGORY_SLOT_STATE.getMetric(String.valueOf(queueIndex))
                        .increase((long) incr);
            }

            public boolean isEmpty() {
                return slots.isEmpty();
            }

            public int size() {
                return slots.size();
            }

            public SlotContext add(SlotContext context) {
                return slots.put(context.getSlotId(), context);
            }

            public SlotContext remove(TUniqueId slotId) {
                return slots.remove(slotId);
            }

            public SlotContext peak() {
                return slots.values().iterator().next();
            }

            public long getSkipTimes() {
                return skipTimes;
            }
        }
    }

    @VisibleForTesting
    static class SlotContext {
        private final LogicalSlot slot;
        private boolean allocatedAsSmallSlot = false;
        private int subQueueIndex = 0;
        private long createTime;

        public SlotContext(LogicalSlot slot) {
            this.slot = slot;
            this.createTime = System.currentTimeMillis();
        }

        public boolean isAllocatedAsSmallSlot() {
            return allocatedAsSmallSlot;
        }

        public void setAllocateAsSmallSlot() {
            this.allocatedAsSmallSlot = true;
        }

        public LogicalSlot getSlot() {
            return slot;
        }

        public TUniqueId getSlotId() {
            return slot.getSlotId();
        }

        public int getSubQueueIndex() {
            return subQueueIndex;
        }

        public void setSubQueueIndex(int subQueueIndex) {
            this.subQueueIndex = subQueueIndex;
        }

        public long getCreateTime() {
            return createTime;
        }

        @VisibleForTesting
        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }
    }
}
