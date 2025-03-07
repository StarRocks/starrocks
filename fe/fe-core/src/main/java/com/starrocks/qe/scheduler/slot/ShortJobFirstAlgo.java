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
import com.starrocks.sql.optimizer.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * Short Job First + Aging
 */
class ShortJobFirstAlgo implements SlotScheduleAlgorithm {

    private static final Logger LOG = LogManager.getLogger(ShortJobFirstAlgo.class);
    public static final double AGING_WEIGHT = 1; // Weight factor for aging effect
    private static final long AGING_UPDATE_INTERVAL_MS = 1000; // Update aging score every second
    private static final long AGING_GRANULARITY_MS = 100; // Aging calculation granularity: 100ms

    private PriorityQueue<SlotSelectionStrategyV2.SlotContext> q;
    private long lastUpdateTime;
    private long baseTime;  // Base time for aging calculation

    private static final Comparator<SlotSelectionStrategyV2.SlotContext> COMPARATOR = (lhs, rhs) -> {
        double lhsScore = lhs.getSlot().getNumPhysicalSlots();
        double rhsScore = rhs.getSlot().getNumPhysicalSlots();
        return Double.compare(lhsScore, rhsScore);
    };

    public ShortJobFirstAlgo() {
        baseTime = currentTime();
        lastUpdateTime = baseTime;
        q = new PriorityQueue<>(COMPARATOR);
    }

    @Override
    public int size() {
        return q.size();
    }

    @Override
    public void add(SlotSelectionStrategyV2.SlotContext slotContext) {
        q.add(slotContext);
        updateAgingIfNeeded();
    }

    @Override
    public boolean remove(SlotSelectionStrategyV2.SlotContext slotContext) {
        return q.remove(slotContext);
    }

    @Override
    public SlotSelectionStrategyV2.SlotContext poll() {
        updateAgingIfNeeded();
        return q.poll();
    }

    @Override
    public SlotSelectionStrategyV2.SlotContext peak() {
        updateAgingIfNeeded();
        return q.peek();
    }

    private static long roundToGranularity(long timeMs) {
        return (timeMs / AGING_GRANULARITY_MS) * AGING_GRANULARITY_MS;
    }

    protected double calculateScore(SlotSelectionStrategyV2.SlotContext slot) {
        // Aging calculation examples:
        // 100ms -> 0, 200ms -> 1, 400ms -> 2...
        // 10s -> 6, 30s -> 8, 60s -> 9
        // The longer the wait time, the higher the aging score
        long age = (baseTime - roundToGranularity(slot.getCreateTime())) / AGING_GRANULARITY_MS;
        age = Utils.log2(Math.max(1, age));
        return slot.getSlot().getNumPhysicalSlots() - age * AGING_WEIGHT;
    }

    @VisibleForTesting
    protected void updateBaseTime(long millis) {
        this.baseTime = millis;
    }

    public long currentTime() {
        return System.currentTimeMillis();
    }

    private void updateAgingIfNeeded() {
        long currentTime = roundToGranularity(currentTime());
        if (currentTime - lastUpdateTime >= AGING_UPDATE_INTERVAL_MS) {
            baseTime = currentTime;
            // Rebuild queue with updated aging scores
            PriorityQueue<SlotSelectionStrategyV2.SlotContext> newQueue = new PriorityQueue<>(
                    (lhs, rhs) -> Double.compare(calculateScore(lhs), calculateScore(rhs)));
            newQueue.addAll(q);
            q = newQueue;
            lastUpdateTime = currentTime;

            if (LOG.isDebugEnabled()) {
                String str =
                        q.stream().map(x -> String.format("[slot %d score %.1f]",
                                        x.getSlot().getNumPhysicalSlots(),
                                        calculateScore(x)))
                                .collect(Collectors.joining(", "));
                LOG.debug("ShortJobFirstQueue: {}", str);
            }
        }
    }
}
