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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-FE singleton that tracks queries currently in QueryQueueManager.maybeWait().
 * Holds raw (un-clamped) slot estimates so per-warehouse metrics can aggregate them
 * for K8s autoscaling consumption. Entries are added at pending-enter and removed
 * at pending-exit (success / timeout / cancel).
 *
 * <p>Thread safety: ConcurrentHashMap; aggregate readers iterate values() with weak
 * consistency, which is acceptable for periodic metric scrapes.
 */
public class WarehouseInFlightTracker {

    public static final class InFlightEntry {
        public final TUniqueId slotId;
        public final int rawSlots;
        public final int clampedSlots;
        public final int totalSlotsAtEnter;
        public final boolean isBigQuery;
        public final long enterPendingMs;

        InFlightEntry(TUniqueId slotId, int rawSlots, int clampedSlots,
                      int totalSlotsAtEnter, boolean isBigQuery, long enterPendingMs) {
            this.slotId = slotId;
            this.rawSlots = rawSlots;
            this.clampedSlots = clampedSlots;
            this.totalSlotsAtEnter = totalSlotsAtEnter;
            this.isBigQuery = isBigQuery;
            this.enterPendingMs = enterPendingMs;
        }
    }

    private static class Holder {
        private static final WarehouseInFlightTracker INSTANCE = new WarehouseInFlightTracker();
    }

    public static WarehouseInFlightTracker getInstance() {
        return Holder.INSTANCE;
    }

    private final ConcurrentMap<Long, ConcurrentMap<TUniqueId, InFlightEntry>> byWarehouse =
            new ConcurrentHashMap<>();

    public void onEnterPending(long warehouseId, TUniqueId slotId,
                               int rawSlots, int clampedSlots,
                               int totalSlotsAtEnter, boolean isBigQuery) {
        byWarehouse
                .computeIfAbsent(warehouseId, k -> new ConcurrentHashMap<>())
                .put(slotId, new InFlightEntry(slotId, rawSlots, clampedSlots,
                        totalSlotsAtEnter, isBigQuery, System.currentTimeMillis()));
    }

    public void onExitPending(long warehouseId, TUniqueId slotId) {
        ConcurrentMap<TUniqueId, InFlightEntry> map = byWarehouse.get(warehouseId);
        if (map != null) {
            map.remove(slotId);
        }
    }

    public int getMaxRawSlots(long warehouseId) {
        ConcurrentMap<TUniqueId, InFlightEntry> map = byWarehouse.get(warehouseId);
        if (map == null) {
            return 0;
        }
        return map.values().stream().mapToInt(e -> e.rawSlots).max().orElse(0);
    }

    public long getSumRawSlots(long warehouseId) {
        ConcurrentMap<TUniqueId, InFlightEntry> map = byWarehouse.get(warehouseId);
        if (map == null) {
            return 0L;
        }
        return map.values().stream().mapToLong(e -> e.rawSlots).sum();
    }

    public int getBigQueryCount(long warehouseId) {
        ConcurrentMap<TUniqueId, InFlightEntry> map = byWarehouse.get(warehouseId);
        if (map == null) {
            return 0;
        }
        return (int) map.values().stream().filter(e -> e.isBigQuery).count();
    }

    public Set<Long> getTrackedWarehouseIds() {
        return byWarehouse.keySet();
    }

    public InFlightEntry getEntry(long warehouseId, TUniqueId slotId) {
        ConcurrentMap<TUniqueId, InFlightEntry> map = byWarehouse.get(warehouseId);
        return map == null ? null : map.get(slotId);
    }
}
