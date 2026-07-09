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

package com.starrocks.context.allocator;

import com.starrocks.context.ContextReadExecutor;
import com.starrocks.server.GlobalStateMgr;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-{@code entity_id} monotonic version counter.
 *
 * <p>The {@code (entity_id, version)} primary key in
 * {@link com.starrocks.context.ContextInternalTables#VERSIONS} is the authoritative record of the
 * highest version assigned to each entity. This allocator is just an in-memory cache that avoids
 * issuing {@code SELECT MAX(version)} on every write. On a miss (entity not in the map yet, e.g.
 * after a leader failover or for an entity created since the last bulk seed), the allocator falls
 * back to a single point-lookup against storage so the cache self-heals without depending on the
 * startup-time {@code seedVersionAllocator()} sweep being timely or complete.
 *
 * <p>Returns {@code 1} for the first version of an entity and {@code n+1} for each subsequent write.
 */
public final class ContextVersionAllocator {

    private final ConcurrentMap<Long, AtomicLong> counters = new ConcurrentHashMap<>();

    public long next(long entityId) {
        AtomicLong counter = counters.get(entityId);
        if (counter == null) {
            // Cache miss: consult storage. The (entity_id, version) PK is the source of truth.
            long observed = lookupMaxVersion(entityId);
            counter = counters.computeIfAbsent(entityId, k -> new AtomicLong(observed));
        }
        return counter.incrementAndGet();
    }

    /**
     * Seed the counter for an entity to at least {@code observedVersion}. Used both by the bulk
     * startup sweep ({@code ContextMetaManager.seedVersionAllocator()}) and by read-path callers
     * that want to opportunistically warm the cache from values they already loaded — turns the
     * first-write lookup into a no-op for those entities.
     */
    public void seed(long entityId, long observedVersion) {
        counters.computeIfAbsent(entityId, k -> new AtomicLong(0L))
                .accumulateAndGet(observedVersion, Math::max);
    }

    public long peek(long entityId) {
        AtomicLong counter = counters.get(entityId);
        return counter == null ? 0L : counter.get();
    }

    /**
     * Look up the persisted {@code MAX(version)} for an entity. Returns 0 when the read path is
     * unavailable so a fresh FE before {@code TableKeeper} provisions the internal tables can
     * still allocate version 1 for brand-new entities. The hot path never reaches this method
     * once the entity's counter is in the map.
     */
    private static long lookupMaxVersion(long entityId) {
        try {
            ContextReadExecutor reader = GlobalStateMgr.getCurrentState().getContextReadExecutor();
            if (reader == null) {
                return 0L;
            }
            return reader.maxVersionOf(entityId);
        } catch (Exception e) {
            return 0L;
        }
    }
}
