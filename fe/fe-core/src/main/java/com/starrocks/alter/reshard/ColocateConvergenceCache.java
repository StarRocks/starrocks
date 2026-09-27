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

package com.starrocks.alter.reshard;

import com.starrocks.common.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Negative cache of PACK shard groups StarOS recently reported NOT placement-converged. It exists only
 * to throttle {@link ColocateChecker}'s per-tick {@code queryShardGroupStable} load while a
 * range-colocate group is still migrating.
 *
 * <h3>Negative-only</h3>
 * Only not-converged observations are stored — never converged. The stable flip is terminal (the
 * checker never revisits a group once stable) and StarOS's stability predicate is best-effort /
 * eventually-consistent, so caching a "converged" could flip a group stable on stale data; a cached
 * "not converged" only delays the flip by up to the TTL (a missed-optimization delay, never a wrong
 * flip). An entry is dropped as soon as its group is observed converged, and {@link #clear()} empties
 * the cache when nothing is migrating, so the map stays bounded to the groups actively migrating during
 * a reshard. Entries are timestamped, so any that linger across a same-process leader
 * demotion/re-promotion are treated as expired (re-query) — never a premature flip.
 *
 * <h3>Lifecycle</h3>
 * The TTL is the runtime-mutable {@link Config#tablet_reshard_colocate_checker_convergence_cache_ttl_ms}
 * (values {@code <= 0} disable the cache entirely: no reads, no writes, no growth). Per-leader,
 * in-memory, non-journaled, and accessed only from the single reshard scheduler thread
 * (TabletReshardJobMgr is a FrontendDaemon), so it needs no synchronization.
 */
class ColocateConvergenceCache {
    // packShardGroupId -> wall-clock ms of the last StarOS "not converged" observation.
    private final Map<Long, Long> notConvergedSinceMs = new HashMap<>();

    /**
     * @return {@code true} iff some group in {@code packGroupIds} was reported not-converged within the
     *         TTL window — the whole colocate group cannot be converged yet, so the StarOS query can be
     *         skipped this tick. Always {@code false} when the cache is disabled (TTL {@code <= 0}).
     */
    boolean shouldSkipQuery(List<Long> packGroupIds) {
        long ttlMs = Config.tablet_reshard_colocate_checker_convergence_cache_ttl_ms;
        if (ttlMs <= 0) {
            return false;
        }
        long now = currentTimeMillis();
        for (long packGroupId : packGroupIds) {
            Long since = notConvergedSinceMs.get(packGroupId);
            // A stale/backward wall clock (elapsed < 0) is treated as expired (re-query) — fail-safe.
            if (since != null && now - since >= 0 && now - since < ttlMs) {
                return true;
            }
        }
        return false;
    }

    /**
     * Records one PACK group's freshly-queried stability: caches a not-converged result so it is not
     * re-queried within the TTL, or drops the entry once the group is converged (converged is never
     * cached — the flip must ride a fresh read). No-op when the cache is disabled.
     */
    void record(long packShardGroupId, boolean converged) {
        if (Config.tablet_reshard_colocate_checker_convergence_cache_ttl_ms <= 0) {
            return;
        }
        if (converged) {
            notConvergedSinceMs.remove(packShardGroupId);
        } else {
            notConvergedSinceMs.put(packShardGroupId, currentTimeMillis());
        }
    }

    /** Drops every entry; a no-op if already empty. Called when no group is migrating. */
    void clear() {
        if (!notConvergedSinceMs.isEmpty()) {
            notConvergedSinceMs.clear();
        }
    }

    // package-private seam so tests can control the time source via JMockit MockUp.
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
