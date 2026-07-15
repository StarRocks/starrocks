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

package com.starrocks.lake;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages version pinning for in-flight queries in shared-data (lake) mode.
 * <p>
 * When a query plans a scan against a lake table partition, it captures a specific
 * visible version. Without pinning, autovacuum may delete the .meta file for that
 * version before the query finishes execution, causing S3 404 errors.
 * <p>
 * This manager tracks which versions are actively referenced by in-flight queries
 * and provides a minimum pinned version per partition for vacuum to respect.
 * <p>
 * Lifecycle:
 * <ul>
 *   <li>Pin: Called when query coordinator starts scheduling (after plan finalization)</li>
 *   <li>Unpin: Called when query coordinator finishes (success, failure, or cancel)</li>
 *   <li>Cleanup: Periodic stale pin removal as a safety net against leaked pins</li>
 * </ul>
 *
 * @see com.starrocks.lake.vacuum.AutovacuumDaemon
 * @see com.starrocks.qe.DefaultCoordinator
 */
public class QueryVersionPinManager {
    private static final Logger LOG = LogManager.getLogger(QueryVersionPinManager.class);

    private static final QueryVersionPinManager INSTANCE = new QueryVersionPinManager();

    // partitionId -> (version -> refCount)
    // Uses ConcurrentHashMap for thread safety on the outer map.
    // Inner TreeMap operations are synchronized on the inner map object.
    private final ConcurrentHashMap<Long, TreeMap<Long, AtomicInteger>> pinnedVersions = new ConcurrentHashMap<>();

    // queryId (as string) -> pinTime (epoch ms), for stale pin cleanup
    private final ConcurrentHashMap<String, Long> queryPinTimes = new ConcurrentHashMap<>();

    // queryId -> Map<partitionId, version>, for unpin-by-query
    private final ConcurrentHashMap<String, Map<Long, Long>> queryPartitionVersions = new ConcurrentHashMap<>();

    public static QueryVersionPinManager getInstance() {
        return INSTANCE;
    }

    @VisibleForTesting
    QueryVersionPinManager() {
    }

    /**
     * Pin a set of partition versions for a query. Vacuum will not delete .meta files
     * for pinned versions.
     *
     * @param queryId unique query identifier (used for unpin and stale cleanup)
     * @param partitionVersions map of partitionId to version that the query will read
     */
    public void pinVersions(String queryId, Map<Long, Long> partitionVersions) {
        if (partitionVersions == null || partitionVersions.isEmpty()) {
            return;
        }

        Map<Long, Long> copied = new HashMap<>(partitionVersions);
        queryPartitionVersions.put(queryId, copied);
        queryPinTimes.put(queryId, System.currentTimeMillis());

        for (Map.Entry<Long, Long> entry : copied.entrySet()) {
            long partitionId = entry.getKey();
            long version = entry.getValue();
            pinVersion(partitionId, version);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Pinned versions for query {}: {}", queryId, copied);
        }
    }

    /**
     * Unpin all partition versions previously pinned for a query.
     * Safe to call multiple times (idempotent for the same queryId).
     *
     * @param queryId the query whose versions should be unpinned
     */
    public void unpinVersions(String queryId) {
        Map<Long, Long> versions = queryPartitionVersions.remove(queryId);
        queryPinTimes.remove(queryId);

        if (versions == null || versions.isEmpty()) {
            return;
        }

        for (Map.Entry<Long, Long> entry : versions.entrySet()) {
            long partitionId = entry.getKey();
            long version = entry.getValue();
            unpinVersion(partitionId, version);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unpinned versions for query {}: {}", queryId, versions);
        }
    }

    /**
     * Get the minimum pinned version for a partition. Vacuum must not delete
     * any version >= this value's .meta file.
     *
     * @param partitionId the physical partition id
     * @return the minimum pinned version, or Long.MAX_VALUE if no versions are pinned
     */
    public long getMinPinnedVersion(long partitionId) {
        TreeMap<Long, AtomicInteger> versionMap = pinnedVersions.get(partitionId);
        if (versionMap == null) {
            return Long.MAX_VALUE;
        }
        synchronized (versionMap) {
            if (versionMap.isEmpty()) {
                return Long.MAX_VALUE;
            }
            return versionMap.firstKey();
        }
    }

    /**
     * Check if any version is pinned for a partition.
     *
     * @param partitionId the physical partition id
     * @return true if at least one version is pinned
     */
    public boolean hasPinnedVersions(long partitionId) {
        return getMinPinnedVersion(partitionId) != Long.MAX_VALUE;
    }

    /**
     * Clean up stale pins that have been held longer than the specified max age.
     * This is a safety net against leaked pins from crashed or abandoned queries.
     *
     * @param maxAgeMs maximum age in milliseconds before a pin is considered stale
     * @return number of stale queries cleaned up
     */
    public int cleanupStalePins(long maxAgeMs) {
        long now = System.currentTimeMillis();
        int cleaned = 0;
        for (Map.Entry<String, Long> entry : queryPinTimes.entrySet()) {
            if (now - entry.getValue() > maxAgeMs) {
                String queryId = entry.getKey();
                LOG.warn("Cleaning up stale version pin for query {} (age={}ms)", queryId, now - entry.getValue());
                unpinVersions(queryId);
                cleaned++;
            }
        }
        return cleaned;
    }

    /**
     * Get the total number of queries with active pins. Used for monitoring.
     */
    public int getActivePinCount() {
        return queryPartitionVersions.size();
    }

    /**
     * Get all pinned versions for a partition. Used for testing and diagnostics.
     */
    @VisibleForTesting
    public Map<Long, Integer> getPinnedVersionsForPartition(long partitionId) {
        TreeMap<Long, AtomicInteger> versionMap = pinnedVersions.get(partitionId);
        if (versionMap == null) {
            return Collections.emptyMap();
        }
        synchronized (versionMap) {
            Map<Long, Integer> result = new HashMap<>();
            for (Map.Entry<Long, AtomicInteger> entry : versionMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().get());
            }
            return result;
        }
    }

    @VisibleForTesting
    public void clear() {
        pinnedVersions.clear();
        queryPinTimes.clear();
        queryPartitionVersions.clear();
    }

    private void pinVersion(long partitionId, long version) {
        TreeMap<Long, AtomicInteger> versionMap =
                pinnedVersions.computeIfAbsent(partitionId, k -> new TreeMap<>());
        synchronized (versionMap) {
            versionMap.computeIfAbsent(version, k -> new AtomicInteger(0)).incrementAndGet();
        }
    }

    private void unpinVersion(long partitionId, long version) {
        TreeMap<Long, AtomicInteger> versionMap = pinnedVersions.get(partitionId);
        if (versionMap == null) {
            return;
        }
        synchronized (versionMap) {
            AtomicInteger refCount = versionMap.get(version);
            if (refCount != null && refCount.decrementAndGet() <= 0) {
                versionMap.remove(version);
            }
            if (versionMap.isEmpty()) {
                pinnedVersions.remove(partitionId, versionMap);
            }
        }
    }
}
