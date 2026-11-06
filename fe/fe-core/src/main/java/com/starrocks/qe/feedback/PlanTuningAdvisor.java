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

package com.starrocks.qe.feedback;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.feedback.skeleton.ScanNode;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class PlanTuningAdvisor {

    private static final int CAPACITY = 300;

    // StmtExecutor removes the record of a query when the statement ends. Queries planned outside StmtExecutor.execute
    // never remove theirs, so the records are also bounded. The TTL only has to outlive the queries that still run.
    private static final int MAX_OPTIMIZED_RECORDS = 10_000;
    private static final long OPTIMIZED_RECORD_TTL_HOURS = 24;

    private static final PlanTuningAdvisor INSTANCE = new PlanTuningAdvisor(CAPACITY);

    // Keys are equal when plans have the same shape, regardless of the scan predicate constants. The stored key
    // carries the merged predicate ranges its guides apply to, so it is kept in the value to check coverage.
    private final Cache<PlanTuningCacheKey, CacheEntry> cache;

    // Record and permanently disable tuning guide usage for patterns with poor performance.
    // All blacklisted keys of a shape are kept, since each of them only covers its own predicate ranges.
    private final Cache<PlanTuningCacheKey, List<PlanTuningCacheKey>> tuningGuideBlacklist;

    // The tuning guides each running query was planned with, keyed by query id
    private final Cache<UUID, OperatorTuningGuides.OptimizedRecord> optimizedQueryRecords;

    @VisibleForTesting
    PlanTuningAdvisor(int capacity) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(capacity)
                .build();


        this.tuningGuideBlacklist = Caffeine.newBuilder()
                .maximumSize(capacity)
                .build();

        this.optimizedQueryRecords = Caffeine.newBuilder()
                .maximumSize(MAX_OPTIMIZED_RECORDS)
                .expireAfterWrite(OPTIMIZED_RECORD_TTL_HOURS, TimeUnit.HOURS)
                .build();
    }

    public static PlanTuningAdvisor getInstance() {
        return INSTANCE;
    }


    public OperatorTuningGuides getTuningGuides(String sql, SkeletonNode skeletonNode) {
        PlanTuningCacheKey key = new PlanTuningCacheKey(sql, skeletonNode);
        CacheEntry entry = cache.getIfPresent(key);
        return entry != null && key.isCoveredBy(entry.key()) ? entry.tuningGuides() : null;
    }

    public OperatorTuningGuides getOperatorTuningGuides(UUID queryId) {
        OperatorTuningGuides.OptimizedRecord record = getOptimizedRecord(queryId);
        if (record == null) {
            return null;
        }
        return record.getOperatorTuningGuides();
    }

    public OperatorTuningGuides.OptimizedRecord getOptimizedRecord(UUID queryId) {
        if (queryId == null) {
            return null;
        }
        return optimizedQueryRecords.getIfPresent(queryId);
    }

    public void putTuningGuides(String sql, SkeletonNode skeletonNode, OperatorTuningGuides tuningGuides) {
        if (tuningGuides.isEmpty()) {
            return;
        }

        PlanTuningCacheKey key = new PlanTuningCacheKey(sql, skeletonNode);
        if (isBlacklisted(key)) {
            Tracers.record(Tracers.Module.BASE, "IgnoreBlacklistTuningGuide",
                    "Pattern previously marked as ineffective, ignoring new analyzed tuning guide");
            return;
        }

        // A pattern keeps its first guides; the same guides found again extend the ranges they apply to. Both happen
        // in one compute, so concurrent puts of a pattern neither add a second entry nor lose a merge.
        cache.asMap().compute(key, (k, existing) -> {
            if (existing == null) {
                tuningGuides.getAllTuningGuides().forEach(PlanAdvisorMetrics::increaseGuideGenerated);
                return new CacheEntry(key, tuningGuides);
            }
            if (existing.tuningGuides().equals(tuningGuides)) {
                mergeRangesForScanNodes(existing.key(), key);
            }
            return existing;
        });
    }

    private boolean isBlacklisted(PlanTuningCacheKey key) {
        List<PlanTuningCacheKey> blacklistedKeys = tuningGuideBlacklist.getIfPresent(key);
        return blacklistedKeys != null && blacklistedKeys.stream().anyMatch(key::isCoveredBy);
    }

    private void mergeRangesForScanNodes(PlanTuningCacheKey targetKey, PlanTuningCacheKey sourceKey) {
        List<SkeletonNode> targetNodes = targetKey.getSkeletonNodes();
        List<SkeletonNode> sourceNodes = sourceKey.getSkeletonNodes();

        for (int i = 0; i < targetNodes.size(); i++) {
            if (targetNodes.get(i) instanceof ScanNode targetScanNode &&
                    sourceNodes.get(i) instanceof ScanNode sourceScanNode) {
                try {
                    targetScanNode.mergeColumnRangePredicate(sourceScanNode);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    public void clearAllAdvisor() {
        cache.invalidateAll();
        optimizedQueryRecords.invalidateAll();
        tuningGuideBlacklist.invalidateAll();
    }

    public void deleteTuningGuides(UUID queryId, boolean preventFutureTuning) {
        for (Map.Entry<PlanTuningCacheKey, CacheEntry> entry : cache.asMap().entrySet()) {
            if (entry.getValue().tuningGuides().getOriginalQueryId().equals(queryId)) {
                cache.invalidate(entry.getKey());
                if (preventFutureTuning) {
                    tuningGuideBlacklist.asMap().compute(entry.getKey(), (k, keys) -> {
                        List<PlanTuningCacheKey> blacklistedKeys = keys != null ? keys : new CopyOnWriteArrayList<>();
                        blacklistedKeys.add(entry.getValue().key());
                        return blacklistedKeys;
                    });
                    Tracers.record(Tracers.Module.BASE, "AddBlacklistTuningGuide", "The tuning guides for the current " +
                            "query pattern are ineffective and will be removed and permanently disabled.");
                }
            }
        }
        optimizedQueryRecords.invalidate(queryId);
    }

    public long getAdvisorSize() {
        return cache.estimatedSize();
    }

    @VisibleForTesting
    long blacklistSize() {
        return tuningGuideBlacklist.asMap().values().stream().mapToLong(List::size).sum();
    }

    /**
     * Runs the pending cache maintenance, then checks that the caches are consistent: the size matches the live
     * entries, which a mapping left behind by an eviction breaks, every entry is reachable by its own key, and no two
     * keys of a cache are equal.
     */
    @VisibleForTesting
    void checkInvariants() {
        cache.cleanUp();
        tuningGuideBlacklist.cleanUp();

        List<PlanTuningCacheKey> keys = Lists.newArrayList(cache.asMap().keySet());
        Preconditions.checkState(keys.size() == cache.estimatedSize(),
                "%s live entries, but the cache size is %s", keys.size(), cache.estimatedSize());
        for (PlanTuningCacheKey key : keys) {
            CacheEntry entry = cache.asMap().get(key);
            Preconditions.checkState(entry != null && entry.key() == key,
                    "the entry of %s is not reachable by its key", key.getSql());
        }
        checkNoEqualKeys(keys);

        List<PlanTuningCacheKey> blacklistKeys = Lists.newArrayList(tuningGuideBlacklist.asMap().keySet());
        Preconditions.checkState(blacklistKeys.size() == tuningGuideBlacklist.estimatedSize(),
                "%s live blacklist entries, but the blacklist size is %s",
                blacklistKeys.size(), tuningGuideBlacklist.estimatedSize());
        for (PlanTuningCacheKey key : blacklistKeys) {
            List<PlanTuningCacheKey> blacklistedKeys = tuningGuideBlacklist.asMap().get(key);
            Preconditions.checkState(blacklistedKeys != null && !blacklistedKeys.isEmpty() &&
                    blacklistedKeys.stream().allMatch(key::equals), "bad blacklist entry of %s", key.getSql());
        }
        checkNoEqualKeys(blacklistKeys);
    }

    private static void checkNoEqualKeys(List<PlanTuningCacheKey> keys) {
        for (int i = 0; i < keys.size(); i++) {
            for (int j = i + 1; j < keys.size(); j++) {
                Preconditions.checkState(!keys.get(i).equals(keys.get(j)) && !keys.get(j).equals(keys.get(i)),
                        "two keys are equal: %s / %s", keys.get(i).getSql(), keys.get(j).getSql());
            }
        }
    }

    public void addOptimizedQueryRecord(UUID queryId, OperatorTuningGuides.OptimizedRecord optimizedRecord) {
        optimizedQueryRecords.put(queryId, optimizedRecord);
    }

    public void removeOptimizedQueryRecord(UUID queryId) {
        if (queryId != null) {
            optimizedQueryRecords.invalidate(queryId);
        }
    }

    @VisibleForTesting
    long optimizedQueryRecordCount() {
        optimizedQueryRecords.cleanUp();
        return optimizedQueryRecords.estimatedSize();
    }

    public List<List<String>> getShowResult() {
        List<List<String>> result = Lists.newArrayList();
        String nodeName = GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName();
        for (Map.Entry<PlanTuningCacheKey, CacheEntry> entry : cache.asMap().entrySet()) {
            OperatorTuningGuides tuningGuides = entry.getValue().tuningGuides();
            List<String> row = Lists.newArrayList();
            row.add(tuningGuides.getOriginalQueryId().toString());
            row.add(entry.getKey().getSql());
            row.add(String.valueOf(tuningGuides.getOriginalTimeCost()));
            row.add(tuningGuides.getTuneGuidesInfo(false));
            row.add(String.valueOf(tuningGuides.getAvgTunedTimeCost()));
            row.add(String.valueOf(tuningGuides.optimizedQueryCount()));
            row.add(String.valueOf(tuningGuides.isUseful()));
            row.add(nodeName);
            result.add(row);
        }
        return result;

    }

    private record CacheEntry(PlanTuningCacheKey key, OperatorTuningGuides tuningGuides) {
    }

}
