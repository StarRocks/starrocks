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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PlanTuningAdvisor {

    private static final PlanTuningAdvisor INSTANCE = new PlanTuningAdvisor();

    private final Cache<PlanTuningCacheKey, OperatorTuningGuides> cache;

    private final Map<UUID, OperatorTuningGuides.OptimizedRecord> optimizedQueryRecords;
    private PlanTuningAdvisor() {
        this.cache = Caffeine.newBuilder()
                .maximumSize(300)
                .build();
        this.optimizedQueryRecords = Maps.newConcurrentMap();
    }

    public static PlanTuningAdvisor getInstance() {
        return INSTANCE;
    }


    public OperatorTuningGuides getTuningGuides(String sql, SkeletonNode skeletonNode) {
        PlanTuningCacheKey key = new PlanTuningCacheKey(sql, skeletonNode);
        return cache.getIfPresent(key);
    }

    public OperatorTuningGuides getOperatorTuningGuides(UUID queryId) {
        OperatorTuningGuides.OptimizedRecord record = optimizedQueryRecords.get(queryId);
        if (record == null) {
            return null;
        }
        return record.getOperatorTuningGuides();
    }

    public OperatorTuningGuides.OptimizedRecord getOptimizedRecord(UUID queryId) {
        if (queryId == null) {
            return null;
        }
        return optimizedQueryRecords.get(queryId);
    }

    public void putTuningGuides(String sql, SkeletonNode skeletonNode, OperatorTuningGuides tuningGuides) {
        if (tuningGuides.isEmpty()) {
            return;
        }
        PlanTuningCacheKey key = new PlanTuningCacheKey(sql, skeletonNode);
        if (cache.getIfPresent(key) == null) {
            cache.put(key, tuningGuides);
        }
    }

    public void clearAllAdvisor() {
        cache.invalidateAll();
        optimizedQueryRecords.clear();
    }

    public void deleteTuningGuides(UUID queryId) {
        for (Map.Entry<PlanTuningCacheKey, OperatorTuningGuides> entry : cache.asMap().entrySet()) {
            if (entry.getValue().getOriginalQueryId().equals(queryId)) {
                cache.invalidate(entry.getKey());
            }
        }
        optimizedQueryRecords.remove(queryId);
    }

    public long getAdvisorSize() {
        return cache.estimatedSize();
    }

    public void addOptimizedQueryRecord(UUID queryId, OperatorTuningGuides.OptimizedRecord optimizedRecord) {
        optimizedQueryRecords.put(queryId, optimizedRecord);
    }

    public void removeOptimizedQueryRecord(UUID queryId) {
        optimizedQueryRecords.remove(queryId);
    }

    public List<List<String>> getShowResult() {
        List<List<String>> result = Lists.newArrayList();
        String nodeName = GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName();
        for (Map.Entry<PlanTuningCacheKey, OperatorTuningGuides> entry : cache.asMap().entrySet()) {
            List<String> row = Lists.newArrayList();
            row.add(entry.getValue().getOriginalQueryId().toString());
            row.add(entry.getKey().getSql());
            row.add(String.valueOf(entry.getValue().getOriginalTimeCost()));
            row.add(entry.getValue().getTuneGuidesInfo(false));
            row.add(String.valueOf(entry.getValue().getAvgTunedTimeCost()));
            row.add(String.valueOf(entry.getValue().optimizedQueryCount()));
            row.add(String.valueOf(entry.getValue().isUseful()));
            row.add(nodeName);
            result.add(row);
        }
        return result;

    }

}
