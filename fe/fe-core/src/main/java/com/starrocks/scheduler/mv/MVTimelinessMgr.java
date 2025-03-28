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

package com.starrocks.scheduler.mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.memory.MemoryTrackable;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manage the timeliness of materialized view according to the MVChangeEvent across multiple queries.
 * </p>
 * MV's timeliness is a bit heavy for each query's execution, so we cache the timeliness info for each MV. MV can have
 * different query consistency modes, each mode has different timeliness info needs.
 *
 * Now {@code MVTimelinessMgr} only supports `FORCE_MV` mode's timeliness info cache because it's only affected by
 * the MV's refresh and partition drop in this mode.
 *
 * TODO: we can track more mv/base table's changes to update the timeliness info in the future.
 */
public class MVTimelinessMgr implements MemoryTrackable {
    private static final int MEMORY_META_SAMPLES = 10;

    // materialized view -> MvUpdateInfo
    private final Map<MaterializedView, MvUpdateInfo> mvTimelinessMap = Maps.newConcurrentMap();

    public enum MVChangeEvent {
        MV_REFRESHED,
        MV_PARTITION_DROPPED,
    }

    public MvUpdateInfo getMVTimelinessInfo(MaterializedView mv) {
        if (!isEnableMVTimelinessGlobalCache(mv)) {
            return MvRefreshArbiter.getMVTimelinessUpdateInfo(mv, true);
        } else {
            return mvTimelinessMap.computeIfAbsent(mv,
                    (ignored) -> MvRefreshArbiter.getMVTimelinessUpdateInfo(mv, true));
        }
    }

    public static boolean isEnableMVTimelinessGlobalCache(MaterializedView mv) {
        // To avoid bad cases, we disable the global cache for MV timeliness
        if (!Config.enable_mv_query_context_cache) {
            return false;
        }
        TableProperty.QueryRewriteConsistencyMode consistencyMode =
                mv.getTableProperty().getQueryRewriteConsistencyMode();
        return consistencyMode == TableProperty.QueryRewriteConsistencyMode.FORCE_MV;
    }

    public void triggerEvent(MaterializedView mv, MVChangeEvent event) {
        if (!isEnableMVTimelinessGlobalCache(mv)) {
            return;
        }
        TableProperty.QueryRewriteConsistencyMode consistencyMode =
                mv.getTableProperty().getQueryRewriteConsistencyMode();
        if (consistencyMode == TableProperty.QueryRewriteConsistencyMode.FORCE_MV) {
            if (event == MVChangeEvent.MV_REFRESHED || event == MVChangeEvent.MV_PARTITION_DROPPED) {
                mvTimelinessMap.remove(mv);
            }
        } else {
            mvTimelinessMap.remove(mv);
        }
    }

    public void remove(MaterializedView mv) {
        mvTimelinessMap.remove(mv);
    }

    @Override
    public Map<String, Long> estimateCount() {
        return Map.of("mvTimelinessMap", (long) mvTimelinessMap.size());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> mvTimelinessMapSamples = mvTimelinessMap.values()
                .stream()
                .limit(MEMORY_META_SAMPLES)
                .collect(Collectors.toList());
        return Lists.newArrayList(Pair.create(mvTimelinessMapSamples, (long) mvTimelinessMap.size()));
    }
}