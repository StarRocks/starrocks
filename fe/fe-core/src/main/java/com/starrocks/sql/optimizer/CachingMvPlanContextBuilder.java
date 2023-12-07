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

package com.starrocks.sql.optimizer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.common.Config;

import java.util.concurrent.TimeUnit;

public class CachingMvPlanContextBuilder {
    private static final CachingMvPlanContextBuilder INSTANCE = new CachingMvPlanContextBuilder();

    private Cache<MvId, MvPlanContext> mvPlanContextCache = Caffeine.newBuilder()
            .expireAfterAccess(Config.mv_plan_cache_expire_interval_sec, TimeUnit.SECONDS)
            .maximumSize(Config.mv_plan_cache_max_size)
            .build();

    private CachingMvPlanContextBuilder() {

    }

    public static CachingMvPlanContextBuilder getInstance() {
        return INSTANCE;
    }

    public MvPlanContext getPlanContext(MaterializedView mv, boolean useCache) {
        MvId mvId = new MvId(mv.getDbId(), mv.getId());
        MvPlanContext result = useCache ? mvPlanContextCache.getIfPresent(mvId) : null;
        if (result == null) {
            MvPlanContextBuilder builder = new MvPlanContextBuilder();
            result = builder.getPlanContext(mv);
            if (result.isValidMvPlan()) {
                mvPlanContextCache.put(mvId, result);
                mv.setPlanMode(MaterializedView.PlanMode.VALID);
            }
        }

        return result;
    }

    @VisibleForTesting
    public boolean contains(MaterializedView mv) {
        return mvPlanContextCache.asMap().containsKey(new MvId(mv.getDbId(), mv.getId()));
    }

    public void invalidateFromCache(MaterializedView mv) {
        mvPlanContextCache.invalidate(new MvId(mv.getDbId(), mv.getId()));
    }
}
