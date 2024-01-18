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
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class CachingMvPlanContextBuilder {

    private static final Logger LOG = LogManager.getLogger(CachingMvPlanContextBuilder.class);
    private static final CachingMvPlanContextBuilder INSTANCE = new CachingMvPlanContextBuilder();

    private Cache<MaterializedView, MvPlanContext> mvPlanContextCache = buildCache();

    private CachingMvPlanContextBuilder() {
    }

    public static CachingMvPlanContextBuilder getInstance() {
        return INSTANCE;
    }

    private Cache<MaterializedView, MvPlanContext> buildCache() {
        return Caffeine.newBuilder()
                .expireAfterAccess(Config.mv_plan_cache_expire_interval_sec, TimeUnit.SECONDS)
                .maximumSize(Config.mv_plan_cache_max_size)
                .build();
    }

    @VisibleForTesting
    public void rebuildCache() {
        mvPlanContextCache = buildCache();
    }

    public MvPlanContext getPlanContext(MaterializedView mv, boolean useCache) {
        if (useCache) {
            return mvPlanContextCache.get(mv, this::loadMvPlanContext);
        } else {
            return loadMvPlanContext(mv);
        }
    }

    private MvPlanContext loadMvPlanContext(MaterializedView mv) {
        MvPlanContextBuilder builder = new MvPlanContextBuilder();
        try {
            return builder.getPlanContext(mv);
        } catch (Throwable e) {
            LOG.warn("load mv plan cache failed: {}", mv.getName(), e);
            return null;
        }
    }

    @VisibleForTesting
    public boolean contains(MaterializedView mv) {
        return mvPlanContextCache.asMap().containsKey(mv);
    }

    public void invalidateFromCache(MaterializedView mv) {
        mvPlanContextCache.invalidate(mv);
    }
}
