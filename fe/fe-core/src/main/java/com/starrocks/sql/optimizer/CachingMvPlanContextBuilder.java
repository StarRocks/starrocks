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
import com.google.api.client.util.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CachingMvPlanContextBuilder {

    private static final Logger LOG = LogManager.getLogger(CachingMvPlanContextBuilder.class);
    private static final CachingMvPlanContextBuilder INSTANCE = new CachingMvPlanContextBuilder();

    private Cache<MaterializedView, List<MvPlanContext>> mvPlanContextCache = buildCache();

    public static class AstKey {
        private final ParseNode parseNode;
        public AstKey(ParseNode parseNode) {
            this.parseNode = parseNode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || ! (o instanceof AstKey)) {
                return false;
            }
            AstKey other = (AstKey) o;
            // TODO: add more checks.
            return this.toString().equals(other.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.toString());
        }

        @Override
        public String toString() {
            return new AstToSQLBuilder.AST2SQLBuilderVisitor(true, false).visit(parseNode);
        }
    }

    private Map<AstKey, Set<MaterializedView>> astToMvPlanContextMap = Maps.newConcurrentMap();

    private CachingMvPlanContextBuilder() {
    }

    public static CachingMvPlanContextBuilder getInstance() {
        return INSTANCE;
    }

    // After view based mv rewrite, one mv may has views as based tables.
    // It can return logical plans with or without inline views.
    // So here should return a List<MvPlanContext> for one mv
    private Cache<MaterializedView, List<MvPlanContext>> buildCache() {
        return Caffeine.newBuilder()
                .expireAfterAccess(Config.mv_plan_cache_expire_interval_sec, TimeUnit.SECONDS)
                .maximumSize(Config.mv_plan_cache_max_size)
                .build();
    }

    @VisibleForTesting
    public void rebuildCache() {
        mvPlanContextCache = buildCache();
    }

    public List<MvPlanContext> getPlanContext(MaterializedView mv, boolean useCache) {
        if (useCache) {
            return mvPlanContextCache.get(mv, this::loadMvPlanContext);
        } else {
            return loadMvPlanContext(mv);
        }
    }

    /**
     * Get plan cache only if mv is present in the plan cache, otherwise null is returned.
     */
    public List<MvPlanContext> getPlanContextFromCacheIfPresent(MaterializedView mv) {
        return mvPlanContextCache.getIfPresent(mv);
    }

    private List<MvPlanContext> loadMvPlanContext(MaterializedView mv) {
        try {
            return MvPlanContextBuilder.getPlanContext(mv);
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

    public void putAstIfAbsent(MaterializedView mv) {
        ParseNode parseNode = mv.getDefineQueryParseNode();
        if (parseNode == null) {
            return;
        }
        astToMvPlanContextMap.computeIfAbsent(new AstKey(parseNode), ignored -> Sets.newHashSet())
                .add(mv);
        LOG.info("Add mv {} input ast cache", mv.getName());
    }

    public void invalidMvByAst(MaterializedView mv) {
        ParseNode parseNode = mv.getDefineQueryParseNode();
        if (parseNode == null) {
            return;
        }
        AstKey astKey = new AstKey(parseNode);
        if (!astToMvPlanContextMap.containsKey(astKey)) {
            return;
        }
        astToMvPlanContextMap.get(astKey).remove(mv);
        LOG.info("Remove mv {} from ast cache", mv.getName());
    }

    public Set<MaterializedView> getMaterializedViewsByAst(ParseNode ast) {
        AstKey key = new AstKey(ast);
        Set<MaterializedView> mvs = astToMvPlanContextMap.get(key);
        if (mvs == null) {
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null && connectContext.getSessionVariable().getQueryDebugOptions()
                    .isEnableQueryTraceLog()) {
                LOG.warn("\n<<<<<<<<<<<<<<<<<<");
                LOG.warn("Query Key: {}", key);
                for (AstKey cacheKey : astToMvPlanContextMap.keySet()) {
                    LOG.warn("Cached Key: {}", cacheKey);
                }
            }
            return Sets.newHashSet();
        }
        return mvs;
    }
}
