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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.common.Config;
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

    // store the ast of mv's define query to mvs
    private Map<AstKey, Set<MaterializedView>> astToMvsMap = Maps.newConcurrentMap();

    public static class AstKey {
        private final String sql;
        public AstKey(ParseNode parseNode) {
            this.sql = new AstToSQLBuilder.AST2SQLBuilderVisitor(true, false).visit(parseNode);
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
            if (this.sql == null) {
                return false;
            }
            // TODO: add more checks.
            return this.sql.equals(other.sql);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.sql);
        }

        @Override
        public String toString() {
            return this.sql;
        }
    }


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
                .recordStats()
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

    public void invalidateFromCache(MaterializedView mv, boolean isActive) {
        mvPlanContextCache.invalidate(mv);
        invalidateAstFromCache(mv);

        // if transfer to active, put it into cache
        if (isActive) {
            putAstIfAbsent(mv);
        }
    }

    public void invalidateAstFromCache(MaterializedView mv) {
        try {
            ParseNode parseNode = mv.getDefineQueryParseNode();
            if (parseNode == null) {
                return;
            }
            AstKey astKey = new AstKey(parseNode);
            if (!astToMvsMap.containsKey(astKey)) {
                return;
            }
            astToMvsMap.get(astKey).remove(mv);
            LOG.info("Remove mv {} from ast cache", mv.getName());
        } catch (Exception e) {
            LOG.warn("invalidateAstFromCache failed: {}", mv.getName(), e);
        }
    }

    /**
     * This method is used to put mv into ast cache, this will be only called in the first time.
     */
    public void putAstIfAbsent(MaterializedView mv) {
        if (!Config.enable_materialized_view_text_based_rewrite || mv == null || !mv.isEnableRewrite()) {
            return;
        }
        try {
            // initialize define query parse node each time
            mv.initDefineQueryParseNode();

            // cache by ast
            ParseNode parseNode = mv.getDefineQueryParseNode();
            if (parseNode == null) {
                return;
            }
            astToMvsMap.computeIfAbsent(new AstKey(parseNode), ignored -> Sets.newHashSet())
                    .add(mv);
            LOG.info("Add mv {} input ast cache", mv.getName());
        } catch (Exception e) {
            LOG.warn("putAstIfAbsent failed: {}", mv.getName(), e);
        }
    }

    /**
     * @param parseNode: ast to query.
     * @return: null if parseNode is null or astToMvsMap doesn't contain this ast, otherwise return the mvs
     */
    public Set<MaterializedView> getMvsByAst(ParseNode parseNode) {
        if (parseNode == null) {
            return null;
        }
        return astToMvsMap.get(new AstKey(parseNode));
    }

    /**
     * Get associated asts of related mvs for debug usage to find the difference between the asts of related mvs and real ast.
     */
    public List<AstKey> getAstsOfRelatedMvs(Set<MaterializedView> relatedMvs) {
        List<AstKey> keys = Lists.newArrayList();
        for (Map.Entry<CachingMvPlanContextBuilder.AstKey, Set<MaterializedView>> e : this.astToMvsMap.entrySet()) {
            CachingMvPlanContextBuilder.AstKey cacheKey = e.getKey();
            Set<MaterializedView> cacheMvs = e.getValue();
            if (Sets.intersection(cacheMvs, relatedMvs).isEmpty()) {
                continue;
            }
            keys.add(cacheKey);
        }
        return keys;
    }
}
