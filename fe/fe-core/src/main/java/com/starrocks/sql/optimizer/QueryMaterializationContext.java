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
import com.google.api.client.util.Lists;
import com.google.api.client.util.Sets;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Tracers;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.catalog.MvRefreshArbiter.getMVTimelinessUpdateInfo;

/**
 * Store materialized view context during the query lifecycle which is seperated from per materialized view's context.
 */
public class QueryMaterializationContext {
    protected static final Logger LOG = LogManager.getLogger(QueryMaterializationContext.class);

    // MVs that are related to the query
    private Set<MaterializedView> relatedMVs = Sets.newHashSet();
    // MVs with context that are valid (SPJG pattern) candidates for materialization rewrite
    private List<MaterializationContext> validCandidateMVs = Lists.newArrayList();
    // MV with the cached timeliness update info which should be initialized once in one query context.
    private Map<MaterializedView, MvUpdateInfo> mvTimelinessInfos = Maps.newHashMap();

    // used by view based mv rewrite
    // query's logical plan with view
    private OptExpression logicalTreeWithView;
    // collect LogicalViewScanOperators
    private List<LogicalViewScanOperator> viewScans;

    // `mvQueryContextCache` is designed for a common cache which is used during the query's rewrite lifecycle, so can be
    // managed in a more unified mode. In the current situation, it's used in two ways:
    // 1. cache query predicates to its final predicate split per query because PredicateSplit's construct is expensive.
    // 2. cache query predicate to its canonized predicate to avoid one predicate's repeat canonized.
    // It can be be used for more situations later.
    private Cache<Object, Object> mvQueryContextCache = null;

    private final QueryCacheStats queryCacheStats = new QueryCacheStats();

    /**
     * It's used to record the cache stats of `mvQueryContextCache`.
     */
    public class QueryCacheStats {
        @SerializedName("counter")
        private final Map<String, Long> counter = Maps.newHashMap();
        public QueryCacheStats() {
        }

        public void incr(String key) {
            counter.put(key, counter.getOrDefault(key, 0L) + 1);
        }

        public Map<String, Long> getCounter() {
            return counter;
        }

        @Override
        public String toString() {
            return GsonUtils.GSON.toJson(this);
        }
    }

    private boolean hasRewrittenSuccess = false;

    public QueryMaterializationContext() {
    }

    public Cache<Object, Object> getMvQueryContextCache() {
        if (mvQueryContextCache == null) {
            mvQueryContextCache = Caffeine.newBuilder()
                    .maximumSize(Config.mv_query_context_cache_max_size)
                    .recordStats()
                    .build();
        }
        return mvQueryContextCache;
    }

    public PredicateSplit getPredicateSplit(Set<ScalarOperator> predicates,
                                            ReplaceColumnRefRewriter columnRefRewriter) {
        // Cache predicate split for predicates because it's time costing if there are too many materialized views.
        Object cached = getMvQueryContextCache().getIfPresent(predicates);
        if (cached != null) {
            return (PredicateSplit) cached;
        }
        ScalarOperator queryPredicate = rewriteOptExprCompoundPredicate(predicates, columnRefRewriter);
        PredicateSplit predicateSplit = PredicateSplit.splitPredicate(queryPredicate);
        if (predicateSplit != null) {
            getMvQueryContextCache().put(predicates, predicateSplit);
        }
        return predicateSplit;
    }

    private ScalarOperator rewriteOptExprCompoundPredicate(Set<ScalarOperator> conjuncts,
                                                           ReplaceColumnRefRewriter columnRefRewriter) {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return null;
        }
        ScalarOperator compoundPredicate = Utils.compoundAnd(conjuncts);
        compoundPredicate = columnRefRewriter.rewrite(compoundPredicate.clone());
        return getCanonizedPredicate(compoundPredicate);
    }

    public ScalarOperator getCanonizedPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }

        return (ScalarOperator) getMvQueryContextCache().get(predicate, x -> {
            ScalarOperator rewritten = new ScalarOperatorRewriter()
                    .rewrite(predicate.clone(), ScalarOperatorRewriter.MV_SCALAR_REWRITE_RULES);
            return rewritten;
        });
    }

    public QueryCacheStats getQueryCacheStats() {
        return queryCacheStats;
    }

    public OptExpression getLogicalTreeWithView() {
        return logicalTreeWithView;
    }

    public void setLogicalTreeWithView(OptExpression logicalTreeWithView) {
        this.logicalTreeWithView = logicalTreeWithView;
    }

    public void setViewScans(List<LogicalViewScanOperator> viewScans) {
        this.viewScans = viewScans;
    }

    public List<LogicalViewScanOperator> getViewScans() {
        return viewScans;
    }

    /**
     * Add related mvs about this query.
     * @param mvs: related mvs
     */
    public void addRelatedMVs(Set<MaterializedView> mvs) {
        relatedMVs.addAll(mvs);
    }

    /**
     * Add valid candidate materialized view for the query:
     * @param mv mv with context that is valid (SPJG pattern) candidate for materialization rewrite
     */
    public void addValidCandidateMV(MaterializationContext mv) {
        validCandidateMVs.add(mv);
    }

    /**
     * Get or init the cached timeliness update info for the materialized view.
     * @param mv intput mv
     * @return MvUpdateInfo of the mv, null if mv is null or initialize fail
     */
    public MvUpdateInfo getOrInitMVTimelinessInfos(MaterializedView mv) {
        if (mv == null) {
            return null;
        }
        if (!mvTimelinessInfos.containsKey(mv)) {
            MvUpdateInfo result = getMVTimelinessUpdateInfo(mv, true);
            mvTimelinessInfos.put(mv, result);
            return result;
        } else {
            return mvTimelinessInfos.get(mv);
        }
    }

    /**
     * All related mvs about this query which contains valid candidate mvs(SPJG) and other mvs(non SPGJ).
     * @return
     */
    public Set<MaterializedView> getRelatedMVs() {
        return relatedMVs;
    }

    /**
     * Get all valid candidate materialized views for the query:
     * - The materialized view is valid to rewrite by rule(SPJG)
     * - The materialized view's refresh-ness is valid to rewrite.
     */
    public List<MaterializationContext> getValidCandidateMVs() {
        return validCandidateMVs;
    }

    // Invalidate all caches by hand to avoid memory allocation after query optimization.
    public void clear() {
        if (mvQueryContextCache == null) {
            return;
        }
        if (ConnectContext.get() != null) {
            QueryDebugOptions debugOptions = ConnectContext.get().getSessionVariable().getQueryDebugOptions();
            if (debugOptions.isEnableQueryTraceLog()) {
                LOG.info("MVQueryContextCache Stats:{}, estimatedSize:{}",
                        mvQueryContextCache.stats(), mvQueryContextCache.estimatedSize());
            }
        }
        // record cache stats
        Tracers.record(Tracers.Module.BASE, "MVQueryContextCacheStats", mvQueryContextCache.stats().toString());
        Tracers.record(Tracers.Module.BASE, "MVQueryCacheStats", queryCacheStats.toString());
        this.mvQueryContextCache.invalidateAll();
    }

    public void markRewriteSuccess(boolean val) {
        this.hasRewrittenSuccess = val;
    }

    public boolean hasRewrittenSuccess() {
        return this.hasRewrittenSuccess;
    }
}
