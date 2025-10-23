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

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.mv.MVTimelinessMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.formatter.FormatOptions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class CachingMvPlanContextBuilder {
    private static final Logger LOG = LogManager.getLogger(CachingMvPlanContextBuilder.class);

    private static final CachingMvPlanContextBuilder INSTANCE = new CachingMvPlanContextBuilder();

    private static final ExecutorService MV_PLAN_CACHE_EXECUTOR = Executors.newFixedThreadPool(
            Config.mv_plan_cache_thread_pool_size,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("mv-plan-cache-%d").build());

    private static final AsyncCacheLoader<MaterializedView, List<MvPlanContext>> MV_PLAN_CACHE_LOADER =
            new AsyncCacheLoader<>() {
                @Override
                public @NonNull CompletableFuture<List<MvPlanContext>> asyncLoad(@NonNull MaterializedView mv,
                                                                                 @NonNull Executor executor) {
                    return CompletableFuture
                            .supplyAsync(() -> loadMvPlanContext(mv), executor)
                            .exceptionally(e -> {
                                LOG.warn("load mv plan cache failed: {}", mv.getName(), e);
                                return Lists.newArrayList();
                            });
                }

                @Override
                public CompletableFuture<List<MvPlanContext>> asyncReload(@NonNull MaterializedView mv,
                                                                          @NonNull List<MvPlanContext> oldValue,
                                                                          @NonNull Executor executor) {
                    return asyncLoad(mv, executor);
                }
            };

    // After view-based mv rewrite, one mv may has views as based tables, It can return logical plans with or without inline
    // views. So here should return a List<MvPlanContext> for one mv.
    private static final AsyncLoadingCache<MaterializedView, List<MvPlanContext>> MV_PLAN_CONTEXT_CACHE = Caffeine.newBuilder()
            .maximumSize(Config.mv_plan_cache_max_size)
            .executor(MV_PLAN_CACHE_EXECUTOR)
            .recordStats()
            .buildAsync(MV_PLAN_CACHE_LOADER);

    // store the ast of mv's define query to mvs
    private static final Map<AstKey, Set<MaterializedView>> AST_TO_MV_MAP = Maps.newConcurrentMap();

    public static class AstKey {
        private final String sql;

        /**
         * Create a AstKey with parseNode(sub parse node)
         */
        public AstKey(ParseNode parseNode) {
            this.sql = AST2SQLVisitor.withOptions(FormatOptions.allEnable().setEnableDigest(false)).visit(parseNode);
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

    private CompletableFuture<List<MvPlanContext>> getPlanContextFuture(MaterializedView mv) {
        return MV_PLAN_CONTEXT_CACHE.get(mv);
    }

    /**
     * Get plan cache, if enabled; otherwise, load plan context directly.
     */
    public List<MvPlanContext> getPlanContext(SessionVariable sessionVariable,
                                              MaterializedView mv) {
        return getPlanContext(sessionVariable, mv, sessionVariable.getOptimizerExecuteTimeout());
    }

    public List<MvPlanContext> getPlanContext(SessionVariable sessionVariable,
                                              MaterializedView mv,
                                              long timeoutMs) {
        if (!sessionVariable.isEnableMaterializedViewPlanCache()) {
            return loadMvPlanContext(mv);
        }
        return getOrLoadPlanContext(mv, timeoutMs);
    }

    /**
     * Get or load plan cache(always from cache), return null if failed to get or load plan cache.
     */
    public List<MvPlanContext> getOrLoadPlanContext(MaterializedView mv,
                                                    long timeoutMs) {
        CompletableFuture<List<MvPlanContext>> future = getPlanContextFuture(mv);
        return getMvPlanCacheFromFuture(mv, future, timeoutMs);
    }

    /**
     * Get plan cache only if mv is present in the plan cache, otherwise null is returned.
     */
    public List<MvPlanContext> getPlanContextIfPresent(MaterializedView mv,
                                                       long timeoutMs) {
        CompletableFuture<List<MvPlanContext>> future = MV_PLAN_CONTEXT_CACHE.getIfPresent(mv);
        if (future == null) {
            return Lists.newArrayList();
        }
        return getMvPlanCacheFromFuture(mv, future, timeoutMs);
    }

    /**
     * Build the plan for MV, return an empty list if no plan is available
     */
    private static List<MvPlanContext> loadMvPlanContext(MaterializedView mv) {
        try {
            return MvPlanContextBuilder.getPlanContext(mv, false);
        } catch (Throwable e) {
            LOG.warn("load mv plan cache failed: {}", mv.getName(), e);
            return Lists.newArrayList();
        }
    }

    /**
     * Get mv plan cache from future with timeout (use new_planner_optimize_timeout as timeout by default)
     */
    private List<MvPlanContext> getMvPlanCacheFromFuture(MaterializedView mv,
                                                         CompletableFuture<List<MvPlanContext>> future,
                                                         long timeoutMs) {
        List<MvPlanContext> result;
        long startTime = System.currentTimeMillis();
        try {
            result = future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOG.warn("get mv plan cache timeout: {}", mv.getName());
            return null;
        } catch (Throwable e) {
            LOG.warn("get mv plan cache failed: {}", mv.getName(), e);
            return null;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get mv plan cache success: {}, cost: {}ms", mv.getName(),
                    System.currentTimeMillis() - startTime);
        }
        return result;
    }

    /**
     * Rebuild the cache, this method is used for test only.
     */
    @VisibleForTesting
    public void rebuildCache() {
        MV_PLAN_CONTEXT_CACHE.synchronous().invalidateAll();
    }

    @VisibleForTesting
    public boolean contains(MaterializedView mv) {
        return MV_PLAN_CONTEXT_CACHE.asMap().containsKey(mv);
    }

    /**
     * Cache materialized view, this will put the mv into ast cache and load plan context asynchronously.
     * @param mv: the materialized view to cache.
     */
    public void cacheMaterializedView(MaterializedView mv) {
        // evict mv from cache first
        evictMaterializedViewCache(mv);
        // then put mv into ast cache and load plan context
        try {
            putAstIfAbsent(mv);
            loadPlanContextAsync(mv);
        } catch (Exception e) {
            LOG.warn("cacheMaterializedView failed: {}", mv.getName(), e);
        }
    }

    /**
     * Evict materialized view from plan cache and ast cache.
     * @param mv: the materialized view to evict from cache.
     */
    public void evictMaterializedViewCache(MaterializedView mv) {
        try {
            // invalidate mv from plan cache
            MV_PLAN_CONTEXT_CACHE.synchronous().invalidate(mv);

            // invalidate mv from timeline cache
            MVTimelinessMgr mvTimelinessMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr().getMvTimelinessMgr();
            mvTimelinessMgr.remove(mv);

            List<AstKey> astKeys = getAstKeysOfMV(mv);
            if (CollectionUtils.isEmpty(astKeys)) {
                return;
            }
            synchronized (AST_TO_MV_MAP) {
                for (AstKey astKey : astKeys) {
                    if (!AST_TO_MV_MAP.containsKey(astKey)) {
                        continue;
                    }
                    // remove mv from ast cache
                    Set<MaterializedView> relatedMVs = AST_TO_MV_MAP.get(astKey);
                    relatedMVs.remove(mv);

                    // remove ast key if no related mvs
                    if (relatedMVs.isEmpty()) {
                        AST_TO_MV_MAP.remove(astKey);
                    }
                }
            }
            LOG.debug("Remove mv {} from ast cache", mv.getName());
        } catch (Exception e) {
            LOG.warn("invalidateAstFromCache failed: {}", mv.getName(), e);
        }
    }

    /**
     * Load plan context asynchronously and put it into cache.
     * @param mv: the materialized view to load plan context for.
     */
    public void loadPlanContextAsync(MaterializedView mv) {
        long startTime = System.currentTimeMillis();
        CompletableFuture<List<MvPlanContext>> future = MV_PLAN_CONTEXT_CACHE.get(mv);
        // do not join.
        future.whenComplete((result, e) -> {
            long duration = System.currentTimeMillis() - startTime;
            if (e == null) {
                LOG.info("finish adding mv plan into cache success: {}, cost: {}ms", mv.getName(),
                        duration);
            } else {
                LOG.warn("adding mv plan into cache failed: {}, cost: {}ms", mv.getName(), duration, e);
            }
        });
    }

    /**
     * This method is used to put mv into ast cache, this will be only called in the first time.
     */
    private void putAstIfAbsent(MaterializedView mv) {
        if (!Config.enable_materialized_view_text_based_rewrite || mv == null || !mv.isEnableRewrite()) {
            return;
        }
        long startTime = System.currentTimeMillis();
        try {
            // cache by ast
            List<AstKey> astKeys = getAstKeysOfMV(mv);
            if (CollectionUtils.isEmpty(astKeys)) {
                return;
            }
            synchronized (AST_TO_MV_MAP) {
                for (AstKey astKey : astKeys) {
                    AST_TO_MV_MAP.computeIfAbsent(astKey, ignored -> Sets.newHashSet()).add(mv);
                }
            }
            LOG.info("finish to put mv into ast cache: {}, cost:{}(ms)", mv.getName(),
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            LOG.warn("put to mv into ast cache failed: {}, cost:{}(ms)", mv.getName(),
                    System.currentTimeMillis() - startTime, e);
        }
    }

    private List<AstKey> getAstKeysOfMV(MaterializedView mv) {
        List<AstKey> keys = Lists.newArrayList();
        ParseNode parseNode = mv.getDefineQueryParseNode();
        if (parseNode == null) {
            return keys;
        }
        // add the complete ast tree
        keys.add(new AstKey(parseNode));
        if (!(parseNode instanceof QueryStatement)) {
            return keys;
        }

        // add the ast tree without an order by clause
        QueryStatement queryStatement = (QueryStatement) parseNode;
        QueryRelation queryRelation = queryStatement.getQueryRelation();
        if (!queryRelation.hasLimit() && queryRelation.hasOrderByClause()) {
            // it's fine to change query relation directly since it's not used anymore.
            List<OrderByElement> orderByElements = Lists.newArrayList(queryRelation.getOrderBy());
            try {
                queryRelation.clearOrder();
                keys.add(new AstKey(parseNode));
            } finally {
                queryRelation.setOrderBy(orderByElements);
            }
        }
        return keys;
    }

    /**
     * @return: null if parseNode is null or astToMvsMap doesn't contain this ast, otherwise return the mvs
     */
    public Set<MaterializedView> getMvsByAst(AstKey ast) {
        if (ast == null) {
            return null;
        }
        return AST_TO_MV_MAP.get(ast);
    }

    /**
     * Get associated asts of related mvs for debug usage to find the difference between the asts of related mvs and real ast.
     */
    public List<AstKey> getAstsOfRelatedMvs(Set<MaterializedView> relatedMvs) {
        List<AstKey> keys = Lists.newArrayList();
        for (Map.Entry<CachingMvPlanContextBuilder.AstKey, Set<MaterializedView>> e : this.AST_TO_MV_MAP.entrySet()) {
            CachingMvPlanContextBuilder.AstKey cacheKey = e.getKey();
            Set<MaterializedView> cacheMvs = e.getValue();
            if (Sets.intersection(cacheMvs, relatedMvs).isEmpty()) {
                continue;
            }
            keys.add(cacheKey);
        }
        return keys;
    }

    /**
     * Submit an async task to be executed in MV plan cache executor.
     * @param taskName: the name of the task.
     * @param task: the task to be executed.
     */
    public static void submitAsyncTask(String taskName, Supplier<Void> task) {
        CompletableFuture<?> future = CompletableFuture.supplyAsync(task, MV_PLAN_CACHE_EXECUTOR);
        long startTime = System.currentTimeMillis();
        future.whenComplete((result, e) -> {
            long duration = System.currentTimeMillis() - startTime;
            if (e == null) {
                LOG.info("async task {} finished successfully, cost: {}ms", taskName, duration);
            } else {
                LOG.warn("async task {} failed: {}, cost: {}ms", taskName, e.getMessage(), duration, e);
            }
        });
    }
}
