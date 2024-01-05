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
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

/**
 * Store materialized view context during the query lifecycle which is seperated from per materialized view's context.
 */
public class QueryMaterializationContext {
    protected static final Logger LOG = LogManager.getLogger(QueryMaterializationContext.class);

    // Because PredicateSplit's construct is expensive, cache query predicates to its final predicate split per query.
    // Cache query predicate to its canonized predicate to avoid one predicate's repeat canonized.
    private final Cache<Object, Object> mvQueryContextCache = Caffeine.newBuilder()
            .maximumSize(Config.mv_query_context_cache_max_size)
            .recordStats()
            .build();

    public QueryMaterializationContext() {
    }

    public Cache<Object, Object> getMvQueryContextCache() {
        return mvQueryContextCache;
    }

    public PredicateSplit getPredicateSplit(Set<ScalarOperator> predicates,
                                            ReplaceColumnRefRewriter columnRefRewriter) {
        // Cache predicate split for predicates because it's time costing if there are too many materialized views.
        Object cached = mvQueryContextCache.getIfPresent(predicates);
        if (cached != null) {
            return (PredicateSplit) cached;
        }
        ScalarOperator queryPredicate = rewriteOptExprCompoundPredicate(predicates, columnRefRewriter);
        PredicateSplit predicateSplit = PredicateSplit.splitPredicate(queryPredicate);
        if (predicateSplit != null) {
            mvQueryContextCache.put(predicates, predicateSplit);
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
        try {
            return (ScalarOperator) mvQueryContextCache.get(predicate, x -> {
                ScalarOperator rewritten = new ScalarOperatorRewriter()
                        .rewrite(predicate.clone(), ScalarOperatorRewriter.MV_SCALAR_REWRITE_RULES);
                return rewritten;
            });
        } catch (NullPointerException e) {
            return null;
        } catch (Exception e) {
            LOG.warn("Canonize predicate for rewrite failed: ", e);
            return null;
        }
    }

    // Invalidate all caches by hand to avoid memory allocation after query optimization.
    public void clear() {
        if (ConnectContext.get() != null) {
            QueryDebugOptions debugOptions = ConnectContext.get().getSessionVariable().getQueryDebugOptions();
            if (debugOptions.isEnableQueryTraceLog()) {
                LOG.info("MVQueryContextCache Stats:{}, estimatedSize:{}",
                        mvQueryContextCache.stats(), mvQueryContextCache.estimatedSize());
            }
        }
        this.mvQueryContextCache.invalidateAll();
    }
}
