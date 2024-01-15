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

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.metric.MaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVColumnPruner;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVPartitionPruner;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.metric.MaterializedViewMetricsEntity.isUpdateMaterializedViewMetrics;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

public abstract class BaseMaterializedViewRewriteRule extends TransformationRule {

    protected BaseMaterializedViewRewriteRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    private boolean checkOlapScanWithoutTabletOrPartitionHints(OptExpression input) {
        if (input.getOp() instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scan = input.getOp().cast();
            if (scan.hasTableHints()) {
                return false;
            }
            // Avoid rewrite the query repeat, add a shortcut.
            Table table = scan.getTable();
            if ((table instanceof MaterializedView) && ((MaterializedView) (table)).getRefreshScheme().isSync()) {
                return false;
            }
        }
        if (input.getInputs().isEmpty()) {
            return true;
        }
        return input.getInputs().stream().allMatch(this::checkOlapScanWithoutTabletOrPartitionHints);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return !context.getCandidateMvs().isEmpty() && checkOlapScanWithoutTabletOrPartitionHints(input);
    }

    @Override
    public boolean exhausted(OptimizerContext context) {
        if (context.ruleExhausted(type())) {
            OptimizerTraceUtil.logMVRewrite(context, this, "rule exhausted");
            return true;
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression queryExpression, OptimizerContext context) {
        List<MaterializationContext> mvCandidateContexts = Lists.newArrayList();
        if (queryExpression.getGroupExpression() != null) {
            int currentRootGroupId = queryExpression.getGroupExpression().getGroup().getId();
            for (MaterializationContext mvContext : context.getCandidateMvs()) {
                if (!mvContext.isMatchedGroup(currentRootGroupId)) {
                    mvCandidateContexts.add(mvContext);
                }
            }
        } else {
            mvCandidateContexts.addAll(context.getCandidateMvs());
        }
        mvCandidateContexts.removeIf(x -> !x.prune(context, queryExpression));
        MaterializationContext.RewriteOrdering ordering =
                new MaterializationContext.RewriteOrdering(queryExpression, context.getColumnRefFactory());
        mvCandidateContexts.sort(ordering);
        int numCandidates = context.getSessionVariable().getCboMaterializedViewRewriteCandidateLimit();
        if (numCandidates > 0 && mvCandidateContexts.size() > numCandidates) {
            logMVRewrite(context, this, "too many MV candidates, truncate them to " + numCandidates);
            mvCandidateContexts = mvCandidateContexts.subList(0, numCandidates);
        }
        if (CollectionUtils.isEmpty(mvCandidateContexts)) {
            return Lists.newArrayList();
        }

        List<OptExpression> results = Lists.newArrayList();
        // Construct queryPredicateSplit to avoid creating multi times for multi MVs.
        // Compute Query queryPredicateSplit
        final ColumnRefFactory queryColumnRefFactory = context.getColumnRefFactory();
        final ReplaceColumnRefRewriter queryColumnRefRewriter =
                MvUtils.getReplaceColumnRefWriter(queryExpression, queryColumnRefFactory);

        List<ScalarOperator> onPredicates = MvUtils.collectOnPredicate(queryExpression);
        QueryMaterializationContext queryMaterializationContext = context.getQueryMaterializationContext();
        onPredicates = onPredicates.stream()
                .map(p -> MvUtils.canonizePredicateForRewrite(queryMaterializationContext, p))
                .collect(Collectors.toList());
        List<Table> queryTables = MvUtils.getAllTables(queryExpression);
        ConnectContext connectContext = ConnectContext.get();
        for (MaterializationContext mvContext : mvCandidateContexts) {
            context.checkTimeout();
            PredicateSplit queryPredicateSplit = getQuerySplitPredicate(context, mvContext, queryExpression,
                    queryColumnRefFactory, queryColumnRefRewriter);
            if (queryPredicateSplit == null) {
                continue;
            }
            MvRewriteContext mvRewriteContext = new MvRewriteContext(mvContext, queryTables, queryExpression,
                        queryColumnRefRewriter, queryPredicateSplit, onPredicates, this);

            // rewrite query
            MaterializedViewRewriter mvRewriter = getMaterializedViewRewrite(mvRewriteContext);
            OptExpression candidate = mvRewriter.rewrite();
            if (candidate == null) {
                continue;
            }

            candidate = postRewriteMV(context, mvRewriteContext, candidate);
            if (queryExpression.getGroupExpression() != null) {
                int currentRootGroupId = queryExpression.getGroupExpression().getGroup().getId();
                mvContext.addMatchedGroup(currentRootGroupId);
            }

            results.add(candidate);

            // update metrics
            mvContext.updateMVUsedCount();
            if (isUpdateMaterializedViewMetrics(connectContext)) {
                MaterializedViewMetricsEntity mvEntity =
                        MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mvContext.getMv().getMvId());
                mvEntity.increaseQueryMatchedCount(1L);
            }

            // Do not try to enumerate all plans, it would take a lot of time
            int limit = context.getSessionVariable().getCboMaterializedViewRewriteRuleOutputLimit();
            if (limit > 0 && results.size() >= limit) {
                logMVRewrite(context, this, "too many MV rewrite results generated, but limit to {}", limit);
                break;
            }

            // Give up rewrite if it exceeds the optimizer timeout
            context.checkTimeout();
        }

        return results;
    }

    /**
     * Return the query predicate split with/without compensate :
     * - with compensate    : deducing from the selected partition ids.
     * - without compensate : only get the partition predicate from pruned partitions of scan operator
     * eg: for sync mv without partition columns, we always no need compensate partition predicates because
     * mv and the base table are always synced.
     */
    private PredicateSplit getQuerySplitPredicate(OptimizerContext optimizerContext,
                                                  MaterializationContext mvContext,
                                                  OptExpression queryExpression,
                                                  ColumnRefFactory queryColumnRefFactory,
                                                  ReplaceColumnRefRewriter queryColumnRefRewriter) {
        // Cache partition predicate predicates because it's expensive time costing if there are too many materialized views or
        // query expressions are too complex.
        final ScalarOperator queryPartitionPredicate = MvUtils.compensatePartitionPredicate(mvContext,
                queryColumnRefFactory, queryExpression);
        if (queryPartitionPredicate == null) {
            logMVRewrite(mvContext.getOptimizerContext(), this, "Compensate query expression's partition " +
                    "predicates from pruned partitions failed.");
            return null;
        }
        // only add valid predicates into query split predicate
        Set<ScalarOperator> queryConjuncts = MvUtils.getAllValidPredicates(queryExpression);
        if (!ConstantOperator.TRUE.equals(queryPartitionPredicate)) {
            logMVRewrite(optimizerContext, this, "Query compensate partition predicate:{}",
                    queryPartitionPredicate);
            queryConjuncts.addAll(MvUtils.getAllValidPredicates(queryPartitionPredicate));
        }

        QueryMaterializationContext queryMaterializationContext = optimizerContext.getQueryMaterializationContext();
        Cache<Object, Object> predicateSplitCache = queryMaterializationContext.getMvQueryContextCache();
        Preconditions.checkArgument(predicateSplitCache != null);
        // Cache predicate split for predicates because it's time costing if there are too many materialized views.
        return queryMaterializationContext.getPredicateSplit(queryConjuncts, queryColumnRefRewriter);
    }

    /**
     * After plan is rewritten by MV, still do some actions for new MV's plan.
     * 1. column prune
     * 2. partition prune
     * 3. bucket prune
     */
    private OptExpression postRewriteMV(
            OptimizerContext optimizerContext, MvRewriteContext mvRewriteContext, OptExpression candidate) {
        if (candidate == null) {
            return null;
        }
        candidate = new MVColumnPruner().pruneColumns(candidate);
        candidate = new MVPartitionPruner(optimizerContext, mvRewriteContext).prunePartition(candidate);
        return candidate;
    }

    public MaterializedViewRewriter getMaterializedViewRewrite(MvRewriteContext mvContext) {
        return new MaterializedViewRewriter(mvContext);
    }
}
