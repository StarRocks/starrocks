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

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.profile.Tracers;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.BestMvSelector;
import com.starrocks.sql.optimizer.rule.transformation.materialization.IMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import com.starrocks.sql.optimizer.rule.transformation.materialization.compensation.MVCompensation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_UNION_REWRITE;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getQuerySplitPredicate;

public abstract class BaseMaterializedViewRewriteRule extends TransformationRule implements IMaterializedViewRewriteRule {

    protected BaseMaterializedViewRewriteRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    private boolean checkOlapScanWithoutTabletOrPartitionHints(OptExpression input) {
        Operator op = input.getOp();
        if (op instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scan = op.cast();
            if (scan.hasTableHints()) {
                return false;
            }
            // Avoid rewriting the query repeat, add a shortcut.
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
        // To avoid dead-loop rewrite, no rewrite when query extra predicate is not changed
        if (Utils.isOptHasAppliedRule(input, OP_MV_UNION_REWRITE)) {
            return false;
        }
        return !context.getCandidateMvs().isEmpty() && checkOlapScanWithoutTabletOrPartitionHints(input);
    }

    @Override
    public boolean exhausted(OptimizerContext context) {
        if (context.ruleExhausted(type())) {
            Tracers.log(Tracers.Module.MV, args -> String.format("[MV TRACE] RULE %s exhausted\n", this));
            return true;
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression queryExpression, OptimizerContext context) {
        try {
            List<OptExpression> expressions = doTransform(queryExpression, context);
            if (expressions == null || expressions.isEmpty()) {
                return Lists.newArrayList();
            }
            if (context.isInMemoPhase()) {
                return expressions;
            } else {
                // in rule phase, only return the best one result
                BestMvSelector bestMvSelector = new BestMvSelector(expressions, context, queryExpression, this);
                return Lists.newArrayList(bestMvSelector.selectBest());
            }
        } catch (Exception e) {
            String errMsg = ExceptionUtils.getStackTrace(e);
            // for mv rewrite rules, do not disturb query when exception.
            logMVRewrite(context, this, "mv rewrite exception, exception message:{}", errMsg);
            return Lists.newArrayList();
        }
    }

    @Override
    public List<MaterializationContext> doPrune(OptExpression queryExpression,
                                                OptimizerContext context,
                                                List<MaterializationContext> mvCandidateContexts) {
        mvCandidateContexts.removeIf(x -> !x.prune(context, queryExpression));
        return mvCandidateContexts;
    }

    public List<OptExpression> doTransform(OptExpression queryExpression, OptimizerContext context) {
        // 1. collect all candidate mvs for the input
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
        if (CollectionUtils.isEmpty(mvCandidateContexts)) {
            return Lists.newArrayList();
        }

        // 2. prune candidate mvs
        mvCandidateContexts = doPrune(queryExpression, context, mvCandidateContexts);
        // Order all candidate mvs by priority so can be rewritten fast.
        MaterializationContext.RewriteOrdering ordering =
                new MaterializationContext.RewriteOrdering(queryExpression, context.getColumnRefFactory());
        mvCandidateContexts.sort(ordering);
        int numCandidates = context.getSessionVariable().getCboMaterializedViewRewriteCandidateLimit();
        if (numCandidates > 0 && mvCandidateContexts.size() > numCandidates) {
            logMVRewrite(context, this, "too many MV candidates, truncate them to " + numCandidates);
            mvCandidateContexts = mvCandidateContexts.subList(0, numCandidates);
        }
        if (mvCandidateContexts.isEmpty()) {
            return Lists.newArrayList();
        }
        logMVRewrite(context, this, "MV Candidates: {}",
                mvCandidateContexts.stream().map(x -> x.getMv().getName()).collect(Collectors.toList()));

        // 3. do rewrite with associated mvs
        return doTransform(mvCandidateContexts, queryExpression, context);
    }

    /**
     * Transform/Rewrite the query with the given materialized view context.
     * @param mvCandidateContexts: pruned materialized view candidates.
     * @param queryExpression: query opt expression.
     * @param context: optimizer context.
     * @return: the rewritten query opt expression and associated materialization context pair if rewrite success, otherwise null.
     */
    protected List<OptExpression> doTransform(List<MaterializationContext> mvCandidateContexts,
                                              OptExpression queryExpression,
                                              OptimizerContext context) {
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
                .map(predicate -> queryColumnRefRewriter.rewrite(predicate))
                .collect(Collectors.toList());
        List<Table> queryTables = MvUtils.getAllTables(queryExpression);

        for (MaterializationContext mvContext : mvCandidateContexts) {
            // initialize query's compensate type based on query and mv's partition refresh status
            MVCompensation mvCompensation = mvContext.getOrInitMVCompensation(queryExpression);
            if (mvCompensation.getState().isNoRewrite()) {
                continue;
            }

            PredicateSplit queryPredicateSplit = getQuerySplitPredicate(context, mvContext, queryExpression,
                    queryColumnRefFactory, queryColumnRefRewriter, this);
            if (queryPredicateSplit == null) {
                continue;
            }
            MvRewriteContext mvRewriteContext = new MvRewriteContext(mvContext, queryTables, queryExpression,
                    queryColumnRefRewriter, queryPredicateSplit, onPredicates, this);

            IMaterializedViewRewriter mvRewriter = createRewriter(context, mvRewriteContext);
            if (mvRewriter == null) {
                logMVRewrite(mvRewriteContext, "create materialized view rewriter failed");
                continue;
            }

            // rewrite query
            OptExpression candidate = mvRewriter.doRewrite(mvRewriteContext);
            if (candidate == null) {
                logMVRewrite(mvRewriteContext, "doRewrite phase failed");
                continue;
            }

            candidate = mvRewriter.postRewrite(context, mvRewriteContext, candidate);
            if (candidate == null) {
                logMVRewrite(mvRewriteContext, "doPostAfterRewrite phase failed");
                continue;
            }

            if (queryExpression.getGroupExpression() != null) {
                int currentRootGroupId = queryExpression.getGroupExpression().getGroup().getId();
                mvContext.addMatchedGroup(currentRootGroupId);
            }

            // NOTE: derive logical property is necessary for statistics calculation.
            deriveLogicalProperty(candidate);

            results.add(candidate);
            mvContext.updateMVUsedCount();
            IMaterializedViewMetricsEntity mvEntity =
                    MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mvContext.getMv().getMvId());
            mvEntity.increaseQueryMatchedCount(1L);
            // mark: query has been rewritten by mv success.
            context.getQueryMaterializationContext().addRewrittenSuccessMVContext(mvContext);

            // Do not try to enumerate all plans, it would take a lot of time
            int limit = context.getSessionVariable().getCboMaterializedViewRewriteRuleOutputLimit();
            if (limit > 0 && results.size() >= limit) {
                logMVRewrite(mvRewriteContext, "too many MV rewrite results generated, but limit to {}", limit);
                break;
            }

            // mark this mv has applied this query
            MvUtils.getScanOperator(candidate)
                    .stream()
                    .forEach(op -> op.setOpAppliedMV(mvContext.getMv().getId()));

            // Give up rewrite if it exceeds the optimizer timeout
            context.checkTimeout();
        }
        return results;
    }

    /**
     * Create a materialized view rewriter to do the mv rewrite for the specific query opt expression.
     * @param mvContext: materialized view context of query and associated mv.
     * @return: the specific rewriter for the mv rewrite.
     */
    @Override
    public IMaterializedViewRewriter createRewriter(OptimizerContext optimizerContext,
                                                    MvRewriteContext mvContext) {
        return new MaterializedViewRewriter(mvContext);
    }
}
