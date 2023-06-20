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
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
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

import java.util.List;
import java.util.stream.Collectors;

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
            mvCandidateContexts = context.getCandidateMvs();
        }

        List<OptExpression> results = Lists.newArrayList();

        // Construct queryPredicateSplit to avoid creating multi times for multi MVs.
        // Compute Query queryPredicateSplit
        final ColumnRefFactory queryColumnRefFactory = context.getColumnRefFactory();
        final ReplaceColumnRefRewriter queryColumnRefRewriter =
                MvUtils.getReplaceColumnRefWriter(queryExpression, queryColumnRefFactory);
        // Compensate partition predicates and add them into query predicate.
        final ScalarOperator queryPartitionPredicate =
                MvUtils.compensatePartitionPredicate(queryExpression, queryColumnRefFactory);
        if (queryPartitionPredicate == null) {
            logMVRewrite(context, this, "Query partition compensate from partition prune failed.");
            return Lists.newArrayList();
        }
        ScalarOperator queryPredicate = MvUtils.rewriteOptExprCompoundPredicate(queryExpression, queryColumnRefRewriter);
        if (!ConstantOperator.TRUE.equals(queryPartitionPredicate)) {
            queryPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(queryPredicate, queryPartitionPredicate));
        }
        final PredicateSplit queryPredicateSplit = PredicateSplit.splitPredicate(queryPredicate);
        List<ScalarOperator> onPredicates = collectOnPredicate(queryExpression);
        onPredicates = onPredicates.stream().map(MvUtils::canonizePredicate).collect(Collectors.toList());
        List<Table> queryTables = MvUtils.getAllTables(queryExpression);
        for (MaterializationContext mvContext : mvCandidateContexts) {
            MvRewriteContext mvRewriteContext = new MvRewriteContext(mvContext, queryTables, queryExpression,
                    queryColumnRefRewriter, queryPredicateSplit, onPredicates, this);
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
            mvContext.updateMVUsedCount();
        }
        logMVRewrite(context, this, "Generate %d candidate plans after this rule rewrite", results.size());

        return results;
    }

    private List<ScalarOperator> collectOnPredicate(OptExpression optExpression) {
        List<ScalarOperator> onPredicates = Lists.newArrayList();
        collectOnPredicate(optExpression, onPredicates);
        return onPredicates;
    }

    private void collectOnPredicate(OptExpression optExpression, List<ScalarOperator> onPredicates) {
        if (optExpression.getOp() instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = optExpression.getOp().cast();
            onPredicates.addAll(Utils.extractConjuncts(joinOperator.getOnPredicate()));
        } else {
            for (OptExpression child : optExpression.getInputs()) {
                collectOnPredicate(child, onPredicates);
            }
        }
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
