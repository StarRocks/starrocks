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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.LineageFactory;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVColumnPruner;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class BaseMaterializedViewRewriteRule extends TransformationRule {

    protected BaseMaterializedViewRewriteRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return !context.getCandidateMvs().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression queryExpression, OptimizerContext context) {
        List<OptExpression> results = Lists.newArrayList();
        ColumnRefFactory queryColumnRefFactory = context.getColumnRefFactory();

        // Construct queryPredicateSplit to avoid creating multi times for multi MVs.

        // Compute Query queryPredicateSplit
        Map<ColumnRefOperator, ScalarOperator> queryLineage =
                LineageFactory.getLineage(queryExpression, queryColumnRefFactory);
        ReplaceColumnRefRewriter queryColumnRefRewriter = new ReplaceColumnRefRewriter(queryLineage, true);
        List<ScalarOperator> queryConjuncts = MvUtils.getAllPredicates(queryExpression);
        ScalarOperator queryPredicate = null;
        if (!queryConjuncts.isEmpty()) {
            queryPredicate = Utils.compoundAnd(queryConjuncts);
            queryPredicate = queryColumnRefRewriter.rewrite(queryPredicate.clone());
        }
        // Compensate partition predicates and add them into query predicate.
        ScalarOperator queryPartitionPredicate =
                compensatePartitionPredicate(queryExpression, queryColumnRefFactory);
        if (queryPartitionPredicate == null) {
            return Lists.newArrayList();
        }
        if (!ConstantOperator.TRUE.equals(queryPartitionPredicate)) {
            queryPredicate = MvUtils.canonizePredicate(Utils.compoundAnd(queryPredicate, queryPartitionPredicate));
        }
        final PredicateSplit queryPredicateSplit = PredicateSplit.splitPredicate(queryPredicate);

        for (MaterializationContext mvContext : context.getCandidateMvs()) {
            mvContext.setQueryExpression(queryExpression);
            mvContext.setOptimizerContext(context);
            MaterializedViewRewriter rewriter = getMaterializedViewRewrite(mvContext);
            List<OptExpression> rewrittens = rewriter.rewrite(queryColumnRefRewriter,
                    queryPredicateSplit);
            if (rewrittens != null && !rewrittens.isEmpty()) {
                for (OptExpression rewritten : rewrittens) {
                    MVColumnPruner columnPruner = new MVColumnPruner();
                    OptExpression exprAfterPrune = columnPruner.pruneColumns(rewritten);
                    if (exprAfterPrune != null) {
                        results.add(exprAfterPrune);
                    }
                }
            }
        }

        return results;
    }

    public MaterializedViewRewriter getMaterializedViewRewrite(MaterializationContext mvContext) {
        return new MaterializedViewRewriter(mvContext);
    }

    private ScalarOperator compensatePartitionPredicate(OptExpression plan, ColumnRefFactory columnRefFactory) {
        List<LogicalOlapScanOperator> olapScanOperators = MvUtils.getOlapScanNode(plan);
        if (olapScanOperators.isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }
        List<ScalarOperator> partitionPredicates = Lists.newArrayList();
        for (LogicalOlapScanOperator olapScanOperator : olapScanOperators) {
            Preconditions.checkState(olapScanOperator.getTable().isNativeTable());
            OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
            if (olapScanOperator.getSelectedPartitionId() != null
                    && olapScanOperator.getSelectedPartitionId().size() == olapTable.getPartitions().size()) {
                continue;
            }

            if (olapTable.getPartitionInfo() instanceof ExpressionRangePartitionInfo) {
                ExpressionRangePartitionInfo partitionInfo = (ExpressionRangePartitionInfo) olapTable.getPartitionInfo();
                Expr partitionExpr = partitionInfo.getPartitionExprs().get(0);
                List<SlotRef> slotRefs = Lists.newArrayList();
                partitionExpr.collect(SlotRef.class, slotRefs);
                Preconditions.checkState(slotRefs.size() == 1);
                Optional<ColumnRefOperator> partitionColumn = olapScanOperator.getColRefToColumnMetaMap().keySet().stream()
                        .filter(columnRefOperator -> columnRefOperator.getName().equals(slotRefs.get(0).getColumnName()))
                        .findFirst();
                if (!partitionColumn.isPresent()) {
                    return null;
                }
                ExpressionMapping mapping = new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()));
                mapping.put(slotRefs.get(0), partitionColumn.get());
                ScalarOperator partitionScalarOperator =
                        SqlToScalarOperatorTranslator.translate(partitionExpr, mapping, columnRefFactory);
                List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
                for (long pid : olapScanOperator.getSelectedPartitionId()) {
                    selectedRanges.add(partitionInfo.getRange(pid));
                }
                List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
                List<ScalarOperator> rangePredicates = MvUtils.convertRanges(partitionScalarOperator, mergedRanges);
                ScalarOperator partitionPredicate = Utils.compoundOr(rangePredicates);
                partitionPredicates.add(partitionPredicate);
            } else if (olapTable.getPartitionInfo() instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                if (partitionColumns.size() != 1) {
                    // now do not support more than one partition columns
                    return null;
                }
                List<Range<PartitionKey>> selectedRanges = Lists.newArrayList();
                for (long pid : olapScanOperator.getSelectedPartitionId()) {
                    selectedRanges.add(rangePartitionInfo.getRange(pid));
                }
                List<Range<PartitionKey>> mergedRanges = MvUtils.mergeRanges(selectedRanges);
                ColumnRefOperator partitionColumnRef = olapScanOperator.getColumnReference(partitionColumns.get(0));
                List<ScalarOperator> rangePredicates = MvUtils.convertRanges(partitionColumnRef, mergedRanges);
                ScalarOperator partitionPredicate = Utils.compoundOr(rangePredicates);
                if (partitionPredicate != null) {
                    partitionPredicates.add(partitionPredicate);
                }
            } else {
                return null;
            }
        }
        return partitionPredicates.isEmpty() ?
                ConstantOperator.createBoolean(true) : Utils.compoundAnd(partitionPredicates);
    }
}
