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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/*
 * For ranking window functions, such as row_number, rank, dense_rank, if there exists rank related predicate
 * then we can add a TopN to filter data in order to reduce the amount of data to be exchanged and sorted
 * E.g.
 *      select * from (
 *          select *, rank() over (order by v2) as rk from t0
 *      ) sub_t0
 *      where rk < 4;
 * Before:
 *       Filter
 *         |
 *       Window
 *
 * After:
 *       Filter
 *         |
 *       Window
 *         |
 *       TopN
 */
public class PushDownPredicateRankingWindowRule extends TransformationRule {

    public PushDownPredicateRankingWindowRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_RANKING_WINDOW,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_WINDOW, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // This rule introduce a new version of TopNOperator, i.e. PartitionTopNOperator
        // which only supported in pipeline engine, so we cannot apply this rule in non-pipeline engine
        if (!context.getSessionVariable().isEnablePipelineEngine()) {
            return false;
        }

        OptExpression childExpr = input.inputAt(0);
        LogicalWindowOperator firstWindowOperator = childExpr.getOp().cast();

        // in window tranform phase, if two window op in same sort group and have same partition exps, then rank-related window will be
        // put on the top of the another window op
        if (!isSingleRankRelatedAnalyticOperator(firstWindowOperator)) {
            return false;
        }

        // only one window op which is SingleRankRelatedAnalyticOperator
        if (!(childExpr.inputAt(0).getOp() instanceof LogicalWindowOperator)) {
            return true;
        }

        LogicalWindowOperator secondWindowOperator = childExpr.inputAt(0).getOp().cast();
        if (!(samePartitionWithRankRelatedWindow(secondWindowOperator, firstWindowOperator))) {
            // There might be a negative gain if we add a partitionTopN between two window operators that
            // share the same sort group, because extra enforcer will add
            return !Objects.equals(firstWindowOperator.getEnforceSortColumns(),
                    secondWindowOperator.getEnforceSortColumns());
        }

        if (childExpr.inputAt(0).inputAt(0) != null &&
                childExpr.inputAt(0).inputAt(0).getOp() instanceof LogicalWindowOperator) {
            LogicalWindowOperator thirdWindowOperator = childExpr.inputAt(0).inputAt(0).getOp().cast();
            // There might be a negative gain if we add a partitionTopN between two window operators that
            // share the same sort group, because extra enforcer will add
            return !Objects.equals(secondWindowOperator.getEnforceSortColumns(),
                    thirdWindowOperator.getEnforceSortColumns());
        }

        return true;
    }

    private boolean isSingleRankRelatedAnalyticOperator(LogicalWindowOperator operator) {
        if (operator.getWindowCall().size() != 1) {
            return false;
        }

        ColumnRefOperator windowCol = Lists.newArrayList(operator.getWindowCall().keySet()).get(0);
        CallOperator callOperator = operator.getWindowCall().get(windowCol);

        return FunctionSet.ROW_NUMBER.equals(callOperator.getFnName()) ||
                FunctionSet.RANK.equals(callOperator.getFnName());
    }

    // actually if operator's window partition columns is subset of rankRelatedOperator's window partition columns and operator's window
    // functions are all rollup agg function, we also can support this optimization. But this means rankRelatedOperator's partition columns
    // maybe has high cardinality which is bad case, so we only support same partition columns case.
    private boolean samePartitionWithRankRelatedWindow(LogicalWindowOperator operator,
                                                       LogicalWindowOperator rankRelatedOperator) {
        // same partition exprs
        if (!operator.getPartitionExpressions().containsAll(rankRelatedOperator.getPartitionExpressions()) ||
                !rankRelatedOperator.getPartitionExpressions().containsAll(operator.getPartitionExpressions())) {
            return false;
        }

        // if operator has window clause, we can't support this optimization.
        if (operator.getAnalyticWindow() != null) {
            return false;
        }

        // if operator has order by elements, we can't support this optimization.
        // because order by without window clause will have "Range between UNBOUNDED_PRECEDING and CURRENT_ROW"
        if (operator.getOrderByElements() != null && !operator.getOrderByElements().isEmpty()) {
            return false;
        }

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : operator.getWindowCall().entrySet()) {
            CallOperator callOperator = entry.getValue();
            AggregateFunction function = (AggregateFunction) callOperator.getFunction();
            // if any function is only window function, we can't support this optimization.
            if (!function.isAggregateFn()) {
                return false;
            }
        }

        // same partition and without order by, so they are in same sort group and partition group, which means share the same sort Node and exchange Node
        Preconditions.checkState(operator.getEnforceSortColumns().equals(rankRelatedOperator.getEnforceSortColumns()));
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOperator = input.getOp().cast();
        List<ScalarOperator> filters = Utils.extractConjuncts(filterOperator.getPredicate());
        OptExpression childOptExpr = input.inputAt(0);
        LogicalWindowOperator rankRelatedWindowOperator = childOptExpr.getOp().cast();

        ColumnRefOperator windowCol = Lists.newArrayList(rankRelatedWindowOperator.getWindowCall().keySet()).get(0);
        CallOperator callOperator = rankRelatedWindowOperator.getWindowCall().get(windowCol);

        List<BinaryPredicateOperator> lessPredicates =
                filters.stream().filter(op -> op instanceof BinaryPredicateOperator)
                        .map(ScalarOperator::<BinaryPredicateOperator>cast)
                        .filter(op -> Objects.equals(BinaryType.LE, op.getBinaryType()) ||
                                Objects.equals(BinaryType.LT, op.getBinaryType()) ||
                                Objects.equals(BinaryType.EQ, op.getBinaryType()))
                        .filter(op -> Objects.equals(windowCol, op.getChild(0)))
                        .filter(op -> op.getChild(1) instanceof ConstantOperator)
                        .collect(Collectors.toList());

        if (lessPredicates.size() != 1) {
            return Collections.emptyList();
        }

        BinaryPredicateOperator lessPredicate = lessPredicates.get(0);
        ConstantOperator rightChild = lessPredicate.getChild(1).cast();
        long limitValue = rightChild.getBigint();

        List<ColumnRefOperator> partitionByColumns = rankRelatedWindowOperator.getPartitionExpressions().stream()
                .map(ScalarOperator::<ColumnRefOperator>cast)
                .collect(Collectors.toList());

        TopNType topNType = TopNType.parse(callOperator.getFnName());

        // If partition by columns is not empty, then we cannot derive sort property from the SortNode
        // OutputPropertyDeriver will generate PhysicalPropertySet.EMPTY if sortPhase is SortPhase.PARTIAL
        final SortPhase sortPhase = partitionByColumns.isEmpty() ? SortPhase.FINAL : SortPhase.PARTIAL;
        final long limit = partitionByColumns.isEmpty() ? limitValue : Operator.DEFAULT_LIMIT;
        final long partitionLimit = partitionByColumns.isEmpty() ? Operator.DEFAULT_LIMIT : limitValue;

        LogicalTopNOperator.Builder topNBuilder = new LogicalTopNOperator.Builder()
                .setPartitionByColumns(partitionByColumns)
                .setPartitionLimit(partitionLimit)
                .setOrderByElements(rankRelatedWindowOperator.getEnforceSortColumns())
                .setLimit(limit)
                .setTopNType(topNType)
                .setSortPhase(sortPhase);

        if ((childOptExpr.inputAt(0).getOp() instanceof LogicalWindowOperator)) {
            LogicalWindowOperator secondWindowOperator = childOptExpr.inputAt(0).getOp().cast();
            if (samePartitionWithRankRelatedWindow(secondWindowOperator, rankRelatedWindowOperator)) {
                topNBuilder.setPartitionPreAggCall(createNormalAgg(false, rankRelatedWindowOperator.getWindowCall()));
                Map<ColumnRefOperator, CallOperator> newRankRelatedWindowCall =
                        createNormalAgg(true, rankRelatedWindowOperator.getWindowCall());
                // change rankRelated window call's input column and mark this window op's input is binary
                rankRelatedWindowOperator = new LogicalWindowOperator.Builder().withOperator(rankRelatedWindowOperator)
                        .setWindowCall(newRankRelatedWindowCall)
                        .setInputIsBinary(true)
                        .build();
            }
        }

        OptExpression newTopNOptExp = OptExpression.create(topNBuilder.build(), childOptExpr.getInputs());

        OptExpression newWindowOptExp = OptExpression.create(rankRelatedWindowOperator, newTopNOptExp);
        return Collections.singletonList(OptExpression.create(filterOperator, newWindowOptExp));
    }

    public Map<ColumnRefOperator, CallOperator> createNormalAgg(boolean isGlobal,
                                                                Map<ColumnRefOperator, CallOperator> aggregationMap) {
        Map<ColumnRefOperator, CallOperator> newAggregationMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationMap.entrySet()) {
            ColumnRefOperator column = entry.getKey();
            CallOperator aggregation = entry.getValue();

            CallOperator callOperator;
            Type intermediateType = getIntermediateType(aggregation);
            // For merge agg function, we need to replace the agg input args to the update agg function result
            if (isGlobal) {
                List<ScalarOperator> arguments = Lists.newArrayList(
                        new ColumnRefOperator(column.getId(), intermediateType, column.getName(), column.isNullable()));
                appendConstantColumns(arguments, aggregation);
                callOperator = new CallOperator(aggregation.getFnName(), aggregation.getType(), arguments,
                        aggregation.getFunction());
            } else {
                callOperator = new CallOperator(aggregation.getFnName(), intermediateType, aggregation.getChildren(),
                        aggregation.getFunction(), aggregation.isDistinct(), aggregation.isRemovedDistinct());
            }

            newAggregationMap.put(
                    new ColumnRefOperator(column.getId(), column.getType(), column.getName(), column.isNullable()),
                    callOperator);
        }

        return newAggregationMap;
    }

    protected Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
    }

    protected static void appendConstantColumns(List<ScalarOperator> arguments, CallOperator aggregation) {
        if (aggregation.getChildren().size() > 1) {
            aggregation.getChildren().stream().filter(ScalarOperator::isConstantRef).forEach(arguments::add);
        }
    }
}
