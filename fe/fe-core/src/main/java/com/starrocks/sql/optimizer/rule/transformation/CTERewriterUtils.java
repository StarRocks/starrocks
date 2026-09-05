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
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for CTE rewrite operations.
 * Provides common methods for building CTE consume operators and aggregate nodes.
 */
public class CTERewriterUtils {

    /**
     * Build a CTE consume operator for the given required columns.
     *
     * @param cteProduce the CTE produce expression
     * @param requiredColumns the columns required by the CTE consume
     * @param factory the column reference factory
     * @return the CTE consume operator
     */
    public static LogicalCTEConsumeOperator buildCteConsume(OptExpression cteProduce,
                                                            ColumnRefSet requiredColumns,
                                                            ColumnRefFactory factory) {
        LogicalCTEProduceOperator produceOperator = (LogicalCTEProduceOperator) cteProduce.getOp();
        int cteId = produceOperator.getCteId();

        // create cte consume, cte output columns
        Map<ColumnRefOperator, ColumnRefOperator> consumeOutputMap = Maps.newHashMap();
        for (int columnId : requiredColumns.getColumnIds()) {
            ColumnRefOperator produceOutput = factory.getColumnRef(columnId);
            ColumnRefOperator consumeOutput =
                    factory.create(produceOutput, produceOutput.getType(), produceOutput.isNullable());
            consumeOutputMap.put(consumeOutput, produceOutput);
        }
        // If there is no requiredColumns, we need to add least one column which is smallest
        if (consumeOutputMap.isEmpty()) {
            List<ColumnRefOperator> outputColumns =
                    produceOperator.getOutputColumns(new ExpressionContext(cteProduce)).getStream().
                            map(factory::getColumnRef).collect(Collectors.toList());
            ColumnRefOperator smallestColumn = Utils.findSmallestColumnRef(outputColumns);
            ColumnRefOperator consumeOutput =
                    factory.create(smallestColumn, smallestColumn.getType(), smallestColumn.isNullable());
            consumeOutputMap.put(consumeOutput, smallestColumn);
        }

        return new LogicalCTEConsumeOperator(cteId, consumeOutputMap);
    }

    /*
     * create aggregate node with cte consume:
     *
     * Output:
     *      Agg[sum(v1), max(v1)]
     *          |
     *      CTEConsume
     */
    public static OptExpression buildOtherAggregateCTEConsume(List<ColumnRefOperator> otherAggregateRef,
                                                              LogicalAggregationOperator aggregate,
                                                              OptExpression cteProduce,
                                                              ColumnRefFactory factory,
                                                              Map<ColumnRefOperator, ScalarOperator> projectionMap) {
        ColumnRefSet allCteConsumeRequiredColumns = new ColumnRefSet();
        otherAggregateRef.stream().map(k -> aggregate.getAggregations().get(k).getUsedColumns())
                .forEach(allCteConsumeRequiredColumns::union);
        List<ColumnRefOperator> groupingKeys = aggregate.getGroupingKeys();
        allCteConsumeRequiredColumns.union(groupingKeys);

        LogicalCTEConsumeOperator cteConsume = buildCteConsume(cteProduce, allCteConsumeRequiredColumns, factory);
        // rewrite aggregate
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();
        cteConsume.getCteOutputColumnRefMap().forEach((k, v) -> rewriteMap.put(v, k));
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        Map<ColumnRefOperator, CallOperator> aggregateFn = Maps.newHashMap();
        for (ColumnRefOperator otherRef : otherAggregateRef) {
            CallOperator otherAggFn = (CallOperator) rewriter.rewrite(aggregate.getAggregations().get(otherRef));
            aggregateFn.put(otherRef, otherAggFn);
            projectionMap.put(otherRef, otherRef);
        }
        List<ColumnRefOperator> rewriteGroupingKeys = groupingKeys.stream().
                map(column -> (ColumnRefOperator) rewriter.rewrite(column)).collect(
                        Collectors.toList());

        LogicalAggregationOperator newAggregate = new LogicalAggregationOperator.Builder().withOperator(aggregate).
                setAggregations(aggregateFn).setGroupingKeys(rewriteGroupingKeys).
                setPartitionByColumns(rewriteGroupingKeys).setPredicate(null).
                setLimit(Operator.DEFAULT_LIMIT).build();
        return OptExpression.create(newAggregate, OptExpression.create(cteConsume));
    }

    /**
     * Get the join on predicate columns from an operator.
     * For aggregation operator, returns grouping keys.
     * For join operator, extracts columns from the on predicate.
     *
     * @param operator the operator (either LogicalAggregationOperator or LogicalJoinOperator)
     * @return list of column references used in join predicate
     */
    public static List<ColumnRefOperator> getJoinOnPredicateColumn(Operator operator) {
        List<ColumnRefOperator> onPredicateColumns;
        if (operator instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) operator;
            onPredicateColumns = aggregationOperator.getGroupingKeys();
        } else {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            onPredicateColumns = Utils.extractConjuncts(joinOperator.getOnPredicate()).stream().
                    map(predicate -> (ColumnRefOperator) predicate.getChild(0)).collect(Collectors.toList());
        }
        return onPredicateColumns;
    }

    /**
     * Build a join expression between left and right expressions.
     * The join predicate is automatically extracted from the grouping keys of aggregation operators
     * or from the on predicate of join operators.
     *
     * @param left the left expression
     * @param right the right expression
     * @param joinType the type of join (e.g., LEFT_OUTER_JOIN, INNER_JOIN)
     * @return the join expression
     */
    public static OptExpression buildJoin(OptExpression left, OptExpression right, JoinOperator joinType) {
        // Get on predicate columns from left and right children.
        List<ColumnRefOperator> onPredicateLeftColumns = getJoinOnPredicateColumn(left.getOp());
        List<ColumnRefOperator> onPredicateRightColumns = getJoinOnPredicateColumn(right.getOp());
        Preconditions.checkState(onPredicateLeftColumns.size() == onPredicateRightColumns.size());

        List<ScalarOperator> onPredicateList = Lists.newArrayList();
        for (int index = 0; index < onPredicateLeftColumns.size(); ++index) {
            // Build on predicate for the join.
            ScalarOperator onPredicate = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                    onPredicateLeftColumns.get(index), onPredicateRightColumns.get(index));
            onPredicateList.add(onPredicate);
        }
        return OptExpression.create(new LogicalJoinOperator(joinType,
                Utils.compoundAnd(onPredicateList), HintNode.HINT_JOIN_UNREORDER), left, right);
    }
}

