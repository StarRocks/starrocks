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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/*
 * When a top-n operator follows after a 2 phase aggregation, and the top-n order by columns do not depend
 * on the aggregation results, the topN could be pushed down below the global aggregation.
 * In order to correctly compute the topN each local node has to fully aggregate the data, this means
 * streaming aggregations have to be set to force pre-aggregation mode.
 * In order to avoid introducing a local shuffle before pre-aggregations, the topN is computed separately for
 * each pipeline without a merge step.
 *
 * before:
 *           | cardinality: n
 *     TopN(Partial)
 *           |
 *      Agg(Global)
 *           |
 *        Exchange
 *           |
 *      Agg(Local)
 *
 * after:
 *           | cardinality: n
 *     TopN(Partial)
 *           |
 *      Agg(Global)
 *           |
 *        Exchange
 *           | cardinality: dop * n
 *     TopN(Partial) [without merge]
 *           |
 *      Agg(Local) [streaming_preaggregation_mode: "force_preaggregation"]
 **/
public class PushDownTopNToPreAggRule extends TransformationRule {

    private PushDownTopNToPreAggRule() {
        super(RuleType.TF_PUSH_DOWN_TOPN_AGG, Pattern.create(OperatorType.LOGICAL_TOPN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF))));
    }

    private static final PushDownTopNToPreAggRule INSTANCE = new PushDownTopNToPreAggRule();

    public static PushDownTopNToPreAggRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnablePreAggTopNPushDown()) {
            return false;
        }

        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();

        if (!topn.hasLimit() || topn.getLimit() > context.getSessionVariable().getCboPushDownTopNLimit()) {
            return false;
        }

        if (topn.getSortPhase() != SortPhase.PARTIAL || topn.hasOffset() || topn.getPredicate() != null) {
            return false;
        }

        if (topn.getPartitionByColumns() != null && !topn.getPartitionByColumns().isEmpty() ||
                topn.getPartitionPreAggCall() != null && !topn.getPartitionPreAggCall().isEmpty()) {
            return false;
        }

        OptExpression topnChild = input.inputAt(0);
        LogicalAggregationOperator aggGlobal = (LogicalAggregationOperator) topnChild.getOp();

        if (!aggGlobal.isSplit() || aggGlobal.getType() != AggType.GLOBAL || aggGlobal.getPredicate() != null) {
            return false;
        }

        OptExpression aggGlobalChild = topnChild.inputAt(0);
        LogicalAggregationOperator aggLocal = (LogicalAggregationOperator) aggGlobalChild.getOp();

        if (aggLocal.getType() != AggType.LOCAL || aggLocal.getPredicate() != null) {
            return false;
        }

        // verify aggregation result columns are not used in the order by columns of topN.
        List<Ordering> orderByElements = topn.getOrderByElements();
        List<ColumnRefOperator> groupingKeys = aggGlobal.getGroupingKeys();
        return orderByElements.stream().allMatch(orderByElement -> groupingKeys.contains(orderByElement.getColumnRef()));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();

        OptExpression agg = input.inputAt(0);
        LogicalAggregationOperator aggOp = (LogicalAggregationOperator) agg.getOp();

        OptExpression localAgg = agg.inputAt(0);
        LogicalAggregationOperator localAggOp = (LogicalAggregationOperator) localAgg.getOp();

        OptExpression newLocalAgg = OptExpression.create(new LogicalAggregationOperator.Builder()
                        .withOperator(localAggOp)
                        .setTopNLocalAgg(true)
                        .build(), localAgg.getInputs());

        OptExpression newLocalTopN = OptExpression.create(new LogicalTopNOperator.Builder()
                .setOrderByElements(topn.getOrderByElements())
                .setLimit(topn.getLimit())
                .setTopNType(topn.getTopNType())
                .setSortPhase(topn.getSortPhase())
                .setIsSplit(false)
                .setPerPipeline(true)
                .build(), newLocalAgg);

        OptExpression newAgg = OptExpression.create(aggOp, newLocalTopN);
        return Lists.newArrayList(OptExpression.create(topn, newAgg));
    }
}
