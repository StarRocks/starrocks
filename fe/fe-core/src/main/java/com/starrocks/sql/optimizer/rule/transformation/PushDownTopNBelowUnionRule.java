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

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PushDownTopNBelowUnionRule extends TransformationRule {
    public PushDownTopNBelowUnionRule() {
        super(RuleType.TF_PUSH_DOWN_TOPN_UNION,
                Pattern.create(OperatorType.LOGICAL_TOPN).addChildren(
                        Pattern.create(OperatorType.LOGICAL_UNION, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = (LogicalTopNOperator) input.getOp();

        if (!topn.hasLimit() || topn.getLimit() > context.getSessionVariable().getCboPushDownTopNLimit()) {
            return false;
        }

        if (topn.hasOffset()) {
            return false;
        }

        OptExpression childExpr = input.inputAt(0);
        LogicalUnionOperator unionOperator = childExpr.getOp().cast();
        if (!unionOperator.isUnionAll()) {
            return false;
        }

        Set<Integer> unionAllOutputColumnRefIds = unionOperator.getOutputColumnRefOp().stream()
                .map(colRef -> colRef.getId()).collect(Collectors.toSet());
        if (topn.getOrderByElements().stream().anyMatch(colRef ->
                !unionAllOutputColumnRefIds.contains(colRef.getColumnRef().getId()))) {
            return false;
        }

        List<OptExpression> unionChildren = childExpr.getInputs();
        if (unionChildren.stream().noneMatch(unionChild -> canPushDownTopNBelowUnionChild(topn, unionChild))) {
            return false;
        }
        return true;
    }

    private boolean canPushDownTopNBelowUnionChild(LogicalTopNOperator topn,
                                                   OptExpression unionChild) {
        // if union child has limit and is less than the topn's limit, should not push down the topn's limit.
        if (topn.hasLimit() && unionChild.getOp().hasLimit() && topn.getLimit() >= unionChild.getOp().getLimit()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topn = input.getOp().cast();
        OptExpression childExpr = input.inputAt(0);
        LogicalUnionOperator unionOperator = childExpr.getOp().cast();
        List<OptExpression> newUnionChildren = Lists.newArrayList();
        List<ColumnRefOperator> unionAllOutputColumnRefs = unionOperator.getOutputColumnRefOp();
        Map<Integer, Integer> unionAllOutputColumnRefIdToIndexMap = Maps.newHashMap();
        for (int i = 0; i < unionAllOutputColumnRefs.size(); i++) {
            unionAllOutputColumnRefIdToIndexMap.put(unionAllOutputColumnRefs.get(i).getId(), i);
        }
        List<Integer> orderByIndexes = topn.getOrderByElements().stream()
                .map(x -> unionAllOutputColumnRefIdToIndexMap.get(x.getColumnRef().getId()))
                .collect(Collectors.toList());
        List<List<ColumnRefOperator>> unionChildrenOutputColumns = unionOperator.getChildOutputColumns();
        List<Ordering> oldOrderings = topn.getOrderByElements();
        Preconditions.checkState(oldOrderings.size() == orderByIndexes.size());
        for (int i = 0; i < childExpr.getInputs().size(); i++) {
            OptExpression unionChild = childExpr.inputAt(i);
            if (canPushDownTopNBelowUnionChild(topn, unionChild)) {
                List<ColumnRefOperator> unionChildOutputColRefs = unionChildrenOutputColumns.get(i);
                List<Ordering> newOrderings =
                        buildUnionChildOrderings(oldOrderings, orderByIndexes, unionChildOutputColRefs);
                OptExpression newTopNOperator = OptExpression.create(new LogicalTopNOperator.Builder()
                        .setOrderByElements(newOrderings)
                        .setLimit(topn.getLimit())
                        .setTopNType(topn.getTopNType())
                        .setSortPhase(topn.getSortPhase())
                        .setIsSplit(false)
                        .build(), unionChild);
                newUnionChildren.add(newTopNOperator);
            } else {
                newUnionChildren.add(unionChild);

            }
        }
        OptExpression newUnionOperator = OptExpression.create(unionOperator, newUnionChildren);
        return Collections.singletonList(OptExpression.create(topn, newUnionOperator));
    }

    /**
     * @param oldOrderings              : old orderings before pushed down
     * @param orderByIndexes            : ordering keys' indexes in the output columns
     * @param unionChildOutputColRefs   : the output column refs of the ith union child
     * @return  : construct new orderings for the ith union child.
     */
    private List<Ordering> buildUnionChildOrderings(List<Ordering> oldOrderings,
                                                    List<Integer> orderByIndexes,
                                                    List<ColumnRefOperator> unionChildOutputColRefs) {
        List<Ordering> newOrderings = Lists.newArrayList();
        for (int j = 0; j < orderByIndexes.size(); j++) {
            int orderColIdx = orderByIndexes.get(j);
            Preconditions.checkState(orderColIdx < unionChildOutputColRefs.size());
            ColumnRefOperator orderColumnRef = unionChildOutputColRefs.get(orderColIdx);
            Ordering oldOrdering = oldOrderings.get(j);
            Ordering newOrdering = new Ordering(orderColumnRef, oldOrdering.isAscending(), oldOrdering.isNullsFirst());
            newOrderings.add(newOrdering);
        }
        return newOrderings;
    }
}
