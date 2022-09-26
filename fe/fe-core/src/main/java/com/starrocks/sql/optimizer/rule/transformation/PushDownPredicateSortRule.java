// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PushDownPredicateSortRule extends TransformationRule {
    public PushDownPredicateSortRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_SORT, Pattern.create(OperatorType.LOGICAL_FILTER).addChildren(
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator logicalFilterOperator = (LogicalFilterOperator) input.getOp();

        OptExpression sortOpt = input.getInputs().get(0);
        LogicalTopNOperator logicalTopNOperator = (LogicalTopNOperator) sortOpt.getOp();

        Map<ColumnRefOperator, ScalarOperator> projection = new HashMap<>();


        List<ScalarOperator> conjuncts = Utils.extractConjuncts(logicalFilterOperator.getPredicate());
        Map<ColumnRefOperator, ConstantOperator> equalConstant = new HashMap<>();
        for (ScalarOperator ss : conjuncts) {
            if (ss instanceof BinaryPredicateOperator
                    && ((BinaryPredicateOperator) ss).getBinaryType().isEqual()
                    && ss.getChild(0) instanceof ColumnRefOperator
                    && ss.getChild(1) instanceof ConstantOperator) {
                equalConstant.put((ColumnRefOperator) ss.getChild(0), (ConstantOperator) ss.getChild(1));
            }
        }

        List<Ordering> orderingList = new ArrayList<>();
        for (Ordering ordering : logicalTopNOperator.getOrderByElements()) {
            if (equalConstant.containsKey(ordering.getColumnRef())) {
                projection.put(ordering.getColumnRef(), equalConstant.get(ordering.getColumnRef()));
                continue;
            }
            orderingList.add(ordering);
            projection.put(ordering.getColumnRef(), ordering.getColumnRef());
        }

        if (orderingList.isEmpty()) {
            input.setChild(0, input.inputAt(0).inputAt(0));
            return Lists.newArrayList(input);
        }

        if (orderingList.size() == logicalTopNOperator.getOrderByElements().size()) {
            return Collections.emptyList();
        }

        OptExpression aggOpt = OptExpression.create(new LogicalTopNOperator.Builder()
                .withOperator(logicalTopNOperator)
                .setOrderByElements(orderingList)
                .build(), input.getInputs());
        return Lists.newArrayList(OptExpression.create(new LogicalProjectOperator(projection), Lists.newArrayList(aggOpt)));
    }
}
