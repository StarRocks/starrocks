// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PushDownPredicateWindowRule extends TransformationRule {
    public PushDownPredicateWindowRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_WINDOW,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_WINDOW, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOperator = (LogicalFilterOperator) input.getOp();
        List<ScalarOperator> filters = Utils.extractConjuncts(filterOperator.getPredicate());
        LogicalWindowOperator windowOperator = (LogicalWindowOperator) input.inputAt(0).getOp();

        /*
         * Only push down column contained by partition columns
         */
        ColumnRefSet partitionColumns = new ColumnRefSet();
        windowOperator.getPartitionExpressions().stream().map(ScalarOperator::getUsedColumns)
                .forEach(partitionColumns::union);

        List<ScalarOperator> pushDownPredicates = Lists.newArrayList();
        for (Iterator<ScalarOperator> iter = filters.iterator(); iter.hasNext(); ) {
            ScalarOperator filter = iter.next();
            if (partitionColumns.containsAll(filter.getUsedColumns())) {
                iter.remove();
                pushDownPredicates.add(filter);
            }
        }

        //Create a new expression to ensure that the newly generated windowExpr are copied correctly
        OptExpression windowExpr = OptExpression.create(input.inputAt(0).getOp(), input.inputAt(0).getInputs());
        if (pushDownPredicates.size() > 0) {
            LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates));
            OptExpression oe = new OptExpression(newFilter);
            oe.getInputs().addAll(windowExpr.getInputs());

            windowExpr.getInputs().clear();
            windowExpr.getInputs().add(oe);
        }

        if (filters.isEmpty()) {
            return Lists.newArrayList(windowExpr);
        } else if (filters.size() == Utils.extractConjuncts(filterOperator.getPredicate()).size()) {
            return Collections.emptyList();
        } else {
            filterOperator.setPredicate(Utils.compoundAnd(filters));
            input.setChild(0, windowExpr);
            return Lists.newArrayList(input);
        }
    }
}
