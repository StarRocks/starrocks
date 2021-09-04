// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Iterator;
import java.util.List;

public class PushDownPredicateAggRule extends TransformationRule {
    public PushDownPredicateAggRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_AGG,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();
        List<ScalarOperator> filters = Utils.extractConjuncts(lfo.getPredicate());

        OptExpression aggOe = input.getInputs().get(0);
        LogicalAggregationOperator lao = (LogicalAggregationOperator) aggOe.getOp();
        List<ColumnRefOperator> groupColumns = lao.getGroupingKeys();

        List<ScalarOperator> pushDownPredicates = Lists.newArrayList();

        for (Iterator<ScalarOperator> iter = filters.iterator(); iter.hasNext(); ) {
            ScalarOperator scalar = iter.next();
            List<ColumnRefOperator> columns = Utils.extractColumnRef(scalar);

            // push down predicate
            if (groupColumns.containsAll(columns) && !columns.isEmpty()) {
                // remove from filter
                iter.remove();
                // and to predicates
                pushDownPredicates.add(scalar);
            }
        }

        // merge filter
        filters.add(lao.getPredicate());
        lao.setPredicate(Utils.compoundAnd(filters));

        // push down
        if (pushDownPredicates.size() > 0) {
            LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates));
            OptExpression oe = new OptExpression(newFilter);
            oe.getInputs().addAll(aggOe.getInputs());

            aggOe.getInputs().clear();
            aggOe.getInputs().add(oe);
        }

        return Lists.newArrayList(aggOe);
    }

}
