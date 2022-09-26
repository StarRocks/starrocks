// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PushDownPredicateAggRule extends TransformationRule {
    public PushDownPredicateAggRule() {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_AGG,
                Pattern.create(OperatorType.LOGICAL_FILTER).
                        addChildren(Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator logicalFilterOperator = (LogicalFilterOperator) input.getOp();
        List<ScalarOperator> filters = Utils.extractConjuncts(logicalFilterOperator.getPredicate());

        LogicalAggregationOperator logicalAggOperator = (LogicalAggregationOperator) input.inputAt(0).getOp();
        List<ColumnRefOperator> groupColumns = logicalAggOperator.getGroupingKeys();

        List<ScalarOperator> pushDownPredicates = Lists.newArrayList();

        Map<ColumnRefOperator, ScalarOperator> columnRefToConstant = new HashMap<>();
        for (Iterator<ScalarOperator> iter = filters.iterator(); iter.hasNext(); ) {
            ScalarOperator scalar = iter.next();
            List<ColumnRefOperator> columns = Utils.extractColumnRef(scalar);

            // push down predicate
            if (groupColumns.containsAll(columns) && !columns.isEmpty()) {
                // remove from filter
                iter.remove();
                // and to predicates
                pushDownPredicates.add(scalar);

                if (scalar instanceof BinaryPredicateOperator
                        && ((BinaryPredicateOperator) scalar).getBinaryType().isEqual()
                        && scalar.getChild(0) instanceof ColumnRefOperator
                        && scalar.getChild(1) instanceof ConstantOperator) {
                    columnRefToConstant.put((ColumnRefOperator) scalar.getChild(0), scalar.getChild(1));
                }
            }
        }

        // merge filter
        filters.add(logicalAggOperator.getPredicate());
        ScalarOperator scalarOperator = Utils.compoundAnd(filters);
        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        // The calculation of the null value is in the constant fold
        if (scalarOperator != null) {
            scalarOperator = scalarRewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        }

        //常量重写
        if (!columnRefToConstant.isEmpty()) {
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(columnRefToConstant);

            Map<ColumnRefOperator, ScalarOperator> projection = new HashMap<>();

            List<ColumnRefOperator> groupingKeys = new ArrayList<>();
            for (ColumnRefOperator columnRefOperator : logicalAggOperator.getGroupingKeys()) {
                if (columnRefToConstant.containsKey(columnRefOperator)) {
                    projection.put(columnRefOperator, columnRefToConstant.get(columnRefOperator));
                    continue;
                }
                groupingKeys.add(columnRefOperator);
            }

            if (groupingKeys.isEmpty() && !logicalAggOperator.getGroupingKeys().isEmpty()) {
                groupingKeys.add(logicalAggOperator.getGroupingKeys().get(0));
            }

            Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : logicalAggOperator.getAggregations().entrySet()) {
                aggregations.put(entry.getKey(), (CallOperator) rewriter.rewrite(entry.getValue()));
            }

            for (ColumnRefOperator columnRefOperator : groupingKeys) {
                projection.put(columnRefOperator, columnRefOperator);
            }
            for (ColumnRefOperator columnRefOperator : aggregations.keySet()) {
                projection.put(columnRefOperator, columnRefOperator);
            }

            OptExpression aggOpt = OptExpression.create(new LogicalAggregationOperator.Builder()
                    .withOperator(logicalAggOperator)
                    .setGroupingKeys(groupingKeys)
                    .setAggregations(aggregations)
                    .build(), input.getInputs());

            OptExpression projectOpt =
                    OptExpression.create(new LogicalProjectOperator(projection), Lists.newArrayList(aggOpt));

            if (pushDownPredicates.size() > 0) {
                LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates));
                OptExpression oe = new OptExpression(newFilter);
                oe.getInputs().addAll(input.inputAt(0).getInputs());
                aggOpt.setChild(0, oe);
            }
            return Lists.newArrayList(projectOpt);
        } else {
            input.setChild(0, OptExpression.create(new LogicalAggregationOperator.Builder()
                            .withOperator(logicalAggOperator)
                            .setPredicate(scalarOperator)
                            .build(),
                    input.inputAt(0).getInputs()));

            // push down
            if (pushDownPredicates.size() > 0) {
                LogicalFilterOperator newFilter = new LogicalFilterOperator(Utils.compoundAnd(pushDownPredicates));
                OptExpression oe = new OptExpression(newFilter);
                oe.getInputs().addAll(input.inputAt(0).getInputs());

                input.inputAt(0).getInputs().clear();
                input.inputAt(0).getInputs().add(oe);
            }

            return Lists.newArrayList(input.inputAt(0));
        }
    }
}
