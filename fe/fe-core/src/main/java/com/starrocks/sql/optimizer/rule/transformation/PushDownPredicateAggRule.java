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
        Map<ColumnRefOperator, ScalarOperator> constantEquivalentPredicate = new HashMap<>();
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
                    constantEquivalentPredicate.put((ColumnRefOperator) scalar.getChild(0), scalar.getChild(1));
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

        // If there is a constant equivalent predicate, delete the column ref corresponding to
        // the constant in the aggregation node
        // (because the column ref must be a constant, the aggregation of constants has no meaning
        // unless the constant is the only grouping key)
        if (!constantEquivalentPredicate.isEmpty()) {
            Map<ColumnRefOperator, ScalarOperator> projection = new HashMap<>();

            List<ColumnRefOperator> groupingKeys = new ArrayList<>(logicalAggOperator.getGroupingKeys());
            for (ColumnRefOperator columnRefOperator : logicalAggOperator.getGroupingKeys()) {
                if (constantEquivalentPredicate.containsKey(columnRefOperator)) {
                    groupingKeys.remove(columnRefOperator);
                    // Because the constant column in grouping has been deleted,
                    // this constant map needs to be added to the project for reference by nodes above agg
                    projection.put(columnRefOperator, constantEquivalentPredicate.get(columnRefOperator));
                }
            }

            //Cannot delete all grouping keys because scalar aggregation and no-scalar aggregation are not equivalent
            if (groupingKeys.isEmpty() && !logicalAggOperator.getGroupingKeys().isEmpty()) {
                groupingKeys.add(logicalAggOperator.getGroupingKeys().get(0));
            }

            List<ColumnRefOperator> partitionByColumns = new ArrayList<>(logicalAggOperator.getPartitionByColumns());
            for (ColumnRefOperator columnRefOperator : logicalAggOperator.getPartitionByColumns()) {
                if (constantEquivalentPredicate.containsKey(columnRefOperator)) {
                    partitionByColumns.remove(columnRefOperator);
                }
            }
            if (partitionByColumns.isEmpty() && !logicalAggOperator.getPartitionByColumns().isEmpty()) {
                partitionByColumns.add(logicalAggOperator.getPartitionByColumns().get(0));
            }

            Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(constantEquivalentPredicate);
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
                    .setPartitionByColumns(partitionByColumns)
                    .setAggregations(aggregations)
                    .build(), input.getInputs());

            OptExpression projectOpt = OptExpression.create(new LogicalProjectOperator(projection), Lists.newArrayList(aggOpt));
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
