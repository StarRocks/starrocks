// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Filter            Project
 * |     --->        |
 * Scan           Scan(Predicate)
 */
public class MergePredicateRule extends TransformationRule {
    public static final MergePredicateRule HIVE_SCAN =
            new MergePredicateRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final MergePredicateRule SCHEMA_SCAN =
            new MergePredicateRule(OperatorType.LOGICAL_SCHEMA_SCAN);
    public static final MergePredicateRule MYSQL_SCAN =
            new MergePredicateRule(OperatorType.LOGICAL_MYSQL_SCAN);
    public static final MergePredicateRule REPEAT_NODE =
            new MergePredicateRule(OperatorType.LOGICAL_REPEAT);

    public MergePredicateRule(OperatorType type) {
        super(RuleType.TF_MERGE_PREDICATE_SCAN, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(type, OperatorType.PATTERN_MULTI_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();

        OptExpression optExpression = input.getInputs().get(0);
        Operator operator = optExpression.getOp();

        LogicalOperator newOperator = (LogicalOperator) operator;
        newOperator.setPredicate(Utils.compoundAnd(lfo.getPredicate(), newOperator.getPredicate()));

        if (newOperator instanceof LogicalScanOperator) {
            // Add project node upon scan node for column prune later
            LogicalScanOperator scanOperator = (LogicalScanOperator) newOperator;
            Map<ColumnRefOperator, ScalarOperator> scanOutput = scanOperator.getOutputColumns().stream()
                    .collect(Collectors.toMap(Function.identity(), Function.identity()));
            LogicalProjectOperator lpo = new LogicalProjectOperator(scanOutput);
            OptExpression project =
                    OptExpression.create(lpo, OptExpression.create(newOperator, optExpression.getInputs()));
            return Lists.newArrayList(project);
        } else {
            return Lists.newArrayList(OptExpression.create(newOperator, optExpression.getInputs()));
        }
    }
}
