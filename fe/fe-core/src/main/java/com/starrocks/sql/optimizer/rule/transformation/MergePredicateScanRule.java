// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
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
public class MergePredicateScanRule extends TransformationRule {
    public static final MergePredicateScanRule HIVE_SCAN =
            new MergePredicateScanRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final MergePredicateScanRule SCHEMA_SCAN =
            new MergePredicateScanRule(OperatorType.LOGICAL_SCHEMA_SCAN);
    public static final MergePredicateScanRule MYSQL_SCAN =
            new MergePredicateScanRule(OperatorType.LOGICAL_MYSQL_SCAN);

    public MergePredicateScanRule(OperatorType type) {
        super(RuleType.TF_MERGE_PREDICATE_SCAN, Pattern.create(OperatorType.LOGICAL_FILTER, type));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();

        OptExpression scan = input.getInputs().get(0);
        LogicalScanOperator lso = (LogicalScanOperator) scan.getOp();

        lso.setPredicate(Utils.compoundAnd(lfo.getPredicate(), lso.getPredicate()));

        Map<ColumnRefOperator, ScalarOperator> scanOutput =
                lso.getOutputColumns().stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
        LogicalProjectOperator lpo = new LogicalProjectOperator(scanOutput);
        OptExpression project = OptExpression.create(lpo, scan);

        return Lists.newArrayList(project);
    }
}
