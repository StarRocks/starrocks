// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarRangePredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PushDownPredicateScanRule extends TransformationRule {
    public static final PushDownPredicateScanRule OLAP_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_OLAP_SCAN);
    public static final PushDownPredicateScanRule ES_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_ES_SCAN);

    public PushDownPredicateScanRule(OperatorType type) {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_SCAN, Pattern.create(OperatorType.LOGICAL_FILTER, type));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();

        OptExpression scan = input.getInputs().get(0);
        LogicalScanOperator loso = (LogicalScanOperator) scan.getOp();

        ScalarOperator predicates = Utils.compoundAnd(lfo.getPredicate(), loso.getPredicate());
        ScalarRangePredicateExtractor rangeExtractor = new ScalarRangePredicateExtractor();
        predicates = rangeExtractor.rewriteOnlyColumn(predicates);
        loso.setPredicate(predicates);
        loso.setColumnFilters(ColumnFilterConverter.convertColumnFilter(Utils.extractConjuncts(loso.getPredicate())));

        Map<ColumnRefOperator, ScalarOperator> scanOutput =
                loso.getOutputColumns().stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
        LogicalProjectOperator lpo = new LogicalProjectOperator(scanOutput);
        OptExpression project = OptExpression.create(lpo, scan);

        return Lists.newArrayList(project);
    }
}
