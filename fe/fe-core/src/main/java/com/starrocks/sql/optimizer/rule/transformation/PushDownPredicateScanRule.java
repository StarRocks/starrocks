// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarRangePredicateExtractor;
import com.starrocks.sql.optimizer.rewrite.scalar.PruneTediousPredicateRule;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedCaseWhenRule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PushDownPredicateScanRule extends TransformationRule {
    public static final PushDownPredicateScanRule OLAP_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_OLAP_SCAN);
    public static final PushDownPredicateScanRule HIVE_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final PushDownPredicateScanRule ICEBERG_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    public static final PushDownPredicateScanRule HUDI_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_HUDI_SCAN);
    public static final PushDownPredicateScanRule DELTALAKE_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_DELTALAKE_SCAN);
    public static final PushDownPredicateScanRule FILE_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_FILE_SCAN);
    public static final PushDownPredicateScanRule SCHEMA_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_SCHEMA_SCAN);
    public static final PushDownPredicateScanRule MYSQL_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_MYSQL_SCAN);
    public static final PushDownPredicateScanRule ES_SCAN = new PushDownPredicateScanRule(OperatorType.LOGICAL_ES_SCAN);
    public static final PushDownPredicateScanRule META_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_META_SCAN);
    public static final PushDownPredicateScanRule JDBC_SCAN =
            new PushDownPredicateScanRule(OperatorType.LOGICAL_JDBC_SCAN);

    public PushDownPredicateScanRule(OperatorType type) {
        super(RuleType.TF_PUSH_DOWN_PREDICATE_SCAN, Pattern.create(OperatorType.LOGICAL_FILTER, type));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator lfo = (LogicalFilterOperator) input.getOp();

        OptExpression scan = input.getInputs().get(0);
        LogicalScanOperator logicalScanOperator = (LogicalScanOperator) scan.getOp();

        ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
        ScalarOperator predicates = Utils.compoundAnd(lfo.getPredicate(), logicalScanOperator.getPredicate());
        // simplify case-when
        if (context.getSessionVariable().isEnableSimplifyCaseWhen()) {
            predicates = scalarOperatorRewriter.rewrite(predicates, Lists.newArrayList(
                    SimplifiedCaseWhenRule.INSTANCE,
                    PruneTediousPredicateRule.INSTANCE
            ));
        }

        ScalarRangePredicateExtractor rangeExtractor = new ScalarRangePredicateExtractor();
        predicates = rangeExtractor.rewriteOnlyColumn(Utils.compoundAnd(Utils.extractConjuncts(predicates)
                .stream().map(rangeExtractor::rewriteOnlyColumn).collect(Collectors.toList())));
        Preconditions.checkState(predicates != null);

<<<<<<< HEAD
        // simplify case-when
        predicates = scalarOperatorRewriter.rewrite(predicates, Lists.newArrayList(
                SimplifiedCaseWhenRule.INSTANCE,
                PruneTediousPredicateRule.INSTANCE
        ));
=======
>>>>>>> 2.5.18
        predicates = scalarOperatorRewriter.rewrite(predicates,
                ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
        predicates = Utils.transTrue2Null(predicates);

        // clone a new scan operator and rewrite predicate.
        Operator.Builder builder = OperatorBuilderFactory.build(logicalScanOperator);
        LogicalScanOperator newScanOperator = (LogicalScanOperator) builder.withOperator(logicalScanOperator)
                .setPredicate(predicates)
                .build();
        newScanOperator.buildColumnFilters(predicates);
        Map<ColumnRefOperator, ScalarOperator> projectMap =
                newScanOperator.getOutputColumns().stream()
                        .collect(Collectors.toMap(Function.identity(), Function.identity()));
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projectMap);
        OptExpression project = OptExpression.create(logicalProjectOperator, OptExpression.create(newScanOperator));
        return Lists.newArrayList(project);
    }
}
