// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
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
        LogicalScanOperator logicalScanOperator = (LogicalScanOperator) scan.getOp();

        ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
        ScalarOperator predicates = Utils.compoundAnd(lfo.getPredicate(), logicalScanOperator.getPredicate());
        ScalarRangePredicateExtractor rangeExtractor = new ScalarRangePredicateExtractor();
        predicates = rangeExtractor.rewriteOnlyColumn(Utils.compoundAnd(Utils.extractConjuncts(predicates)
                .stream().map(rangeExtractor::rewriteOnlyColumn).collect(Collectors.toList())));
        Preconditions.checkState(predicates != null);
        predicates = scalarOperatorRewriter.rewrite(predicates,
                ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
        predicates = Utils.transTrue2Null(predicates);

        if (logicalScanOperator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) logicalScanOperator;
            LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(
                    olapScanOperator.getTable(),
                    olapScanOperator.getColRefToColumnMetaMap(),
                    olapScanOperator.getColumnMetaToColRefMap(),
                    olapScanOperator.getDistributionSpec(),
                    olapScanOperator.getLimit(),
                    predicates,
                    olapScanOperator.getSelectedIndexId(),
                    olapScanOperator.getSelectedPartitionId(),
                    olapScanOperator.getPartitionNames(),
                    olapScanOperator.getSelectedTabletId(),
                    olapScanOperator.getHintsTabletIds());

            Map<ColumnRefOperator, ScalarOperator> projectMap =
                    newScanOperator.getOutputColumns().stream()
                            .collect(Collectors.toMap(Function.identity(), Function.identity()));
            LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projectMap);
            OptExpression project = OptExpression.create(logicalProjectOperator, OptExpression.create(newScanOperator));
            return Lists.newArrayList(project);
        } else if (logicalScanOperator instanceof LogicalEsScanOperator) {
            LogicalEsScanOperator esScanOperator = (LogicalEsScanOperator) logicalScanOperator;
            LogicalEsScanOperator newScanOperator = new LogicalEsScanOperator(
                    esScanOperator.getTable(),
                    esScanOperator.getColRefToColumnMetaMap(),
                    esScanOperator.getColumnMetaToColRefMap(),
                    esScanOperator.getLimit(),
                    predicates,
                    esScanOperator.getProjection());

            Map<ColumnRefOperator, ScalarOperator> projectMap =
                    newScanOperator.getOutputColumns().stream()
                            .collect(Collectors.toMap(Function.identity(), Function.identity()));
            LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(projectMap);
            OptExpression project = OptExpression.create(logicalProjectOperator, OptExpression.create(newScanOperator));
            return Lists.newArrayList(project);
        } else {
            throw new StarRocksPlannerException("Error scan push down type", ErrorType.INTERNAL_ERROR);
        }
    }
}
