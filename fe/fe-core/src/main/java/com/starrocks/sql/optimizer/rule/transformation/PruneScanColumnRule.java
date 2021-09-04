// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public class PruneScanColumnRule extends TransformationRule {
    public static final PruneScanColumnRule OLAP_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_OLAP_SCAN);
    public static final PruneScanColumnRule HIVE_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final PruneScanColumnRule SCHEMA_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_SCHEMA_SCAN);
    public static final PruneScanColumnRule MYSQL_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_MYSQL_SCAN);
    public static final PruneScanColumnRule ES_SCAN = new PruneScanColumnRule(OperatorType.LOGICAL_ES_SCAN);

    public PruneScanColumnRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PRUNE_OLAP_SCAN_COLUMNS, Pattern.create(logicalOperatorType));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getOp();

        ColumnRefSet requiredOutputColumns = context.getTaskContext().get(0).getRequiredColumns();

        // The `newOutputs`s are some columns required but not specified by `requiredOutputColumns`.
        // including columns in predicate or some specialized columns defined by scan operator.
        Set<ColumnRefOperator> scanOutputColumns = scanOperator.getOutputColumns().stream()
                .filter(requiredOutputColumns::contains)
                .collect(Collectors.toSet());
        scanOutputColumns.addAll(Utils.extractColumnRef(scanOperator.getPredicate()));
        scanOperator.tryExtendOutputColumns(scanOutputColumns);

        // Scan column ref map must contain predicate used columns
        List<ColumnRefOperator> finalizedOutputColumns = new ArrayList<>(scanOutputColumns);

        if (finalizedOutputColumns.equals(scanOperator.getOutputColumns())) {
            return Collections.emptyList();
        }

        scanOperator.setOutputColumns(finalizedOutputColumns);
        Map<ColumnRefOperator, Column> newColumnRefMap = scanOutputColumns.stream()
                .collect(Collectors.toMap(identity(), scanOperator.getColumnRefMap()::get));
        scanOperator.setColumnRefMap(newColumnRefMap);

        return Lists.newArrayList(new OptExpression(scanOperator));
    }
}
