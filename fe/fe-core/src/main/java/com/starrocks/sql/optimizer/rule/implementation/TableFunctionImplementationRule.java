// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;

public class TableFunctionImplementationRule extends ImplementationRule {
    public TableFunctionImplementationRule() {
        super(RuleType.IMP_TABLE_FUNCTION,
                Pattern.create(OperatorType.LOGICAL_TABLE_FUNCTION, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTableFunctionOperator logicalTableFunctionOperator = (LogicalTableFunctionOperator) input.getOp();
        PhysicalTableFunctionOperator physicalLateral = new PhysicalTableFunctionOperator(
                logicalTableFunctionOperator.getFnResultColumnRefSet(),
                logicalTableFunctionOperator.getFn(),
                new ColumnRefSet(new ArrayList(logicalTableFunctionOperator.getFnParamColumnProjectMap().keySet())),
                logicalTableFunctionOperator.getOuterColumnRefSet(),
                logicalTableFunctionOperator.getLimit(),
                logicalTableFunctionOperator.getPredicate(),
                logicalTableFunctionOperator.getProjection());
        return Lists.newArrayList(OptExpression.create(physicalLateral, input.getInputs()));
    }
}