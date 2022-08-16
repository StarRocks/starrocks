// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class EsScanImplementationRule extends ImplementationRule {
    public EsScanImplementationRule() {
        super(RuleType.IMP_ES_LSCAN_TO_PSCAN, Pattern.create(OperatorType.LOGICAL_ES_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalEsScanOperator logical = (LogicalEsScanOperator) input.getOp();
        PhysicalEsScanOperator physical = new PhysicalEsScanOperator(
                logical.getTable(),
                logical.getColRefToColumnMetaMap(),
                logical.getSelectedIndex(),
                logical.getLimit(),
                logical.getPredicate(),
                logical.getProjection());

        OptExpression result = new OptExpression(physical);
        return Lists.newArrayList(result);
    }
}
