// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class MetaScanImplementationRule extends ImplementationRule {
    public MetaScanImplementationRule() {
        super(RuleType.IMP_META_LSCAN_TO_PSCAN, Pattern.create(OperatorType.LOGICAL_META_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalMetaScanOperator logical = (LogicalMetaScanOperator) input.getOp();
        PhysicalMetaScanOperator physical = new PhysicalMetaScanOperator(
                logical.getAggColumnIdToNames(),
                logical.getTable(),
                logical.getColRefToColumnMetaMap(),
                logical.getLimit(),
                logical.getPredicate(),
                logical.getProjection());
        OptExpression result = new OptExpression(physical);
        return Lists.newArrayList(result);
    }
}