// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class SchemaScanImplementationRule extends ImplementationRule {
    public SchemaScanImplementationRule() {
        super(RuleType.IMP_SCHEMA_LSCAN_TO_PSCAN, Pattern.create(OperatorType.LOGICAL_SCHEMA_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalSchemaScanOperator logical = (LogicalSchemaScanOperator) input.getOp();
        PhysicalSchemaScanOperator physical =
                new PhysicalSchemaScanOperator(logical.getTable(),
                        logical.getColRefToColumnMetaMap(),
                        logical.getLimit(),
                        logical.getPredicate(),
                        logical.getProjection());

        OptExpression result = new OptExpression(physical);
        return Lists.newArrayList(result);
    }
}
