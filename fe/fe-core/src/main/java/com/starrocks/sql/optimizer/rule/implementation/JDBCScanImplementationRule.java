// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class JDBCScanImplementationRule extends ImplementationRule {
    public JDBCScanImplementationRule() {
        super(RuleType.IMP_JDBC_LSCAN_TO_PSCAN, Pattern.create(OperatorType.LOGICAL_JDBC_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJDBCScanOperator logical = (LogicalJDBCScanOperator) input.getOp();
        PhysicalJDBCScanOperator physical = new PhysicalJDBCScanOperator(logical.getTable(),
                logical.getColRefToColumnMetaMap(),
                logical.getLimit(),
                logical.getPredicate(),
                logical.getProjection());

        OptExpression result = new OptExpression(physical);
        return Lists.newArrayList(result);
    }
}
