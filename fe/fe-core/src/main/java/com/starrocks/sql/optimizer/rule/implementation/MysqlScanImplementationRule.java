// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class MysqlScanImplementationRule extends ImplementationRule {
    public MysqlScanImplementationRule() {
        super(RuleType.IMP_MYSQL_LSCAN_TO_PSCAN, Pattern.create(OperatorType.LOGICAL_MYSQL_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalMysqlScanOperator logical = (LogicalMysqlScanOperator) input.getOp();
        PhysicalMysqlScanOperator physical = new PhysicalMysqlScanOperator(logical.getTable(),
                logical.getColRefToColumnMetaMap(),
                logical.getLimit(),
                logical.getPredicate(),
                logical.getProjection());

        if (logical.getTemporalClause() != null) {
            physical.setTemporalClause(logical.getTemporalClause());
        }

        OptExpression result = new OptExpression(physical);
        return Lists.newArrayList(result);
    }
}
