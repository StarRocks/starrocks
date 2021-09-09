// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class TopNImplementationRule extends ImplementationRule {
    public TopNImplementationRule() {
        super(RuleType.IMP_TOPN,
                Pattern.create(OperatorType.LOGICAL_TOPN, OperatorType.PATTERN_LEAF));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator logicalTopN = (LogicalTopNOperator) input.getOp();

        PhysicalTopNOperator physicalTopN =
                new PhysicalTopNOperator(new OrderSpec(logicalTopN.getOrderByElements()),
                        logicalTopN.getLimit(),
                        logicalTopN.getOffset(),
                        logicalTopN.getSortPhase(),
                        logicalTopN.isSplit(),
                        false);
        return Lists.newArrayList(OptExpression.create(physicalTopN, input.getInputs()));
    }
}
