// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class CastToEmptyRule extends TransformationRule {
    private static final ConstantOperator FALSE_OPERATOR = ConstantOperator.createBoolean(false);

    public CastToEmptyRule() {
        super(RuleType.TF_CAST_TO_EMPTY, Pattern.create(OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalOperator lo = (LogicalOperator) input.getOp();
        for (ScalarOperator op : Utils.extractConjuncts(lo.getPredicate())) {
            if (!(op.isConstantRef())) {
                continue;
            }

            ConstantOperator predicate = (ConstantOperator) op;
            if (FALSE_OPERATOR.equals(predicate) || predicate.isNull()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        List<ColumnRefOperator> refs = Lists.newArrayList();
        for (int columnId : input.getOutputColumns().getColumnIds()) {
            refs.add(context.getColumnRefFactory().getColumnRef(columnId));
        }
        return Lists.newArrayList(OptExpression.create(new LogicalValuesOperator(refs, Lists.newArrayList())));
    }
}
