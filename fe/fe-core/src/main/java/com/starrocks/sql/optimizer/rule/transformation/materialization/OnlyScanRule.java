// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Scan with predicates
 *
 */
public class OnlyScanRule extends BaseMaterializedViewRewriteRule {
    public OnlyScanRule() {
        super(RuleType.TF_MV_ONLY_SCAN_RULE, Pattern.create(OperatorType.PATTERN_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Operator currentOp = input.getOp();
        if (!(currentOp instanceof LogicalScanOperator)) {
            return false;
        }
        ScalarOperator predicate = input.getOp().getPredicate();
        return predicate == null ? false : true;
    }
}
