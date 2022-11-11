// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Filter - Scan
 * Keep single table rewrite in rule-base phase to reduce the search space
 *
 */
public class FilterScanRule extends SingleTableRewriteBaseRule {
    private static FilterScanRule INSTANCE = new FilterScanRule();

    public FilterScanRule() {
        super(RuleType.TF_MV_FILTER_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.PATTERN_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Operator childOperator = input.getInputs().get(0).getOp();
        if (!(childOperator instanceof LogicalScanOperator)) {
            return false;
        }
        return super.check(input, context);
    }

    public static FilterScanRule getInstance() {
        return INSTANCE;
    }
}
