// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Scan
 *
 */
public class OnlyScanRule extends SingleTableRewriteBaseRule {
    private static OnlyScanRule INSTANCE = new OnlyScanRule();

    public OnlyScanRule() {
        super(RuleType.TF_MV_ONLY_SCAN_RULE, Pattern.create(OperatorType.PATTERN_SCAN));
    }

    public static OnlyScanRule getInstance() {
        return INSTANCE;
    }
}
