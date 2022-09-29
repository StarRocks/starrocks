// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Filter - Scan
 * Keep single table rewrite in rule-base phase to reduce the search space
 *
 */
public class FilterScanRule extends BaseMaterializedViewRewriteRule {
    public FilterScanRule() {
        super(RuleType.TF_MV_FILTER_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.PATTERN_SCAN)));
    }
}
