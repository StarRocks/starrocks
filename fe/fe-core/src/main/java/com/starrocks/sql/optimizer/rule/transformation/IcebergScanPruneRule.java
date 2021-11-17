// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class IcebergScanPruneRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(IcebergScanPruneRule.class);

    public IcebergScanPruneRule() {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(OperatorType.LOGICAL_ICEBERG_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        return Collections.emptyList();
    }
}
