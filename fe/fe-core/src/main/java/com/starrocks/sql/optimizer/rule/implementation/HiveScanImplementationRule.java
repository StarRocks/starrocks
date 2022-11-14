// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class HiveScanImplementationRule extends ImplementationRule {
    public HiveScanImplementationRule() {
        super(RuleType.IMP_HIVE_LSCAN_TO_PSCAN,
                Pattern.create(OperatorType.LOGICAL_HIVE_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression result = null;
        LogicalHiveScanOperator scan = (LogicalHiveScanOperator) input.getOp();
        if (scan.getTable().getType() == Table.TableType.HIVE) {
            PhysicalHiveScanOperator physicalHiveScan = new PhysicalHiveScanOperator(scan.getTable(),
                    scan.getColRefToColumnMetaMap(),
                    scan.getScanOperatorPredicates(),
                    scan.getLimit(),
                    scan.getPredicate(),
                    scan.getProjection());

            result = new OptExpression(physicalHiveScan);
        }
        return Lists.newArrayList(result);
    }
}
