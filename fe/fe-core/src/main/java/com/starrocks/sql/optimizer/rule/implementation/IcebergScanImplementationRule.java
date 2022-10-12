// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class IcebergScanImplementationRule extends ImplementationRule {

    public IcebergScanImplementationRule() {
        super(RuleType.IMP_ICEBERG_LSCAN_TO_PSCAN,
                Pattern.create(OperatorType.LOGICAL_ICEBERG_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression result = null;
        LogicalIcebergScanOperator scan = (LogicalIcebergScanOperator) input.getOp();
        if (scan.getTableType() == Table.TableType.ICEBERG) {
            PhysicalIcebergScanOperator physicalIcebergScan = new PhysicalIcebergScanOperator(scan.getTable(),
                    scan.getColRefToColumnMetaMap(),
                    scan.getScanOperatorPredicates(),
                    scan.getLimit(),
                    scan.getPredicate(),
                    scan.getProjection());

            result = new OptExpression(physicalIcebergScan);
        }
        return Lists.newArrayList(result);
    }
}
