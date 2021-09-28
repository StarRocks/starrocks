// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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
        if (scan.getTableType() == Table.TableType.HIVE) {
            PhysicalHiveScanOperator physicalHiveScan = new PhysicalHiveScanOperator(scan.getTable(),
                    scan.getOutputColumns(),
                    scan.getColRefToColumnMetaMap(),
                    scan.getSelectedPartitionIds(),
                    scan.getIdToPartitionKey(),
                    scan.getNoEvalPartitionConjuncts(),
                    scan.getNonPartitionConjuncts(),
                    scan.getMinMaxConjuncts(),
                    scan.getMinMaxColumnRefMap());
            physicalHiveScan.setLimit(scan.getLimit());

            result = new OptExpression(physicalHiveScan);
        }
        return Lists.newArrayList(result);
    }
}
