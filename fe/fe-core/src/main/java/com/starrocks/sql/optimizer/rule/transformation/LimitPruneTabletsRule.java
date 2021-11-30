// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

// For SQL select * from table limit x, we could only query a few of tablets
public class LimitPruneTabletsRule extends TransformationRule {
    private LimitPruneTabletsRule() {
        super(RuleType.TF_LIMIT_TABLETS_PRUNE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    private static final LimitPruneTabletsRule instance = new LimitPruneTabletsRule();

    public static LimitPruneTabletsRule getInstance() {
        return instance;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) input.getOp();
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        return olapScanOperator.getPredicate() == null && olapScanOperator.hasLimit() &&
                olapScanOperator.getHintsTabletIds().isEmpty() && !olapTable.hasDelete();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) input.getOp();

        if (olapScanOperator.getSelectedTabletId().size() <= 1) {
            return Collections.emptyList();
        }

        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        long limit = olapScanOperator.getLimit();
        long totalRow = 0;
        List<Long> result = Lists.newArrayList();
        for (Long partitionId : olapScanOperator.getSelectedPartitionId()) {
            if (totalRow >= limit) {
                break;
            }
            Partition partition = olapTable.getPartition(partitionId);
            long version = partition.getVisibleVersion();
            long versionHash = partition.getVisibleVersionHash();
            MaterializedIndex index = partition.getIndex(olapScanOperator.getSelectedIndexId());

            for (Tablet tablet : index.getTablets()) {
                long tabletRowCount = 0L;
                for (Replica replica : tablet.getReplicas()) {
                    if (replica.checkVersionCatchUp(version, versionHash, false)
                            && replica.getRowCount() > tabletRowCount) {
                        tabletRowCount = replica.getRowCount();
                        break;
                    }
                }
                totalRow += tabletRowCount;

                result.add(tablet.getId());
                if (totalRow >= limit) {
                    break;
                }
            }
        }

        if (result.equals(olapScanOperator.getSelectedTabletId())) {
            return Collections.emptyList();
        }

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        return Lists.newArrayList(OptExpression.create(
                builder.withOperator(olapScanOperator).setSelectedTabletId(result).build(),
                input.getInputs()));
    }
}
