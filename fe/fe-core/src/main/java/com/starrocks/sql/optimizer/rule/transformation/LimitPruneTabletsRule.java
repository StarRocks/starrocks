// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
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

    private static final LimitPruneTabletsRule INSTANCE = new LimitPruneTabletsRule();

    public static LimitPruneTabletsRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) input.getOp();
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        // For other key models, Other key models where the number of tablet rows does not
        // represent the true number of rows of data (need sorted aggregation)
        return olapTable.getKeysType() == KeysType.DUP_KEYS && olapScanOperator.getPredicate() == null &&
                olapScanOperator.hasLimit() &&
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
            MaterializedIndex index = partition.getIndex(olapScanOperator.getSelectedIndexId());

            for (Tablet tablet : index.getTablets()) {
                // Note: the tablet row count metadata in FE maybe delay because of BE tablet row count.
                // So the tablet row count in FE is less than or equal real tablet row count.
                long tabletRowCount = tablet.getRowCount(version);

                // Needn't select empty tablet
                if (tabletRowCount == 0) {
                    continue;
                }
                totalRow += tabletRowCount;

                result.add(tablet.getId());
                if (totalRow >= limit) {
                    break;
                }
            }
        }

        // 1. totalRow must larger than limit
        // 2. Don't select any tablet
        // 3. selected tablets don't change
        if (totalRow < limit || result.isEmpty() || result.equals(olapScanOperator.getSelectedTabletId())) {
            return Collections.emptyList();
        }

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        return Lists.newArrayList(OptExpression.create(
                builder.withOperator(olapScanOperator).setSelectedTabletId(result).build(),
                input.getInputs()));
    }
}
