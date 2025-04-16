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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TableProperty;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OpRuleBit;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.rule.RuleType.TF_MV_COMPENSATION_PRUNE_UNION;

/**
 * After mv partition compensation, prune the iceberg scan operator if mv's partition is satisfied.
 * This is necessary because iceberg doesn't support partition prune in FE, use this to avoid plan expansion after mv rewrite.
 */
public class MVCompensationPruneUnionRule extends TransformationRule {
    public MVCompensationPruneUnionRule() {
        super(TF_MV_COMPENSATION_PRUNE_UNION, Pattern.create(OperatorType.LOGICAL_UNION)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        if (input.getInputs().size() != 2) {
            return false;
        }
        // If the materialized view force rewrite is disabled, we don't need to prune the iceberg scan.
        if (!context.getSessionVariable().isEnableMaterializedViewForceRewrite()) {
            return false;
        }
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(input);
        if (scanOperators.size() != 2) {
            return false;
        }
        // only iceberg scan and olap scan can be pruned
        if (scanOperators.get(0) instanceof LogicalScanOperator &&
                MvPartitionCompensator.isUnSupportedPartitionPruneExternalScanType(scanOperators.get(1).getOpType())) {
            return isMVPartitionSatisfied(scanOperators.get(0));
        } else if (scanOperators.get(1) instanceof LogicalScanOperator &&
                MvPartitionCompensator.isUnSupportedPartitionPruneExternalScanType(scanOperators.get(0).getOpType())) {
            return isMVPartitionSatisfied(scanOperators.get(1));
        } else {
            return false;
        }
    }

    private boolean isMVPartitionSatisfied(LogicalScanOperator scanOperator) {
        if (!(scanOperator instanceof LogicalOlapScanOperator)) {
            return false;
        }
        if (!scanOperator.isOpRuleBitSet(OpRuleBit.OP_MV_UNION_REWRITE)) {
            return false;
        }
        OlapTable olapTable = (OlapTable) scanOperator.getTable();
        if (!olapTable.isMaterializedView()) {
            return false;
        }
        MaterializedView mv = (MaterializedView) olapTable;
        TableProperty.QueryRewriteConsistencyMode queryRewriteConsistencyMode =
                mv.getTableProperty().getQueryRewriteConsistencyMode();
        if (queryRewriteConsistencyMode != TableProperty.QueryRewriteConsistencyMode.FORCE_MV) {
            return false;
        }
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
        List<Long> partitionIds = olapScanOperator.getSelectedPartitionId();
        if (partitionIds == null || partitionIds.isEmpty()) {
            return false;
        }
        // If the partition number of iceberg scan is less than the total partition number of mv,
        // we can prune the iceberg scan.
        long mvTotalPartitionNum = mv.getVisiblePartitions().stream().filter(p -> p.hasData()).count();
        return partitionIds.size() < mvTotalPartitionNum;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator unionOperator = (LogicalUnionOperator) input.getOp();
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(input);
        if (scanOperators.size() != 2) {
            return Lists.newArrayList();
        }
        // If mv's partition is satisfied, we can prune the iceberg scan operator.
        int reserveIdx = scanOperators.get(0) instanceof LogicalScanOperator ? 0 : 1;
        List<ColumnRefOperator> unionColRefs = unionOperator.getOutputColumnRefOp();
        List<ColumnRefOperator> childColRefs = unionOperator.getChildOutputColumns().get(reserveIdx);
        if (unionColRefs.size() != childColRefs.size()) {
            return Lists.newArrayList();
        }
        Map<ColumnRefOperator, ScalarOperator> newProjectionMap = Maps.newHashMap();
        for (int i = 0; i < unionColRefs.size(); i++) {
            newProjectionMap.put(unionColRefs.get(i), childColRefs.get(i));
        }
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(newProjectionMap);
        return Lists.newArrayList(OptExpression.create(projectOperator, input.inputAt(reserveIdx)));
    }
}
