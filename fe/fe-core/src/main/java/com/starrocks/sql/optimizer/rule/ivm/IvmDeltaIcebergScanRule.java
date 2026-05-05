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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Maps;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves a {@link LogicalDeltaOperator} wrapping a {@link LogicalIcebergScanOperator} by converting it
 * into an incremental scan that reads only the delta data between two Iceberg snapshots.
 *
 * <p>Pattern: {@code LogicalDeltaOperator -> LogicalIcebergScanOperator}
 *
 * <p>The scan operator's {@code tvrVersionRange} (a {@link TvrTableDelta}) carries the from/to snapshot IDs,
 * set by {@code MVIVMBasedRefreshProcessor.buildInsertPlan()} via {@code RelationTransformer}.
 * This rule reads that same data source as {@code TvrTableScanRule}.
 *
 * <p>For append-only Iceberg tables, {@code __ACTION__} is a constant {@code 0} (INSERT = UPSERT).
 */
public class IvmDeltaIcebergScanRule extends TransformationRule {
    public IvmDeltaIcebergScanRule() {
        super(RuleType.TF_IVM_DELTA_ICEBERG_SCAN,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_ICEBERG_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalIcebergScanOperator scan = (LogicalIcebergScanOperator) input.inputAt(0).getOp();
        Optional<TvrTableDeltaTrait> trait = scan.getTvrTableDeltaTrait();
        return trait.isPresent() && trait.get().isAppendOnly();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalIcebergScanOperator scan = (LogicalIcebergScanOperator) input.inputAt(0).getOp();
        TvrTableDeltaTrait trait = scan.getTvrTableDeltaTrait().get();
        TvrTableDelta tvrDelta = trait.getTvrDelta();

        // No changes: from == to
        if (tvrDelta.isEmpty()) {
            List<ColumnRefOperator> outputColumns = new java.util.ArrayList<>(scan.getOutputColumns());
            ColumnRefOperator actionColumn = delta.getActionColumn();
            if (actionColumn != null) {
                outputColumns.add(actionColumn);
            }
            return List.of(OptExpression.create(
                    new LogicalValuesOperator(outputColumns, Collections.emptyList())));
        }

        // Build a project on top of the scan that adds __ACTION__ = 0 (append-only INSERT).
        ColumnRefOperator actionColumn = delta.getActionColumn();
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator col : scan.getOutputColumns()) {
            projectMap.put(col, col);
        }
        if (actionColumn != null) {
            projectMap.put(actionColumn, ConstantOperator.createTinyInt((byte) 0));
        }

        // Keep the scan with its tvrVersionRange intact — the physical layer (IcebergMetadata)
        // knows how to handle TvrTableDelta by using IncrementalAppendScan.
        OptExpression scanExpr = OptExpression.create(scan);
        OptExpression projectExpr = OptExpression.create(new LogicalProjectOperator(projectMap), scanExpr);
        return List.of(projectExpr);
    }
}
