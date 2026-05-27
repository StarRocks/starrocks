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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.transformer.ChangesScanBuilder;
import com.starrocks.thrift.TChangesMetaKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves a {@link LogicalDeltaOperator} wrapping a {@link LogicalOlapScanOperator}
 * over a cloud-native table into a CHANGES delta scan covering the
 * (base, head] bookmark range carried on the scan's {@code TvrTableDelta}.
 *
 * <p>Pattern: {@code LogicalDeltaOperator -> LogicalOlapScanOperator}
 *
 * <p>Mirrors {@code IvmDeltaIcebergScanRule}. Cloud-native CDC produces real per-row
 * {@code __CHANGE_TYPE__} / {@code __ROW_VERSION__} columns; this rule wraps the
 * scan in a {@code LogicalProject} that aliases {@code __CHANGE_TYPE__} to the
 * IVM-wide {@code __ACTION__} column (same {@code TINYINT} domain: 0=UPSERT,
 * 1=DELETE) and drops {@code __ROW_VERSION__}. Iceberg's analog stamps a
 * constant {@code 0}; the cloud-native alias works for Stage-2 CDC DELETE without
 * change.
 */
public class IvmDeltaOlapScanRule extends TransformationRule {
    public IvmDeltaOlapScanRule() {
        super(RuleType.TF_IVM_DELTA_OLAP_SCAN,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.inputAt(0).getOp();
        if (!scan.getTable().isCloudNativeTableOrMaterializedView()) {
            return false;
        }
        Optional<TvrTableDeltaTrait> trait = scan.getTvrTableDeltaTrait();
        return trait.isPresent() && trait.get().isAppendOnly();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.inputAt(0).getOp();
        OlapTable table = (OlapTable) scan.getTable();
        TvrTableDelta tvrDelta = (TvrTableDelta) scan.getTvrVersionRange();

        // No changes: from == to. Emit an empty Values that carries the action
        // column upward so downstream IVM consumers see a consistent shape.
        if (tvrDelta.isEmpty()) {
            List<ColumnRefOperator> outputColumns = new ArrayList<>(scan.getOutputColumns());
            ColumnRefOperator actionColumn = delta.getActionColumn();
            if (actionColumn != null) {
                outputColumns.add(actionColumn);
            }
            return List.of(OptExpression.create(
                    new LogicalValuesOperator(outputColumns, Collections.emptyList())));
        }

        BookmarkRange range = new BookmarkRange(
                tvrDelta.from().getVersion(), tvrDelta.to().getVersion());

        List<ChangesMetaDescriptor> descriptors =
                ChangesMetaDescriptor.resolve(table.getBaseSchema());
        Map<ColumnRefOperator, Column> colRefToCol = new HashMap<>(scan.getColRefToColumnMetaMap());
        Map<Column, ColumnRefOperator> colToColRef = new HashMap<>(scan.getColumnMetaToColRefMap());
        ColumnRefOperator changeTypeRef = null;
        for (ChangesMetaDescriptor descriptor : descriptors) {
            Column column = new Column(descriptor.name(), descriptor.type(), descriptor.isNullable());
            ColumnRefOperator ref = context.getColumnRefFactory()
                    .create(descriptor.name(), descriptor.type(), descriptor.isNullable());
            colRefToCol.put(ref, column);
            colToColRef.put(column, ref);
            if (descriptor.kind() == TChangesMetaKind.CHANGE_TYPE) {
                changeTypeRef = ref;
            }
        }

        // Throws SemanticException on any non-trackable change.
        LogicalChangesScanOperator changesScan = ChangesScanBuilder.buildScanOperator(
                table, range, colRefToCol, colToColRef, descriptors);

        ColumnRefOperator actionColumn = delta.getActionColumn();
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator col : scan.getOutputColumns()) {
            projectMap.put(col, col);
        }
        if (actionColumn != null && changeTypeRef != null) {
            projectMap.put(actionColumn, changeTypeRef);
        }

        return List.of(OptExpression.create(
                new LogicalProjectOperator(projectMap),
                OptExpression.create(changesScan)));
    }
}
