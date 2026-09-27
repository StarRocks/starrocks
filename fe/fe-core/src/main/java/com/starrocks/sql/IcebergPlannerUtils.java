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

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.SlotId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Shared utilities for Iceberg DML planners (DeletePlanner, UpdatePlanner, future MergePlanner).
 */
public class IcebergPlannerUtils {

    public static PhysicalPropertySet createShuffleProperty(IcebergTable icebergTable,
                                                             List<ColumnRefOperator> outputColumns) {
        List<String> names = outputColumns.stream()
                .map(ColumnRefOperator::getName)
                .collect(Collectors.toList());
        return createShuffleProperty(icebergTable, outputColumns, names);
    }

    /**
     * Overload that matches partition columns by explicit column names rather than
     * ColumnRefOperator.getName(). Needed for MERGE INTO where CASE expressions cause
     * ColumnRefFactory to assign generic names like "case", losing the original column name.
     *
     * @param outputColumnNames the column names from the Analyzer's SELECT list aliases
     */
    public static PhysicalPropertySet createShuffleProperty(IcebergTable icebergTable,
                                                             List<ColumnRefOperator> outputColumns,
                                                             List<String> outputColumnNames) {
        Preconditions.checkArgument(outputColumns.size() == outputColumnNames.size(),
                "output columns size %s does not match output column names size %s",
                outputColumns.size(), outputColumnNames.size());
        if (!icebergTable.isPartitioned()) {
            return new PhysicalPropertySet();
        }

        List<String> partitionColNames = icebergTable.getPartitionColumnNames();
        List<Integer> partitionColumnIds = Lists.newArrayList();
        for (String partCol : partitionColNames) {
            for (int i = 0; i < outputColumnNames.size(); i++) {
                if (outputColumnNames.get(i).equalsIgnoreCase(partCol)) {
                    partitionColumnIds.add(outputColumns.get(i).getId());
                    break;
                }
            }
        }

        if (partitionColumnIds.isEmpty()) {
            return new PhysicalPropertySet();
        }

        return createHashShuffleProperty(partitionColumnIds);
    }

    /**
     * Builds a required property that hash-distributes the plan output by the given
     * optimizer column ids.
     */
    private static PhysicalPropertySet createHashShuffleProperty(List<Integer> columnIds) {
        Preconditions.checkArgument(!columnIds.isEmpty(), "shuffle column ids must not be empty");
        HashDistributionDesc distributionDesc = new HashDistributionDesc(
                columnIds, HashDistributionDesc.SourceType.SHUFFLE_AGG);
        DistributionProperty distributionProperty = DistributionProperty.createProperty(
                DistributionSpec.createHashDistributionSpec(distributionDesc));
        return new PhysicalPropertySet(distributionProperty);
    }

    /**
     * Extracts the base snapshot id frozen at plan time from the IcebergScanNode that
     * scans {@code targetTable}. The plan may contain scans for other Iceberg tables
     * (e.g. {@code DELETE FROM t WHERE id IN (SELECT id FROM other_iceberg)}); using a
     * source table's snapshot id with RowDelta.validateFromSnapshot is either rejected by
     * Iceberg (snapshot not in the target's history) or silently validates against the
     * wrong window.
     * <p>
     * Returns null when no scan over {@code targetTable} exists or the table had no
     * snapshot when planned.
     */
    public static Long extractBaseSnapshotId(ExecPlan execPlan, IcebergTable targetTable) {
        IcebergScanNode scanNode = findScanNodeFor(execPlan, targetTable);
        return scanNode == null ? null : scanNode.getBaseSnapshotId().orElse(null);
    }

    /**
     * Finds the {@link IcebergScanNode} in {@code execPlan} that reads {@code targetTable},
     * matched by {@link IcebergTable#equals(Object)} — never by {@link IcebergTable#getId()},
     * a synthetic id minted fresh on every external-table resolution, so the separately
     * resolved DML target and scan-node table always differ in id.
     * <p>
     * A self-referential DML scans the target table more than once; among identity-equal
     * candidates, the DML target is the scan producing the output row-locator slots
     * ({@code _file}/{@code _pos} at output positions 0/1). Falls back to the first match
     * when those slots cannot be resolved.
     */
    private static IcebergScanNode findScanNodeFor(ExecPlan execPlan, IcebergTable targetTable) {
        if (execPlan == null || execPlan.getScanNodes() == null || targetTable == null) {
            return null;
        }
        List<IcebergScanNode> candidates = Lists.newArrayList();
        for (PlanNode node : execPlan.getScanNodes()) {
            if (node instanceof IcebergScanNode scanNode
                    && scanNode.getIcebergTable().equals(targetTable)) {
                candidates.add(scanNode);
            }
        }
        if (candidates.size() <= 1) {
            return candidates.isEmpty() ? null : candidates.get(0);
        }
        IcebergScanNode rowLocatorProducer = findRowLocatorProducingScan(execPlan, candidates);
        return rowLocatorProducer != null ? rowLocatorProducer : candidates.get(0);
    }

    /**
     * Returns the candidate scan whose tuple produces both output row-locator slots
     * ({@code _file} at output position 0, {@code _pos} at position 1), or null when the
     * slots cannot be resolved or no candidate produces them.
     */
    private static IcebergScanNode findRowLocatorProducingScan(ExecPlan execPlan,
                                                               List<IcebergScanNode> candidates) {
        List<Expr> outputExprs = execPlan.getOutputExprs();
        if (outputExprs == null || outputExprs.size() < 2) {
            return null;
        }
        SlotId fileSlotId = tryExtractSlotId(outputExprs.get(0));
        SlotId posSlotId = tryExtractSlotId(outputExprs.get(1));
        if (fileSlotId == null || posSlotId == null) {
            return null;
        }
        for (IcebergScanNode scanNode : candidates) {
            var slotIds = scanNode.getSlotIds(execPlan.getDescTbl());
            if (slotIds.contains(fileSlotId.asInt()) && slotIds.contains(posSlotId.asInt())) {
                return scanNode;
            }
        }
        return null;
    }

    /**
     * Extracts the single slot id referenced by {@code expr}, unwrapping trivial casts;
     * null when the expression does not reduce to exactly one slot. Shared with
     * {@link MergeIntoPlanner}'s row-locator extraction.
     */
    static SlotId tryExtractSlotId(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return slotRef.getSlotId();
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefs);
        return slotRefs.size() == 1 ? slotRefs.get(0).getSlotId() : null;
    }

    /**
     * Builds the Iceberg conflict-detection filter from the scan node that targets
     * {@code targetTable}. Selecting any other Iceberg scan in the plan would produce a
     * filter whose column references and schema do not match the target's, leading to
     * spurious conflicts or invalid expressions on the Iceberg side.
     */
    public static org.apache.iceberg.expressions.Expression buildIcebergFilterExpr(ExecPlan execPlan,
                                                                                     IcebergTable targetTable) {
        IcebergScanNode scanNode = findScanNodeFor(execPlan, targetTable);
        if (scanNode == null) {
            return null;
        }

        ScalarOperator predicate = scanNode.getIcebergJobPlanningPredicate();
        org.apache.iceberg.Schema nativeSchema = scanNode.getIcebergTable().getNativeTable().schema();
        if (predicate == null || nativeSchema == null) {
            return null;
        }

        ScalarOperatorToIcebergExpr.IcebergContext icebergContext =
                new ScalarOperatorToIcebergExpr.IcebergContext(nativeSchema.asStruct());
        return new ScalarOperatorToIcebergExpr().convert(Collections.singletonList(predicate), icebergContext);
    }

    public static void configureIcebergSinkPipeline(ExecPlan execPlan, ConnectContext session,
                                                     boolean canUsePipeline) {
        if (!canUsePipeline) {
            execPlan.getFragments().get(0).setPipelineDop(1);
            return;
        }

        SessionVariable sv = session.getSessionVariable();
        if (sv.isEnableConnectorSinkSpill()) {
            sv.setEnableSpill(true);
            if (sv.getConnectorSinkSpillMemLimitThreshold() < sv.getSpillMemLimitThreshold()) {
                sv.setSpillMemLimitThreshold(sv.getConnectorSinkSpillMemLimitThreshold());
            }
        }

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        if (sv.getEnableAdaptiveSinkDop()) {
            long warehouseId = session.getCurrentComputeResource().getWarehouseId();
            sinkFragment.setPipelineDop(sv.getSinkDegreeOfParallelism(warehouseId));
        } else {
            sinkFragment.setPipelineDop(sv.getParallelExecInstanceNum());
        }
        sinkFragment.setHasIcebergTableSink();
        sinkFragment.disableRuntimeAdaptiveDop();
        sinkFragment.setForceSetTableSinkDop();
    }
}
