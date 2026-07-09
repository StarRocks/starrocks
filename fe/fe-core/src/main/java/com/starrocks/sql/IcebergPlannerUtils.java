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

import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.SlotId;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Collections;
import java.util.List;

/**
 * Shared utilities for Iceberg DML planners (DeletePlanner, UpdatePlanner, future MergePlanner).
 */
public class IcebergPlannerUtils {
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
     * null when the expression does not reduce to exactly one slot.
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
}
