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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanNode;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Collections;

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
        if (execPlan == null || execPlan.getScanNodes() == null || targetTable == null) {
            return null;
        }
        for (PlanNode node : execPlan.getScanNodes()) {
            if (node instanceof IcebergScanNode scanNode
                    && scanNode.getIcebergTable().getId() == targetTable.getId()) {
                return scanNode.getBaseSnapshotId().orElse(null);
            }
        }
        return null;
    }

    /**
     * Builds the Iceberg conflict-detection filter from the scan node that targets
     * {@code targetTable}. Selecting any other Iceberg scan in the plan would produce a
     * filter whose column references and schema do not match the target's, leading to
     * spurious conflicts or invalid expressions on the Iceberg side.
     */
    public static org.apache.iceberg.expressions.Expression buildIcebergFilterExpr(ExecPlan execPlan,
                                                                                     IcebergTable targetTable) {
        if (execPlan == null || execPlan.getScanNodes() == null || targetTable == null) {
            return null;
        }

        ScalarOperator predicate = null;
        org.apache.iceberg.Schema nativeSchema = null;

        for (PlanNode node : execPlan.getScanNodes()) {
            if (node instanceof IcebergScanNode scanNode
                    && scanNode.getIcebergTable().getId() == targetTable.getId()) {
                predicate = scanNode.getIcebergJobPlanningPredicate();
                nativeSchema = scanNode.getIcebergTable().getNativeTable().schema();
                break;
            }
        }

        if (predicate == null || nativeSchema == null) {
            return null;
        }

        ScalarOperatorToIcebergExpr.IcebergContext icebergContext =
                new ScalarOperatorToIcebergExpr.IcebergContext(nativeSchema.asStruct());
        return new ScalarOperatorToIcebergExpr().convert(Collections.singletonList(predicate), icebergContext);
    }
}
