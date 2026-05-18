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

package com.starrocks.cdc;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.bookmark.BookmarkScopedTableResolver;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build CDC plan subtrees for non-SQL callers (IVM refresh, integration tests).
 *
 * <p>The caller must hold a Bookmark reference on both {@code base} and {@code head}
 * for the entire query lifetime. This class does not acquire references on its own;
 * dropping a reference mid-query may cause the BE scan to fail with
 * {@code CHANGES_NOT_FOUND}.
 *
 * <p>Stage-1 limit: primary-key tables are rejected.
 */
public class CDCPlanHelper {

    public static final String CDC_CHANGE_TYPE_COLUMN_NAME = "__CHANGE_TYPE__";
    public static final String CDC_ROW_VERSION_COLUMN_NAME = "__ROW_VERSION__";

    /**
     * Build a CDC logical plan subtree for {@code (base, head)} on {@code table}.
     *
     * @throws SemanticException if {@code table} is a primary-key table, if base/head are
     *     mis-ordered, or if the delta contains non-trackable changes
     *     ({@code INDEX_REPLACED} / {@code TABLET_RESHARD} / {@code DROPPED}).
     */
    public CDCPlanResult buildCDCPlanTree(OlapTable table,
                                          Bookmark base,
                                          Bookmark head,
                                          List<Column> requiredColumns) {
        if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
            throw new SemanticException("CHANGES on primary-key table is not supported yet");
        }
        if (base.getBookmarkId() > head.getBookmarkId()) {
            throw new SemanticException("CHANGES base must not be later than head");
        }

        BookmarkChange delta = BookmarkChange.computeChanges(base, head);
        if (!delta.isTrackable()) {
            throw new SemanticException(buildNonTrackableMessage(delta, table));
        }

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        List<ColumnRefOperator> outputColumns = new ArrayList<>();

        for (Column col : requiredColumns) {
            ColumnRefOperator ref = columnRefFactory.create(
                    col.getName(), col.getType(), col.isAllowNull());
            colRefToColumnMetaMap.put(ref, col);
            columnMetaToColRefMap.put(col, ref);
            outputColumns.add(ref);
        }

        Column changeTypeCol = new Column(CDC_CHANGE_TYPE_COLUMN_NAME,
                TypeFactory.createType(PrimitiveType.TINYINT), false);
        ColumnRefOperator changeTypeRef = columnRefFactory.create(
                CDC_CHANGE_TYPE_COLUMN_NAME, changeTypeCol.getType(), false);
        colRefToColumnMetaMap.put(changeTypeRef, changeTypeCol);
        columnMetaToColRefMap.put(changeTypeCol, changeTypeRef);
        outputColumns.add(changeTypeRef);

        Column rowVersionCol = new Column(CDC_ROW_VERSION_COLUMN_NAME,
                TypeFactory.createType(PrimitiveType.BIGINT), false);
        ColumnRefOperator rowVersionRef = columnRefFactory.create(
                CDC_ROW_VERSION_COLUMN_NAME, rowVersionCol.getType(), false);
        colRefToColumnMetaMap.put(rowVersionRef, rowVersionCol);
        columnMetaToColRefMap.put(rowVersionCol, rowVersionRef);
        outputColumns.add(rowVersionRef);

        // Hand the scan operator a shadow OlapTable carrying only the
        // physicals touched by the trackable delta, each stamped with
        // the head bookmark's visible version. Downstream planning
        // (column ref, partition prune) sees the scoped view, not the
        // mutable live catalog table.
        OlapTable scoped = BookmarkScopedTableResolver.resolveByChange(table, delta);

        LogicalChangesScanOperator scanOp = new LogicalChangesScanOperator(
                scoped, colRefToColumnMetaMap, columnMetaToColRefMap,
                base, head, delta, Operator.DEFAULT_LIMIT);
        scanOp.setSelectedPartitionId(new ArrayList<>(delta.getChanges().keySet()));
        OptExpression scanPlan = new OptExpression(scanOp);

        return new CDCPlanResult(scanPlan, outputColumns);
    }

    /**
     * Render the per-partition spec-3.6 invalidation messages for the
     * non-trackable changes in {@code delta}, joined with {@code "; "}.
     * Each entry resolves the user-visible partition name from {@code table};
     * if the logical partition is gone (DROPPED beyond head), the message
     * falls back to {@code <id=N>} so the operator still has a stable handle.
     */
    public static String buildNonTrackableMessage(BookmarkChange delta, OlapTable table) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, List<BookmarkChange.PhysicalPartitionChange>> e :
                delta.getChanges().entrySet()) {
            for (BookmarkChange.PhysicalPartitionChange c : e.getValue()) {
                String reason;
                if (c instanceof BookmarkChange.PartitionDropped) {
                    reason = "has been dropped or truncated between base and head";
                } else if (c instanceof BookmarkChange.IndexReplaced) {
                    reason = "has been modified in a way that rewrote its data";
                } else if (c instanceof BookmarkChange.TabletReshard) {
                    reason = "has been redistributed";
                } else {
                    // ADDED / DATA_CHANGED are trackable; isTrackable() never lets us reach here for them.
                    continue;
                }
                if (sb.length() > 0) {
                    sb.append("; ");
                }
                sb.append("CHANGES not trackable: physical partition '")
                        .append(resolvePartitionName(table, c))
                        .append("' ")
                        .append(reason);
            }
        }
        return sb.toString();
    }

    private static String resolvePartitionName(OlapTable table,
                                               BookmarkChange.PhysicalPartitionChange change) {
        Partition logical = table.getPartition(change.getLogicalPartitionId());
        if (logical != null) {
            return logical.getName();
        }
        return "<id=" + change.getPhysicalPartitionId() + ">";
    }

    public static boolean containsChangesScan(OptExpression root) {
        OperatorType type = root.getOp().getOpType();
        if (type == OperatorType.LOGICAL_CHANGES_SCAN || type == OperatorType.PHYSICAL_CHANGES_SCAN) {
            return true;
        }
        for (OptExpression child : root.getInputs()) {
            if (containsChangesScan(child)) {
                return true;
            }
        }
        return false;
    }
}
