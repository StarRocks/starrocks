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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkChange;
import com.starrocks.lake.bookmark.BookmarkScopedTableResolver;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared entry point for building a {@link LogicalChangesScanOperator} from a
 * pair of Bookmarks on the same OlapTable. The SQL path calls {@link #build}
 * via {@code RelationTransformer}; non-SQL callers (e.g. IVM refresh) drive
 * the same path with their own column-ref map.
 */
public final class CdcScanHelper {

    public static final String CDC_CHANGE_TYPE_COLUMN_NAME = "__CHANGE_TYPE__";
    public static final String CDC_ROW_VERSION_COLUMN_NAME = "__ROW_VERSION__";

    private CdcScanHelper() {}

    /**
     * Synthetic CDC metadata columns appended after the business columns:
     * a TINYINT change-type column and a BIGINT row-version column.
     */
    public static List<Column> getCdcMetadataColumns() {
        List<Column> columns = new ArrayList<>(2);
        columns.add(new Column(CDC_CHANGE_TYPE_COLUMN_NAME, IntegerType.TINYINT));
        columns.add(new Column(CDC_ROW_VERSION_COLUMN_NAME, IntegerType.BIGINT));
        return columns;
    }

    /**
     * Validate the CHANGES request and produce the scan operator over a
     * bookmark-scoped view of {@code table}. The caller must have already
     * registered column refs for both the business columns and the CDC
     * metadata columns (see {@link #getCdcMetadataColumns()}) in
     * {@code colRefToColumnMetaMap}.
     *
     * @throws SemanticException if {@code table} is a primary-key table,
     *     if {@code base} is later than {@code head}, or if the computed
     *     delta contains non-trackable changes
     */
    public static LogicalChangesScanOperator build(
            OlapTable table, Bookmark base, Bookmark head,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
            throw new SemanticException("CHANGES on primary-key table is not supported yet");
        }
        if (base.getBookmarkId() > head.getBookmarkId()) {
            throw new SemanticException("CHANGES base must not be later than head");
        }
        BookmarkChange delta = BookmarkChange.computeChanges(base, head);
        if (!delta.isTrackable()) {
            throw new SemanticException(formatNotTrackableMessage(delta, table));
        }
        OlapTable scoped = BookmarkScopedTableResolver.resolveByChange(table, delta);
        LogicalChangesScanOperator op = new LogicalChangesScanOperator(
                scoped, colRefToColumnMetaMap, columnMetaToColRefMap,
                base, head, delta, Operator.DEFAULT_LIMIT);
        op.setSelectedPartitionId(new ArrayList<>(delta.getChanges().keySet()));
        return op;
    }

    /**
     * Render the spec §3.6 invalidation messages for each non-trackable
     * physical-partition change in {@code delta}, joined with {@code "; "}.
     * Each entry resolves the user-visible partition name from {@code table};
     * if the logical partition is gone (DROPPED beyond head), the message
     * falls back to {@code <id=N>} so the operator still has a stable handle.
     */
    public static String formatNotTrackableMessage(BookmarkChange delta, OlapTable table) {
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
}
