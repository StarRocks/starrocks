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
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.lake.bookmark.BookmarkScopedTableResolver;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared entry point for building a {@link LogicalChangesScanOperator} from a
 * {@link BookmarkRange} on an OlapTable. The SQL path calls {@link #build}
 * via {@code RelationTransformer}; non-SQL callers (e.g. IVM refresh) drive
 * the same path with their own column-ref map. PK rejection and the
 * base<=head invariant are enforced by the caller (QueryAnalyzer for SQL).
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
     * Resolve {@code range}'s base and head ids against the BookmarkManager,
     * compute the bookmark delta, reject non-trackable changes, and produce
     * the scan operator over a bookmark-scoped view of {@code table}. The
     * caller must have already registered column refs for both the business
     * columns and the CDC metadata columns (see {@link #getCdcMetadataColumns()})
     * in {@code colRefToColumnMetaMap}.
     *
     * @throws SemanticException if either bookmark id is not registered for
     *     {@code table}, or if the computed delta contains non-trackable
     *     changes
     */
    public static LogicalChangesScanOperator build(
            OlapTable table, BookmarkRange range,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        long dbId = table.mayGetDatabaseId().orElseThrow(() ->
                new IllegalStateException(
                        String.format("dbId missing on %s", table.getName())));
        BookmarkManager mgr = GlobalStateMgr.getCurrentState().getBookmarkManager();
        Bookmark base = mgr.findBookmarkById(dbId, table.getId(), range.base())
                .orElseThrow(() -> new SemanticException(String.format(
                        "bookmark %d not found on table '%s'", range.base(), table.getName())));
        Bookmark head = mgr.findBookmarkById(dbId, table.getId(), range.head())
                .orElseThrow(() -> new SemanticException(String.format(
                        "bookmark %d not found on table '%s'", range.head(), table.getName())));
        BookmarkChange delta = BookmarkChange.computeChanges(base, head);
        if (!delta.isTrackable()) {
            throw new SemanticException(formatNotTrackableMessage(delta, table));
        }
        OlapTable scoped = BookmarkScopedTableResolver.resolveByChange(table, delta);
        return new LogicalChangesScanOperator(
                scoped, colRefToColumnMetaMap, columnMetaToColRefMap,
                base, head, delta, Operator.DEFAULT_LIMIT);
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
