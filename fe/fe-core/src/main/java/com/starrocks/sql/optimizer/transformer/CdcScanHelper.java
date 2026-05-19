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
 * Builder for LogicalChangesScanOperator from a BookmarkRange on an OlapTable,
 * shared by SQL and non-SQL (e.g. IVM refresh) callers. PK rejection and the
 * base&lt;=head invariant are enforced by the caller; this builder enforces
 * bookmark resolution and trackability.
 */
public final class CdcScanHelper {

    public static final String CDC_CHANGE_TYPE_COLUMN_NAME = "__CHANGE_TYPE__";
    public static final String CDC_ROW_VERSION_COLUMN_NAME = "__ROW_VERSION__";

    /** Synthetic CDC metadata columns appended after the business columns. */
    public static List<Column> getCdcMetadataColumns() {
        List<Column> columns = new ArrayList<>(2);
        columns.add(new Column(CDC_CHANGE_TYPE_COLUMN_NAME, IntegerType.TINYINT));
        columns.add(new Column(CDC_ROW_VERSION_COLUMN_NAME, IntegerType.BIGINT));
        return columns;
    }

    /**
     * Resolve {@code range}'s base and head ids against the BookmarkManager,
     * compute the delta, reject non-trackable changes, and produce the scan
     * operator over a bookmark-scoped view of {@code table}.
     *
     * <p>ASSUMES: the caller has already registered column refs for both the
     * business columns and the CDC metadata columns in
     * {@code colRefToColumnMetaMap} — this helper only consumes the maps,
     * it does not populate them.
     *
     * @throws SemanticException if either bookmark id is not registered for
     *     {@code table}, or the delta contains non-trackable changes
     */
    public static LogicalChangesScanOperator build(
            OlapTable table, BookmarkRange range,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        long dbId = table.mayGetDatabaseId().orElseThrow(() ->
                new IllegalStateException(
                        String.format("dbId missing on %s", table.getName())));
        BookmarkManager bookmarkManager = GlobalStateMgr.getCurrentState().getBookmarkManager();
        Bookmark base = bookmarkManager.findBookmarkById(dbId, table.getId(), range.base())
                .orElseThrow(() -> new SemanticException(String.format(
                        "bookmark %d not found on table '%s'", range.base(), table.getName())));
        Bookmark head = bookmarkManager.findBookmarkById(dbId, table.getId(), range.head())
                .orElseThrow(() -> new SemanticException(String.format(
                        "bookmark %d not found on table '%s'", range.head(), table.getName())));
        BookmarkChange delta = BookmarkChange.computeChanges(base, head);
        checkTrackable(delta, table.getName(), base.getBookmarkId(), head.getBookmarkId());
        OlapTable scopedTable = BookmarkScopedTableResolver.resolveByChange(table, delta);
        return new LogicalChangesScanOperator(
                scopedTable, colRefToColumnMetaMap, columnMetaToColRefMap,
                base, head, delta, Operator.DEFAULT_LIMIT);
    }

    /** Reject the first non-trackable physical-partition change in {@code delta}. */
    private static void checkTrackable(BookmarkChange delta, String tableName, long baseId, long headId) {
        for (List<BookmarkChange.PhysicalPartitionChange> changes : delta.getChanges().values()) {
            for (BookmarkChange.PhysicalPartitionChange change : changes) {
                String reason;
                if (change instanceof BookmarkChange.PartitionDropped) {
                    reason = "dropped";
                } else if (change instanceof BookmarkChange.IndexReplaced) {
                    reason = "rewritten";
                } else if (change instanceof BookmarkChange.TabletReshard) {
                    reason = "resharded";
                } else {
                    continue;
                }
                throw new SemanticException(String.format(
                        "CHANGES from bookmark %d to %d on table '%s' not trackable: physical partition %d %s",
                        baseId, headId, tableName, change.getPhysicalPartitionId(), reason));
            }
        }
    }
}
