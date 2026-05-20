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
import com.starrocks.lake.changes.ChangesMetaDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Shared entry point for SQL planning and IVM refresh to build a CHANGES scan. */
public final class ChangesScanBuilder {

    /**
     * Resolves {@code range}'s base and head ids against the BookmarkManager,
     * computes the delta, rejects non-trackable changes, and returns the scan
     * operator over a bookmark-scoped view of {@code table}. PK rejection and
     * the base &lt;= head invariant are the caller's responsibility.
     *
     * @throws SemanticException if either bookmark id is not registered for
     *     {@code table}, or the delta contains non-trackable changes
     */
    public static LogicalChangesScanOperator buildScanOperator(
            OlapTable table, BookmarkRange range,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            List<ChangesMetaDescriptor> metaDescriptors) {
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
        BookmarkChange delta = BookmarkChange.computeChanges(Optional.of(base), head);
        OlapTable scopedTable = BookmarkScopedTableResolver.resolveByChange(table, delta);
        return new LogicalChangesScanOperator(
                scopedTable, colRefToColumnMetaMap, columnMetaToColRefMap,
                base, head, delta, Operator.DEFAULT_LIMIT,
                metaDescriptors);
    }
}
