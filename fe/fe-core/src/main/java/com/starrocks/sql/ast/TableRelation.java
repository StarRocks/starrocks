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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.lake.bookmark.BookmarkRange;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableRelation extends Relation {

    public enum TableHint {
        _META_,
        _BINLOG_,
        _SYNC_MV_,
        _USE_PK_INDEX_,
        _CACHE_STATS_,
    }

    // Bookmark hint syntax: `[_BOOKMARK_<id>_]` where <id> is a non-negative integer.
    // The id is encoded into the hint identifier itself rather than via an argument
    // grammar, which keeps bracketHint as an identifier-only list.
    private static final String BOOKMARK_HINT_PREFIX = "_BOOKMARK_";
    private static final Pattern BOOKMARK_HINT_PATTERN = Pattern.compile("^_BOOKMARK_(\\d+)_$");

    private static final String CHANGES_HINT_PREFIX = "_CHANGES_";
    private static final Pattern CHANGES_HINT_PATTERN = Pattern.compile("^_CHANGES_(\\d+)_(\\d+)_$");

    private final TableName name;
    private Table table;
    private Map<Field, Column> columns;
    // Support temporary partition
    private PartitionRef partitionNames;
    private final List<Long> tabletIds;
    private final List<Long> replicaIds;
    private final Set<TableHint> tableHints = new HashSet<>();
    private OptionalLong bookmarkId = OptionalLong.empty();
    private Optional<BookmarkRange> bookmarkRange = Optional.empty();
    // used for mysql external table
    private String queryPeriodString;

    // used for time travel
    private QueryPeriod queryPeriod;
    // used for tvr incremental read
    private TvrVersionRange tvrVersionRange;

    // TABLE SAMPLE
    private TableSampleClause sampleClause;

    private Expr partitionPredicate;

    private Map<Expr, SlotRef> generatedExprToColumnRef = new HashMap<>();

    private List<String> pruneScanColumns = Collections.emptyList();

    private long gtid = 0;

    public TableRelation(TableName name) {
        super(name.getPos());
        this.name = name;
        this.partitionNames = null;
        this.tabletIds = Lists.newArrayList();
        this.replicaIds = Lists.newArrayList();
    }

    public TableRelation(TableName name, PartitionRef partitionNames, List<Long> tabletIds, List<Long> replicaIds) {
        this(name, partitionNames, tabletIds, replicaIds, NodePosition.ZERO);
    }

    public TableRelation(TableName name, PartitionRef partitionNames, List<Long> tabletIds, List<Long> replicaIds,
                         NodePosition pos) {
        super(pos);
        this.name = name;
        this.partitionNames = partitionNames;
        this.tabletIds = tabletIds;
        this.replicaIds = replicaIds;
    }

    public TableName getName() {
        return name;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public PartitionRef getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(PartitionRef partitionNames) {
        this.partitionNames = partitionNames;
    }

    // Check whether the table has some table hints, some rules should not be applied.
    public boolean hasTableHints() {
        return partitionNames != null || isSyncMVQuery() || (tabletIds != null && !tabletIds.isEmpty()) ||
                (replicaIds != null && !replicaIds.isEmpty());
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public List<Long> getReplicaIds() {
        return replicaIds;
    }

    public Column getColumn(Field field) {
        return columns.get(field);
    }

    public void setColumns(Map<Field, Column> columns) {
        this.columns = columns;
    }

    public List<String> getPruneScanColumns() {
        return pruneScanColumns;
    }

    public void setPruneScanColumns(List<String> pruneScanColumns) {
        this.pruneScanColumns = pruneScanColumns;
    }

    public Map<Field, Column> getColumns() {
        return columns;
    }

    public Expr getPartitionPredicate() {
        return this.partitionPredicate;
    }

    public void setPartitionPredicate(Expr partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
    }

    @Override
    public TableName getResolveTableName() {
        if (alias != null) {
            if (name.getDb() != null) {
                if (name.getCatalog() != null) {
                    return new TableName(name.getCatalog(), name.getDb(), alias.getTbl(), name.getPos());
                } else {
                    return new TableName(null, name.getDb(), alias.getTbl(), name.getPos());
                }
            } else {
                return alias;
            }
        } else {
            return name;
        }
    }

    // Return true if add the hint successfully, otherwise return false.
    // For example, if the hint name is not defined, false will be returned.
    public boolean addTableHint(String hintName) {
        if (tryAddChangesHint(hintName)) {
            return true;
        }
        if (tryAddBookmarkHint(hintName)) {
            return true;
        }
        try {
            TableHint hint = TableHint.valueOf(hintName);
            tableHints.add(hint);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // Bookmark hints are claimed by prefix so that a malformed payload throws
    // here rather than being silently dropped like an unknown hint — the user
    // clearly intended a bookmark hint.
    private boolean tryAddBookmarkHint(String hintName) {
        if (!hintName.startsWith(BOOKMARK_HINT_PREFIX)) {
            return false;
        }
        Matcher m = BOOKMARK_HINT_PATTERN.matcher(hintName);
        if (!m.matches()) {
            throw new SemanticException(
                    "invalid bookmark hint format: [" + hintName + "]; expected [_BOOKMARK_<id>_]");
        }
        if (bookmarkId.isPresent()) {
            throw new SemanticException("multiple bookmark hints are not allowed");
        }
        long id;
        try {
            id = Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            throw new SemanticException(
                    "bookmark id in [" + hintName + "] is out of BIGINT range");
        }
        bookmarkId = OptionalLong.of(id);
        return true;
    }

    public OptionalLong getBookmarkId() {
        return bookmarkId;
    }

    // Changes hints are claimed by prefix so that a malformed payload throws
    // here rather than being silently dropped like an unknown hint — the user
    // clearly intended a changes hint.
    private boolean tryAddChangesHint(String hintName) {
        if (!hintName.startsWith(CHANGES_HINT_PREFIX)) {
            return false;
        }
        Matcher m = CHANGES_HINT_PATTERN.matcher(hintName);
        if (!m.matches()) {
            throw new SemanticException(
                    "invalid changes hint format: [" + hintName + "]; expected [_CHANGES_<base>_<head>_]");
        }
        if (bookmarkRange.isPresent()) {
            throw new SemanticException("multiple changes hints are not allowed");
        }
        long base;
        long head;
        try {
            base = Long.parseLong(m.group(1));
            head = Long.parseLong(m.group(2));
        } catch (NumberFormatException e) {
            throw new SemanticException(
                    "bookmark id in [" + hintName + "] is out of BIGINT range");
        }
        bookmarkRange = Optional.of(new BookmarkRange(base, head));
        return true;
    }

    public Optional<BookmarkRange> getBookmarkRange() {
        return bookmarkRange;
    }

    public Set<TableHint> getTableHints() {
        return tableHints;
    }

    public boolean isMetaQuery() {
        return tableHints.contains(TableHint._META_);
    }

    public boolean isBinlogQuery() {
        return tableHints.contains(TableHint._BINLOG_) && table.isOlapTable();
    }

    public boolean isSyncMVQuery() {
        return tableHints.contains(TableHint._SYNC_MV_);
    }

    public boolean isUsePkIndex() {
        return tableHints.contains(TableHint._USE_PK_INDEX_);
    }

    public boolean isCacheStatsQuery() {
        return tableHints.contains(TableHint._CACHE_STATS_);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitTable(this, context);
    }

    @Override
    public String toString() {
        return name.toString();
    }

    public String getQueryPeriodString() {
        return queryPeriodString;
    }

    public void setQueryPeriodString(String queryPeriodString) {
        this.queryPeriodString = queryPeriodString;
    }

    public QueryPeriod getQueryPeriod() {
        return queryPeriod;
    }

    public void setQueryPeriod(QueryPeriod queryPeriod) {
        this.queryPeriod = queryPeriod;
    }

    public void setTvrVersionRange(TvrVersionRange tvrVersionRange) {
        this.tvrVersionRange = tvrVersionRange;
    }

    public TvrVersionRange getTvrVersionRange() {
        return tvrVersionRange;
    }

    public TableSampleClause getSampleClause() {
        return sampleClause;
    }

    public void setSampleClause(TableSampleClause sampleClause) {
        this.sampleClause = sampleClause;
    }

    public void setGeneratedExprToColumnRef(Map<Expr, SlotRef> generatedExprToColumnRef) {
        this.generatedExprToColumnRef = generatedExprToColumnRef;
    }

    public Map<Expr, SlotRef> getGeneratedExprToColumnRef() {
        return generatedExprToColumnRef;
    }

    public void setGtid(long gtid) {
        this.gtid = gtid;
    }

    public long getGtid() {
        return gtid;
    }
}
