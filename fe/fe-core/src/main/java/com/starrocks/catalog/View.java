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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/View.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.TableIdentifier;
import com.starrocks.analysis.TableName;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Table metadata representing a globalStateMgr view or a local view from a WITH clause.
 * Most methods inherited from Table are not supposed to be called on this class because
 * views are substituted with their underlying definition during analysis of a statement.
 * <p>
 * Refreshing or invalidating a view will reload the view's definition but will not
 * affect the metadata of the underlying tables (if any).
 */
public class View extends Table {
    private static final Logger LOG = LogManager.getLogger(GlobalStateMgr.class);

    // The original SQL-string given as view definition. Set during analysis.
    // Corresponds to Hive's viewOriginalText.
    @Deprecated
    private String originalViewDef = "";

    // Query statement (as SQL string) that defines the View for view substitution.
    // It is a transformation of the original view definition, e.g., to enforce the
    // explicit column definitions even if the original view definition has explicit
    // column aliases.
    // If column definitions were given, then this "expanded" view definition
    // wraps the original view definition in a select stmt as follows.
    //
    // SELECT viewName.origCol1 AS colDesc1, viewName.origCol2 AS colDesc2, ...
    // FROM (originalViewDef) AS viewName
    //
    // Corresponds to Hive's viewExpandedText, but is not identical to the SQL
    // Hive would produce in view creation.
    @SerializedName(value = "i")
    String inlineViewDef;

    // for persist
    @SerializedName(value = "m")
    private long sqlMode = 0L;


    // Record the alter view time
    public AtomicLong lastSchemaUpdateTime = new AtomicLong(-1);

    // cache used table names
    private List<TableName> tableRefsCache = Lists.newArrayList();

    // store all refs tables and views in the def
    private AtomicReference<Set<TableIdentifier>> tblIdCache = new AtomicReference<>();

    // store the analyzed result
    private AtomicReference<ViewCache> analyzedCache = new AtomicReference<>();

    // Used for read from image
    public View() {
        super(TableType.VIEW);
    }

    public View(long id, String name, List<Column> schema) {
        super(id, name, TableType.VIEW, schema);
    }

    /**
     * Attention: if you want a new queryStatement when you want to set some special value different from
     * its query definition, please set reuseViewDef to false.
     * @return QueryStatement
     */
    public QueryStatement getQueryStatement() {
        Preconditions.checkNotNull(inlineViewDef);

        if (analyzedCache.get() == null || ConnectContext.get().cannotReuseViewDef()) {
            QueryStatement stmt = parseAndAnalyzeDef();
            if (ConnectContext.get().cannotReuseViewDef()) {
                return stmt;
            }
            analyzedCache.compareAndSet(null, new ViewCache(createViewFingerprint(), stmt));
            return analyzedCache.get().getAnalyzedDef();
        }

        String latestFingerprint = createViewFingerprint();
        ViewCache oldCache = analyzedCache.get();
        if (latestFingerprint.isEmpty() || !StringUtils.equals(latestFingerprint, oldCache.getFingerprint())) {
            QueryStatement stmt = parseAndAnalyzeDef();
            analyzedCache.compareAndSet(oldCache, new ViewCache(latestFingerprint, stmt));
        }
        return analyzedCache.get().getAnalyzedDef();
    }

    public boolean isAnalyzed() {
        return analyzedCache.get() != null;
    }


    public void setInlineViewDefWithSqlMode(String inlineViewDef, long sqlMode) {
        this.inlineViewDef = inlineViewDef;
        this.sqlMode = sqlMode;
        this.analyzedCache = new AtomicReference<>();
        this.tblIdCache = new AtomicReference<>();
        this.tableRefsCache = Lists.newArrayList();
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    /**
     * Initializes the originalViewDef, inlineViewDef, and queryStmt members
     * by parsing the expanded view definition SQL-string.
     * Throws a TableLoadingException if there was any error parsing the
     * SQL or if the view definition did not parse into a QueryStmt.
     */
    public synchronized QueryStatement init() throws UserException {
        Preconditions.checkNotNull(inlineViewDef);
        // Parse the expanded view definition SQL-string into a QueryStmt and
        // populate a view definition.
        ParseNode node;
        try {
            node = SqlParser.parse(inlineViewDef, sqlMode).get(0);
        } catch (Exception e) {
            LOG.warn("view-definition: {}. got exception: {}", inlineViewDef, e.getMessage(), e);
            // Do not pass e as the exception cause because it might reveal the existence
            // of tables that the user triggering this load may not have privileges on.
            throw new UserException(
                    String.format("Failed to parse view: %s. Its definition is:%n%s ", name, inlineViewDef));
        }
        // Make sure the view definition parses to a query statement.
        if (!(node instanceof QueryStatement)) {
            throw new UserException(String.format("View %s without query statement. Its definition is:%n%s",
                    name, inlineViewDef));
        }
        return (QueryStatement) node;
    }

    public QueryStatement parseAndAnalyzeDef() {
        ParseNode node;
        node = SqlParser.parse(inlineViewDef, sqlMode).get(0);
        // Make sure the view definition parses to a query statement.
        if (!(node instanceof QueryStatement)) {
            throw new StarRocksPlannerException(String.format("View %s without query statement. Its definition is:%n%s",
                    name, inlineViewDef), ErrorType.INTERNAL_ERROR);
        }

        QueryStatement stmt = (QueryStatement) node;
        Analyzer.analyze(stmt, ConnectContext.get());
        return stmt;
    }

    public synchronized List<TableName> getTableRefs() {
        if (this.tableRefsCache.isEmpty()) {
            QueryStatement stmt = parseAndAnalyzeDef();
            Map<TableName, Table> allTables = AnalyzerUtils.collectAllTableAndView(stmt);
            this.tableRefsCache = Lists.newArrayList(allTables.keySet());
        }

        return Lists.newArrayList(this.tableRefsCache);
    }

    public Set<TableIdentifier> getTableIdentifiers() {
        if (tblIdCache.get() == null) {
            QueryStatement stmt = parseAndAnalyzeDef();
            Set<TableIdentifier> tblIds = AnalyzerUtils.collectAllTableIdentifier(stmt);
            tblIdCache.compareAndSet(null, tblIds);
        }
        return tblIdCache.get();
    }

    public String createViewFingerprint() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        Set<TableIdentifier> tblIds = getTableIdentifiers();
        for (TableIdentifier tableId : tblIds) {
            Optional<Table> table = resolveTable(tableId);
            if (table.isPresent()) {
                if (table.get().isView()) {
                    joiner.add(table.get().getId() + "-" + ((View) table.get()).lastSchemaUpdateTime.get());
                } else if (table.get().isNativeTableOrMaterializedView()) {
                    joiner.add(table.get().getId() + "-" + ((OlapTable) table.get()).lastSchemaUpdateTime.get());
                } else {
                    // external table set a cache ttl of 180s
                    long now = System.currentTimeMillis();
                    now = now - now % (1000 * 60 * 3) + (1000 * 60 * 3);
                    joiner.add(table.get().getId() + "-" + now);
                }
            } else {
                return "";
            }
        }
        return joiner.toString();
    }

    public Optional<Table> resolveTable(TableIdentifier identifier) {
        TableName tableName = identifier.getTableName();
        if (identifier.isSyncMv()) {
            return Optional.empty();
        }
        MetaUtils.normalizationTableName(ConnectContext.get(), tableName);
        String catalogName = tableName.getCatalog();
        String dbName = tableName.getDb();
        String tbName = tableName.getTbl();
        if (Strings.isNullOrEmpty(dbName)) {
            return Optional.empty();
        }

        if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
            return Optional.empty();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        Database database = metadataMgr.getDb(catalogName, dbName);
        if (database == null) {
            return Optional.empty();
        }

        Table table = metadataMgr.getTable(catalogName, dbName, tbName);
        if (table == null) {
            return Optional.empty();
        }

        if (table.isNativeTableOrMaterializedView()) {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() == OlapTable.OlapTableState.RESTORE ||
                    olapTable.getState() == OlapTable.OlapTableState.RESTORE_WITH_LOAD) {
                return Optional.empty();
            }
        }

        return Optional.of(table);
    }


    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, originalViewDef);
        Text.writeString(out, inlineViewDef);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        // just do not want to modify the meta version, so leave originalViewDef here but set it as empty
        Text.readString(in);
        originalViewDef = "";
        inlineViewDef = Text.readString(in);
        inlineViewDef = inlineViewDef.replaceAll("default_cluster:", "");
    }

    public static class ViewCache {
        private final String fingerprint;

        private final QueryStatement analyzedDef;

        public ViewCache(String fingerprint, QueryStatement analyzedDef) {
            this.fingerprint = fingerprint;
            this.analyzedDef = analyzedDef;
        }

        public String getFingerprint() {
            return fingerprint;
        }

        public QueryStatement getAnalyzedDef() {
            return analyzedDef;
        }

        @Override
        public int hashCode() {
            return fingerprint.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return fingerprint.equals(((ViewCache) o).fingerprint);
        }
    }
}
