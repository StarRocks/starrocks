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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;

import java.util.ArrayList;
import java.util.List;

/**
 * The source refresh of an INSERT ... SELECT under {@code enable_insert_select_external_auto_refresh}: every
 * filesystem-backed external table the statement reads, through views of any depth and in subqueries, is refreshed
 * for this statement before it is planned, the plan reads the table as reloaded after that refresh, and a refresh that
 * fails fails the statement.
 *
 * <p>Most tables are refreshed by the unlocked pre-pass ({@code QueryAnalyzer#analyzeExternalTablesOnly}), which
 * binds the reloaded table and records it here. What it does not reach is handled by {@link #refreshRemaining} once
 * analysis has expanded every view and subquery: a relation already bound before planning started (the INSERT a
 * CTAS carries, a retried or replanned statement), a scalar subquery, a view nested deeper than the pre-pass expands
 * or one it could not pre-resolve. A refresh drops what is cached about the table
 * ({@code ConnectorMetadata#invalidateTableForRead}); the table is then reloaded once and every relation of it is
 * rebound to the reloaded object, because a table object can carry the data it reads (an Iceberg snapshot, say), so
 * dropping the cache alone would not change what this plan reads.
 *
 * <p>The record lives in the statement's {@link PreResolvedState}, which {@code StatementPlanner} clears when
 * planning ends: every plan of the statement -- INSERT OVERWRITE plans twice -- refreshes its sources again.
 */
public final class InsertSourceRefresher {
    private InsertSourceRefresher() {
    }

    public static boolean isEnabled(ConnectContext session) {
        return session.getSessionVariable().isEnableInsertSelectExternalAutoRefresh();
    }

    /**
     * @return this table as reloaded after it was refreshed for the statement being planned, or null if it was not
     */
    static Table refreshedTable(ConnectContext session, Table table) {
        return session.getPreResolvedState().get(PreResolvedState.REFRESHED_SOURCE, keyOf(table));
    }

    /**
     * Refresh this table for the statement being planned, reload it and record the reloaded table.
     *
     * @return the reloaded table, or null if it no longer exists (nothing is recorded then)
     */
    static Table refreshAndReload(ConnectContext session, String catalogName, String dbName, String tableName,
                                  Table table) {
        session.getGlobalStateMgr().getMetadataMgr().invalidateTableForRead(catalogName, dbName, table);
        Tracers.count(Tracers.Module.EXTERNAL, "InsertSourceRefreshed", 1);
        Table reloaded = session.getGlobalStateMgr().getMetadataMgr().getTable(session, catalogName, dbName, tableName);
        if (reloaded != null) {
            session.getPreResolvedState().put(PreResolvedState.REFRESHED_SOURCE, keyOf(table), reloaded);
        }
        return reloaded;
    }

    /**
     * Refresh every source of the analyzed query that has not been refreshed for this statement yet, and bind every
     * relation of a source to the table as reloaded after its refresh.
     */
    public static void refreshRemaining(QueryStatement analyzedQuery, ConnectContext session) {
        SourceRelationCollector collector = new SourceRelationCollector();
        collector.visit(analyzedQuery);
        int refreshed = 0;
        for (TableRelation relation : collector.relations) {
            Table bound = relation.getTable();
            Table reloaded = refreshedTable(session, bound);
            if (reloaded == null) {
                // The table's own names: the relation's resolved name is its alias when it has one.
                reloaded = refreshAndReload(session, bound.getCatalogName(), bound.getCatalogDBName(),
                        bound.getCatalogTableName(), bound);
                if (reloaded == null) {
                    throw new SemanticException("Table %s was dropped while the statement was being planned",
                            keyOf(bound).toString());
                }
                refreshed++;
            }
            rebind(relation, reloaded);
        }
        if (refreshed > 0) {
            Tracers.count(Tracers.Module.EXTERNAL, "InsertSourceRefreshedAfterAnalyze", refreshed);
        }
    }

    private static void rebind(TableRelation relation, Table reloaded) {
        // A time-travel read is pinned to the version its clause names, resolved and bound during analysis.
        if (relation.getQueryPeriod() != null) {
            return;
        }
        Table bound = relation.getTable();
        Table forPlanning = reloaded.forQueryPlanning();
        if (bound == reloaded || bound == forPlanning) {
            return;
        }
        // The analyzed statement -- its scope, the relation's column map -- was built from the columns of the bound
        // table; a table whose columns differ in anything planning reads would not fit it.
        if (!sameSchema(bound, forPlanning)) {
            throw new SemanticException("The schema of table %s changed while the statement was being planned, " +
                    "please retry", keyOf(bound).toString());
        }
        relation.setTable(forPlanning);
    }

    private static boolean sameSchema(Table a, Table b) {
        List<Column> left = a.getFullSchema();
        List<Column> right = b.getFullSchema();
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!left.get(i).equalsIgnoreComment(right.get(i))) {
                return false;
            }
        }
        return a.getPartitionColumnNames().equals(b.getPartitionColumnNames());
    }

    /**
     * Built from the table itself at both ends, so the name the statement wrote does not matter.
     */
    private static TableName keyOf(Table table) {
        return new TableName(table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());
    }

    /**
     * The relations of filesystem-backed external tables in an analyzed query, including those inside view bodies,
     * CTEs and subqueries.
     */
    private static final class SourceRelationCollector extends AstTraverser<Void, Void> {
        private final List<TableRelation> relations = new ArrayList<>();

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            if (table != null && table.isExternalTableWithFileSystem()) {
                relations.add(node);
            }
            return null;
        }
    }
}
