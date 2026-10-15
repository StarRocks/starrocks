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

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps {@code enable_insert_select_external_auto_refresh} strict: every filesystem-backed external table an
 * INSERT ... SELECT reads is refreshed for the plan being built.
 *
 * <p>The refresh runs in the unlocked external-table pre-pass ({@code QueryAnalyzer#analyzeExternalTablesOnly}),
 * which records each source it refreshes here. The pre-pass cannot see everything the statement reads: it skips a
 * relation whose table is already resolved (the INSERT a CTAS carries, a re-plan of the same statement), it does
 * not walk scalar subqueries, and it does not expand views. So once the SELECT is analyzed -- every view expanded,
 * every subquery analyzed -- {@link #refreshRemaining} refreshes what the pre-pass did not.
 *
 * <p>A table object can carry the data it reads (an Iceberg snapshot, say), so refreshing the cache of a source
 * the analyzer already bound does not change what this plan reads. Each refreshed source is therefore reloaded
 * once, and every relation of it is bound to the reloaded object.
 *
 * <p>The record is per plan: {@code StatementPlanner} clears it when planning ends, so every plan refreshes again.
 */
public final class InsertSourceRefresher {
    private InsertSourceRefresher() {
    }

    public static boolean isEnabled(ConnectContext session) {
        return session.getSessionVariable().isEnableInsertSelectExternalAutoRefresh();
    }

    /** Called by the pre-pass for each source it refreshed and reloaded. */
    static void recordRefreshed(ConnectContext session, Table table, Table reloaded) {
        session.getRefreshedInsertSources().put(keyOf(table), reloaded);
    }

    /**
     * Refresh every filesystem-backed source of the analyzed SELECT that is not refreshed for this plan yet, and
     * bind every relation of a refreshed source to its reloaded table.
     */
    public static void refreshRemaining(QueryStatement analyzedQuery, ConnectContext session) {
        SourceRelationCollector collector = new SourceRelationCollector();
        collector.visit(analyzedQuery);
        int refreshed = 0;
        for (TableRelation relation : collector.relations) {
            Table bound = relation.getTable();
            TableName key = keyOf(bound);
            Table reloaded = session.getRefreshedInsertSources().get(key);
            if (reloaded == null) {
                reloaded = refreshAndReload(session, bound);
                if (reloaded == null) {
                    throw new SemanticException("Table %s was dropped while the statement was being planned",
                            key.toString());
                }
                session.getRefreshedInsertSources().put(key, reloaded);
                refreshed++;
            }
            rebind(relation, reloaded);
        }
        if (refreshed > 0) {
            Tracers.count(Tracers.Module.EXTERNAL, "InsertSourceRefreshedAfterAnalyze", refreshed);
        }
    }

    private static Table refreshAndReload(ConnectContext session, Table table) {
        MetadataMgr metadataMgr = session.getGlobalStateMgr().getMetadataMgr();
        metadataMgr.refreshTable(table.getCatalogName(), table.getCatalogDBName(), table, Lists.newArrayList(),
                false);
        return metadataMgr.getTable(session, table.getCatalogName(), table.getCatalogDBName(),
                table.getCatalogTableName());
    }

    private static void rebind(TableRelation relation, Table reloaded) {
        // A time-travel relation reads the version it names, which a refresh does not move.
        if (relation.getQueryPeriodString() != null) {
            return;
        }
        Table bound = relation.getTable();
        if (bound == reloaded) {
            return;
        }
        if (!sameSchema(bound, reloaded)) {
            throw new SemanticException("The schema of table %s changed while the statement was being planned, " +
                    "please retry", keyOf(bound).toString());
        }
        relation.setTable(reloaded);
    }

    // The statement was analyzed against the bound table's columns -- name, type and nullability -- so the reloaded
    // one has to match them.
    private static boolean sameSchema(Table a, Table b) {
        List<Column> left = a.getFullSchema();
        List<Column> right = b.getFullSchema();
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (!left.get(i).getName().equalsIgnoreCase(right.get(i).getName())
                    || !left.get(i).getType().equals(right.get(i).getType())
                    || left.get(i).isAllowNull() != right.get(i).isAllowNull()) {
                return false;
            }
        }
        return a.getPartitionColumnNames().equals(b.getPartitionColumnNames());
    }

    private static TableName keyOf(Table table) {
        return new TableName(table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());
    }

    private static final class SourceRelationCollector extends AstTraverser<Void, Void> {
        private final List<TableRelation> relations = new ArrayList<>();

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            // Same reach as the pre-pass, which never refreshes a table of the internal catalog: a resource-mapped
            // table (ENGINE=HIVE and the like) keeps its own declared schema, which a reload from the backing
            // catalog would not return.
            if (table != null && table.isExternalTableWithFileSystem()
                    && !CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(table.getCatalogName())) {
                relations.add(node);
            }
            return null;
        }
    }
}
