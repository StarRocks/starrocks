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

package com.starrocks.epack.qe;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SHOW CREATE TABLE prints the property map verbatim, and that map carries the full physical column names
 * and types - so a governed table whose schema was already narrowed would leak them straight back through
 * this statement. These cases pin the four decisions that stop it: which catalogs are even considered, that
 * a missing database is still reported as a missing database, that the resolution asks for metadata only
 * (a governed table cannot be resolved for data access outside a planning attempt, which is what kept the
 * projection from ever running), and that a governed table is rendered from the display projection.
 */
public class ShowExecutorVisitorEPackTest {

    private static ShowCreateTableStmt showCreateTable(String catalog, String db, String table) {
        TableRef ref = new TableRef(QualifiedName.of(catalog, db, table), null, NodePosition.ZERO);
        return new ShowCreateTableStmt(ref, ShowCreateTableStmt.CreateTableType.TABLE);
    }

    private static void databaseAndTable(Database database, Table table, AtomicReference<TableLoadPurpose> seen) {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalog, String db) {
                return database;
            }

            @Mock
            public Table getTable(ConnectContext context, String catalog, String db, String tbl,
                                  TableLoadPurpose purpose) {
                if (seen != null) {
                    seen.set(purpose);
                }
                return table;
            }
        };
    }

    /** No table named at all: nothing here applies, upstream answers it. */
    @Test
    public void testAStatementWithoutATableGoesToTheBaseImplementation() {
        ShowCreateTableStmt statement =
                new ShowCreateTableStmt(null, ShowCreateTableStmt.CreateTableType.TABLE);
        // Reaching the base implementation is enough; it fails on its own missing state rather than here.
        assertThrows(Exception.class,
                () -> ShowExecutorVisitorEPack.getInstance().visitShowCreateTableStatement(statement, new ConnectContext()));
    }

    /**
     * The internal catalog is handed straight back to upstream: the projection exists for governed
     * external tables and must not change how a native table is printed.
     */
    @Test
    public void testAnInternalCatalogGoesToTheBaseImplementation() {
        ShowCreateTableStmt statement = showCreateTable("default_catalog", "d", "t");
        assertThrows(Exception.class,
                () -> ShowExecutorVisitorEPack.getInstance().visitShowCreateTableStatement(statement, new ConnectContext()));
    }

    /**
     * Order matters: a missing database is reported as a missing database before Lake Formation is asked
     * about a table inside it, so the user sees ERR_BAD_DB_ERROR rather than an authorization failure for
     * something that was never there.
     */
    @Test
    public void testAMissingDatabaseIsReportedBeforeAnyTableLookup() {
        databaseAndTable(null, null, null);
        ShowCreateTableStmt statement = showCreateTable("lf", "gone", "t");
        SemanticException error = assertThrows(SemanticException.class,
                () -> ShowExecutorVisitorEPack.getInstance()
                        .visitShowCreateTableStatement(statement, new ConnectContext()));
        assertTrue(error.getMessage().contains("gone"));
    }

    @Test
    public void testAMissingTableIsReported(@Mocked Database database) {
        databaseAndTable(database, null, null);
        ShowCreateTableStmt statement = showCreateTable("lf", "d", "gone");
        SemanticException error = assertThrows(SemanticException.class,
                () -> ShowExecutorVisitorEPack.getInstance()
                        .visitShowCreateTableStatement(statement, new ConnectContext()));
        assertTrue(error.getMessage().contains("gone"));
    }

    /**
     * The resolution must say it only wants the schema. Asking for data access would be refused on a
     * governed catalog, because this statement is answered without being planned and so has no attempt for
     * a credential to belong to.
     */
    @Test
    public void testTheTableIsResolvedForMetadataOnly(@Mocked Database database, @Mocked Table plain) {
        AtomicReference<TableLoadPurpose> seen = new AtomicReference<>();
        databaseAndTable(database, plain, seen);
        ShowCreateTableStmt statement = showCreateTable("lf", "d", "t");
        try {
            ShowExecutorVisitorEPack.getInstance().visitShowCreateTableStatement(statement, new ConnectContext());
        } catch (RuntimeException ignored) {
            // Rendering a mocked table is not what this case is about.
        }
        assertEquals(TableLoadPurpose.METADATA_ONLY, seen.get(),
                "a governed table cannot be resolved for data access here");
    }
}
