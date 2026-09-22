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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.epack.connector.lakeformation.LakeFormationCatalogs;
import com.starrocks.epack.connector.lakeformation.LakeFormationHiveTable;
import com.starrocks.epack.connector.lakeformation.LakeFormationTableIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SHOW CREATE TABLE prints the property map verbatim, and that map carries the full physical column names
 * and types - so a governed table whose schema was already narrowed would leak them straight back through
 * this statement. These cases pin the decisions that stop it: which catalogs are even considered, that a
 * missing database is still reported as a missing database, and that a governed table is rendered from the
 * display projection.
 */
public class ShowExecutorVisitorEPackTest {

    private static ShowCreateTableStmt showCreateTable(String catalog, String db, String table) {
        TableRef ref = new TableRef(QualifiedName.of(catalog, db, table), null, NodePosition.ZERO);
        return new ShowCreateTableStmt(ref, ShowCreateTableStmt.CreateTableType.TABLE);
    }

    /** Whether the catalog under test looks like a Lake Formation one; nothing else reads it. */
    private static void lakeFormationCatalog(boolean governed) {
        new MockUp<LakeFormationCatalogs>() {
            @Mock
            public boolean isLakeFormationCatalog(String catalogName) {
                return governed;
            }
        };
    }

    private static void databaseAndTable(Database database, Table table) {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalog, String db) {
                return database;
            }

            @Mock
            public Table getTable(ConnectContext context, String catalog, String db, String tbl) {
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
     * An external catalog that is not configured for Lake Formation keeps the behaviour it had. It cannot
     * resolve to a governed table, so there is nothing to project - and resolving it here would cost every
     * such connector a second lookup for no reason.
     */
    @Test
    public void testAnExternalCatalogWithoutLakeFormationGoesToTheBaseImplementation() {
        lakeFormationCatalog(false);
        // Would be found if the local path ran; reaching the base implementation instead is the assertion.
        databaseAndTable(null, null);
        ShowCreateTableStmt statement = showCreateTable("plain_hive", "d", "t");
        assertThrows(Exception.class, () -> ShowExecutorVisitorEPack.getInstance()
                .visitShowCreateTableStatement(statement, new ConnectContext()));
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
        lakeFormationCatalog(true);
        databaseAndTable(null, null);
        ShowCreateTableStmt statement = showCreateTable("lf", "gone", "t");
        SemanticException error = assertThrows(SemanticException.class,
                () -> ShowExecutorVisitorEPack.getInstance()
                        .visitShowCreateTableStatement(statement, new ConnectContext()));
        assertTrue(error.getMessage().contains("gone"));
    }

    @Test
    public void testAMissingTableIsReported(@Mocked Database database) {
        lakeFormationCatalog(true);
        databaseAndTable(database, null);
        ShowCreateTableStmt statement = showCreateTable("lf", "d", "gone");
        SemanticException error = assertThrows(SemanticException.class,
                () -> ShowExecutorVisitorEPack.getInstance()
                        .visitShowCreateTableStatement(statement, new ConnectContext()));
        assertTrue(error.getMessage().contains("gone"));
    }

    /** A governed table is rendered here, from the projection - the property map never reaches the output. */
    @Test
    public void testAGovernedTableIsRenderedFromTheProjection(@Mocked Database database) {
        lakeFormationCatalog(true);
        LakeFormationHiveTable governed = governedTable();
        databaseAndTable(database, governed);

        ShowResultSet result = (ShowResultSet) ShowExecutorVisitorEPack.getInstance()
                .visitShowCreateTableStatement(showCreateTable("lf", "db", "t"), new ConnectContext());

        assertEquals(1, result.getResultRows().size());
        String ddl = result.getResultRows().get(0).get(1);
        assertTrue(ddl.contains("id"), ddl);
        // The unauthorized column must not appear, neither as a column nor inside a property value.
        assertFalse(ddl.contains("ssn"), ddl);
    }

    /**
     * A table the catalog resolved to something other than a governed table is printed as it is. The
     * catalog being configured for Lake Formation is not by itself a reason to project anything.
     */
    @Test
    public void testAnUngovernedTableInALakeFormationCatalogIsRenderedAsItIs(@Mocked Database database) {
        lakeFormationCatalog(true);
        databaseAndTable(database, plainTable());

        ShowResultSet result = (ShowResultSet) ShowExecutorVisitorEPack.getInstance()
                .visitShowCreateTableStatement(showCreateTable("lf", "db", "t"), new ConnectContext());

        assertEquals(1, result.getResultRows().size());
        assertTrue(result.getResultRows().get(0).get(1).contains("ssn"),
                "an ungoverned table keeps its own schema");
    }

    private static List<Column> schema() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", IntegerType.INT));
        columns.add(new Column("region", IntegerType.INT));
        columns.add(new Column("ssn", IntegerType.INT));
        return columns;
    }

    private static HiveTable plainTable() {
        return HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName("lf")
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema())
                .setPartitionColumnNames(new ArrayList<>(List.of("region")))
                .setDataColumnNames(new ArrayList<>(List.of("id", "ssn")))
                .setProperties(new HashMap<>())
                .setSerdeProperties(new HashMap<>())
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .build();
    }

    private static LakeFormationHiveTable governedTable() {
        return LakeFormationHiveTable.of(plainTable(),
                List.of(new Column("id", IntegerType.INT), new Column("region", IntegerType.INT)),
                new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "t"));
    }
}
