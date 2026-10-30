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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.parser.SqlParser;
import mockit.Expectations;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PaimonCreateTableLikeAnalyzerTest {
    private static final String CATALOG = "paimon_like";
    private static final String SOURCE = "paimon_like.source_db.source";
    private ConnectContext context;
    private MetadataMgr metadata;
    private CatalogMgr catalogs;
    private FileStoreTable nativeTable;
    private PaimonTable sourceTable;
    private ExternalCatalog catalog;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.initWithoutTableAndDb(RunMode.SHARED_NOTHING);
    }

    @BeforeEach
    public void mockNativeCatalog() {
        context = AnalyzeTestUtil.getConnectContext();
        catalogs = GlobalStateMgr.getCurrentState().getCatalogMgr();
        metadata = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT().notNull())
                .column("ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                .column("dt", DataTypes.DATE().notNull()).primaryKey("id", "dt").partitionKeys("dt")
                .option("bucket", "2").build();
        nativeTable = mock(FileStoreTable.class);
        when(nativeTable.schema()).thenReturn(TableSchema.create(8, schema));
        when(nativeTable.rowType()).thenReturn(schema.rowType());
        when(nativeTable.partitionKeys()).thenReturn(schema.partitionKeys());
        sourceTable = new PaimonTable(CATALOG, "source_db", "source", List.of(), nativeTable);
        clearInvocations(nativeTable);
        catalog = new ExternalCatalog(100021, CATALOG, "", Map.of("type", "paimon"));
        new Expectations(catalogs, metadata) {
            {
                catalogs.catalogExists(CATALOG);
                result = true;
                minTimes = 0;
                catalogs.getCatalogByName(CATALOG);
                result = catalog;
                minTimes = 0;
                metadata.getTable((ConnectContext) any, CATALOG, "source_db", "source");
                result = sourceTable;
                minTimes = 0;
                metadata.getDb((ConnectContext) any, CATALOG, "target_db");
                result = new Database(100022, "target_db");
                minTimes = 0;
                metadata.tableExists((ConnectContext) any, CATALOG, "target_db", anyString);
                result = false;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testSameCatalogCrossDatabaseUsesNativeSchemaAndAuthorizationTarget() {
        CreateTableLikeStmt stmt = analyze("create table paimon_like.target_db.target " +
                "properties ('file.compression'='zstd', 'primary-key'='ID, DT') like " + SOURCE);
        Assertions.assertEquals(CATALOG, stmt.getCatalogName());
        Assertions.assertEquals("target_db", stmt.getDbName());
        Assertions.assertEquals("source_db", stmt.getExistedDbName());
        Assertions.assertEquals("source", stmt.getExistedTableName());
        Assertions.assertEquals(Map.of("file.compression", "zstd", "primary-key", "ID, DT"), stmt.getProperties());
        CreateTableStmt authorizationTarget = stmt.getCreateTableStmt();
        Assertions.assertNotNull(authorizationTarget);
        Assertions.assertSame(stmt.getTableRef(), authorizationTarget.getTableRef());
        Assertions.assertEquals("paimon", authorizationTarget.getEngineName());
        Assertions.assertEquals("target_db", authorizationTarget.getDbName());
        Assertions.assertTrue(authorizationTarget.getColumnDefs().isEmpty());
        Assertions.assertNull(authorizationTarget.getColumns());
        Assertions.assertNull(authorizationTarget.getDistributionDesc());
        Assertions.assertNull(authorizationTarget.getPartitionDesc());
        Assertions.assertEquals(stmt.getProperties(), authorizationTarget.getProperties());
        Assertions.assertNotSame(stmt.getProperties(), authorizationTarget.getProperties());
        // No SR columns were supplied. SHOW CREATE/reparse or the generic CREATE analyzer cannot be the copy path.
        verify(nativeTable).schema();
        verify(nativeTable, never()).uuid();
        verify(nativeTable, never()).newScan();
    }

    @Test
    public void testIfNotExistsIsPreservedAndExistingTargetOtherwiseFails() {
        new Expectations(metadata) {
            {
                metadata.tableExists((ConnectContext) any, CATALOG, "target_db", "target");
                result = true;
                minTimes = 0;
            }
        };
        assertInvalid("create table paimon_like.target_db.target like " + SOURCE, "already exists");
        CreateTableLikeStmt stmt = analyze("create table if not exists paimon_like.target_db.target like " + SOURCE);
        Assertions.assertTrue(stmt.isSetIfNotExists());
        Assertions.assertTrue(stmt.getCreateTableStmt().isSetIfNotExists());
    }

    @Test
    public void testMissingTargetDatabaseFails() {
        new Expectations(metadata) {
            {
                metadata.getDb((ConnectContext) any, CATALOG, "missing");
                result = null;
            }
        };
        assertInvalid("create table paimon_like.missing.target like " + SOURCE, "missing");
        verify(nativeTable, never()).schema();
    }

    @Test
    public void testPaimonSourceCannotTargetInternalOrDifferentCatalog() {
        assertInvalid("create table test.target like " + SOURCE, "same external Paimon catalog");
        ExternalCatalog other = new ExternalCatalog(100023, "other_paimon", "", Map.of("type", "paimon"));
        new Expectations(catalogs) {
            {
                catalogs.catalogExists("other_paimon");
                result = true;
                minTimes = 0;
                catalogs.getCatalogByName("other_paimon");
                result = other;
                minTimes = 0;
            }
        };
        assertInvalid("create table other_paimon.target_db.target like " + SOURCE, "same external Paimon catalog");
        verify(nativeTable, never()).schema();
    }

    @Test
    public void testPaimonTargetRejectsOtherConnectorSource() {
        ExternalCatalog hive = new ExternalCatalog(100024, "hive_like", "", Map.of("type", "hive"));
        Table hiveTable = new Table(100025, "source", Table.TableType.HIVE, List.of());
        new Expectations(catalogs, metadata) {
            {
                catalogs.catalogExists("hive_like");
                result = true;
                minTimes = 0;
                catalogs.getCatalogByName("hive_like");
                result = hive;
                minTimes = 0;
                metadata.getTable((ConnectContext) any, "hive_like", "source_db", "source");
                result = hiveTable;
            }
        };
        assertInvalid("create table paimon_like.target_db.target like hive_like.source_db.source",
                "same external Paimon catalog");
        verify(nativeTable, never()).schema();
    }

    @Test
    public void testTemporaryPartitionAndDistributionOverridesFail() {
        assertInvalid("create temporary table paimon_like.target_db.target like " + SOURCE, "temporary");
        assertInvalid("create table paimon_like.target_db.target partition by (dt) like " + SOURCE, "PARTITION BY");
        assertInvalid("create table paimon_like.target_db.target distributed by hash(id) buckets 2 like " + SOURCE,
                "DISTRIBUTED BY");
        verify(nativeTable, never()).schema();
    }

    @Test
    public void testViewOrOtherNonFileStoreNativeTableIsRejected() {
        sourceTable.setPaimonNativeTable(mock(org.apache.paimon.table.Table.class));
        assertInvalid("create table paimon_like.target_db.target like " + SOURCE, "file-store");
        verify(nativeTable, never()).schema();
    }

    @Test
    public void testUnsafeAndSchemaChangingOptionOverridesFail() {
        assertInvalid("create table paimon_like.target_db.target properties ('path'='redacted') like " + SOURCE, "path");
        assertInvalid("create table paimon_like.target_db.target properties ('primary-key'='id') like " + SOURCE,
                "cannot change primary-key");
        assertInvalid("create table paimon_like.target_db.target properties ('bucket-key'='missing') like " + SOURCE,
                "Invalid Paimon CREATE TABLE LIKE");
    }

    private CreateTableLikeStmt analyze(String sql) {
        CreateTableLikeStmt stmt = (CreateTableLikeStmt) SqlParser.parse(sql, 0L).get(0);
        CreateTableLikeAnalyzer.analyze(stmt, context);
        return stmt;
    }

    private void assertInvalid(String sql, String expected) {
        SemanticException error = Assertions.assertThrows(SemanticException.class, () -> analyze(sql));
        Assertions.assertTrue(error.getMessage().contains(expected), error.getMessage());
    }
}
