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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** DDL persistence and cache behavior against a temporary Paimon filesystem catalog. */
public class PaimonDdlMetadataTest {
    private static final String CATALOG = "paimon_ddl";
    private static final String DB = "ddl_db";
    private static final String TABLE = "target";
    private static final Identifier TARGET = Identifier.create(DB, TABLE);

    @TempDir
    java.nio.file.Path warehouse;

    private Catalog catalog;
    private PaimonMetadata metadata;

    @BeforeEach
    public void setUp() throws Exception {
        catalog = newCatalog();
        metadata = newMetadata(catalog);
        metadata.createDb(null, DB, Map.of());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testCreateDbRejectsDuplicatesAndLocationOverrides() throws Exception {
        Database cached = metadata.getDb(null, DB);
        assertNotNull(cached);
        assertThrows(AlreadyExistsException.class, () -> metadata.createDb(null, DB, Map.of()));
        assertSame(cached, metadata.getDb(null, DB));
        assertThrows(DdlException.class,
                () -> metadata.createDb(null, "unsafe_db", Map.of("location", warehouse.resolve("other").toString())));
        assertFalse(catalog.listDatabases().contains("unsafe_db"));
        metadata.createDb(null, "another_db", null);
        try (Catalog fresh = newCatalog()) {
            assertNotNull(fresh.getDatabase("another_db"));
        }
    }

    @Test
    public void testDropEmptyDbClearsCacheAndMissingDbFailsEvenWithForce() throws Exception {
        Database before = metadata.getDb(null, DB);
        metadata.dropDb(null, DB, true);
        assertNull(metadata.getDb(null, DB));
        assertFalse(catalog.listDatabases().contains(DB));
        assertThrows(MetaNotFoundException.class, () -> metadata.dropDb(null, DB, false));
        assertThrows(MetaNotFoundException.class, () -> metadata.dropDb(null, DB, true));
        metadata.createDb(null, DB, Map.of());
        assertNotSame(before, metadata.getDb(null, DB));
    }

    @Test
    public void testDropDbEvictsTablesDroppedByAnotherCatalog() throws Exception {
        createTarget();
        // Exercise the delegate wrapper used around a cached catalog in production.
        PaimonMetadata wrapped = newMetadata(new GuardedPaimonCatalog(CATALOG, catalog));
        assertNotNull(wrapped.getTable(null, DB, TABLE));
        try (Catalog other = newCatalog()) {
            other.dropTable(TARGET, false);
        }
        wrapped.dropDb(null, DB, false);
        assertNull(wrapped.getTable(null, DB, TABLE));
        assertNull(newMetadata(catalog).getTable(null, DB, TABLE));
        assertFalse(catalog.listDatabases().contains(DB));
        wrapped.createDb(null, DB, Map.of());
        assertNull(wrapped.getTable(null, DB, TABLE));
    }

    @Test
    public void testSuccessfulDdlIsNotFailedByCacheInvalidation() throws Exception {
        Catalog failing = new GuardedPaimonCatalog(CATALOG, catalog) {
            @Override
            public void invalidateTable(Identifier identifier) {
                throw new IllegalStateException("cache unavailable");
            }
        };
        PaimonMetadata mutation = newMetadata(failing);
        assertTrue(mutation.createTable(null, targetStatement()));
        assertNotNull(mutation.getTable(null, DB, TABLE));
        mutation.dropTable(null, drop(false, false));
        assertNull(mutation.getTable(null, DB, TABLE));
        try (Catalog fresh = newCatalog()) {
            assertThrows(Catalog.TableNotExistException.class, () -> fresh.getTable(TARGET));
        }
    }

    @Test
    public void testCreateTablePersistsExactScalarAndNestedSdkSchema() throws Exception {
        CreateTableStmt stmt = create("create table " + CATALOG + "." + DB + "." + TABLE + " ("
                + "id bigint not null comment 'identifier', p varchar(11) not null, "
                + "flag boolean not null, tiny tinyint, small smallint not null, i int, f float, d double, "
                + "day date, ts datetime, fixed char(7), `text` varchar(29) default null comment 'nullable text', "
                + "bytes varbinary(19), money decimal(20,4), "
                + "arr array<struct<leaf int, label varchar(13)>>, attrs map<varchar(17), array<int>>, "
                + "`record` struct<seq bigint, nested struct<ratio decimal(9,2), active boolean>>"
                + ") engine=paimon primary key(id,p) comment 'persisted table' partition by(p) "
                + "properties ('bucket'='4', 'bucket-key'='id', 'file.format'='parquet', 'file.compression'='zstd')");
        assertTrue(metadata.createTable(null, stmt));

        try (Catalog fresh = newCatalog()) {
            TableSchema schema = ((FileStoreTable) fresh.getTable(TARGET)).schema();
            List<DataType> scalarTypes = List.of(DataTypes.BIGINT().notNull(), DataTypes.VARCHAR(11).notNull(),
                    DataTypes.BOOLEAN().notNull(), DataTypes.TINYINT(), DataTypes.SMALLINT().notNull(), DataTypes.INT(),
                    DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DATE(), DataTypes.TIMESTAMP(6), DataTypes.CHAR(7),
                    DataTypes.VARCHAR(29), DataTypes.VARBINARY(19), DataTypes.DECIMAL(20, 4));
            for (int i = 0; i < scalarTypes.size(); i++) {
                assertEquals(scalarTypes.get(i), schema.fields().get(i).type(), schema.fields().get(i).name());
            }
            assertEquals(List.of("id", "p"), schema.primaryKeys());
            assertEquals(List.of("p"), schema.partitionKeys());
            assertEquals(List.of("id"), schema.bucketKeys());
            assertEquals(4, schema.numBuckets());
            assertEquals("identifier", schema.fields().get(0).description());
            assertEquals("nullable text", schema.fields().get(11).description());
            assertEquals("persisted table", schema.comment());
            assertEquals("parquet", schema.options().get("file.format"));
            assertEquals("zstd", schema.options().get("file.compression"));
            assertFalse(schema.options().containsKey("fields.text.default-value"));

            ArrayType array = (ArrayType) schema.fields().get(14).type();
            assertTrue(array.isNullable());
            RowType element = (RowType) array.getElementType();
            assertTrue(element.isNullable());
            assertEquals(List.of("leaf", "label"), element.getFieldNames());
            assertEquals(DataTypes.INT(), element.getTypeAt(0));
            assertEquals(DataTypes.VARCHAR(13), element.getTypeAt(1));
            MapType map = (MapType) schema.fields().get(15).type();
            assertEquals(DataTypes.VARCHAR(17), map.getKeyType());
            assertEquals(DataTypes.ARRAY(DataTypes.INT()), map.getValueType());
            RowType record = (RowType) schema.fields().get(16).type();
            assertEquals(DataTypes.BIGINT(), record.getTypeAt(0));
            RowType nested = (RowType) record.getTypeAt(1);
            assertEquals(DataTypes.DECIMAL(9, 2), nested.getTypeAt(0));
            assertEquals(DataTypes.BOOLEAN(), nested.getTypeAt(1));
            Set<Integer> ids = new HashSet<>();
            assertUniqueFieldIds(schema.logicalRowType(), ids);
            assertEquals(23, ids.size());
        }
        assertNotNull(metadata.getTable(null, DB, TABLE));
        assertTrue(metadata.listTableNames(null, DB).contains(TABLE));
    }

    @Test
    public void testIfNotExistsReturnsFalseWithoutChangingSchemaOrCachedTable() throws Exception {
        createTarget();
        PaimonTable cached = table();
        String schema = nativeTable().schema().toString();
        String identity = nativeTable().uuid();
        CreateTableStmt competing = create("create table if not exists " + CATALOG + "." + DB + "." + TABLE
                + " (different bigint not null) engine=paimon properties ('bucket'='-1', 'file.format'='parquet')");
        assertFalse(metadata.createTable(null, competing));
        assertSame(cached, table());
        try (Catalog fresh = newCatalog()) {
            FileStoreTable persisted = (FileStoreTable) fresh.getTable(TARGET);
            assertEquals(schema, persisted.schema().toString());
            assertEquals(identity, persisted.uuid());
        }
        assertThrows(DdlException.class, () -> metadata.createTable(null, targetStatement()));
        assertThrows(DdlException.class, () -> metadata.createTable(null, create("create table " + CATALOG
                + ".missing_db.t (id int) engine=paimon properties ('bucket'='-1', 'file.format'='parquet')")));
        assertFalse(catalog.listDatabases().contains("missing_db"));
    }

    @Test
    public void testForceDropDbDoesNotCascadeNonemptyExternalDatabase() throws Exception {
        createTarget();
        PaimonTable cached = table();
        assertThrows(DdlException.class, () -> metadata.dropDb(null, DB, false));
        assertThrows(DdlException.class, () -> metadata.dropDb(null, DB, true));
        assertSame(cached, table());
        try (Catalog fresh = newCatalog()) {
            assertNotNull(fresh.getDatabase(DB));
            assertNotNull(fresh.getTable(TARGET));
        }
        metadata.dropTable(null, drop(false, true));
        metadata.dropDb(null, DB, true);
        assertNull(metadata.getDb(null, DB));
    }

    @Test
    public void testDropTableEvictsTableAndPartitionCachesBeforeRecreation() throws Exception {
        createTarget();
        seed(row(1, "one", 10));
        PaimonTable before = table();
        assertEquals(List.of("p=one"), metadata.listPartitionNames(DB, TABLE, null));
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(List.of("id", "p", "v"))
                .setTableVersionRange(TvrTableSnapshot.of(-1L)).build();
        assertFalse(((PaimonRemoteFileDesc) metadata.getRemoteFiles(before, params).get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().isEmpty());
        metadata.dropTable(null, drop(false, false));
        assertNull(metadata.getTable(null, DB, TABLE));
        assertThrows(StarRocksConnectorException.class, () -> metadata.dropTable(null, drop(false, true)));
        metadata.dropTable(null, drop(true, false));
        metadata.dropTable(null, drop(true, true));

        assertTrue(metadata.createTable(null, create("create table " + CATALOG + "." + DB + "." + TABLE
                + " (id bigint not null, p varchar(20), v int, added varchar(9)) engine=paimon partition by(p) "
                + "properties ('bucket'='-1', 'file.format'='parquet', 'write-only'='true')")));
        PaimonTable recreated = table();
        assertNotSame(before, recreated);
        assertEquals(List.of("id", "p", "v", "added"), recreated.getFieldNames());
        assertTrue(metadata.listPartitionNames(DB, TABLE, null).isEmpty());
        assertTrue(((PaimonRemoteFileDesc) metadata.getRemoteFiles(recreated, params).get(0).getFiles().get(0))
                .getPaimonSplitsInfo().getPaimonSplits().isEmpty());
        try (Catalog fresh = newCatalog()) {
            assertNull(((FileStoreTable) fresh.getTable(TARGET)).snapshotManager().latestSnapshotId());
        }
    }

    @Test
    public void testSuccessfulCreateEvictsAnOutdatedPositiveMetadataCacheEntry() throws Exception {
        createTarget();
        PaimonTable stale = table();
        // Simulate an external drop between MetadataMgr's precheck and the connector's atomic create.
        catalog.dropTable(TARGET, false);
        assertTrue(metadata.createTable(null, create("create table " + CATALOG + "." + DB + "." + TABLE
                + " (replacement int) engine=paimon properties ('bucket'='-1', 'file.format'='parquet')")));
        assertNotSame(stale, table());
        assertEquals(List.of("replacement"), table().getFieldNames());
    }

    private Catalog newCatalog() {
        Options options = new Options();
        options.setString("warehouse", warehouse.toUri().toString());
        options.setString("cache-enabled", "true");
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }

    private static PaimonMetadata newMetadata(Catalog catalog) {
        return new PaimonMetadata(CATALOG, new HdfsEnvironment(), catalog, new ConnectorProperties(ConnectorType.PAIMON));
    }

    private static CreateTableStmt create(String sql) {
        return (CreateTableStmt) SqlParser.parse(sql, 0L).get(0);
    }

    private static CreateTableStmt targetStatement() {
        return create("create table " + CATALOG + "." + DB + "." + TABLE
                + " (id int not null, p varchar(20), v int) engine=paimon partition by(p) "
                + "properties ('bucket'='-1', 'file.format'='parquet', 'write-only'='true')");
    }

    private void createTarget() throws DdlException {
        assertTrue(metadata.createTable(null, targetStatement()));
    }

    private static DropTableStmt drop(boolean ifExists, boolean force) {
        return new DropTableStmt(ifExists, new TableRef(QualifiedName.of(List.of(CATALOG, DB, TABLE)),
                null, NodePosition.ZERO), force);
    }

    private PaimonTable table() {
        return (PaimonTable) metadata.getTable(null, DB, TABLE);
    }

    private FileStoreTable nativeTable() throws Catalog.TableNotExistException {
        return (FileStoreTable) catalog.getTable(TARGET);
    }

    private static GenericRow row(int id, String partition, int value) {
        return GenericRow.of(id, BinaryString.fromString(partition), value);
    }

    private void seed(GenericRow... rows) throws Exception {
        // The SDK writer only creates a partition fixture; StarRocks INSERT remains unsupported.
        BatchWriteBuilder builder = nativeTable().newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite(); var commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                writer.write(row);
            }
            commit.commit(writer.prepareCommit());
        }
    }

    private static void assertUniqueFieldIds(DataType type, Set<Integer> ids) {
        if (type instanceof RowType row) {
            for (DataField field : row.getFields()) {
                assertTrue(ids.add(field.id()), "Repeated SDK field ID: " + field.id());
                assertUniqueFieldIds(field.type(), ids);
            }
        } else if (type instanceof ArrayType array) {
            assertUniqueFieldIds(array.getElementType(), ids);
        } else if (type instanceof MapType map) {
            assertUniqueFieldIds(map.getKeyType(), ids);
            assertUniqueFieldIds(map.getValueType(), ids);
        }
    }
}
