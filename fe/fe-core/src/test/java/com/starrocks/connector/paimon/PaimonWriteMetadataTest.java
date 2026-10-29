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

import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TPaimonCommitInfo;
import com.starrocks.thrift.TSinkCommitInfo;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.connector.paimon.PaimonSinkExtra.PublicationState.ABORTED;
import static com.starrocks.connector.paimon.PaimonSinkExtra.PublicationState.COMMITTED;
import static com.starrocks.connector.paimon.PaimonSinkExtra.PublicationState.PREPARED;
import static com.starrocks.connector.paimon.PaimonSinkExtra.PublicationState.PUBLISHING;
import static com.starrocks.connector.paimon.PaimonSinkExtra.PublicationState.UNKNOWN;
import static com.starrocks.connector.paimon.PaimonSinkExtra.PublicationState.WRITING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * FE metadata/publication tests against a temporary Java SDK filesystem catalog.
 * Java writers below only manufacture data fixtures and public SerializeList reports; these tests do not execute C++.
 */
public class PaimonWriteMetadataTest {
    private static final String CATALOG = "paimon_write";
    private static final String DB = "write_db";
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
        assertThrows(DdlException.class, () -> metadata.createDb(null, DB, Map.of()));
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
        assertThrows(DdlException.class, () -> metadata.dropDb(null, DB, false));
        assertThrows(DdlException.class, () -> metadata.dropDb(null, DB, true));
        metadata.createDb(null, DB, Map.of());
        assertNotSame(before, metadata.getDb(null, DB));
    }

    @Test
    public void testCreateTablePersistsExactScalarAndNestedSdkSchema() throws Exception {
        CreateTableStmt stmt = create("create table " + CATALOG + "." + DB + "." + TABLE + " ("
                + "id bigint not null comment 'identifier', p varchar(11) not null, "
                + "flag boolean not null, tiny tinyint, small smallint not null, i int, f float, d double, "
                + "day date, ts datetime, fixed char(7), text varchar(29) default null comment 'nullable text', "
                + "bytes varbinary(19), money decimal(20,4), "
                + "arr array<struct<leaf int, label varchar(13)>>, attrs map<varchar(17), array<int>>, "
                + "record struct<seq bigint, nested struct<ratio decimal(9,2), active boolean>>"
                + ") engine=paimon primary key(id,p) comment 'persisted table' partition by(p) "
                + "properties ('bucket'='4', 'bucket-key'='id', 'file.format'='parquet', 'file.compression'='zstd')");
        // Craft the analyzed column list rather than assuming the read-side SR converter preserves nullability.
        PaimonTableSchemaConverter.analyze(stmt);
        stmt.setColumns(stmt.getColumnDefs().stream().map(ColumnBuilder::buildColumn).collect(Collectors.toList()));
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
        assertTrue(freshRows().isEmpty());
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

    @Test
    public void testSameTableOperationsFreezeIndependentContractsAndPublicationStates() throws Exception {
        createTarget();
        FileStoreTable nativeTarget = spy(nativeTable());
        Map<String, String> sourceOptions = new HashMap<>(nativeTarget.options());
        doReturn(sourceOptions).when(nativeTarget).options();
        PaimonTable target = new PaimonTable(CATALOG, DB, TABLE, List.of(), nativeTarget);
        target.resolveNativeWriteIdentity();
        Map<String, String> partition = new HashMap<>(Map.of("p", "one"));
        PaimonSinkExtra first = PaimonSinkExtra.from(target, "STATIC", partition, "first_operation", "Asia/Shanghai");
        PaimonSinkExtra second = PaimonSinkExtra.from(target, "APPEND", Map.of(), "second_operation", "UTC");
        partition.put("p", "changed");
        sourceOptions.put("file.compression", "gzip");
        assertFalse(first.getOptions().containsValue("gzip"));
        assertFalse(second.getOptions().containsValue("gzip"));
        assertNotSame(first, second);
        assertEquals(Map.of("p", "one"), first.getStaticPartition());
        assertEquals(first.getTableIdentity(), second.getTableIdentity());
        assertEquals(nativeTable().schema().toString(), first.getSchemaJson());
        assertEquals(nativeTable().schema().id(), first.getSchemaId());
        assertEquals(nativeTable().location().toString(), first.getLocation());
        assertEquals(List.of("id", "p", "v"), first.getColumnNames());
        assertEquals(List.of("p"), first.getPartitionColumns());
        assertEquals("starrocks-first_operation", first.getCommitUser());
        assertEquals(BatchWriteBuilder.COMMIT_IDENTIFIER, first.getCommitIdentifier());
        assertEquals("Asia/Shanghai", first.getTimeZone());
        assertTrue(first.isOverwrite());
        assertFalse(second.isOverwrite());
        assertThrows(UnsupportedOperationException.class, () -> first.getStaticPartition().clear());
        assertThrows(UnsupportedOperationException.class, () -> first.getOptions().put("bucket", "8"));
        assertThrows(UnsupportedOperationException.class, () -> first.getColumnNames().clear());
        assertThrows(UnsupportedOperationException.class, () -> first.getPartitionColumns().clear());
        assertTrue(first.canAbort());
        assertThrows(StarRocksConnectorException.class, () -> first.transition(PREPARED, COMMITTED));
        assertEquals(WRITING, first.getPublicationState());
        first.transition(WRITING, PREPARED);
        assertTrue(first.canAbort());
        first.transition(PREPARED, PUBLISHING);
        assertFalse(first.canAbort());
        first.transition(PUBLISHING, UNKNOWN);
        assertFalse(first.canAbort());
        assertEquals(WRITING, second.getPublicationState());
        assertTrue(second.canAbort());
    }

    @Test
    public void testChangedSdkSchemaCannotPublishUsingFrozenOperation() throws Exception {
        createTarget();
        PaimonSinkExtra operation = operation("APPEND", Map.of(), "schema_change");
        String frozen = operation.getSchemaJson();
        catalog.alterTable(TARGET, SchemaChange.setOption("file.compression", "gzip"), false);
        assertThrows(StarRocksConnectorException.class,
                () -> metadata.finishSink(DB, TABLE, List.of(), null, operation, null));
        assertEquals(WRITING, operation.getPublicationState());
        assertEquals(frozen, operation.getSchemaJson());
        try (Catalog fresh = newCatalog()) {
            FileStoreTable changed = (FileStoreTable) fresh.getTable(TARGET);
            assertTrue(changed.schema().id() > operation.getSchemaId());
            assertThrows(StarRocksConnectorException.class, () -> operation.validateTable(changed));
            assertNull(changed.snapshotManager().latestSnapshotId());
        }
    }

    @Test
    public void testEmptyAppendAndDynamicAreCommittedNoOps() throws Exception {
        createTarget();
        seed(row(1, "one", 10), row(2, "two", 20));
        long before = latestSnapshot().id();
        PaimonTable cached = table();
        for (String mode : List.of("APPEND", "DYNAMIC")) {
            PaimonSinkExtra operation = operation(mode, Map.of(), "empty_" + mode);
            List<TSinkCommitInfo> reports = mode.equals("APPEND") ? List.of()
                    : List.of(report(operation, "empty_producer", List.of()));
            metadata.finishSink(DB, TABLE, reports, null, operation, null);
            assertEquals(COMMITTED, operation.getPublicationState());
            assertFalse(operation.canAbort());
            assertEquals(before, latestSnapshot().id());
            assertEquals(List.of("1|one|10", "2|two|20"), freshRows());
            assertSame(cached, table());
            // A completed operation is idempotent, even if a late report is malformed.
            metadata.finishSink(DB, TABLE, List.of(new TSinkCommitInfo()), null, operation, null);
            metadata.abortSink(DB, TABLE, null, operation, null);
            assertEquals(COMMITTED, operation.getPublicationState());
        }
    }

    @Test
    public void testEmptyFullOverwritePublishesFreshSnapshotAndClearsAllPartitions() throws Exception {
        createTarget();
        seed(row(1, "one", 10), row(2, "two", 20));
        long before = latestSnapshot().id();
        PaimonTable cached = table();
        assertEquals(Set.of("p=one", "p=two"), new HashSet<>(metadata.listPartitionNames(DB, TABLE, null)));
        PaimonSinkExtra operation = operation("FULL", Map.of(), "empty_full");
        metadata.finishSink(DB, TABLE, List.of(report(operation, "empty_producer", List.of())),
                "main", operation, null);
        assertPublishedAfter(operation, before);
        assertTrue(freshRows().isEmpty());
        assertNotSame(cached, table());
        assertTrue(metadata.listPartitionNames(DB, TABLE, null).isEmpty());
        long committed = latestSnapshot().id();
        metadata.finishSink(DB, TABLE, List.of(), null, operation, null);
        metadata.abortSink(DB, TABLE, null, operation, null);
        assertEquals(committed, latestSnapshot().id());
        assertEquals(COMMITTED, operation.getPublicationState());
    }

    @Test
    public void testEmptyStaticOverwriteRemovesOnlyItsExplicitPartition() throws Exception {
        createTarget();
        seed(row(1, "one", 10), row(2, "two", 20), row(3, "one", 30));
        long before = latestSnapshot().id();
        assertEquals(2, metadata.listPartitionNames(DB, TABLE, null).size());
        PaimonSinkExtra operation = operation("STATIC", Map.of("p", "one"), "empty_static");
        metadata.finishSink(DB, TABLE, List.of(), null, operation, null);
        assertPublishedAfter(operation, before);
        assertEquals(List.of("2|two|20"), freshRows());
        assertEquals(List.of("p=two"), metadata.listPartitionNames(DB, TABLE, null));
    }

    @Test
    public void testDynamicOverwriteConsumesNativeProtocolReportsFromJavaDataFixturesExactlyOnce() throws Exception {
        createTarget();
        seed(row(1, "one", 10), row(2, "two", 20), row(3, "one", 30));
        long before = latestSnapshot().id();
        PaimonTable cached = table();
        PaimonSinkExtra operation = operation("DYNAMIC", Map.of(), "dynamic_fixture");
        List<CommitMessage> prepared = prepare(row(4, "one", 40), row(5, "one", 50));
        TSinkCommitInfo producer = report(operation, "producer_0", prepared);
        assertEquals(12, producer.getPaimon_commit_info().getSerializer_version());
        assertEquals(prepared.size(), ByteBuffer.wrap(producer.getPaimon_commit_info().getCommit_messages()).getInt());
        assertEquals(prepared.size(), PaimonCommitCodec.collect(List.of(producer, producer.deepCopy()), operation)
                .validateOwnership(nativeTable()).size());
        metadata.finishSink(DB, TABLE, List.of(producer, producer.deepCopy()), null, operation, null);
        assertPublishedAfter(operation, before);
        assertEquals(List.of("2|two|20", "4|one|40", "5|one|50"), freshRows());
        assertNotSame(cached, table());
        long committed = latestSnapshot().id();
        metadata.finishSink(DB, TABLE, List.of(producer), null, operation, null);
        assertEquals(committed, latestSnapshot().id());
    }

    @Test
    public void testAbortKnownUnpublishedFilesPreservesPreviouslyCommittedData() throws Exception {
        createTarget();
        seed(row(1, "one", 10));
        long before = latestSnapshot().id();
        PaimonSinkExtra operation = operation("APPEND", Map.of(), "known_abort");
        TSinkCommitInfo producer = report(operation, "producer_0", prepare(row(2, "two", 20)));
        List<String> ownedFiles = producer.getPaimon_commit_info().getOwned_files();
        assertFalse(ownedFiles.isEmpty());
        assertFilesExist(ownedFiles, true);
        metadata.abortSink(DB, TABLE, List.of(producer), operation, null);
        assertEquals(ABORTED, operation.getPublicationState());
        assertFalse(operation.canAbort());
        assertFilesExist(ownedFiles, false);
        assertEquals(before, latestSnapshot().id());
        assertEquals(List.of("1|one|10"), freshRows());
        metadata.abortSink(DB, TABLE, null, operation, null);
        assertThrows(StarRocksConnectorException.class,
                () -> metadata.finishSink(DB, TABLE, List.of(producer), null, operation, null));
    }

    @Test
    public void testStatementCancellationBeforePublicationRemainsSafelyAbortable() throws Exception {
        createTarget();
        seed(row(1, "one", 10));
        long before = latestSnapshot().id();
        PaimonSinkExtra operation = operation("FULL", Map.of(), "cancel_before_publish");
        TSinkCommitInfo producer = report(operation, "producer_0", prepare(row(2, "two", 20)));
        ConnectContext context = new ConnectContext();
        StmtExecutor executor = mock(StmtExecutor.class);
        when(executor.isCancelled()).thenReturn(true);
        context.setExecutor(executor);
        assertFalse(context.isKilled());
        assertTrue(context.isStatementCancelled());
        assertThrows(StarRocksConnectorException.class,
                () -> metadata.finishSink(DB, TABLE, List.of(producer), null, operation, context));
        assertEquals(PREPARED, operation.getPublicationState());
        assertTrue(operation.canAbort());
        assertEquals(before, latestSnapshot().id());
        metadata.abortSink(DB, TABLE, List.of(producer), operation, context);
        assertEquals(ABORTED, operation.getPublicationState());
        assertFilesExist(producer.getPaimon_commit_info().getOwned_files(), false);
        assertEquals(List.of("1|one|10"), freshRows());
    }

    @Test
    public void testUnknownPublicationNeverAbortsOrRetries() throws Exception {
        createTarget();
        PaimonSinkExtra operation = operation("FULL", Map.of(), "unknown_publication");
        Catalog isolated = mock(Catalog.class);
        FileStoreTable target = spy(nativeTable());
        TableCommitImpl commit = mock(TableCommitImpl.class);
        SnapshotManager snapshots = mock(SnapshotManager.class);
        when(isolated.getTable(TARGET)).thenReturn(target);
        doReturn(commit).when(target).newCommit(operation.getCommitUser());
        doReturn(snapshots).when(target).snapshotManager();
        when(snapshots.latestSnapshotOfUserFromFilesystem(operation.getCommitUser())).thenReturn(Optional.empty());
        doThrow(new IllegalStateException("publication acknowledgement lost"))
                .when(commit).commit(eq(operation.getCommitIdentifier()), anyList());
        PaimonMetadata faulted = newMetadata(isolated);
        assertThrows(PaimonCommitUnknownException.class,
                () -> faulted.finishSink(DB, TABLE, List.of(), null, operation, null));
        assertEquals(UNKNOWN, operation.getPublicationState());
        assertFalse(operation.canAbort());
        verify(commit, never()).abort(anyList());
        clearInvocations(isolated, target, commit, snapshots);
        // UNKNOWN must return before even decoding reports or resolving the target.
        faulted.abortSink(DB, TABLE, null, operation, null);
        assertThrows(StarRocksConnectorException.class,
                () -> faulted.finishSink(DB, TABLE, List.of(), null, operation, null));
        verifyNoInteractions(isolated, target, commit, snapshots);
        assertEquals(UNKNOWN, operation.getPublicationState());
        assertNull(nativeTable().snapshotManager().latestSnapshotId());
    }

    @Test
    public void testDurableSnapshotProvesCommitDespiteSdkCloseFailure() throws Exception {
        createTarget();
        seed(row(1, "one", 10));
        long before = latestSnapshot().id();
        PaimonSinkExtra operation = operation("FULL", Map.of(), "visible_after_close_failure");
        Catalog isolated = mock(Catalog.class);
        FileStoreTable target = spy(nativeTable());
        TableCommitImpl commit = spy(target.newCommit(operation.getCommitUser()));
        when(isolated.getTable(TARGET)).thenReturn(target);
        doReturn(commit).when(target).newCommit(operation.getCommitUser());
        doAnswer(invocation -> {
            invocation.callRealMethod();
            throw new IOException("maintenance close acknowledgement lost");
        }).when(commit).close();
        PaimonMetadata faulted = newMetadata(isolated);
        faulted.finishSink(DB, TABLE, List.of(), null, operation, null);
        assertPublishedAfter(operation, before);
        assertTrue(freshRows().isEmpty());
        verify(commit, never()).abort(anyList());
        clearInvocations(isolated, target, commit);
        faulted.abortSink(DB, TABLE, null, operation, null);
        verifyNoInteractions(isolated, target, commit);
    }

    @Test
    public void testMalformedReportsFailBeforeLoadingOrPublishingTarget() throws Exception {
        createTarget();
        PaimonSinkExtra operation = operation("FULL", Map.of(), "malformed_reports");
        Catalog noIo = mock(Catalog.class);
        PaimonMetadata checked = newMetadata(noIo);
        TSinkCommitInfo valid = report(operation, "producer_0", List.of());
        List<Consumer<TPaimonCommitInfo>> corruptions = List.of(
                info -> info.setOperation_id("another_operation"),
                info -> info.setTable_identity("another_table"),
                info -> info.setSchema_id(operation.getSchemaId() + 1),
                info -> info.unsetSchema_id(),
                info -> info.setProducer_id(""),
                info -> info.setAttempt_id(-1),
                info -> info.setPrepare_complete(false),
                info -> info.unsetPrepare_complete(),
                info -> info.setRow_count(-1),
                info -> info.setRow_count(1),
                info -> info.setSerializer_version(11),
                info -> info.setSerializer_version(13),
                info -> info.setCommit_messages(new byte[3]),
                info -> info.setCommit_messages(ByteBuffer.allocate(4).putInt(-1).array()),
                info -> info.setCommit_messages(ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array()),
                info -> info.setCommit_messages(new byte[5]),
                info -> info.setCommit_messages(new byte[] {(byte) 0xac, (byte) 0xed, 0, 5}));
        for (Consumer<TPaimonCommitInfo> corruption : corruptions) {
            TSinkCommitInfo invalid = valid.deepCopy();
            corruption.accept(invalid.getPaimon_commit_info());
            assertThrows(StarRocksConnectorException.class,
                    () -> checked.finishSink(DB, TABLE, List.of(invalid), null, operation, null));
            assertEquals(WRITING, operation.getPublicationState());
        }
        assertThrows(StarRocksConnectorException.class,
                () -> checked.finishSink(DB, TABLE, null, null, operation, null));
        assertThrows(StarRocksConnectorException.class,
                () -> checked.finishSink(DB, TABLE, Collections.singletonList(null), null, operation, null));
        assertThrows(StarRocksConnectorException.class,
                () -> checked.finishSink(DB, TABLE, List.of(new TSinkCommitInfo()), null, operation, null));
        assertThrows(StarRocksConnectorException.class,
                () -> checked.finishSink(DB, TABLE, List.of(valid), "unsupported_branch", operation, null));
        assertThrows(StarRocksConnectorException.class,
                () -> checked.finishSink(DB, TABLE, List.of(valid), null, new Object(), null));
        checked.abortSink(DB, TABLE, null, new Object(), null);
        verifyNoInteractions(noIo);
        assertEquals(WRITING, operation.getPublicationState());
        assertNull(nativeTable().snapshotManager().latestSnapshotId());
    }

    @Test
    public void testCodecDeduplicatesOnlyIdenticalProducerAttempts() throws Exception {
        createTarget();
        PaimonSinkExtra operation = operation("APPEND", Map.of(), "duplicate_reports");
        TSinkCommitInfo valid = report(operation, "producer_0", List.of());
        assertTrue(PaimonCommitCodec.collect(List.of(valid, valid.deepCopy(),
                report(operation, "producer_1", List.of())), operation).validateOwnership(nativeTable()).isEmpty());
        List<Consumer<TPaimonCommitInfo>> conflicts = List.of(
                info -> info.setAttempt_id(1), info -> info.setSerializer_version(11),
                info -> info.setRow_count(1), info -> info.setCommit_messages(new byte[5]),
                info -> info.setOwned_files(List.of(operation.getLocation() + "/different-receipt.parquet")));
        for (Consumer<TPaimonCommitInfo> conflict : conflicts) {
            TSinkCommitInfo duplicate = valid.deepCopy();
            conflict.accept(duplicate.getPaimon_commit_info());
            assertThrows(StarRocksConnectorException.class,
                    () -> PaimonCommitCodec.collect(List.of(valid, duplicate), operation));
        }
        assertEquals(WRITING, operation.getPublicationState());
    }

    @Test
    public void testUnownedAndSharedFileReportsFailBeforePublishOrAbort() throws Exception {
        createTarget();
        PaimonSinkExtra operation = operation("APPEND", Map.of(), "invalid_file_contract");
        List<CommitMessage> prepared = prepare(row(1, "one", 10));
        TSinkCommitInfo valid = report(operation, "producer_0", prepared);
        assertFalse(PaimonCommitCodec.collect(List.of(valid), operation).validateOwnership(nativeTable()).isEmpty());
        List<TSinkCommitInfo> invalid = new ArrayList<>();
        TSinkCommitInfo unowned = valid.deepCopy();
        unowned.getPaimon_commit_info().unsetOwned_files();
        invalid.add(unowned);
        TSinkCommitInfo outside = valid.deepCopy();
        outside.getPaimon_commit_info().setOwned_files(List.of("file:///another-table/not-owned.parquet"));
        invalid.add(outside);
        CommitMessageImpl message = (CommitMessageImpl) prepared.get(0);
        invalid.add(report(operation, "producer_0", List.of(new CommitMessageImpl(message.partition(), 1,
                message.totalBuckets(), message.newFilesIncrement(), message.compactIncrement()))));
        TSinkCommitInfo wrongPartition = valid.deepCopy();
        DataOutputSerializer wrongPartitionPayload = new DataOutputSerializer(256);
        new CommitMessageSerializer().serializeList(List.of(new CommitMessageImpl(BinaryRow.EMPTY_ROW, message.bucket(),
                message.totalBuckets(), message.newFilesIncrement(), message.compactIncrement())), wrongPartitionPayload);
        wrongPartition.getPaimon_commit_info().setCommit_messages(wrongPartitionPayload.getCopyOfBuffer());
        invalid.add(wrongPartition);
        invalid.add(report(operation, "producer_0", List.of(new CommitMessageImpl(message.partition(), message.bucket(),
                8, message.newFilesIncrement(), message.compactIncrement()))));
        invalid.add(report(operation, "producer_0", List.of(new CommitMessageImpl(message.partition(), message.bucket(),
                message.totalBuckets(), new DataIncrement(message.newFilesIncrement().newFiles(),
                message.newFilesIncrement().newFiles(), List.of()), message.compactIncrement()))));
        var file = message.newFilesIncrement().newFiles().get(0);
        for (var unsupported : List.of(file.newExternalPath(valid.getPaimon_commit_info().getOwned_files().get(0)),
                file.copy(List.of("auxiliary.index")))) {
            invalid.add(report(operation, "producer_0", List.of(new CommitMessageImpl(message.partition(), message.bucket(),
                    message.totalBuckets(), new DataIncrement(List.of(unsupported), List.of(), List.of()),
                    message.compactIncrement()))));
        }
        Catalog noIo = mock(Catalog.class);
        PaimonMetadata checked = newMetadata(noIo);
        for (TSinkCommitInfo malformed : invalid) {
            assertThrows(StarRocksConnectorException.class,
                    () -> checked.finishSink(DB, TABLE, List.of(malformed), null, operation, null));
            assertThrows(StarRocksConnectorException.class,
                    () -> checked.abortSink(DB, TABLE, List.of(malformed), operation, null));
            assertEquals(WRITING, operation.getPublicationState());
        }
        verifyNoInteractions(noIo);
        assertFilesExist(valid.getPaimon_commit_info().getOwned_files(), true);
        metadata.abortSink(DB, TABLE, List.of(valid), operation, null);
        assertEquals(ABORTED, operation.getPublicationState());
    }

    @Test
    public void testSameFileNameInDifferentPartitionCannotBorrowOwnershipForCommitOrAbort() throws Exception {
        createTarget();
        PaimonSinkExtra operation = operation("APPEND", Map.of(), "partition_receipt_mismatch");
        List<CommitMessage> prepared = prepare(row(1, "one", 10));
        TSinkCommitInfo valid = report(operation, "producer_0", prepared);
        CommitMessageImpl message = (CommitMessageImpl) prepared.get(0);
        BinaryRow otherPartition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(otherPartition);
        writer.writeString(0, BinaryString.fromString("two"));
        writer.complete();
        TSinkCommitInfo misrouted = report(operation, "producer_0", List.of(new CommitMessageImpl(otherPartition,
                message.bucket(), message.totalBuckets(), message.newFilesIncrement(), message.compactIncrement())));
        misrouted.getPaimon_commit_info().setOwned_files(valid.getPaimon_commit_info().getOwned_files());
        assertOwnershipMismatchRejected(operation, misrouted);
        assertFilesExist(valid.getPaimon_commit_info().getOwned_files(), true);
    }

    @Test
    public void testSameFileNameInDifferentBucketCannotBorrowOwnershipForCommitOrAbort() throws Exception {
        assertTrue(metadata.createTable(null, create("create table " + CATALOG + "." + DB + "." + TABLE
                + " (id int not null, p varchar(20), v int) engine=paimon partition by(p) "
                + "properties ('bucket'='4', 'bucket-key'='id', 'file.format'='parquet', 'write-only'='true')")));
        PaimonSinkExtra operation = operation("APPEND", Map.of(), "bucket_receipt_mismatch");
        List<CommitMessage> prepared = prepare(row(1, "one", 10));
        TSinkCommitInfo valid = report(operation, "producer_0", prepared);
        CommitMessageImpl message = (CommitMessageImpl) prepared.get(0);
        int otherBucket = (message.bucket() + 1) % operation.getBucketCount();
        TSinkCommitInfo misrouted = report(operation, "producer_0", List.of(new CommitMessageImpl(message.partition(),
                otherBucket, message.totalBuckets(), message.newFilesIncrement(), message.compactIncrement())));
        misrouted.getPaimon_commit_info().setOwned_files(valid.getPaimon_commit_info().getOwned_files());
        assertOwnershipMismatchRejected(operation, misrouted);
        assertFilesExist(valid.getPaimon_commit_info().getOwned_files(), true);
    }

    @Test
    public void testOwnershipReceiptsUseSdkPartitionEscapingAndNullMarkers() throws Exception {
        createTarget();
        PaimonSinkExtra operation = operation("APPEND", Map.of(), "encoded_partition_receipts");
        List<CommitMessage> prepared = prepare(row(1, "a/b=50%雪", 10), GenericRow.of(2, null, 20));
        TSinkCommitInfo valid = report(operation, "producer_0", prepared);
        assertEquals(prepared.size(), PaimonCommitCodec.collect(List.of(valid), operation)
                .validateOwnership(nativeTable()).size());
        metadata.finishSink(DB, TABLE, List.of(valid), null, operation, null);
        assertEquals(COMMITTED, operation.getPublicationState());
        assertEquals(operation.getCommitUser(), latestSnapshot().commitUser());
        assertFilesExist(valid.getPaimon_commit_info().getOwned_files(), true);
    }

    private void assertOwnershipMismatchRejected(PaimonSinkExtra operation, TSinkCommitInfo misrouted) throws Exception {
        FileStoreTable target = spy(nativeTable());
        Catalog checkedCatalog = mock(Catalog.class);
        when(checkedCatalog.getTable(TARGET)).thenReturn(target);
        PaimonMetadata checked = newMetadata(checkedCatalog);
        StarRocksConnectorException publishError = assertThrows(StarRocksConnectorException.class,
                () -> checked.finishSink(DB, TABLE, List.of(misrouted), null, operation, null));
        assertTrue(publishError.getMessage().contains("exact operation ownership receipt"));
        StarRocksConnectorException abortError = assertThrows(StarRocksConnectorException.class,
                () -> checked.abortSink(DB, TABLE, List.of(misrouted), operation, null));
        assertTrue(abortError.getMessage().contains("exact operation ownership receipt"));
        verify(target, never()).newCommit(operation.getCommitUser());
        assertEquals(WRITING, operation.getPublicationState());
        assertNull(nativeTable().snapshotManager().latestSnapshotId());
    }

    @Test
    public void testFileSchemaIdMustMatchFrozenSchemaBeforePublication() throws Exception {
        createTarget();
        List<CommitMessage> oldSchemaFiles = prepare(row(1, "one", 10));
        catalog.alterTable(TARGET, SchemaChange.setOption("file.compression", "gzip"), false);
        catalog.invalidateTable(TARGET);
        FileStoreTable changed = nativeTable();
        PaimonTable target = new PaimonTable(CATALOG, DB, TABLE, List.of(), changed);
        target.resolveNativeWriteIdentity();
        PaimonSinkExtra operation = PaimonSinkExtra.from(target, "APPEND", Map.of(), "wrong_file_schema", "UTC");
        assertTrue(operation.getSchemaId() > ((CommitMessageImpl) oldSchemaFiles.get(0))
                .newFilesIncrement().newFiles().get(0).schemaId());
        TSinkCommitInfo stale = report(operation, "producer_0", oldSchemaFiles);
        Catalog noIo = mock(Catalog.class);
        assertThrows(StarRocksConnectorException.class,
                () -> newMetadata(noIo).finishSink(DB, TABLE, List.of(stale), null, operation, null));
        verifyNoInteractions(noIo);
        assertEquals(WRITING, operation.getPublicationState());
        assertNull(changed.snapshotManager().latestSnapshotId());
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

    private PaimonSinkExtra operation(String mode, Map<String, String> partition, String operationId) {
        PaimonTable table = table();
        table.resolveNativeWriteIdentity();
        return PaimonSinkExtra.from(table, mode, partition, operationId, "UTC");
    }

    private static GenericRow row(int id, String partition, int value) {
        return GenericRow.of(id, BinaryString.fromString(partition), value);
    }

    private void seed(GenericRow... rows) throws Exception {
        // Only fixture setup uses Java's writer/commit builder, never the metadata publication under test.
        BatchWriteBuilder builder = nativeTable().newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite(); var commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                writer.write(row);
            }
            commit.commit(writer.prepareCommit());
        }
    }

    private List<CommitMessage> prepare(GenericRow... rows) throws Exception {
        // The fixture writer stops at PrepareCommit. Metadata must decode the public wire protocol and publish it.
        try (BatchTableWrite writer = nativeTable().newBatchWriteBuilder().newWrite()) {
            for (GenericRow row : rows) {
                writer.write(row);
            }
            return writer.prepareCommit();
        }
    }

    private TSinkCommitInfo report(PaimonSinkExtra operation, String producer, List<CommitMessage> messages) throws Exception {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serializeList(messages, output);
        List<String> ownedFiles = new ArrayList<>();
        long rows = 0;
        FileStoreTable table = nativeTable();
        for (CommitMessage message : messages) {
            for (var file : ((CommitMessageImpl) message).newFilesIncrement().newFiles()) {
                ownedFiles.add(table.store().pathFactory().createDataFilePathFactory(message.partition(), message.bucket())
                        .toPath(file).toString());
                rows += file.rowCount();
            }
        }
        TPaimonCommitInfo info = new TPaimonCommitInfo();
        info.setOperation_id(operation.getOperationId());
        info.setProducer_id(producer);
        info.setAttempt_id(0);
        info.setSerializer_version(serializer.getVersion());
        info.setCommit_messages(output.getCopyOfBuffer());
        info.setSchema_id(operation.getSchemaId());
        info.setTable_identity(operation.getTableIdentity());
        info.setRow_count(rows);
        info.setOwned_files(ownedFiles);
        info.setPrepare_complete(true);
        return new TSinkCommitInfo().setPaimon_commit_info(info);
    }

    private Snapshot latestSnapshot() throws Exception {
        try (Catalog fresh = newCatalog()) {
            Snapshot snapshot = ((FileStoreTable) fresh.getTable(TARGET)).snapshotManager().latestSnapshot();
            assertNotNull(snapshot);
            return snapshot;
        }
    }

    private void assertPublishedAfter(PaimonSinkExtra operation, long previousSnapshot) throws Exception {
        assertEquals(COMMITTED, operation.getPublicationState());
        assertFalse(operation.canAbort());
        Snapshot snapshot = latestSnapshot();
        assertTrue(snapshot.id() > previousSnapshot);
        assertEquals(operation.getCommitUser(), snapshot.commitUser());
        assertEquals(operation.getCommitIdentifier(), snapshot.commitIdentifier());
        assertEquals(operation.getSchemaId(), snapshot.schemaId());
    }

    private List<String> freshRows() throws Exception {
        // A new SDK catalog and reader are intentional: do not mistake an FE/native cache for durable visibility.
        try (Catalog fresh = newCatalog()) {
            var read = fresh.getTable(TARGET).newReadBuilder();
            List<String> result = new ArrayList<>();
            for (var split : read.newScan().plan().splits()) {
                try (var rows = new RecordReaderIterator<>(read.newRead().createReader(split))) {
                    while (rows.hasNext()) {
                        var row = rows.next();
                        result.add(row.getInt(0) + "|" + row.getString(1) + "|" + row.getInt(2));
                    }
                }
            }
            Collections.sort(result);
            return result;
        }
    }

    private void assertFilesExist(List<String> paths, boolean expected) throws Exception {
        for (String path : paths) {
            assertEquals(expected, nativeTable().fileIO().exists(new Path(path)), path);
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
