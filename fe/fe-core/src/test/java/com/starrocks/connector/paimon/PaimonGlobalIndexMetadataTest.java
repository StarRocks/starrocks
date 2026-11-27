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

import com.starrocks.catalog.PaimonTable;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonGlobalIndexMetadataTest {
    private FileStoreTable table;
    private PaimonTable origin;
    private Snapshot snapshot;
    private IndexFileHandler handler;
    private TableSchema schema;
    private PaimonGlobalIndexRequest request;

    private static TableSchema schema(long id, List<DataField> fields) {
        return new TableSchema(id, fields, 20, List.of(), List.of(),
                Map.of("data-evolution.enabled", "true"), "");
    }

    @BeforeEach
    void setUp() {
        ConnectContext.remove();
        schema = schema(1, List.of(DataTypes.FIELD(7, "k", DataTypes.INT()),
                DataTypes.FIELD(9, "s", DataTypes.STRING())));
        table = mock(FileStoreTable.class, RETURNS_DEEP_STUBS);
        origin = mock(PaimonTable.class);
        when(origin.getNativeTable()).thenReturn(table);
        when(table.schema()).thenReturn(schema);
        when(table.location()).thenReturn(new Path("file:/warehouse/orders"));
        when(table.options()).thenReturn(schema.options());
        when(table.schemaManager().schema(1)).thenReturn(schema);
        when(table.schemaManager().schema(0)).thenReturn(schema(0, schema.fields()));
        snapshot = table.snapshotManager().snapshot(42);
        when(snapshot.schemaId()).thenReturn(1L);
        handler = table.store().newIndexFileHandler();
        when(handler.filePath(any())).thenAnswer(call ->
                new Path("file:/warehouse/orders/index/" + ((IndexManifestEntry) call.getArgument(0)).indexFile().fileName()));
        setData(List.of(new Range(0, 19)));
        request = request(false);
        indexes(List.of(index(7, 0, 19, 100)));
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    private PaimonGlobalIndexRequest request(boolean twoFields) {
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, IntegerType.INT, "k", true), ConstantOperator.createInt(10));
        if (twoFields) {
            predicate = Utils.compoundAnd(predicate, new IsNullPredicateOperator(false,
                    new ColumnRefOperator(2, VarcharType.VARCHAR, "s", true)));
        }
        return PaimonGlobalIndexRequest.create(table.location().toString(), "main", 42,
                schema.id(), schema.timeMillis(), predicate, Map.of(1, 7, 2, 9));
    }

    private static IndexManifestEntry index(int field, long from, long to, long bytes) {
        return index(field, from, to, bytes, "btree", new byte[9]);
    }

    private static IndexManifestEntry index(int field, long from, long to, long bytes, String provider, byte[] metadata) {
        var global = new GlobalIndexMeta(from, to, field, new int[0], metadata);
        var file = new IndexFileMeta(provider, "index-" + field + "-" + from, bytes, to - from + 1, global, null);
        return new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, file);
    }

    private void indexes(List<IndexManifestEntry> entries) {
        when(handler.scan(eq(snapshot), org.mockito.ArgumentMatchers.<Filter<IndexManifestEntry>>any())).thenReturn(entries);
    }

    private void splits(List<Split> splits) {
        when(table.newSnapshotReader().withSnapshot(snapshot).withMode(ScanMode.ALL).read().splits()).thenReturn(splits);
    }

    private void setData(List<Range> ranges) {
        splits(List.of(dataSplit(BinaryRow.EMPTY_ROW, ranges)));
    }

    private DataSplit dataSplit(BinaryRow partition, List<Range> ranges) {
        List<DataFileMeta> files = new ArrayList<>();
        for (Range range : ranges) {
            DataFileMeta file = mock(DataFileMeta.class);
            when(file.firstRowId()).thenReturn(range.from);
            when(file.schemaId()).thenReturn(schema.id());
            when(file.rowCount()).thenReturn(range.count());
            when(file.writeCols()).thenReturn(null);
            when(file.nonNullRowIdRange()).thenReturn(range);
            files.add(file);
        }
        DataSplit split = mock(DataSplit.class);
        when(split.dataFiles()).thenReturn(files);
        when(split.partition()).thenReturn(partition);
        return split;
    }

    private void annRequest() {
        annRequest(List.of());
    }

    private void annRequest(List<String> partitionKeys) {
        schema = new TableSchema(1, List.of(DataTypes.FIELD(7, "vec", new ArrayType(false, DataTypes.FLOAT().notNull())),
                DataTypes.FIELD(9, "key", DataTypes.INT()), DataTypes.FIELD(10, "part", DataTypes.INT())),
                20, partitionKeys, List.of(), Map.of("data-evolution.enabled", "true", "row-tracking.enabled", "true"), "");
        when(table.schema()).thenReturn(schema);
        when(table.options()).thenReturn(schema.options());
        when(table.schemaManager().schema(1)).thenReturn(schema);
        when(table.schemaManager().schema(0)).thenReturn(schema(0, schema.fields()));
        request = PaimonGlobalIndexRequest.createAnn(table.location().toString(), "main", 42, schema.id(),
                schema.timeMillis(), 7, "vec", "l2", List.of(1F, 2F), 3);
    }

    private static IndexManifestEntry annIndex(long from, long to, String metric) {
        byte[] metadata = ("""
                {"index.dimension":"2","index.type":"diskann","distance.metric":"%s","encoding.type":"rawf32"}
                """.formatted(metric)).getBytes(StandardCharsets.UTF_8);
        return index(7, from, to, 100, "lumina", metadata);
    }

    private static IndexManifestEntry inPartition(IndexManifestEntry index, BinaryRow partition) {
        return new IndexManifestEntry(index.kind(), partition, index.bucket(), index.indexFile());
    }

    private static ScalarOperator keyPredicate() {
        return new IsNullPredicateOperator(true, new ColumnRefOperator(11, IntegerType.INT, "key", true));
    }

    @Test
    void testAnnRequestRequiresAnIndexOnTheBoundField() {
        annRequest();
        var query = new PaimonAnnQuery("vec", "l2", List.of(1F, 2F), 3);
        when(handler.scan(snapshot, "lumina")).thenReturn(List.of());
        assertNull(PaimonGlobalIndexPlanner.annRequest(table, 42, query));
        when(handler.scan(snapshot, "lumina")).thenReturn(List.of(annIndex(0, 19, "l2")));
        assertEquals(request.toJson(), PaimonGlobalIndexPlanner.annRequest(table, 42, query).toJson());
    }

    @Test
    void testAnnMetadataUsesPhysicalSegmentBoundaries() {
        annRequest();
        List<IndexManifestEntry> manifests = List.of(annIndex(256, 511, "l2"), annIndex(0, 255, "l2"));
        when(handler.scan(snapshot, "lumina")).thenReturn(manifests);
        setData(List.of(new Range(0, 127), new Range(128, 383), new Range(384, 511)));

        var shards = PaimonGlobalIndexPlanner.metadata(origin, request).getSplits().stream()
                .map(split -> (PaimonGlobalIndexPlanner.Shard) split).toList();
        assertEquals(2, shards.size());
        assertEquals(List.of(new Range(0, 255), new Range(256, 511)),
                shards.stream().map(shard -> new Range(shard.from(), shard.to())).toList());
        assertEquals(1, shards.get(1).files().size());
        var file = shards.get(1).files().get(0);
        assertEquals("array<float>", file.getField_type());
        assertEquals(256, file.getRow_range_from());
        assertEquals("file:/warehouse/orders/index/index-7-256", file.getPath());
    }

    @Test
    void testFilteredAnnKeepsVectorShardsWithDifferentBTreeBoundaries() {
        annRequest();
        var query = new PaimonAnnQuery("vec", "l2", List.of(1F, 2F), 3);
        setData(List.of(new Range(0, 511)));
        var vectorIndexes = List.of(annIndex(0, 255, "l2"), annIndex(256, 511, "l2"));
        var btreeIndexes = List.of(index(9, 0, 127, 50), index(9, 128, 383, 60), index(9, 384, 511, 70));
        when(handler.scan(snapshot, "lumina")).thenReturn(vectorIndexes);
        when(handler.scan(snapshot, "btree")).thenReturn(btreeIndexes);
        indexes(btreeIndexes);
        var predicate = keyPredicate();
        request = PaimonGlobalIndexPlanner.annRequest(table, 42, query, predicate, Map.of(11, "key"));
        assertNotNull(request);
        assertTrue(request.isFilteredAnn() && request.hasScalarPredicate());
        assertEquals(1, predicate.getColumnRefs().size());
        var shards = PaimonGlobalIndexPlanner.metadata(origin, request).getSplits().stream()
                .map(split -> (PaimonGlobalIndexPlanner.Shard) split).toList();
        assertEquals(2, shards.size());
        assertEquals(List.of(0L, 256L), shards.stream().map(PaimonGlobalIndexPlanner.Shard::from).toList());
        assertEquals(List.of(0L, 0L, 128L), shards.get(0).files().stream()
                .map(file -> file.getRow_range_from()).toList());
        assertEquals(List.of(256L, 128L, 384L), shards.get(1).files().stream()
                .map(file -> file.getRow_range_from()).toList());
        assertEquals(230, shards.get(1).bytes());

        when(handler.scan(snapshot, "btree")).thenReturn(List.of(btreeIndexes.get(0), btreeIndexes.get(2)));
        rejects("does not cover its vector shard");
        when(handler.scan(snapshot, "btree")).thenReturn(List.of(
                index(9, 0, 128, 50), btreeIndexes.get(1), btreeIndexes.get(2)));
        rejects("overlapping BTree ranges");
    }

    @Test
    void testFilteredAnnPartitionAndCombinedSelection() {
        annRequest(List.of("part"));
        var first = BinaryRow.singleColumn(1);
        var second = BinaryRow.singleColumn(2);
        splits(List.of(dataSplit(first, List.of(new Range(0, 255))), dataSplit(second, List.of(new Range(256, 511)))));
        var vectorIndexes = List.of(inPartition(annIndex(0, 255, "l2"), first),
                inPartition(annIndex(256, 511, "l2"), second));
        when(handler.scan(snapshot, "lumina")).thenReturn(vectorIndexes);
        var scalar = inPartition(index(9, 256, 511, 50), second);
        when(handler.scan(snapshot, "btree")).thenReturn(List.of(scalar));
        indexes(List.of(scalar));

        var query = new PaimonAnnQuery("vec", "l2", List.of(1F, 2F), 3);
        var part = new ColumnRefOperator(12, IntegerType.INT, "alias", true);
        var partitionFilter = new BinaryPredicateOperator(BinaryType.EQ, part, ConstantOperator.createInt(2));
        var names = Map.of(11, "key", 12, "part");
        var partitionOnly = PaimonGlobalIndexPlanner.annRequest(table, 42, query, partitionFilter, names);
        assertNotNull(partitionOnly);
        assertEquals(List.of(new Range(256, 511)), partitionOnly.getPartitionRanges());
        assertFalse(partitionOnly.hasScalarPredicate());
        assertEquals("alias", part.getName());
        assertEquals(1, ((PaimonGlobalIndexPlanner.Shard) PaimonGlobalIndexPlanner.metadata(origin, partitionOnly)
                .getSplits().get(0)).files().size());

        request = PaimonGlobalIndexPlanner.annRequest(table, 42, query,
                Utils.compoundAnd(partitionFilter, keyPredicate()), names);
        assertNotNull(request);
        var shards = PaimonGlobalIndexPlanner.metadata(origin, request).getSplits();
        assertEquals(1, shards.size());
        var shard = (PaimonGlobalIndexPlanner.Shard) shards.get(0);
        assertEquals(256L, shard.from());
        assertEquals(2, shard.files().size());

        var unfiltered = PaimonGlobalIndexPlanner.annRequest(table, 42, query);
        request = PaimonGlobalIndexRequest.createFilteredAnn(unfiltered, null, Map.of(), List.of(new Range(1, 511)));
        rejects("does not match complete ANN shards");
        var noPartition = new BinaryPredicateOperator(BinaryType.EQ, part, ConstantOperator.createInt(3));
        assertNull(PaimonGlobalIndexPlanner.annRequest(table, 42, query, noPartition, names));
    }

    @Test
    void testFilteredAnnDoesNotDiscardAnUnsupportedResidual() {
        annRequest();
        var query = new PaimonAnnQuery("vec", "l2", List.of(1F, 2F), 3);
        when(handler.scan(snapshot, "lumina")).thenReturn(List.of(annIndex(0, 19, "l2")));
        indexes(List.of(index(9, 0, 19, 50)));
        var key = new ColumnRefOperator(11, IntegerType.INT, "key", true);
        var notIn = new InPredicateOperator(true,
                List.of(key, ConstantOperator.createInt(1), ConstantOperator.createInt(2)));
        assertNull(PaimonGlobalIndexPlanner.annRequest(table, 42, query,
                Utils.compoundAnd(keyPredicate(), notIn), Map.of(11, "key")));
        indexes(List.of());
        assertNull(PaimonGlobalIndexPlanner.annRequest(table, 42, query, keyPredicate(), Map.of(11, "key")));
    }

    @Test
    void testAnnRejectsMissingOrExtraVisibilityAndOverlappingSegments() {
        annRequest();
        setData(List.of(new Range(50, 99)));
        when(handler.scan(snapshot, "lumina")).thenReturn(List.of(annIndex(0, 99, "l2")));
        rejects("do not exactly cover");
        when(handler.scan(snapshot, "lumina")).thenReturn(List.of(annIndex(50, 98, "l2")));
        rejects("do not exactly cover");
        when(handler.scan(snapshot, "lumina")).thenReturn(List.of(annIndex(50, 80, "l2"), annIndex(80, 99, "l2")));
        rejects("overlapping");
        when(handler.scan(snapshot, "lumina")).thenReturn(List.of(annIndex(50, 99, "cosine")));
        rejects("metadata does not match");
    }

    private void rejects(String reason) {
        Exception error = assertThrows(IllegalArgumentException.class,
                () -> PaimonGlobalIndexPlanner.metadata(origin, request));
        assertTrue(error.getMessage().contains(reason), error.getMessage());
    }

    @Test
    void testMetadataEntryRequiresInternalContextAndMatchingSnapshot() {
        var metadata = spy(new PaimonMetadata("paimon", null, null, null));
        var type = MetadataTableType.PAIMON_GLOBAL_INDEX;
        String transport = request.toTransportString();
        assertThrows(IllegalArgumentException.class,
                () -> metadata.getSerializedMetaSpec("db", "orders", 42, transport, type));
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        assertThrows(IllegalArgumentException.class,
                () -> metadata.getSerializedMetaSpec("db", "orders", 42, transport, type));
        context.setMetadataContext(true);
        assertThrows(IllegalArgumentException.class,
                () -> metadata.getSerializedMetaSpec("db", "orders", 43, transport, type));
        verify(metadata, never()).getTable(any(), any(), any());
        doReturn(origin).when(metadata).getTable(context, "db", "orders");
        PaimonGlobalIndexPlanner.Metadata result = metadata.getSerializedMetaSpec(
                "db", "orders", 42, transport, type).cast();
        assertEquals(request.toJson(), result.request().toJson());
        assertEquals(1, result.getSplits().size());
    }

    @Test
    void testShardBoundariesFilesAndByteLengths() {
        request = request(true);
        indexes(List.of(index(7, 0, 4, 100), index(7, 5, 14, 200), index(7, 15, 19, 300),
                index(9, 0, 9, 400), index(9, 10, 19, 500), index(11, 0, 19, 999)));
        var metadata = PaimonGlobalIndexPlanner.metadata(origin, request);
        assertEquals(request.getTablePath(), metadata.getTable());
        var shards = metadata.getSplits().stream().map(split -> (PaimonGlobalIndexPlanner.Shard) split).toList();
        assertEquals(List.of(new Range(0, 4), new Range(5, 9), new Range(10, 14), new Range(15, 19)),
                shards.stream().map(shard -> new Range(shard.from(), shard.to())).toList());
        assertEquals(List.of(500L, 600L, 700L, 800L), shards.stream().map(PaimonGlobalIndexPlanner.Shard::length).toList());
        for (int i = 0; i < shards.size(); i++) {
            var shard = shards.get(i);
            assertEquals(i, shard.id());
            assertEquals(request.getTablePath() + "/#global-index/42/" + i, shard.path());
            assertEquals(request.toJson(), shard.getSerializeSplit());
            assertEquals(2, shard.files().size());
            assertEquals(List.of("int", "string"), shard.files().stream().map(file -> file.getField_type()).toList());
            assertTrue(shard.files().stream().allMatch(file -> file.getMetadata().length == 9
                    && file.getPath().startsWith("file:/warehouse/orders/index/")));
        }
    }

    @Test
    void testCoverageRequiresEveryDataRange() {
        List<Range> merged = PaimonGlobalIndexPlanner.mergeRanges(List.of(
                new Range(10, 12), new Range(0, 4), new Range(5, 9), new Range(20, 21)));
        assertEquals(List.of(new Range(0, 12), new Range(20, 21)), merged);
        assertTrue(PaimonGlobalIndexPlanner.covers(merged, List.of(new Range(2, 8), new Range(20, 20))));
        assertFalse(PaimonGlobalIndexPlanner.covers(merged, List.of(new Range(2, 13))));
        assertFalse(PaimonGlobalIndexPlanner.covers(merged, List.of(new Range(22, 23))));
        assertFalse(PaimonGlobalIndexPlanner.covers(List.of(new Range(1, 19)), List.of(new Range(0, 19))));
        assertEquals(List.of(new Range(0, 4), new Range(5, 12), new Range(20, 21)),
                PaimonGlobalIndexPlanner.mergeRanges(List.of(
                        new Range(5, 9), new Range(0, 4), new Range(8, 12), new Range(20, 21)), false));
        for (List<IndexManifestEntry> entries : List.of(List.of(index(7, 0, 18, 100)),
                List.of(index(7, 0, 9, 100), index(7, 11, 19, 100)))) {
            indexes(entries);
            rejects("completely cover");
        }
        indexes(List.of(index(7, 0, 19, 100)));
        setData(List.of());
        rejects("completely cover");
    }

    @Test
    void testMissingIndexAndMixedProviders() {
        indexes(List.of(index(9, 0, 19, 100)));
        rejects("no BTree index");
        indexes(List.of(index(7, 0, 19, 100), index(7, 0, 19, 100, "bitmap", new byte[9])));
        rejects("mixed providers");
    }

    @Test
    void testInvalidFileMetadata() {
        for (IndexManifestEntry entry : List.of(index(7, 0, 19, 0), index(7, -1, 19, 100),
                index(7, 0, Long.MAX_VALUE, 100), index(7, 0, 19, 100, "btree", new byte[8]),
                index(7, 0, 19, 100, "btree", new byte[65537]))) {
            indexes(List.of(entry));
            rejects("Invalid or oversized");
        }
    }

    @Test
    void testUnsupportedDataSplitAndMissingRowId() {
        splits(List.of(mock(Split.class)));
        rejects("Unsupported Paimon data split");
        DataSplit split = mock(DataSplit.class);
        DataFileMeta file = mock(DataFileMeta.class);
        when(file.firstRowId()).thenReturn(null);
        when(split.dataFiles()).thenReturn(List.of(file));
        splits(List.of(split));
        rejects("no stable row ID");
    }

    @Test
    void testSnapshotAndReadSchemaMustRemainCompatible() {
        when(table.location()).thenReturn(new Path("file:/other"));
        rejects("table, branch or schema changed");
        when(table.location()).thenReturn(new Path(request.getTablePath()));
        when(table.options()).thenReturn(Map.of("branch", "dev"));
        rejects("table, branch or schema changed");
        when(table.options()).thenReturn(schema.options());
        when(table.schema()).thenReturn(schema(2, schema.fields()));
        rejects("table, branch or schema changed");
        when(table.schema()).thenReturn(schema);
        when(table.schemaManager().schema(1)).thenReturn(schema(1, List.of(DataTypes.FIELD(9, "s", DataTypes.STRING()))));
        rejects("incompatible key schema");
    }

    @Test
    void testSchemaHistoryAllowsRenameButRejectsEncodingChanges() {
        when(table.schemaManager().schema(0)).thenReturn(schema(0, List.of(DataTypes.FIELD(7, "old_name", DataTypes.INT()))));
        assertEquals(1, PaimonGlobalIndexPlanner.metadata(origin, request).getSplits().size());
        when(table.schemaManager().schema(0)).thenReturn(schema(0, List.of(DataTypes.FIELD(7, "k", DataTypes.BIGINT()))));
        rejects("incompatible key schema");
        when(table.schemaManager().schema(0)).thenReturn(null);
        assertThrows(IllegalStateException.class, () -> PaimonGlobalIndexPlanner.metadata(origin, request));
    }

    @Test
    void testMetadataAndRepeatedTaskPayloadBudgets() {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        context.getSessionVariable().setMaxExecMemByte(1024);
        rejects("file metadata exceeds");
        // File descriptions fit, but repeating the request per shard exceeds the task budget.
        context.getSessionVariable().setMaxExecMemByte(32 * 1024);
        List<IndexManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            entries.add(index(7, i * 5, i * 5 + 4, 100));
        }
        indexes(entries);
        rejects("task payload exceeds");
    }
}
