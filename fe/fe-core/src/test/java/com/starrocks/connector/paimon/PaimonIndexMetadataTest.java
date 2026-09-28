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
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexOperation;
import com.starrocks.connector.index.ConnectorIndexTableType;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.VectorIndexMetric;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Verifications;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class PaimonIndexMetadataTest {
    @Test
    public void testPaimonProviderMapping() {
        Assertions.assertEquals(ConnectorIndexType.BITMAP, PaimonMetadata.toConnectorIndexType("bitmap"));
        Assertions.assertEquals(ConnectorIndexType.RANGE, PaimonMetadata.toConnectorIndexType("btree"));
        Assertions.assertEquals(ConnectorIndexType.VECTOR, PaimonMetadata.toConnectorIndexType("lumina"));
        Assertions.assertEquals(ConnectorIndexType.VECTOR,
                PaimonMetadata.toConnectorIndexType("lumina-vector-ann"));
        for (String provider : List.of("ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann")) {
            Assertions.assertEquals(ConnectorIndexType.VECTOR,
                    PaimonMetadata.toConnectorIndexType(provider));
        }
        Assertions.assertEquals(ConnectorIndexType.FULL_TEXT, PaimonMetadata.toConnectorIndexType("full-text"));
        for (String unsupported : List.of("pk-vector", "pk-vector-ann", "pk-fulltext", "sorted",
                "pk-sorted", "lucene", "lucene-fts", "tantivy-fts", "tantivy-fulltext", "es-index")) {
            Assertions.assertNull(PaimonMetadata.toConnectorIndexType(unsupported));
        }
        Assertions.assertNull(PaimonMetadata.toConnectorIndexType(null));
        Assertions.assertNull(PaimonMetadata.toConnectorIndexType("unknown"));
    }

    @Test
    public void testSnapshotBoundDiscoveryDeduplicatesAndSharesCache(
            @Injectable FileStoreTable nativeTable,
            @Injectable RowType rowType,
            @Injectable SnapshotManager snapshotManager,
            @Injectable Snapshot snapshot,
            @Injectable FileStore<?> fileStore,
            @Injectable IndexFileHandler indexFileHandler,
            @Injectable SchemaManager schemaManager,
            @Injectable TableSchema tableSchema,
            @Injectable IndexManifestEntry firstEntry,
            @Injectable IndexManifestEntry secondEntry,
            @Injectable IndexFileMeta firstIndexFile,
            @Injectable IndexFileMeta secondIndexFile) throws FileNotFoundException {
        DataField vectorField = new DataField(7, "embedding", new IntType());
        GlobalIndexMeta globalIndex = new GlobalIndexMeta(0, 10, 7, new int[0],
                "{\"index.dimension\":\"2\",\"distance.metric\":\"l2\"}"
                        .getBytes(StandardCharsets.UTF_8));
        new Expectations() {
            {
                nativeTable.partitionKeys();
                result = Collections.emptyList();
                nativeTable.rowType();
                result = rowType;
                rowType.getFields();
                result = List.of(vectorField);
                nativeTable.snapshotManager();
                result = snapshotManager;
                snapshotManager.tryGetSnapshot(11L);
                result = snapshot;
                minTimes = 2;
                snapshot.id();
                result = 11L;
                snapshot.schemaId();
                result = 3L;
                nativeTable.schemaManager();
                result = schemaManager;
                schemaManager.schema(3L);
                result = tableSchema;
                nativeTable.store();
                result = fileStore;
                fileStore.newIndexFileHandler();
                result = indexFileHandler;
                indexFileHandler.scan(snapshot, (Filter<IndexManifestEntry>) any);
                result = List.of(firstEntry, secondEntry);
                times = 1;
                firstEntry.indexFile();
                result = firstIndexFile;
                secondEntry.indexFile();
                result = secondIndexFile;
                firstIndexFile.globalIndexMeta();
                result = globalIndex;
                secondIndexFile.globalIndexMeta();
                result = globalIndex;
                firstIndexFile.indexType();
                result = "lumina";
                secondIndexFile.indexType();
                result = "lumina";
                tableSchema.idToFieldMap();
                result = Map.of(7, vectorField);
                tableSchema.options();
                result = Map.of(
                        CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true",
                        "lumina.distance.metric", "cosine",
                        "lumina.index.dimension", "99");
                tableSchema.primaryKeys();
                result = List.of("id");
            }
        };

        PaimonTable table = new PaimonTable("paimon", "db", "tbl", Collections.emptyList(), nativeTable);
        PaimonIndexMetadataCache cache = new PaimonIndexMetadataCache(Duration.ofMinutes(1));
        ConnectorIndexMetadata first = new PaimonMetadata("paimon", null, null, null, cache)
                .getIndexMetadata(table, TvrTableSnapshot.of(11L));
        ConnectorIndexMetadata second = new PaimonMetadata("paimon", null, null, null, cache)
                .getIndexMetadata(table, TvrTableSnapshot.of(11L));
        ConnectorIndexMetadata asOf = new PaimonMetadata("paimon", null, null, null, cache)
                .getIndexMetadata(table, TvrTableDelta.of(Optional.empty(), Optional.of(11L)));

        Assertions.assertSame(first.getDescriptors().get(0), second.getDescriptors().get(0));
        Assertions.assertSame(first.getDescriptors().get(0), asOf.getDescriptors().get(0));
        Assertions.assertEquals(11L, first.getSnapshotId());
        Assertions.assertEquals(ConnectorIndexTableType.PRIMARY_KEY, first.getTableType());
        Assertions.assertEquals(1, first.getDescriptors().size());
        Assertions.assertEquals(7, first.getDescriptors().get(0).getFieldId());
        Assertions.assertEquals(VectorIndexMetric.L2,
                first.getDescriptors().get(0).getVectorMetric().orElseThrow());
        Assertions.assertEquals(2, first.getDescriptors().get(0).getVectorDimension().orElseThrow());
        new Verifications() {
            {
                // Cache-key construction must not call uuid(), whose filesystem fallback performs
                // remote status IO for catalogs without a persisted table UUID.
                nativeTable.uuid();
                times = 0;
                nativeTable.schemaManager();
                times = 1;
            }
        };
    }

    @Test
    public void testInconsistentVectorFileMetadataIsNotAdvertised(
            @Injectable FileStoreTable nativeTable,
            @Injectable RowType rowType,
            @Injectable SnapshotManager snapshotManager,
            @Injectable Snapshot snapshot,
            @Injectable FileStore<?> fileStore,
            @Injectable IndexFileHandler indexFileHandler,
            @Injectable SchemaManager schemaManager,
            @Injectable TableSchema tableSchema,
            @Injectable IndexManifestEntry firstEntry,
            @Injectable IndexManifestEntry secondEntry,
            @Injectable IndexFileMeta firstIndexFile,
            @Injectable IndexFileMeta secondIndexFile) throws FileNotFoundException {
        DataField vectorField = new DataField(7, "embedding", new IntType());
        GlobalIndexMeta l2 = new GlobalIndexMeta(0, 10, 7, new int[0],
                "{\"index.dimension\":\"2\",\"distance.metric\":\"l2\"}"
                        .getBytes(StandardCharsets.UTF_8));
        GlobalIndexMeta cosine = new GlobalIndexMeta(11, 20, 7, new int[0],
                "{\"index.dimension\":\"2\",\"distance.metric\":\"cosine\"}"
                        .getBytes(StandardCharsets.UTF_8));
        new Expectations() {
            {
                nativeTable.partitionKeys();
                result = Collections.emptyList();
                nativeTable.rowType();
                result = rowType;
                rowType.getFields();
                result = List.of(vectorField);
                nativeTable.snapshotManager();
                result = snapshotManager;
                snapshotManager.tryGetSnapshot(11L);
                result = snapshot;
                snapshot.id();
                result = 11L;
                snapshot.schemaId();
                result = 3L;
                nativeTable.schemaManager();
                result = schemaManager;
                schemaManager.schema(3L);
                result = tableSchema;
                nativeTable.store();
                result = fileStore;
                fileStore.newIndexFileHandler();
                result = indexFileHandler;
                indexFileHandler.scan(snapshot, (Filter<IndexManifestEntry>) any);
                result = List.of(firstEntry, secondEntry);
                firstEntry.indexFile();
                result = firstIndexFile;
                secondEntry.indexFile();
                result = secondIndexFile;
                firstIndexFile.globalIndexMeta();
                result = l2;
                secondIndexFile.globalIndexMeta();
                result = cosine;
                firstIndexFile.indexType();
                result = "lumina";
                secondIndexFile.indexType();
                result = "lumina";
                tableSchema.idToFieldMap();
                result = Map.of(7, vectorField);
                tableSchema.options();
                result = Map.of(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                tableSchema.primaryKeys();
                result = Collections.emptyList();
            }
        };

        PaimonTable table = new PaimonTable("paimon", "db", "tbl", Collections.emptyList(), nativeTable);
        ConnectorIndexMetadata metadata = new PaimonMetadata("paimon", null, null, null)
                .getIndexMetadata(table, TvrTableSnapshot.of(11L));

        Assertions.assertEquals(11L, metadata.getSnapshotId());
        Assertions.assertTrue(metadata.getDescriptors().isEmpty());
    }

    @Test
    public void testNativeVectorDiscoveryDoesNotUseLuminaFallback(
            @Injectable FileStoreTable nativeTable,
            @Injectable RowType rowType,
            @Injectable SnapshotManager snapshotManager,
            @Injectable Snapshot snapshot,
            @Injectable FileStore<?> fileStore,
            @Injectable IndexFileHandler indexFileHandler,
            @Injectable SchemaManager schemaManager,
            @Injectable TableSchema tableSchema,
            @Injectable IndexManifestEntry entry,
            @Injectable IndexFileMeta indexFile) throws FileNotFoundException {
        DataField vectorField = new DataField(
                7, "embedding", new org.apache.paimon.types.ArrayType(new FloatType()));
        GlobalIndexMeta globalIndex = new GlobalIndexMeta(0, 10, 7, new int[0], new byte[0]);
        new Expectations() {
            {
                nativeTable.partitionKeys();
                result = Collections.emptyList();
                nativeTable.rowType();
                result = rowType;
                rowType.getFields();
                result = List.of(vectorField);
                nativeTable.snapshotManager();
                result = snapshotManager;
                snapshotManager.tryGetSnapshot(11L);
                result = snapshot;
                snapshot.id();
                result = 11L;
                snapshot.schemaId();
                result = 3L;
                nativeTable.schemaManager();
                result = schemaManager;
                schemaManager.schema(3L);
                result = tableSchema;
                nativeTable.store();
                result = fileStore;
                fileStore.newIndexFileHandler();
                result = indexFileHandler;
                indexFileHandler.scan(snapshot, (Filter<IndexManifestEntry>) any);
                result = List.of(entry);
                entry.indexFile();
                result = indexFile;
                indexFile.globalIndexMeta();
                result = globalIndex;
                indexFile.indexType();
                result = "ivf-pq";
                tableSchema.idToFieldMap();
                result = Map.of(7, vectorField);
                tableSchema.options();
                result = Map.of(
                        CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true",
                        "lumina.distance.metric", "l2",
                        "lumina.index.dimension", "256");
                tableSchema.primaryKeys();
                result = Collections.emptyList();
            }
        };

        PaimonTable table = new PaimonTable("paimon", "db", "tbl", Collections.emptyList(), nativeTable);
        ConnectorIndexMetadata metadata = new PaimonMetadata("paimon", null, null, null)
                .getIndexMetadata(table, TvrTableSnapshot.of(11L));

        Assertions.assertEquals(1, metadata.getDescriptors().size());
        Assertions.assertEquals(VectorIndexMetric.INNER_PRODUCT,
                metadata.getDescriptors().get(0).getVectorMetric().orElseThrow());
        Assertions.assertEquals(128, metadata.getDescriptors().get(0).getVectorDimension().orElseThrow());
        Assertions.assertFalse(metadata.getDescriptors().get(0).getOptions().containsKey("lumina.distance.metric"));
        Assertions.assertFalse(metadata.getDescriptors().get(0).getOptions().containsKey("lumina.index.dimension"));
    }

    @Test
    public void testTaggedExpiredSnapshotCanStillDiscoverMetadata(
            @Injectable FileStoreTable nativeTable,
            @Injectable RowType rowType,
            @Injectable SnapshotManager snapshotManager,
            @Injectable TagManager tagManager,
            @Injectable Snapshot taggedSnapshot,
            @Injectable FileStore<?> fileStore,
            @Injectable IndexFileHandler indexFileHandler,
            @Injectable SchemaManager schemaManager,
            @Injectable TableSchema tableSchema) throws FileNotFoundException {
        new Expectations() {
            {
                nativeTable.partitionKeys();
                result = Collections.emptyList();
                nativeTable.rowType();
                result = rowType;
                rowType.getFields();
                result = Collections.emptyList();
                nativeTable.snapshotManager();
                result = snapshotManager;
                snapshotManager.tryGetSnapshot(11L);
                result = new FileNotFoundException("expired snapshot");
                nativeTable.tagManager();
                result = tagManager;
                tagManager.taggedSnapshots();
                result = List.of(taggedSnapshot);
                taggedSnapshot.id();
                result = 11L;
                taggedSnapshot.schemaId();
                result = 3L;
                nativeTable.schemaManager();
                result = schemaManager;
                schemaManager.schema(3L);
                result = tableSchema;
                tableSchema.primaryKeys();
                result = Collections.emptyList();
                tableSchema.options();
                result = Map.of(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                nativeTable.store();
                result = fileStore;
                fileStore.newIndexFileHandler();
                result = indexFileHandler;
                indexFileHandler.scan(taggedSnapshot, (Filter<IndexManifestEntry>) any);
                result = Collections.emptyList();
            }
        };

        PaimonTable table = new PaimonTable("paimon", "db", "tbl", Collections.emptyList(), nativeTable);
        ConnectorIndexMetadata metadata = new PaimonMetadata("paimon", null, null, null)
                .getIndexMetadata(table, TvrTableSnapshot.of(11L));

        Assertions.assertEquals(11L, metadata.getSnapshotId());
        Assertions.assertEquals(ConnectorIndexTableType.DATA_EVOLUTION, metadata.getTableType());
    }

    @Test
    public void testIncrementalBetweenDoesNotDiscoverIndexes(
            @Injectable FileStoreTable nativeTable,
            @Injectable RowType rowType) {
        new Expectations() {
            {
                nativeTable.partitionKeys();
                result = Collections.emptyList();
                nativeTable.rowType();
                result = rowType;
                rowType.getFields();
                result = Collections.emptyList();
            }
        };
        PaimonTable table = new PaimonTable("paimon", "db", "tbl", Collections.emptyList(), nativeTable);
        PaimonMetadata metadata = new PaimonMetadata("paimon", null, null, null);

        Assertions.assertSame(ConnectorIndexMetadata.empty(),
                metadata.getIndexMetadata(table, TvrTableDelta.of(10L, 11L)));
    }

    @Test
    public void testNegativeSnapshotSkipsNativeTableIo() {
        FileStoreTable nativeTable = mock(FileStoreTable.class);
        RowType rowType = mock(RowType.class);
        when(nativeTable.partitionKeys()).thenReturn(Collections.emptyList());
        when(nativeTable.rowType()).thenReturn(rowType);
        when(rowType.getFields()).thenReturn(Collections.emptyList());
        PaimonTable table = new PaimonTable("paimon", "db", "empty", Collections.emptyList(), nativeTable);
        clearInvocations(nativeTable, rowType);

        ConnectorIndexMetadata result = new PaimonMetadata("paimon", null, null, null)
                .getIndexMetadata(table, TvrTableSnapshot.of(-1L));

        Assertions.assertSame(ConnectorIndexMetadata.empty(), result);
        verifyNoInteractions(nativeTable, rowType);
    }

    @Test
    public void testCacheInvalidationIsScopedToTable() {
        PaimonIndexMetadataCache cache = new PaimonIndexMetadataCache(Duration.ofMinutes(1));
        PaimonIndexMetadataCacheKey firstSnapshot =
                new PaimonIndexMetadataCacheKey("paimon", "db", "tbl", "oss://bucket/tbl",
                        DEFAULT_MAIN_BRANCH, 11L, 100L, "index-11");
        PaimonIndexMetadataCacheKey secondSnapshot =
                new PaimonIndexMetadataCacheKey("paimon", "db", "tbl", "oss://bucket/tbl",
                        DEFAULT_MAIN_BRANCH, 12L, 200L, "index-12");
        PaimonIndexMetadataCacheKey branchSnapshot =
                new PaimonIndexMetadataCacheKey("paimon", "db", "tbl", "oss://bucket/tbl",
                        "dev", 11L, 100L, "index-11");
        PaimonIndexMetadataCacheKey recreatedTable =
                new PaimonIndexMetadataCacheKey("paimon", "db", "tbl", "oss://bucket/recreated",
                        DEFAULT_MAIN_BRANCH, 11L, 300L, "index-recreated");
        PaimonIndexMetadataCacheKey otherTable =
                new PaimonIndexMetadataCacheKey("paimon", "db", "other", "oss://bucket/other",
                        DEFAULT_MAIN_BRANCH, 11L, 100L, "index-11");
        AtomicInteger loads = new AtomicInteger();

        ConnectorIndexMetadata first = cache.get(firstSnapshot, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.of(11L, ConnectorIndexTableType.DATA_EVOLUTION, List.of());
        });
        Assertions.assertSame(first, cache.get(firstSnapshot, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.empty();
        }));
        ConnectorIndexMetadata second = cache.get(secondSnapshot, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.of(12L, ConnectorIndexTableType.DATA_EVOLUTION, List.of());
        });
        ConnectorIndexMetadata branch = cache.get(branchSnapshot, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.of(11L, ConnectorIndexTableType.PRIMARY_KEY, List.of());
        });
        ConnectorIndexMetadata recreated = cache.get(recreatedTable, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.of(11L, ConnectorIndexTableType.APPEND_ONLY, List.of());
        });
        ConnectorIndexMetadata other = cache.get(otherTable, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.of(11L, ConnectorIndexTableType.APPEND_ONLY, List.of());
        });
        Assertions.assertNotSame(first, second);
        Assertions.assertNotSame(first, branch);
        Assertions.assertNotSame(first, recreated);
        Assertions.assertEquals(5, loads.get());

        cache.invalidateTable("paimon", "db", "tbl");
        ConnectorIndexMetadata reloaded = cache.get(firstSnapshot, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.empty();
        });
        Assertions.assertSame(ConnectorIndexMetadata.empty(), reloaded);
        Assertions.assertSame(other, cache.get(otherTable, () -> {
            loads.incrementAndGet();
            return ConnectorIndexMetadata.empty();
        }));
        Assertions.assertEquals(6, loads.get());
    }

    @Test
    public void testFailedCacheLoadIsRetried() {
        PaimonIndexMetadataCache cache = new PaimonIndexMetadataCache(Duration.ofMinutes(1));
        PaimonIndexMetadataCacheKey key = new PaimonIndexMetadataCacheKey(
                "paimon", "db", "tbl", "oss://bucket/tbl", DEFAULT_MAIN_BRANCH,
                11L, 100L, "index-11");
        AtomicInteger loads = new AtomicInteger();

        for (int i = 0; i < 2; i++) {
            Assertions.assertThrows(IllegalStateException.class, () -> cache.get(key, () -> {
                loads.incrementAndGet();
                throw new IllegalStateException("transient failure");
            }));
        }
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void testIndexMetadataFailureLogIsRateLimited() {
        AtomicLong lastLogTime = new AtomicLong(Long.MIN_VALUE);
        Assertions.assertTrue(PaimonMetadata.shouldLogIndexMetadataFailure(lastLogTime, 1_000L));
        Assertions.assertFalse(PaimonMetadata.shouldLogIndexMetadataFailure(lastLogTime, 1_001L));
        Assertions.assertTrue(PaimonMetadata.shouldLogIndexMetadataFailure(lastLogTime, 61_000L));
    }

    @Test
    public void testFloatingPointScalarIndexesDeclareNoOperations() {
        for (ConnectorIndexType type : List.of(ConnectorIndexType.BITMAP, ConnectorIndexType.RANGE)) {
            Assertions.assertEquals(Collections.<ConnectorIndexOperation>emptySet(),
                    PaimonMetadata.supportedOperations(type, new FloatType()));
            Assertions.assertEquals(Collections.<ConnectorIndexOperation>emptySet(),
                    PaimonMetadata.supportedOperations(type, new DoubleType()));
        }
    }

    @Test
    public void testVectorMetricUsesFieldOverrideAndPaimonDefault() {
        Map<String, String> options = Map.of(
                "fields.embedding.distance.metric", "cosine",
                "fields.embedding.pk-vector.distance.metric", "l2",
                "lumina.distance.metric", "inner_product");

        Assertions.assertEquals(VectorIndexMetric.COSINE,
                PaimonMetadata.vectorMetric(options, "embedding", "lumina",
                        ConnectorIndexTableType.DATA_EVOLUTION).orElseThrow());
        Assertions.assertEquals(VectorIndexMetric.COSINE,
                PaimonMetadata.vectorMetric(options, "embedding", "lumina",
                        ConnectorIndexTableType.PRIMARY_KEY).orElseThrow());
        Assertions.assertEquals(VectorIndexMetric.INNER_PRODUCT,
                PaimonMetadata.vectorMetric(Map.of(), "embedding", "lumina",
                        ConnectorIndexTableType.DATA_EVOLUTION).orElseThrow());
        Assertions.assertTrue(PaimonMetadata.vectorMetric(
                Map.of("lumina.distance.metric", "unsupported"), "embedding", "lumina",
                ConnectorIndexTableType.DATA_EVOLUTION).isEmpty());
        Assertions.assertEquals(VectorIndexMetric.COSINE,
                PaimonMetadata.vectorMetric(options, "embedding", "unknown",
                        ConnectorIndexTableType.DATA_EVOLUTION).orElseThrow());

        // Native vector providers must not inherit Lumina's table-level fallback.
        Assertions.assertEquals(VectorIndexMetric.INNER_PRODUCT,
                PaimonMetadata.vectorMetric(Map.of("lumina.distance.metric", "l2"),
                        "embedding", "ivf-pq", ConnectorIndexTableType.DATA_EVOLUTION).orElseThrow());

        // NativeVectorGlobalIndexerFactory applies field-level options after provider-level
        // options and prefers canonical aliases when both aliases are present.
        Map<String, String> nativeOptions = Map.of(
                "ivf-pq.metric", "l2",
                "ivf-pq.distance.metric", "cosine",
                "fields.embedding.metric", "inner_product",
                "fields.embedding.distance.metric", "l2");
        Assertions.assertEquals(VectorIndexMetric.INNER_PRODUCT,
                PaimonMetadata.vectorMetric(nativeOptions, "embedding", "ivf-pq",
                        ConnectorIndexTableType.DATA_EVOLUTION).orElseThrow());
    }

    @Test
    public void testNativeVectorDimensionUsesFactoryPrecedence() {
        Map<String, String> nativeOptions = Map.of(
                "ivf-pq.dimension", "64",
                "ivf-pq.index.dimension", "32",
                "fields.embedding.dimension", "96",
                "fields.embedding.index.dimension", "48",
                "lumina.index.dimension", "256");

        Assertions.assertEquals(96,
                PaimonMetadata.vectorDimension(nativeOptions, "embedding", "ivf-pq").orElseThrow());
        Assertions.assertTrue(PaimonMetadata.vectorDimension(
                Map.of("lumina.index.dimension", "256"), "embedding", "ivf-pq").isEmpty());
    }

    @Test
    public void testNativeTableBranchSurvivesConsumedThreadLocal() {
        Assertions.assertEquals("dev", PaimonMetadata.resolveIndexBranch(
                Map.of(CoreOptions.BRANCH.key(), "dev"), DEFAULT_MAIN_BRANCH));
        Assertions.assertEquals("dev", PaimonMetadata.resolveIndexBranch(Collections.emptyMap(), "dev"));
        Assertions.assertEquals(DEFAULT_MAIN_BRANCH,
                PaimonMetadata.resolveIndexBranch(Collections.emptyMap(), DEFAULT_MAIN_BRANCH));
    }

    @Test
    public void testSnapshotSchemaDeterminesTableType(
            @Injectable TableSchema primaryKeySchema,
            @Injectable TableSchema dataEvolutionSchema,
            @Injectable TableSchema appendOnlySchema) {
        new Expectations() {
            {
                primaryKeySchema.primaryKeys();
                result = List.of("id");
                dataEvolutionSchema.primaryKeys();
                result = Collections.emptyList();
                dataEvolutionSchema.options();
                result = Map.of(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
                appendOnlySchema.primaryKeys();
                result = Collections.emptyList();
                appendOnlySchema.options();
                result = Collections.emptyMap();
            }
        };

        Assertions.assertEquals(ConnectorIndexTableType.PRIMARY_KEY,
                PaimonMetadata.toConnectorIndexTableType(primaryKeySchema));
        Assertions.assertEquals(ConnectorIndexTableType.DATA_EVOLUTION,
                PaimonMetadata.toConnectorIndexTableType(dataEvolutionSchema));
        Assertions.assertEquals(ConnectorIndexTableType.APPEND_ONLY,
                PaimonMetadata.toConnectorIndexTableType(appendOnlySchema));
    }

}
