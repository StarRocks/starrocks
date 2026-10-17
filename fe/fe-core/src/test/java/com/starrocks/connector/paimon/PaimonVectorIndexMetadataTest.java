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
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import com.starrocks.connector.index.VectorIndexMetric;
import mockit.Expectations;
import mockit.Injectable;
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
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class PaimonVectorIndexMetadataTest {
    @Test
    public void testVectorProvidersExcludeDeferredFullText() {
        for (String provider : List.of("lumina", "lumina-vector-ann", "ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann")) {
            Assertions.assertEquals(IndexType.VECTOR, PaimonIndexMetadataReader.toConnectorIndexType(provider));
        }
        Assertions.assertNull(PaimonIndexMetadataReader.toConnectorIndexType("full-text"));
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
    public void testVectorMetricUsesFieldOverrideAndPaimonDefault() {
        Map<String, String> options = Map.of(
                "fields.embedding.distance.metric", "cosine",
                "fields.embedding.pk-vector.distance.metric", "l2",
                "lumina.distance.metric", "inner_product");

        Assertions.assertEquals(VectorIndexMetric.COSINE,
                PaimonIndexMetadataReader.vectorMetric(options, "embedding", "lumina",
                        TableType.DATA_EVOLUTION).orElseThrow());
        Assertions.assertEquals(VectorIndexMetric.COSINE,
                PaimonIndexMetadataReader.vectorMetric(options, "embedding", "lumina",
                        TableType.PRIMARY_KEY).orElseThrow());
        Assertions.assertEquals(VectorIndexMetric.INNER_PRODUCT,
                PaimonIndexMetadataReader.vectorMetric(Map.of(), "embedding", "lumina",
                        TableType.DATA_EVOLUTION).orElseThrow());
        Assertions.assertTrue(PaimonIndexMetadataReader.vectorMetric(
                Map.of("lumina.distance.metric", "unsupported"), "embedding", "lumina",
                TableType.DATA_EVOLUTION).isEmpty());
        Assertions.assertEquals(VectorIndexMetric.COSINE,
                PaimonIndexMetadataReader.vectorMetric(options, "embedding", "unknown",
                        TableType.DATA_EVOLUTION).orElseThrow());

        // Native vector providers must not inherit Lumina's table-level fallback.
        Assertions.assertEquals(VectorIndexMetric.INNER_PRODUCT,
                PaimonIndexMetadataReader.vectorMetric(Map.of("lumina.distance.metric", "l2"),
                        "embedding", "ivf-pq", TableType.DATA_EVOLUTION).orElseThrow());

        // NativeVectorGlobalIndexerFactory applies field-level options after provider-level
        // options and prefers canonical aliases when both aliases are present.
        Map<String, String> nativeOptions = Map.of(
                "ivf-pq.metric", "l2",
                "ivf-pq.distance.metric", "cosine",
                "fields.embedding.metric", "inner_product",
                "fields.embedding.distance.metric", "l2");
        Assertions.assertEquals(VectorIndexMetric.INNER_PRODUCT,
                PaimonIndexMetadataReader.vectorMetric(nativeOptions, "embedding", "ivf-pq",
                        TableType.DATA_EVOLUTION).orElseThrow());
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
                PaimonIndexMetadataReader.vectorDimension(nativeOptions, "embedding", "ivf-pq").orElseThrow());
        Assertions.assertTrue(PaimonIndexMetadataReader.vectorDimension(
                Map.of("lumina.index.dimension", "256"), "embedding", "ivf-pq").isEmpty());
    }
}
