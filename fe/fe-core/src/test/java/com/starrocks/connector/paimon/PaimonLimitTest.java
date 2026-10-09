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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableQueryAuth;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises the connector with real Paimon manifests and data, counting actual manifest opens. */
class PaimonLimitTest {
    @TempDir
    java.nio.file.Path tempDir;

    private final CountingFileIO fileIO = new CountingFileIO();
    private FileStoreTable table;
    private PaimonTable srTable;
    private PaimonMetadata metadata;

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100})
    void testLimitStopsManifestReads(int limit) throws Exception {
        createTable(true, Map.of());
        writeManifests(20);
        PaimonSplitsInfo plan = plan(limit, null);
        assertThat(fileIO.manifestReads.get()).isEqualTo(Math.min(20, (limit + 3) / 4));
        assertThat(readIds(plan, limit)).hasSize(Math.min(limit, 80)).doesNotHaveDuplicates()
                .allMatch(id -> id >= 0 && id < 80);
    }

    @Test
    void testUnpartitionedLimit() throws Exception {
        createTable(false, Map.of());
        writeManifests(20);
        PaimonSplitsInfo plan = plan(10, null);
        assertThat(fileIO.manifestReads.get()).isEqualTo(3);
        assertThat(readIds(plan, 10)).hasSize(10).doesNotHaveDuplicates();
    }

    @Test
    void testNoLimit() throws Exception {
        createTable(true, Map.of());
        writeManifests(20);
        PaimonSplitsInfo plan = plan(-1, null);
        assertThat(fileIO.manifestReads.get()).isEqualTo(20);
        assertThat(readIds(plan, 100)).hasSize(80).doesNotHaveDuplicates();
    }

    @Test
    void testDataFilterFindsLaterRows() throws Exception {
        createTable(true, Map.of());
        writeManifests(20);
        PaimonSplitsInfo plan = plan(10, predicate("id", BinaryType.GE, 76));
        assertThat(fileIO.manifestReads.get()).isEqualTo(20);
        assertThat(readIds(plan, 10)).containsExactlyInAnyOrder(76, 77, 78, 79);
    }

    @Test
    void testPartitionFilter() throws Exception {
        createTable(true, Map.of());
        writeManifests(20);
        PaimonSplitsInfo plan = plan(10, predicate("p", BinaryType.EQ, 1));
        assertThat(fileIO.manifestReads.get()).isEqualTo(3);
        assertThat(readIds(plan, 10)).hasSize(10).doesNotHaveDuplicates().allMatch(id -> (id / 4) % 2 == 1);
    }

    @Test
    void testDeletesAreMergedBeforeLimit() throws Exception {
        createTable(true, Map.of());
        writeManifests(4);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(List.of(Map.of("p", "0")));
        }
        assertThat(readIds(plan(10, null), 10)).containsExactlyInAnyOrder(4, 5, 6, 7, 12, 13, 14, 15);
    }

    @Test
    void testEmptyTable() throws Exception {
        createTable(true, Map.of());
        // No snapshot exists to pin yet; exercise the adapter without metadata's snapshot option.
        assertThat(PaimonScan.create(table, List.of(), new int[] {0, 1}, 1).plan().splits()).isEmpty();
        assertThat(fileIO.manifestReads.get()).isZero();
    }

    @Test
    void testCacheDoesNotReuseSmallerLimit() throws Exception {
        createTable(true, Map.of());
        writeManifests(20);
        assertThat(readIds(plan(1, null), 1)).hasSize(1);
        assertThat(readIds(plan(10, null), 10)).hasSize(10);
        assertThat(readIds(plan(-1, null), 100)).hasSize(80).doesNotHaveDuplicates();
        PaimonSplitsInfo repeated = plan(10, null);
        assertThat(fileIO.manifestReads.get()).isZero();
        assertThat(readIds(repeated, 10)).hasSize(10);
    }

    @Test
    void testPinnedSnapshot() throws Exception {
        createTable(true, Map.of());
        writeManifests(4);
        PaimonSplitsInfo plan = plan(10, null, 1L);
        assertThat(readIds(plan, 10)).containsExactlyInAnyOrder(0, 1, 2, 3);
        assertThat(readIds(plan(10, null), 10)).hasSize(10);
    }

    @Test
    void testManifestBatchSize() throws Exception {
        createTable(true, Map.of("scan.manifest.parallelism", "2"));
        writeManifests(20);
        PaimonSplitsInfo plan = plan(10, null);
        assertThat(fileIO.manifestReads.get()).isEqualTo(4);
        assertThat(readIds(plan, 10)).hasSize(10);
    }

    @Test
    void testPrimaryKeyUpdatesAndDeletes() throws Exception {
        createTable(false, Map.of("bucket", "1"), true);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            for (int i = 0; i < 4; i++) {
                write.write(GenericRow.of(i, 0));
            }
            commit.commit(write.prepareCommit());
        }
        builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            write.write(GenericRow.ofKind(RowKind.DELETE, 0, 0));
            write.write(GenericRow.of(1, 1));
            write.write(GenericRow.of(4, 0));
            commit.commit(write.prepareCommit());
        }
        assertThat(readIds(plan(10, null), 10)).containsExactlyInAnyOrder(1, 2, 3, 4);
        assertThat(readIncrementalIds()).containsExactlyInAnyOrder(1, 2, 3, 4);
    }

    @ParameterizedTest
    @ValueSource(strings = {"deletion-vectors.enabled", "data-evolution.enabled"})
    void testSpecialAppendTablesKeepNativePlanning(String option) throws Exception {
        createTable(true, Map.of(option, "true", "row-tracking.enabled", "true"));
        writeManifests(4);
        PaimonSplitsInfo plan = plan(10, null);
        assertThat(readIds(plan, 10)).hasSize(10).doesNotHaveDuplicates();
    }

    @Test
    void testScanPreservesQueryAuthFilter() throws Exception {
        createTable(true, Map.of("query-auth.enabled", "true"));
        writeManifests(20);
        String filter = JsonSerdeUtil.toJson(new PredicateBuilder(table.rowType()).greaterOrEqual(0, 76));
        CatalogEnvironment environment = new CatalogEnvironment(null, null, null, null, null, null, false, false) {
            @Override
            public TableQueryAuth tableQueryAuth(CoreOptions options) {
                return select -> {
                    assertThat(select).containsExactly("id", "p");
                    return new TableQueryAuthResult(List.of(filter), null);
                };
            }
        };
        table = new AppendOnlyFileStoreTable(fileIO, table.location(), table.schema(), environment);
        fileIO.manifestReads.set(0);
        // Validate the adapter directly: metadata currently assumes DataSplit when tracing metrics.
        PaimonSplitsInfo plan = new PaimonSplitsInfo(List.of(),
                PaimonScan.create(table, List.of(), new int[] {0, 1}, 10).plan().splits());
        assertThat(fileIO.manifestReads.get()).isEqualTo(20);
        assertThat(readIds(plan, 10)).containsExactlyInAnyOrder(76, 77, 78, 79);
        assertThat(readIncrementalIds()).containsExactlyInAnyOrder(76, 77, 78, 79);
    }

    private List<Integer> readIncrementalIds() throws Exception {
        long snapshot = table.latestSnapshot().get().id();
        List<Integer> result = new ArrayList<>();
        try (RemoteFileInfoSource source = new PaimonRemoteFileInfoSource(
                table.copy(Map.of("scan.snapshot-id", String.valueOf(snapshot))), List.of(), new int[] {0, 1},
                snapshot, true)) {
            while (source.hasMoreOutput()) {
                result.addAll(readIds(((PaimonRemoteFileDesc) source.getOutput().getFiles().get(0)).getPaimonSplitsInfo(), 100));
            }
        }
        return result;
    }

    @ParameterizedTest
    @CsvSource({"0, 1, 1", "0, 10, 3", "36, 1, 10", "36, 10, 12", "76, 1, 20", "76, 10, 20", "100, 10, 20"})
    void testFilteredIncrementalScan(int firstMatch, int limit, int expectedManifestReads) throws Exception {
        createTable(false, Map.of());
        writeManifests(20);
        fileIO.manifestReads.set(0);
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(List.of("id", "p"))
                .setLimit(limit).setPredicate(predicate("id", BinaryType.GE, firstMatch))
                .setTableVersionRange(TvrTableSnapshot.of(table.latestSnapshot().get().id())).build();
        RemoteFileInfoSource source = metadata.getRemoteFilesAsync(srTable, params);
        assertThat(fileIO.manifestReads.get()).as("Creating the source must not enumerate manifests").isZero();
        List<Integer> matches = new ArrayList<>();
        try (source) {
            while (matches.size() < limit && source.hasMoreOutput()) {
                PaimonSplitsInfo batch = ((PaimonRemoteFileDesc) source.getOutput().getFiles().get(0)).getPaimonSplitsInfo();
                matches.addAll(readIds(batch, limit - matches.size()));
            }
        }
        assertThat(matches).hasSize(Math.min(limit, Math.max(0, 80 - firstMatch)))
                .doesNotHaveDuplicates().allMatch(id -> id >= firstMatch && id < 80);
        assertThat(fileIO.manifestReads.get()).isEqualTo(expectedManifestReads);
        assertThat(source.hasMoreOutput()).isFalse();
        assertThat(fileIO.manifestReads.get()).as("Closing the source must not read remaining manifests")
                .isEqualTo(expectedManifestReads);
    }

    @Test
    void testIncrementalScanMergesDeletesAndPinsSnapshot() throws Exception {
        createTable(true, Map.of());
        writeManifests(4);
        long oldSnapshot = table.latestSnapshot().get().id();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(List.of(Map.of("p", "0")));
        }
        for (long snapshot : new long[] {oldSnapshot, table.latestSnapshot().get().id()}) {
            GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(List.of("id", "p"))
                    .setLimit(100).setPredicate(predicate("id", BinaryType.GE, 0))
                    .setTableVersionRange(TvrTableSnapshot.of(snapshot)).build();
            List<Integer> matches = new ArrayList<>();
            try (RemoteFileInfoSource source = metadata.getRemoteFilesAsync(srTable, params)) {
                while (source.hasMoreOutput()) {
                    matches.addAll(readIds(((PaimonRemoteFileDesc) source.getOutput().getFiles().get(0))
                            .getPaimonSplitsInfo(), 100));
                }
            }
            assertThat(matches).hasSize(snapshot == oldSnapshot ? 16 : 8).doesNotHaveDuplicates();
            if (snapshot != oldSnapshot) {
                assertThat(matches).containsExactlyInAnyOrder(4, 5, 6, 7, 12, 13, 14, 15);
            }
        }
    }

    @Test
    void testCloseBeforeFirstBatch() throws Exception {
        createTable(false, Map.of());
        writeManifests(4);
        fileIO.manifestReads.set(0);
        try (RemoteFileInfoSource source = new PaimonRemoteFileInfoSource(table, List.of(), new int[] {0, 1},
                table.latestSnapshot().get().id(), true)) {
            source.close();
            assertThat(source.hasMoreOutput()).isFalse();
            assertThat(fileIO.manifestReads.get()).isZero();
        }
    }

    private void createTable(boolean partitioned, Map<String, String> extraOptions) throws Exception {
        createTable(partitioned, extraOptions, false);
    }

    private void createTable(boolean partitioned, Map<String, String> extraOptions, boolean primaryKey) throws Exception {
        Path path = new Path(tempDir.toUri());
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT()).column("p", DataTypes.INT())
                .option("bucket", "-1").option("file.format", "avro").option("write-only", "true")
                .option("manifest.merge-min-count", "1000").option("scan.manifest.parallelism", "1");
        if (partitioned) {
            schema.partitionKeys("p");
        }
        if (primaryKey) {
            schema.primaryKey("id");
        }
        extraOptions.forEach(schema::option);
        new SchemaManager(fileIO, path).createTable(schema.build());
        table = FileStoreTableFactory.create(fileIO, path);
        Catalog catalog = mock(Catalog.class);
        when(catalog.getTable(any(Identifier.class))).thenAnswer(ignored -> table);
        srTable = new PaimonTable("paimon", "db", "t", List.of(new Column("id", IntegerType.INT),
                new Column("p", IntegerType.INT)), table);
        metadata = new PaimonMetadata("paimon", new HdfsEnvironment(), catalog,
                new ConnectorProperties(ConnectorType.PAIMON));
    }

    private void writeManifests(int count) throws Exception {
        for (int i = 0; i < count; i++) {
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                for (int j = 0; j < 4; j++) {
                    write.write(GenericRow.of(i * 4 + j, i % 2));
                }
                commit.commit(write.prepareCommit());
            }
        }
        assertThat(table.store().manifestListFactory().create().readDataManifests(table.latestSnapshot().get()))
                .hasSize(count);
    }

    private PaimonSplitsInfo plan(long limit, ScalarOperator predicate) {
        return plan(limit, predicate, table.latestSnapshot().map(snapshot -> snapshot.id()).orElse(-1L));
    }

    private PaimonSplitsInfo plan(long limit, ScalarOperator predicate, long snapshot) {
        fileIO.manifestReads.set(0);
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setFieldNames(List.of("id", "p"))
                .setLimit(limit).setPredicate(predicate).setTableVersionRange(TvrTableSnapshot.of(snapshot)).build();
        return ((PaimonRemoteFileDesc) metadata.getRemoteFiles(srTable, params).get(0).getFiles().get(0))
                .getPaimonSplitsInfo();
    }

    private ScalarOperator predicate(String field, BinaryType type, int value) {
        return new BinaryPredicateOperator(type, new ColumnRefOperator(field.equals("id") ? 1 : 2,
                IntegerType.INT, field, true), ConstantOperator.createInt(value));
    }

    private List<Integer> readIds(PaimonSplitsInfo plan, int limit) throws Exception {
        ReadBuilder read = table.newReadBuilder().withFilter(plan.getPredicate());
        List<Integer> result = new ArrayList<>();
        // Plans retain whole files. StarRocks enforces the global SQL LIMIT while consuming rows.
        try (CloseableIterator<InternalRow> rows = read.newRead().executeFilter()
                .createReader(plan.getPaimonSplits()).toCloseableIterator()) {
            while (result.size() < limit && rows.hasNext()) {
                result.add(rows.next().getInt(0));
            }
        }
        return result;
    }

    private static class CountingFileIO extends LocalFileIO {
        private static final long serialVersionUID = 1L;
        private final AtomicInteger manifestReads = new AtomicInteger();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            String name = path.getName();
            if (name.startsWith("manifest-") && !name.startsWith("manifest-list-")) {
                manifestReads.incrementAndGet();
            }
            return super.newInputStream(path);
        }
    }
}
