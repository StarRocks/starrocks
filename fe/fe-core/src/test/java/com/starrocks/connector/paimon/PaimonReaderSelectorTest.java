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

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.SessionVariable.PaimonReaderMode;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.paimon.PaimonReaderSelector.Reader.JNI;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reader.NATIVE;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reader.STARROCKS;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.DELETION_VECTOR;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.FILE_FORMAT;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.INDEXED_SPLIT;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.MERGE_FUNCTION;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.NATIVE_SUPPORTED;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.NULLABLE_MAP_KEY;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.ORC_LEGACY_TIMESTAMP;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.RAW_FILES;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.SCHEMA_EVOLUTION;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.TABLE_SCHEMA;
import static com.starrocks.connector.paimon.PaimonReaderSelector.Reason.TIMESTAMP;
import static com.starrocks.qe.SessionVariable.PaimonReaderMode.AUTO;
import static org.apache.paimon.io.DataFileMeta.DUMMY_LEVEL;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MAX_KEY;
import static org.apache.paimon.io.DataFileMeta.EMPTY_MIN_KEY;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonReaderSelectorTest {
    private static TableSchema schema(long id, DataType value, boolean pk, Map<String, String> options) {
        return new TableSchema(id, List.of(DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "v", value)), 10, List.of(), pk ? List.of("id") : List.of(), options, "");
    }

    private static DataSplit split(String format, boolean raw, long schemaId, boolean dv) {
        DataFileMeta file = DataFileMeta.create("file." + format, 100L, 10L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
                EMPTY_STATS, EMPTY_STATS, 1L, 10L, schemaId, DUMMY_LEVEL, List.of(), 0L, null,
                FileSource.APPEND, List.of(), null, null, List.of());
        var builder = DataSplit.builder().withSnapshot(1L).withPartition(BinaryRow.EMPTY_ROW).withBucket(0)
                .withBucketPath("file:///warehouse/db.db/t/bucket-0").withDataFiles(List.of(file))
                .rawConvertible(raw).isStreaming(false);
        if (dv) {
            builder.withDataDeletionFiles(List.of(new DeletionFile("dv", 0, 20, 1L)));
        }
        return builder.build();
    }

    private static PaimonReaderSelector selector(TableSchema schema, String... fields) {
        return new PaimonReaderSelector(schema, List.of(fields), id -> {
            throw new AssertionError("Unexpected schema lookup " + id);
        });
    }

    private static void assertAuto(PaimonReaderSelector selector, Split split,
                                   PaimonReaderSelector.Reader reader, PaimonReaderSelector.Reason reason) {
        assertEquals(new PaimonReaderSelector.Decision(reader, reason), selector.select(split, AUTO));
    }

    @Test
    void honorsExplicitModesAndKeepsRawFastPath() {
        var selector = selector(schema(1, DataTypes.INT(), true, Map.of("bucket", "-1")), "id", "v");
        var raw = split("parquet", true, 1, false);
        assertAuto(selector, raw, STARROCKS, RAW_FILES);
        assertEquals(JNI, selector.select(raw, PaimonReaderMode.JNI).reader());
        assertEquals(STARROCKS, selector.select(raw, PaimonReaderMode.NATIVE).reader());
        assertAuto(selector, split("parquet", false, 1, false), NATIVE, NATIVE_SUPPORTED);
        assertAuto(selector, split("avro", true, 1, false), NATIVE, NATIVE_SUPPORTED);
    }

    @Test
    void nativeErrorsDescribeDefaultAggregateFunction() {
        var selector = selector(schema(1, DataTypes.INT(), true,
                Map.of("merge-engine", "aggregation", "fields.default-aggregate-function", "product")), "v");
        var data = split("parquet", false, 1, false);
        assertAuto(selector, data, JNI, MERGE_FUNCTION);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(data, PaimonReaderMode.NATIVE));
        assertTrue(error.getMessage().contains("MERGE_FUNCTION"));
        assertTrue(error.getMessage().contains("merge-engine=aggregation"));
        assertTrue(error.getMessage().contains("fields.default-aggregate-function=product"));
    }

    @Test
    void nativeErrorsPreserveReasonWhenDiagnosticsFail() {
        var selector = selector(schema(1, DataTypes.INT(), false, Map.of("file.format", "json")), "v");
        var data = org.mockito.Mockito.spy(split("json", false, 1, false));
        org.mockito.Mockito.doThrow(new IllegalStateException("diagnostic failure")).when(data).dataFiles();
        assertAuto(selector, data, JNI, FILE_FORMAT);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(data, PaimonReaderMode.NATIVE));
        assertEquals("Paimon native reader cannot read this split: FILE_FORMAT; additional diagnostics unavailable",
                error.getMessage());
    }

    @Test
    void nativeErrorsDescribeFormatsWithoutLoadingAdditionalSchemas() {
        var selector = selector(schema(1, DataTypes.INT(), false,
                Map.of("file.format", "avro", "file.format.per.level", "0:avro,1:json")), "v");
        var data = split("avro", false, 99, false);
        assertAuto(selector, data, JNI, FILE_FORMAT);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(data, PaimonReaderMode.NATIVE));
        assertTrue(error.getMessage().contains("FILE_FORMAT"));
        assertTrue(error.getMessage().contains("file.format.per.level=0:avro,1:json"));
        assertTrue(error.getMessage().contains("file formats=[avro]"));
        // Schema 99 is intentionally unavailable: error formatting must use cached metadata only.
        assertTrue(error.getMessage().contains("file schemas=[99]"));
    }

    @Test
    void nativeErrorsDescribeCurrentAndHistoricalColumnTypes() {
        var old = schema(0, DataTypes.INT(), false, Map.of());
        var current = schema(1, DataTypes.STRING(), false, Map.of());
        AtomicInteger loads = new AtomicInteger();
        var selector = new PaimonReaderSelector(current, List.of("v"), id -> {
            loads.incrementAndGet();
            assertEquals(0, id);
            return old;
        });
        var data = split("parquet", false, 0, false);
        assertAuto(selector, data, JNI, SCHEMA_EVOLUTION);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(data, PaimonReaderMode.NATIVE));
        assertTrue(error.getMessage().contains("SCHEMA_EVOLUTION"));
        assertTrue(error.getMessage().contains("current {schema=1"));
        assertTrue(error.getMessage().contains("historical {schema=0"));
        assertTrue(error.getMessage().contains("v#1=STRING"));
        assertTrue(error.getMessage().contains("v#1=INT"));
        assertEquals(1, loads.get());
    }

    @Test
    void nativeErrorsDescribeFallbackFormats() {
        var main = org.mockito.Mockito.mock(FileStoreTable.class);
        var branch = org.mockito.Mockito.mock(FileStoreTable.class);
        org.mockito.Mockito.when(main.schema()).thenReturn(schema(1, DataTypes.INT(), false,
                Map.of("scan.fallback-branch", "archive", "file.format", "avro")));
        org.mockito.Mockito.when(branch.schema()).thenReturn(schema(2, DataTypes.INT(), false,
                Map.of("file.format", "json")));
        org.mockito.Mockito.when(main.switchToBranch("archive")).thenReturn(branch);
        var selector = new PaimonReaderSelector(main, List.of("v"));
        var data = FallbackReadFileStoreTable.toFallbackSplit(split("avro", false, 1, false), false);
        assertAuto(selector, data, JNI, FILE_FORMAT);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(data, PaimonReaderMode.NATIVE));
        assertTrue(error.getMessage().contains("fallback branch=archive"));
        assertTrue(error.getMessage().contains("fallback {schema=2"));
        assertTrue(error.getMessage().contains("file.format=json"));
        org.mockito.Mockito.verify(main, org.mockito.Mockito.times(1)).switchToBranch("archive");
    }

    @Test
    void unsupportedFormatsUseJniAndExplicitNativeReportsReason() {
        for (String format : List.of("json", "csv", "text")) {
            var selector = selector(schema(1, DataTypes.INT(), false, Map.of("file.format", format)), "v");
            var split = split(format, true, 1, false);
            assertAuto(selector, split, JNI, FILE_FORMAT);
            var error = assertThrows(StarRocksConnectorException.class,
                    () -> selector.select(split, PaimonReaderMode.NATIVE));
            assertTrue(error.getMessage().contains("FILE_FORMAT"));
        }
        var selector = selector(schema(1, DataTypes.INT(), false, Map.of()), "v");
        assertAuto(selector, split("csv", false, 1, false), JNI, FILE_FORMAT);
    }

    @Test
    void unspecifiedModeUsesAutoSelection() {
        var selector = selector(schema(1, DataTypes.INT(), false, Map.of()), "v");
        for (String format : List.of("parquet", "avro", "json")) {
            var split = split(format, true, 1, false);
            assertEquals(selector.select(split, AUTO), selector.select(split, null));
        }
    }

    @Test
    void unsupportedMergeDoesNotLoadHistoricalSchemas() {
        var schema = schema(1, DataTypes.INT(), true,
                Map.of("merge-engine", "aggregation", "fields.v.aggregate-function", "product"));
        var selector = selector(schema, "v");
        var split = split("parquet", false, 0, false);
        assertAuto(selector, split, JNI, MERGE_FUNCTION);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(split, PaimonReaderMode.NATIVE));
        assertTrue(error.getMessage().contains("MERGE_FUNCTION"));
    }

    @Test
    void cppParsesUnsupportedUnselectedTypes() {
        for (DataType type : List.of(DataTypes.TIME(), DataTypes.MULTISET(DataTypes.INT()),
                DataTypes.VECTOR(3, DataTypes.FLOAT()))) {
            var selector = selector(schema(1, type, false, Map.of()), "id");
            assertAuto(selector, split("avro", true, 1, false), JNI, TABLE_SCHEMA);
            assertEquals(JNI, selector.select(split("avro", true, 1, false), PaimonReaderMode.JNI).reader());
        }
    }

    @Test
    void historicalSchemasAreAlsoParsedBeforeProjection() {
        var current = schema(1, DataTypes.INT(), false, Map.of());
        var old = schema(0, DataTypes.TIME(), false, Map.of());
        assertAuto(new PaimonReaderSelector(current, List.of("id"), id -> old),
                split("avro", true, 0, false), JNI, TABLE_SCHEMA);
    }

    @Test
    void aggregationDependsOnProjectionAndLayout() {
        for (String function : List.of("product", "collect", "merge_map")) {
            var schema = schema(1, DataTypes.INT(), true,
                    Map.of("merge-engine", "aggregation", "fields.v.aggregate-function", function));
            assertAuto(selector(schema, "v"), split("parquet", false, 1, false), JNI, MERGE_FUNCTION);
            assertAuto(selector(schema, "id"), split("parquet", false, 1, false), NATIVE, NATIVE_SUPPORTED);
            assertAuto(selector(schema, "v"), split("avro", true, 1, false), NATIVE, NATIVE_SUPPORTED);
            assertAuto(selector(schema, "v"), split("parquet", true, 1, false), STARROCKS, RAW_FILES);
        }
        var schema = schema(1, DataTypes.INT(), true, Map.of("merge-engine", "aggregation",
                "fields.default-aggregate-function", "product", "fields.v.aggregate-function", "sum"));
        assertAuto(selector(schema, "v"), split("parquet", false, 1, false), NATIVE, NATIVE_SUPPORTED);
    }

    @Test
    void legacyRawConvertibleFilesStillNeedMergeCompatibility() {
        var schema = schema(1, DataTypes.INT(), true,
                Map.of("merge-engine", "aggregation", "fields.v.aggregate-function", "product"));
        DataFileMeta legacy = DataFileMeta.create("file.avro", 100L, 10L, EMPTY_MIN_KEY, EMPTY_MAX_KEY,
                EMPTY_STATS, EMPTY_STATS, 1L, 10L, 1L, DUMMY_LEVEL, List.of(), null, null,
                FileSource.APPEND, List.of(), null, null, List.of());
        var split = DataSplit.builder().withSnapshot(1L).withPartition(BinaryRow.EMPTY_ROW).withBucket(0)
                .withBucketPath("bucket-0").withDataFiles(List.of(legacy)).rawConvertible(true).build();
        assertAuto(selector(schema, "v"), split, JNI, MERGE_FUNCTION);
    }

    @Test
    void sequenceDependenciesDoNotDependOnOptionIterationOrder() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("merge-engine", "partial-update");
        options.put("fields.seq2.sequence-group", "seq1");
        options.put("fields.seq1.sequence-group", "v");
        var schema = new TableSchema(1, List.of(DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "v", DataTypes.INT()), DataTypes.FIELD(2, "seq1", DataTypes.INT()),
                DataTypes.FIELD(3, "seq2", DataTypes.TIMESTAMP(9))), 3, List.of(), List.of("id"), options, "");
        assertAuto(selector(schema, "v"), split("parquet", false, 1, false), JNI, TIMESTAMP);
    }

    @Test
    void compositeSequenceGroupsIncludeEveryOrderingField() {
        for (boolean timestampFirst : List.of(false, true)) {
            var schema = new TableSchema(1, List.of(DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                    DataTypes.FIELD(1, "v", DataTypes.INT()),
                    DataTypes.FIELD(2, "seq1", timestampFirst ? DataTypes.TIMESTAMP(9) : DataTypes.INT()),
                    DataTypes.FIELD(3, "seq2", timestampFirst ? DataTypes.INT() : DataTypes.TIMESTAMP(9))),
                    3, List.of(), List.of("id"), Map.of("merge-engine", "partial-update",
                    "fields.seq1,seq2.sequence-group", "v"), "");
            var selector = selector(schema, "v");
            assertAuto(selector, split("parquet", false, 1, false), JNI, TIMESTAMP);
            var error = assertThrows(StarRocksConnectorException.class,
                    () -> selector.select(split("parquet", false, 1, false), PaimonReaderMode.NATIVE));
            assertTrue(error.getMessage().contains("TIMESTAMP"));
        }

        Map<String, String> options = new LinkedHashMap<>();
        options.put("merge-engine", "partial-update");
        options.put("fields.seq0.sequence-group", "seq2");
        options.put("fields.seq1,seq2.sequence-group", "v");
        var schema = new TableSchema(1, List.of(DataTypes.FIELD(0, "id", DataTypes.INT().notNull()),
                DataTypes.FIELD(1, "v", DataTypes.INT()), DataTypes.FIELD(2, "seq1", DataTypes.INT()),
                DataTypes.FIELD(3, "seq2", DataTypes.INT()), DataTypes.FIELD(4, "seq0", DataTypes.TIMESTAMP(9))),
                4, List.of(), List.of("id"), options, "");
        assertAuto(selector(schema, "v"), split("parquet", false, 1, false), JNI, TIMESTAMP);
    }

    @Test
    void nestedEvolutionIsDifferentFromNestedReading() {
        var old = schema(0, DataTypes.ROW(DataTypes.FIELD(2, "a", DataTypes.INT())), true, Map.of());
        var current = schema(1, DataTypes.ROW(DataTypes.FIELD(2, "a", DataTypes.BIGINT())), true, Map.of());
        var selector = new PaimonReaderSelector(current, List.of("v"), id -> old);
        assertAuto(selector, split("parquet", false, 0, false), JNI, SCHEMA_EVOLUTION);
        assertAuto(selector, split("parquet", true, 0, false), JNI, SCHEMA_EVOLUTION);
        assertAuto(selector(current, "v"), split("parquet", false, 1, false), NATIVE, NATIVE_SUPPORTED);
        assertAuto(new PaimonReaderSelector(current, List.of("id"), id -> old),
                split("parquet", false, 0, false), NATIVE, NATIVE_SUPPORTED);
    }

    @Test
    void fieldIdsPreventRawRenameAndDropReaddErrors() {
        var old = schema(0, DataTypes.INT(), false, Map.of());
        for (var field : List.of(DataTypes.FIELD(1, "renamed", DataTypes.INT()),
                DataTypes.FIELD(4, "v", DataTypes.INT()))) {
            var current = new TableSchema(1, List.of(old.fields().get(0), field), 4,
                    List.of(), List.of(), Map.of(), "");
            AtomicInteger loads = new AtomicInteger();
            var selector = new PaimonReaderSelector(current, List.of(field.name()), id -> {
                loads.incrementAndGet();
                return old;
            });
            var split = split("orc", true, 0, false);
            assertAuto(selector, split, NATIVE, NATIVE_SUPPORTED);
            assertAuto(selector, split, NATIVE, NATIVE_SUPPORTED);
            assertEquals(1, loads.get());
        }
    }

    @Test
    void scalarEvolutionUsesConservativeCompatibility() {
        var old = schema(0, DataTypes.INT(), false, Map.of());
        var wider = schema(1, DataTypes.BIGINT(), false, Map.of());
        assertAuto(new PaimonReaderSelector(wider, List.of("v"), id -> old),
                split("parquet", true, 0, false), NATIVE, NATIVE_SUPPORTED);
        var text = schema(1, DataTypes.STRING(), false, Map.of());
        var selector = new PaimonReaderSelector(text, List.of("v"), id -> old);
        assertAuto(selector, split("parquet", true, 0, false), JNI, SCHEMA_EVOLUTION);
        // Explicit NATIVE and AUTO share the same conservative SDK compatibility checks.
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(split("parquet", true, 0, false), PaimonReaderMode.NATIVE));
        assertTrue(error.getMessage().contains("SCHEMA_EVOLUTION"));
    }

    @Test
    void bitmap64PreservesSafeRawAndFallsBackForSdk() {
        var schema = schema(1, DataTypes.INT(), true, Map.of("deletion-vectors.bitmap64", "true"));
        assertAuto(selector(schema, "v"), split("parquet", true, 1, true), STARROCKS, RAW_FILES);
        assertAuto(selector(schema, "v"), split("avro", true, 1, true), JNI, DELETION_VECTOR);
        var current = schema(2, DataTypes.INT(), true, Map.of());
        assertAuto(new PaimonReaderSelector(current, List.of("v"), id -> schema),
                split("avro", true, 1, true), JNI, DELETION_VECTOR);
        assertAuto(selector(current, "v"), split("avro", true, 2, true), NATIVE, NATIVE_SUPPORTED);
    }

    @Test
    void orcLegacyTimezoneMayAffectUnselectedFields() {
        var schema = schema(1, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), false, Map.of());
        assertAuto(selector(schema, "id"), split("orc", false, 1, false), JNI, ORC_LEGACY_TIMESTAMP);
        var modern = schema(1, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), false,
                Map.of("orc.timestamp-ltz.legacy.type", "false"));
        assertAuto(selector(modern, "id"), split("orc", false, 1, false), NATIVE, NATIVE_SUPPORTED);
    }

    @Test
    void autoAvoidsTimestampAndNullableMapDataRisks() {
        var timestamp = selector(schema(1, DataTypes.TIMESTAMP(6), false, Map.of()), "v");
        assertAuto(timestamp, split("parquet", true, 1, false), STARROCKS, RAW_FILES);
        assertAuto(timestamp, split("avro", true, 1, false), JNI, TIMESTAMP);
        assertThrows(StarRocksConnectorException.class,
                () -> timestamp.select(split("avro", true, 1, false), PaimonReaderMode.NATIVE));
        var nano = selector(schema(1, DataTypes.TIMESTAMP(9), false, Map.of()), "v");
        assertAuto(nano, split("parquet", true, 1, false), JNI, TIMESTAMP);
        var map = selector(schema(1, DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()), false, Map.of()), "v");
        assertAuto(map, split("parquet", true, 1, false), STARROCKS, RAW_FILES);
        assertAuto(map, split("avro", true, 1, false), JNI, NULLABLE_MAP_KEY);
        assertThrows(StarRocksConnectorException.class,
                () -> map.select(split("avro", true, 1, false), PaimonReaderMode.NATIVE));
        var nonnull = selector(schema(1, DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT()), false, Map.of()), "v");
        assertAuto(nonnull, split("avro", true, 1, false), NATIVE, NATIVE_SUPPORTED);
    }

    @Test
    void fileNeedsNativeAndImpossibleCombinationsFail() {
        var file = selector(schema(1, DataTypes.BLOB(), false, Map.of()), "v");
        assertAuto(file, split("parquet", true, 1, false), NATIVE, NATIVE_SUPPORTED);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> file.select(split("parquet", true, 1, false), PaimonReaderMode.JNI));
        assertTrue(error.getMessage().contains("Unsupported type: BLOB"));
        var impossible = schema(1, DataTypes.ROW(DataTypes.FIELD(2, "blob", DataTypes.BLOB()),
                DataTypes.FIELD(3, "time", DataTypes.TIME())), false, Map.of());
        assertThrows(StarRocksConnectorException.class,
                () -> selector(impossible, "v").select(split("parquet", true, 1, false), AUTO));
        assertEquals(JNI, selector(impossible, "id").select(split("parquet", false, 1, false), AUTO).reader());
    }

    @Test
    void variantRequiresNativeButCanBeExcluded() {
        var schema = schema(1, DataTypes.VARIANT(), false, Map.of());
        var split = split("parquet", false, 1, false);
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector(schema, "v").select(split, PaimonReaderMode.JNI));
        assertTrue(error.getMessage().contains("Reading VARIANT through the Paimon JNI reader is not yet supported"));
        assertEquals(JNI, selector(schema, "id").select(split, PaimonReaderMode.JNI).reader());
        var indexed = new IndexedSplit(split, List.of(new Range(0, 0)), null);
        assertThrows(StarRocksConnectorException.class,
                () -> selector(schema, "v").select(indexed, PaimonReaderMode.JNI));
        assertEquals(JNI, selector(schema, "id").select(indexed, PaimonReaderMode.JNI).reader());
    }

    private static void write(FileStoreTable table, int id, int value) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            writer.write(GenericRow.of(id, value));
            commit.commit(writer.prepareCommit());
        }
    }

    @Test
    void unsupportedPerLevelFormatUsesJni(@TempDir java.nio.file.Path dir) throws Exception {
        var io = LocalFileIO.create();
        var path = new Path(dir.toUri());
        new SchemaManager(io, path).createTable(new Schema(
                schema(1, DataTypes.INT(), true, Map.of()).fields(), List.of(), List.of("id"),
                Map.of("file.format", "avro", "bucket", "1", "file.format.per.level", "0:avro,1:json"), ""));
        var table = FileStoreTableFactory.create(io, path);
        write(table, 1, 7);
        var read = table.newReadBuilder();
        var splits = read.newScan().plan().splits();
        assertEquals(1, splits.size());
        var selector = new PaimonReaderSelector(table, List.of("id", "v"));
        assertAuto(selector, splits.get(0), JNI, FILE_FORMAT);
        assertThrows(StarRocksConnectorException.class, () -> selector.select(splits.get(0), PaimonReaderMode.NATIVE));
        try (var rows = new RecordReaderIterator<>(read.newRead().createReader(splits.get(0)))) {
            assertTrue(rows.hasNext());
            assertEquals(7, rows.next().getInt(1));
            assertFalse(rows.hasNext());
        }
        var supported = selector(schema(1, DataTypes.INT(), true,
                Map.of("file.format.per.level", "0:avro,1:parquet")), "v");
        assertAuto(supported, split("avro", false, 1, false), NATIVE, NATIVE_SUPPORTED);
        var raw = selector(schema(1, DataTypes.INT(), true,
                Map.of("file.format.per.level", "0:parquet,1:json")), "v");
        assertAuto(raw, split("parquet", true, 1, false), STARROCKS, RAW_FILES);
    }

    @Test
    void mainSplitChecksFallbackInitialization(@TempDir java.nio.file.Path dir) throws Exception {
        var io = LocalFileIO.create();
        var path = new Path(dir.toUri());
        var manager = new SchemaManager(io, path);
        manager.createTable(new Schema(schema(1, DataTypes.INT(), false, Map.of()).fields(),
                List.of("id"), List.of(), Map.of("file.format", "avro", "bucket", "-1"), ""));
        var main = FileStoreTableFactory.create(io, path);
        write(main, 1, 7);
        main.createBranch("archive");
        manager.copyWithBranch("archive").commitChanges(SchemaChange.setOption("file.format", "json"),
                SchemaChange.setOption("file.compression", "none"));
        write(main.switchToBranch("archive"), 2, 8);
        manager.commitChanges(SchemaChange.setOption("scan.fallback-branch", "archive"));
        var table = FileStoreTableFactory.create(io, path);
        var read = table.newReadBuilder();
        var splits = read.newScan().plan().splits();
        assertEquals(2, splits.size());
        var selector = new PaimonReaderSelector(table, List.of("id", "v"));
        var values = new java.util.ArrayList<Integer>();
        for (var split : splits) {
            assertAuto(selector, split, JNI, FILE_FORMAT);
            assertEquals(JNI, selector.select(split, PaimonReaderMode.JNI).reader());
            assertThrows(StarRocksConnectorException.class, () -> selector.select(split, PaimonReaderMode.NATIVE));
            try (var rows = new RecordReaderIterator<>(read.newRead().createReader(split))) {
                while (rows.hasNext()) {
                    values.add(rows.next().getInt(1));
                }
            }
        }
        assertEquals(List.of(7, 8), values.stream().sorted().toList());
    }

    @Test
    void fallbackInitializationChecksFormatsAndTypesOnce() {
        for (boolean unsupportedType : List.of(false, true)) {
            var main = org.mockito.Mockito.mock(FileStoreTable.class);
            var branch = org.mockito.Mockito.mock(FileStoreTable.class);
            org.mockito.Mockito.when(main.schema()).thenReturn(schema(1, DataTypes.INT(), false,
                    Map.of("scan.fallback-branch", "archive")));
            org.mockito.Mockito.when(branch.schema()).thenReturn(schema(0,
                    unsupportedType ? DataTypes.TIME() : DataTypes.INT(), false,
                    unsupportedType ? Map.of() : Map.of("file.format.per.level", "0:avro,1:json")));
            org.mockito.Mockito.when(main.switchToBranch("archive")).thenReturn(branch);
            var selector = new PaimonReaderSelector(main, List.of("id"));
            var mainSplit = FallbackReadFileStoreTable.toFallbackSplit(split("avro", false, 1, false), false);
            var reason = unsupportedType ? TABLE_SCHEMA : FILE_FORMAT;
            assertAuto(selector, mainSplit, JNI, reason);
            assertAuto(selector, mainSplit, JNI, reason);
            assertEquals(JNI, selector.select(mainSplit, PaimonReaderMode.JNI).reader());
            assertThrows(StarRocksConnectorException.class, () -> selector.select(mainSplit, PaimonReaderMode.NATIVE));
            org.mockito.Mockito.verify(main, org.mockito.Mockito.times(1)).switchToBranch("archive");
            org.mockito.Mockito.verify(branch, org.mockito.Mockito.times(1)).schema();
            assertAuto(selector, split("parquet", true, 1, false), STARROCKS, RAW_FILES);
        }
    }

    @Test
    void fallbackDataSplitResolvesBranchOnlyOnce() {
        var main = org.mockito.Mockito.mock(FileStoreTable.class);
        var branch = org.mockito.Mockito.mock(FileStoreTable.class);
        org.mockito.Mockito.when(main.schema()).thenReturn(schema(1, DataTypes.INT(), true,
                Map.of("scan.fallback-branch", "fallback")));
        org.mockito.Mockito.when(branch.schema()).thenReturn(schema(0, DataTypes.INT(), true, Map.of()));
        org.mockito.Mockito.when(main.switchToBranch("fallback")).thenReturn(branch);
        var wrapped = FallbackReadFileStoreTable.toFallbackSplit(split("parquet", false, 0, false), true);
        assertTrue(wrapped == wrapped.wrapped());
        var selector = new PaimonReaderSelector(main, List.of("id", "v"));
        assertEquals(NATIVE, selector.select((Split) wrapped, AUTO).reader());
        assertEquals(JNI, selector.select((Split) wrapped, PaimonReaderMode.JNI).reader());
        org.mockito.Mockito.verify(main, org.mockito.Mockito.times(1)).switchToBranch("fallback");
    }

    @Test
    void systemTablesStillRejectVariantThroughJni() {
        var table = org.mockito.Mockito.mock(org.apache.paimon.table.Table.class);
        org.mockito.Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT())));
        Split system = new Split() {
            @Override
            public long rowCount() {
                return 1;
            }
            @Override
            public OptionalLong mergedRowCount() {
                return OptionalLong.empty();
            }
        };
        assertThrows(StarRocksConnectorException.class,
                () -> new PaimonReaderSelector(table, List.of("v")).select(system, AUTO));
    }

    @Test
    void specialSplitRulesPreserveIndexRangesAndSystemRouting() {
        var selector = selector(schema(1, DataTypes.INT(), false, Map.of()), "id");
        Split system = new Split() {
            @Override
            public long rowCount() {
                return 1;
            }
            @Override
            public OptionalLong mergedRowCount() {
                return OptionalLong.empty();
            }
        };
        var error = assertThrows(StarRocksConnectorException.class,
                () -> selector.select(system, PaimonReaderMode.NATIVE));
        assertTrue(error.getMessage().contains("SYSTEM_SPLIT"));
        assertEquals(JNI, selector.select(system, AUTO).reader());
        assertEquals(JNI, selector.select(system, PaimonReaderMode.JNI).reader());
        var indexed = new IndexedSplit(split("parquet", true, 1, false), List.of(new Range(0, 0)), null);
        assertEquals(new PaimonReaderSelector.Decision(NATIVE, INDEXED_SPLIT),
                selector.select(indexed, AUTO));
        assertEquals(JNI, selector.select(indexed, PaimonReaderMode.JNI).reader());
    }

    @Test
    void indexedSplitsCanFallBackToJniWithoutOverridingExplicitModes() {
        var selector = selector(schema(1, DataTypes.INT(), false, Map.of("file.format", "json")), "id");
        var indexed = new IndexedSplit(split("json", false, 1, false), List.of(new Range(0, 0)), null);
        assertEquals(new PaimonReaderSelector.Decision(JNI, FILE_FORMAT), selector.select(indexed, AUTO));
        assertEquals(JNI, selector.select(indexed, PaimonReaderMode.JNI).reader());
        assertThrows(StarRocksConnectorException.class, () -> selector.select(indexed, PaimonReaderMode.NATIVE));
    }

    @Test
    void indexedFallbackProtocolIsRejectedForEveryMode() {
        var selector = selector(schema(1, DataTypes.INT(), false, Map.of()), "id");
        for (boolean fallback : List.of(false, true)) {
            var data = FallbackReadFileStoreTable.toFallbackSplit(split("parquet", true, 1, false), fallback);
            var indexed = new IndexedSplit((DataSplit) data, List.of(new Range(0, 2)), null);
            for (PaimonReaderMode mode : new PaimonReaderMode[] {null, AUTO,
                    PaimonReaderMode.JNI, PaimonReaderMode.NATIVE}) {
                var error = assertThrows(StarRocksConnectorException.class, () -> selector.select(indexed, mode));
                assertTrue(error.getMessage().contains("IndexedSplit"));
                assertTrue(error.getMessage().contains("FallbackDataSplit"));
                assertTrue(error.getMessage().contains("SPLIT_PROTOCOL"));
            }
        }
    }

    @Test
    void primaryKeyIndexedSplitFallsBackToJniAndPreservesRanges() throws Exception {
        var selector = selector(schema(1, DataTypes.INT(), true, Map.of()), "v");
        for (boolean raw : List.of(false, true)) {
            var indexed = new IndexedSplit(split("parquet", raw, 1, false),
                    List.of(new Range(0, 2), new Range(6, 7)), null);
            var decision = selector.select(indexed, AUTO);
            assertEquals(JNI, decision.reader());
            assertEquals("PRIMARY_KEY_INDEXED_SPLIT", decision.reason().name());
            assertEquals(decision, selector.select(indexed, null));
            assertEquals(JNI, selector.select(indexed, PaimonReaderMode.JNI).reader());
            var error = assertThrows(StarRocksConnectorException.class,
                    () -> selector.select(indexed, PaimonReaderMode.NATIVE));
            assertTrue(error.getMessage().contains("PRIMARY_KEY_INDEXED_SPLIT"));

            // JNI uses Java object serialization; ordinary indexed splits must retain every row range.
            var bytes = new ByteArrayOutputStream();
            try (var out = new ObjectOutputStream(bytes)) {
                out.writeObject(indexed);
            }
            try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                var restored = (IndexedSplit) in.readObject();
                assertEquals(indexed.rowRanges(), restored.rowRanges());
            }
        }
    }
}
