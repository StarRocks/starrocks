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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.BiConsumer;

class ParquetRowGroupStatisticsReaderTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    @Test
    void readsBigintStatisticsAcrossWholeFile() throws Exception {
        // Tiny block size to force the writer to split into multiple row
        // groups; the exact count is implementation-defined so the assertions
        // exercise aggregate invariants only.
        Path parquetPath = writeParquet(
                "message schema { required int64 sort_key; }",
                /*rowCount=*/ 64,
                (group, rowIndex) -> group.append("sort_key", (long) rowIndex));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                statusOf(parquetPath), newConfiguration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertFalse(stats.isEmpty());
        long totalRowCount = 0L;
        long globalMin = Long.MAX_VALUE;
        long globalMax = Long.MIN_VALUE;
        for (RowGroupStatistics rg : stats) {
            Assertions.assertNotNull(rg.getMinTuple());
            Assertions.assertNotNull(rg.getMaxTuple());
            Assertions.assertFalse(rg.isTruncated());
            long minValue = Long.parseLong(rg.getMinTuple().getValues().get(0).getStringValue());
            long maxValue = Long.parseLong(rg.getMaxTuple().getValues().get(0).getStringValue());
            Assertions.assertTrue(minValue <= maxValue,
                    "row-group min " + minValue + " > max " + maxValue);
            totalRowCount += rg.getRowCount();
            globalMin = Math.min(globalMin, minValue);
            globalMax = Math.max(globalMax, maxValue);
        }
        Assertions.assertEquals(64L, totalRowCount);
        Assertions.assertEquals(0L, globalMin);
        Assertions.assertEquals(63L, globalMax);
    }

    @Test
    void readsVarcharStatistics() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required binary tenant (UTF8); }",
                /*rowCount=*/ 16,
                (group, rowIndex) -> group.append("tenant", String.format("tenant-%02d", rowIndex)));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                statusOf(parquetPath), newConfiguration(), new Column("tenant", VarcharType.VARCHAR));

        Assertions.assertFalse(stats.isEmpty());
        String globalMin = null;
        String globalMax = null;
        for (RowGroupStatistics rg : stats) {
            // Binary stats are conservatively marked truncated so string sort keys
            // route through Tier 2 — see the class javadoc for rationale.
            Assertions.assertTrue(rg.isTruncated());
            String minValue = rg.getMinTuple().getValues().get(0).getStringValue();
            String maxValue = rg.getMaxTuple().getValues().get(0).getStringValue();
            Assertions.assertTrue(minValue.compareTo(maxValue) <= 0);
            globalMin = (globalMin == null || minValue.compareTo(globalMin) < 0) ? minValue : globalMin;
            globalMax = (globalMax == null || maxValue.compareTo(globalMax) > 0) ? maxValue : globalMax;
        }
        Assertions.assertEquals("tenant-00", globalMin);
        Assertions.assertEquals("tenant-15", globalMax);
    }

    @Test
    void dateAnnotatedColumnFallsBackToTier2() throws Exception {
        // INT32+DATE is days-since-epoch; mapping its raw int32 stats into a BIGINT
        // sort-key column would produce nonsensical boundaries. Logical annotations
        // are deferred to a follow-up commit.
        Path parquetPath = writeParquet(
                "message schema { required int32 event_day (DATE); }",
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("event_day", rowIndex + 19000));

        Assertions.assertThrows(Tier1UnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        statusOf(parquetPath), newConfiguration(),
                        new Column("event_day", IntegerType.BIGINT)));
    }

    @Test
    void unannotatedBinaryColumnFallsBackToTier2() throws Exception {
        // Parquet BINARY without a UTF8/string annotation could hold arbitrary bytes;
        // toStringUsingUTF8 would corrupt non-UTF8 data and change ordering. Tier 1
        // only admits BINARY when the string annotation is explicit.
        Path parquetPath = writeParquet(
                "message schema { required binary opaque_bytes; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("opaque_bytes", "value-" + rowIndex));

        Assertions.assertThrows(Tier1UnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        statusOf(parquetPath), newConfiguration(),
                        new Column("opaque_bytes", VarcharType.VARCHAR)));
    }

    @Test
    void readsIntStatisticsForParquetInt32() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required int32 region_id; }",
                /*rowCount=*/ 5,
                (group, rowIndex) -> group.append("region_id", rowIndex + 100));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                statusOf(parquetPath), newConfiguration(), new Column("region_id", IntegerType.INT));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertEquals("100", stats.get(0).getMinTuple().getValues().get(0).getStringValue());
        Assertions.assertEquals("104", stats.get(0).getMaxTuple().getValues().get(0).getStringValue());
    }

    @Test
    void readsBooleanStatistics() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required boolean flag; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("flag", rowIndex % 2 == 0));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                statusOf(parquetPath), newConfiguration(), new Column("flag", BooleanType.BOOLEAN));

        Assertions.assertEquals(1, stats.size());
        Assertions.assertEquals(2L, stats.get(0).getRowCount());
    }

    @Test
    void columnAbsentFromSchemaFallsBackToTier2() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required int64 other; }",
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("other", (long) rowIndex));

        Assertions.assertThrows(Tier1UnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        statusOf(parquetPath), newConfiguration(),
                        new Column("missing_sort_key", IntegerType.BIGINT)));
    }

    @Test
    void unsupportedParquetTypeFallsBackToTier2() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required double payload; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("payload", rowIndex * 1.5));

        // Even with a numeric StarRocks sort key, DOUBLE is outside the Tier-1
        // mapping window — caller should fall through to reservoir sampling.
        Assertions.assertThrows(Tier1UnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        statusOf(parquetPath), newConfiguration(),
                        new Column("payload", IntegerType.BIGINT)));
    }

    @Test
    void mismatchedStarRocksTypeFallsBackToTier2() throws Exception {
        Path parquetPath = writeParquet(
                "message schema { required int32 region_id; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("region_id", rowIndex));

        // Parquet INT32 cannot route into a VARCHAR sort-key column.
        Assertions.assertThrows(Tier1UnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        statusOf(parquetPath), newConfiguration(),
                        new Column("region_id", VarcharType.VARCHAR)));
    }

    @Test
    void caseVariantDuplicatesFallBackToTier2() throws Exception {
        // Parquet's schema spec permits sibling fields that differ only in case;
        // StarRocks column names are case-insensitive, so the reader cannot pick
        // one silently. Both must be rejected as ambiguous.
        Path parquetPath = writeParquet(
                "message schema { required int64 sort_key; required int64 SORT_KEY; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> {
                    group.append("sort_key", (long) rowIndex);
                    group.append("SORT_KEY", (long) -rowIndex);
                });

        Assertions.assertThrows(Tier1UnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        statusOf(parquetPath), newConfiguration(),
                        new Column("sort_key", IntegerType.BIGINT)));
    }

    @Test
    void outOfRangeStatsValueFallsBackToTier2() throws Exception {
        // INT64 stats (260) outside StarRocks TINYINT range. IntVariant's
        // Preconditions.checkArgument fires, the reader's catch wraps it as
        // Tier1UnavailableException so the pipeline retries with Tier 2.
        Path parquetPath = writeParquet(
                "message schema { required int64 wide_value; }",
                /*rowCount=*/ 2,
                (group, rowIndex) -> group.append("wide_value", 260L + rowIndex));

        Assertions.assertThrows(Tier1UnavailableException.class, () ->
                ParquetRowGroupStatisticsReader.read(
                        statusOf(parquetPath), newConfiguration(),
                        new Column("wide_value", IntegerType.TINYINT)));
    }

    @Test
    void allNullRowGroupReportsAbsentStatistics() throws Exception {
        // Group factory's `.append(name, value)` requires a value, so leaving
        // the optional sort_key column unset makes every row's value null.
        // Parquet then writes the row group with hasNonNullValue() == false.
        Path parquetPath = writeParquet(
                "message schema { optional int64 sort_key; required int64 keepalive; }",
                /*rowCount=*/ 3,
                (group, rowIndex) -> group.append("keepalive", (long) rowIndex));

        List<RowGroupStatistics> stats = ParquetRowGroupStatisticsReader.read(
                statusOf(parquetPath), newConfiguration(), new Column("sort_key", IntegerType.BIGINT));

        Assertions.assertEquals(1, stats.size());
        RowGroupStatistics only = stats.get(0);
        Assertions.assertEquals(3L, only.getRowCount());
        Assertions.assertNull(only.getMinTuple());
        Assertions.assertNull(only.getMaxTuple());
    }

    private Path writeParquet(String schemaText, int rowCount, BiConsumer<Group, Integer> rowFiller)
            throws IOException {
        java.nio.file.Path file = Files.createTempFile(tempDirectory, "presplit-tier1-", ".parquet");
        Path outputPath = new Path(file.toUri());
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
                .withType(schema)
                .withConf(newConfiguration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Group group = groupFactory.newGroup();
                rowFiller.accept(group, rowIndex);
                writer.write(group);
            }
        }
        return outputPath;
    }

    private static Configuration newConfiguration() {
        Configuration configuration = new Configuration();
        // Tiny page/block sizes coax the writer into emitting multiple row
        // groups even for the small fixtures the tests need.
        configuration.setLong("parquet.block.size", 256);
        configuration.setLong("parquet.page.size", 64);
        return configuration;
    }

    private static FileStatus statusOf(Path path) throws IOException {
        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(path.toUri(), new Configuration());
        return fs.getFileStatus(path);
    }
}
