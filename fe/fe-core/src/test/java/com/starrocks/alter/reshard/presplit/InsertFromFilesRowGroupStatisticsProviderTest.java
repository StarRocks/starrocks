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
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

class InsertFromFilesRowGroupStatisticsProviderTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    private final InsertFromFilesRowGroupStatisticsProvider provider = new InsertFromFilesRowGroupStatisticsProvider();

    @Test
    void singleParquetFileProducesStatistics() throws Exception {
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 32, /*valueOffset=*/ 0L);

        SampleRequest request = bigintSampleRequest(
                List.of(brokerFileStatus(parquetPath)), Long.MAX_VALUE);

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        Assertions.assertEquals(32L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void multipleParquetFilesAreAggregated() throws Exception {
        Path firstFile = writeBigintParquet(/*rowCount=*/ 16, /*valueOffset=*/ 0L);
        Path secondFile = writeBigintParquet(/*rowCount=*/ 24, /*valueOffset=*/ 1000L);

        SampleRequest request = bigintSampleRequest(
                List.of(brokerFileStatus(firstFile), brokerFileStatus(secondFile)),
                Long.MAX_VALUE);

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertEquals(40L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void directoryEntriesAreSkipped() throws Exception {
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 8, /*valueOffset=*/ 0L);
        TBrokerFileStatus directoryEntry = new TBrokerFileStatus(
                parquetPath.getParent().toString(), /*isDir=*/ true, /*size=*/ 0L, /*isSplitable=*/ false);

        SampleRequest request = bigintSampleRequest(
                List.of(directoryEntry, brokerFileStatus(parquetPath)), Long.MAX_VALUE);

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertEquals(8L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void emptyFileListReturnsEmptyStatistics() throws Exception {
        SampleRequest request = bigintSampleRequest(Collections.emptyList(), Long.MAX_VALUE);
        Assertions.assertTrue(provider.fetch(request).isEmpty());
    }

    @Test
    void singleOrcFileProducesStatistics() throws Exception {
        Path orcPath = writeBigintOrc(/*rowCount=*/ 32, /*valueOffset=*/ 0L);

        TableFunctionTable orcSourceTable = mockTableFunctionTable("orc", List.of(brokerFileStatus(orcPath)));
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(orcSourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        Assertions.assertEquals(32L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void nonParquetFormatFallsBackToDataTier() throws Exception {
        TableFunctionTable csvSourceTable = mockTableFunctionTable("csv", Collections.emptyList());
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(csvSourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void compositeSortKeyProjectsAllColumns() throws Exception {
        Path parquetPath = PresplitTestSupport.writeCompositeParquetFixture(tempDirectory, /*rowCount=*/ 16);
        TableFunctionTable sourceTable = mockTableFunctionTable("parquet", List.of(brokerFileStatus(parquetPath)));
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("tenant", VarcharType.VARCHAR), new Column("position", IntegerType.BIGINT)),
                Long.MAX_VALUE, /*seed=*/ 0L);

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        for (RowGroupStatistics rg : rowGroupStatistics) {
            // arity 2 proves the provider forwarded the FULL sort-key list, not get(0).
            Assertions.assertEquals(2, rg.getMinTuple().getValues().size());
            Assertions.assertEquals(2, rg.getMaxTuple().getValues().size());
        }
    }

    @Test
    void wrongScanContextTypeFallsBackToDataTier() throws Exception {
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        null, Collections.emptyList(), Collections.emptyList(),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void footerReadParallelismOneUsesSerialPathAndAggregates() throws Exception {
        // parallelism == 1 takes the serial branch; aggregation must still cover every file.
        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        Config.tablet_pre_split_meta_tier_footer_read_parallelism = 1;
        try {
            SampleRequest request = bigintSampleRequest(
                    List.of(brokerFileStatus(writeBigintParquet(16, 0L)),
                            brokerFileStatus(writeBigintParquet(24, 1000L)),
                            brokerFileStatus(writeBigintParquet(8, 2000L))),
                    Long.MAX_VALUE);
            Assertions.assertEquals(48L, totalRowCount(provider.fetch(request)));
        } finally {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = saved;
        }
    }

    @Test
    void serialAndParallelReadsProduceIdenticalStatistics() throws Exception {
        // Reading footers concurrently must not change the result: the same aggregated row count and
        // row-group count whether parallelism is 1 (serial) or > 1 (concurrent), since futures are
        // collected in file order.
        List<TBrokerFileStatus> files = List.of(
                brokerFileStatus(writeBigintParquet(16, 0L)),
                brokerFileStatus(writeBigintParquet(24, 1000L)),
                brokerFileStatus(writeBigintParquet(40, 2000L)));

        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        try {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = 1;
            List<RowGroupStatistics> serial = provider.fetch(bigintSampleRequest(files, Long.MAX_VALUE));
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = 8;
            List<RowGroupStatistics> parallel = provider.fetch(bigintSampleRequest(files, Long.MAX_VALUE));

            Assertions.assertEquals(80L, totalRowCount(parallel));
            Assertions.assertEquals(totalRowCount(serial), totalRowCount(parallel));
            Assertions.assertEquals(serial.size(), parallel.size(),
                    "same row-group count regardless of parallelism");
        } finally {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = saved;
        }
    }

    @Test
    void unreadableFileInParallelReadFallsBackToDataTier() throws Exception {
        // A corrupt file among valid ones: the parallel footer read surfaces the per-file failure via
        // joinFooterRead as a StarRocksException, so the pipeline falls back to the data tier.
        Path good = writeBigintParquet(16, 0L);
        java.nio.file.Path corrupt = tempDirectory.resolve("corrupt.parquet");
        Files.write(corrupt, "not a parquet file".getBytes());

        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        Config.tablet_pre_split_meta_tier_footer_read_parallelism = 8;   // force the parallel path
        try {
            SampleRequest request = bigintSampleRequest(
                    List.of(brokerFileStatus(good), brokerFileStatus(new Path(corrupt.toUri()))),
                    Long.MAX_VALUE);
            Assertions.assertThrows(StarRocksException.class, () -> provider.fetch(request));
        } finally {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = saved;
        }
    }

    @Test
    void parallelReadPreservesMetaTierUnavailableSignal() throws Exception {
        // joinFooterRead rethrows a StarRocksException *subclass* unchanged. That subtype matters: a
        // MetaTierUnavailableException means "fall back to data tier", while a plain StarRocksException
        // means "skip pre-split". A footer read that raises MetaTierUnavailableException (here: the
        // sort-key column is absent from the Parquet schema) must surface through the parallel path as
        // MetaTierUnavailableException, not get downgraded to a plain StarRocksException.
        TableFunctionTable sourceTable = mockTableFunctionTable("parquet",
                List.of(brokerFileStatus(writeBigintParquet(16, 0L)),
                        brokerFileStatus(writeBigintParquet(24, 1000L))));
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("absent_sort_key", IntegerType.BIGINT)),   // not present in the file schema
                Long.MAX_VALUE, /*seed=*/ 0L);

        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        Config.tablet_pre_split_meta_tier_footer_read_parallelism = 8;   // force the parallel path
        try {
            Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
        } finally {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = saved;
        }
    }

    @Test
    void nonPositiveParallelismClampsToSerialPath() throws Exception {
        // Config <= 0 must not create a 0-thread pool: Math.max(1, ...) clamps it to the serial path,
        // which still aggregates every file.
        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        Config.tablet_pre_split_meta_tier_footer_read_parallelism = 0;
        try {
            SampleRequest request = bigintSampleRequest(
                    List.of(brokerFileStatus(writeBigintParquet(16, 0L)),
                            brokerFileStatus(writeBigintParquet(24, 1000L))),
                    Long.MAX_VALUE);
            Assertions.assertEquals(40L, totalRowCount(provider.fetch(request)));
        } finally {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = saved;
        }
    }

    @Test
    void moreFilesThanParallelismAggregatesAllViaBoundedPool() throws Exception {
        // files.size() > parallelism: the bounded pool must queue the excess tasks and still aggregate
        // every file (parallelism caps concurrency, never coverage).
        List<TBrokerFileStatus> files = List.of(
                brokerFileStatus(writeBigintParquet(4, 0L)),
                brokerFileStatus(writeBigintParquet(4, 1000L)),
                brokerFileStatus(writeBigintParquet(4, 2000L)),
                brokerFileStatus(writeBigintParquet(4, 3000L)),
                brokerFileStatus(writeBigintParquet(4, 4000L)),
                brokerFileStatus(writeBigintParquet(4, 5000L)));

        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        Config.tablet_pre_split_meta_tier_footer_read_parallelism = 2;   // fewer threads than files
        try {
            Assertions.assertEquals(24L, totalRowCount(provider.fetch(bigintSampleRequest(files, Long.MAX_VALUE))));
        } finally {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = saved;
        }
    }

    @Test
    void parallelReadPreservesFileOrder() throws Exception {
        // The parallel branch collects futures in submission order, so the aggregated stats must land
        // in the same order as the serial branch — the sampler downstream relies on a stable ordering.
        List<TBrokerFileStatus> files = List.of(
                brokerFileStatus(writeBigintParquet(16, 0L)),
                brokerFileStatus(writeBigintParquet(24, 1000L)),
                brokerFileStatus(writeBigintParquet(40, 2000L)));

        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        try {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = 1;
            List<Long> serialOrder = rowCountsInOrder(provider.fetch(bigintSampleRequest(files, Long.MAX_VALUE)));
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = 8;
            List<Long> parallelOrder = rowCountsInOrder(provider.fetch(bigintSampleRequest(files, Long.MAX_VALUE)));

            Assertions.assertEquals(serialOrder, parallelOrder, "aggregated stats must keep file order");
        } finally {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = saved;
        }
    }

    private Path writeBigintParquet(int rowCount, long valueOffset) throws IOException {
        return PresplitTestSupport.writeParquetFixture(
                tempDirectory,
                "message schema { required int64 sort_key; }",
                rowCount,
                (group, rowIndex) -> group.append("sort_key", valueOffset + rowIndex));
    }

    private Path writeBigintOrc(int rowCount, long valueOffset) throws IOException {
        return PresplitTestSupport.writeOrcFixture(
                tempDirectory,
                "struct<sort_key:bigint>",
                rowCount,
                (batch, batchRow, rowIndex) ->
                        ((LongColumnVector) batch.cols[0]).vector[batchRow] = valueOffset + rowIndex);
    }

    private SampleRequest bigintSampleRequest(List<TBrokerFileStatus> fileStatuses, long byteLimit) {
        TableFunctionTable sourceTable = mockTableFunctionTable("parquet", fileStatuses);
        return new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                byteLimit,
                /*seed=*/ 0L);
    }

    private static TableFunctionTable mockTableFunctionTable(String format, List<TBrokerFileStatus> fileStatuses) {
        TableFunctionTable table = Mockito.mock(TableFunctionTable.class);
        Mockito.when(table.getFormat()).thenReturn(format);
        Mockito.when(table.getProperties()).thenReturn(Collections.emptyMap());
        Mockito.when(table.loadFileList()).thenReturn(fileStatuses);
        return table;
    }

    private static TBrokerFileStatus brokerFileStatus(Path path) throws IOException {
        // ParquetFileReader seeks the footer using FileStatus.getLen(), so the
        // claimed size must match the on-disk file or footer-discovery fails.
        long size = Files.size(java.nio.file.Path.of(path.toUri()));
        return new TBrokerFileStatus(path.toString(), /*isDir=*/ false, size, /*isSplitable=*/ true);
    }

    private static long totalRowCount(List<RowGroupStatistics> rowGroupStatistics) {
        return rowGroupStatistics.stream().mapToLong(RowGroupStatistics::getRowCount).sum();
    }

    private static List<Long> rowCountsInOrder(List<RowGroupStatistics> rowGroupStatistics) {
        return rowGroupStatistics.stream().map(RowGroupStatistics::getRowCount).toList();
    }
}
