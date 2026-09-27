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
import com.starrocks.common.Config;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.expression.Expr;
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
import java.util.HashMap;
import java.util.List;

class BrokerLoadRowGroupStatisticsProviderTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    private final BrokerLoadRowGroupStatisticsProvider provider = new BrokerLoadRowGroupStatisticsProvider();

    @Test
    void singleFileGroupProducesStatistics() throws Exception {
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 32, /*valueOffset=*/ 0L);

        SampleRequest request = bigintSampleRequest(
                List.of(parquetFileGroup()),
                List.of(List.of(brokerFileStatus(parquetPath))));

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        Assertions.assertEquals(32L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void compositeSortKeyProjectsAllColumns() throws Exception {
        Path parquetPath = PresplitTestSupport.writeCompositeParquetFixture(tempDirectory, /*rowCount=*/ 16);

        SampleRequest request = compositeSampleRequest(
                List.of(parquetFileGroup()),
                List.of(List.of(brokerFileStatus(parquetPath))));

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        for (RowGroupStatistics rg : rowGroupStatistics) {
            // arity 2 proves the provider forwarded the FULL sort-key list, not get(0).
            Assertions.assertEquals(2, rg.getMinTuple().getValues().size());
            Assertions.assertEquals(2, rg.getMaxTuple().getValues().size());
        }
    }

    @Test
    void derivedColumnFileGroupFallsBackToDataTier() throws Exception {
        // SET sort_key = <expr> maps the sort key, so the raw footer column diverges from the loaded
        // value. The meta path must reject it before reading footers (reusing the data tier's guard)
        // and fall back rather than emit skewed boundaries.
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 8, /*valueOffset=*/ 0L);
        BrokerFileGroup derivedGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(derivedGroup.getFileFormat()).thenReturn("parquet");
        Mockito.when(derivedGroup.getColumnExprList()).thenReturn(List.of(
                new ImportColumnDesc("sort_key", Mockito.mock(Expr.class))));

        SampleRequest request = bigintSampleRequest(
                List.of(derivedGroup), List.of(List.of(brokerFileStatus(parquetPath))));

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void identityColumnListWithDisjointPathColumnProducesStatistics() throws Exception {
        // COLUMNS (sort_key, dt) COLUMNS FROM PATH AS (dt): the sort key is still read verbatim from
        // the file, so its footer statistics describe exactly the values the load inserts. The meta
        // tier must read them rather than defer -- this is the ordinary partition-directory load.
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 32, /*valueOffset=*/ 0L);
        BrokerFileGroup pathColumnGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(pathColumnGroup.getFileFormat()).thenReturn("parquet");
        Mockito.when(pathColumnGroup.getColumnsFromPath()).thenReturn(List.of("dt"));
        Mockito.when(pathColumnGroup.getColumnExprList()).thenReturn(List.of(
                new ImportColumnDesc("sort_key"), new ImportColumnDesc("dt")));

        SampleRequest request = bigintSampleRequest(
                List.of(pathColumnGroup), List.of(List.of(brokerFileStatus(parquetPath))));

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertFalse(rowGroupStatistics.isEmpty());
        Assertions.assertEquals(32L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void pathColumnSupplyingTheSortKeyFallsBackToDataTier() throws Exception {
        // The sort key itself comes from the directory name, so no footer carries it.
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 8, /*valueOffset=*/ 0L);
        BrokerFileGroup keyFromPathGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(keyFromPathGroup.getFileFormat()).thenReturn("parquet");
        Mockito.when(keyFromPathGroup.getColumnsFromPath()).thenReturn(List.of("sort_key"));
        Mockito.when(keyFromPathGroup.getColumnExprList())
                .thenReturn(List.of(new ImportColumnDesc("sort_key")));

        SampleRequest request = bigintSampleRequest(
                List.of(keyFromPathGroup), List.of(List.of(brokerFileStatus(parquetPath))));

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void multipleFileGroupsAreAggregated() throws Exception {
        Path firstFile = writeBigintParquet(/*rowCount=*/ 16, /*valueOffset=*/ 0L);
        Path secondFile = writeBigintParquet(/*rowCount=*/ 24, /*valueOffset=*/ 1000L);

        SampleRequest request = bigintSampleRequest(
                List.of(parquetFileGroup(), parquetFileGroup()),
                List.of(List.of(brokerFileStatus(firstFile)), List.of(brokerFileStatus(secondFile))));

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertEquals(40L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void directoryEntriesAreSkipped() throws Exception {
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 8, /*valueOffset=*/ 0L);
        TBrokerFileStatus directoryEntry = new TBrokerFileStatus(
                parquetPath.getParent().toString(), /*isDir=*/ true, /*size=*/ 0L, /*isSplitable=*/ false);

        SampleRequest request = bigintSampleRequest(
                List.of(parquetFileGroup()),
                List.of(List.of(directoryEntry, brokerFileStatus(parquetPath))));

        List<RowGroupStatistics> rowGroupStatistics = provider.fetch(request);

        Assertions.assertEquals(8L, totalRowCount(rowGroupStatistics));
    }

    @Test
    void emptyFileGroupListReturnsEmptyStatistics() throws Exception {
        SampleRequest request = bigintSampleRequest(Collections.emptyList(), Collections.emptyList());
        Assertions.assertTrue(provider.fetch(request).isEmpty());
    }

    @Test
    void declaredOrcFormatProducesStatistics() throws Exception {
        // ORC is in Load.getFormatType's explicit branch. Meta tier now reads ORC
        // stripe statistics directly, so a declared-ORC group with ORC data is
        // sampled on the FE rather than routed through data tier.
        Path orcPath = writeBigintOrc(/*rowCount=*/ 16, /*valueOffset=*/ 0L);
        BrokerFileGroup orcGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(orcGroup.getFileFormat()).thenReturn("orc");

        SampleRequest request = bigintSampleRequest(
                List.of(orcGroup), List.of(List.of(brokerFileStatus(orcPath))));

        Assertions.assertEquals(16L, totalRowCount(provider.fetch(request)));
    }

    @Test
    void nullDeclaredFormatInfersParquetFromFileExtension() throws Exception {
        // Broker Load auto-detects format from the extension when FileFormat is
        // not declared. A .parquet file with a null-format group must still
        // reach meta tier — the previous strict null rejection skipped it.
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 8, /*valueOffset=*/ 0L);
        BrokerFileGroup nullFormatGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(nullFormatGroup.getFileFormat()).thenReturn(null);

        SampleRequest request = bigintSampleRequest(
                List.of(nullFormatGroup), List.of(List.of(brokerFileStatus(parquetPath))));

        Assertions.assertEquals(8L, totalRowCount(provider.fetch(request)));
    }

    @Test
    void nonParquetExtensionWithNullFormatFallsBackToDataTier() throws Exception {
        // Mirror of the previous test: null format + non-parquet extension
        // means Broker Load would have read CSV, so meta tier must defer.
        BrokerFileGroup nullFormatGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(nullFormatGroup.getFileFormat()).thenReturn(null);
        TBrokerFileStatus csvFileStatus = new TBrokerFileStatus(
                "oss://bucket/load/data.csv", /*isDir=*/ false, /*size=*/ 256L, /*isSplitable=*/ true);

        SampleRequest request = bigintSampleRequest(
                List.of(nullFormatGroup), List.of(List.of(csvFileStatus)));

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void wrongScanContextTypeFallsBackToDataTier() throws Exception {
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(
                        Mockito.mock(com.starrocks.catalog.TableFunctionTable.class),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void brokerBackedLoadFallsBackToDataTier() throws Exception {
        // Broker-backed loads route IO through the broker; FE-local Hadoop
        // access may use different filesystem/auth, so meta tier only handles
        // direct (no-broker) loads today. A future commit will route footer
        // reads through a broker-backed seekable input.
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 4, /*valueOffset=*/ 0L);
        BrokerDesc brokerBackedDesc = Mockito.mock(BrokerDesc.class);
        Mockito.when(brokerBackedDesc.hasBroker()).thenReturn(true);
        Mockito.when(brokerBackedDesc.getProperties()).thenReturn(new HashMap<>());

        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        brokerBackedDesc,
                        List.of(parquetFileGroup()),
                        List.of(List.of(brokerFileStatus(parquetPath))),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void missingBrokerDescFallsBackToDataTier() throws Exception {
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(
                        /*brokerDesc=*/ null,
                        List.of(parquetFileGroup()),
                        List.of(List.<TBrokerFileStatus>of()),
                        Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void serialAndParallelFooterReadsProduceIdenticalStatistics() throws Exception {
        // Broker Load shares the INSERT-from-FILES concurrent footer reader, so reading footers
        // concurrently must not change the result: same aggregated row count and same row-group
        // count whether parallelism is 1 (serial) or > 1 (concurrent), across file groups.
        List<BrokerFileGroup> fileGroups = List.of(parquetFileGroup(), parquetFileGroup());
        List<List<TBrokerFileStatus>> fileStatuses = List.of(
                List.of(brokerFileStatus(writeBigintParquet(16, 0L)),
                        brokerFileStatus(writeBigintParquet(24, 1000L))),
                List.of(brokerFileStatus(writeBigintParquet(40, 2000L))));

        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        try {
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = 1;
            List<RowGroupStatistics> serial = provider.fetch(bigintSampleRequest(fileGroups, fileStatuses));
            Config.tablet_pre_split_meta_tier_footer_read_parallelism = 8;
            List<RowGroupStatistics> parallel = provider.fetch(bigintSampleRequest(fileGroups, fileStatuses));

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
        // A missing file among valid ones: the concurrent footer read must preserve the
        // MetaTierUnavailableException signal so the pipeline falls back to the data tier.
        Path good = writeBigintParquet(16, 0L);
        Path missing = new Path(tempDirectory.resolve("broker-missing.parquet").toUri());
        TBrokerFileStatus missingStatus = new TBrokerFileStatus(
                missing.toString(), false, 1L, true);

        int saved = Config.tablet_pre_split_meta_tier_footer_read_parallelism;
        Config.tablet_pre_split_meta_tier_footer_read_parallelism = 8;   // force the parallel path
        try {
            SampleRequest request = bigintSampleRequest(
                    List.of(parquetFileGroup()),
                    List.of(List.of(brokerFileStatus(good), missingStatus)));
            Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
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

    private static BrokerFileGroup parquetFileGroup() {
        BrokerFileGroup fileGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(fileGroup.getFileFormat()).thenReturn("parquet");
        return fileGroup;
    }

    private SampleRequest bigintSampleRequest(
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatusesPerGroup) {
        BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
        Mockito.when(brokerDesc.hasBroker()).thenReturn(false);
        Mockito.when(brokerDesc.getProperties()).thenReturn(new HashMap<>());
        return new SampleRequest(
                new BrokerLoadScanContext(
                        brokerDesc, fileGroups, fileStatusesPerGroup, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);
    }

    private SampleRequest compositeSampleRequest(
            List<BrokerFileGroup> fileGroups, List<List<TBrokerFileStatus>> fileStatusesPerGroup) {
        BrokerDesc brokerDesc = Mockito.mock(BrokerDesc.class);
        Mockito.when(brokerDesc.hasBroker()).thenReturn(false);
        Mockito.when(brokerDesc.getProperties()).thenReturn(new HashMap<>());
        return new SampleRequest(
                new BrokerLoadScanContext(
                        brokerDesc, fileGroups, fileStatusesPerGroup, Mockito.mock(ComputeResource.class), "UTC"),
                List.of(new Column("tenant", VarcharType.VARCHAR), new Column("position", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);
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
}
