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
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.hadoop.fs.Path;
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
    void declaredOrcFormatFallsBackToDataTier() throws Exception {
        // ORC is in Load.getFormatType's explicit branch, so it overrides
        // the file-extension fallback. Sampling Parquet footers against ORC
        // data is nonsensical, so route through data tier.
        Path parquetPath = writeBigintParquet(/*rowCount=*/ 4, /*valueOffset=*/ 0L);
        BrokerFileGroup orcGroup = Mockito.mock(BrokerFileGroup.class);
        Mockito.when(orcGroup.getFileFormat()).thenReturn("orc");

        SampleRequest request = bigintSampleRequest(
                List.of(orcGroup), List.of(List.of(brokerFileStatus(parquetPath))));

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
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
                        Mockito.mock(ComputeResource.class)),
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
                        Mockito.mock(ComputeResource.class)),
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
                        Mockito.mock(ComputeResource.class)),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(MetaTierUnavailableException.class, () -> provider.fetch(request));
    }

    private Path writeBigintParquet(int rowCount, long valueOffset) throws IOException {
        return PresplitTestSupport.writeParquetFixture(
                tempDirectory,
                "message schema { required int64 sort_key; }",
                rowCount,
                (group, rowIndex) -> group.append("sort_key", valueOffset + rowIndex));
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
                        brokerDesc, fileGroups, fileStatusesPerGroup, Mockito.mock(ComputeResource.class)),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
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
