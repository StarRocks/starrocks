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
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
    void nonParquetFormatFallsBackToTier2() throws Exception {
        TableFunctionTable csvSourceTable = mockTableFunctionTable("csv", Collections.emptyList());
        SampleRequest request = new SampleRequest(
                new InsertFromFilesScanContext(csvSourceTable, Mockito.mock(ComputeResource.class)),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(Tier1UnavailableException.class, () -> provider.fetch(request));
    }

    @Test
    void hadoopConfigurationDisablesFileSystemCacheForCredentialedSchemes() {
        // Hadoop's FileSystem.CACHE ignores the passed Configuration on a cache
        // hit; without disable-cache the second concurrent load with different
        // credentials would silently reuse the first load's filesystem. Pin
        // the contract for every scheme StarRocks's HdfsFsManager supports.
        Configuration hadoopConfig = InsertFromFilesRowGroupStatisticsProvider
                .buildHadoopConfiguration(Collections.emptyMap());
        for (String scheme : InsertFromFilesRowGroupStatisticsProvider.SCHEMES_TO_BUILD_FRESH_FILESYSTEM) {
            Assertions.assertTrue(
                    hadoopConfig.getBoolean("fs." + scheme + ".impl.disable.cache", /*defaultValue=*/ false),
                    "fs." + scheme + ".impl.disable.cache must be true");
        }
    }

    @Test
    void wrongScanContextTypeFallsBackToTier2() throws Exception {
        SampleRequest request = new SampleRequest(
                new BrokerLoadScanContext(null, Collections.emptyList(), Mockito.mock(ComputeResource.class)),
                List.of(new Column("sort_key", IntegerType.BIGINT)),
                Long.MAX_VALUE,
                /*seed=*/ 0L);

        Assertions.assertThrows(Tier1UnavailableException.class, () -> provider.fetch(request));
    }

    private Path writeBigintParquet(int rowCount, long valueOffset) throws IOException {
        return PresplitTestSupport.writeParquetFixture(
                tempDirectory,
                "message schema { required int64 sort_key; }",
                rowCount,
                (group, rowIndex) -> group.append("sort_key", valueOffset + rowIndex));
    }

    private SampleRequest bigintSampleRequest(List<TBrokerFileStatus> fileStatuses, long byteLimit) {
        TableFunctionTable sourceTable = mockTableFunctionTable("parquet", fileStatuses);
        return new SampleRequest(
                new InsertFromFilesScanContext(sourceTable, Mockito.mock(ComputeResource.class)),
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
}
