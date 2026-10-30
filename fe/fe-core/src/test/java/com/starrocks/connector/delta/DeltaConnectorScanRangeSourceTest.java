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

package com.starrocks.connector.delta;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.utils.FileStatus;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

// DeltaLakeTable.getDeltaMetadata() delegates to a real Delta Kernel snapshot, which this test doesn't have
// (and doesn't need - addPartition() never touches it). getDeltaMetadata() is stubbed via MockUp so the
// DeltaConnectorScanRangeSource constructor's remoteFileInputFormat lookup doesn't NPE; every other method on
// the DeltaLakeTable instance runs its real implementation.
public class DeltaConnectorScanRangeSourceTest {
    private DeltaLakeTable table;
    private ConnectContext connectContext;

    @BeforeEach
    public void setUp() {
        List<Column> columns = ImmutableList.of(
                new Column("id", IntegerType.INT, true),
                new Column("date", StringType.STRING, true));
        table = new DeltaLakeTable(1L, "delta_catalog", "delta_db", "t1", columns, ImmutableList.of("date"),
                null, null, new MetastoreTable("delta_db", "t1", "file:///tmp/delta/t1", 0));

        connectContext = new ConnectContext();
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setThreadLocalInfo();
    }

    private DeltaConnectorScanRangeSource newScanRangeSource(@Mocked Metadata metadata, @Mocked Format format) {
        new Expectations() {
            {
                metadata.getFormat();
                result = format;
                minTimes = 0;

                format.getProvider();
                result = "parquet";
                minTimes = 0;
            }
        };
        new MockUp<DeltaLakeTable>() {
            @Mock
            public Metadata getDeltaMetadata() {
                return metadata;
            }
        };
        return new DeltaConnectorScanRangeSource(table, RemoteFileInfoDefaultSource.EMPTY, PartitionIdGenerator.of());
    }

    private FileScanTask newFileScanTask(String path, String partitionValue) {
        FileStatus fileStatus = FileStatus.of(path, 1024, 0);
        return new FileScanTask(fileStatus, 1, Map.of("date", partitionValue), null);
    }

    @Test
    public void testPartitionNumLimitThrowsWhenExceeded(@Mocked Metadata metadata, @Mocked Format format) {
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(1);
        DeltaConnectorScanRangeSource scanRangeSource = newScanRangeSource(metadata, format);

        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            scanRangeSource.addPartition(newFileScanTask("/path/to/f1.parquet", "2020-01-01"));
            scanRangeSource.addPartition(newFileScanTask("/path/to/f2.parquet", "2020-01-02"));
        });
    }

    @Test
    public void testPartitionNumLimitNotExceededWithinLimit(@Mocked Metadata metadata, @Mocked Format format)
            throws Exception {
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(2);
        DeltaConnectorScanRangeSource scanRangeSource = newScanRangeSource(metadata, format);

        scanRangeSource.addPartition(newFileScanTask("/path/to/f1.parquet", "2020-01-01"));
        scanRangeSource.addPartition(newFileScanTask("/path/to/f2.parquet", "2020-01-02"));
        Assertions.assertEquals(2, scanRangeSource.selectedPartitionCount());
    }

    @Test
    public void testPartitionNumLimitDisabledByDefault(@Mocked Metadata metadata, @Mocked Format format)
            throws Exception {
        // 0 (the default) means unlimited.
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(0);
        DeltaConnectorScanRangeSource scanRangeSource = newScanRangeSource(metadata, format);

        scanRangeSource.addPartition(newFileScanTask("/path/to/f1.parquet", "2020-01-01"));
        scanRangeSource.addPartition(newFileScanTask("/path/to/f2.parquet", "2020-01-02"));
        Assertions.assertEquals(2, scanRangeSource.selectedPartitionCount());
    }

    @Test
    public void testRepeatedPartitionDoesNotCountTwice(@Mocked Metadata metadata, @Mocked Format format)
            throws Exception {
        connectContext.getSessionVariable().setScanLakePartitionNumLimit(1);
        DeltaConnectorScanRangeSource scanRangeSource = newScanRangeSource(metadata, format);

        // Two files in the same partition should not trip a limit of 1.
        scanRangeSource.addPartition(newFileScanTask("/path/to/f1.parquet", "2020-01-01"));
        scanRangeSource.addPartition(newFileScanTask("/path/to/f2.parquet", "2020-01-01"));
        Assertions.assertEquals(1, scanRangeSource.selectedPartitionCount());
    }
}
