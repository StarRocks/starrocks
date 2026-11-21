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

package com.starrocks.fluss.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketSnapshot;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlussSplitScannerTest {

    @Test
    public void testOpen() throws Exception {
        TableBucket bucket = new TableBucket(1, 0);
        LogScanner logScanner = mock(LogScanner.class);
        InternalRow row = GenericRow.of(
                7,
                null,
                GenericRow.of(42, BinaryString.fromString("hello")));
        when(logScanner.poll(any(Duration.class))).thenReturn(new ScanRecords(
                Collections.singletonMap(
                        bucket,
                        Collections.singletonList(
                                new ScanRecord(0, 0, ChangeType.INSERT, row)))));

        TablePath tablePath = TablePath.of("db", "table");
        TableInfo tableInfo = TableInfo.of(
                tablePath,
                1,
                1,
                TableDescriptor.builder()
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("info", DataTypes.ROW(
                                        DataTypes.FIELD("f1", DataTypes.INT()),
                                        DataTypes.FIELD("f2", DataTypes.STRING())))
                                .build())
                        .distributedBy(1)
                        .build(),
                null,
                0,
                0);
        Scan scan = mock(Scan.class);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);
        Table table = mock(Table.class);
        when(table.getTableInfo()).thenReturn(tableInfo);
        when(table.newScan()).thenReturn(scan);
        Connection connection = mock(Connection.class);
        when(connection.getTable(tablePath)).thenReturn(table);

        Configuration conf = new Configuration();
        String runtimeConf = encode(conf);
        String catalogName = "test-catalog";
        String cacheKey = catalogName + ":" + Integer.toHexString(runtimeConf.hashCode());
        connectionCache().put(cacheKey, connection);
        ColumnType[][] capturedTypes = new ColumnType[1][];
        ColumnValue[] values = new ColumnValue[3];
        FlussSplitScanner scanner = new FlussSplitScanner(
                1,
                params(
                        "id,name,info",
                        "info.f2",
                        encodeSplit(new LogSplit(bucket, null, 0, 1)),
                        runtimeConf,
                        catalogName)) {
            @Override
            protected void initOffHeapTableWriter(
                    ColumnType[] requiredTypes, String[] requiredFields, int fetchSize) {
                capturedTypes[0] = requiredTypes;
                super.initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
            }

            @Override
            protected void appendData(int index, ColumnValue value) {
                values[index] = value;
            }
        };

        try {
            scanner.open();
            Assertions.assertEquals(1, scanner.getNext());
            Assertions.assertEquals(7, values[0].getInt());
            Assertions.assertNull(values[1]);
            Assertions.assertEquals(List.of("f2"), capturedTypes[0][2].getChildNames());
            Assertions.assertEquals(List.of(1), capturedTypes[0][2].getFieldIndex());
        } finally {
            scanner.close();
            connectionCache().remove(cacheKey);
        }

        verify(logScanner).subscribe(0, 0);
        verify(logScanner).close();
        verify(table).close();
    }

    @Test
    public void testEmptyBatch() throws Exception {
        InternalRow row = GenericRow.of(7);
        BatchScanner batchScanner = mock(BatchScanner.class);
        when(batchScanner.pollBatch(any(Duration.class)))
                .thenReturn(CloseableIterator.emptyIterator())
                .thenReturn(CloseableIterator.wrap(Collections.singletonList(row).iterator()))
                .thenReturn(null);
        FlussBatchScanner scanner = new FlussBatchScanner(batchScanner);
        scanner.open();

        Assertions.assertTrue(scanner.hasNext());
        Assertions.assertSame(row, scanner.getNext());
        Assertions.assertFalse(scanner.hasNext());
        scanner.close();

        verify(batchScanner, times(3)).pollBatch(any(Duration.class));
        verify(batchScanner).close();
    }

    @Test
    public void testOpenPrimaryKeySnapshotAndLog() throws Exception {
        TableBucket bucket = new TableBucket(1, 0);
        LogScanner logScanner = mock(LogScanner.class);
        InternalRow row = GenericRow.of(7, BinaryString.fromString("seven"));
        when(logScanner.poll(any(Duration.class))).thenReturn(new ScanRecords(
                Collections.singletonMap(
                        bucket,
                        Collections.singletonList(new ScanRecord(0, 0, ChangeType.INSERT, row)))));
        Scan scan = mock(Scan.class);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);
        TablePath tablePath = TablePath.of("db", "table");
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build();
        Table table = mock(Table.class);
        when(table.getTableInfo()).thenReturn(TableInfo.of(
                tablePath, 1, 1, TableDescriptor.builder().schema(schema).distributedBy(1).build(), null, 0, 0));
        when(table.newScan()).thenReturn(scan);
        Connection connection = mock(Connection.class);
        when(connection.getTable(tablePath)).thenReturn(table);

        String runtimeConf = encode(new Configuration());
        String catalogName = "test-catalog-pk";
        String cacheKey = catalogName + ":" + Integer.toHexString(runtimeConf.hashCode());
        connectionCache().put(cacheKey, connection);
        ColumnValue[] values = new ColumnValue[2];
        // No KV snapshot yet: the bucket is read from its changelog only, offsets [0, 1).
        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(bucket, null, TableBucketSnapshot.NO_SNAPSHOT_ID, 0, false, 0, 1, true);
        FlussSplitScanner scanner = new FlussSplitScanner(
                2, params("id,name", "", encodeSplit(split), runtimeConf, catalogName)) {
            @Override
            protected void appendData(int index, ColumnValue value) {
                values[index] = value;
            }
        };

        try {
            scanner.open();
            Assertions.assertEquals(1, scanner.getNext());
            Assertions.assertEquals(7, values[0].getInt());
            Assertions.assertEquals("seven", values[1].getString(ColumnType.TypeValue.STRING));
            Assertions.assertEquals(0, scanner.getNext());
        } finally {
            scanner.close();
            connectionCache().remove(cacheKey);
        }

        verify(logScanner).subscribe(0, 0L);
        verify(logScanner).close();
    }

    @Test
    public void testPrimaryKeySnapshotMergedWithLogAfterIt() throws Exception {
        // KV snapshot 5 holds ids 1 and 2 as of log offset 3; the log then updates id 1 at offset 3.
        TableBucket bucket = new TableBucket(1, 0);
        BatchScanner snapshotScanner = mock(BatchScanner.class);
        when(snapshotScanner.pollBatch(any(Duration.class)))
                .thenReturn(CloseableIterator.wrap(List.<InternalRow>of(
                        GenericRow.of(1, BinaryString.fromString("one")),
                        GenericRow.of(2, BinaryString.fromString("two"))).iterator()))
                .thenReturn(null);
        LogScanner logScanner = mock(LogScanner.class);
        when(logScanner.poll(any(Duration.class))).thenReturn(new ScanRecords(Collections.singletonMap(bucket,
                List.of(new ScanRecord(3, 0, ChangeType.UPDATE_AFTER, GenericRow.of(1, BinaryString.fromString("uno")))))));
        Scan scan = mock(Scan.class);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createBatchScanner(bucket, 5L)).thenReturn(snapshotScanner);
        when(scan.createLogScanner()).thenReturn(logScanner);
        Table table = primaryKeyTable(scan);
        HybridSnapshotLogSplit split = new HybridSnapshotLogSplit(bucket, null, 5L, 0, false, 3L, 4L, true);

        ConnectorScannerProxy scanner = FlussSplitScanner.createScanner(table, null, split, new int[] {0, 1});
        List<String> rows = new ArrayList<>();
        try {
            scanner.open();
            while (scanner.hasNext()) {
                InternalRow row = scanner.getNext();
                rows.add(row.getInt(0) + ":" + row.getString(1));
            }
        } finally {
            scanner.close();
        }

        Assertions.assertEquals(List.of("1:uno", "2:two"), rows);
        verify(scan).createBatchScanner(bucket, 5L);
        verify(logScanner).subscribe(0, 3L);
    }

    @Test
    public void testBoundedSplitsRequireStoppingOffset() {
        TableBucket bucket = new TableBucket(1, 0);
        for (SourceSplitBase split : List.of(
                new HybridSnapshotLogSplit(bucket, null, TableBucketSnapshot.NO_SNAPSHOT_ID, 0, false, 0, -1, false),
                new LakeSnapshotAndFlussLogSplit(bucket, null, null, 3, -1))) {
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> FlussSplitScanner.createScanner(null, null, split, null));
            Assertions.assertTrue(error.getMessage().contains("Stopping offset is required"), error.getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOpenSnapshotAndLog() throws Exception {
        TableBucket bucket = new TableBucket(1, 0);
        LogScanner logScanner = mock(LogScanner.class);
        Scan scan = mock(Scan.class);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);
        Table table = mock(Table.class);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build();
        when(table.getTableInfo()).thenReturn(TableInfo.of(
                TablePath.of("db", "table"), 1, 1,
                TableDescriptor.builder().schema(schema).distributedBy(1).build(), null, 0, 0));
        when(table.newScan()).thenReturn(scan);
        LakeSource<LakeSplit> lakeSource = mock(LakeSource.class);
        LakeSnapshotAndFlussLogSplit split = new LakeSnapshotAndFlussLogSplit(bucket, null, null, 3, 4);
        ConnectorScannerProxy scanner = FlussSplitScanner.createScanner(table, lakeSource, split, new int[] {1});
        try {
            scanner.open();
        } finally {
            scanner.close();
        }

        verify(scan).project(new int[] {1, 0});
        verify(lakeSource).withProject(argThat(fields -> Arrays.deepEquals(new int[][] { {1}, {0} }, fields)));
        verify(logScanner).subscribe(0, 3L);
        verify(logScanner).close();
    }

    private static Table primaryKeyTable(Scan scan) {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build();
        Table table = mock(Table.class);
        when(table.getTableInfo()).thenReturn(TableInfo.of(TablePath.of("db", "table"), 1, 1,
                TableDescriptor.builder().schema(schema).distributedBy(1).build(), null, 0, 0));
        when(table.newScan()).thenReturn(scan);
        return table;
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, Connection> connectionCache() throws Exception {
        Field field = FlussSplitScanner.class.getDeclaredField("CONNECTION_CACHE");
        field.setAccessible(true);
        return (ConcurrentHashMap<String, Connection>) field.get(null);
    }

    private static Map<String, String> params(
            String requiredFields, String nestedFields, String splitInfo, String runtimeConf, String catalogName) {
        return Map.of(
                "required_fields", requiredFields,
                "nested_fields", nestedFields,
                "split_info", splitInfo,
                "predicate_info", "",
                "runtime_conf", runtimeConf,
                "catalog_name", catalogName,
                "db_name", "db",
                "table_name", "table",
                "time_zone", "UTC");
    }

    private static String encode(Object value) throws IOException {
        return Base64.getUrlEncoder().encodeToString(InstantiationUtil.serializeObject(value));
    }

    private static String encodeSplit(SourceSplitBase split) throws IOException {
        return Base64.getUrlEncoder().encodeToString(
                new SourceSplitSerializer(null).serialize(split));
    }
}
