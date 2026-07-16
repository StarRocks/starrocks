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

import com.starrocks.jni.connector.ColumnValue;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.lake.reader.LakeSnapshotAndLogSplitScanner;
import org.apache.fluss.flink.lake.reader.LakeSnapshotScanner;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitSerializer;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
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
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlussSplitScannerTest {

    @Test
    public void testOpen() throws Exception {
        TableBucket bucket = new TableBucket(1, 0);
        LogScanner logScanner = mock(LogScanner.class);
        InternalRow row = GenericRow.of(7, null);
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
                                .build())
                        .distributedBy(1)
                        .build(),
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
        ColumnValue[] values = new ColumnValue[2];
        FlussSplitScanner scanner = new FlussSplitScanner(
                1,
                params(
                        "id,name",
                        encodeSplit(new LogSplit(bucket, null, 0, 1)),
                        runtimeConf,
                        catalogName)) {
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
        assertSkipsEmptyBatch(
                new FlussSnapshotScanner(null, null),
                mock(LakeSnapshotScanner.class));
        assertSkipsEmptyBatch(
                new FlussSnapshotAndLogScanner(null, null, null, null),
                mock(LakeSnapshotAndLogSplitScanner.class));
    }

    private static void assertSkipsEmptyBatch(
            ConnectorScannerProxy scanner, BatchScanner batchScanner) throws Exception {
        InternalRow row = GenericRow.of(7);
        when(batchScanner.pollBatch(any(Duration.class)))
                .thenReturn(CloseableIterator.emptyIterator())
                .thenReturn(CloseableIterator.wrap(Collections.singletonList(row).iterator()))
                .thenReturn(null);
        setBatchScanner(scanner, batchScanner);

        Assertions.assertTrue(scanner.hasNext());
        Assertions.assertSame(row, scanner.getNext());
        Assertions.assertFalse(scanner.hasNext());
        scanner.close();

        verify(batchScanner, times(3)).pollBatch(any(Duration.class));
        verify(batchScanner).close();
    }

    private static void setBatchScanner(
            ConnectorScannerProxy scanner, BatchScanner batchScanner) throws Exception {
        Field field = scanner.getClass().getDeclaredField("batchScanner");
        field.setAccessible(true);
        field.set(scanner, batchScanner);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, Connection> connectionCache() throws Exception {
        Field field = FlussSplitScanner.class.getDeclaredField("CONNECTION_CACHE");
        field.setAccessible(true);
        return (ConcurrentHashMap<String, Connection>) field.get(null);
    }

    private static Map<String, String> params(
            String requiredFields, String splitInfo, String runtimeConf, String catalogName) {
        return Map.of(
                "required_fields", requiredFields,
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

    private static String encodeSplit(LogSplit split) throws IOException {
        return Base64.getUrlEncoder().encodeToString(
                new SourceSplitSerializer(null).serialize(split));
    }
}
