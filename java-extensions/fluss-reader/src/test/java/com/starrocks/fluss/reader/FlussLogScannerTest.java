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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlussLogScannerTest {

    @Test
    public void testPoll() throws Exception {
        TableBucket bucket = new TableBucket(1, 0);
        LogScanner logScanner = mock(LogScanner.class);
        List<ScanRecord> records = List.of(
                record(0, GenericRow.of(0)),
                record(1, GenericRow.of(1)),
                record(2, GenericRow.of(2)),
                record(3, GenericRow.of(3)));
        when(logScanner.poll(any(Duration.class))).thenReturn(
                ScanRecords.EMPTY,
                new ScanRecords(Collections.singletonMap(bucket, records)));
        FlussLogScanner scanner = newScanner(new LogSplit(bucket, null, 0, 3), logScanner);
        scanner.open();

        for (int expected = 0; expected < 3; expected++) {
            Assertions.assertTrue(scanner.hasNext());
            Assertions.assertEquals(expected, scanner.getNext().getInt(0));
        }
        Assertions.assertFalse(scanner.hasNext());

        scanner.close();
        verify(logScanner, times(2)).poll(any(Duration.class));
        verify(logScanner).close();
    }

    @Test
    public void testStoppingOffset() throws Exception {
        TableBucket bucket = new TableBucket(1, 0);
        FlussLogScanner scanner = newScanner(
                new LogSplit(bucket, null, 0, 5), mock(LogScanner.class));
        scanner.open();
        List<ScanRecord> earlyRecords = List.of(record(1, GenericRow.of(1)), record(2, GenericRow.of(2)));
        List<ScanRecord> finalRecords = List.of(record(3, GenericRow.of(3)), record(4, GenericRow.of(4)));
        List<ScanRecord> crossingRecords = List.of(
                record(3, GenericRow.of(3)),
                record(4, GenericRow.of(4)),
                record(5, GenericRow.of(5)));

        Assertions.assertSame(earlyRecords, scanner.trimToStoppingOffset(earlyRecords));
        Assertions.assertSame(finalRecords, scanner.trimToStoppingOffset(finalRecords));
        Assertions.assertEquals(
                crossingRecords.subList(0, 2),
                scanner.trimToStoppingOffset(crossingRecords));
        Assertions.assertFalse(scanner.hasNext());
        scanner.close();

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new FlussLogScanner(
                        mock(Table.class), new LogSplit(bucket, null, 0), new int[] {0}));
    }

    private static FlussLogScanner newScanner(LogSplit split, LogScanner logScanner) {
        Scan scan = mock(Scan.class);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);
        Table table = mock(Table.class);
        when(table.newScan()).thenReturn(scan);
        return new FlussLogScanner(table, split, new int[] {0});
    }

    private static ScanRecord record(long offset, InternalRow row) {
        return new ScanRecord(offset, 0, ChangeType.INSERT, row);
    }
}
