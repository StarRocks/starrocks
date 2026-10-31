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

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StallGuardedLogScannerTest {
    private static final TableBucket BUCKET = new TableBucket(1, 0);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);

    @Test
    public void testEmptyPollsFailTheRead() {
        AtomicLong clock = new AtomicLong();
        LogScanner delegate = mock(LogScanner.class);
        when(delegate.poll(any(Duration.class))).thenAnswer(invocation -> {
            clock.addAndGet(Duration.ofSeconds(30).toNanos());
            return ScanRecords.EMPTY;
        });
        LogScanner scanner = new StallGuardedLogScanner(delegate, "the read", Duration.ofMinutes(2), clock::get);

        for (int i = 0; i < 3; i++) {
            Assertions.assertTrue(scanner.poll(POLL_TIMEOUT).isEmpty());
        }
        IllegalStateException error =
                Assertions.assertThrows(IllegalStateException.class, () -> scanner.poll(POLL_TIMEOUT));
        Assertions.assertTrue(error.getMessage().startsWith("Read nothing from the read in 120 seconds of polling."),
                error.getMessage());
    }

    @Test
    public void testProgressRestartsTheLimit() {
        ScanRecords rows = new ScanRecords(Collections.singletonMap(
                BUCKET, List.of(new ScanRecord(0, 0, ChangeType.INSERT, GenericRow.of(0)))));
        // A poll can advance the offset without returning rows.
        ScanRecords offsetOnly = new ScanRecords(Collections.emptyMap(), Collections.singletonMap(BUCKET, 5L));
        Deque<ScanRecords> polls = new ArrayDeque<>(List.of(
                ScanRecords.EMPTY, rows, ScanRecords.EMPTY, offsetOnly, ScanRecords.EMPTY, ScanRecords.EMPTY));
        AtomicLong clock = new AtomicLong();
        LogScanner delegate = mock(LogScanner.class);
        when(delegate.poll(any(Duration.class))).thenAnswer(invocation -> {
            clock.addAndGet(Duration.ofSeconds(90).toNanos());
            return polls.poll();
        });
        LogScanner scanner = new StallGuardedLogScanner(delegate, "the read", Duration.ofMinutes(2), clock::get);

        // Each poll takes 90 s: no gap without progress reaches 120 s until the last two empty polls.
        for (int i = 0; i < 5; i++) {
            scanner.poll(POLL_TIMEOUT);
        }
        Assertions.assertThrows(IllegalStateException.class, () -> scanner.poll(POLL_TIMEOUT));
    }

    @Test
    public void testTimeBetweenPollsDoesNotCount() {
        AtomicLong clock = new AtomicLong();
        LogScanner delegate = mock(LogScanner.class);
        when(delegate.poll(any(Duration.class))).thenAnswer(invocation -> {
            clock.addAndGet(Duration.ofSeconds(50).toNanos());
            return ScanRecords.EMPTY;
        });
        LogScanner scanner = new StallGuardedLogScanner(delegate, "the read", Duration.ofMinutes(2), clock::get);

        // The caller pauses between polls, for example while downstream operators are busy.
        scanner.poll(POLL_TIMEOUT);
        clock.addAndGet(Duration.ofMinutes(10).toNanos());
        scanner.poll(POLL_TIMEOUT);
        clock.addAndGet(Duration.ofMinutes(10).toNanos());
        Assertions.assertThrows(IllegalStateException.class, () -> scanner.poll(POLL_TIMEOUT));
    }

    @Test
    public void testDelegates() throws Exception {
        LogScanner delegate = mock(LogScanner.class);
        LogScanner scanner = new StallGuardedLogScanner(delegate, "the read", Duration.ofMinutes(2));
        scanner.subscribe(0, 3);
        scanner.subscribe(7, 0, 3);
        scanner.unsubscribe(0);
        scanner.unsubscribe(7, 0);
        scanner.wakeup();
        scanner.close();

        verify(delegate).subscribe(0, 3);
        verify(delegate).subscribe(7, 0, 3);
        verify(delegate).unsubscribe(0);
        verify(delegate).unsubscribe(7, 0);
        verify(delegate).wakeup();
        verify(delegate).close();
    }
}
