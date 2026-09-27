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

package com.starrocks.connector.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// Unit tests for the bounded-cost statistics-scan budget helpers (design 2.2/2.4).
public class IcebergScanBudgetTest {

    // A CloseableIterator over an in-memory list that records whether close() was called, so we can assert
    // the budget stops the underlying planner early instead of draining it.
    private static class RecordingIterator implements CloseableIterator<FileScanTask> {
        private final Iterator<FileScanTask> delegate;
        boolean closed = false;
        int consumed = 0;

        RecordingIterator(List<FileScanTask> tasks) {
            this.delegate = tasks.iterator();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public FileScanTask next() {
            consumed++;
            return delegate.next();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    // task.length() controls the byte budget; file().recordCount()/fileSizeInBytes() feed the row estimate.
    private static FileScanTask task(long length, long recordCount, long fileSizeInBytes) {
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.length()).thenReturn(length);
        DataFile file = Mockito.mock(DataFile.class);
        Mockito.when(file.recordCount()).thenReturn(recordCount);
        Mockito.when(file.fileSizeInBytes()).thenReturn(fileSizeInBytes);
        Mockito.when(task.file()).thenReturn(file);
        return task;
    }

    private static List<FileScanTask> tasks(int n, long length) {
        List<FileScanTask> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(task(length, length, length));
        }
        return list;
    }

    private static List<FileScanTask> drain(CloseableIterator<FileScanTask> it) throws Exception {
        List<FileScanTask> out = new ArrayList<>();
        while (it.hasNext()) {
            out.add(it.next());
        }
        it.close();
        return out;
    }

    @Test
    public void testBytesCapSoftStopWithOvershoot() throws Exception {
        // 10 tasks x 100 bytes. bytesCap = 250 -> after task #3 accumulated bytes (300) >= 250, so it stops
        // *after* returning the task that tripped the cap (soft cap, one-task overshoot).
        RecordingIterator delegate = new RecordingIterator(tasks(10, 100));
        List<FileScanTask> out = drain(IcebergMetadata.boundedFileScanTaskIterator(delegate, 250, -1, -1));
        Assertions.assertEquals(3, out.size());
        // The delegate must be closed early (not fully drained).
        Assertions.assertTrue(delegate.closed);
        Assertions.assertEquals(3, delegate.consumed);
    }

    @Test
    public void testFilesCap() throws Exception {
        RecordingIterator delegate = new RecordingIterator(tasks(10, 100));
        List<FileScanTask> out = drain(IcebergMetadata.boundedFileScanTaskIterator(delegate, -1, 4, -1));
        Assertions.assertEquals(4, out.size());
        Assertions.assertTrue(delegate.closed);
    }

    @Test
    public void testRowsCap() throws Exception {
        // Each whole-file task estimates recordCount rows = 100. rowsCap = 350 -> stops after 4 tasks (400 >= 350).
        RecordingIterator delegate = new RecordingIterator(tasks(10, 100));
        List<FileScanTask> out = drain(IcebergMetadata.boundedFileScanTaskIterator(delegate, -1, -1, 350));
        Assertions.assertEquals(4, out.size());
    }

    @Test
    public void testFirstCapWins() throws Exception {
        // bytesCap trips at 3 tasks (300 >= 250), filesCap would trip at 5. The tighter budget stops first.
        RecordingIterator delegate = new RecordingIterator(tasks(10, 100));
        List<FileScanTask> out = drain(IcebergMetadata.boundedFileScanTaskIterator(delegate, 250, 5, -1));
        Assertions.assertEquals(3, out.size());
    }

    @Test
    public void testUnderBudgetDrainsFully() throws Exception {
        // No cap is reached -> every task is returned and the delegate is exhausted naturally.
        RecordingIterator delegate = new RecordingIterator(tasks(5, 100));
        List<FileScanTask> out = drain(IcebergMetadata.boundedFileScanTaskIterator(delegate, 10000, 100, 100000));
        Assertions.assertEquals(5, out.size());
    }

    @Test
    public void testEstimateTaskRowsWholeFile() {
        // task.length() >= fileSizeInBytes -> exact record count.
        Assertions.assertEquals(1000, IcebergMetadata.estimateTaskRows(task(500, 1000, 500)));
    }

    @Test
    public void testEstimateTaskRowsProrated() {
        // Split covers half the file bytes -> ~half the rows.
        Assertions.assertEquals(500, IcebergMetadata.estimateTaskRows(task(500, 1000, 1000)));
    }

    @Test
    public void testEstimateTaskRowsDegenerate() {
        // Zero file size cannot be pro-rated; fall back to the record count (or 0 when unknown).
        Assertions.assertEquals(1000, IcebergMetadata.estimateTaskRows(task(500, 1000, 0)));
        Assertions.assertEquals(0, IcebergMetadata.estimateTaskRows(task(500, 0, 0)));
    }
}
