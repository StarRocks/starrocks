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

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BernoulliFileScanTaskIteratorTest {

    private static CloseableIterator<FileScanTask> tasks(int n) {
        List<FileScanTask> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(Mockito.mock(FileScanTask.class));
        }
        Iterator<FileScanTask> it = list.iterator();
        return new CloseableIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public FileScanTask next() {
                return it.next();
            }
        };
    }

    @Test
    public void testRatioOneKeepsAll() throws Exception {
        try (BernoulliFileScanTaskIterator sampled = new BernoulliFileScanTaskIterator(tasks(50), 1.0, 42)) {
            int count = 0;
            while (sampled.hasNext()) {
                sampled.next();
                count++;
            }
            assertEquals(50, count);
            assertEquals(50, sampled.getTotalFileCount());
            assertEquals(50, sampled.getSelectedFileCount());
        }
    }

    @Test
    public void testRatioZeroDropsAll() throws Exception {
        try (BernoulliFileScanTaskIterator sampled = new BernoulliFileScanTaskIterator(tasks(50), 0.0, 42)) {
            assertFalse(sampled.hasNext());
            assertEquals(50, sampled.getTotalFileCount());
            assertEquals(0, sampled.getSelectedFileCount());
        }
    }

    @Test
    public void testFixedSeedIsDeterministic() throws Exception {
        long selectedA;
        long selectedB;
        try (BernoulliFileScanTaskIterator a = new BernoulliFileScanTaskIterator(tasks(2000), 0.1, 7)) {
            while (a.hasNext()) {
                a.next();
            }
            selectedA = a.getSelectedFileCount();
        }
        try (BernoulliFileScanTaskIterator b = new BernoulliFileScanTaskIterator(tasks(2000), 0.1, 7)) {
            while (b.hasNext()) {
                b.next();
            }
            selectedB = b.getSelectedFileCount();
        }
        assertEquals(selectedA, selectedB);
    }

    @Test
    public void testRatioApproximatelyMatchesSelectionFraction() throws Exception {
        int total = 20000;
        double ratio = 0.03;
        try (BernoulliFileScanTaskIterator sampled = new BernoulliFileScanTaskIterator(tasks(total), ratio, 123)) {
            int count = 0;
            while (sampled.hasNext()) {
                sampled.next();
                count++;
            }
            assertEquals(total, sampled.getTotalFileCount());
            assertEquals(count, sampled.getSelectedFileCount());
            double observedRatio = (double) count / total;
            // Bernoulli(p=0.03) over 20k trials: std dev ~= sqrt(20000*0.03*0.97) ~= 24 files ~= 0.0012 in ratio.
            // Allow a generous 10x margin to keep this non-flaky.
            assertTrue(Math.abs(observedRatio - ratio) < 0.012,
                    "observed ratio " + observedRatio + " too far from target " + ratio);
        }
    }

    @Test
    public void testNextThrowsAfterExhausted() throws Exception {
        try (BernoulliFileScanTaskIterator sampled = new BernoulliFileScanTaskIterator(tasks(0), 1.0, 1)) {
            assertFalse(sampled.hasNext());
            assertThrows(NoSuchElementException.class, sampled::next);
        }
    }
}
