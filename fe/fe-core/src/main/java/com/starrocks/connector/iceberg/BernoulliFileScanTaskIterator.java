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

import java.util.NoSuchElementException;
import java.util.Random;

/**
 * File-level Bernoulli sampling iterator used by background external-table statistics
 * collection (Iceberg ANALYZE SAMPLE). Each {@link FileScanTask} produced by the wrapped
 * iterator is independently kept with probability {@code ratio}; dropped files never reach
 * split/scan-range construction, so BE never opens them. Lazy evaluation, no materialization
 * of the full file list.
 *
 * <p>Must wrap the planned task iterator AFTER file splitting (after
 * {@code buildSplitFileScanTaskIterator}), so the keep/drop decision is made per split
 * (uniform ~targetSplitSize rows), achieving approximate row-level uniform Bernoulli
 * sampling independent of original file-size distribution.
 */
public class BernoulliFileScanTaskIterator implements CloseableIterator<FileScanTask> {

    private final CloseableIterator<FileScanTask> inner;
    private final Random random;
    private final double ratio;

    private long totalFileCount = 0;
    private long selectedFileCount = 0;

    private FileScanTask next;
    private boolean exhausted = false;

    public BernoulliFileScanTaskIterator(CloseableIterator<FileScanTask> inner, double ratio, long seed) {
        this.inner = inner;
        this.ratio = ratio;
        this.random = (seed != 0) ? new Random(seed) : new Random();
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        if (exhausted) {
            return false;
        }
        while (inner.hasNext()) {
            FileScanTask candidate = inner.next();
            totalFileCount++;
            if (random.nextDouble() < ratio) {
                selectedFileCount++;
                next = candidate;
                return true;
            }
        }
        exhausted = true;
        return false;
    }

    @Override
    public FileScanTask next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        FileScanTask result = next;
        next = null;
        return result;
    }

    @Override
    public void close() throws java.io.IOException {
        inner.close();
    }

    public long getTotalFileCount() {
        return totalFileCount;
    }

    public long getSelectedFileCount() {
        return selectedFileCount;
    }
}
