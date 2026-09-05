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

package com.starrocks.connector;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorTableIdTest {

    private static final long OFFSET = 100_000_000L;

    @Test
    public void testIdsAreOffsetAndMonotonic() {
        ConnectorTableId.Generator gen = new ConnectorTableId.Generator();
        long first = gen.getNextId().asLong();
        long second = gen.getNextId().asLong();
        long third = gen.getNextId().asLong();

        assertEquals(OFFSET, first);
        assertEquals(OFFSET + 1, second);
        assertEquals(OFFSET + 2, third);
    }

    @Test
    public void testGetMaxIdReturnsLastAllocated() {
        ConnectorTableId.Generator gen = new ConnectorTableId.Generator();
        gen.getNextId();
        gen.getNextId();
        long last = gen.getNextId().asLong();

        assertEquals(last, gen.getMaxId().asLong());
    }

    /**
     * Regression: before the long fix, the int counter would overflow past
     * Integer.MAX_VALUE - OFFSET allocations and produce negative IDs, which BE rejects
     * as "Invalid table type". The ID must stay positive and monotonic well past that point.
     */
    @Test
    public void testIdDoesNotOverflowPastIntegerMaxValue() {
        long seed = (long) Integer.MAX_VALUE + 1;
        ConnectorTableId id = new ConnectorTableId(seed);

        assertTrue(id.asLong() > 0, "ID must remain positive past int range");
        assertEquals(seed + OFFSET, id.asLong());
    }

    @Test
    public void testConcurrentAllocationsAreUnique() throws InterruptedException {
        ConnectorTableId.Generator gen = new ConnectorTableId.Generator();
        int threads = 8;
        int perThread = 1000;
        Set<Long> ids = java.util.Collections.synchronizedSet(new HashSet<>());
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        ids.add(gen.getNextId().asLong());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdownNow();

        assertEquals(threads * perThread, ids.size(), "all concurrently-allocated IDs must be unique");
    }
}
