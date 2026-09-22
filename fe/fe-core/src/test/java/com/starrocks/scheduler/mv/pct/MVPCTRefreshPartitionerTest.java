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

package com.starrocks.scheduler.mv.pct;

import com.google.common.collect.Lists;
import com.starrocks.sql.ast.PartitionDesc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The batching and wait policy for MV partition creation lives in
 * {@link MVPCTRefreshPartitioner#addPartitionsInBatches}, shared by the range and list partitioners so
 * the two cannot drift. These tests pin the policy itself: batches are handed over whole and in order,
 * and the interval is spent BETWEEN batches only -- never after the last one.
 */
public class MVPCTRefreshPartitionerTest {
    private static final int BATCH_SIZE = 64;

    private static List<PartitionDesc> descs(int n) {
        List<PartitionDesc> descs = Lists.newArrayListWithCapacity(n);
        for (int i = 0; i < n; i++) {
            descs.add(Mockito.mock(PartitionDesc.class));
        }
        return descs;
    }

    /** Runs the shared helper and reports how it batched and how often it waited. */
    private static Result run(int partitionCount) {
        List<PartitionDesc> all = descs(partitionCount);
        List<List<PartitionDesc>> seen = Lists.newArrayList();
        AtomicInteger waits = new AtomicInteger();
        MVPCTRefreshPartitioner.addPartitionsInBatches(all, seen::add, waits::incrementAndGet);
        return new Result(all, seen, waits.get());
    }

    private record Result(List<PartitionDesc> all, List<List<PartitionDesc>> batches, int waits) {
    }

    @Test
    public void testSingleBatchDoesNotWait() {
        // one partition: the common case for a first refresh, and the one that used to pay a full
        // interval for nothing
        Result one = run(1);
        Assertions.assertEquals(1, one.batches().size());
        Assertions.assertEquals(0, one.waits(), "a single batch must not wait");

        // exactly one full batch: still nothing to space out
        Result full = run(BATCH_SIZE);
        Assertions.assertEquals(1, full.batches().size());
        Assertions.assertEquals(0, full.waits(), "a single full batch must not wait");
    }

    @Test
    public void testMultipleBatchesWaitBetweenThemOnly() {
        // one past a batch boundary, and several batches: waits are always batches - 1
        for (int count : new int[] {BATCH_SIZE + 1, BATCH_SIZE * 2, BATCH_SIZE * 3 + 7}) {
            Result r = run(count);
            int expectedBatches = (count + BATCH_SIZE - 1) / BATCH_SIZE;
            Assertions.assertEquals(expectedBatches, r.batches().size(),
                    "unexpected batch count for " + count + " partitions");
            Assertions.assertEquals(expectedBatches - 1, r.waits(),
                    "waits must be one fewer than batches for " + count + " partitions");
        }
    }

    @Test
    public void testEveryPartitionIsAddedOnceInOrder() {
        Result r = run(BATCH_SIZE * 2 + 5);
        List<PartitionDesc> flattened = Lists.newArrayList();
        r.batches().forEach(flattened::addAll);
        Assertions.assertEquals(r.all(), flattened, "batching must not drop, duplicate or reorder partitions");
        for (int i = 0; i < r.batches().size() - 1; i++) {
            Assertions.assertEquals(BATCH_SIZE, r.batches().get(i).size(), "non-final batches must be full");
        }
    }

    @Test
    public void testEmptyInputDoesNothing() {
        Result r = run(0);
        Assertions.assertTrue(r.batches().isEmpty());
        Assertions.assertEquals(0, r.waits());
    }
}
