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

package org.apache.iceberg.util;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ParallelIterableTest {

    @Test
    public void testShadowsIcebergCore() {
        // the scan planning fix lives in this copy; it must win over the class in iceberg-core
        String location = ParallelIterable.class.getProtectionDomain().getCodeSource().getLocation().toString();
        Assertions.assertFalse(location.contains("iceberg-core"), location);
    }

    @Test
    public void testQueueSizeIsTrackedAndLimited() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            List<Iterable<Integer>> iterables = ImmutableList.of(
                    () -> IntStream.range(0, 100).iterator(),
                    () -> IntStream.range(0, 100).iterator(),
                    () -> IntStream.range(0, 100).iterator());
            Multiset<Integer> expected = IntStream.range(0, 100).boxed()
                    .flatMap(i -> Stream.of(i, i, i))
                    .collect(ImmutableMultiset.toImmutableMultiset());

            int maxQueueSize = 20;
            ParallelIterable.ParallelIterator<Integer> iterator = (ParallelIterable.ParallelIterator<Integer>)
                    new ParallelIterable<>(iterables, executor, maxQueueSize).iterator();
            // A task checks the limit only when it starts and then adds its whole input (100 items). With one worker
            // thread one task adds at a time, and it starts only below the limit, so whatever the interleaving the
            // count stays below maxQueueSize + 100 (the extra 1 covers an item added but not yet counted).
            int bound = maxQueueSize + 100;
            Multiset<Integer> actual = HashMultiset.create();
            while (iterator.hasNext()) {
                int size = iterator.queueSize();
                Assertions.assertTrue(size <= bound, "queue size " + size);
                actual.add(iterator.next());
            }
            Assertions.assertEquals(expected, actual);
            Assertions.assertEquals(0, iterator.queueSize());
            iterator.close();
        } finally {
            executor.shutdown();
        }
    }
}
