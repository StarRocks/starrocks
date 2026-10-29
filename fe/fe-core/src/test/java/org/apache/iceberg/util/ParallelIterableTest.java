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
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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

    @Test
    public void testManyTinyIterablesDoNotWaitOnFixedSleeps() {
        // Like planning a table with thousands of small, cached manifests: every task finishes at once, so a pass of
        // 2 x WORKER_THREAD_POOL_SIZE tasks used to end in a fixed 10ms sleep, 400 passes took seconds.
        int count = 400 * 2 * ThreadPools.WORKER_THREAD_POOL_SIZE;
        List<Iterable<Integer>> iterables = IntStream.range(0, count)
                .mapToObj(i -> (Iterable<Integer>) ImmutableList.of(i))
                .collect(Collectors.toList());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            long start = System.nanoTime();
            long sum = 0;
            try (CloseableIterator<Integer> iterator = new ParallelIterable<>(iterables, executor).iterator()) {
                while (iterator.hasNext()) {
                    sum += iterator.next();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assertions.assertEquals((long) count * (count - 1) / 2, sum);
            Assertions.assertTrue(elapsedMs < 1500, "took " + elapsedMs + "ms");
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testWaitingOnASlowTaskDoesNotPileUpCallbacks() throws Exception {
        // a manifest read that produces nothing for a while: the consumer times out every 10ms and must keep waiting
        // on the same aggregate future instead of hanging one more callback on the task each time
        CountDownLatch release = new CountDownLatch(1);
        Iterable<Integer> slow = () -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ImmutableList.of(7).iterator();
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        try {
            ParallelIterable.ParallelIterator<Integer> iterator = (ParallelIterable.ParallelIterator<Integer>)
                    new ParallelIterable<>(ImmutableList.of(slow), executor).iterator();
            Future<Integer> first = consumer.submit(() -> iterator.hasNext() ? iterator.next() : null);
            for (int i = 0; i < 100 && iterator.maxRunningTaskDependents() == 0; i++) {
                Thread.sleep(10);
            }
            // about 20 timed out waits
            Thread.sleep(200);
            Assertions.assertEquals(1, iterator.maxRunningTaskDependents());
            release.countDown();
            Assertions.assertEquals(7, first.get(10, TimeUnit.SECONDS));
            iterator.close();
        } finally {
            release.countDown();
            consumer.shutdown();
            executor.shutdown();
        }
    }
}
