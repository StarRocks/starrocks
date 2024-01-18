// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util.concurrent;

import com.starrocks.common.concurrent.locks.CountingLatch;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class CountingLatchTest {
    @Test
    public void testCountUpAndDown() throws InterruptedException {
        CountingLatch customCounter = new CountingLatch(0);

        // Create a CountDownLatch to synchronize the start of all threads
        CountDownLatch startLatch = new CountDownLatch(1);

        // Create and start multiple threads
        int numThreads = 5;
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startLatch.await(); // Wait until all threads are ready to start
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                Random random = new Random();
                // Simulate some work (e.g., incrementing and decrementing)
                for (int idx = 0; idx < 100; idx++) {
                    customCounter.increment();
                    try {
                        int time = random.nextInt(100) + 1;
                        Thread.sleep(time);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    customCounter.decrement();
                }
            });
            threads[i].start();
        }

        // Start all threads concurrently
        startLatch.countDown();

        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }

        // Ensure that the count reaches zero
        customCounter.awaitZero();
        Assert.assertEquals(0, customCounter.getCount());
        System.out.println("Count reached zero.");
    }

}
