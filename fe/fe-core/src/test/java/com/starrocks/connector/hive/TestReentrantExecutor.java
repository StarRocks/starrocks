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


package com.starrocks.connector.hive;

import com.google.common.util.concurrent.SettableFuture;
import com.starrocks.connector.ReentrantExecutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestReentrantExecutor {
    @Test
    public void testReentrantExecutor() throws ExecutionException, InterruptedException {
        AtomicInteger callCounter = new AtomicInteger();
        SettableFuture<Object> future = SettableFuture.create();
        ExecutorService executor = newCachedThreadPool();
        try {
            Executor reentrantExecutor = new ReentrantExecutor(executor, 1);
            reentrantExecutor.execute(() -> {
                callCounter.incrementAndGet();
                reentrantExecutor.execute(() -> {
                    callCounter.incrementAndGet();
                    future.set(null);
                });
                try {
                    future.get();
                } catch (Exception ignored) {
                }
            });
            future.get();

            SettableFuture<Object> secondFuture = SettableFuture.create();
            reentrantExecutor.execute(() -> secondFuture.set(null));
            secondFuture.get();

            Assert.assertEquals(2, callCounter.get());
        } finally {
            executor.shutdownNow();
        }
    }
}
