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

package com.starrocks.context;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChannelWaitRegistryTest {

    @Test
    public void testSignalWakesWaiter() throws Exception {
        ChannelWaitRegistry registry = new ChannelWaitRegistry();
        long observed = registry.currentGeneration("cb.channel");
        CountDownLatch ready = new CountDownLatch(1);
        AtomicBoolean changed = new AtomicBoolean(false);

        Thread waiter = new Thread(() -> {
            ready.countDown();
            try {
                changed.set(registry.awaitChange("cb.channel", observed, 1000L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        Assertions.assertTrue(ready.await(1, TimeUnit.SECONDS));

        registry.signal("cb.channel");
        waiter.join(1000L);
        Assertions.assertFalse(waiter.isAlive());
        Assertions.assertTrue(changed.get());
    }

    @Test
    public void testAwaitChangeTimesOutWithoutSignal() throws Exception {
        ChannelWaitRegistry registry = new ChannelWaitRegistry();
        long observed = registry.currentGeneration("cb.channel");
        long start = System.currentTimeMillis();
        boolean changed = registry.awaitChange("cb.channel", observed, 50L);
        long elapsed = System.currentTimeMillis() - start;

        Assertions.assertFalse(changed);
        Assertions.assertTrue(elapsed >= 40L);
    }
}
