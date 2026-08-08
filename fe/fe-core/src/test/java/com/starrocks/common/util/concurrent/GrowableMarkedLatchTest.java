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

package com.starrocks.common.util.concurrent;

import com.starrocks.common.Status;
import com.starrocks.thrift.TStatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class GrowableMarkedLatchTest {

    @Test
    public void testGrowWhileRunning() throws Exception {
        GrowableMarkedLatch<String, Long> latch = new GrowableMarkedLatch<>();
        Assertions.assertTrue(latch.addMark("a", 1L));
        Assertions.assertTrue(latch.addMark("b", 1L));
        Assertions.assertEquals(2, latch.getCount());

        Assertions.assertTrue(latch.markedCountDown("a", 1L));
        // still running: growth allowed
        Assertions.assertTrue(latch.addMark("c", 1L));
        Assertions.assertEquals(2, latch.getCount());

        Assertions.assertTrue(latch.markedCountDown("b", 1L));
        Assertions.assertTrue(latch.markedCountDown("c", 1L));
        Assertions.assertEquals(0, latch.getCount());
        Assertions.assertTrue(latch.await(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSealedAfterCompletion() {
        GrowableMarkedLatch<String, Long> latch = new GrowableMarkedLatch<>();
        Assertions.assertTrue(latch.addMark("a", 1L));
        Assertions.assertTrue(latch.markedCountDown("a", 1L));
        // completed: growth rejected, completion irrevocable
        Assertions.assertFalse(latch.addMark("b", 1L));
        Assertions.assertEquals(0, latch.getCount());
    }

    @Test
    public void testCountDownToZeroSealsAndSetsStatus() {
        GrowableMarkedLatch<String, Long> latch = new GrowableMarkedLatch<>();
        Assertions.assertTrue(latch.addMark("a", 1L));
        Assertions.assertTrue(latch.addMark("b", 1L));
        latch.countDownToZero(new Status(TStatusCode.CANCELLED, "cancelled"));
        Assertions.assertEquals(0, latch.getCount());
        Assertions.assertFalse(latch.addMark("c", 1L));
        Assertions.assertEquals(TStatusCode.CANCELLED, latch.getStatus().getErrorCode());
    }

    @Test
    public void testUnknownMarkCountDownReturnsFalse() {
        GrowableMarkedLatch<String, Long> latch = new GrowableMarkedLatch<>();
        Assertions.assertTrue(latch.addMark("a", 1L));
        Assertions.assertFalse(latch.markedCountDown("unknown", 1L));
        Assertions.assertEquals(1, latch.getCount());
    }

    @Test
    public void testAwaitBlocksUntilLastMark() throws Exception {
        GrowableMarkedLatch<String, Long> latch = new GrowableMarkedLatch<>();
        Assertions.assertTrue(latch.addMark("a", 1L));
        Assertions.assertFalse(latch.await(10, TimeUnit.MILLISECONDS));

        Thread t = new Thread(() -> latch.markedCountDown("a", 1L));
        t.start();
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        t.join();
    }

    @Test
    public void testListenersFireOnceOnCompletion() {
        GrowableMarkedLatch<String, Long> latch = new GrowableMarkedLatch<>();
        AtomicInteger fired = new AtomicInteger();
        Assertions.assertTrue(latch.addMark("a", 1L));
        latch.addListener(fired::incrementAndGet);
        Assertions.assertEquals(0, fired.get());

        Assertions.assertTrue(latch.markedCountDown("a", 1L));
        Assertions.assertEquals(1, fired.get());
        // registering after completion fires immediately, and completion listeners are one-shot
        latch.addListener(fired::incrementAndGet);
        Assertions.assertEquals(2, fired.get());
        latch.countDownToZero(Status.OK);
        Assertions.assertEquals(2, fired.get());
    }
}
