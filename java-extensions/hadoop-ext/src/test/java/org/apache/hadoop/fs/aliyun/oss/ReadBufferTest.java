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

package org.apache.hadoop.fs.aliyun.oss;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class ReadBufferTest {
    private long previousTimeoutNanos;

    @BeforeEach
    public void setUp() {
        previousTimeoutNanos = ReadBuffer.awaitTimeoutNanos;
    }

    @AfterEach
    public void tearDown() {
        ReadBuffer.awaitTimeoutNanos = previousTimeoutNanos;
    }

    @Test
    public void testLostReadAheadTaskFailsTheWaitInsteadOfHanging() throws Exception {
        ReadBuffer.awaitTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(100);
        // No task ever fills or signals this buffer, as when the executor dropped it.
        ReadBuffer readBuffer = new ReadBuffer(0, 9);
        readBuffer.lock();
        try {
            readBuffer.await(ReadBuffer.STATUS.INIT);
        } finally {
            readBuffer.unlock();
        }
        Assertions.assertEquals(ReadBuffer.STATUS.ERROR, readBuffer.getStatus());
    }

    @Test
    public void testSignalledReadAheadTaskEndsTheWait() throws Exception {
        ReadBuffer.awaitTimeoutNanos = TimeUnit.MINUTES.toNanos(1);
        ReadBuffer readBuffer = new ReadBuffer(0, 9);
        Thread task = new Thread(() -> {
            readBuffer.lock();
            try {
                readBuffer.setStatus(ReadBuffer.STATUS.SUCCESS);
                readBuffer.signalAll();
            } finally {
                readBuffer.unlock();
            }
        });
        long start = System.nanoTime();
        readBuffer.lock();
        try {
            task.start();
            readBuffer.await(ReadBuffer.STATUS.INIT);
        } finally {
            readBuffer.unlock();
        }
        task.join();
        Assertions.assertEquals(ReadBuffer.STATUS.SUCCESS, readBuffer.getStatus());
        Assertions.assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(30));
    }
}
