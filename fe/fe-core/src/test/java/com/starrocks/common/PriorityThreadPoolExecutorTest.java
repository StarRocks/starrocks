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


package com.starrocks.common;

import com.starrocks.common.concurrent.PriorityFutureTask;
import com.starrocks.common.concurrent.PriorityRunnable;
import com.starrocks.common.concurrent.PriorityThreadPoolExecutor;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PriorityThreadPoolExecutorTest {

    @Test
    public void testDefault() throws InterruptedException, ExecutionException {
        PriorityBlockingQueue<Runnable> workQueue = new PriorityBlockingQueue<Runnable>(1000);
        PriorityThreadPoolExecutor pool = new PriorityThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, workQueue);

        Future[] futures = new Future[20];
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < futures.length; i++) {
            int index = i;
            futures[i] = pool.submit(new PriorityRunnable(0) {

                @Override
                public void run() {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    buffer.append(index + ", ");
                }
            });
        }
        for (int i = 0; i < futures.length; i++) {
            futures[i].get();
        }
        assertEquals("0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, ", buffer.toString());
    }

    @Test
    public void testSamePriority() throws InterruptedException, ExecutionException {
        PriorityBlockingQueue<Runnable> workQueue = new PriorityBlockingQueue<Runnable>(1000);
        PriorityThreadPoolExecutor pool = new PriorityThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, workQueue);
        sleepFlag.set(true);

        Future[] futures = new Future[10];
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < futures.length; i++) {
            futures[i] = pool.submit(new TenSecondTask(i, 1, buffer));
        }
        sleepFlag.set(false);
        for (int i = 0; i < futures.length; i++) {
            futures[i].get();
        }
        assertEquals("01@00, 01@01, 01@02, 01@03, 01@04, 01@05, 01@06, 01@07, 01@08, 01@09, ", buffer.toString());
    }

    @Test
    public void testDynamicPriority() throws InterruptedException, ExecutionException {
        PriorityBlockingQueue<Runnable> workQueue = new PriorityBlockingQueue<Runnable>(1000);
        PriorityThreadPoolExecutor pool = new PriorityThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, workQueue);
        sleepFlag.set(true);

        PriorityFutureTask<Void>[] futures = new PriorityFutureTask[20];
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < futures.length; i++) {
            futures[i] = pool.submit(new TenSecondTask(i, i, buffer));
        }
        for (int i = 10; i < futures.length; i++) {
            assertTrue(pool.updatePriority(futures[i], i - 10));
        }

        sleepFlag.set(false);

        for (int i = 0; i < futures.length; i++) {
            futures[i].get();
        }

        for (int i = 0; i < futures.length; i++) {
            assertFalse(pool.updatePriority(futures[i], i + 20));
        }

        assertEquals(
                "00@00, 09@09, 09@19, 08@08, 08@18, 07@07, 07@17, 06@06, 06@16, " +
                        "05@05, 05@15, 04@04, 04@14, 03@03, 03@13, 02@02, 02@12, 01@01, 01@11, 00@10, ",
                buffer.toString());
    }

    public static AtomicBoolean sleepFlag = new AtomicBoolean(true);

    public static class TenSecondTask extends PriorityRunnable {
        private StringBuffer buffer;
        int index;

        public TenSecondTask(int index, int priority, StringBuffer buffer) {
            super(priority);
            this.index = index;
            this.buffer = buffer;
        }

        @Override
        public void run() {
            try {
                while (sleepFlag.get()) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            buffer.append(String.format("%02d@%02d", getPriority(), index)).append(", ");
            //System.out.println(buffer);
        }
    }
}