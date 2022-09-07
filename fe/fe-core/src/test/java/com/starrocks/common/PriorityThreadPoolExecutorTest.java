// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

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

        Future[] futures = new Future[10];
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < futures.length; i++) {
            futures[i] = pool.submit(new TenSecondTask(i, 1, buffer));
        }
        for (int i = 0; i < futures.length; i++) {
            futures[i].get();
        }
        assertEquals("01@00, 01@01, 01@02, 01@03, 01@04, 01@05, 01@06, 01@07, 01@08, 01@09, ", buffer.toString());
    }

    @Test
    public void testRandomPriority() throws InterruptedException, ExecutionException {
        PriorityBlockingQueue<Runnable> workQueue = new PriorityBlockingQueue<Runnable>(1000);
        PriorityThreadPoolExecutor pool = new PriorityThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, workQueue);

        Future[] futures = new Future[20];
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < futures.length; i++) {
            int r = (int) (Math.random() * 100);
            futures[i] = pool.submit(new TenSecondTask(i, r, buffer));
        }
        for (int i = 0; i < futures.length; i++) {
            futures[i].get();
        }

        buffer.append("01@00");
        String[] split = buffer.toString().split(", ");
        for (int i = 2; i < split.length - 1; i++) {
            String s = split[i].split("@")[0];
            assertTrue(Integer.valueOf(s) >= Integer.valueOf(split[i + 1].split("@")[0]));
        }
    }

    @Test
    public void testDynamicPriority() throws InterruptedException, ExecutionException {
        PriorityBlockingQueue<Runnable> workQueue = new PriorityBlockingQueue<Runnable>(1000);
        PriorityThreadPoolExecutor pool = new PriorityThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, workQueue);

        PriorityFutureTask<Void>[] futures = new PriorityFutureTask[20];
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < futures.length; i++) {
            futures[i] = pool.submit(new TenSecondTask(i, i, buffer));
        }
        for (int i = 10; i < futures.length; i++) {
            assertTrue(pool.updatePriority(futures[i], i - 10));
        }

        flag = 1;

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

    public static int flag = 0;

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
                if (flag == 0) {
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