// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.starrocks.common.io.Text;
import com.starrocks.journal.JournalQueueEntity;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class EditLogTest {
    public static final Logger LOG = LogManager.getLogger(EditLogTest.class);
    @Test
    public void testtNormal() throws Exception {
        BlockingQueue<JournalQueueEntity> logQueue = new ArrayBlockingQueue<>(100);
        short THREAD_NUM = 20;
        List<Thread> allThreads = new ArrayList<>();
        for (short i = 0; i != THREAD_NUM; i++) {
            final short n = i;
            allThreads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    EditLog editLog = new EditLog(null, logQueue);
                    editLog.logEdit(n, new Text("111"), -1);

                }
            }));
        }

        Thread consumer = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i != THREAD_NUM; i++) {
                    try {
                        JournalQueueEntity entity = logQueue.take();
                        LOG.info("got {} entity op {}", i, entity.getOp());
                        entity.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        consumer.start();
        for(Thread producer: allThreads) {
            producer.start();
        }

        for(Thread producer: allThreads) {
            producer.join();
        }
        consumer.join();
        Assert.assertEquals(0, logQueue.size());
    }


    @Test
    public void testNoWaitInterrupt() throws Exception {
        BlockingQueue<JournalQueueEntity> logQueue = new ArrayBlockingQueue<>(1);
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                EditLog editLog = new EditLog(null, logQueue);
                editLog.logEdit((short) 1, new Text("111"), -1);
            }
        });
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                EditLog editLog = new EditLog(null, logQueue);
                editLog.logEdit((short) 2, new Text("111"), -1);
            }
        });
        t1.start();
        while (logQueue.isEmpty()) {
            Thread.sleep(50);
        }
        Assert.assertEquals(1, logQueue.size());

        // t2 is waiting
        t2.start();

        // t2 got interrupt exception
        for (int i = 0; i != 3; i ++) {
            t2.interrupt();
            Thread.sleep(100);
        }

        Assert.assertEquals(1, logQueue.size());
        JournalQueueEntity entity = logQueue.take();
        Assert.assertEquals((short)1, entity.getOp());
        entity.countDown();
        entity = logQueue.take();
        Assert.assertEquals((short)2, entity.getOp());
        entity.countDown();

        t1.join();
        t2.join();
    }

    @Test
    public void testWaitInterrupt(@Mocked JournalQueueEntity entity) throws Exception {
        new Expectations(entity) {
            {
                entity.waitLatch();
                times = 2;
                result = new InterruptedException("mock mock");
                result = null;
            }
        };
        EditLog editLog = new EditLog(null, null);
        editLog.waitLogConsumed(entity);
    }
}
