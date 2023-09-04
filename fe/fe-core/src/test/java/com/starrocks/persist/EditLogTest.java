// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.starrocks.common.io.Text;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalTask;
import com.starrocks.lake.ShardDeleter;
import com.starrocks.lake.ShardManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class EditLogTest {
    public static final Logger LOG = LogManager.getLogger(EditLogTest.class);

    @Before
    public void setUp() {
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
    }

    @Test
    public void testtNormal() throws Exception {
        BlockingQueue<JournalTask> logQueue = new ArrayBlockingQueue<>(100);
        short threadNum = 20;
        List<Thread> allThreads = new ArrayList<>();
        for (short i = 0; i != threadNum; i++) {
            final short n = i;
            allThreads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    EditLog editLog = new EditLog(logQueue);
                    editLog.logEdit(n, new Text("111"));
                }
            }));
        }

        Thread consumer = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i != threadNum; i++) {
                    try {
                        JournalTask task = logQueue.take();
                        task.markSucceed();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        consumer.start();
        for (Thread producer : allThreads) {
            producer.start();
        }

        for (Thread producer : allThreads) {
            producer.join();
        }
        consumer.join();
        Assert.assertEquals(0, logQueue.size());
    }

    @Test
    public void testInterrupt() throws Exception {
        // block if more than one task is put
        BlockingQueue<JournalTask> journalQueue = new ArrayBlockingQueue<>(1);
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                EditLog editLog = new EditLog(journalQueue);
                editLog.logEdit((short) 1, new Text("111"));
            }
        });

        t1.start();
        while (journalQueue.isEmpty()) {
            Thread.sleep(50);
        }
        // t1 is blocked in task.get() now
        Assert.assertEquals(1, journalQueue.size());

        // t2 will be blocked in queue.put() because queue is full
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                EditLog editLog = new EditLog(journalQueue);
                editLog.logEdit((short) 2, new Text("222"));
            }
        });
        t2.start();

        // t1 got interrupt exception while blocking in task.get()
        for (int i = 0; i != 3; i++) {
            t1.interrupt();
            Thread.sleep(100);
        }

        // t2 got interrupt exception while blocking in queue.put()
        for (int i = 0; i != 3; i++) {
            t2.interrupt();
            Thread.sleep(100);
        }

        Assert.assertEquals(1, journalQueue.size());
        JournalTask task = journalQueue.take();

        task.markSucceed();
        task = journalQueue.take();
        task.markSucceed();

        t1.join();
        t2.join();
    }

    private GlobalStateMgr mockGlobalStateMgr() throws Exception {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        NodeMgr nodeMgr = new NodeMgr(false, globalStateMgr);
        Field field1 = nodeMgr.getClass().getDeclaredField("frontends");
        field1.setAccessible(true);

        ConcurrentHashMap<String, Frontend> frontends = new ConcurrentHashMap<>();
        Frontend fe1 = new Frontend(FrontendNodeType.LEADER, "testName", "127.0.0.1", 1000);
        frontends.put("testName", fe1);
        field1.set(nodeMgr, frontends);

        Field field2 = globalStateMgr.getClass().getDeclaredField("nodeMgr");
        field2.setAccessible(true);
        field2.set(globalStateMgr, nodeMgr);

        ShardManager shardManager = new ShardManager();
        Field field3 = globalStateMgr.getClass().getDeclaredField("shardManager");
        field3.setAccessible(true);
        field3.set(globalStateMgr, shardManager);

        return globalStateMgr;
    }

    @Test
    public void testOpUpdateFrontend() throws Exception {
        GlobalStateMgr mgr = mockGlobalStateMgr();
        List<Frontend> frontends = mgr.getFrontends(null);
        Frontend fe = frontends.get(0);
        fe.updateHostAndEditLogPort("testHost", 1000);
        JournalEntity journal = new JournalEntity();
        journal.setData(fe);
        journal.setOpCode(OperationType.OP_UPDATE_FRONTEND);
        EditLog.loadJournal(mgr, journal);
        List<Frontend> updatedFrontends = mgr.getFrontends(null);
        Frontend updatedfFe = updatedFrontends.get(0);
        Assert.assertEquals("testHost", updatedfFe.getHost());
        Assert.assertTrue(updatedfFe.getEditLogPort() == 1000);
    }

    @Test
    public void testUpdateUnusedShardId() throws Exception {
        GlobalStateMgr mgr = mockGlobalStateMgr();
        JournalEntity journal = new JournalEntity();
        Set<Long> shardIds = new HashSet<>();
        shardIds.add(1L);
        shardIds.add(2L);
        ShardInfo info = new ShardInfo(shardIds);
        journal.setData(info);
        journal.setOpCode(OperationType.OP_ADD_UNUSED_SHARD);
        EditLog.loadJournal(mgr, journal);

        ShardDeleter shardDeleter = mgr.getShardManager().getShardDeleter();
        Assert.assertEquals(Deencapsulation.getField(shardDeleter, "shardIds"), shardIds);

        journal.setData(info);
        journal.setOpCode(OperationType.OP_DELETE_UNUSED_SHARD);
        EditLog.loadJournal(mgr, journal);
        Assert.assertEquals(Deencapsulation.getField(shardDeleter, "shardIds"), new HashSet<>());
    }

}
