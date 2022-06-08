package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.journal.bdbje.BDBJEJournal;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class JournalWriterTest {
    @Mocked
    private Journal journal = new BDBJEJournal(null, null, null);
    private BlockingQueue<JournalQueueEntity> logQueue = new ArrayBlockingQueue<>(100);
    private JournalWriter writer = new JournalWriter(journal, logQueue);

    private DataOutputBuffer makeBuffer(int size) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, StringUtils.repeat("x", size - 4));
        Assert.assertEquals(size, buffer.getLength());
        return buffer;
    }

    @After
    public void setup() throws Exception {
        // reset all configs
        Config.edit_log_roll_num = 50000;
        Config.batch_journal_size_mb = 100;
        Config.batch_journal_cnt = 100;
    }

    @Test
    public void testWriteOneLog() throws Exception {
        new Expectations(journal) {
            {
                journal.writeWithinTxn(anyShort, (DataOutputBuffer) any);
                times = 1;

                journal.commitTxn();
                times = 1;
            }
        };
        JournalQueueEntity entity = new JournalQueueEntity((short) -1, makeBuffer(10), -1);
        logQueue.add(entity);
        writer.runOneCycle();
        Assert.assertEquals(1, writer.rollEditCounter);
        Assert.assertEquals(1, writer.staggingLogs.size());
        Assert.assertEquals(0, entity.latch.getCount());
    }

    @Test
    public void testMustCommitBefore() throws Exception {
        new Expectations(journal) {
            {
                journal.writeWithinTxn(anyShort, (DataOutputBuffer) any);
                times = 2;

                journal.commitTxn();
                times = 1;
            }
        };
        // 3 logs, the second one is emergency and must commit
        JournalQueueEntity expectConsumedEntity = new JournalQueueEntity((short) -1, makeBuffer(10), -1);
        logQueue.add(expectConsumedEntity);
        JournalQueueEntity emergency = new JournalQueueEntity((short) -1, makeBuffer(10), -1);
        emergency.betterCommitBeforeTime = System.currentTimeMillis() - 10;
        logQueue.add(emergency);
        JournalQueueEntity expectNotConsumedEntity = new JournalQueueEntity((short) -1, makeBuffer(10), -1);
        logQueue.add(expectNotConsumedEntity);

        writer.runOneCycle();
        Assert.assertEquals(2, writer.rollEditCounter);
        Assert.assertEquals(2, writer.staggingLogs.size());
        Assert.assertEquals(0, expectConsumedEntity.latch.getCount());
        Assert.assertEquals(0, emergency.latch.getCount());
        // one log left
        Assert.assertEquals(1, expectNotConsumedEntity.latch.getCount());
        Assert.assertEquals(1, logQueue.size());
    }

    @Test
    public void testTooManyLogs() throws Exception {
        Config.batch_journal_cnt = 2;
        Config.batch_journal_size_mb = 2;
        new Expectations(journal) {
            {
                journal.writeWithinTxn(anyShort, (DataOutputBuffer) any);
                times = 3;

                journal.commitTxn();
                times = 2;
            }
        };
        // 4 logs
        // round 1: 2 logs
        JournalQueueEntity expectConsumedEntity = new JournalQueueEntity((short) -1, makeBuffer(10), -1);
        logQueue.add(expectConsumedEntity);
        JournalQueueEntity expectConsumedEntity2 = new JournalQueueEntity((short) -1, makeBuffer(10), -1);
        logQueue.add(expectConsumedEntity2);
        // round 2: 1 big log
        JournalQueueEntity bigLog = new JournalQueueEntity((short) -1, makeBuffer(2 * 1024 * 1024 - 8), -1);
        Assert.assertEquals(2 * 1024 * 1024, bigLog.estimatedSizeByte());
        logQueue.add(bigLog);
        // this one should be left in queue
        JournalQueueEntity expectNotConsumedEntity = new JournalQueueEntity((short) -1, makeBuffer(10), -1);
        logQueue.add(expectNotConsumedEntity);

        // round 1
        writer.runOneCycle();
        Assert.assertEquals(2, writer.rollEditCounter);
        Assert.assertEquals(2, writer.staggingLogs.size());
        Assert.assertEquals(0, expectConsumedEntity.latch.getCount());
        Assert.assertEquals(0, expectConsumedEntity2.latch.getCount());
        // two log left
        Assert.assertEquals(1, bigLog.latch.getCount());
        Assert.assertEquals(1, expectNotConsumedEntity.latch.getCount());
        Assert.assertEquals(2, logQueue.size());

        // rount 2
        writer.runOneCycle();
        Assert.assertEquals(3, writer.rollEditCounter);
        Assert.assertEquals(1, writer.staggingLogs.size());
        Assert.assertEquals(0, bigLog.latch.getCount());
        // 1 log left
        Assert.assertEquals(1, expectNotConsumedEntity.latch.getCount());
        Assert.assertEquals(1, logQueue.size());
    }

    @Test
    public void testRollLog() throws Exception {
        Config.edit_log_roll_num = 4;

        // write 3 log * 4 times
        // expect roll 2 times, commit 4 times, put 12 times
        new Expectations(journal) {
            {
                journal.writeWithinTxn(anyShort, (DataOutputBuffer) any);
                times = 12;

                journal.commitTxn();
                times = 4;

                journal.rollJournal();
                times = 2;
            }
        };

        for (int i = 0; i != 4; i ++) {
            for (int j = 0; j != 3; j ++) {
                logQueue.add(new JournalQueueEntity((short) -1, makeBuffer(10), -1));
            }
            writer.runOneCycle();
            Assert.assertEquals(0, logQueue.size());
        }
    }
}
