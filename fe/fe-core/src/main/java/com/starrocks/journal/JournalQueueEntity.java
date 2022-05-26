// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal;

import com.starrocks.common.io.DataOutputBuffer;

import java.util.concurrent.CountDownLatch;

public class JournalQueueEntity {
    // op
    private short op;
    // serialized JournalEntity
    private DataOutputBuffer buffer;
    // count down latch, the producer which called logXX() will wait on it.
    // JournalWriter will call notify() after log is committed.
    protected CountDownLatch latch;
    // JournalWrite will commit immediately if recieved a log with betterCommitBeforeTime > now
    protected long betterCommitBeforeTime;

    public JournalQueueEntity(short op, DataOutputBuffer buffer, long maxWaitIntervalMs) {
        this.op = op;
        this.buffer = buffer;
        this.latch = new CountDownLatch(1);
        if (maxWaitIntervalMs > 0) {
            this.betterCommitBeforeTime = System.currentTimeMillis() + maxWaitIntervalMs;
        } else {
            this.betterCommitBeforeTime = -1;
        }
    }

    public void waitLatch() throws InterruptedException {
        latch.await();
    }

    public void countDown() {
        latch.countDown();
    }

    public long getBetterCommitBeforeTime() {
        return betterCommitBeforeTime;
    }

    public long estimatedSizeByte() {
        // journal id + buffer
        return Long.SIZE / 8 + buffer.getLength();
    }

    public DataOutputBuffer getBuffer() {
        return buffer;
    }

    public short getOp() {
        return op;
    }
}
