// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.journal;

import com.starrocks.common.io.DataOutputBuffer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JournalTask implements Future<Void> {
    // op
    private short op;
    // serialized JournalEntity
    private DataOutputBuffer buffer;
    // count down latch, the producer which called logXX() will wait on it.
    // JournalWriter will call notify() after log is committed.
    protected CountDownLatch latch;
    // JournalWrite will commit immediately if recieved a log with betterCommitBeforeTime > now
    protected long betterCommitBeforeTime;

    public JournalTask(short op, DataOutputBuffer buffer, long maxWaitIntervalMs) {
        this.op = op;
        this.buffer = buffer;
        this.latch = new CountDownLatch(1);
        if (maxWaitIntervalMs > 0) {
            this.betterCommitBeforeTime = System.currentTimeMillis() + maxWaitIntervalMs;
        } else {
            this.betterCommitBeforeTime = -1;
        }
    }

    public void markDone() {
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

    @Override
    public boolean isDone() {
        return latch.getCount() == 0;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        latch.await();
        return null;
    }

    @Override
    public Void get(long timeout, @NotNull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        latch.await(timeout, unit);
        return null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // cannot canceled for now
        return false;
    }

    @Override
    public boolean isCancelled() {
        // cannot canceled for now
        return false;
    }
}
