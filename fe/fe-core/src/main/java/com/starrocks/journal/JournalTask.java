// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.journal;

import com.starrocks.common.io.DataOutputBuffer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JournalTask implements Future<Boolean> {
    // serialized JournalEntity
    private DataOutputBuffer buffer;
    // write result
    private Boolean isSucceed = null;
    // count down latch, the producer which called logEdit() will wait on it.
    // JournalWriter will call notify() after log is committed.
    protected CountDownLatch latch;
    // JournalWrite will commit immediately if received a log with betterCommitBeforeTime > now
    protected long betterCommitBeforeTimeInNano;

    public JournalTask(DataOutputBuffer buffer, long maxWaitIntervalMs) {
        this.buffer = buffer;
        this.latch = new CountDownLatch(1);
        if (maxWaitIntervalMs > 0) {
            this.betterCommitBeforeTimeInNano = System.nanoTime() + maxWaitIntervalMs * 1000000;
        } else {
            this.betterCommitBeforeTimeInNano = -1;
        }
    }

    public void markSucceed() {
        isSucceed = true;
        latch.countDown();
    }

    public void markAbort() {
        isSucceed = false;
        latch.countDown();
    }

    public long getBetterCommitBeforeTimeInNano() {
        return betterCommitBeforeTimeInNano;
    }

    public long estimatedSizeByte() {
        // journal id + buffer
        return Long.SIZE / 8 + (long) buffer.getLength();
    }

    public DataOutputBuffer getBuffer() {
        return buffer;
    }

    @Override
    public boolean isDone() {
        return latch.getCount() == 0;
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        latch.await();
        return isSucceed;
    }

    @Override
    public Boolean get(long timeout, @NotNull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        latch.await(timeout, unit);
        return isSucceed;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // cannot be canceled for now
        return false;
    }

    @Override
    public boolean isCancelled() {
        // cannot be canceled for now
        return false;
    }
}
