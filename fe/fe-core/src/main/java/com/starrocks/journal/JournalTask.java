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


package com.starrocks.journal;

import com.starrocks.common.io.DataOutputBuffer;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JournalTask implements Future<Boolean> {
    public enum TaskType {
        // A normal edit log append that consumes a real journal id and writes data to the journal.
        APPEND,
        // An internal in-memory barrier used to seal the writer and observe the last committed
        // journal watermark. It does not write a journal entry or consume a journal id.
        BARRIER
    }

    // serialized JournalEntity
    private final DataOutputBuffer buffer;
    private final TaskType taskType;
    // write result
    private Boolean isSucceed = null;
    // count down latch, the producer which called logEdit() will wait on it.
    // JournalWriter will call notify() after log is committed.
    protected CountDownLatch latch;
    private Exception executeException;
    // JournalWrite will commit immediately if received a log with betterCommitBeforeTime > now
    protected long betterCommitBeforeTimeInNano;
    private final long startTimeNano;
    private volatile JournalWriter.DrainResult.Status drainStatus;
    private volatile long committedJournalId = -1L;

    public JournalTask(long startTimeNano, DataOutputBuffer buffer, long maxWaitIntervalMs) {
        this(startTimeNano, buffer, maxWaitIntervalMs, TaskType.APPEND);
    }

    private JournalTask(long startTimeNano, DataOutputBuffer buffer, long maxWaitIntervalMs, TaskType taskType) {
        this.startTimeNano = startTimeNano;
        this.buffer = buffer;
        this.taskType = taskType;
        this.latch = new CountDownLatch(1);
        if (maxWaitIntervalMs > 0) {
            this.betterCommitBeforeTimeInNano = System.nanoTime() + maxWaitIntervalMs * 1000000;
        } else {
            this.betterCommitBeforeTimeInNano = -1;
        }
    }

    public static JournalTask createBarrierTask() {
        return new JournalTask(System.nanoTime(), null, -1, TaskType.BARRIER);
    }

    public long getStartTimeNano() {
        return startTimeNano;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public boolean isBarrierTask() {
        return taskType == TaskType.BARRIER;
    }

    public void markSucceed() {
        isSucceed = true;
        latch.countDown();
    }

    public void markAbort() {
        markAbort(null);
    }

    public void markAbort(Exception e) {
        executeException = e;
        isSucceed = false;
        latch.countDown();
    }

    public void markBarrierReached(long committedJournalId) {
        this.committedJournalId = committedJournalId;
        this.drainStatus = JournalWriter.DrainResult.Status.BARRIER_REACHED;
        markSucceed();
    }

    public void markBarrierFailed(JournalWriter.DrainResult.Status drainStatus, long committedJournalId, Exception e) {
        this.committedJournalId = committedJournalId;
        this.drainStatus = drainStatus;
        markAbort(e);
    }

    public long getBetterCommitBeforeTimeInNano() {
        return betterCommitBeforeTimeInNano;
    }

    public long estimatedSizeByte() {
        if (buffer == null) {
            return 0L;
        }
        // journal id + buffer
        return Long.SIZE / 8 + (long) buffer.getLength();
    }

    public DataOutputBuffer getBuffer() {
        return buffer;
    }

    public JournalWriter.DrainResult.Status getDrainStatus() {
        return drainStatus;
    }

    public long getCommittedJournalId() {
        return committedJournalId;
    }

    @Override
    public boolean isDone() {
        return latch.getCount() == 0;
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        latch.await();
        if (executeException != null) {
            throw new ExecutionException(executeException);
        }
        return isSucceed;
    }

    @Override
    public Boolean get(long timeout, @NotNull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!latch.await(timeout, unit)) {
            throw new TimeoutException("journal task wait timed out");
        }
        if (executeException != null) {
            throw new ExecutionException(executeException);
        }
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
