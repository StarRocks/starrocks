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
import com.starrocks.common.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Covers the JournalWriter demotion seal in the barrier-free model:
 * beginSeal() (RUNNING -> SEALING) then close() (stop daemon, assert queue empty, return the committed
 * watermark). The queue drain is owned by EditLog's in-flight fence (awaitWalDrained); the writer no longer
 * enqueues a barrier task, so these tests exercise the batch-commit state machine and close() directly.
 */
public class JournalWriterSealTest {

    @Test
    public void testWriteBatchCommitsAndAdvancesWatermark() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);

        JournalTask task1 = new JournalTask(System.nanoTime(), makeBuffer(10), -1);
        JournalTask task2 = new JournalTask(System.nanoTime(), makeBuffer(11), -1);
        queue.put(task1);
        queue.put(task2);

        writer.writeOneBatch();

        Assertions.assertTrue(task1.get());
        Assertions.assertTrue(task2.get());
        Assertions.assertEquals(5L, writer.getLastCommittedJournalId());
        Assertions.assertEquals(List.of(4L, 5L), journal.getCommittedJournalIds());
    }

    @Test
    public void testCommitFailureWhileSealingAbortsBatchGracefully() throws Exception {
        // G4: once sealing, a commit failure aborts the batch with WRITER_ABORTED instead of exiting the JVM.
        TestJournal journal = new TestJournal();
        journal.setCommitFailure(true);
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);

        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(10), -1);
        queue.put(task);

        writer.beginSeal();
        writer.writeOneBatch();

        ExecutionException abortException = Assertions.assertThrows(ExecutionException.class, task::get);
        Assertions.assertInstanceOf(JournalWriteException.class, abortException.getCause());
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED,
                ((JournalWriteException) abortException.getCause()).getReason());
        Assertions.assertEquals(3L, writer.getLastCommittedJournalId());
    }

    @Test
    public void testCommitRetryDisabledAfterSealStarts() throws Exception {
        // G6: the commit retry predicate is true while RUNNING and false once beginSeal() flips to SEALING,
        // so a healthy batch can still commit during a drain but a failing one stops retrying.
        BlockingCommitJournal journal = new BlockingCommitJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);

        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(10), -1);
        queue.put(task);

        CompletableFuture<Void> writeFuture = CompletableFuture.runAsync(() -> {
            try {
                writer.writeOneBatch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        Assertions.assertTrue(journal.commitEntered.await(5, TimeUnit.SECONDS));
        Assertions.assertTrue(journal.retryAllowedBeforeSeal.get());

        writer.beginSeal();
        journal.allowCommitReturn.countDown();
        writeFuture.get(5, TimeUnit.SECONDS);

        Assertions.assertFalse(journal.retryAllowedAfterSeal.get());

        ExecutionException abortException = Assertions.assertThrows(ExecutionException.class, task::get);
        Assertions.assertInstanceOf(JournalWriteException.class, abortException.getCause());
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED,
                ((JournalWriteException) abortException.getCause()).getReason());
    }

    @Test
    public void testCloseReturnsWatermarkStopsDaemonAndAllowsCleanRestart() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        writer.startDaemon();
        waitUntilDaemonAlive(writer);

        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(10), -1);
        queue.put(task);
        Assertions.assertTrue(task.get());

        writer.beginSeal();
        long watermark = writer.close(5000L);
        Assertions.assertEquals(4L, watermark);
        Assertions.assertFalse(writer.isDaemonAlive());

        // the writer can be re-initialized and restarted cleanly after a close
        writer.init(3L);
        writer.startDaemon();
        waitUntilDaemonAlive(writer);
        Assertions.assertEquals(3L, writer.close(5000L));
        Assertions.assertFalse(writer.isDaemonAlive());
    }

    @Test
    public void testCloseFailsWhenQueueNotDrained() throws Exception {
        // G5: close() loudly fails if a task is still queued (a count-all coverage gap) instead of orphaning it.
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        // no daemon started, so the queued task is never consumed
        queue.put(new JournalTask(System.nanoTime(), makeBuffer(10), -1));

        Assertions.assertThrows(IllegalStateException.class, () -> writer.close(1000L));
    }

    @Test
    public void testClosedWriterAbortsStragglerTask() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        writer.close(1000L);   // no daemon, empty queue -> writer state CLOSED

        JournalTask straggler = new JournalTask(System.nanoTime(), makeBuffer(10), -1);
        queue.put(straggler);
        writer.writeOneBatch();

        ExecutionException abortException = Assertions.assertThrows(ExecutionException.class, straggler::get);
        Assertions.assertInstanceOf(JournalWriteException.class, abortException.getCause());
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED,
                ((JournalWriteException) abortException.getCause()).getReason());
    }

    @Test
    public void testForceRollJournalTriggersManualRollReason() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);
        journal.clearRollJournalIds();

        queue.put(new JournalTask(System.nanoTime(), makeBuffer(10), -1));
        writer.setForceRollJournal();
        writer.writeOneBatch();

        Assertions.assertEquals(List.of(5L), journal.getRollJournalIds());
    }

    private void waitUntilDaemonAlive(JournalWriter writer) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!writer.isDaemonAlive() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        Assertions.assertTrue(writer.isDaemonAlive(), "journal writer daemon must start");
    }

    private DataOutputBuffer makeBuffer(int size) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, "x".repeat(size - 4));
        return buffer;
    }

    private static class TestJournal implements Journal {
        private long maxJournalId = 0L;
        private boolean commitFailure;
        private final List<Long> stagingJournalIds = new ArrayList<>();
        private final List<Long> committedJournalIds = new ArrayList<>();
        private final List<Long> rollJournalIds = new ArrayList<>();

        @Override
        public void open() {
        }

        @Override
        public void rollJournal(long journalId) throws JournalException {
            rollJournalIds.add(journalId);
        }

        @Override
        public long getMaxJournalId() {
            return maxJournalId;
        }

        @Override
        public void close() {
        }

        @Override
        public JournalCursor read(long fromKey, long toKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteJournals(long deleteJournalToId) {
        }

        @Override
        public long getFinalizedJournalId() {
            return 0;
        }

        @Override
        public List<Long> getDatabaseNames() {
            return List.of();
        }

        @Override
        public void batchWriteBegin() {
            stagingJournalIds.clear();
        }

        @Override
        public void batchWriteAppend(long journalId, DataOutputBuffer buffer) {
            stagingJournalIds.add(journalId);
        }

        @Override
        public void batchWriteCommit() throws JournalException {
            if (commitFailure) {
                throw new JournalException("mock commit failure");
            }
            committedJournalIds.addAll(stagingJournalIds);
            maxJournalId += stagingJournalIds.size();
            stagingJournalIds.clear();
        }

        @Override
        public void batchWriteAbort() {
            stagingJournalIds.clear();
        }

        @Override
        public String getPrefix() {
            return "";
        }

        public void setCommitFailure(boolean commitFailure) {
            this.commitFailure = commitFailure;
        }

        public List<Long> getCommittedJournalIds() {
            return committedJournalIds;
        }

        public void clearRollJournalIds() {
            rollJournalIds.clear();
        }

        public List<Long> getRollJournalIds() {
            return rollJournalIds;
        }
    }

    private static final class BlockingCommitJournal extends TestJournal {
        private final CountDownLatch commitEntered = new CountDownLatch(1);
        private final CountDownLatch allowCommitReturn = new CountDownLatch(1);
        private final AtomicBoolean retryAllowedBeforeSeal = new AtomicBoolean(false);
        private final AtomicBoolean retryAllowedAfterSeal = new AtomicBoolean(true);

        @Override
        public void batchWriteCommit(BooleanSupplier shouldRetry) throws InterruptedException, JournalException {
            retryAllowedBeforeSeal.set(shouldRetry.getAsBoolean());
            commitEntered.countDown();
            Assertions.assertTrue(allowCommitReturn.await(5, TimeUnit.SECONDS));
            retryAllowedAfterSeal.set(shouldRetry.getAsBoolean());
            throw new JournalException("mock commit failure after seal");
        }
    }
}
