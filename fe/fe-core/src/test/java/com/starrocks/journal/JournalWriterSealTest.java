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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class JournalWriterSealTest {
    @Test
    public void testSealOnEmptyQueueReturnsCommittedWatermark() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);

        CompletableFuture<JournalWriter.DrainResult> resultFuture = runSeal(writer, 5000L);
        waitUntilQueueSize(queue, 1);

        writer.writeOneBatch();

        JournalWriter.DrainResult result = resultFuture.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(JournalWriter.DrainResult.Status.BARRIER_REACHED, result.getStatus());
        Assertions.assertEquals(3L, result.getLastCommittedJournalId());
        Assertions.assertEquals(3L, writer.getLastCommittedJournalId());
    }

    @Test
    public void testSealBarrierCutsBatchAndAbortsPostBarrierTasks() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);

        JournalTask task1 = new JournalTask(System.nanoTime(), makeBuffer(10), -1);
        JournalTask task2 = new JournalTask(System.nanoTime(), makeBuffer(11), -1);
        JournalTask task3 = new JournalTask(System.nanoTime(), makeBuffer(12), -1);
        queue.put(task1);
        queue.put(task2);

        CompletableFuture<JournalWriter.DrainResult> resultFuture = runSeal(writer, 5000L);
        waitUntilQueueSize(queue, 3);
        queue.put(task3);

        writer.writeOneBatch();

        JournalWriter.DrainResult result = resultFuture.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(JournalWriter.DrainResult.Status.BARRIER_REACHED, result.getStatus());
        Assertions.assertEquals(5L, result.getLastCommittedJournalId());
        Assertions.assertTrue(task1.get());
        Assertions.assertTrue(task2.get());

        ExecutionException abortException = Assertions.assertThrows(ExecutionException.class, task3::get);
        Assertions.assertInstanceOf(JournalWriteException.class, abortException.getCause());
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED,
                ((JournalWriteException) abortException.getCause()).getReason());
        Assertions.assertEquals(List.of(4L, 5L), journal.getCommittedJournalIds());
    }

    @Test
    public void testSealCommitFailureReturnsLeaderLost() throws Exception {
        TestJournal journal = new TestJournal();
        journal.setCommitFailure(true);
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);

        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(10), -1);
        queue.put(task);

        CompletableFuture<JournalWriter.DrainResult> resultFuture = runSeal(writer, 5000L);
        waitUntilQueueSize(queue, 2);

        writer.writeOneBatch();

        JournalWriter.DrainResult result = resultFuture.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(JournalWriter.DrainResult.Status.LEADER_LOST, result.getStatus());
        Assertions.assertEquals(3L, result.getLastCommittedJournalId());

        ExecutionException abortException = Assertions.assertThrows(ExecutionException.class, task::get);
        Assertions.assertInstanceOf(JournalWriteException.class, abortException.getCause());
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED,
                ((JournalWriteException) abortException.getCause()).getReason());
    }

    @Test
    public void testSealTimeoutReturnsLatestCommittedWatermark() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(7L);

        JournalWriter.DrainResult result = writer.sealAndGetCommittedWatermark(1L);

        Assertions.assertEquals(JournalWriter.DrainResult.Status.TIMEOUT, result.getStatus());
        Assertions.assertEquals(7L, result.getLastCommittedJournalId());
    }

    @Test
    public void testSealOnClosedWriterReturnsCachedResult() throws Exception {
        TestJournal journal = new TestJournal();
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(16);
        JournalWriter writer = new JournalWriter(journal, queue);
        writer.init(3L);

        CompletableFuture<JournalWriter.DrainResult> resultFuture = runSeal(writer, 5000L);
        waitUntilQueueSize(queue, 1);
        writer.writeOneBatch();

        JournalWriter.DrainResult first = resultFuture.get(5, TimeUnit.SECONDS);
        JournalWriter.DrainResult second = writer.sealAndGetCommittedWatermark(100L);

        Assertions.assertEquals(first.getStatus(), second.getStatus());
        Assertions.assertEquals(first.getLastCommittedJournalId(), second.getLastCommittedJournalId());
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

    private CompletableFuture<JournalWriter.DrainResult> runSeal(JournalWriter writer, long timeoutMs) {
        CompletableFuture<JournalWriter.DrainResult> future = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                future.complete(writer.sealAndGetCommittedWatermark(timeoutMs));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        thread.start();
        return future;
    }

    private void waitUntilQueueSize(BlockingQueue<JournalTask> queue, int expectedSize) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (queue.size() < expectedSize && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        Assertions.assertTrue(queue.size() >= expectedSize,
                "expected queue size >= " + expectedSize + ", actual=" + queue.size());
    }

    private DataOutputBuffer makeBuffer(int size) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, "x".repeat(size - 4));
        return buffer;
    }

    private static final class TestJournal implements Journal {
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
}
