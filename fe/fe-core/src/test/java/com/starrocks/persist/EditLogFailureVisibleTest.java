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

package com.starrocks.persist;

import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriteException;
import com.starrocks.journal.SerializeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Covers the EditLog leader WAL-apply fence: the write-admission gate (open/close, fixed-open by
 * construction), the count-all in-flight accounting, the WALApplier hook, and the demotion drain
 * (awaitWalDrained). The gate/fence lives entirely in EditLog, so these tests drive it directly via the
 * constructor gate state and openWalGate/closeWalGate, with no GlobalStateMgr state.
 */
public class EditLogFailureVisibleTest {

    // ---- write-admission gate ----

    @Test
    public void testGatedWriteRejectedWhenGateClosed() {
        EditLog editLog = new EditLog(new ArrayBlockingQueue<>(4), false);
        JournalWriteException e = Assertions.assertThrows(JournalWriteException.class,
                () -> editLog.logJsonObjectOrThrow((short) 1, "x", obj -> { }));
        Assertions.assertEquals(JournalWriteException.Reason.ADMISSION_CLOSED, e.getReason());
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    @Test
    public void testGatedWriteRejectedAfterCloseWalGate() {
        EditLog editLog = new EditLog(new ArrayBlockingQueue<>(4), true);
        editLog.closeWalGate();
        JournalWriteException e = Assertions.assertThrows(JournalWriteException.class,
                () -> editLog.logJsonObjectOrThrow((short) 1, "x", obj -> { }));
        Assertions.assertEquals(JournalWriteException.Reason.ADMISSION_CLOSED, e.getReason());
    }

    @Test
    public void testGateOpenByConstructorAdmitsWrites() throws Exception {
        // An EditLog constructed gate-open (StarMgr, checkpoint, tests) admits writes with no openWalGate call.
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);
        AtomicBoolean applied = new AtomicBoolean(false);
        Thread consumer = succeedQueuedTaskAsync(queue);
        editLog.logJsonObjectOrThrow((short) 1, "payload", obj -> applied.set(true));
        consumer.join();
        Assertions.assertTrue(applied.get());
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    // ---- count-all in-flight accounting ----

    @Test
    public void testNoApplierGatedWriteIsCounted() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);

        // a plain write with no WALApplier is still admitted through the gate and counted (count-all)
        Thread writer = new Thread(() -> editLog.logJsonObject((short) 1, "payload"));
        writer.setDaemon(true);
        writer.start();

        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        Assertions.assertNotNull(task);
        Assertions.assertEquals(1, editLog.inFlightForTest());

        task.markSucceed();
        writer.join(5000);
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    // ---- WALApplier hook ----

    @Test
    public void testGatedWriteAppliesOnSuccess() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);
        AtomicBoolean applied = new AtomicBoolean(false);

        Thread consumer = succeedQueuedTaskAsync(queue);
        editLog.logJsonObject((short) 1, "payload", obj -> applied.set(true));
        consumer.join();

        Assertions.assertTrue(applied.get());
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    @Test
    public void testGatedWriteDoesNotApplyOnAbortAndReleasesFence() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);
        AtomicBoolean applied = new AtomicBoolean(false);

        Thread consumer = abortQueuedTaskAsync(queue);
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> editLog.logJsonObject((short) 1, "payload", obj -> applied.set(true)));
        consumer.join();

        Assertions.assertInstanceOf(JournalWriteException.class, exception.getCause());
        Assertions.assertFalse(applied.get());
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    @Test
    public void testOrThrowAppliesOnSuccess() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);
        AtomicBoolean applied = new AtomicBoolean(false);

        Thread consumer = succeedQueuedTaskAsync(queue);
        editLog.logJsonObjectOrThrow((short) 1, "payload", obj -> applied.set(true));
        consumer.join();

        Assertions.assertTrue(applied.get());
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    @Test
    public void testOrThrowDoesNotApplyOnAbort() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);
        AtomicBoolean applied = new AtomicBoolean(false);

        Thread consumer = abortQueuedTaskAsync(queue);
        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> editLog.logJsonObjectOrThrow((short) 1, "payload", obj -> applied.set(true)));
        consumer.join();

        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED, exception.getReason());
        Assertions.assertFalse(applied.get());
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    @Test
    public void testSerializeFailureReleasesFence() {
        EditLog editLog = new EditLog(new ArrayBlockingQueue<>(4), true);

        Assertions.assertThrows(SerializeException.class,
                () -> editLog.logEdit((short) 1, new Writable() {
                    @Override
                    public void write(DataOutput out) throws IOException {
                        throw new IOException("boom");
                    }
                }));
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    // ---- demotion drain (awaitWalDrained) ----

    @Test
    public void testAwaitWalDrainedReturnsImmediatelyWhenNoInFlight() {
        EditLog editLog = new EditLog(new ArrayBlockingQueue<>(4), true);
        editLog.awaitWalDrained(1000L);
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    @Test
    public void testAwaitWalDrainedWaitsForInFlightThenReturns() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);

        Thread writer = new Thread(() -> editLog.logJsonObject((short) 1, "payload"));
        writer.setDaemon(true);
        writer.start();
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        Assertions.assertNotNull(task);

        CompletableFuture<Void> drain = CompletableFuture.runAsync(() -> editLog.awaitWalDrained(5000L));
        Thread.sleep(150L);
        Assertions.assertFalse(drain.isDone(), "drain must block while a write is in flight");

        task.markSucceed();
        drain.get(5, TimeUnit.SECONDS);
        writer.join(5000);
        Assertions.assertEquals(0, editLog.inFlightForTest());
    }

    @Test
    public void testAwaitWalDrainedTimesOutWhenInFlightStuck() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);

        Thread writer = new Thread(() -> {
            try {
                editLog.logJsonObject((short) 1, "payload");
            } catch (Throwable ignore) {
                // released below
            }
        });
        writer.setDaemon(true);
        writer.start();
        JournalTask task = queue.poll(5, TimeUnit.SECONDS);
        Assertions.assertNotNull(task);

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> editLog.awaitWalDrained(200L));
        Assertions.assertTrue(e.getMessage().contains("timed out"));

        task.markSucceed();
        writer.join(5000);
    }

    @Test
    public void testAwaitWalDrainedSurfacesApplyFailure() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);

        RuntimeException boom = new RuntimeException("apply boom");
        Thread consumer = succeedQueuedTaskAsync(queue);
        RuntimeException thrown = Assertions.assertThrows(RuntimeException.class,
                () -> editLog.logJsonObject((short) 1, "payload", obj -> {
                    throw boom;
                }));
        consumer.join();
        Assertions.assertSame(boom, thrown);
        Assertions.assertEquals(0, editLog.inFlightForTest());

        IllegalStateException drainEx = Assertions.assertThrows(IllegalStateException.class,
                () -> editLog.awaitWalDrained(1000L));
        Assertions.assertTrue(drainEx.getMessage().contains("apply failed"));
    }

    @Test
    public void testOpenGateClearsPriorApplyFailure() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue, true);

        RuntimeException boom = new RuntimeException("apply boom");
        Thread consumer = succeedQueuedTaskAsync(queue);
        Assertions.assertThrows(RuntimeException.class,
                () -> editLog.logJsonObject((short) 1, "payload", obj -> {
                    throw boom;
                }));
        consumer.join();

        // a fresh leader session (openWalGate) clears the recorded apply failure
        editLog.openWalGate();
        editLog.awaitWalDrained(1000L);
    }

    // ---- static wait helpers (unchanged terminal/transient classification) ----

    @Test
    public void testWaitOrThrowPropagatesAbortReason() throws Exception {
        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(8), -1);
        task.markAbort(new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED, "writer sealed"));

        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> EditLog.waitOrThrow(task, 1000L));
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED, exception.getReason());
    }

    @Test
    public void testWaitOrThrowWrapsUnknownAbortCause() throws Exception {
        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(8), -1);
        RuntimeException cause = new RuntimeException("boom");
        task.markAbort(cause);

        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> EditLog.waitOrThrow(task, 1000L));
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED, exception.getReason());
        Assertions.assertSame(cause, exception.getCause());
    }

    @Test
    public void testWaitOrThrowRejectsAbortWithoutDetailedCause() throws Exception {
        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(8), -1);
        task.markAbort();

        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> EditLog.waitOrThrow(task, -1L));
        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED, exception.getReason());
    }

    @Test
    public void testWaitOrThrowTimesOut() {
        JournalTask task = new JournalTask(System.nanoTime(), new DataOutputBuffer(), -1);

        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> EditLog.waitOrThrow(task, 1L));
        Assertions.assertEquals(JournalWriteException.Reason.TIMEOUT, exception.getReason());
    }

    @Test
    public void testWaitInfinityStopsOnTerminalAbort() throws Exception {
        JournalTask task = new JournalTask(System.nanoTime(), makeBuffer(8), -1);
        task.markAbort(new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED,
                "journal commit failed while sealing"));

        CountDownLatch finished = new CountDownLatch(1);
        AtomicBoolean failedWithAbort = new AtomicBoolean(false);
        Thread waiter = new Thread(() -> {
            try {
                EditLog.waitInfinity(task);
            } catch (IllegalStateException e) {
                failedWithAbort.set(e.getCause() instanceof JournalWriteException);
            } finally {
                finished.countDown();
            }
        });
        waiter.setDaemon(true);
        waiter.start();

        Assertions.assertTrue(finished.await(1, TimeUnit.SECONDS),
                "waitInfinity must stop after a terminal (WRITER_ABORTED) journal abort");
        Assertions.assertTrue(failedWithAbort.get());
    }

    private Thread abortQueuedTaskAsync(BlockingQueue<JournalTask> queue) {
        Thread consumer = new Thread(() -> {
            try {
                JournalTask task = queue.poll(5, TimeUnit.SECONDS);
                if (task != null) {
                    task.markAbort(new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED,
                            "journal writer closed"));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();
        return consumer;
    }

    private Thread succeedQueuedTaskAsync(BlockingQueue<JournalTask> queue) {
        Thread consumer = new Thread(() -> {
            try {
                JournalTask task = queue.poll(5, TimeUnit.SECONDS);
                if (task != null) {
                    task.markSucceed();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();
        return consumer;
    }

    private DataOutputBuffer makeBuffer(int size) throws IOException {
        DataOutputBuffer buffer = new DataOutputBuffer();
        Text.writeString(buffer, "x".repeat(size - 4));
        return buffer;
    }
}
