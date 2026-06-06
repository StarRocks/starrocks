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
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriteException;
import com.starrocks.journal.SerializeException;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EditLogFailureVisibleTest {
    @BeforeEach
    public void setUp() throws Exception {
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        setLeaderWorkAdmissionOpen(false);
    }

    @Test
    public void testSubmitLogOrThrowRejectsClosedAdmission() {
        EditLog editLog = new EditLog(new ArrayBlockingQueue<>(4));

        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> editLog.submitLogOrThrow((short) 1, new Text("111"), -1));
        Assertions.assertEquals(JournalWriteException.Reason.ADMISSION_CLOSED, exception.getReason());
    }

    @Test
    public void testSubmitLogOrThrowRejectsNotLeader() throws Exception {
        EditLog editLog = new EditLog(new ArrayBlockingQueue<>(4));
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);

        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> editLog.submitLogOrThrow((short) 1, new Text("111"), -1));
        Assertions.assertEquals(JournalWriteException.Reason.NOT_LEADER, exception.getReason());
    }

    @Test
    public void testSubmitLogOrThrowAllowsStarMgrOnFollower() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue);
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);

        JournalTask task = editLog.submitLogOrThrow(OperationType.OP_STARMGR, new Text("111"), -1);

        Assertions.assertSame(task, queue.poll(1, TimeUnit.SECONDS));
    }

    @Test
    public void testSubmitLogOrThrowSerializeFailure() throws Exception {
        EditLog editLog = new EditLog(new ArrayBlockingQueue<>(4));
        setLeaderWorkAdmissionOpen(true);

        Assertions.assertThrows(SerializeException.class,
                () -> editLog.submitLogOrThrow((short) 1, new Writable() {
                    @Override
                    public void write(java.io.DataOutput out) throws IOException {
                        throw new IOException("boom");
                    }
                }, -1));
    }

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
    public void testLogJsonObjectLegacyCompletesSuccessfully() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue);

        Thread consumer = succeedQueuedTaskAsync(queue);
        editLog.logJsonObject((short) 1, "payload");
        consumer.join();
    }

    @Test
    public void testLogJsonObjectLegacyWalApplierAppliesOnSuccess() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue);
        AtomicBoolean applied = new AtomicBoolean(false);

        Thread consumer = succeedQueuedTaskAsync(queue);
        editLog.logJsonObject((short) 1, "payload", obj -> applied.set(true));
        consumer.join();

        Assertions.assertTrue(applied.get());
    }

    @Test
    public void testLogJsonObjectOrThrowAppliesOnSuccess() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue);
        AtomicBoolean applied = new AtomicBoolean(false);

        Thread consumer = succeedQueuedTaskAsync(queue);
        editLog.logJsonObjectOrThrow(OperationType.OP_STARMGR, "payload", obj -> applied.set(true));
        consumer.join();

        Assertions.assertTrue(applied.get());
    }

    @Test
    public void testLogJsonObjectOrThrowDoesNotApplyOnAbort() throws Exception {
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog editLog = new EditLog(queue);
        AtomicBoolean applied = new AtomicBoolean(false);

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

        JournalWriteException exception = Assertions.assertThrows(JournalWriteException.class,
                () -> editLog.logJsonObjectOrThrow(OperationType.OP_STARMGR, "payload", obj -> applied.set(true)));
        consumer.join();

        Assertions.assertEquals(JournalWriteException.Reason.WRITER_ABORTED, exception.getReason());
        Assertions.assertFalse(applied.get());
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

    private void setLeaderWorkAdmissionOpen(boolean open) throws Exception {
        Field field = GlobalStateMgr.class.getDeclaredField("leaderWorkAdmissionOpen");
        field.setAccessible(true);
        AtomicBoolean admissionOpen = (AtomicBoolean) field.get(GlobalStateMgr.getCurrentState());
        admissionOpen.set(open);
    }
}
