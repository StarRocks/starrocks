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

package com.starrocks.server;

import com.starrocks.catalog.AIModel;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.JournalWriteException;
import com.starrocks.persist.DropAIModelLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EditLogException;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AIModelMgrEditLogTest {
    private AIModelMgr mgr;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
        mgr = new AIModelMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testRealJournalRoundTrip() throws Exception {
        AIModel model = mgr.createModel("Chat", AIModelMgrTest.properties(), "", false);
        AIModelMgr follower = new AIModelMgr();
        GlobalStateMgr followerState = mock(GlobalStateMgr.class);
        when(followerState.getAIModelMgr()).thenReturn(follower);
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        AIModel created = (AIModel) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_AI_MODEL);
        Assertions.assertEquals(model, created);
        editLog.loadJournal(followerState, new JournalEntity(OperationType.OP_CREATE_AI_MODEL, created));
        AIModel changed = mgr.alterModel(model, Map.of("model", "new"), "changed");
        AIModel altered = (AIModel) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_ALTER_AI_MODEL);
        editLog.loadJournal(followerState, new JournalEntity(OperationType.OP_ALTER_AI_MODEL, altered));
        Assertions.assertEquals(changed, follower.getById(model.getId()));
        mgr.dropModel(changed);
        DropAIModelLog drop = (DropAIModelLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_AI_MODEL);
        Assertions.assertEquals(model.getId(), drop.getId());
        editLog.loadJournal(followerState, new JournalEntity(OperationType.OP_DROP_AI_MODEL, drop));
        Assertions.assertTrue(follower.listModels().isEmpty());
    }

    @Test
    public void testWalFailuresDoNotPublish() throws Exception {
        EditLog editLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);
        doThrow(new RuntimeException("journal failed")).when(editLog).logCreateAIModel(any(), any());
        Assertions.assertThrows(RuntimeException.class,
                () -> mgr.createModel("Chat", AIModelMgrTest.properties(), "", false));
        Assertions.assertTrue(mgr.listModels().isEmpty());
        AIModel current = AIModel.create(123, "Chat", AIModelMgrTest.properties(), "");
        mgr.replayCreateModel(current);
        doThrow(new RuntimeException("journal failed")).when(editLog).logAlterAIModel(any(), any());
        Assertions.assertThrows(RuntimeException.class,
                () -> mgr.alterModel(current, Map.of("model", "new"), null));
        Assertions.assertSame(current, mgr.getByName("Chat"));
        doThrow(new RuntimeException("journal failed")).when(editLog).logDropAIModel(any(), any());
        Assertions.assertThrows(RuntimeException.class, () -> mgr.dropModel(current));
        Assertions.assertSame(current, mgr.getById(current.getId()));
    }

    @Test
    public void testPublicationOccursOnlyInCallbackAndNoOpDoesNotJournal() throws Exception {
        EditLog editLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);
        doAnswer(invocation -> {
            AIModel payload = invocation.getArgument(0);
            Assertions.assertNull(mgr.getByName(payload.getName()));
            Assertions.assertNull(mgr.getById(payload.getId()));
            WALApplier applier = invocation.getArgument(1);
            applier.apply(payload);
            Assertions.assertSame(payload, mgr.getByName(payload.getName()));
            return null;
        }).when(editLog).logCreateAIModel(any(), any());
        AIModel model = mgr.createModel("Chat", AIModelMgrTest.properties(), "", false);
        Assertions.assertSame(model, mgr.alterModel(model, Map.of("model", "chat-model"), null));
        verify(editLog, never()).logAlterAIModel(any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ALTER", "DROP"})
    public void testRealJournalAbortDoesNotPublish(String operation) throws Exception {
        AIModel current = AIModel.create(123, "Chat", AIModelMgrTest.properties(), "");
        mgr.replayCreateModel(current);
        BlockingQueue<JournalTask> queue = new ArrayBlockingQueue<>(4);
        EditLog original = GlobalStateMgr.getCurrentState().getEditLog();
        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(queue, true));
        AtomicReference<JournalTask> abortedTask = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                JournalTask task = queue.poll(5, TimeUnit.SECONDS);
                if (task != null) {
                    abortedTask.set(task);
                    task.markAbort(new JournalWriteException(JournalWriteException.Reason.WRITER_ABORTED,
                            "journal writer closed"));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.setDaemon(true);
        consumer.start();
        try {
            EditLogException error = Assertions.assertThrows(EditLogException.class, () -> {
                if (operation.equals("ALTER")) {
                    mgr.alterModel(current, Map.of("model", "not-published"), null);
                } else {
                    mgr.dropModel(current);
                }
            });
            Assertions.assertInstanceOf(JournalWriteException.class, error.getCause());
            consumer.join(5000);
            Assertions.assertFalse(consumer.isAlive());
            Assertions.assertNotNull(abortedTask.get());
            Assertions.assertSame(current, mgr.getById(current.getId()));
            Assertions.assertSame(current, mgr.getByName(current.getName()));
        } finally {
            consumer.interrupt();
            consumer.join(5000);
            GlobalStateMgr.getCurrentState().setEditLog(original);
        }
    }
}
