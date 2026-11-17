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

package com.starrocks.persist.image;

import com.starrocks.common.Config;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.proto.GlobalTransactionMgrPB;
import com.starrocks.persist.proto.TransactionIdGeneratorPB;
import com.starrocks.persist.proto.TransactionStatePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.DatabaseTransactionMgr;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionGraph;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TxnStateChangeCallback;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.invoke;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static com.starrocks.persist.image.TransactionStateProtoTest.transaction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GlobalTransactionMgrImageV3Test {
    @TempDir
    Path tempDir;

    static GlobalTransactionMgr manager(long... dbIds) {
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(state.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        GlobalTransactionMgr manager = new GlobalTransactionMgr(state);
        for (long dbId : dbIds) {
            manager.addDatabaseTransactionMgr(dbId);
        }
        return manager;
    }

    static List<Long> finalIds(DatabaseTransactionMgr manager) {
        ArrayDeque<TransactionState> deque = getField(manager, "finalStatusTransactionStateDeque");
        return deque.stream().map(TransactionState::getTransactionId).collect(Collectors.toList());
    }

    private Path writeStates(List<TransactionState> states) throws Exception {
        Path file = tempDir.resolve("transactions-" + System.nanoTime());
        try (OutputStream output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 17)) {
            try (SectionWriter section = writer.beginSection(SectionId.GLOBAL_TRANSACTION_MGR)) {
                section.writeManager(GlobalTransactionMgrPB.newBuilder()
                        .setIdGenerator(TransactionIdGeneratorPB.newBuilder().setBatchEndId(5000)).build());
                for (TransactionState state : states) {
                    section.writeEntry(state.toProto());
                }
            }
            writer.finish();
        }
        return file;
    }

    private GlobalTransactionMgr load(List<TransactionState> states) throws Exception {
        GlobalTransactionMgr loaded = manager(1, 2, 3);
        try (ImageReaderV3 reader = new ImageReaderV3(writeStates(states))) {
            assertEquals(states.size(), reader.sections().get(0).getNumEntries());
            reader.load(loaded.getImageV3Loaders());
        }
        return loaded;
    }

    @Test
    public void testCommitOrderRestoresGraphAndFinalQueue() throws Exception {
        boolean oldPublish = Config.enable_new_publish_mechanism;
        Config.enable_new_publish_mechanism = true;
        try {
            long now = System.currentTimeMillis();
            TransactionState firstCommit = transaction(1, 103, TransactionStatus.COMMITTED, now - 30, -1);
            TransactionState secondCommit = transaction(1, 101, TransactionStatus.COMMITTED, now - 20, -1);
            TransactionState routine = transaction(1, 104, TransactionStatus.PREPARED, -1, -1);
            setField(routine, "sourceType", TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK);
            TransactionState visibleOld = transaction(1, 201, TransactionStatus.VISIBLE, now - 700, now - 500);
            TransactionState visibleYoung = transaction(1, 202, TransactionStatus.VISIBLE, now - 800, now - 100);
            TransactionState aborted = transaction(1, 203, TransactionStatus.ABORTED, -1, now - 300);
            List<TransactionState> input = List.of(visibleYoung, secondCommit, aborted, routine, firstCommit, visibleOld,
                    transaction(2, 301, TransactionStatus.COMMITTED, now - 20, -1),
                    transaction(2, 302, TransactionStatus.ABORTED, -1, now - 50));
            GlobalTransactionMgr loaded = load(input);
            DatabaseTransactionMgr db = loaded.getDatabaseTransactionMgr(1);
            assertEquals(List.of(203L, 202L, 201L), finalIds(db));
            assertEquals(6, db.getTransactionNum());
            assertEquals(2, (int) getField(db, "runningTxnNums"));
            assertEquals(1, (int) getField(db, "runningRoutineLoadTxnNums"));
            assertEquals(now - 20, (long) getField(db, "maxCommitTs"));
            TransactionGraph graph = getField(db, "transactionGraph");
            assertEquals(List.of(103L), graph.getTxnsWithoutDependency());
            graph.remove(103L);
            assertEquals(List.of(101L), graph.getTxnsWithoutDependency());
            TransactionGraph other = getField(loaded.getDatabaseTransactionMgr(2), "transactionGraph");
            assertEquals(List.of(301L), other.getTxnsWithoutDependency());
            assertEquals(List.of(302L), finalIds(loaded.getDatabaseTransactionMgr(2)));
            assertTrue(finalIds(loaded.getDatabaseTransactionMgr(3)).isEmpty());
            assertEquals(5001, loaded.getTransactionIDGenerator().peekNextTransactionId());
            for (TransactionState source : input) {
                GsonEquivalence.assertEquivalent(source,
                        loaded.getTransactionState(source.getDbId(), source.getTransactionId()));
            }

        } finally {
            Config.enable_new_publish_mechanism = oldPublish;
        }
    }

    @Test
    public void testSharedLabelTransactionsStayComplete() throws Exception {
        long now = System.currentTimeMillis();
        List<TransactionState> input = new ArrayList<>();
        for (long i = 1; i <= 40; i++) {
            TransactionState state = transaction(1, i, TransactionStatus.ABORTED, -1, now);
            setField(state, "label", "shared-label");
            input.add(state);
        }
        Collections.reverse(input);
        GlobalTransactionMgr loaded = load(input);
        DatabaseTransactionMgr db = loaded.getDatabaseTransactionMgr(1);
        assertEquals(40, finalIds(db).size());
        Set<Long> expected = input.stream().map(TransactionState::getTransactionId).collect(Collectors.toSet());
        assertEquals(expected, Set.copyOf(finalIds(db)));
        Map<String, Set<Long>> labels = getField(db, "labelToTxnIds");
        assertEquals(expected, labels.get("shared-label"));
        int oldMax = Config.label_keep_max_num;
        Config.label_keep_max_num = 1;
        try {
            db.removeExpiredTxns(now);
            assertEquals(1, finalIds(db).size());
            assertEquals(Set.copyOf(finalIds(db)), labels.get("shared-label"));
        } finally {
            Config.label_keep_max_num = oldMax;
        }
    }

    @Test
    public void testExpiredUnknownAndEmptyImage() throws Exception {
        long now = System.currentTimeMillis();
        long expired = now - (Config.label_keep_max_second + 10L) * 1000;
        List<TransactionState> input = List.of(
                transaction(1, 1, TransactionStatus.ABORTED, -1, expired),
                transaction(1, 2, TransactionStatus.VISIBLE, expired - 1, expired),
                transaction(1, 3, TransactionStatus.UNKNOWN, -1, -1),
                transaction(1, 4, TransactionStatus.PREPARED, -1, -1));
        GlobalTransactionMgr loaded = load(input);
        assertEquals(1, loaded.getDatabaseTransactionMgr(1).getTransactionNum());
        assertTrue(finalIds(loaded.getDatabaseTransactionMgr(1)).isEmpty());
        assertNotNull(loaded.getTransactionState(1, 4));
        GlobalTransactionMgr empty = load(List.of());
        assertEquals(0, empty.getDatabaseTransactionMgr(1).getTransactionNum());
    }

    @Test
    public void testMissingDatabaseFailsAndLoadHasNoWalOrCallbacks() throws Exception {
        Path file = writeStates(List.of(transaction(1, 10, TransactionStatus.PREPARED, -1, -1)));
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            assertThrows(IOException.class, () -> reader.load(manager().getImageV3Loaders()));
        }
        GlobalTransactionMgr loaded = manager(1);
        TxnStateChangeCallback callback = Mockito.mock(TxnStateChangeCallback.class);
        Mockito.when(callback.getId()).thenReturn(77L);
        loaded.getCallbackFactory().addCallback(callback);
        Mockito.clearInvocations(callback);
        TransactionState source = transaction(1, 11, TransactionStatus.VISIBLE, 1, System.currentTimeMillis());
        setField(source, "callbackIdList", List.of(77L));
        Path withCallback = writeStates(List.of(source));
        try (ImageReaderV3 reader = new ImageReaderV3(withCallback)) {
            reader.load(loaded.getImageV3Loaders());
        }
        GlobalStateMgr state = getField(loaded, "globalStateMgr");
        Mockito.verifyNoInteractions(state.getEditLog(), callback);
        assertEquals(List.of(11L), finalIds(loaded.getDatabaseTransactionMgr(1)));
    }

    @Test
    public void testManagerSaveAndV2Reference() throws Exception {
        long now = System.currentTimeMillis();
        GlobalTransactionMgr live = manager(1);
        List<TransactionState> input = List.of(transaction(1, 3, TransactionStatus.ABORTED, -1, now - 1),
                transaction(1, 2, TransactionStatus.VISIBLE, now - 100, now - 30),
                transaction(1, 1, TransactionStatus.PREPARED, -1, -1));
        for (TransactionState state : input) {
            invoke(live.getDatabaseTransactionMgr(1), "unprotectUpsertTransactionState", state);
        }
        live.getTransactionIDGenerator().initTransactionId(3000);
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        live.saveTransactionStateV2(image.getImageWriter());
        GlobalTransactionMgr v2 = manager(1);
        v2.loadTransactionStateV2(image.getMetaBlockReader());
        Path file = tempDir.resolve("manager-save");
        try (OutputStream output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 1)) {
            live.save(writer);
            writer.finish();
        }
        GlobalTransactionMgr v3 = manager(1);
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            assertEquals(3, reader.sections().get(0).getNumEntries());
            reader.load(v3.getImageV3Loaders());
        }
        assertEquals(List.of(3L, 2L), finalIds(v3.getDatabaseTransactionMgr(1)));
        assertEquals(finalIds(v2.getDatabaseTransactionMgr(1)), finalIds(v3.getDatabaseTransactionMgr(1)));
        v2.getDatabaseTransactionMgr(1).removeExpiredTxns(now);
        v3.getDatabaseTransactionMgr(1).removeExpiredTxns(now);
        assertEquals(finalIds(v2.getDatabaseTransactionMgr(1)), finalIds(v3.getDatabaseTransactionMgr(1)));
        assertEquals(3001, v3.getTransactionIDGenerator().peekNextTransactionId());
        for (TransactionState source : input) {
            GsonEquivalence.assertEquivalent(source, v2.getTransactionState(1, source.getTransactionId()));
            GsonEquivalence.assertEquivalent(source, v3.getTransactionState(1, source.getTransactionId()));
        }
    }

    @Test
    public void testFinalCompactionsClearStartupTracking() throws Exception {
        CompactionMgr compactions = Mockito.mock(CompactionMgr.class);
        GlobalStateMgr global = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(global.getCompactionMgr()).thenReturn(compactions);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return global;
            }
        };
        long now = System.currentTimeMillis();
        List<TransactionState> states = List.of(
                transaction(1, 91, TransactionStatus.VISIBLE, now - 10, now),
                transaction(1, 92, TransactionStatus.ABORTED, -1, now),
                transaction(1, 93, TransactionStatus.PREPARED, -1, -1));
        for (TransactionState state : states) {
            setField(state, "sourceType", TransactionState.LoadJobSourceType.LAKE_COMPACTION);
        }
        GlobalTransactionMgr loaded = load(states);
        Mockito.verify(compactions).removeFromStartupActiveCompactionTransactionMap(91);
        Mockito.verify(compactions).removeFromStartupActiveCompactionTransactionMap(92);
        Mockito.verifyNoMoreInteractions(compactions);
        assertEquals(Set.of(91L, 92L), Set.copyOf(finalIds(loaded.getDatabaseTransactionMgr(1))));
        assertNotNull(loaded.getTransactionState(1, 93));
    }

    @Test
    public void testInvalidStatusFailsAsImageError() throws Exception {
        Path file = tempDir.resolve("invalid-status");
        try (OutputStream output = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(output, 1)) {
            try (SectionWriter section = writer.beginSection(SectionId.GLOBAL_TRANSACTION_MGR)) {
                section.writeManager(GlobalTransactionMgrPB.getDefaultInstance());
                section.writeEntry(transaction(1, 1, TransactionStatus.PREPARED, -1, -1).toProto());
                section.writeEntry(TransactionStatePB.newBuilder().setDbId(1).setTransactionStatusValue(999).build());
            }
            writer.finish();
        }
        GlobalTransactionMgr loaded = manager(1);
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            assertEquals(2, reader.sections().get(0).getNumEntries());
            assertThrows(IOException.class, () -> reader.load(loaded.getImageV3Loaders()));
        }
        assertEquals(0, loaded.getDatabaseTransactionMgr(1).getTransactionNum());
    }
}
