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

package com.starrocks.transaction;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.TxnInfoHelper;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.PUniqueId;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The load ids of an explicit transaction (BEGIN; INSERT ...; COMMIT) name the per-statement txn logs on a
// shared-data table, so the FE that publishes the transaction after a restart or a leader change needs them
// from the journal.
public class ExplicitTxnLoadIdsPersistTest {
    private static final long DB_ID = 10001L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // The transaction state as TransactionStmtExecutor builds it: beginStmt creates it and commitStmt records the
    // load id of every statement before the commit journals it as COMMITTED.
    private static TransactionState newCommittedExplicitTxn(long txnId, List<TUniqueId> statementLoadIds) {
        TransactionState state = new TransactionState(txnId, "explicit_txn_" + txnId, null,
                LoadJobSourceType.INSERT_STREAMING, new TxnCoordinator(TxnSourceType.FE, "127.0.0.1"), 60_000L);
        state.setDbId(DB_ID);
        statementLoadIds.forEach(state::addLoadId);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        return state;
    }

    private static void assertLoadIds(List<TUniqueId> expected, TxnInfoPB info) {
        assertNotNull(info.loadIds);
        assertEquals(expected.size(), info.loadIds.size());
        for (int i = 0; i < expected.size(); i++) {
            PUniqueId actual = info.loadIds.get(i);
            assertEquals(expected.get(i).getHi(), actual.getHi().longValue(), "hi of load id " + i);
            assertEquals(expected.get(i).getLo(), actual.getLo().longValue(), "lo of load id " + i);
        }
    }

    @Test
    public void testLoadIdsSurviveJournalReplay() throws Exception {
        long txnId = 20001L;
        List<TUniqueId> statementLoadIds = List.of(UUIDUtil.genTUniqueId(), UUIDUtil.genTUniqueId());
        TransactionState leaderState = newCommittedExplicitTxn(txnId, statementLoadIds);
        assertLoadIds(statementLoadIds, TxnInfoHelper.fromTransactionState(leaderState));

        GlobalStateMgr.getCurrentState().getEditLog().logInsertTransactionState(leaderState, state -> {
        });

        // A follower (or the restarted leader) replays the journal and later publishes the transaction.
        TransactionState replayed = (TransactionState) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_UPSERT_TRANSACTION_STATE_V2);
        GlobalTransactionMgr followerTxnMgr = new GlobalTransactionMgr(GlobalStateMgr.getCurrentState());
        followerTxnMgr.addDatabaseTransactionMgr(DB_ID);
        followerTxnMgr.replayUpsertTransactionState(replayed);

        TransactionState toPublish = followerTxnMgr.getTransactionState(DB_ID, txnId);
        assertNotNull(toPublish);
        assertEquals(TransactionStatus.COMMITTED, toPublish.getTransactionStatus());
        assertFalse(toPublish.hasLostLoadIds());
        assertLoadIds(statementLoadIds, TxnInfoHelper.fromTransactionState(toPublish));
    }

    @Test
    public void testLoadWithoutLoadIdIsNotRecorded() {
        // commitStmt of a multi-statement stream load adds the load id of every load, which is null.
        TransactionState state = new TransactionState(20004L, "stream_load_txn", null,
                LoadJobSourceType.MULTI_STATEMENT_STREAMING, new TxnCoordinator(TxnSourceType.FE, "127.0.0.1"), 60_000L);
        state.addLoadId(null);
        state.addLoadId(null);
        assertNull(state.getLoadIds());
        assertFalse(state.hasLostLoadIds());
    }

    @Test
    public void testNullLoadIdsJournaledByOlderFeAreDropped() {
        TransactionState state = new TransactionState(20005L, "stream_load_txn", null,
                LoadJobSourceType.MULTI_STATEMENT_STREAMING, new TxnCoordinator(TxnSourceType.FE, "127.0.0.1"), 60_000L);
        state.setDbId(DB_ID);
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        // What an older FE journaled for a multi-statement stream load: a null load id per load.
        JsonObject json = GsonUtils.GSON.toJsonTree(state).getAsJsonObject();
        JsonArray oldLoadIds = new JsonArray();
        oldLoadIds.add(JsonNull.INSTANCE);
        oldLoadIds.add(JsonNull.INSTANCE);
        json.add("loadIds", oldLoadIds);

        TransactionState replayed = GsonUtils.GSON.fromJson(json.toString(), TransactionState.class);
        assertNull(replayed.getLoadIds());
        assertFalse(replayed.hasLostLoadIds());
        assertNotNull(replayed.toString());
    }

    @Test
    public void testLoadIdsAreNotPersistedUnderTheOldKey() {
        TransactionState state = newCommittedExplicitTxn(20002L, List.of(UUIDUtil.genTUniqueId()));
        JsonObject json = GsonUtils.GSON.toJsonTree(state).getAsJsonObject();
        // An older FE reading this entry finds no load ids, so the BE looks for the txn log of a single-statement
        // load, finds none and fails the publish, instead of reading the ids as 0-0.
        assertFalse(json.has("loadIds"));
        assertTrue(json.has("lids"));
    }

    @Test
    public void testLoadIdsJournaledByOlderFeAreLost() {
        long txnId = 20003L;
        TransactionState state = newCommittedExplicitTxn(txnId, List.of(UUIDUtil.genTUniqueId(), UUIDUtil.genTUniqueId()));
        // What an older FE journaled: every TUniqueId as {}, under "loadIds".
        JsonObject json = GsonUtils.GSON.toJsonTree(state).getAsJsonObject();
        json.remove("lids");
        JsonArray oldLoadIds = new JsonArray();
        oldLoadIds.add(new JsonObject());
        oldLoadIds.add(new JsonObject());
        json.add("loadIds", oldLoadIds);

        TransactionState replayed = GsonUtils.GSON.fromJson(json.toString(), TransactionState.class);
        assertEquals(TransactionStatus.COMMITTED, replayed.getTransactionStatus());
        assertTrue(replayed.hasLostLoadIds());
        // The publish keeps one load id per statement. They are 0-0 and name no txn log, so the BE fails the
        // publish rather than make the transaction visible without the data of its statements.
        TxnInfoPB info = TxnInfoHelper.fromTransactionState(replayed);
        assertNotNull(info.loadIds);
        assertEquals(2, info.loadIds.size());
        for (PUniqueId loadId : info.loadIds) {
            assertEquals(0L, loadId.getHi().longValue());
            assertEquals(0L, loadId.getLo().longValue());
        }
    }
}
