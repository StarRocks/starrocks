// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.transaction;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.TxnFinishStatePB;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";

    @AfterEach
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerDe() {
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", UUIDUtil.genTUniqueId(),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);

        String json = GsonUtils.GSON.toJson(transactionState);
        TransactionState readTransactionState = GsonUtils.GSON.fromJson(json, TransactionState.class);
        Assertions.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
    }

    @Test
    public void testSerDeTxnFinishStatePB() throws IOException {
        Codec<TxnFinishStatePB> finishStatePBCodec = ProtobufProxy.create(TxnFinishStatePB.class);
        for (int i = 1; i <= 100000; i *= 10) {
            TxnFinishStatePB txnFinishStatePB = buildTxnFinishState(i).toPB();
            byte[] bytes = finishStatePBCodec.encode(txnFinishStatePB);
            // System.out.printf("normal: %d abnormal: %d  size: %d\n", txnFinishStatePB.normalReplicas.size(),
            //        txnFinishStatePB.abnormalReplicasWithVersion.size(), bytes.length);
            TxnFinishStatePB txn2 = finishStatePBCodec.decode(bytes);
            Assertions.assertEquals(txnFinishStatePB.normalReplicas.size(), txn2.normalReplicas.size());
            Assertions.assertEquals(txnFinishStatePB.abnormalReplicasWithVersion.size(), txn2.abnormalReplicasWithVersion.size());
        }
    }

    @Test
    public void testSerDeTxnFinishStateJSON() throws IOException {
        for (int i = 1; i <= 100000; i *= 10) {
            TxnFinishState s1 = buildTxnFinishState(i);
            String json = GsonUtils.GSON.toJson(s1);
            // System.out.printf("json: %s\n", json);
            TxnFinishState s2 = GsonUtils.GSON.fromJson(json, TxnFinishState.class);
            Assertions.assertEquals(s1.normalReplicas.size(), s2.normalReplicas.size());
            Assertions.assertEquals(s1.abnormalReplicasWithVersion.size(), s2.abnormalReplicasWithVersion.size());
        }
    }

    @NotNull
    private TxnFinishState buildTxnFinishState(int numNormal) {
        TxnFinishState txnFinishState = new TxnFinishState();
        txnFinishState.normalReplicas = new HashSet<>();
        for (long j = 0; j < numNormal; j++) {
            txnFinishState.normalReplicas.add(10000 + j);
        }
        txnFinishState.abnormalReplicasWithVersion = new HashMap<>();
        for (long j = 0; j < 10; j++) {
            txnFinishState.abnormalReplicasWithVersion.put(j + 10000, j + 1000);
        }
        return txnFinishState;
    }

    @Test
    public void testSerDeTxnStateNewFinish() {
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", UUIDUtil.genTUniqueId(),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);

        transactionState.setFinishState(buildTxnFinishState(10));
        transactionState.setErrorReplicas(Sets.newHashSet(20000L, 20001L));
        transactionState.setFinishTime(System.currentTimeMillis());
        transactionState.clearErrorMsg();
        transactionState.setNewFinish();
        transactionState.setTransactionStatus(TransactionStatus.VISIBLE);

        String json = GsonUtils.GSON.toJson(transactionState);
        TransactionState readTransactionState = GsonUtils.GSON.fromJson(json, TransactionState.class);
        assertTrue(readTransactionState.isNewFinish());
    }

    @Test
    public void testIsRunning() {
        Set<TransactionStatus> nonRunningStatus = new HashSet<>();
        nonRunningStatus.add(TransactionStatus.UNKNOWN);
        nonRunningStatus.add(TransactionStatus.VISIBLE);
        nonRunningStatus.add(TransactionStatus.ABORTED);

        for (TransactionStatus status : TransactionStatus.values()) {
            TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                    3000, "label123", UUIDUtil.genTUniqueId(),
                    LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                    60 * 1000L);
            transactionState.setTransactionStatus(status);
            Assertions.assertEquals(nonRunningStatus.contains(status), !transactionState.isRunning());
        }
    }

    @Test
    public void testCheckReplicaNeedSkip() {
        TransactionState state = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L), 3000, "label123",
                UUIDUtil.genTUniqueId(), LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"),
                50000L, 60 * 1000L);

        PartitionCommitInfo pcInfo = new PartitionCommitInfo(1L, 100L, 1L);

        Tablet tablet0 = new LocalTablet(1001L);
        Tablet tablet1 = new LocalTablet(10001L);
        Tablet tablet2 = new LocalTablet(10002L);

        TabletCommitInfo info1 = new TabletCommitInfo(10001L, 10001L);
        TabletCommitInfo info2 = new TabletCommitInfo(10001L, 10002L);
        TabletCommitInfo info3 = new TabletCommitInfo(10002L, 10002L);
        List<TabletCommitInfo> infos = new ArrayList<>();
        infos.add(info1);
        infos.add(info2);
        infos.add(info3);
        state.setTabletCommitInfos(infos);

        // replica state is not normal and clone
        assertFalse(state.checkReplicaNeedSkip(tablet0, new Replica(1L, 1L, ReplicaState.ALTER, 1L, 0), pcInfo));
        assertFalse(
                state.checkReplicaNeedSkip(tablet0, new Replica(1L, 1L, ReplicaState.SCHEMA_CHANGE, 1L, 0), pcInfo));
        assertTrue(state.checkReplicaNeedSkip(tablet0, new Replica(1L, 1L, ReplicaState.NORMAL, 1L, 0), pcInfo));
        assertTrue(state.checkReplicaNeedSkip(tablet0, new Replica(1L, 1L, ReplicaState.CLONE, 1L, 0), pcInfo));

        // replica is in tabletCommitInfos
        assertFalse(state.checkReplicaNeedSkip(tablet1, new Replica(2L, 10001L, ReplicaState.NORMAL, 99L, 0), pcInfo));
        assertFalse(state.checkReplicaNeedSkip(tablet1, new Replica(3L, 10002L, ReplicaState.NORMAL, 99L, 0), pcInfo));
        assertFalse(state.checkReplicaNeedSkip(tablet2, new Replica(4L, 10002L, ReplicaState.NORMAL, 99L, 0), pcInfo));

        // replica current version >= commit version
        assertFalse(state.checkReplicaNeedSkip(tablet0, new Replica(1L, 1L, ReplicaState.NORMAL, 100L, 0), pcInfo));

        // follower tabletCommitInfos is null
        Deencapsulation.setField(state, "tabletCommitInfos", null);
        assertFalse(state.checkReplicaNeedSkip(tablet0, new Replica(5L, 1L, ReplicaState.NORMAL, 1L, 0), pcInfo));

        // follower tabletCommitInfos is null and unknownReplicas contains the replica
        state.addUnknownReplica(5L);
        assertTrue(state.checkReplicaNeedSkip(tablet0, new Replica(5L, 1L, ReplicaState.NORMAL, 1L, 0), pcInfo));
    }

    @Test
    public void testTimeout() {
        {
            TransactionState txn = new TransactionState(1000L, Lists.newArrayList(20000L),
                    3000, "label123", UUIDUtil.genTUniqueId(),
                    LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                    1000L);

            txn.setTransactionStatus(TransactionStatus.PREPARE);
            txn.setPrepareTime(1);
            assertEquals(1, txn.getPrepareTime());
            assertFalse(txn.isTimeout(500));
            assertTrue(txn.isTimeout(1500));

            txn.setTransactionStatus(TransactionStatus.PREPARED);
            txn.setPreparedTimeAndTimeout(2000, TransactionState.DEFAULT_PREPARED_TIMEOUT_MS);
            assertEquals(Config.prepared_transaction_default_timeout_second * 1000L, txn.getPreparedTimeoutMs());
            assertFalse(txn.isTimeout(2000 + Config.prepared_transaction_default_timeout_second * 1000L));
            assertTrue(txn.isTimeout(2000 + Config.prepared_transaction_default_timeout_second * 1000L + 10));

            txn.setTransactionStatus(TransactionStatus.COMMITTED);
            assertFalse(txn.isTimeout(4000));
        }

        {
            TransactionState txn = new TransactionState(1000L, Lists.newArrayList(20000L),
                    3000, "label123", UUIDUtil.genTUniqueId(),
                    LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                    1000L);

            txn.setTransactionStatus(TransactionStatus.PREPARE);
            txn.setPrepareTime(1);
            assertEquals(1, txn.getPrepareTime());
            assertFalse(txn.isTimeout(500));
            assertTrue(txn.isTimeout(1500));

            txn.setTransactionStatus(TransactionStatus.PREPARED);
            txn.setPreparedTimeAndTimeout(2000, 1000);
            assertEquals(1000, txn.getPreparedTimeoutMs());
            assertFalse(txn.isTimeout(2500));
            assertTrue(txn.isTimeout(3500));

            txn.setTransactionStatus(TransactionStatus.COMMITTED);
            assertFalse(txn.isTimeout(4000));
        }
    }

    @Test
    public void testAddLoadId() {
        TransactionState txn = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label_addLoadId", UUIDUtil.genTUniqueId(),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);

        TUniqueId id1 = UUIDUtil.genTUniqueId();
        TUniqueId id2 = UUIDUtil.genTUniqueId();

        txn.addLoadId(id1);
        txn.addLoadId(id2);

        List<TUniqueId> ids = txn.getLoadIds();
        Assertions.assertNotNull(ids);
        assertEquals(2, ids.size());
        assertEquals(id1, ids.get(0));
        assertEquals(id2, ids.get(1));
    }
}
