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
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.TxnFinishStatePB;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState.LoadJobSourceType;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class TransactionStateTest {

    private static String fileName = "./TransactionStateTest";

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testSerDe() {
        UUID uuid = UUID.randomUUID();
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);

        String json = GsonUtils.GSON.toJson(transactionState);
        TransactionState readTransactionState = GsonUtils.GSON.fromJson(json, TransactionState.class);
        Assert.assertEquals(transactionState.getCoordinator().ip, readTransactionState.getCoordinator().ip);
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
            Assert.assertEquals(txnFinishStatePB.normalReplicas.size(), txn2.normalReplicas.size());
            Assert.assertEquals(txnFinishStatePB.abnormalReplicasWithVersion.size(), txn2.abnormalReplicasWithVersion.size());
        }
    }

    @Test
    public void testSerDeTxnFinishStateJSON() throws IOException {
        for (int i = 1; i <= 100000; i *= 10) {
            TxnFinishState s1 = buildTxnFinishState(i);
            String json = GsonUtils.GSON.toJson(s1);
            // System.out.printf("json: %s\n", json);
            TxnFinishState s2 = GsonUtils.GSON.fromJson(json, TxnFinishState.class);
            Assert.assertEquals(s1.normalReplicas.size(), s2.normalReplicas.size());
            Assert.assertEquals(s1.abnormalReplicasWithVersion.size(), s2.abnormalReplicasWithVersion.size());
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
        UUID uuid = UUID.randomUUID();
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
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
        Assert.assertTrue(readTransactionState.isNewFinish());
    }

    @Test
    public void testIsRunning() {
        Set<TransactionStatus> nonRunningStatus = new HashSet<>();
        nonRunningStatus.add(TransactionStatus.UNKNOWN);
        nonRunningStatus.add(TransactionStatus.VISIBLE);
        nonRunningStatus.add(TransactionStatus.ABORTED);

        UUID uuid = UUID.randomUUID();
        for (TransactionStatus status : TransactionStatus.values()) {
            TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                    3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                    LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                    60 * 1000L);
            transactionState.setTransactionStatus(status);
            Assert.assertEquals(nonRunningStatus.contains(status), !transactionState.isRunning());
        }
    }

    @Test
    public void testCommitInfos() {
        UUID uuid = UUID.randomUUID();
        TransactionState transactionState = new TransactionState(1000L, Lists.newArrayList(20000L, 20001L),
                3000, "label123", new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()),
                LoadJobSourceType.BACKEND_STREAMING, new TxnCoordinator(TxnSourceType.BE, "127.0.0.1"), 50000L,
                60 * 1000L);
        Assert.assertTrue(transactionState.tabletCommitInfosContainsReplica(1001, 1001, ReplicaState.NORMAL));
        TabletCommitInfo info1 = new TabletCommitInfo(10001, 10001);
        TabletCommitInfo info2 = new TabletCommitInfo(10001, 10002);
        TabletCommitInfo info3 = new TabletCommitInfo(10002, 10002);
        List<TabletCommitInfo> infos = new ArrayList<>();
        infos.add(info1);
        infos.add(info2);
        infos.add(info3);
        transactionState.setTabletCommitInfos(infos);
        Assert.assertFalse(transactionState.tabletCommitInfosContainsReplica(1001, 1001, ReplicaState.NORMAL));
        Assert.assertTrue(transactionState.tabletCommitInfosContainsReplica(10001, 10001, ReplicaState.NORMAL));
        Assert.assertTrue(transactionState.tabletCommitInfosContainsReplica(10001, 10002, ReplicaState.NORMAL));
        Assert.assertTrue(transactionState.tabletCommitInfosContainsReplica(10002, 10002, ReplicaState.NORMAL));
        Assert.assertTrue(transactionState.tabletCommitInfosContainsReplica(1001, 1001, ReplicaState.ALTER));
        Assert.assertTrue(transactionState.tabletCommitInfosContainsReplica(1001, 1001, ReplicaState.SCHEMA_CHANGE));
        Assert.assertTrue(transactionState.tabletCommitInfosContainsReplica(1001, 1001, ReplicaState.CLONE));
    }
}
