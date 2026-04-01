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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/transaction/DatabaseTransactionMgrTest.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.StringUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.replication.ReplicationTxnCommitAttachment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class DatabaseTransactionMgrTest {

    private static FakeEditLog fakeEditLog;
    private static FakeGlobalStateMgr fakeGlobalStateMgr;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static GlobalStateMgr masterGlobalStateMgr;
    private static GlobalStateMgr slaveGlobalStateMgr;
    private static Map<String, Long> lableToTxnId;
    private static boolean origin_enable_metric_calculator_value;

    private static TransactionGraph transactionGraph = new TransactionGraph();

    private TransactionState.TxnCoordinator transactionSource =
            new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe");

    @BeforeEach
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, StarRocksException {
        Config.label_keep_max_second = 10;
        fakeEditLog = new FakeEditLog();
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();
        slaveGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();

        origin_enable_metric_calculator_value = Config.enable_metric_calculator;
        Config.enable_metric_calculator = false;
        MetricRepo.init();

        masterTransMgr = masterGlobalStateMgr.getGlobalTransactionMgr();

        slaveTransMgr = slaveGlobalStateMgr.getGlobalTransactionMgr();

        lableToTxnId = addTransactionToTransactionMgr();
    }

    @AfterEach
    public void tearDown() {
        Config.enable_metric_calculator = origin_enable_metric_calculator_value;
    }

    public void prepareCommittedTransaction() throws StarRocksException {
        long transactionId1 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable10,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, transTablets,
                Lists.newArrayList(), null);
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTxnState(transactionId1).getStatus());
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable10, transactionId1);

    }

    public Map<String, Long> addTransactionToTransactionMgr() throws StarRocksException {
        TransactionIdGenerator idGenerator = masterTransMgr.getTransactionIDGenerator();
        Assertions.assertEquals(idGenerator.peekNextTransactionId(), masterTransMgr.getMinActiveTxnId());
        Assertions.assertEquals(idGenerator.peekNextTransactionId(), masterTransMgr.getMinActiveCompactionTxnId());

        Map<String, Long> lableToTxnId = Maps.newHashMap();
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long transactionId1 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        Assertions.assertEquals(transactionId1, masterTransMgr.getMinActiveTxnId());
        Assertions.assertEquals(idGenerator.peekNextTransactionId(), masterTransMgr.getMinActiveCompactionTxnId());

        // commit a transaction
        List<TabletCommitInfo> transTablets = buildTabletCommitInfoList();
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, transTablets,
                Lists.newArrayList(), null);
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTxnState(transactionId1).getStatus());

        Assertions.assertEquals(transactionId1, masterTransMgr.getMinActiveTxnId());
        Assertions.assertEquals(idGenerator.peekNextTransactionId(), masterTransMgr.getMinActiveCompactionTxnId());

        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, null);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable1, transactionId1);

        Assertions.assertEquals(idGenerator.peekNextTransactionId(), masterTransMgr.getMinActiveTxnId());
        Assertions.assertEquals(idGenerator.peekNextTransactionId(), masterTransMgr.getMinActiveCompactionTxnId());

        TransactionState.TxnCoordinator beTransactionSource =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "be1");
        TransactionState.TxnCoordinator feTransactionSource =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "fe1");
        long transactionId2 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable2,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK,
                        Config.stream_load_default_timeout_second);
        long transactionId3 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable3,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.stream_load_default_timeout_second);
        long transactionId4 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable4,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.stream_load_default_timeout_second);
        long transactionId5 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable5,
                        feTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.max_load_timeout_second);
        // for test batch
        long transactionId6 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable6,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.stream_load_default_timeout_second);
        long transactionId7 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable7,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.stream_load_default_timeout_second);
        long transactionId8 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable8,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.stream_load_default_timeout_second);


        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId6, transTablets,
                Lists.newArrayList(), null);
        assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTxnState(transactionId6).getStatus());
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId7, transTablets,
                Lists.newArrayList(), null);
        assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTxnState(transactionId7).getStatus());
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId8, transTablets,
                Lists.newArrayList(), null);
        assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTxnState(transactionId8).getStatus());

        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable2, transactionId2);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable3, transactionId3);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable4, transactionId4);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable5, transactionId5);

        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable6, transactionId6);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable7, transactionId7);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable8, transactionId8);

        Assertions.assertEquals(transactionId2, masterTransMgr.getMinActiveTxnId());

        transactionGraph.add(transactionId6, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));
        transactionGraph.add(transactionId7, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));
        transactionGraph.add(transactionId8, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));

        Deencapsulation.setField(masterDbTransMgr, "transactionGraph", transactionGraph);

        TransactionState transactionState1 = fakeEditLog.getTransaction(transactionId1);

        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState1);
        return lableToTxnId;
    }

    private List<TabletCommitInfo> buildTabletCommitInfoList() {
        return buildTabletCommitInfoList(GlobalStateMgrTestUtil.testTabletId1);
    }

    private List<TabletCommitInfo> buildTabletCommitInfoList(long tabletId) {
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(tabletId,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(tabletId,
                GlobalStateMgrTestUtil.testBackendId2);
        TabletCommitInfo tabletCommitInfo3 = new TabletCommitInfo(tabletId,
                GlobalStateMgrTestUtil.testBackendId3);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        transTablets.add(tabletCommitInfo3);
        return transTablets;
    }

    @Test
    public void getLakeCompactionActiveTxnListTest() throws StarRocksException {
        TransactionState.TxnCoordinator feTransactionSource =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "fe1");
        long committedCompactionTransactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLableCompaction1,
                        feTransactionSource,
                        TransactionState.LoadJobSourceType.LAKE_COMPACTION,
                        Config.lake_compaction_default_timeout_second);

        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<TabletCommitInfo> transTablets = buildTabletCommitInfoList();
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, committedCompactionTransactionId, transTablets,
                Lists.newArrayList(), null);
        assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTxnState(committedCompactionTransactionId).getStatus());

        long preparedCompactionTransactionId = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLableCompaction2,
                        feTransactionSource,
                        TransactionState.LoadJobSourceType.LAKE_COMPACTION,
                        Config.lake_compaction_default_timeout_second);

        Map<Long, Long> compactionActiveTxnMap = masterDbTransMgr.getLakeCompactionActiveTxnMap();
        Assertions.assertEquals(2, compactionActiveTxnMap.size());
        Assertions.assertTrue(compactionActiveTxnMap.containsKey(committedCompactionTransactionId));
        Assertions.assertTrue(compactionActiveTxnMap.containsKey(preparedCompactionTransactionId));

        // global transaction stats check
        Map<Long, Long> globalCompactionActiveTxnMap = masterTransMgr.getLakeCompactionActiveTxnStats();
        Assertions.assertEquals(2, globalCompactionActiveTxnMap.size());
        Assertions.assertTrue(globalCompactionActiveTxnMap.containsKey(committedCompactionTransactionId));
        Assertions.assertTrue(globalCompactionActiveTxnMap.containsKey(preparedCompactionTransactionId));
    }

    @Test
    public void testLakeCompactionTxnRemovesStartupMapOnVisible() throws StarRocksException {
        // Use the real GlobalStateMgr in tests so that GlobalStateMgr.getCurrentState() works as expected.
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        CompactionMgr compactionMgr = masterGlobalStateMgr.getCompactionMgr();
        @SuppressWarnings("unchecked")
        Map<Long, Long> startupActiveMap =
                Deencapsulation.getField(compactionMgr, "remainedActiveCompactionTxnWhenStart");
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        TransactionState.TxnCoordinator feTransactionSource =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "fe1");
        long tableId = GlobalStateMgrTestUtil.testTableId1;
        long txnId = 123456L;
        TransactionState txnState = new TransactionState(
                GlobalStateMgrTestUtil.testDbId1,
                Lists.newArrayList(tableId),
                txnId,
                "test_lake_compaction_txn_single",
                null,
                TransactionState.LoadJobSourceType.LAKE_COMPACTION,
                feTransactionSource,
                -1L,
                Config.lake_compaction_default_timeout_second * 1000L);

        // Register txn as running in db transaction manager.
        Deencapsulation.invoke(masterDbTransMgr, "unprotectUpsertTransactionState", txnState);

        // Simulate that this txn was active when FE restarted.
        startupActiveMap.put(txnId, tableId);
        Assertions.assertTrue(startupActiveMap.containsKey(txnId));

        // Simulate the txn becoming VISIBLE and being replayed/applied.
        txnState.setTransactionStatus(TransactionStatus.VISIBLE);
        Deencapsulation.invoke(masterDbTransMgr, "unprotectUpsertTransactionState", txnState);

        // The startup active compaction map should be cleaned for this lake compaction txn.
        Assertions.assertFalse(startupActiveMap.containsKey(txnId));
    }

    @Test
    public void testNormal() throws StarRocksException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        assertEquals(8, masterDbTransMgr.getTransactionNum());
        assertEquals(6, masterDbTransMgr.getRunningTxnNums());
        assertEquals(1, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(1, masterDbTransMgr.getFinishedTxnNums());
        DatabaseTransactionMgr slaveDbTransMgr =
                slaveTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        assertEquals(1, slaveDbTransMgr.getTransactionNum());
        assertEquals(1, slaveDbTransMgr.getFinishedTxnNums());

        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1).size());
        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable2).size());
        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable3).size());
        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable4).size());

        Long txnId1 =
                masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1).iterator().next();
        assertEquals(txnId1, lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1));
        TransactionState transactionState1 =
                masterDbTransMgr.getTransactionState(lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1));
        assertEquals(txnId1.longValue(), transactionState1.getTransactionId());
        assertEquals(TransactionStatus.VISIBLE, transactionState1.getTransactionStatus());
        assertEquals(TransactionStatus.VISIBLE, masterDbTransMgr.getTxnState(txnId1.longValue()).getStatus());

        Long txnId2 =
                masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable2).iterator().next();
        assertEquals(txnId2, lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable2));
        TransactionState transactionState2 = masterDbTransMgr.getTransactionState(txnId2);
        assertEquals(txnId2.longValue(), transactionState2.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState2.getTransactionStatus());
        assertEquals(TransactionStatus.PREPARE, masterDbTransMgr.getTxnState(txnId2.longValue()).getStatus());

        assertEquals(TransactionStatus.UNKNOWN, masterDbTransMgr.getTxnState(12134).getStatus());
    }

    @Test
    public void testAbortTransactionWithAttachment() throws StarRocksException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId1 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        Throwable exception = assertThrows(StarRocksException.class, () -> {
            TxnCommitAttachment txnCommitAttachment = new RLTaskTxnCommitAttachment();
            masterDbTransMgr.abortTransaction(txnId1, "test abort transaction", txnCommitAttachment);
        });
        assertThat(exception.getMessage(), containsString("transaction not found"));
    }

    @Test
    public void testAbortTransaction() throws StarRocksException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        long txnId2 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable2);
        masterDbTransMgr.abortTransaction(txnId2, "test abort transaction", null);
        assertEquals(6, masterDbTransMgr.getRunningTxnNums());
        assertEquals(0, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(2, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(8, masterDbTransMgr.getTransactionNum());
        assertEquals(TransactionStatus.ABORTED, masterDbTransMgr.getTxnState(txnId2).getStatus());

        long txnId3 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable3);
        masterDbTransMgr.abortTransaction(txnId3, "test abort transaction", null);
        assertEquals(5, masterDbTransMgr.getRunningTxnNums());
        assertEquals(0, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(3, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(8, masterDbTransMgr.getTransactionNum());
        assertEquals(TransactionStatus.ABORTED, masterDbTransMgr.getTxnState(txnId3).getStatus());
    }

    @Test
    public void testFinishTransactionTableRemove() throws StarRocksException {
        prepareCommittedTransaction();
        new MockUp<Database>() {
            @Mock
            public Table getTable(long tableId) {
                return null;
            }
        };

        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable10);
        masterDbTransMgr.finishTransaction(txnId, null, 0);
        assertEquals(TransactionStatus.VISIBLE, masterDbTransMgr.getTxnState(txnId).getStatus());
    }


    @Test
    public void testFinishTransactionPartitionRemove() throws StarRocksException {
        prepareCommittedTransaction();
        new MockUp<OlapTable>() {
            @Mock
            public PhysicalPartition getPhysicalPartition(long partitionId) {
                return null;
            }
        };

        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable10);
        masterDbTransMgr.finishTransaction(txnId, null, 0);
        assertEquals(TransactionStatus.VISIBLE, masterDbTransMgr.getTxnState(txnId).getStatus());
    }
    @Test
    public void testAbortTransactionWithNotFoundException() throws StarRocksException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        long txnId1 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        Throwable exception = assertThrows(StarRocksException.class,
                () -> masterDbTransMgr.abortTransaction(txnId1, "test abort transaction", null));
        assertThat(exception.getMessage(), containsString("transaction not found"));
    }

    @Test
    public void testGetTransactionIdByCoordinateBe() throws StarRocksException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<Pair<Long, Long>> transactionInfoList = masterDbTransMgr.getTransactionIdByCoordinateBe("be1", 10);
        assertEquals(6, transactionInfoList.size());
        assertEquals(GlobalStateMgrTestUtil.testDbId1, transactionInfoList.get(0).first.longValue());
        assertEquals(TransactionStatus.PREPARE,
                masterDbTransMgr.getTransactionState(transactionInfoList.get(0).second).getTransactionStatus());
    }

    @Test
    public void testGetSingleTranInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        List<List<String>> singleTranInfos =
                masterDbTransMgr.getSingleTranInfo(GlobalStateMgrTestUtil.testDbId1, txnId);
        assertEquals(1, singleTranInfos.size());
        List<String> txnInfo = singleTranInfos.get(0);
        assertEquals("1000", txnInfo.get(0));
        assertEquals(GlobalStateMgrTestUtil.testTxnLable1, txnInfo.get(1));
        assertEquals("FE: localfe", txnInfo.get(2));
        assertEquals("VISIBLE", txnInfo.get(3));
        assertEquals("FRONTEND", txnInfo.get(4));
        long currentTime = System.currentTimeMillis();
        assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(5)));
        assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(6)));
        assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(7)));
        assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(8)));
        assertTrue(currentTime > TimeUtils.timeStringToLong(txnInfo.get(9)));
        assertEquals("", txnInfo.get(10));
        assertEquals("0", txnInfo.get(11));
        assertEquals("[-1]", txnInfo.get(12));
        assertEquals(String.valueOf(Config.stream_load_default_timeout_second * 1000L), txnInfo.get(13));
        assertEquals(String.valueOf(Config.prepared_transaction_default_timeout_second * 1000L), txnInfo.get(14));
    }

    @Test
    public void testRemoveExpiredTxns() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        Config.label_keep_max_second = -1;
        long currentMillis = System.currentTimeMillis();
        masterDbTransMgr.removeExpiredTxns(currentMillis);
        assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(7, masterDbTransMgr.getTransactionNum());
        assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1));
    }

    @Test
    public void testGetTableTransInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        Long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        List<List<Comparable>> tableTransInfos = masterDbTransMgr.getTableTransInfo(txnId);
        assertEquals(1, tableTransInfos.size());
        List<Comparable> tableTransInfo = tableTransInfos.get(0);
        assertEquals(2, tableTransInfo.size());
        assertEquals(2L, tableTransInfo.get(0));
        assertEquals("103", tableTransInfo.get(1));
    }

    @Test
    public void testGetPartitionTransInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        Long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        List<List<Comparable>> partitionTransInfos =
                masterDbTransMgr.getPartitionTransInfo(txnId, GlobalStateMgrTestUtil.testTableId1);
        assertEquals(1, partitionTransInfos.size());
        List<Comparable> partitionTransInfo = partitionTransInfos.get(0);
        assertEquals(2, partitionTransInfo.size());
        assertEquals(103L, partitionTransInfo.get(0));
        assertEquals(13L, partitionTransInfo.get(1));
    }

    @Test
    public void testDeleteTransaction() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        TransactionState transactionState = masterDbTransMgr.getTransactionState(txnId);
        masterDbTransMgr.deleteTransaction(transactionState);
        assertEquals(6, masterDbTransMgr.getRunningTxnNums());
        assertEquals(1, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(7, masterDbTransMgr.getTransactionNum());
        assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1));
    }

    @Test
    public void testCheckRunningTxnExceedLimit() {
        int maxRunningTxnNumPerDb = Config.max_running_txn_num_per_db;
        DatabaseTransactionMgr mgr = new DatabaseTransactionMgr(0, masterGlobalStateMgr);
        Deencapsulation.setField(mgr, "runningTxnNums", maxRunningTxnNumPerDb);
        ExceptionChecker.expectThrowsNoException(
                () -> mgr.checkRunningTxnExceedLimit(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK));
        ExceptionChecker.expectThrowsNoException(
                () -> mgr.checkRunningTxnExceedLimit(TransactionState.LoadJobSourceType.LAKE_COMPACTION));
        ExceptionChecker.expectThrows(RunningTxnExceedException.class,
                () -> mgr.checkRunningTxnExceedLimit(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
    }

    @Test
    public void testGetReadyToPublishTxnListBatch() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<TransactionStateBatch> stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(1, stateBatchesList.size());
        assertEquals(3, stateBatchesList.get(0).size());

        // Table was dropped before committing the txn 'testTxnLabel8'
        masterDbTransMgr.getLabelTransactionState(GlobalStateMgrTestUtil.testTxnLable8)
                .removeTable(GlobalStateMgrTestUtil.testTableId1);
        stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(1, stateBatchesList.size());
        assertEquals(2, stateBatchesList.get(0).size());

        // Table was dropped before committing the txn 'testTxnLabel7'
        masterDbTransMgr.getLabelTransactionState(GlobalStateMgrTestUtil.testTxnLable7)
                .removeTable(GlobalStateMgrTestUtil.testTableId1);
        stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(1, stateBatchesList.size());
        assertEquals(1, stateBatchesList.get(0).size());

        // Table was dropped before committing the txn 'testTxnLabel6'
        masterDbTransMgr.getLabelTransactionState(GlobalStateMgrTestUtil.testTxnLable6)
                .removeTable(GlobalStateMgrTestUtil.testTableId1);
        stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(1, stateBatchesList.size());
        assertEquals(1, stateBatchesList.get(0).size());
    }

    @Test
    public void testGetReadyToPublishTxnListBatchWithReplicationTxn() throws StarRocksException {

        // begin transaction
        long replicationTransactionId1 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLableReplication1,
                        transactionSource,
                        TransactionState.LoadJobSourceType.REPLICATION, Config.replication_transaction_timeout_sec);

        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);

        Map<Long, Long> partitionVersions = new HashMap<>();
        Table table = masterGlobalStateMgr.getLocalMetastore().getTable(GlobalStateMgrTestUtil.testDbId1,
                GlobalStateMgrTestUtil.testTableId1);
        for (Partition partition : table.getPartitions()) {
            partitionVersions.put(partition.getDefaultPhysicalPartition().getId(),
                    partition.getDefaultPhysicalPartition().getVisibleVersion() + 2);
        }

        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, replicationTransactionId1, transTablets,
                Lists.newArrayList(), new ReplicationTxnCommitAttachment(partitionVersions, partitionVersions));

        // transactionGraph have 4 transactions total, and the last one is a replication transaction
        transactionGraph.add(replicationTransactionId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));

        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<TransactionStateBatch> stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(4, masterDbTransMgr.getCommittedTxnList().size());
        assertEquals(1, stateBatchesList.size());
        assertEquals(3, stateBatchesList.get(0).size());

        // Let's finish the first batch transactions
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long txnId6 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable6);
        TransactionState transactionState6 = masterDbTransMgr.getTransactionState(txnId6);
        long txnId7 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable7);
        TransactionState transactionState7 = masterDbTransMgr.getTransactionState(txnId7);
        long txnId8 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable8);
        TransactionState transactionState8 = masterDbTransMgr.getTransactionState(txnId8);
        List<TransactionState> states = new ArrayList<>();
        states.add(transactionState6);
        states.add(transactionState7);
        states.add(transactionState8);

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        TransactionStateBatch stateBatch = new TransactionStateBatch(states);
        masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null);

        // after the first batch transactions are finished, the transactionGraph has only on transaction state
        stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(1, masterDbTransMgr.getCommittedTxnList().size());
        assertEquals(TransactionType.TXN_REPLICATION, masterDbTransMgr.getCommittedTxnList().get(0).getTransactionType());
        assertEquals(1, stateBatchesList.size());
        assertEquals(1, stateBatchesList.get(0).size());

        // Add one more normal transaction after the replication transaction.
        // The batching logic should process the replication transaction in its own batch first,
        // and then the new normal transaction will be in a separate batch.
        long transactionId9 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable9,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId9, transTablets,
                Lists.newArrayList(), null);
        stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(2, masterDbTransMgr.getCommittedTxnList().size());
        assertEquals(1, stateBatchesList.size());
        assertEquals(1, stateBatchesList.get(0).size());
    }

    @Test
    public void testGetReadyToPublishTxnListBatchWithDeleteTxn() throws StarRocksException {
        // Test case: DELETE transactions should not be batched with normal transactions.
        // Setup: Create transactions in order: normal -> DELETE -> normal -> normal
        // Expected batches:
        // Batch 1: [txn_normal_0] (cut off before DELETE)
        // Batch 2: [txn_delete] (separate due to DELETE source type)
        // Batch 3: [txn_normal_1, txn_normal_2] (can be batched together)

        // First, finish the existing committed transactions to start fresh
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId6 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable6);
        TransactionState transactionState6 = masterDbTransMgr.getTransactionState(txnId6);
        long txnId7 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable7);
        TransactionState transactionState7 = masterDbTransMgr.getTransactionState(txnId7);
        long txnId8 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable8);
        TransactionState transactionState8 = masterDbTransMgr.getTransactionState(txnId8);
        List<TransactionState> states = new ArrayList<>();
        states.add(transactionState6);
        states.add(transactionState7);
        states.add(transactionState8);

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        TransactionStateBatch stateBatch = new TransactionStateBatch(states);
        masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null);

        // Create a new transaction graph for the test
        TransactionGraph newTransactionGraph = new TransactionGraph();
        Deencapsulation.setField(masterDbTransMgr, "transactionGraph", newTransactionGraph);

        // Begin and commit a normal transaction
        List<TabletCommitInfo> transTablets = buildTabletCommitInfoList();
        long normalTxn1 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        "testDeleteBatch_normal1",
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, normalTxn1, transTablets,
                Lists.newArrayList(), null);
        newTransactionGraph.add(normalTxn1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));

        // Begin and commit a DELETE transaction
        long deleteTxn = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        "testDeleteBatch_delete",
                        transactionSource,
                        TransactionState.LoadJobSourceType.DELETE, Config.stream_load_default_timeout_second);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, deleteTxn, transTablets,
                Lists.newArrayList(), null);
        newTransactionGraph.add(deleteTxn, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));

        // Begin and commit two more normal transactions
        long normalTxn2 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        "testDeleteBatch_normal2",
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, normalTxn2, transTablets,
                Lists.newArrayList(), null);
        newTransactionGraph.add(normalTxn2, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));

        long normalTxn3 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        "testDeleteBatch_normal3",
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, normalTxn3, transTablets,
                Lists.newArrayList(), null);
        newTransactionGraph.add(normalTxn3, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1));

        // Verify committed transactions
        assertEquals(4, masterDbTransMgr.getCommittedTxnList().size());

        // Get batches and verify DELETE transaction is not batched with normal transactions
        List<TransactionStateBatch> stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();

        // First batch should be the first normal transaction only (cut off before DELETE)
        assertEquals(1, stateBatchesList.size());
        assertEquals(1, stateBatchesList.get(0).size());
        assertEquals(normalTxn1, stateBatchesList.get(0).getTransactionStates().get(0).getTransactionId());

        // Finish the first batch
        masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatchesList.get(0), null);

        // Get next batches - should be DELETE transaction alone
        stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(1, stateBatchesList.size());
        assertEquals(1, stateBatchesList.get(0).size());
        assertEquals(deleteTxn, stateBatchesList.get(0).getTransactionStates().get(0).getTransactionId());
        assertEquals(TransactionState.LoadJobSourceType.DELETE,
                stateBatchesList.get(0).getTransactionStates().get(0).getSourceType());

        // Finish the DELETE batch
        masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatchesList.get(0), null);

        // Get next batches - should be the remaining normal transactions batched together
        stateBatchesList = masterDbTransMgr.getReadyToPublishTxnListBatch();
        assertEquals(1, stateBatchesList.size());
        // The two remaining normal transactions should be batched together
        assertEquals(2, stateBatchesList.get(0).size());
        assertEquals(normalTxn2, stateBatchesList.get(0).getTransactionStates().get(0).getTransactionId());
        assertEquals(normalTxn3, stateBatchesList.get(0).getTransactionStates().get(1).getTransactionId());
    }

    @Test
    public void testFinishTransactionBatch() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId6 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable6);
        TransactionState transactionState6 = masterDbTransMgr.getTransactionState(txnId6);
        long txnId7 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable7);
        TransactionState transactionState7 = masterDbTransMgr.getTransactionState(txnId7);
        long txnId8 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable8);
        TransactionState transactionState8 = masterDbTransMgr.getTransactionState(txnId8);
        List<TransactionState> states = new ArrayList<>();
        states.add(transactionState6);
        states.add(transactionState7);
        states.add(transactionState8);

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        TransactionStateBatch stateBatch = new TransactionStateBatch(states);
        masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null);

        assertEquals(3, masterDbTransMgr.getRunningTxnNums());
        assertEquals(4, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(TransactionStatus.VISIBLE, masterDbTransMgr.getTransactionState(txnId6).getTransactionStatus());
        assertEquals(TransactionStatus.VISIBLE, masterDbTransMgr.getTransactionState(txnId7).getTransactionStatus());
        assertEquals(TransactionStatus.VISIBLE, masterDbTransMgr.getTransactionState(txnId8).getTransactionStatus());

        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        TransactionStateBatch replayStateBatch = new TransactionStateBatch(List.of(
                fakeEditLog.getTransaction(txnId6),
                fakeEditLog.getTransaction(txnId7),
                fakeEditLog.getTransaction(txnId8)));
        slaveTransMgr.replayUpsertTransactionStateBatch(replayStateBatch);
        assertEquals(4, masterDbTransMgr.getFinishedTxnNums());
    }

    @Test
    public void testFinishTransactionBatchReturnsLatestStateBatch() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId6 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable6);
        long txnId7 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable7);
        long txnId8 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable8);
        TransactionState transactionState6 = masterDbTransMgr.getTransactionState(txnId6);
        TransactionState transactionState7 = masterDbTransMgr.getTransactionState(txnId7);
        TransactionState transactionState8 = masterDbTransMgr.getTransactionState(txnId8);

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        TransactionStateBatch stateBatch = new TransactionStateBatch(
                List.of(transactionState6, transactionState7, transactionState8));
        Map<ComputeNode, List<Long>> nodeToTablets = Maps.newHashMap();
        nodeToTablets.put(new ComputeNode(10001L, "host", 9050), Lists.newArrayList(10002L));
        stateBatch.putBeTablets(10003L, nodeToTablets);

        TransactionStateBatch latestStateBatch =
                masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null);

        Assertions.assertNotSame(stateBatch, latestStateBatch);
        assertEquals(TransactionStatus.COMMITTED, stateBatch.getTransactionStates().get(0).getTransactionStatus());
        assertEquals(TransactionStatus.VISIBLE, latestStateBatch.getTransactionStates().get(0).getTransactionStatus());
        Assertions.assertSame(stateBatch.getPartitionToTablets(), latestStateBatch.getPartitionToTablets());
        Assertions.assertSame(masterDbTransMgr.getTransactionState(txnId6), latestStateBatch.getTransactionStates().get(0));
        Assertions.assertSame(masterDbTransMgr.getTransactionState(txnId7), latestStateBatch.getTransactionStates().get(1));
        Assertions.assertSame(masterDbTransMgr.getTransactionState(txnId8), latestStateBatch.getTransactionStates().get(2));
    }

    @Test
    public void testFinishTransactionBatchEditLogException() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId6 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable6);
        long txnId7 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable7);
        long txnId8 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable8);
        TransactionStateBatch stateBatch = new TransactionStateBatch(List.of(
                masterDbTransMgr.getTransactionState(txnId6),
                masterDbTransMgr.getTransactionState(txnId7),
                masterDbTransMgr.getTransactionState(txnId8)));

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        EditLog spyEditLog = spy(masterGlobalStateMgr.getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logInsertTransactionStateBatch(any(TransactionStateBatch.class), any());
        EditLog originalEditLog = replaceDatabaseTransactionMgrEditLog(masterDbTransMgr, spyEditLog);
        try {
            RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                    () -> masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null));
            assertEditLogWriteFailed(exception);
            assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTransactionState(txnId6).getTransactionStatus());
            assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTransactionState(txnId7).getTransactionStatus());
            assertEquals(TransactionStatus.COMMITTED, masterDbTransMgr.getTransactionState(txnId8).getTransactionStatus());
        } finally {
            replaceDatabaseTransactionMgrEditLog(masterDbTransMgr, originalEditLog);
        }
    }

    @Test
    public void testCommitPreparedTransactionCommitTimeIsStrictlyIncreasing() throws Exception {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        Database db = masterGlobalStateMgr.getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDbId1);
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long maxCommitTsBefore = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
        Deencapsulation.setField(masterDbTransMgr, "maxCommitTs", maxCommitTsBefore);
        long secondTableId = 20001L;
        long secondPartitionId = 20002L;
        long secondIndexId = secondTableId;
        long secondTabletId = 20003L;
        createSimpleOlapTable(db, secondTableId, "testTableCommitTime2", secondPartitionId,
                "testPartitionCommitTime2", secondIndexId, "testIndexCommitTime2", secondTabletId,
                GlobalStateMgrTestUtil.testStartVersion, 20004L, 20005L, 20006L);

        long transactionId1 = masterTransMgr.beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                "test_commit_prepared_commit_time_1",
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);
        long transactionId2 = masterTransMgr.beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                Lists.newArrayList(secondTableId),
                "test_commit_prepared_commit_time_2",
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        masterTransMgr.prepareTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, -1,
                buildTabletCommitInfoList(), Lists.newArrayList(), null);
        masterTransMgr.prepareTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId2, -1,
                buildTabletCommitInfoList(secondTabletId), Lists.newArrayList(), null);

        CountDownLatch firstPersistEntered = new CountDownLatch(1);
        CountDownLatch releasePersist = new CountDownLatch(1);
        CountDownLatch secondPersistEntered = new CountDownLatch(1);
        AtomicInteger persistCallCount = new AtomicInteger(0);
        try {
            new MockUp<EditLog>() {
                @Mock
                public void logInsertTransactionState(TransactionState transactionState, WALApplier walApplier)
                        throws InterruptedException {
                    if (persistCallCount.incrementAndGet() == 1) {
                        firstPersistEntered.countDown();
                        assertTrue(releasePersist.await(10, TimeUnit.SECONDS));
                    } else {
                        secondPersistEntered.countDown();
                    }
                    if (walApplier != null) {
                        walApplier.apply(transactionState);
                    }
                }
            };

            Throwable[] errors = new Throwable[2];
            Thread thread1 = new Thread(() -> {
                try {
                    masterTransMgr.commitPreparedTransaction(db, transactionId1, 1000L);
                } catch (Throwable t) {
                    errors[0] = t;
                }
            });
            Thread thread2 = new Thread(() -> {
                try {
                    masterTransMgr.commitPreparedTransaction(db, transactionId2, 1000L);
                } catch (Throwable t) {
                    errors[1] = t;
                }
            });

            thread1.start();
            assertTrue(firstPersistEntered.await(10, TimeUnit.SECONDS));
            thread2.start();
            assertTrue(secondPersistEntered.await(10, TimeUnit.SECONDS));

            thread2.join(5000);
            releasePersist.countDown();
            thread1.join(5000);

            assertPublishTimeoutOrNull(errors[0]);
            assertPublishTimeoutOrNull(errors[1]);

            TransactionState transactionState1 = masterDbTransMgr.getTransactionState(transactionId1);
            TransactionState transactionState2 = masterDbTransMgr.getTransactionState(transactionId2);
            assertEquals(TransactionStatus.COMMITTED, transactionState1.getTransactionStatus());
            assertEquals(TransactionStatus.COMMITTED, transactionState2.getTransactionStatus());
            assertEquals(2, Sets.newHashSet(transactionState1.getCommitTime(), transactionState2.getCommitTime()).size());
            assertEquals(1, Math.abs(transactionState1.getCommitTime() - transactionState2.getCommitTime()));
            assertTrue(transactionState1.getCommitTime() > maxCommitTsBefore);
            assertTrue(transactionState2.getCommitTime() > maxCommitTsBefore);
        } finally {
            releasePersist.countDown();
        }
    }

    @Test
    public void testPublishVersionMissing() throws StarRocksException {
        TransactionIdGenerator idGenerator = masterTransMgr.getTransactionIDGenerator();
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        // begin transaction
        long transactionId1 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                        Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable9,
                        transactionSource,
                        TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        // commit a transaction
        TabletCommitInfo tabletCommitInfo1 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId1);
        TabletCommitInfo tabletCommitInfo2 = new TabletCommitInfo(GlobalStateMgrTestUtil.testTabletId1,
                GlobalStateMgrTestUtil.testBackendId2);
        // skip replica 3
        List<TabletCommitInfo> transTablets = Lists.newArrayList();
        transTablets.add(tabletCommitInfo1);
        transTablets.add(tabletCommitInfo2);
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, transTablets,
                Lists.newArrayList(), null);
        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, null);
    }

    @Test
    public void testFinishTransactionWithLockTimeout() throws Exception {
        String testTxnLabel = StringUtils.generateRandomString(10);

        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        while (true) { // clean up previous committed transactions
            List<TransactionState> txnList = masterDbTransMgr.getCommittedTxnList();
            if (txnList.isEmpty()) {
                break;
            }
            for (TransactionState state : txnList) {
                if (masterTransMgr.canTxnFinished(state, Sets.newHashSet(), null)) {
                    masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, state.getTransactionId(), null);
                }
            }
        }
        long transactionId = masterTransMgr.beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                testTxnLabel,
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        List<TabletCommitInfo> transTablets = buildTabletCommitInfoList();
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), null);

        TransactionState txnState = masterDbTransMgr.getTransactionState(transactionId);
        assertEquals(TransactionStatus.COMMITTED, txnState.getTransactionStatus());
        Assertions.assertTrue(masterTransMgr.canTxnFinished(txnState, Sets.newHashSet(), null));

        CountDownLatch latchLock = new CountDownLatch(1);
        CountDownLatch latchUnlock = new CountDownLatch(1);
        Thread lockThread = new Thread(() -> {
            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(GlobalStateMgrTestUtil.testDbId1,
                    GlobalStateMgrTestUtil.testTableId1, LockType.WRITE);
            latchLock.countDown();
            try {
                latchUnlock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
            locker.unLockTableWithIntensiveDbLock(GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1,
                    LockType.WRITE);
        });
        lockThread.start();

        latchLock.await();
        // now the locker is held by the lockThread
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, null, 1000L));
        Assertions.assertEquals(ErrorCode.ERR_LOCK_ERROR, exception.getErrorCode());
        latchUnlock.countDown();

        // unlock the lock in lockThread
        Assertions.assertDoesNotThrow(
                () -> masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, null, 1000L));
        assertEquals(TransactionStatus.VISIBLE,
                masterDbTransMgr.getTransactionState(transactionId).getTransactionStatus());
        lockThread.join();
    }

    @Test
    public void testCanTxnFinishedWithLockTimeoutNoContention() throws Exception {
        String testTxnLabel = StringUtils.generateRandomString(10);

        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        while (true) { // clean up previous committed transactions
            List<TransactionState> txnList = masterDbTransMgr.getCommittedTxnList();
            if (txnList.isEmpty()) {
                break;
            }
            for (TransactionState state : txnList) {
                if (masterTransMgr.canTxnFinished(state, Sets.newHashSet(), null)) {
                    masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, state.getTransactionId(), null);
                }
            }
        }

        long transactionId = masterTransMgr.beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                testTxnLabel,
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        List<TabletCommitInfo> transTablets = buildTabletCommitInfoList();
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), null);

        TransactionState txnState = masterDbTransMgr.getTransactionState(transactionId);
        assertEquals(TransactionStatus.COMMITTED, txnState.getTransactionStatus());

        // Should succeed with a generous timeout when there is no lock contention
        Assertions.assertDoesNotThrow(() -> {
            boolean result = masterTransMgr.canTxnFinished(txnState, Sets.newHashSet(), null,
                    Config.finish_transaction_default_lock_timeout_ms);
            Assertions.assertTrue(result);
        });
    }

    @Test
    public void testCanTxnFinishedWithLockTimeout() throws Exception {
        String testTxnLabel = StringUtils.generateRandomString(10);

        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        while (true) { // clean up previous committed transactions
            List<TransactionState> txnList = masterDbTransMgr.getCommittedTxnList();
            if (txnList.isEmpty()) {
                break;
            }
            for (TransactionState state : txnList) {
                if (masterTransMgr.canTxnFinished(state, Sets.newHashSet(), null)) {
                    masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, state.getTransactionId(), null);
                }
            }
        }

        long transactionId = masterTransMgr.beginTransaction(GlobalStateMgrTestUtil.testDbId1,
                Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                testTxnLabel,
                transactionSource,
                TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

        List<TabletCommitInfo> transTablets = buildTabletCommitInfoList();
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId, transTablets,
                Lists.newArrayList(), null);

        TransactionState txnState = masterDbTransMgr.getTransactionState(transactionId);
        assertEquals(TransactionStatus.COMMITTED, txnState.getTransactionStatus());

        // Hold a conflicting WRITE lock in a background thread to trigger the read-lock timeout
        CountDownLatch latchLock = new CountDownLatch(1);
        CountDownLatch latchUnlock = new CountDownLatch(1);
        Thread lockThread = new Thread(() -> {
            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(GlobalStateMgrTestUtil.testDbId1,
                    GlobalStateMgrTestUtil.testTableId1, LockType.WRITE);
            latchLock.countDown();
            try {
                latchUnlock.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
            locker.unLockTableWithIntensiveDbLock(GlobalStateMgrTestUtil.testDbId1,
                    GlobalStateMgrTestUtil.testTableId1, LockType.WRITE);
        });
        lockThread.start();

        latchLock.await();
        // WRITE lock is held — canTxnFinished should time out and throw ERR_LOCK_ERROR
        LockTimeoutException exception = Assertions.assertThrows(LockTimeoutException.class,
                () -> masterTransMgr.canTxnFinished(txnState, Sets.newHashSet(), null,
                        Config.finish_transaction_default_lock_timeout_ms));

        latchUnlock.countDown();
        lockThread.join();

        // After the WRITE lock is released, canTxnFinished should succeed
        Assertions.assertDoesNotThrow(() -> {
            boolean result = masterTransMgr.canTxnFinished(txnState, Sets.newHashSet(), null,
                    Config.finish_transaction_default_lock_timeout_ms);
            Assertions.assertTrue(result);
        });
    }

    @Test
    public void testFinishTransactionBatchPersistVisibleStateBeforeAfterVisible() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<TransactionState> states = getDefaultBatchTxnStates(masterDbTransMgr);

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        // With COW, the original stateBatch is copied internally, so we verify via callbacks
        // that the transaction is persisted before afterVisible is called.
        AtomicBoolean persistedWhenAfterVisible = new AtomicBoolean(false);
        long checkCallbackId = 99999L;
        states.get(0).addCallbackId(checkCallbackId);
        masterTransMgr.getCallbackFactory().addCallback(new AbstractTxnStateChangeCallback() {
            @Override
            public long getId() {
                return checkCallbackId;
            }

            @Override
            public void afterVisible(TransactionState txnState) {
                persistedWhenAfterVisible.set(
                        fakeEditLog.getTransaction(txnState.getTransactionId()) != null);
            }
        });

        TransactionStateBatch stateBatch = new TransactionStateBatch(states);
        masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null);
        assertTrue(persistedWhenAfterVisible.get());
    }

    @Test
    public void testFinishTransactionBatchSwallowsBatchAfterVisibleException() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<TransactionState> states = getDefaultBatchTxnStates(masterDbTransMgr);

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        // Register a throwing callback to verify exception is swallowed
        long throwCallbackId = 99998L;
        states.get(0).addCallbackId(throwCallbackId);
        masterTransMgr.getCallbackFactory().addCallback(new AbstractTxnStateChangeCallback() {
            @Override
            public long getId() {
                return throwCallbackId;
            }

            @Override
            public void afterVisible(TransactionState txnState) {
                throw new RuntimeException("mock afterVisible failure");
            }
        });

        TransactionStateBatch stateBatch = new TransactionStateBatch(states);
        TransactionStateBatch result = Assertions.assertDoesNotThrow(
                () -> masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null));
        for (TransactionState state : result.getTransactionStates()) {
            assertEquals(TransactionStatus.VISIBLE, state.getTransactionStatus());
            assertNotNull(fakeEditLog.getTransaction(state.getTransactionId()));
        }
    }

    @Test
    public void testFinishTransactionBatchAfterVisibleCallbackFailureIsolation() throws StarRocksException {
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<TransactionState> states = getDefaultBatchTxnStates(masterDbTransMgr);
        AtomicInteger callbackInvokeCount = new AtomicInteger(0);

        long failedCallbackId = 90001L;
        long normalCallbackId = 90002L;
        states.get(0).addCallbackId(failedCallbackId);
        states.get(1).addCallbackId(normalCallbackId);
        masterTransMgr.getCallbackFactory().addCallback(
                new TestTxnStateChangeCallback(failedCallbackId, true, callbackInvokeCount));
        masterTransMgr.getCallbackFactory().addCallback(
                new TestTxnStateChangeCallback(normalCallbackId, false, callbackInvokeCount));

        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }
        };

        TransactionStateBatch stateBatch = new TransactionStateBatch(states);
        TransactionStateBatch result = Assertions.assertDoesNotThrow(
                () -> masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null));

        assertEquals(2, callbackInvokeCount.get());
        for (TransactionState state : result.getTransactionStates()) {
            assertEquals(TransactionStatus.VISIBLE, state.getTransactionStatus());
        }
    }

    private List<TransactionState> getDefaultBatchTxnStates(DatabaseTransactionMgr masterDbTransMgr) {
        long txnId6 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable6);
        TransactionState transactionState6 = masterDbTransMgr.getTransactionState(txnId6);
        long txnId7 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable7);
        TransactionState transactionState7 = masterDbTransMgr.getTransactionState(txnId7);
        long txnId8 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable8);
        TransactionState transactionState8 = masterDbTransMgr.getTransactionState(txnId8);
        return Lists.newArrayList(transactionState6, transactionState7, transactionState8);
    }

    private static class TestTxnStateChangeCallback implements TxnStateChangeCallback {
        private final long id;
        private final boolean throwInAfterVisible;
        private final AtomicInteger callbackInvokeCount;

        private TestTxnStateChangeCallback(long id, boolean throwInAfterVisible, AtomicInteger callbackInvokeCount) {
            this.id = id;
            this.throwInAfterVisible = throwInAfterVisible;
            this.callbackInvokeCount = callbackInvokeCount;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public void beforeCommitted(TransactionState txnState) throws TransactionException {
        }

        @Override
        public void beforeAborted(TransactionState txnState) throws TransactionException {
        }

        @Override
        public void afterCommitted(TransactionState txnState) throws StarRocksException {
        }

        @Override
        public void replayOnCommitted(TransactionState txnState) {
        }

        @Override
        public void afterAborted(TransactionState txnState, String txnStatusChangeReason)
                throws StarRocksException {
        }

        @Override
        public void replayOnAborted(TransactionState txnState) {
        }

        @Override
        public void afterVisible(TransactionState txnState) {
            callbackInvokeCount.incrementAndGet();
            if (throwInAfterVisible) {
                throw new RuntimeException("mock callback failure");
            }
        }

        @Override
        public void replayOnVisible(TransactionState txnState) {
        }

        @Override
        public void beforePrepared(TransactionState txnState) throws TransactionException {
        }

        @Override
        public void afterPrepared(TransactionState txnState) throws StarRocksException {
        }

        @Override
        public void replayOnPrepared(TransactionState txnState) {
        }
    }

    private EditLog replaceDatabaseTransactionMgrEditLog(DatabaseTransactionMgr dbTransactionMgr, EditLog editLog) {
        EditLog originalEditLog = Deencapsulation.getField(dbTransactionMgr, "editLog");
        Deencapsulation.setField(dbTransactionMgr, "editLog", editLog);
        return originalEditLog;
    }

    private void assertEditLogWriteFailed(RuntimeException exception) {
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed")
                || exception.getCause() != null && exception.getCause().getMessage().contains("EditLog write failed"));
    }

    private void assertPublishTimeoutOrNull(Throwable throwable) {
        if (throwable == null) {
            return;
        }
        Assertions.assertTrue(throwable instanceof StarRocksException);
        Assertions.assertTrue(throwable.getMessage().contains("publish timeout"));
    }

    private void createSimpleOlapTable(Database db, long tableId, String tableName, long partitionId, String partitionName,
                                       long indexId, String indexName, long tabletId, long version,
                                       long replicaId1, long replicaId2, long replicaId3) {
        Replica replica1 = new Replica(replicaId1, GlobalStateMgrTestUtil.testBackendId1, version, 0, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1, 0);
        Replica replica2 = new Replica(replicaId2, GlobalStateMgrTestUtil.testBackendId2, version, 0, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1, 0);
        Replica replica3 = new Replica(replicaId3, GlobalStateMgrTestUtil.testBackendId3, version, 0, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1, 0);

        LocalTablet tablet = new LocalTablet(tabletId);
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(db.getId(), tableId, partitionId + 100, indexId, TStorageMedium.HDD);
        index.addTablet(tablet, tabletMeta);
        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);

        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(partitionId, partitionId + 100, partitionName, index, distributionInfo);
        partition.getDefaultPhysicalPartition().updateVisibleVersion(version);
        partition.getDefaultPhysicalPartition().setNextVersion(version + 1);

        List<Column> columns = new ArrayList<>();
        Column key1 = new Column("k1", IntegerType.INT);
        key1.setIsKey(true);
        columns.add(key1);
        Column key2 = new Column("k2", IntegerType.INT);
        key2.setIsKey(true);
        columns.add(key2);
        columns.add(new Column("v", FloatType.DOUBLE, false, AggregateType.SUM, "0", ""));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        OlapTable table = new OlapTable(tableId, tableName, columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        table.addPartition(partition);
        table.setIndexMeta(indexId, indexName, columns, 0, GlobalStateMgrTestUtil.testSchemaHash1 + 1, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setBaseIndexMetaId(indexId);
        table.setReplicationNum((short) 3);
        db.registerTableUnlocked(table);
    }
}
