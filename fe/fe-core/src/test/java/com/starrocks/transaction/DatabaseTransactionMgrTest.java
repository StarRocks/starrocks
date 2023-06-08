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
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DatabaseTransactionMgrTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static FakeEditLog fakeEditLog;
    private static FakeGlobalStateMgr fakeGlobalStateMgr;
    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static GlobalStateMgr masterGlobalStateMgr;
    private static GlobalStateMgr slaveGlobalStateMgr;
    private static Map<String, Long> lableToTxnId;

    private TransactionState.TxnCoordinator transactionSource =
            new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "localfe");

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, UserException {
        Config.label_keep_max_second = 10;
        fakeEditLog = new FakeEditLog();
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();
        slaveGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();

        masterTransMgr = masterGlobalStateMgr.getGlobalTransactionMgr();

        slaveTransMgr = slaveGlobalStateMgr.getGlobalTransactionMgr();

        lableToTxnId = addTransactionToTransactionMgr();
    }

    public Map<String, Long> addTransactionToTransactionMgr() throws UserException {
        Map<String, Long> lableToTxnId = Maps.newHashMap();
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        long transactionId1 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable1,
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
        masterTransMgr.commitTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, transTablets);
        masterTransMgr.finishTransaction(GlobalStateMgrTestUtil.testDbId1, transactionId1, null);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable1, transactionId1);

        TransactionState.TxnCoordinator beTransactionSource =
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.BE, "be1");
        long transactionId2 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable2,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK,
                        Config.stream_load_default_timeout_second);
        long transactionId3 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable3,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.stream_load_default_timeout_second);
        long transactionId4 = masterTransMgr
                .beginTransaction(GlobalStateMgrTestUtil.testDbId1, Lists.newArrayList(GlobalStateMgrTestUtil.testTableId1),
                        GlobalStateMgrTestUtil.testTxnLable4,
                        beTransactionSource,
                        TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                        Config.stream_load_default_timeout_second);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable2, transactionId2);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable3, transactionId3);
        lableToTxnId.put(GlobalStateMgrTestUtil.testTxnLable4, transactionId4);

        TransactionState transactionState1 = fakeEditLog.getTransaction(transactionId1);

        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionState(transactionState1);
        return lableToTxnId;
    }

    @Test
    public void testNormal() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        assertEquals(4, masterDbTransMgr.getTransactionNum());
        assertEquals(2, masterDbTransMgr.getRunningTxnNums());
        assertEquals(1, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(1, masterDbTransMgr.getFinishedTxnNums());
        DatabaseTransactionMgr slaveDbTransMgr = slaveTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        assertEquals(1, slaveDbTransMgr.getTransactionNum());
        assertEquals(1, slaveDbTransMgr.getFinishedTxnNums());

        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1).size());
        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable2).size());
        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable3).size());
        assertEquals(1, masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable4).size());

        Long txnId1 = masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1).iterator().next();
        assertEquals(txnId1, lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1));
        TransactionState transactionState1 =
                masterDbTransMgr.getTransactionState(lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1));
        assertEquals(txnId1.longValue(), transactionState1.getTransactionId());
        assertEquals(TransactionStatus.VISIBLE, transactionState1.getTransactionStatus());

        Long txnId2 = masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable2).iterator().next();
        assertEquals(txnId2, lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable2));
        TransactionState transactionState2 = masterDbTransMgr.getTransactionState(txnId2);
        assertEquals(txnId2.longValue(), transactionState2.getTransactionId());
        assertEquals(TransactionStatus.PREPARE, transactionState2.getTransactionStatus());

    }

    @Test
    public void testAbortTransaction() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        long txnId2 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable2);
        masterDbTransMgr.abortTransaction(txnId2, "test abort transaction", null);
        assertEquals(2, masterDbTransMgr.getRunningTxnNums());
        assertEquals(0, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(2, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(4, masterDbTransMgr.getTransactionNum());

        long txnId3 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable3);
        masterDbTransMgr.abortTransaction(txnId3, "test abort transaction", null);
        assertEquals(1, masterDbTransMgr.getRunningTxnNums());
        assertEquals(0, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(3, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(4, masterDbTransMgr.getTransactionNum());
    }

    @Test
    public void testAbortTransactionWithNotFoundException() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        long txnId1 = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        expectedEx.expect(UserException.class);
        expectedEx.expectMessage("transaction not found");
        masterDbTransMgr.abortTransaction(txnId1, "test abort transaction", null);
    }

    @Test
    public void testGetTransactionIdByCoordinateBe() throws UserException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        List<Pair<Long, Long>> transactionInfoList = masterDbTransMgr.getTransactionIdByCoordinateBe("be1", 10);
        assertEquals(3, transactionInfoList.size());
        assertEquals(GlobalStateMgrTestUtil.testDbId1, transactionInfoList.get(0).first.longValue());
        assertEquals(TransactionStatus.PREPARE,
                masterDbTransMgr.getTransactionState(transactionInfoList.get(0).second).getTransactionStatus());
    }

    @Test
    public void testGetSingleTranInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        List<List<String>> singleTranInfos = masterDbTransMgr.getSingleTranInfo(GlobalStateMgrTestUtil.testDbId1, txnId);
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
        assertEquals("", txnInfo.get(9));
        assertEquals("0", txnInfo.get(10));
        assertEquals("-1", txnInfo.get(11));
        assertEquals(String.valueOf(Config.stream_load_default_timeout_second * 1000L), txnInfo.get(12));
    }

    @Test
    public void testRemoveExpiredTxns() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        Config.label_keep_max_second = -1;
        long currentMillis = System.currentTimeMillis();
        masterDbTransMgr.removeExpiredTxns(currentMillis);
        assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(3, masterDbTransMgr.getTransactionNum());
        assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1));
    }

    @Test
    public void testGetTableTransInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        Long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        List<List<Comparable>> tableTransInfos = masterDbTransMgr.getTableTransInfo(txnId);
        assertEquals(1, tableTransInfos.size());
        List<Comparable> tableTransInfo = tableTransInfos.get(0);
        assertEquals(2, tableTransInfo.size());
        assertEquals(2L, tableTransInfo.get(0));
        assertEquals("3", tableTransInfo.get(1));
    }

    @Test
    public void testGetPartitionTransInfo() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        Long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        List<List<Comparable>> partitionTransInfos =
                masterDbTransMgr.getPartitionTransInfo(txnId, GlobalStateMgrTestUtil.testTableId1);
        assertEquals(1, partitionTransInfos.size());
        List<Comparable> partitionTransInfo = partitionTransInfos.get(0);
        assertEquals(2, partitionTransInfo.size());
        assertEquals(3L, partitionTransInfo.get(0));
        assertEquals(13L, partitionTransInfo.get(1));
    }

    @Test
    public void testDeleteTransaction() throws AnalysisException {
        DatabaseTransactionMgr masterDbTransMgr = masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
        long txnId = lableToTxnId.get(GlobalStateMgrTestUtil.testTxnLable1);
        TransactionState transactionState = masterDbTransMgr.getTransactionState(txnId);
        masterDbTransMgr.deleteTransaction(transactionState);
        assertEquals(2, masterDbTransMgr.getRunningTxnNums());
        assertEquals(1, masterDbTransMgr.getRunningRoutineLoadTxnNums());
        assertEquals(0, masterDbTransMgr.getFinishedTxnNums());
        assertEquals(3, masterDbTransMgr.getTransactionNum());
        assertNull(masterDbTransMgr.unprotectedGetTxnIdsByLabel(GlobalStateMgrTestUtil.testTxnLable1));
    }
}
