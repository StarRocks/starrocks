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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.StringUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.replication.ReplicationTxnCommitAttachment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.OptimisticVersion;
import com.starrocks.sql.StatementPlanner;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private static void addCommitInfo(TransactionState txn, long tableId, long physicalPartitionId,
                                      long version, long versionTime) {
        TableCommitInfo tableCommitInfo = txn.getTableCommitInfo(tableId);
        if (tableCommitInfo == null) {
            tableCommitInfo = new TableCommitInfo(tableId);
            txn.putIdToTableCommitInfo(tableId, tableCommitInfo);
        }
        tableCommitInfo.addPartitionCommitInfo(new PartitionCommitInfo(physicalPartitionId, version, versionTime));
    }

    private OlapTable hasDeleteTable() {
        return (OlapTable) masterGlobalStateMgr.getLocalMetastore().getTable(
                GlobalStateMgrTestUtil.testDbId1, GlobalStateMgrTestUtil.testTableId1);
    }

    private DatabaseTransactionMgr hasDeleteTxnMgr(boolean lake) throws AnalysisException {
        // The shared setup ends on the follower; these tests mutate and inspect the leader catalog.
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        new MockUp<Table>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return lake;
            }
        };
        return masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);
    }

    private TransactionState hasDeleteTxn(long id, TransactionState.LoadJobSourceType source,
                                          TransactionStatus status, long version) {
        OlapTable table = hasDeleteTable();
        TransactionState txn = new TransactionState(GlobalStateMgrTestUtil.testDbId1, List.of(table.getId()), id,
                "has_delete_" + id, null, source, transactionSource, -1, 60_000);
        txn.setPrepareTime(System.currentTimeMillis());
        txn.setCommitTime(System.currentTimeMillis());
        if (status == TransactionStatus.VISIBLE) {
            txn.setFinishTime(System.currentTimeMillis());
        }
        txn.setTransactionStatus(status);
        txn.setFinishState(new TxnFinishState());
        addCommitInfo(txn, table.getId(), table.getPartition(GlobalStateMgrTestUtil.testPartition1)
                .getDefaultPhysicalPartition().getId(), version, System.currentTimeMillis());
        // Exercise the persisted transaction representation, with no DeleteJob callback registered.
        TransactionState restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(txn), TransactionState.class);
        assertEquals(source, restored.getSourceType());
        for (long callbackId : restored.getCallbackId()) {
            assertNull(masterTransMgr.getCallbackFactory().getCallback(callbackId));
        }
        return restored;
    }

    private static boolean hasDeletePlanAccepted(OlapTable table, long planStart) {
        // The actual final check used by createQueryPlanWithReTry, not a timestamp comparison in the test.
        return Deencapsulation.invoke(StatementPlanner.class, "checkOlapTableSchemaValid",
                Sets.newHashSet(table), planStart);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHasDeleteVisibleReplay(boolean lake) throws Exception {
        DatabaseTransactionMgr mgr = hasDeleteTxnMgr(lake);
        OlapTable table = hasDeleteTable();
        PhysicalPartition partition = table.getPartition(GlobalStateMgrTestUtil.testPartition1)
                .getDefaultPhysicalPartition();
        long base = partition.getVisibleVersion();
        long planStart = OptimisticVersion.generate();
        Assertions.assertFalse(table.hasDelete());
        mgr.replayUpsertTransactionState(hasDeleteTxn(91001, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.VISIBLE, base + 1));
        assertTrue(table.hasDelete());
        assertEquals(base + 1, partition.getVisibleVersion());
        Assertions.assertFalse(hasDeletePlanAccepted(table, planStart));
        OlapTable image = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(table), OlapTable.class);
        assertTrue(image.hasDelete(), "the corrected flag survives a catalog image round trip");
        long invalidation = table.lastSchemaUpdateTime.get();
        mgr.replayUpsertTransactionState(hasDeleteTxn(91002, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.VISIBLE, base + 2));
        assertTrue(table.hasDelete());
        assertEquals(invalidation, table.lastSchemaUpdateTime.get(), "only the first DELETE invalidates plans");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHasDeleteCommittedRecoveryAndAsyncFinish(boolean lake) throws Exception {
        DatabaseTransactionMgr mgr = hasDeleteTxnMgr(lake);
        OlapTable table = hasDeleteTable();
        PhysicalPartition partition = table.getPartition(GlobalStateMgrTestUtil.testPartition1)
                .getDefaultPhysicalPartition();
        long base = partition.getVisibleVersion();
        TransactionState committed = hasDeleteTxn(92001, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.COMMITTED, base + 1);
        mgr.replayUpsertTransactionState(committed);
        Assertions.assertFalse(table.hasDelete());
        assertEquals(base, partition.getVisibleVersion());
        Assertions.assertFalse(new VisibleStateWaiter(committed).await(0, TimeUnit.MILLISECONDS));
        assertEquals(TransactionStatus.COMMITTED, committed.getTransactionStatus());
        // The client has stopped waiting. Publish completes without DeleteJob.commit/afterVisible.
        mgr.finishTransactionNew(committed, Sets.newHashSet());
        assertEquals(TransactionStatus.VISIBLE, committed.getTransactionStatus());
        assertTrue(table.hasDelete());
        assertEquals(base + 1, partition.getVisibleVersion());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHasDeleteOtherSourcesAndAbort(boolean lake) throws Exception {
        DatabaseTransactionMgr mgr = hasDeleteTxnMgr(lake);
        OlapTable table = hasDeleteTable();
        PhysicalPartition partition = table.getPartition(GlobalStateMgrTestUtil.testPartition1)
                .getDefaultPhysicalPartition();
        long base = partition.getVisibleVersion();
        long invalidation = table.lastSchemaUpdateTime.get();
        mgr.replayUpsertTransactionState(hasDeleteTxn(93001, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.ABORTED, base + 1));
        Assertions.assertFalse(table.hasDelete());
        assertEquals(base, partition.getVisibleVersion());
        mgr.replayUpsertTransactionState(hasDeleteTxn(93002, TransactionState.LoadJobSourceType.FRONTEND,
                TransactionStatus.VISIBLE, base + 1));
        Assertions.assertFalse(table.hasDelete());
        assertEquals(invalidation, table.lastSchemaUpdateTime.get());
        if (lake) {
            mgr.replayUpsertTransactionState(hasDeleteTxn(93003, TransactionState.LoadJobSourceType.LAKE_COMPACTION,
                    TransactionStatus.VISIBLE, base + 2));
            Assertions.assertFalse(table.hasDelete());
            assertEquals(invalidation, table.lastSchemaUpdateTime.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHasDeleteConcurrentPlanInvalidation(boolean lake) throws Exception {
        DatabaseTransactionMgr mgr = hasDeleteTxnMgr(lake);
        OlapTable table = hasDeleteTable();
        PhysicalPartition partition = table.getPartition(GlobalStateMgrTestUtil.testPartition1)
                .getDefaultPhysicalPartition();
        long base = partition.getVisibleVersion();
        TransactionState committed = hasDeleteTxn(94001, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.COMMITTED, base + 1);
        mgr.replayUpsertTransactionState(committed);
        long planStart = OptimisticVersion.generate();
        OlapTable snapshot = new OlapTable();
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(committed.getDbId(), table.getId(), LockType.READ);
        try {
            table.copyOnlyForQuery(snapshot);
        } finally {
            locker.unLockTableWithIntensiveDbLock(committed.getDbId(), table.getId(), LockType.READ);
        }
        PhysicalPartition shared = snapshot.getPhysicalPartition(partition.getId());
        Assertions.assertSame(partition, shared);
        Assertions.assertFalse(snapshot.hasDelete());
        assertTrue(hasDeletePlanAccepted(table, planStart));
        CountDownLatch flagSet = new CountDownLatch(1);
        CountDownLatch allowInvalidate = new CountDownLatch(1);
        CountDownLatch beforePublish = new CountDownLatch(1);
        CountDownLatch allowPublish = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        new MockUp<TableProperty>() {
            @Mock
            public void setHasDelete(Invocation invocation, boolean hasDelete) throws InterruptedException {
                invocation.proceed(hasDelete);
                flagSet.countDown();
                assertTrue(allowInvalidate.await(10, TimeUnit.SECONDS));
            }
        };
        // Both OLAP and lake ultimately invoke this publication method.
        new MockUp<PhysicalPartition>() {
            @Mock
            public void setVisibleVersion(Invocation invocation, long version, long time) throws InterruptedException {
                assertTrue(table.hasDelete());
                Assertions.assertFalse(hasDeletePlanAccepted(table, planStart),
                        "old plans must be invalid before the DELETE version is published");
                beforePublish.countDown();
                assertTrue(allowPublish.await(10, TimeUnit.SECONDS));
                invocation.proceed(version, time);
            }
        };
        Thread publisher = new Thread(() -> {
            try {
                mgr.finishTransactionNew(committed, Sets.newHashSet());
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "has-delete-publisher");
        publisher.start();
        try {
            assertTrue(flagSet.await(10, TimeUnit.SECONDS));
            assertTrue(table.hasDelete());
            Assertions.assertFalse(snapshot.hasDelete());
            assertEquals(base, shared.getVisibleVersion(), "flag mutation cannot publish the DELETE version");
            assertTrue(hasDeletePlanAccepted(table, planStart), "invalidation follows flag mutation");
            allowInvalidate.countDown();
            assertTrue(beforePublish.await(10, TimeUnit.SECONDS));
            assertEquals(base, shared.getVisibleVersion());
            Assertions.assertFalse(hasDeletePlanAccepted(table, planStart));
        } finally {
            allowInvalidate.countDown();
            allowPublish.countDown();
            publisher.join(10_000);
        }
        Assertions.assertFalse(publisher.isAlive());
        assertNull(failure.get());
        // Model a stale planner building scan ranges after publication, then doing its final validation.
        Assertions.assertFalse(snapshot.hasDelete());
        assertEquals(base + 1, shared.getVisibleVersion());
        Assertions.assertFalse(hasDeletePlanAccepted(table, planStart), "the stale plan must be retried");
        long retryStart = OptimisticVersion.generate();
        locker.lockTableWithIntensiveDbLock(committed.getDbId(), table.getId(), LockType.READ);
        try {
            table.copyOnlyForQuery(snapshot);
        } finally {
            locker.unLockTableWithIntensiveDbLock(committed.getDbId(), table.getId(), LockType.READ);
        }
        assertTrue(snapshot.hasDelete());
        assertEquals(base + 1, snapshot.getPhysicalPartition(partition.getId()).getVisibleVersion());
        assertTrue(hasDeletePlanAccepted(table, retryStart), "the refreshed plan is accepted");
    }

    @Test
    public void testHasDeleteBatchInvalidatesBeforeDeleteVersion() throws Exception {
        DatabaseTransactionMgr mgr = hasDeleteTxnMgr(true);
        OlapTable table = hasDeleteTable();
        PhysicalPartition partition = table.getPartition(GlobalStateMgrTestUtil.testPartition1)
                .getDefaultPhysicalPartition();
        long base = partition.getVisibleVersion();
        long planStart = OptimisticVersion.generate();
        TransactionState before = hasDeleteTxn(95001, TransactionState.LoadJobSourceType.FRONTEND,
                TransactionStatus.VISIBLE, base + 1);
        TransactionState delete = hasDeleteTxn(95002, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.VISIBLE, base + 2);
        TransactionState after = hasDeleteTxn(95003, TransactionState.LoadJobSourceType.FRONTEND,
                TransactionStatus.VISIBLE, base + 3);
        AtomicInteger publishes = new AtomicInteger();
        new MockUp<PhysicalPartition>() {
            @Mock
            public void setVisibleVersion(Invocation invocation, long version, long time) {
                // 3.5 applies each transaction separately instead of deferring publication to batch end.
                assertEquals(base + publishes.incrementAndGet(), version);
                if (version >= base + 2) {
                    assertTrue(table.hasDelete());
                    Assertions.assertFalse(hasDeletePlanAccepted(table, planStart));
                } else {
                    Assertions.assertFalse(table.hasDelete());
                    assertTrue(hasDeletePlanAccepted(table, planStart));
                }
                invocation.proceed(version, time);
            }
        };
        mgr.replayUpsertTransactionStateBatch(new TransactionStateBatch(List.of(before, delete, after)));
        assertEquals(3, publishes.get());
        assertEquals(base + 3, partition.getVisibleVersion());
        assertTrue(table.hasDelete());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testHasDeleteReplayDroppedObjects(boolean lake) throws Exception {
        DatabaseTransactionMgr mgr = hasDeleteTxnMgr(lake);
        OlapTable table = hasDeleteTable();
        PhysicalPartition partition = table.getPartition(GlobalStateMgrTestUtil.testPartition1)
                .getDefaultPhysicalPartition();
        long base = partition.getVisibleVersion();
        TransactionState droppedTable = hasDeleteTxn(96001, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.VISIBLE, base + 1);
        droppedTable.getIdToTableCommitInfos().clear();
        addCommitInfo(droppedTable, -100L, partition.getId(), base + 1, System.currentTimeMillis());
        mgr.replayUpsertTransactionState(droppedTable);
        Assertions.assertFalse(table.hasDelete());
        TransactionState droppedPartition = hasDeleteTxn(96002, TransactionState.LoadJobSourceType.DELETE,
                TransactionStatus.VISIBLE, base + 1);
        droppedPartition.getTableCommitInfo(table.getId()).getIdToPartitionCommitInfo().clear();
        addCommitInfo(droppedPartition, table.getId(), -100L, base + 1, System.currentTimeMillis());
        mgr.replayUpsertTransactionState(droppedPartition);
        assertTrue(table.hasDelete());
        assertEquals(base, partition.getVisibleVersion());
    }

    @Test
    public void testHasDeleteLegacySetterInvalidatesPlan() {
        OlapTable table = hasDeleteTable();
        long planStart = OptimisticVersion.generate();
        OlapTable snapshot = new OlapTable();
        table.copyOnlyForQuery(snapshot);
        table.setHasDelete();
        Assertions.assertFalse(snapshot.hasDelete());
        assertTrue(table.hasDelete());
        Assertions.assertFalse(hasDeletePlanAccepted(table, planStart));
        long invalidation = table.lastSchemaUpdateTime.get();
        table.setHasDelete();
        assertEquals(invalidation, table.lastSchemaUpdateTime.get());
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
    public void getMaxCommittedTxnPendingPublishMsTest() throws StarRocksException {
        DatabaseTransactionMgr masterDbTransMgr =
                masterTransMgr.getDatabaseTransactionMgr(GlobalStateMgrTestUtil.testDbId1);

        // setUp() seeds several committed transactions; the metric reports the pending-publish time of the oldest one.
        // getCommittedTxnList() is sorted ascending by commit time, so the first entry is the oldest.
        List<TransactionState> committedTxns = masterDbTransMgr.getCommittedTxnList();
        Assertions.assertFalse(committedTxns.isEmpty());
        long oldestCommitTime = committedTxns.get(0).getCommitTime();

        // pending-publish time is measured from the oldest commit time; use a fixed "now" for a deterministic assertion
        assertEquals(5000L, masterDbTransMgr.getMaxCommittedTxnPendingPublishMs(oldestCommitTime + 5000L));
        // a "now" earlier than the commit time must be clamped to 0, never negative
        assertEquals(0L, masterDbTransMgr.getMaxCommittedTxnPendingPublishMs(oldestCommitTime - 1000L));

        // once every committed transaction becomes visible, nothing is sitting in committed status
        for (TransactionState txn : committedTxns) {
            masterDbTransMgr.finishTransaction(txn.getTransactionId(), null, 0);
        }
        Assertions.assertTrue(masterDbTransMgr.getCommittedTxnList().isEmpty());
        assertEquals(0L, masterDbTransMgr.getMaxCommittedTxnPendingPublishMs(System.currentTimeMillis()));
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
        assertEquals(TransactionStatus.VISIBLE, transactionState6.getTransactionStatus());
        assertEquals(TransactionStatus.VISIBLE, transactionState7.getTransactionStatus());
        assertEquals(TransactionStatus.VISIBLE, transactionState8.getTransactionStatus());

        FakeGlobalStateMgr.setGlobalStateMgr(slaveGlobalStateMgr);
        slaveTransMgr.replayUpsertTransactionStateBatch(stateBatch);
        assertEquals(4, masterDbTransMgr.getFinishedTxnNums());
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
        assertEquals(TransactionStatus.VISIBLE, txnState.getTransactionStatus());
        lockThread.join();
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

        class InspectPersistedBatch extends TransactionStateBatch {
            private boolean persistedWhenAfterVisible = false;

            InspectPersistedBatch(List<TransactionState> transactionStates) {
                super(transactionStates);
            }

            @Override
            public void afterVisible(TransactionStatus transactionStatus, boolean txnOperated) {
                long txnId = getTransactionStates().get(0).getTransactionId();
                persistedWhenAfterVisible = fakeEditLog.getTransaction(txnId) != null;
                super.afterVisible(transactionStatus, txnOperated);
            }

            boolean isPersistedWhenAfterVisible() {
                return persistedWhenAfterVisible;
            }
        }

        InspectPersistedBatch stateBatch = new InspectPersistedBatch(states);
        masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null);
        assertTrue(stateBatch.isPersistedWhenAfterVisible());
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

        class ThrowingBatch extends TransactionStateBatch {
            ThrowingBatch(List<TransactionState> transactionStates) {
                super(transactionStates);
            }

            @Override
            public void afterVisible(TransactionStatus transactionStatus, boolean txnOperated) {
                throw new RuntimeException("mock afterVisible failure");
            }
        }

        TransactionStateBatch stateBatch = new ThrowingBatch(states);
        Assertions.assertDoesNotThrow(
                () -> masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null));
        for (TransactionState state : states) {
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
        Assertions.assertDoesNotThrow(
                () -> masterTransMgr.finishTransactionBatch(GlobalStateMgrTestUtil.testDbId1, stateBatch, null));

        assertEquals(2, callbackInvokeCount.get());
        for (TransactionState state : states) {
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
        public void afterCommitted(TransactionState txnState, boolean txnOperated) throws StarRocksException {
        }

        @Override
        public void replayOnCommitted(TransactionState txnState) {
        }

        @Override
        public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
                throws StarRocksException {
        }

        @Override
        public void replayOnAborted(TransactionState txnState) {
        }

        @Override
        public void afterVisible(TransactionState txnState, boolean txnOperated) {
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
        public void afterPrepared(TransactionState txnState, boolean txnOperated) throws StarRocksException {
        }

        @Override
        public void replayOnPrepared(TransactionState txnState) {
        }
    }
}
