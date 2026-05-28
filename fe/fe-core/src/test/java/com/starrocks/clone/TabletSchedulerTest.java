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

package com.starrocks.clone;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RecyclePartitionInfo;
import com.starrocks.catalog.RecycleRangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.CloneTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletSchema;
import com.starrocks.thrift.TTabletType;
import com.starrocks.transaction.GtidGenerator;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import static com.starrocks.sql.ast.KeysType.DUP_KEYS;

public class TabletSchedulerTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    private NodeMgr nodeMgr;

    @Mocked
    private EditLog editLog;

    SystemInfoService systemInfoService;
    TabletInvertedIndex tabletInvertedIndex;
    TabletSchedulerStat tabletSchedulerStat;
    FakeEditLog fakeEditLog;
    LockManager lockManager;
    VariableMgr variableMgr;

    @BeforeEach
    public void setup() throws Exception {
        systemInfoService = new SystemInfoService();
        tabletInvertedIndex = new TabletInvertedIndex();
        tabletSchedulerStat = new TabletSchedulerStat();
        fakeEditLog = new FakeEditLog();
        lockManager = new LockManager();
        variableMgr = new VariableMgr();


        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                GlobalStateMgr.isCheckpointThread();
                minTimes = 0;
                result = false;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getTabletInvertedIndex();
                minTimes = 0;
                result = tabletInvertedIndex;

                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                globalStateMgr.getLockManager();
                minTimes = 0;
                result = lockManager;

                globalStateMgr.getGtidGenerator();
                minTimes = 0;
                result = new GtidGenerator();

                globalStateMgr.getVariableMgr();
                minTimes = 0;
                result = variableMgr;

                globalStateMgr.getClusterSnapshotMgr();
                minTimes = 0;
                result = new ClusterSnapshotMgr();
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

    }

    @Test
    public void testRemoveAllTabletIdsIfExpired() throws InterruptedException {
        Database db = new Database(1, "db");
        Table table = new Table(3, "table", Table.TableType.OLAP, new ArrayList<>());
        Partition partition = new Partition(5, 6, "partition", new MaterializedIndex(), null);

        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleDatabase(db, new HashSet<>(), true);
        recycleBin.recycleTable(db.getId(), table, true);
        RecyclePartitionInfo recyclePartitionInfo = new RecycleRangePartitionInfo(db.getId(), table.getId(),
                partition, null, new DataProperty(null), (short) 2, null);
        recycleBin.recyclePartition(recyclePartitionInfo);

        List<TabletSchedCtx> allCtxs = new ArrayList<>();
        List<Triple<Database, Table, Partition>> arguments = Arrays.asList(
                Triple.of(db, table, partition),
                Triple.of(db, table, partition),
                Triple.of(db, table, partition),
                Triple.of(db, table, partition)
        );
        int tabletId = 1;
        for (Triple<Database, Table, Partition> triple : arguments) {
            TabletSchedCtx tabletSchedCtx = new TabletSchedCtx(
                    TabletSchedCtx.Type.REPAIR,
                    triple.getLeft().getId(),
                    triple.getMiddle().getId(),
                    triple.getRight().getDefaultPhysicalPartition().getId(),
                    1,
                    tabletId++,
                    System.currentTimeMillis(),
                    systemInfoService);
            tabletSchedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
            allCtxs.add(tabletSchedCtx);
        }

        Deencapsulation.setField(GlobalStateMgr.getCurrentState(), "recycleBin", recycleBin);
        TabletScheduler tabletScheduler = new TabletScheduler(tabletSchedulerStat);

        long originalCatalogTrashExpireSecond = Config.catalog_trash_expire_second;

        try {
            Config.catalog_trash_expire_second = 1;
            allCtxs.forEach(e -> tabletScheduler.addTablet(e, false));
            Assertions.assertEquals(tabletScheduler.getTotalNum(), 4);
            Thread.sleep(1100);
            List<TabletSchedCtx> nextBatch = Deencapsulation.invoke(tabletScheduler, "getNextTabletCtxBatch");
            Assertions.assertEquals(nextBatch.size(), 0);
            Assertions.assertEquals(tabletScheduler.getTotalNum(), 0);
            Assertions.assertEquals(tabletScheduler.getHistoryNum(), 4);
            for (TabletSchedCtx ctx : allCtxs) {
                Assertions.assertEquals(ctx.getState(), TabletSchedCtx.State.EXPIRED);
            }
        } finally {
            Config.catalog_trash_expire_second = originalCatalogTrashExpireSecond;
        }
    }

    @Test
    public void testSubmitBatchTaskIfNotExpired() {
        Database badDb = new Database(1, "mal");
        Database goodDB = new Database(2, "bueno");
        Table badTable = new Table(3, "mal", Table.TableType.OLAP, new ArrayList<>());
        Table goodTable = new Table(4, "bueno", Table.TableType.OLAP, new ArrayList<>());
        Partition badPartition = new Partition(5, 55, "mal", new MaterializedIndex(), null);
        Partition goodPartition = new Partition(6, 66, "bueno", new MaterializedIndex(), null);

        long now = System.currentTimeMillis();
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleDatabase(badDb, new HashSet<>(), true);
        recycleBin.recycleTable(goodDB.getId(), badTable, true);
        RecyclePartitionInfo recyclePartitionInfo = new RecycleRangePartitionInfo(goodDB.getId(), goodTable.getId(),
                badPartition, null, new DataProperty(TStorageMedium.HDD), (short) 2, null);
        recycleBin.recyclePartition(recyclePartitionInfo);

        List<TabletSchedCtx> allCtxs = new ArrayList<>();
        List<Triple<Database, Table, Partition>> arguments = Arrays.asList(
                Triple.of(badDb, goodTable, goodPartition), // will discard
                Triple.of(goodDB, badTable, goodPartition), // will discard
                Triple.of(goodDB, goodTable, badPartition), // will discard
                Triple.of(goodDB, goodTable, goodPartition) // only submit this
        );
        for (Triple<Database, Table, Partition> triple : arguments) {
            allCtxs.add(new TabletSchedCtx(
                    TabletSchedCtx.Type.REPAIR,
                    triple.getLeft().getId(),
                    triple.getMiddle().getId(),
                    triple.getRight().getDefaultPhysicalPartition().getId(),
                    1,
                    1,
                    System.currentTimeMillis(),
                    systemInfoService));
        }
        TabletScheduler tabletScheduler = new TabletScheduler(tabletSchedulerStat);

        long almostExpireTime = now + (Config.catalog_trash_expire_second - 1) * 1000L;
        for (int i = 0; i != allCtxs.size(); ++i) {
            Assertions.assertFalse(tabletScheduler.checkIfTabletExpired(allCtxs.get(i), recycleBin, almostExpireTime));
        }

        long expireTime = now + (Config.catalog_trash_expire_second + 600) * 1000L;
        for (int i = 0; i != allCtxs.size() - 1; ++i) {
            Assertions.assertTrue(tabletScheduler.checkIfTabletExpired(allCtxs.get(i), recycleBin, expireTime));
        }
        // only the last survive
        Assertions.assertFalse(tabletScheduler.checkIfTabletExpired(allCtxs.get(3), recycleBin, expireTime));
    }

    @Test
    public void testPendingAddTabletCtx() throws InterruptedException {
        int oldVal = Config.tablet_sched_max_scheduling_tablets;
        Config.tablet_sched_max_scheduling_tablets = 8;

        TabletScheduler tabletScheduler = new TabletScheduler(tabletSchedulerStat);
        Database goodDB = new Database(2, "bueno");
        Table goodTable = new Table(4, "bueno", Table.TableType.OLAP, new ArrayList<>());
        Partition goodPartition = new Partition(6, 66, "bueno", new MaterializedIndex(), null);


        List<TabletSchedCtx> tabletSchedCtxList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            TabletSchedCtx ctx = new TabletSchedCtx(
                    TabletSchedCtx.Type.REPAIR,
                    goodDB.getId(),
                    goodTable.getId(),
                    goodPartition.getId(),
                    1,
                    i,
                    System.currentTimeMillis(),
                    systemInfoService);
            tabletSchedCtxList.add(ctx);
        }

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                Locker locker = new Locker();
                tabletSchedCtxList.get(i).setOrigPriority(TabletSchedCtx.Priority.NORMAL);
                try {
                    locker.lockDatabase(goodDB.getId(), LockType.READ);
                    tabletScheduler.blockingAddTabletCtxToScheduler(goodDB, tabletSchedCtxList.get(i), false);
                } finally {
                    locker.unLockDatabase(goodDB.getId(), LockType.READ);
                }
            }
        }, "testAddCtx").start();

        Thread.sleep(2000);
        tabletScheduler.removeOneFromPendingQ();
        Thread.sleep(1000);
        Assertions.assertEquals(9, tabletScheduler.getPendingTabletsInfo(100).size());

        Config.tablet_sched_max_scheduling_tablets = oldVal;
    }

    private void updateSlotWithNewConfig(int newSlotPerPath, Method updateWorkingSlotsMethod,
                                         TabletScheduler tabletScheduler)
            throws InvocationTargetException, IllegalAccessException {
        Config.tablet_sched_slot_num_per_path = newSlotPerPath;
        updateWorkingSlotsMethod.invoke(tabletScheduler, (Object[]) null);
    }

    private long takeSlotNTimes(int nTimes, TabletScheduler.PathSlot pathSlot, long pathHash) throws SchedException {
        long result = -1;
        while (nTimes-- > 0) {
            result = pathSlot.takeSlot(pathHash);
        }
        return result;
    }

    private void freeSlotNTimes(int nTimes, TabletScheduler.PathSlot pathSlot, long pathHash) {
        while (nTimes-- > 0) {
            pathSlot.freeSlot(pathHash);
        }
    }

    @Test
    public void testUpdateWorkingSlots() throws NoSuchMethodException, InvocationTargetException,
            IllegalAccessException, SchedException {
        TDisk td11 = new TDisk("/path11", 1L, 2L, true);
        td11.setPath_hash(11);
        TDisk td12 = new TDisk("/path12", 1L, 2L, true);
        td12.setPath_hash(12);
        Map<String, TDisk> backendDisks1 = new HashMap<>();
        backendDisks1.put("/path11", td11);
        backendDisks1.put("/path12", td12);
        Backend be1 = new Backend(1, "192.168.0.1", 9030);
        systemInfoService.addBackend(be1);
        be1.setAlive(true);
        be1.updateDisks(backendDisks1,  GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());

        TDisk td21 = new TDisk("/path21", 1L, 2L, true);
        td21.setPath_hash(21);
        TDisk td22 = new TDisk("/path22", 1L, 2L, true);
        td22.setPath_hash(22);
        Map<String, TDisk> backendDisks2 = new HashMap<>();
        backendDisks2.put("/path21", td21);
        backendDisks2.put("/path22", td22);
        Backend be2 = new Backend(2, "192.168.0.2", 9030);
        systemInfoService.addBackend(be2);
        be2.updateDisks(backendDisks2,  GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        be2.setAlive(true);

        TabletScheduler tabletScheduler = new TabletScheduler(tabletSchedulerStat);
        Method m = TabletScheduler.class.getDeclaredMethod("updateWorkingSlots", (Class<?>[]) null);
        m.setAccessible(true);
        m.invoke(tabletScheduler, (Object[]) null);
        Map<Long, TabletScheduler.PathSlot> bslots = tabletScheduler.getBackendsWorkingSlots();
        Assertions.assertEquals(Config.tablet_sched_slot_num_per_path, bslots.get(1L).peekSlot(11));
        Assertions.assertEquals(Config.tablet_sched_slot_num_per_path, bslots.get(2L).peekSlot(22));
        long result = takeSlotNTimes(Config.tablet_sched_slot_num_per_path, bslots.get(1L), 11L);
        Assertions.assertEquals(11, result);
        result = takeSlotNTimes(1, bslots.get(1L), 11L);
        Assertions.assertEquals(-1, result);
        freeSlotNTimes(Config.tablet_sched_slot_num_per_path, bslots.get(1L), 11L);
        Assertions.assertEquals(Config.tablet_sched_slot_num_per_path, bslots.get(1L).getSlotTotal(11));

        updateSlotWithNewConfig(128, m, tabletScheduler); // test max slot
        Assertions.assertEquals(TabletScheduler.MAX_SLOT_PER_PATH, bslots.get(1L).getSlotTotal(11));
        Assertions.assertEquals(TabletScheduler.MAX_SLOT_PER_PATH, bslots.get(1L).peekSlot(11));

        updateSlotWithNewConfig(0, m, tabletScheduler); // test min slot
        Assertions.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(1L).peekSlot(11));
        Assertions.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(2L).peekSlot(22));
        takeSlotNTimes(10, bslots.get(1L), 11L); // not enough, can only get 2 free slot
        takeSlotNTimes(10, bslots.get(2L), 21L); // not enough, can only get 2 free slot
        Assertions.assertEquals(0, bslots.get(1L).peekSlot(11));
        Assertions.assertEquals(0, bslots.get(2L).peekSlot(21));
        Assertions.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(1L).getSlotTotal(11));

        updateSlotWithNewConfig(2, m, tabletScheduler);
        Assertions.assertEquals(0, bslots.get(1L).peekSlot(11));
        Assertions.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(1L).peekSlot(12));

        updateSlotWithNewConfig(4, m, tabletScheduler);
        Assertions.assertEquals(2, bslots.get(2L).peekSlot(21));
        Assertions.assertEquals(4, bslots.get(2L).peekSlot(22));
        Assertions.assertEquals(4, bslots.get(1L).getSlotTotal(11));

        takeSlotNTimes(5, bslots.get(1L), 11); // not enough, can only get 2 free slot
        updateSlotWithNewConfig(2, m, tabletScheduler); // decrease total slot
        // this is normal because slot taken haven't return
        Assertions.assertEquals(-2, bslots.get(1L).peekSlot(11));
        Assertions.assertEquals(2, bslots.get(1L).peekSlot(12));
        Assertions.assertEquals(0, bslots.get(2L).peekSlot(21));

        freeSlotNTimes(2, bslots.get(1L), 11L);
        Assertions.assertEquals(0, bslots.get(1L).peekSlot(11));

        freeSlotNTimes(2, bslots.get(1L), 11L);
        Assertions.assertEquals(bslots.get(1L).peekSlot(11), bslots.get(1L).getSlotTotal(11));
    }

    @Test
    public void testGetTabletsNumInScheduleForEachCG() {
        TabletScheduler tabletScheduler = new TabletScheduler(tabletSchedulerStat);
        Map<Long, ColocateTableIndex.GroupId> tabletIdToCGIdForPending = Maps.newHashMap();
        tabletIdToCGIdForPending.put(101L, new ColocateTableIndex.GroupId(200L, 300L));
        tabletIdToCGIdForPending.put(102L, new ColocateTableIndex.GroupId(200L, 300L));
        tabletIdToCGIdForPending.put(103L, new ColocateTableIndex.GroupId(200L, 301L));
        tabletIdToCGIdForPending.forEach((k, v) -> {
            TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, 200L, 201L, 202L,
                    203L, k, System.currentTimeMillis());
            ctx.setColocateGroupId(v);
            ctx.setOrigPriority(TabletSchedCtx.Priority.LOW);
            Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx);
        });

        Map<Long, ColocateTableIndex.GroupId> tabletIdToCGIdForRunning = Maps.newHashMap();
        tabletIdToCGIdForRunning.put(104L, new ColocateTableIndex.GroupId(200L, 300L));
        tabletIdToCGIdForRunning.put(105L, new ColocateTableIndex.GroupId(200L, 300L));
        tabletIdToCGIdForRunning.put(106L, new ColocateTableIndex.GroupId(200L, 301L));
        tabletIdToCGIdForRunning.forEach((k, v) -> {
            TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, 200L, 201L, 202L,
                    203L, k, System.currentTimeMillis());
            ctx.setColocateGroupId(v);
            ctx.setOrigPriority(TabletSchedCtx.Priority.LOW);
            if (k == 104L) {
                ctx.setTabletStatus(LocalTablet.TabletHealthStatus.VERSION_INCOMPLETE);
            }
            Deencapsulation.invoke(tabletScheduler, "addToRunningTablets", ctx);
        });

        Map<ColocateTableIndex.GroupId, Long> result = tabletScheduler.getTabletsNumInScheduleForEachCG();
        Assertions.assertEquals(Optional.of(3L).get(),
                result.get(new ColocateTableIndex.GroupId(200L, 300L)));
        Assertions.assertEquals(Optional.of(2L).get(),
                result.get(new ColocateTableIndex.GroupId(200L, 301L)));
    }

    @Test
    public void testForceRecoverWithEmptyTablet() {
        Config.recover_with_empty_tablet = true;
        List<Replica> replicas = new ArrayList<>();
        replicas.add(new Replica(2, 3001, -1, Replica.ReplicaState.NORMAL));
        replicas.add(new Replica(3, 3002, -2, Replica.ReplicaState.NORMAL));
        replicas.add(new Replica(4, 3003, -3, Replica.ReplicaState.NORMAL));

        LocalTablet localTablet = new LocalTablet(5001, replicas);
        Pair<LocalTablet.TabletHealthStatus, TabletSchedCtx.Priority> result = TabletChecker.getTabletHealthStatusWithPriority(
                localTablet, systemInfoService, 1, 3,
                Arrays.asList(1001L, 1002L, 1003L), null);
        System.out.println(result);

        Assertions.assertEquals(LocalTablet.TabletHealthStatus.FORCE_REDUNDANT, result.first);

        Config.recover_with_empty_tablet = false;
    }

    @Test
    public void testFinishCreateReplicaTask() {
        long beId = 10001L;
        long dbId = 10002L;
        long tblId = 10003L;
        long partitionId = 10004L;
        long indexId = 10005L;
        long tabletId = 10006L;
        long replicaId = 10007L;
        long schemaId = indexId;

        TTabletSchema tabletSchema = SchemaInfo.newBuilder().setId(schemaId)
                .setKeysType(DUP_KEYS)
                .setShortKeyColumnCount((short) 1)
                .setSchemaHash(-1)
                .setStorageType(TStorageType.COLUMN)
                .addColumn(new Column("k1", IntegerType.INT))
                .build().toTabletSchema();

        CreateReplicaTask createReplicaTask = CreateReplicaTask.newBuilder()
                .setNodeId(beId)
                .setDbId(dbId)
                .setTableId(tblId)
                .setPartitionId(partitionId)
                .setIndexId(indexId)
                .setVersion(1)
                .setTabletId(tabletId)
                .setStorageMedium(TStorageMedium.HDD)
                .setPrimaryIndexCacheExpireSec(1)
                .setTabletType(TTabletType.TABLET_TYPE_DISK)
                .setCompressionType(TCompressionType.LZ4_FRAME)
                .setTabletSchema(tabletSchema)
                .build();

        TabletMeta tabletMeta = new TabletMeta(dbId, tblId, partitionId, indexId, TStorageMedium.HDD);

        Replica replica = new Replica(replicaId, beId, -1, Replica.ReplicaState.RECOVER);

        tabletInvertedIndex.addTablet(tabletId, tabletMeta);
        tabletInvertedIndex.addReplica(tabletId, replica);

        TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                dbId, tblId, partitionId, indexId, tabletId, System.currentTimeMillis());
        LocalTablet tablet = new LocalTablet(tabletId);
        tablet.addReplica(replica);
        ctx.setTablet(tablet);

        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());

        TFinishTaskRequest request = new TFinishTaskRequest();
        TStatus status = new TStatus();
        status.setStatus_code(TStatusCode.OK);
        request.setTask_status(status);

        // failure test: running tablet ctx is not exist
        tabletScheduler.finishCreateReplicaTask(createReplicaTask, request);
        Assertions.assertEquals(Replica.ReplicaState.RECOVER, replica.getState());

        // failure test: request not ok
        tabletScheduler.addToRunningTablets(ctx);
        status.setStatus_code(TStatusCode.CANCELLED);
        status.setError_msgs(Lists.newArrayList("canceled"));
        tabletScheduler.finishCreateReplicaTask(createReplicaTask, request);
        Assertions.assertEquals(Replica.ReplicaState.RECOVER, replica.getState());

        // success
        tabletScheduler.addToRunningTablets(ctx);
        status.setStatus_code(TStatusCode.OK);
        tabletScheduler.finishCreateReplicaTask(createReplicaTask, request);
        Assertions.assertEquals(Replica.ReplicaState.NORMAL, replica.getState());
    }

    @Test
    public void testScheduleTabletException() {
        long dbId = 10002L;
        long tblId = 10003L;
        long partitionId = 10004L;
        long physicalPartitionId = 10004L;
        long indexId = 10005L;
        long tabletId = 10006L;

        Database db = new Database(dbId, "db");
        OlapTable table = new OlapTable(tblId, "table", null, null, null, null);
        MaterializedIndex index = new MaterializedIndex(indexId);
        PhysicalPartition physicalPartition = new PhysicalPartition(physicalPartitionId, partitionId, index);
        Partition partition = new Partition(partitionId, physicalPartitionId, "partition", index, null);
        ColocateTableIndex colocateTableIndex = new ColocateTableIndex();

        new Expectations() {
            {
                globalStateMgr.getColocateTableIndex();
                minTimes = 0;
                result = colocateTableIndex;
                globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
                minTimes = 0;
                result = db;
                globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tblId);
                minTimes = 0;
                result = table;
                globalStateMgr.getLocalMetastore().getPhysicalPartitionIncludeRecycleBin(table, physicalPartitionId);
                minTimes = 0;
                result = physicalPartition;
                globalStateMgr.getLocalMetastore().getPartitionIncludeRecycleBin(table, partitionId);
                minTimes = 0;
                result = partition;
                globalStateMgr.getLocalMetastore().getReplicationNumIncludeRecycleBin((PartitionInfo) any, partitionId);
                minTimes = 0;
                result = 3;
                globalStateMgr.getLocalMetastore().getDataPropertyIncludeRecycleBin((PartitionInfo) any, partitionId);
                minTimes = 0;
                result = new DataProperty(TStorageMedium.HDD);
            }
        };

        new MockUp<TabletScheduler>() {
            @Mock
            private boolean checkIfTabletExpired(TabletSchedCtx ctx) {
                return false;
            }
        };

        TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbId, tblId, partitionId, indexId, tabletId,
                System.currentTimeMillis());
        LocalTablet tablet = new LocalTablet(tabletId);
        ctx.setTablet(tablet);

        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());
        Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx);
        Assertions.assertEquals(1L, tabletScheduler.getPendingNum(TabletSchedCtx.Type.REPAIR));

        Deencapsulation.invoke(tabletScheduler, "schedulePendingTablets");
        Assertions.assertEquals(TabletSchedCtx.State.UNEXPECTED, ctx.getState());
        // index.getTablet returns null
        // failed at Preconditions.checkNotNull(tablet);
        Assertions.assertEquals(null, ctx.getErrMsg());
    }

    @Test
    public void testFinishCloneTaskException() {
        long beId = 10001L;
        long dbId = 10002L;
        long tblId = 10003L;
        long partitionId = 10004L;
        long indexId = 10005L;
        long tabletId = 10006L;

        TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbId, tblId, partitionId, indexId, tabletId,
                System.currentTimeMillis());
        LocalTablet tablet = new LocalTablet(tabletId);
        ctx.setTablet(tablet);
        ctx.setState(TabletSchedCtx.State.RUNNING);

        // taskVersion is VERSION_1
        CloneTask task = new CloneTask(beId, "127.0.0.1", dbId, tblId, partitionId, indexId, tabletId, 0,
                Arrays.asList(new TBackend("host1", 8290, 8390)), TStorageMedium.HDD, 2L, 3600);

        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());
        Deencapsulation.invoke(tabletScheduler, "addToRunningTablets", ctx);
        Assertions.assertEquals(1L, tabletScheduler.getRunningNum(TabletSchedCtx.Type.REPAIR));

        tabletScheduler.finishCloneTask(task, new TFinishTaskRequest());
        Assertions.assertEquals(TabletSchedCtx.State.UNEXPECTED, ctx.getState());
        // failed at Preconditions.checkArgument(cloneTask.getTaskVersion() == CloneTask.VERSION_2);
        Assertions.assertEquals(null, ctx.getErrMsg());
    }

    @Test
    public void testHandleColocateRedundantNoRedundantReplicas() {
        long beId = 10001L;
        long dbId = 10002L;
        long tblId = 10003L;
        long partitionId = 10004L;
        long physicalPartitionId = 10004L;
        long indexId = 10005L;
        long tabletId = 10006L;
        long replicaId = 10007L;

        Database db = new Database(dbId, "db");
        OlapTable table = new OlapTable(tblId, "table", null, null, null, null);
        Replica replica = new Replica(replicaId, beId, 0, Replica.ReplicaState.NORMAL);
        LocalTablet tablet = new LocalTablet(tabletId, Lists.newArrayList(replica));
        MaterializedIndex index = new MaterializedIndex(indexId);
        index.addTablet(tablet, new TabletMeta(dbId, tblId, physicalPartitionId, indexId, TStorageMedium.HDD));
        PhysicalPartition physicalPartition = new PhysicalPartition(physicalPartitionId, partitionId, index);

        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
                minTimes = 0;
                result = db;
                globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tblId);
                minTimes = 0;
                result = table;
                globalStateMgr.getLocalMetastore().getPhysicalPartitionIncludeRecycleBin(table, physicalPartitionId);
                minTimes = 0;
                result = physicalPartition;
            }
        };

        TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbId, tblId, partitionId, indexId, tabletId,
                System.currentTimeMillis());
        ctx.setTablet(tablet);
        ctx.setTabletStatus(LocalTablet.TabletHealthStatus.COLOCATE_REDUNDANT);
        ctx.setColocateGroupBackendIds(Sets.newHashSet(beId));

        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());
        ExceptionChecker.expectThrowsWithMsg(SchedException.class,
                "unable to delete any colocate redundant replicas. replicas: 10001:-1/-1/-1/0:NORMAL:NIL,, backend set: [10001]",
                () -> Deencapsulation.invoke(tabletScheduler, "handleColocateRedundant", ctx));
    }

    @Test
    public void testResetDecommStatForSingleReplicaTabletWithNullTablet() {
        long tabletId = 10006L;
        long replicaId = 10007L;
        long beId = 10001L;

        // Create a replica with DECOMMISSION state
        Replica decommissionedReplica = new Replica(replicaId, beId, -1, Replica.ReplicaState.DECOMMISSION);
        List<Replica> replicas = Lists.newArrayList(decommissionedReplica);

        // Create a TabletSchedCtx but don't set the tablet (getTablet() will return null)
        TabletSchedCtx ctx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                10002L, 10003L, 10004L, 10005L, tabletId, System.currentTimeMillis());
        ctx.setDecommissionedReplica(decommissionedReplica);

        TabletScheduler tabletScheduler = new TabletScheduler(new TabletSchedulerStat());

        new MockUp<GlobalStateMgr>() {
            @Mock
            public TabletScheduler getTabletScheduler() {
                return tabletScheduler;
            }
        };

        // Add the context to scheduler so getTabletSchedCtx can find it
        Deencapsulation.invoke(tabletScheduler, "addToPendingTablets", ctx);

        // This should not throw NullPointerException even though ctx.getTablet() returns null
        TabletScheduler.resetDecommStatForSingleReplicaTabletUnlocked(tabletId, replicas);
    }

    @Test
    public void testOnStoppedClearsInternalState() {
        TabletScheduler scheduler = new TabletScheduler(new TabletSchedulerStat());

        TabletSchedCtx pending = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, 4L, 100L, System.currentTimeMillis(), systemInfoService);
        pending.setOrigPriority(TabletSchedCtx.Priority.LOW);
        Deencapsulation.invoke(scheduler, "addToPendingTablets", pending);

        // Populate remaining collections that onStopped() must reset.
        java.util.Set<Long> allIds = Deencapsulation.getField(scheduler, "allTabletIds");
        allIds.add(100L);
        Map<Long, TabletSchedCtx> running = Deencapsulation.getField(scheduler, "runningTablets");
        running.put(200L, pending);
        Queue<TabletSchedCtx> history = Deencapsulation.getField(scheduler, "schedHistory");
        history.add(pending);
        Map<Long, TabletScheduler.PathSlot> slots = Deencapsulation.getField(scheduler, "backendsWorkingSlots");
        slots.put(10L, new TabletScheduler.PathSlot(Lists.newArrayList(0L), 1));
        scheduler.setClusterLoadStatistic(new ClusterLoadStatistic(systemInfoService, tabletInvertedIndex));
        Deencapsulation.setField(scheduler, "lastStatUpdateTime", 123L);
        Deencapsulation.setField(scheduler, "lastClusterLoadLoggingTime", 456L);
        Deencapsulation.setField(scheduler, "lastSlotAdjustTime", 789L);
        Deencapsulation.setField(scheduler, "currentSlotPerPathConfig", 7);
        ((java.util.concurrent.atomic.AtomicBoolean) Deencapsulation.getField(scheduler, "forceCleanSchedQ")).set(true);

        Assertions.assertEquals(1, scheduler.getTotalNum());
        Assertions.assertEquals(1, scheduler.getRunningNum());
        Assertions.assertEquals(1, scheduler.getHistoryNum());
        Assertions.assertNotNull(scheduler.getClusterLoadStatistic());

        Deencapsulation.invoke(scheduler, "onStopped");

        Assertions.assertEquals(0, scheduler.getTotalNum());
        Assertions.assertEquals(0, scheduler.getRunningNum());
        Assertions.assertEquals(0, scheduler.getHistoryNum());
        Assertions.assertNull(scheduler.getClusterLoadStatistic());
        Map<Long, TabletScheduler.PathSlot> afterSlots = Deencapsulation.getField(scheduler, "backendsWorkingSlots");
        Assertions.assertTrue(afterSlots.isEmpty(), "backendsWorkingSlots must be cleared");
        PriorityQueue<TabletSchedCtx> afterPending = Deencapsulation.getField(scheduler, "pendingTablets");
        Assertions.assertTrue(afterPending.isEmpty(), "pendingTablets must be cleared");
        Assertions.assertEquals(0L, (long) Deencapsulation.getField(scheduler, "lastStatUpdateTime"));
        Assertions.assertEquals(0L, (long) Deencapsulation.getField(scheduler, "lastClusterLoadLoggingTime"));
        Assertions.assertEquals(0L, (long) Deencapsulation.getField(scheduler, "lastSlotAdjustTime"));
        Assertions.assertEquals(0, (int) Deencapsulation.getField(scheduler, "currentSlotPerPathConfig"));
        Assertions.assertFalse(
                ((java.util.concurrent.atomic.AtomicBoolean) Deencapsulation.getField(scheduler, "forceCleanSchedQ"))
                        .get(),
                "forceCleanSchedQ must reset to false");
    }

    // Combined coverage for the error-state replica handling in the generic redundant-replica path:
    //   * deleteErrorStateReplica drops one error-state replica and bumps the rate limiter,
    //   * the rate limiter then throttles the next would-be drop,
    //   * the safety guard preserves an error-state replica when no healthy peer remains (RF=1).
    @Test
    public void testDeleteErrorStateReplica() throws Exception {
        long beId1 = 10001L;
        long beId2 = 10002L;
        long dbIdMulti = 20001L;
        long tblIdMulti = 20002L;
        long partIdMulti = 20003L;
        long physPartIdMulti = 20003L;
        long indexIdMulti = 20004L;

        long dbIdSingle = 21001L;
        long tblIdSingle = 21002L;
        long partIdSingle = 21003L;
        long physPartIdSingle = 21003L;
        long indexIdSingle = 21004L;
        long singleTabletId = 21005L;

        // Use a low rate so the second deletion in the multi-replica scenario is throttled.
        double savedRate = Config.tablet_sched_delete_error_state_replica_permits_per_second;
        Config.tablet_sched_delete_error_state_replica_permits_per_second = 0.1;

        // Shared backends - registered so deleteBackendDropped()/Unavailable() don't fire.
        Backend be1 = new Backend(beId1, "192.168.0.1", 9030);
        be1.setAlive(true);
        systemInfoService.addBackend(be1);
        Backend be2 = new Backend(beId2, "192.168.0.2", 9030);
        be2.setAlive(true);
        systemInfoService.addBackend(be2);

        // Multi-replica fixture (two error-state tablets sharing the same index).
        MaterializedIndex indexMulti = new MaterializedIndex(indexIdMulti);
        Replica normalA = new Replica(30001L, beId1, 0, Replica.ReplicaState.NORMAL);
        Replica errorA = new Replica(30002L, beId2, 0, Replica.ReplicaState.NORMAL);
        errorA.setIsErrorState(true);
        LocalTablet tabletA = new LocalTablet(20005L, Lists.newArrayList(normalA, errorA));
        indexMulti.addTablet(tabletA, new TabletMeta(dbIdMulti, tblIdMulti, physPartIdMulti, indexIdMulti, TStorageMedium.HDD));

        Replica normalB = new Replica(30003L, beId1, 0, Replica.ReplicaState.NORMAL);
        Replica errorB = new Replica(30004L, beId2, 0, Replica.ReplicaState.NORMAL);
        errorB.setIsErrorState(true);
        LocalTablet tabletB = new LocalTablet(20006L, Lists.newArrayList(normalB, errorB));
        indexMulti.addTablet(tabletB, new TabletMeta(dbIdMulti, tblIdMulti, physPartIdMulti, indexIdMulti, TStorageMedium.HDD));

        PhysicalPartition partMulti = new PhysicalPartition(physPartIdMulti, partIdMulti, indexMulti);
        Database dbMulti = new Database(dbIdMulti, "db_multi");
        OlapTable tableMulti = new OlapTable(tblIdMulti, "table_multi", null, null, null, null);

        // RF=1 fixture - safety guard scenario.
        MaterializedIndex indexSingle = new MaterializedIndex(indexIdSingle);
        Replica errorSolo = new Replica(31001L, beId1, 0, Replica.ReplicaState.NORMAL);
        errorSolo.setIsErrorState(true);
        LocalTablet tabletSolo = new LocalTablet(singleTabletId, Lists.newArrayList(errorSolo));
        indexSingle.addTablet(tabletSolo,
                new TabletMeta(dbIdSingle, tblIdSingle, physPartIdSingle, indexIdSingle, TStorageMedium.HDD));
        PhysicalPartition partSingle = new PhysicalPartition(physPartIdSingle, partIdSingle, indexSingle);
        Database dbSingle = new Database(dbIdSingle, "db_single");
        OlapTable tableSingle = new OlapTable(tblIdSingle, "table_single", null, null, null, null);

        // One mock setup serves both fixtures - minTimes=0 means unused entries are harmless.
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbIdMulti);
                minTimes = 0;
                result = dbMulti;
                globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(dbMulti, tblIdMulti);
                minTimes = 0;
                result = tableMulti;
                globalStateMgr.getLocalMetastore().getPhysicalPartitionIncludeRecycleBin(tableMulti, physPartIdMulti);
                minTimes = 0;
                result = partMulti;

                globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbIdSingle);
                minTimes = 0;
                result = dbSingle;
                globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(dbSingle, tblIdSingle);
                minTimes = 0;
                result = tableSingle;
                globalStateMgr.getLocalMetastore().getPhysicalPartitionIncludeRecycleBin(tableSingle, physPartIdSingle);
                minTimes = 0;
                result = partSingle;
            }
        };
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {
                // no-op for test
            }
        };

        try {
            TabletScheduler scheduler = new TabletScheduler(new TabletSchedulerStat());

            // Scenario A: first error-state tablet -> success (consumes the only token).
            TabletSchedCtx ctxA = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbIdMulti, tblIdMulti, partIdMulti,
                    indexIdMulti, tabletA.getId(), System.currentTimeMillis());
            ctxA.setTablet(tabletA);
            SchedException exA = Assertions.assertThrows(SchedException.class,
                    () -> scheduler.handleTabletByTypeAndStatus(
                            LocalTablet.TabletHealthStatus.FORCE_REDUNDANT, ctxA, new AgentBatchTask()));
            Assertions.assertEquals(SchedException.Status.FINISHED, exA.getStatus());
            Assertions.assertTrue(exA.getMessage().contains("redundant replica is deleted"));

            // Scenario B: second error-state tablet -> throttled, no other deleter matches,
            // so handleRedundantReplica falls through to UNRECOVERABLE.
            TabletSchedCtx ctxB = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbIdMulti, tblIdMulti, partIdMulti,
                    indexIdMulti, tabletB.getId(), System.currentTimeMillis());
            ctxB.setTablet(tabletB);
            SchedException exB = Assertions.assertThrows(SchedException.class,
                    () -> scheduler.handleTabletByTypeAndStatus(
                            LocalTablet.TabletHealthStatus.FORCE_REDUNDANT, ctxB, new AgentBatchTask()));
            Assertions.assertEquals(SchedException.Status.UNRECOVERABLE, exB.getStatus());
            Assertions.assertTrue(exB.getMessage().contains("unable to delete any redundant replicas"));

            // Scenario C: RF=1 -> safety guard fires before the rate limiter, the error-state
            // replica is preserved, and handleRedundantReplica falls through to UNRECOVERABLE.
            TabletSchedCtx ctxC = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbIdSingle, tblIdSingle, partIdSingle,
                    indexIdSingle, singleTabletId, System.currentTimeMillis());
            ctxC.setTablet(tabletSolo);
            SchedException exC = Assertions.assertThrows(SchedException.class,
                    () -> scheduler.handleTabletByTypeAndStatus(
                            LocalTablet.TabletHealthStatus.FORCE_REDUNDANT, ctxC, new AgentBatchTask()));
            Assertions.assertEquals(SchedException.Status.UNRECOVERABLE, exC.getStatus());
            Assertions.assertTrue(exC.getMessage().contains("unable to delete any redundant replicas"));
            Assertions.assertTrue(errorSolo.isErrorState(), "error-state replica should still be present");
        } finally {
            Config.tablet_sched_delete_error_state_replica_permits_per_second = savedRate;
        }
    }

    // Combined coverage for handleColocateRedundant: the bad-replica branch always drops,
    // the error-state-replica branch drops once and is then throttled by the same rate
    // limiter as the generic redundant path so colocate cannot bypass the throttle.
    @Test
    public void testHandleColocateRedundantDropsAbnormalReplicas() throws Exception {
        long beId1 = 12001L;
        long beId2 = 12002L;
        long dbId = 22001L;
        long tblId = 22002L;
        long partitionId = 22003L;
        long physicalPartitionId = 22003L;
        long indexId = 22004L;

        // Low rate so the second error-state colocate drop in this test is throttled.
        double savedRate = Config.tablet_sched_delete_error_state_replica_permits_per_second;
        Config.tablet_sched_delete_error_state_replica_permits_per_second = 0.1;

        Backend be1 = new Backend(beId1, "10.2.0.1", 9030);
        be1.setAlive(true);
        systemInfoService.addBackend(be1);
        Backend be2 = new Backend(beId2, "10.2.0.2", 9030);
        be2.setAlive(true);
        systemInfoService.addBackend(be2);

        // Three tablets share the same index/partition/db. Order in each replicas list
        // matters: handleColocateRedundant acts on the first matching replica.
        MaterializedIndex index = new MaterializedIndex(indexId);

        Replica badReplica = new Replica(32001L, beId1, 0, Replica.ReplicaState.NORMAL);
        badReplica.setBad(true);
        Replica healthyForBad = new Replica(32002L, beId2, 0, Replica.ReplicaState.NORMAL);
        LocalTablet tabletBad = new LocalTablet(22005L, Lists.newArrayList(badReplica, healthyForBad));
        index.addTablet(tabletBad, new TabletMeta(dbId, tblId, physicalPartitionId, indexId, TStorageMedium.HDD));

        Replica errorReplicaA = new Replica(33001L, beId1, 0, Replica.ReplicaState.NORMAL);
        errorReplicaA.setIsErrorState(true);
        Replica healthyForErrA = new Replica(33002L, beId2, 0, Replica.ReplicaState.NORMAL);
        LocalTablet tabletErrA = new LocalTablet(22006L, Lists.newArrayList(errorReplicaA, healthyForErrA));
        index.addTablet(tabletErrA, new TabletMeta(dbId, tblId, physicalPartitionId, indexId, TStorageMedium.HDD));

        Replica errorReplicaB = new Replica(33003L, beId1, 0, Replica.ReplicaState.NORMAL);
        errorReplicaB.setIsErrorState(true);
        Replica healthyForErrB = new Replica(33004L, beId2, 0, Replica.ReplicaState.NORMAL);
        LocalTablet tabletErrB = new LocalTablet(22007L, Lists.newArrayList(errorReplicaB, healthyForErrB));
        index.addTablet(tabletErrB, new TabletMeta(dbId, tblId, physicalPartitionId, indexId, TStorageMedium.HDD));

        PhysicalPartition physicalPartition = new PhysicalPartition(physicalPartitionId, partitionId, index);
        Database db = new Database(dbId, "db_colocate");
        OlapTable table = new OlapTable(tblId, "table_colocate", null, null, null, null);

        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
                minTimes = 0;
                result = db;
                globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tblId);
                minTimes = 0;
                result = table;
                globalStateMgr.getLocalMetastore().getPhysicalPartitionIncludeRecycleBin(table, physicalPartitionId);
                minTimes = 0;
                result = physicalPartition;
            }
        };
        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {
                // no-op for test
            }
        };

        try {
            TabletScheduler scheduler = new TabletScheduler(new TabletSchedulerStat());
            Set<Long> backendSet = Sets.newHashSet(beId1, beId2);

            // Scenario A: bad-replica branch drops unconditionally (no rate limiter on bad path).
            TabletSchedCtx ctxBad = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbId, tblId, partitionId, indexId,
                    tabletBad.getId(), System.currentTimeMillis());
            ctxBad.setTablet(tabletBad);
            ctxBad.setColocateGroupBackendIds(backendSet);
            SchedException exBad = Assertions.assertThrows(SchedException.class,
                    () -> scheduler.handleTabletByTypeAndStatus(
                            LocalTablet.TabletHealthStatus.COLOCATE_REDUNDANT, ctxBad, new AgentBatchTask()));
            Assertions.assertEquals(SchedException.Status.FINISHED, exBad.getStatus());
            Assertions.assertTrue(exBad.getMessage().contains("colocate redundant replica is deleted"));

            // Scenario B: error-state branch drops once and consumes the only available token.
            TabletSchedCtx ctxErrA = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbId, tblId, partitionId, indexId,
                    tabletErrA.getId(), System.currentTimeMillis());
            ctxErrA.setTablet(tabletErrA);
            ctxErrA.setColocateGroupBackendIds(backendSet);
            SchedException exErrA = Assertions.assertThrows(SchedException.class,
                    () -> scheduler.handleTabletByTypeAndStatus(
                            LocalTablet.TabletHealthStatus.COLOCATE_REDUNDANT, ctxErrA, new AgentBatchTask()));
            Assertions.assertEquals(SchedException.Status.FINISHED, exErrA.getStatus());
            Assertions.assertTrue(exErrA.getMessage().contains("colocate redundant replica is deleted"));

            // Scenario C: a second error-state colocate tablet is throttled, so the loop falls
            // through to UNRECOVERABLE and the error-state replica is preserved for the next round.
            TabletSchedCtx ctxErrB = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, dbId, tblId, partitionId, indexId,
                    tabletErrB.getId(), System.currentTimeMillis());
            ctxErrB.setTablet(tabletErrB);
            ctxErrB.setColocateGroupBackendIds(backendSet);
            SchedException exErrB = Assertions.assertThrows(SchedException.class,
                    () -> scheduler.handleTabletByTypeAndStatus(
                            LocalTablet.TabletHealthStatus.COLOCATE_REDUNDANT, ctxErrB, new AgentBatchTask()));
            Assertions.assertEquals(SchedException.Status.UNRECOVERABLE, exErrB.getStatus());
            Assertions.assertTrue(exErrB.getMessage().contains("unable to delete any colocate redundant replicas"));
            Assertions.assertTrue(errorReplicaB.isErrorState(),
                    "throttled error-state colocate replica should still be present");
        } finally {
            Config.tablet_sched_delete_error_state_replica_permits_per_second = savedRate;
        }
    }

    // Reflection-based coverage for the small private helpers introduced by this feature.
    // Bundled together because they share no heavy fixture and do not need the full
    // db/table/partition mock chain.
    @Test
    public void testTabletSchedulerErrorStateHelpers() throws Exception {
        long aliveBe = 30000L;
        long deadBe = 30001L;
        long missingBe = 30002L; // intentionally not registered with systemInfoService
        Backend alive = new Backend(aliveBe, "10.1.0.1", 9030);
        alive.setAlive(true);
        systemInfoService.addBackend(alive);
        Backend dead = new Backend(deadBe, "10.1.0.2", 9030);
        dead.setAlive(false);
        systemInfoService.addBackend(dead);

        // ---- refreshErrorReplicaDeleteRateLimiter ----
        double savedRate = Config.tablet_sched_delete_error_state_replica_permits_per_second;
        try {
            Config.tablet_sched_delete_error_state_replica_permits_per_second = 5.0;
            TabletScheduler scheduler = new TabletScheduler(new TabletSchedulerStat());

            Field rlField = TabletScheduler.class.getDeclaredField("errorReplicaDeleteRateLimiter");
            rlField.setAccessible(true);
            RateLimiter limiter = (RateLimiter) rlField.get(scheduler);
            Assertions.assertEquals(5.0, limiter.getRate(), 1e-9);

            Method refresh = TabletScheduler.class.getDeclaredMethod("refreshErrorReplicaDeleteRateLimiter");
            refresh.setAccessible(true);

            // Same config -> no-op (false branch of the if predicate).
            refresh.invoke(scheduler);
            Assertions.assertEquals(5.0, limiter.getRate(), 1e-9);

            // Changed config -> setRate + LOG.info path.
            Config.tablet_sched_delete_error_state_replica_permits_per_second = 25.0;
            refresh.invoke(scheduler);
            Assertions.assertEquals(25.0, limiter.getRate(), 1e-9);

            // Zero or negative -> ignored (the >0 guard).
            Config.tablet_sched_delete_error_state_replica_permits_per_second = 0.0;
            refresh.invoke(scheduler);
            Assertions.assertEquals(25.0, limiter.getRate(), 1e-9);

            // ---- isDataLost ----
            Method isDataLost = TabletScheduler.class.getDeclaredMethod("isDataLost", List.class);
            isDataLost.setAccessible(true);

            Replica healthy = new Replica(60000L, aliveBe, 0, Replica.ReplicaState.NORMAL);
            Assertions.assertFalse((Boolean) isDataLost.invoke(scheduler, Lists.newArrayList(healthy)));

            Replica bad = new Replica(60001L, aliveBe, 0, Replica.ReplicaState.NORMAL);
            bad.setBad(true);
            Assertions.assertTrue((Boolean) isDataLost.invoke(scheduler, Lists.newArrayList(bad)));

            Replica errored = new Replica(60002L, aliveBe, 0, Replica.ReplicaState.NORMAL);
            errored.setIsErrorState(true);
            Assertions.assertTrue((Boolean) isDataLost.invoke(scheduler, Lists.newArrayList(errored)));

            // Mixed: only some lost -> not data-lost.
            Assertions.assertFalse((Boolean) isDataLost.invoke(scheduler, Lists.newArrayList(healthy, errored)));

            // ---- hasAnotherHealthyReplica ----
            Method hasAnother = TabletScheduler.class.getDeclaredMethod(
                    "hasAnotherHealthyReplica", List.class, Replica.class);
            hasAnother.setAccessible(true);

            Replica exclude = new Replica(50000L, aliveBe, 0, Replica.ReplicaState.NORMAL);

            // Each peer flavor below should be skipped, leaving no other healthy peer.
            Replica peerBad = new Replica(50001L, aliveBe, 0, Replica.ReplicaState.NORMAL);
            peerBad.setBad(true);
            Assertions.assertFalse((Boolean) hasAnother.invoke(null, Lists.newArrayList(exclude, peerBad), exclude));

            Replica peerErr = new Replica(50002L, aliveBe, 0, Replica.ReplicaState.NORMAL);
            peerErr.setIsErrorState(true);
            Assertions.assertFalse((Boolean) hasAnother.invoke(null, Lists.newArrayList(exclude, peerErr), exclude));

            Replica peerClone = new Replica(50003L, aliveBe, 0, Replica.ReplicaState.CLONE);
            Assertions.assertFalse((Boolean) hasAnother.invoke(null, Lists.newArrayList(exclude, peerClone), exclude));

            Replica peerDeadBe = new Replica(50004L, deadBe, 0, Replica.ReplicaState.NORMAL);
            Assertions.assertFalse((Boolean) hasAnother.invoke(null, Lists.newArrayList(exclude, peerDeadBe), exclude));

            Replica peerMissingBe = new Replica(50005L, missingBe, 0, Replica.ReplicaState.NORMAL);
            Assertions.assertFalse((Boolean) hasAnother.invoke(null, Lists.newArrayList(exclude, peerMissingBe), exclude));

            // Healthy peer present -> true.
            Replica peerOk = new Replica(50006L, aliveBe, 0, Replica.ReplicaState.NORMAL);
            Assertions.assertTrue((Boolean) hasAnother.invoke(null, Lists.newArrayList(exclude, peerOk), exclude));
        } finally {
            Config.tablet_sched_delete_error_state_replica_permits_per_second = savedRate;
        }
    }

    @Test
    public void testResetDecommStatForSingleReplicaTabletWithNullTabletScheduler() {
        long tabletId = 10006L;
        long replicaId = 10007L;
        long beId = 10001L;

        // Create a replica with DECOMMISSION state
        Replica decommissionedReplica = new Replica(replicaId, beId, -1, Replica.ReplicaState.DECOMMISSION);
        List<Replica> replicas = Lists.newArrayList(decommissionedReplica);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public TabletScheduler getTabletScheduler() {
                return null;
            }
        };

        // This should not throw NullPointerException even though getTabletScheduler() returns null
        TabletScheduler.resetDecommStatForSingleReplicaTabletUnlocked(tabletId, replicas);

        // If we reach here without exception, the test passes
        Assertions.assertTrue(true);
    }
}
