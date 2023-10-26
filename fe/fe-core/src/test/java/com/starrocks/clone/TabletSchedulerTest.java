// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.clone;

import com.google.common.collect.Maps;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDisk;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.lang3.tuple.Triple;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.catalog.KeysType.DUP_KEYS;

public class TabletSchedulerTest {
    @Mocked
    GlobalStateMgr globalStateMgr;

<<<<<<< HEAD
    @Before
    public void setup() throws Exception {
=======
    SystemInfoService systemInfoService;
    TabletInvertedIndex tabletInvertedIndex;
    TabletSchedulerStat tabletSchedulerStat;
    FakeEditLog fakeEditLog;
    @Before
    public void setup() throws Exception {
        systemInfoService = new SystemInfoService();
        tabletInvertedIndex = new TabletInvertedIndex();
        tabletSchedulerStat = new TabletSchedulerStat();
        fakeEditLog = new FakeEditLog();

>>>>>>> d556a2d2bd ([BugFix] Fix FE crash bug where recover_with_empty_tablet is configured to true and there are single replica tables (#33071))
        new Expectations() {
            {
                globalStateMgr.getColocateTableIndex();
                minTimes = 0;
                result = new ColocateTableIndex();
            }
        };
    }

    SystemInfoService systemInfoService = new SystemInfoService();
    TabletInvertedIndex tabletInvertedIndex = new TabletInvertedIndex();
    TabletSchedulerStat tabletSchedulerStat = new TabletSchedulerStat();
    @Test
    public void testSubmitBatchTaskIfNotExpired() {
        Database badDb = new Database(1, "mal");
        Database goodDB = new Database(2, "bueno");
        Table badTable = new Table(3, "mal", Table.TableType.OLAP, new ArrayList<>());
        Table goodTable = new Table(4, "bueno", Table.TableType.OLAP, new ArrayList<>());
        Partition badPartition = new Partition(5, "mal", null, null);
        Partition goodPartition = new Partition(6, "bueno", null, null);

        long now = System.currentTimeMillis();
        CatalogRecycleBin recycleBin = new CatalogRecycleBin();
        recycleBin.recycleDatabase(badDb, new HashSet<>());
        recycleBin.recycleTable(goodDB.getId(), badTable);
        recycleBin.recyclePartition(goodDB.getId(), goodTable.getId(), badPartition,
                null, new DataProperty(TStorageMedium.HDD), (short) 2, false, null, false);

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
                    triple.getRight().getId(),
                    1,
                    1,
                    System.currentTimeMillis(),
                    systemInfoService));
        }

        TabletScheduler tabletScheduler = new TabletScheduler(globalStateMgr,
                systemInfoService, tabletInvertedIndex, tabletSchedulerStat);

        long almostExpireTime = now + (Config.catalog_trash_expire_second - 1) * 1000L;
        for (int i = 0; i != allCtxs.size(); ++ i) {
            Assert.assertFalse(tabletScheduler.checkIfTabletExpired(allCtxs.get(i), recycleBin, almostExpireTime));
        }

        long expireTime = now + (Config.catalog_trash_expire_second + 600) * 1000L;
        for (int i = 0; i != allCtxs.size() - 1; ++ i) {
            Assert.assertTrue(tabletScheduler.checkIfTabletExpired(allCtxs.get(i), recycleBin, expireTime));
        }
        // only the last survive
        Assert.assertFalse(tabletScheduler.checkIfTabletExpired(allCtxs.get(3), recycleBin, expireTime));
    }

    @Test
    public void testPendingAddTabletCtx() throws InterruptedException {
        int oldVal = Config.tablet_sched_max_scheduling_tablets;
        Config.tablet_sched_max_scheduling_tablets = 8;

        TabletScheduler tabletScheduler = new TabletScheduler(globalStateMgr,
                systemInfoService, tabletInvertedIndex, tabletSchedulerStat);
        Database goodDB = new Database(2, "bueno");
        Table goodTable = new Table(4, "bueno", Table.TableType.OLAP, new ArrayList<>());
        Partition goodPartition = new Partition(6, "bueno", null, null);

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
                tabletSchedCtxList.get(i).setOrigPriority(TabletSchedCtx.Priority.NORMAL);
                try {
                    goodDB.readLock();
                    tabletScheduler.blockingAddTabletCtxToScheduler(goodDB, tabletSchedCtxList.get(i), false);
                } finally {
                    goodDB.readUnlock();
                }
            }
        }, "testAddCtx").start();

        Thread.sleep(2000);
        tabletScheduler.removeOneFromPendingQ();
        Thread.sleep(1000);
        Assert.assertEquals(9, tabletScheduler.getPendingTabletsInfo(100).size());

        Config.tablet_sched_max_scheduling_tablets = oldVal;
    }

    private void updateSlotWithNewConfig(int newSlotPerPath, Method updateWorkingSlotsMethod,
                                         TabletScheduler tabletScheduler)
            throws InvocationTargetException, IllegalAccessException {
        Config.tablet_sched_slot_num_per_path = newSlotPerPath;
        updateWorkingSlotsMethod.invoke(tabletScheduler, null);
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
        be1.setIsAlive(new AtomicBoolean(true));
        be1.updateDisks(backendDisks1);
        systemInfoService.addBackend(be1);

        TDisk td21 = new TDisk("/path21", 1L, 2L, true);
        td21.setPath_hash(21);
        TDisk td22 = new TDisk("/path22", 1L, 2L, true);
        td22.setPath_hash(22);
        Map<String, TDisk> backendDisks2 = new HashMap<>();
        backendDisks2.put("/path21", td21);
        backendDisks2.put("/path22", td22);
        Backend be2 = new Backend(2, "192.168.0.2", 9030);
        be2.updateDisks(backendDisks2);
        be2.setIsAlive(new AtomicBoolean(true));
        systemInfoService.addBackend(be2);

        TabletScheduler tabletScheduler =
                new TabletScheduler(globalStateMgr, systemInfoService, tabletInvertedIndex, tabletSchedulerStat);
        Method m = TabletScheduler.class.getDeclaredMethod("updateWorkingSlots", null);
        m.setAccessible(true);
        m.invoke(tabletScheduler, null);
        Map<Long, TabletScheduler.PathSlot> bslots = tabletScheduler.getBackendsWorkingSlots();
        Assert.assertEquals(Config.tablet_sched_slot_num_per_path, bslots.get(1L).peekSlot(11));
        Assert.assertEquals(Config.tablet_sched_slot_num_per_path, bslots.get(2L).peekSlot(22));
        long result = takeSlotNTimes(Config.tablet_sched_slot_num_per_path, bslots.get(1L), 11L);
        Assert.assertEquals(11, result);
        result = takeSlotNTimes(1, bslots.get(1L), 11L);
        Assert.assertEquals(-1, result);
        freeSlotNTimes(Config.tablet_sched_slot_num_per_path, bslots.get(1L), 11L);
        Assert.assertEquals(Config.tablet_sched_slot_num_per_path, bslots.get(1L).getSlotTotal(11));

        updateSlotWithNewConfig(128, m, tabletScheduler); // test max slot
        Assert.assertEquals(TabletScheduler.MAX_SLOT_PER_PATH, bslots.get(1L).getSlotTotal(11));
        Assert.assertEquals(TabletScheduler.MAX_SLOT_PER_PATH, bslots.get(1L).peekSlot(11));

        updateSlotWithNewConfig(0, m, tabletScheduler); // test min slot
        Assert.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(1L).peekSlot(11));
        Assert.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(2L).peekSlot(22));
        takeSlotNTimes(10, bslots.get(1L), 11L); // not enough, can only get 2 free slot
        takeSlotNTimes(10, bslots.get(2L), 21L); // not enough, can only get 2 free slot
        Assert.assertEquals(0, bslots.get(1L).peekSlot(11));
        Assert.assertEquals(0, bslots.get(2L).peekSlot(21));
        Assert.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(1L).getSlotTotal(11));

        updateSlotWithNewConfig(2, m, tabletScheduler);
        Assert.assertEquals(0, bslots.get(1L).peekSlot(11));
        Assert.assertEquals(TabletScheduler.MIN_SLOT_PER_PATH, bslots.get(1L).peekSlot(12));

        updateSlotWithNewConfig(4, m, tabletScheduler);
        Assert.assertEquals(2, bslots.get(2L).peekSlot(21));
        Assert.assertEquals(4, bslots.get(2L).peekSlot(22));
        Assert.assertEquals(4, bslots.get(1L).getSlotTotal(11));

        takeSlotNTimes(5, bslots.get(1L), 11); // not enough, can only get 2 free slot
        updateSlotWithNewConfig(2, m, tabletScheduler); // decrease total slot
        // this is normal because slot taken haven't return
        Assert.assertEquals(-2, bslots.get(1L).peekSlot(11));
        Assert.assertEquals(2, bslots.get(1L).peekSlot(12));
        Assert.assertEquals(0, bslots.get(2L).peekSlot(21));

        freeSlotNTimes(2, bslots.get(1L), 11L);
        Assert.assertEquals(0, bslots.get(1L).peekSlot(11));

        freeSlotNTimes(2, bslots.get(1L), 11L);
        Assert.assertEquals(bslots.get(1L).peekSlot(11), bslots.get(1L).getSlotTotal(11));
    }

    @Test
    public void testGetTabletsNumInScheduleForEachCG() {
        TabletScheduler tabletScheduler =
                new TabletScheduler(globalStateMgr, systemInfoService, tabletInvertedIndex, tabletSchedulerStat);
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
                ctx.setTabletStatus(LocalTablet.TabletStatus.VERSION_INCOMPLETE);
            }
            Deencapsulation.invoke(tabletScheduler, "addToRunningTablets", ctx);
        });

        Map<ColocateTableIndex.GroupId, Long> result = tabletScheduler.getTabletsNumInScheduleForEachCG();
        Assert.assertEquals(Optional.of(3L).get(),
                result.get(new ColocateTableIndex.GroupId(200L, 300L)));
        Assert.assertEquals(Optional.of(2L).get(),
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
        Pair<LocalTablet.TabletStatus, TabletSchedCtx.Priority> result = localTablet.getHealthStatusWithPriority(
                systemInfoService, 1, 3, Arrays.asList(1001L, 1002L, 1003L));
        System.out.println(result);

        Assert.assertEquals(LocalTablet.TabletStatus.FORCE_REDUNDANT, result.first);

        Config.recover_with_empty_tablet = false;
    }
<<<<<<< HEAD
}
=======

    @Test
    public void testFinishCreateReplicaTask() {
        long beId = 10001L;
        long dbId = 10002L;
        long tblId = 10003L;
        long partitionId = 10004L;
        long indexId = 10005L;
        long tabletId = 10006L;
        long replicaId = 10007L;
        short count = 1;
        TabletMeta tabletMeta = new TabletMeta(dbId, tblId, partitionId, indexId, -1, TStorageMedium.HDD);
        CreateReplicaTask createReplicaTask = new CreateReplicaTask(beId, dbId, tblId, partitionId, indexId, tabletId, count,
                -1, -1L,
                DUP_KEYS,
                TStorageType.COLUMN,
                TStorageMedium.HDD, null, null, 0.0, null,
                null,
                false,
                false,
                1,
                TTabletType.TABLET_TYPE_DISK,
                TCompressionType.LZ4_FRAME);

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
        Assert.assertEquals(Replica.ReplicaState.RECOVER, replica.getState());

        // failure test: request not ok
        tabletScheduler.addToRunningTablets(ctx);
        status.setStatus_code(TStatusCode.CANCELLED);
        status.setError_msgs(Lists.newArrayList("canceled"));
        tabletScheduler.finishCreateReplicaTask(createReplicaTask, request);
        Assert.assertEquals(Replica.ReplicaState.RECOVER, replica.getState());

        // success
        tabletScheduler.addToRunningTablets(ctx);
        status.setStatus_code(TStatusCode.OK);
        tabletScheduler.finishCreateReplicaTask(createReplicaTask, request);
        Assert.assertEquals(Replica.ReplicaState.NORMAL, replica.getState());
    }
}
>>>>>>> d556a2d2bd ([BugFix] Fix FE crash bug where recover_with_empty_tablet is configured to true and there are single replica tables (#33071))
