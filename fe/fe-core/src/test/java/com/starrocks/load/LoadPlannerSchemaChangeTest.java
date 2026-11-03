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

package com.starrocks.load;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockHolder;
import com.starrocks.common.util.concurrent.lock.LockInfo;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.StreamLoadScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TOlapTableIndexSchema;
import com.starrocks.thrift.TOlapTableSchemaParam;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A transaction stream load plans on the FE ({@code StreamLoadTask.executeTask -> LoadPlanner.planUnderTableLock ->
 * plan}): the plan builds the load's slots from the table's columns first and the sink's schema param -- the index
 * columns and the schema id the BE writes with -- at the end. A fast schema change that commits in between hands the
 * BE a schema with a column no slot fills; on a table that is not a primary key table the memtable keeps that column
 * empty and the flush reads rows it does not have (BE SIGSEGV). The plan must read one version of the table schema.
 */
public class LoadPlannerSchemaChangeTest {
    private static final String DB_NAME = "test_load_schema_change";
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        new StarRocksAssert(connectContext).withDatabase(DB_NAME).useDatabase(DB_NAME)
                .withTable("CREATE TABLE t (k INT NOT NULL, v INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1', 'fast_schema_evolution' = 'true')");
    }

    @Test
    public void testFastAddColumnCannotCommitWhileTheLoadIsPlanned() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t");
        long schemaIdBefore = table.getIndexMetaByMetaId(table.getBaseIndexMetaId()).getSchemaId();

        // Pause the planner after it built the load's slots, before it plans the scan and the sink.
        AtomicReference<Thread> plannerThread = new AtomicReference<>();
        CountDownLatch plannerPaused = new CountDownLatch(1);
        CountDownLatch resumePlanner = new CountDownLatch(1);
        new MockUp<StreamLoadScanNode>() {
            @Mock
            public void init(Invocation inv, DescriptorTable descriptorTable) throws Exception {
                if (Thread.currentThread() == plannerThread.get()) {
                    plannerPaused.countDown();
                    resumePlanner.await(30, TimeUnit.SECONDS);
                }
                inv.proceed(descriptorTable);
            }
        };

        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(
                db.getId(), List.of(table.getId()), "label_schema_change",
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, 600);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            LoadPlanner planner = newStreamLoadPlanner(db, table, txnId);
            Future<?> planned = pool.submit(() -> {
                plannerThread.set(Thread.currentThread());
                try (var scope = connectContext.bindScope()) {
                    planner.planUnderTableLock();
                }
                return null;
            });
            Assertions.assertTrue(plannerPaused.await(30, TimeUnit.SECONDS));

            // A fast ADD COLUMN only edits the catalog, under the table WRITE lock, and returns at once.
            AtomicReference<Thread> alterThread = new AtomicReference<>();
            Future<?> altered = pool.submit(() -> {
                alterThread.set(Thread.currentThread());
                ConnectContext alterContext = UtFrameUtils.createDefaultCtx();
                new StarRocksAssert(alterContext).alterTable(
                        "ALTER TABLE " + DB_NAME + ".t ADD COLUMN c_new INT DEFAULT '0'");
                return null;
            });
            // Let the planner go on once the ALTER has either committed or queued for the table lock.
            long deadline = System.currentTimeMillis() + 30_000;
            while (!altered.isDone() && !isWaitingForLock(alterThread.get(), db.getId(), table.getId())) {
                Assertions.assertTrue(System.currentTimeMillis() < deadline, "the ALTER neither finished nor waited");
                Thread.sleep(10);
            }
            boolean alteredDuringPlan = altered.isDone();
            resumePlanner.countDown();
            planned.get(30, TimeUnit.SECONDS);
            // Held off until the plan was done, the ALTER still goes through.
            altered.get(30, TimeUnit.SECONDS);
            Assertions.assertNotNull(table.getColumn("c_new"));

            TOlapTableSchemaParam schema = findOlapTableSink(planner).toThrift().getOlap_table_sink().getSchema();
            Set<String> slotNames = schema.getSlot_descs().stream()
                    .map(TSlotDescriptor::getColName)
                    .collect(Collectors.toSet());
            for (TOlapTableIndexSchema index : schema.getIndexes()) {
                for (String column : index.getColumns()) {
                    Assertions.assertTrue(slotNames.contains(column), "column " + column + " of index " + index.getId()
                            + " (schema id " + index.getSchema_id() + ") has no slot among " + slotNames
                            + ": the load's slots and its sink schema come from different versions of the table");
                }
                Assertions.assertEquals(schemaIdBefore, index.getSchema_id());
            }
            Assertions.assertFalse(alteredDuringPlan, "a schema change must not commit while the load is planned");
        } finally {
            resumePlanner.countDown();
            pool.shutdownNow();
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(db.getId(), txnId, "test done");
        }
    }

    @Test
    public void testFailedPlanReleasesTheTableLock() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t");
        // A partition the table does not have fails the plan from inside, while it holds the table lock.
        LoadPlanner planner = newStreamLoadPlanner(db, table, 1L, new PartitionNames(false, List.of("p_missing")));
        LoadException e = Assertions.assertThrows(LoadException.class, planner::planUnderTableLock);
        Assertions.assertTrue(e.getMessage().contains("unknown partition p_missing"), e.getMessage());

        // Ask from another thread: the lock owner is the thread, so this one would be granted WRITE even over a READ
        // lock it leaked itself.
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> writerLocked = pool.submit(() -> {
                Locker locker = new Locker();
                if (!locker.tryLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE,
                        1, TimeUnit.SECONDS)) {
                    return false;
                }
                locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.WRITE);
                return true;
            });
            Assertions.assertTrue(writerLocked.get(30, TimeUnit.SECONDS), "a failed plan must release the table lock");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testTransactionStreamLoadPlansUnderTheTableLock() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t");
        // Note whether the plan runs under the table READ lock, then stop the load before it reaches a backend.
        AtomicInteger plans = new AtomicInteger();
        AtomicBoolean plannedUnderTableLock = new AtomicBoolean();
        new MockUp<LoadPlanner>() {
            @Mock
            public void plan() throws StarRocksException {
                plans.incrementAndGet();
                plannedUnderTableLock.set(GlobalStateMgr.getCurrentState().getLockManager()
                        .isOwnedByCurrentThread(table.getId(), LockType.READ));
                throw new StarRocksException("stopped after planning");
            }
        };

        StreamLoadTask task = new StreamLoadTask(GlobalStateMgr.getCurrentState().getNextId(), db, table,
                "label_txn_stream_load", "root", "127.0.0.1", 60_000L, 1, 0, System.currentTimeMillis(),
                WarehouseManager.DEFAULT_RESOURCE);
        StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                () -> task.unprotectedExecute(new DefaultHttpHeaders()));
        Assertions.assertTrue(e.getMessage().contains("stopped after planning"), e.getMessage());
        Assertions.assertEquals(1, plans.get());
        Assertions.assertTrue(plannedUnderTableLock.get(), "a transaction stream load must plan under the table lock");
    }

    private static boolean isWaitingForLock(Thread thread, long dbId, long tableId) {
        if (thread == null) {
            return false;
        }
        for (LockInfo lockInfo : GlobalStateMgr.getCurrentState().getLockManager().dumpLockManager()) {
            if (lockInfo.getRid() != dbId && lockInfo.getRid() != tableId) {
                continue;
            }
            for (LockHolder waiter : lockInfo.getWaiters()) {
                if (waiter.getLocker().getThreadId() == thread.getId()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static OlapTableSink findOlapTableSink(LoadPlanner planner) {
        for (PlanFragment fragment : planner.getFragments()) {
            if (fragment.getSink() instanceof OlapTableSink) {
                return (OlapTableSink) fragment.getSink();
            }
        }
        throw new AssertionError("the load plan has no OlapTableSink");
    }

    private static LoadPlanner newStreamLoadPlanner(Database db, OlapTable table, long txnId) {
        return newStreamLoadPlanner(db, table, txnId, null);
    }

    private static LoadPlanner newStreamLoadPlanner(Database db, OlapTable table, long txnId,
                                                    PartitionNames partitions) {
        StreamLoadInfo streamLoadInfo = new StreamLoadInfo(UUIDUtil.genTUniqueId(), txnId, TFileType.FILE_STREAM,
                TFileFormatType.FORMAT_CSV_PLAIN);
        if (partitions != null) {
            Deencapsulation.setField(streamLoadInfo, "partitions", partitions);
        }
        return new LoadPlanner(1L, UUIDUtil.genTUniqueId(), txnId, db.getId(), db.getFullName(),
                table, false, "UTC", false, connectContext, null, 0L, 0L, false, 1, null,
                streamLoadInfo, "label_schema_change", 60L);
    }
}
