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

package com.starrocks.listener;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.streamload.StreamLoadTxnCommitAttachment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadJobStatsListenerTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void streamLoadProtectsPartitionRowCount() throws Exception {
        verifyPartitionRowCountLock(false);
    }

    @Test
    public void brokerLoadReusesPublishWriteLock() throws Exception {
        verifyPartitionRowCountLock(true);
    }

    private void verifyPartitionRowCountLock(boolean brokerLoad) throws Exception {
        String dbName = "test_statistics_listener";
        String tableName = brokerLoad ? "broker_load" : "stream_load";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("create table " + tableName + " (c1 int not null, c2 int not null) " +
                "partition by (c1) properties('replication_num'='1', 'enable_statistic_collect_on_first_load'='true')");
        starRocksAssert.ddl("alter table " + tableName + " add partition p1 values in ('1')");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        OlapTable table = (OlapTable) db.getTable(tableName);
        Partition partition = table.getPartition("p1");
        TransactionState transactionState = new TransactionState();
        transactionState.setDbId(db.getId());
        TableCommitInfo commitInfo = new TableCommitInfo(table.getId());
        commitInfo.addPartitionCommitInfo(new PartitionCommitInfo(partition.getDefaultPhysicalPartition().getId(),
                Partition.PARTITION_INIT_VERSION + 1, 1));
        transactionState.putIdToTableCommitInfo(table.getId(), commitInfo);
        // A small load skips collection after reading the partition's row count.
        transactionState.setTxnCommitAttachment(new StreamLoadTxnCommitAttachment(0, 0, 0, 0, 0, 1, 0, 0, 0, ""));
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return true;
            }

            @Mock
            public boolean isReady() {
                return true;
            }
        };

        AtomicInteger rowCountReads = new AtomicInteger();
        ExecutorService writer = Executors.newSingleThreadExecutor();
        new MockUp<Partition>() {
            @Mock
            public long getRowCount(Invocation invocation) throws Exception {
                Partition current = invocation.getInvokedInstance();
                if (current.getId() != partition.getId()) {
                    return invocation.proceed();
                }
                rowCountReads.incrementAndGet();
                Assertions.assertFalse(writer.submit(() -> tryWriteLock(db, table)).get(5, TimeUnit.SECONDS),
                        "partition row counts must be read while a metadata lock excludes concurrent writers");
                return 1000;
            }
        };

        Locker publisher = new Locker();
        if (brokerLoad) {
            publisher.lockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.WRITE);
        }
        try {
            if (brokerLoad) {
                // A new Locker cannot reacquire the publisher's WRITE lock, even on this thread.
                new MockUp<Locker>() {
                    @Mock
                    public void lockTablesWithIntensiveDbLock(Long dbId, List<Long> tableIds, LockType lockType) {
                        Assertions.fail("broker callback must reuse the publisher's existing WRITE lock");
                    }
                };
                LoadJobStatsListener.INSTANCE.onLoadJobTransactionFinish(transactionState);
            } else {
                // Stream-load callbacks run after the publisher has released its locks.
                LoadJobStatsListener.INSTANCE.onStreamLoadTransactionFinish(transactionState);
            }
            Assertions.assertEquals(1, rowCountReads.get(), "the listener must reach the actual row-count read");
        } finally {
            if (brokerLoad) {
                publisher.unLockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.WRITE);
            }
            writer.shutdownNow();
            Assertions.assertTrue(writer.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static boolean tryWriteLock(Database db, OlapTable table) {
        Locker locker = new Locker();
        if (!locker.tryLockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.WRITE,
                100, TimeUnit.MILLISECONDS)) {
            return false;
        }
        locker.unLockTablesWithIntensiveDbLock(db.getId(), List.of(table.getId()), LockType.WRITE);
        return true;
    }
}
