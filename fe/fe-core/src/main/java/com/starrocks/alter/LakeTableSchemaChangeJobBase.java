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


package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.transaction.GlobalTransactionMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public abstract class LakeTableSchemaChangeJobBase extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(LakeTableSchemaChangeJobBase.class);

    // The job will wait all transactions before this txn id finished, then send the rollup tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;
    @SerializedName(value = "watershedGtid")
    protected long watershedGtid = -1;

    public LakeTableSchemaChangeJobBase(long jobId, JobType jobType, long dbId, long tableId,
                                        String tableName, long timeoutMs) {
        super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
    }

    public LakeTableSchemaChangeJobBase(JobType jobType) {
        super(jobType);
    }

    @Nullable
    protected ReadLockedTable getReadLockedTable(long dbId, long tableId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        return db != null ? new ReadLockedTable(db, tableId) : null;
    }

    @Nullable
    protected WriteLockedTable getWriteLockedTable(long dbId, long tableId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        return db != null ? new WriteLockedTable(db, tableId) : null;
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    @VisibleForTesting
    public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
        GlobalTransactionMgr globalTxnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        return globalTxnMgr.isPreviousTransactionsFinished(txnId, dbId, Lists.newArrayList(tableId));
    }

    protected abstract static class LockedTable implements AutoCloseable {
        protected final Database db;
        protected final long tableId;
        protected Locker locker;

        LockedTable(@NotNull Database db,
                    long tableId) {
            this.locker = new Locker();
            this.tableId = tableId;
            this.db = db;
            lock();
        }

        abstract void lock();

        abstract void unlock();

        @Nullable
        OlapTable getTable() {
            return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        }

        @Override
        public void close() {
            unlock();
        }
    }

    protected static class ReadLockedTable extends LockedTable {
        ReadLockedTable(@NotNull Database db, long tableId) {
            super(db, tableId);
        }

        @Override
        void lock() {
            locker.lockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }

        @Override
        void unlock() {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }

        public String getFullName() {
            return db.getFullName();
        }
    }

    protected static class WriteLockedTable extends LockedTable {
        WriteLockedTable(@NotNull Database db, long tableId) {
            super(db, tableId);
        }

        @Override
        void lock() {
            locker.lockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }

        @Override
        void unlock() {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }
    }

    protected boolean tableExists() {
        try (ReadLockedTable db = getReadLockedTable(dbId, tableId)) {
            return db != null && db.getTable() != null;
        }
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }

    @NotNull
    OlapTable getTableOrThrow(@Nullable LockedTable db, long tableId) throws AlterCancelException {
        if (db == null) {
            throw new AlterCancelException("Database does not exist");
        }
        OlapTable table = db.getTable();
        if (table == null) {
            throw new AlterCancelException("Table does not exist. tableId=" + tableId);
        }
        return table;
    }

    @VisibleForTesting
    public static void sendAgentTask(AgentBatchTask batchTask) {
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
    }




}
