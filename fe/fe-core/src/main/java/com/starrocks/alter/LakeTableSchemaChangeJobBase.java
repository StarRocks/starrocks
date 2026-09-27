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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.transaction.GlobalTransactionMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
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

    protected LakeTableSchemaChangeJobBase(LakeTableSchemaChangeJobBase job) {
        super(job);
        this.watershedTxnId = job.watershedTxnId;
        this.watershedGtid = job.watershedGtid;
    }

    // NOTE: Metadata access in these jobs locks only the job's own table
    // (`tableId`) with an intensive db lock (IS/IX on the database + S/X on the
    // table) via AutoCloseableLock, NOT the whole database:
    //
    //     try (AutoCloseableLock ignore =
    //             new AutoCloseableLock(dbId, List.of(tableId), LockType.READ /* or WRITE */)) {
    //         OlapTable table = getTableOrThrow(); // or getTable() to skip-on-missing
    //         ...
    //     }
    //
    // The intention lock on the database still blocks DROP DATABASE (which takes
    // db WRITE) and the IS/IX still blocks DROP TABLE (which takes db WRITE), so
    // db/table existence is preserved across the critical section just as the
    // old full-db lock guaranteed, while leaving unrelated tables in the same
    // database free of contention. This mirrors LakeTableAlterMetaJobBase and
    // the non-lake SchemaChangeJobV2.

    // Returns the job's table, or null if the database or the table has been
    // dropped. The caller must already hold the table lock.
    @Nullable
    protected OlapTable getTable() {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    @VisibleForTesting
    public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
        GlobalTransactionMgr globalTxnMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        return globalTxnMgr.isPreviousTransactionsFinished(txnId, dbId, Lists.newArrayList(tableId));
    }

    protected boolean tableExists() {
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.READ)) {
            return getTable() != null;
        }
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }

    // Returns the job's table, or throws AlterCancelException distinguishing a
    // dropped database from a dropped table. The caller must already hold the
    // table lock.
    @NotNull
    OlapTable getTableOrThrow() throws AlterCancelException {
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId) == null) {
            throw new AlterCancelException("Database does not exist");
        }
        OlapTable table = getTable();
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

    @VisibleForTesting
    public static long getNextTransactionId() {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
    }

    @VisibleForTesting
    public static long peekNextTransactionId() {
        return GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator().peekNextTransactionId();
    }

    public static long getNextGtid() {
        return GlobalStateMgr.getCurrentState().getGtidGenerator().nextGtid();
    }
}
