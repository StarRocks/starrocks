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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/AlterJobV2.java

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

package com.starrocks.alter;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.TraceManager;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.UserIdentity;
import io.opentelemetry.api.trace.Span;
import org.apache.hadoop.util.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/*
 * Version 2 of AlterJob, for replacing the old version of AlterJob.
 * This base class of RollupJob and SchemaChangeJob
 */
public abstract class AlterJobV2 implements Writable {
    private static final Logger LOG = LogManager.getLogger(AlterJobV2.class);

    public enum JobState {
        PENDING, // Job is created
        WAITING_TXN, // New replicas are created and Shadow globalStateMgr object is visible for incoming txns,
        // waiting for previous txns to be finished
        RUNNING, // alter tasks are sent to BE, and waiting for them finished.
        FINISHED, // job is done
        CANCELLED, // job is cancelled(failed or be cancelled by user)
        FINISHED_REWRITING; // For LakeTable, has finished rewriting historical data.

        public boolean isFinalState() {
            return this == JobState.FINISHED || this == JobState.CANCELLED;
        }
    }

    public enum JobType {
        // DECOMMISSION_BACKEND is for compatible with older versions of metadata
        ROLLUP, SCHEMA_CHANGE, DECOMMISSION_BACKEND, OPTIMIZE
    }

    @SerializedName(value = "type")
    protected JobType type;
    @SerializedName(value = "jobId")
    protected long jobId;
    @SerializedName(value = "jobState")
    protected JobState jobState;

    @SerializedName(value = "dbId")
    protected long dbId;
    @SerializedName(value = "tableId")
    protected long tableId;
    @SerializedName(value = "tableName")
    protected String tableName;

    @SerializedName(value = "errMsg")
    protected String errMsg = "";
    @SerializedName(value = "createTimeMs")
    protected long createTimeMs = -1;
    @SerializedName(value = "finishedTimeMs")
    protected long finishedTimeMs = -1;
    @SerializedName(value = "timeoutMs")
    protected long timeoutMs = -1;
    @SerializedName(value = "warehouseId")
    protected long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;

    protected Span span;

    public AlterJobV2(long jobId, JobType jobType, long dbId, long tableId, String tableName, long timeoutMs) {
        this.jobId = jobId;
        this.type = jobType;
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.timeoutMs = timeoutMs;

        this.createTimeMs = System.currentTimeMillis();
        this.jobState = JobState.PENDING;
        this.span = TraceManager.startSpan(jobType.toString().toLowerCase());
        span.setAttribute("jobId", jobId);
        span.setAttribute("tabletName", tableName);
    }

    protected AlterJobV2(JobType type) {
        this.type = type;
        this.span = TraceManager.startNoopSpan();
    }

    public long getJobId() {
        return jobId;
    }

    public JobState getJobState() {
        return jobState;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public JobType getType() {
        return type;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() - createTimeMs > timeoutMs;
    }

    public boolean isExpire() {
        return isDone() && (System.currentTimeMillis() - finishedTimeMs) / 1000 > Config.history_job_keep_max_second;
    }

    public boolean isDone() {
        return jobState.isFinalState();
    }

    public long getFinishedTimeMs() {
        return finishedTimeMs;
    }

    public void setFinishedTimeMs(long finishedTimeMs) {
        this.finishedTimeMs = finishedTimeMs;
    }

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public void createConnectContextIfNeeded() {
        if (ConnectContext.get() == null) {
            ConnectContext context = new ConnectContext();
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.setQualifiedUser(UserIdentity.ROOT.getUser());
            context.setThreadLocalInfo();
        }
    }

    /**
     * The keyword 'synchronized' only protects 2 methods:
     * run() and cancel()
     * Only these 2 methods can be visited by different thread(internal working thread and user connection thread)
     * So using 'synchronized' to make sure only one thread can run the job at one time.
     * <p>
     * lock order:
     * synchronized
     * db lock
     */
    public synchronized void run() {
        if (isTimeout()) {
            cancelImpl("Timeout");
            return;
        }

        // create connectcontext
        createConnectContextIfNeeded();

        try {
            while (true) {
                JobState prevState = jobState;
                switch (prevState) {
                    case PENDING:
                        runPendingJob();
                        break;
                    case WAITING_TXN:
                        runWaitingTxnJob();
                        break;
                    case RUNNING:
                        runRunningJob();
                        break;
                    case FINISHED_REWRITING:
                        runFinishedRewritingJob();
                        break;
                    default:
                        break;
                }
                if (jobState == prevState) {
                    break;
                } // else: handle the new state
            }
        } catch (AlterCancelException e) {
            cancelImpl(e.getMessage());
        }
    }

    public boolean cancel(String errMsg) {
        synchronized (this) {
            return cancelImpl(errMsg);
        }
    }

    /**
     * should be called before executing the job.
     * return false if table is not stable.
     */
    protected boolean checkTableStable(Database db) throws AlterCancelException {
        long unHealthyTabletId = TabletInvertedIndex.NOT_EXIST_VALUE;
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (tbl == null) {
            throw new AlterCancelException("Table " + tableId + " does not exist");
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        try {
            if (tbl.isOlapTable()) {
                unHealthyTabletId = tbl.checkAndGetUnhealthyTablet(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        GlobalStateMgr.getCurrentState().getTabletScheduler());
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.READ);
        }

        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        try {
            if (unHealthyTabletId != TabletInvertedIndex.NOT_EXIST_VALUE) {
                errMsg = "table is unstable, unhealthy (or doing balance) tablet id: " + unHealthyTabletId;
                LOG.warn("wait table {} to be stable before doing {} job", tableId, type);
                tbl.setState(OlapTableState.WAITING_STABLE);
                return false;
            } else {
                // table is stable, set is to ROLLUP and begin altering.
                LOG.info("table {} is stable, start job{}, type {}", tableId, jobId, type);
                tbl.setState(type == JobType.ROLLUP ? OlapTableState.ROLLUP : OlapTableState.SCHEMA_CHANGE);
                return true;
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(tbl.getId()), LockType.WRITE);
        }
    }

    protected abstract void runPendingJob() throws AlterCancelException;

    protected abstract void runWaitingTxnJob() throws AlterCancelException;

    protected abstract void runRunningJob() throws AlterCancelException;

    protected abstract void runFinishedRewritingJob() throws AlterCancelException;

    protected abstract boolean cancelImpl(String errMsg);

    protected abstract void getInfo(List<List<Comparable>> infos);

    public abstract void replay(AlterJobV2 replayedJob);

    public static AlterJobV2 read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterJobV2.class);
    }

    public abstract Optional<Long> getTransactionId();


    /**
     * Schema change will build a new MaterializedIndexMeta, we need rebuild it(add extra original meta)
     * into it from original index meta. Otherwise, some necessary metas will be lost after fe restart.
     *
     * @param orgIndexMeta : index meta before schema change.
     * @param indexMeta    : new index meta after schema change.
     */
    protected void rebuildMaterializedIndexMeta(MaterializedIndexMeta orgIndexMeta,
                                                MaterializedIndexMeta indexMeta) {
        indexMeta.setViewDefineSql(orgIndexMeta.getViewDefineSql());
        indexMeta.setColocateMVIndex(orgIndexMeta.isColocateMVIndex());
        indexMeta.setDefineStmt(orgIndexMeta.getDefineStmt());
        if (indexMeta.getDefineStmt() != null) {
            try {
                indexMeta.gsonPostProcess();
            } catch (IOException e) {
                LOG.warn("rebuild defined stmt of index meta {}(org)/{}(new) failed :",
                        orgIndexMeta.getIndexId(), indexMeta.getIndexId(), e);
            }
        }
    }
}
