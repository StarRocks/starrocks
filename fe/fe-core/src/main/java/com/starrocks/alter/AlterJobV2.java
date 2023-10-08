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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.TraceManager;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import io.opentelemetry.api.trace.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
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
        ROLLUP, SCHEMA_CHANGE, DECOMMISSION_BACKEND
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

        try {
            switch (jobState) {
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
        } catch (AlterCancelException e) {
            cancelImpl(e.getMessage());
        }
    }

    public final boolean cancel(String errMsg) {
        synchronized (this) {
            return cancelImpl(errMsg);
        }
    }

    /**
     * should be called before executing the job.
     * return false if table is not stable.
     */
    protected boolean checkTableStable(Database db) throws AlterCancelException {
        OlapTable tbl;
        long unHealthyTabletId = TabletInvertedIndex.NOT_EXIST_VALUE;
        db.readLock();
        try {
            tbl = (OlapTable) db.getTable(tableId);
            if (tbl == null) {
                throw new AlterCancelException("Table " + tableId + " does not exist");
            }

            unHealthyTabletId = tbl.checkAndGetUnhealthyTablet(GlobalStateMgr.getCurrentSystemInfo(),
                    GlobalStateMgr.getCurrentState().getTabletScheduler());
        } finally {
            db.readUnlock();
        }

        db.writeLock();
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
            db.writeUnlock();
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
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_86) {
            JobType type = JobType.valueOf(Text.readString(in));
            switch (type) {
                case ROLLUP:
                    return RollupJobV2.read(in);
                case SCHEMA_CHANGE:
                    return SchemaChangeJobV2.read(in);
                default:
                    Preconditions.checkState(false);
                    return null;
            }
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, AlterJobV2.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
        Text.writeString(out, jobState.name());

        out.writeLong(jobId);
        out.writeLong(dbId);
        out.writeLong(tableId);
        Text.writeString(out, tableName);

        Text.writeString(out, errMsg);
        out.writeLong(createTimeMs);
        out.writeLong(finishedTimeMs);
        out.writeLong(timeoutMs);
    }

    public void readFields(DataInput in) throws IOException {
        // read common members as write in AlterJobV2.write().
        // except 'type' member, which is read in AlterJobV2.read()
        jobState = JobState.valueOf(Text.readString(in));

        jobId = in.readLong();
        dbId = in.readLong();
        tableId = in.readLong();
        tableName = Text.readString(in);

        errMsg = Text.readString(in);
        createTimeMs = in.readLong();
        finishedTimeMs = in.readLong();
        timeoutMs = in.readLong();
    }

    public abstract Optional<Long> getTransactionId();


    /**
     * Schema change will build a new MaterializedIndexMeta, we need rebuild it(add extra original meta)
     * into it from original index meta. Otherwise, some necessary metas will be lost after fe restart.
     *
     * @param orgIndexMeta  : index meta before schema change.
     * @param indexMeta     : new index meta after schema change.
     */
    protected void rebuildMaterializedIndexMeta(MaterializedIndexMeta orgIndexMeta,
                                                MaterializedIndexMeta indexMeta) {
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
