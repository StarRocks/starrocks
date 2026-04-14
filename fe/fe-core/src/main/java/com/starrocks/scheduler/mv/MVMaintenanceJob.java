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

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.server.GlobalStateMgr;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Compatibility-only metadata for legacy incremental MV maintenance jobs.
 */
public class MVMaintenanceJob implements Writable, GsonPreProcessable, GsonPostProcessable {
    @SerializedName("jobId")
    private final long jobId;
    @SerializedName("dbId")
    private final long dbId;
    @SerializedName("viewId")
    private final long viewId;
    @SerializedName("state")
    private JobState serializedState;
    @SerializedName("epoch")
    private MVEpoch epoch;

    private transient AtomicReference<JobState> state = new AtomicReference<>(JobState.INIT);

    MVMaintenanceJob(MaterializedView view) {
        this.jobId = view.getId();
        this.dbId = view.getDbId();
        this.viewId = view.getId();
        this.epoch = new MVEpoch(view.getMvId());
        this.serializedState = JobState.INIT;
        setState(JobState.INIT);
    }

    void restore() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, viewId);
        Preconditions.checkState(table != null && table.isMaterializedView());
        this.serializedState = JobState.INIT;
        setState(serializedState);
    }

    public void setEpoch(MVEpoch epoch) {
        this.epoch = epoch;
    }

    public JobState getState() {
        return state.get();
    }

    public long getJobId() {
        return jobId;
    }

    public long getViewId() {
        return viewId;
    }

    public long getDbId() {
        return dbId;
    }

    public MVEpoch getEpoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return String.format("MVJob id=%s,dbId=%s,viewId=%d,epoch=%s,state=%s",
                jobId, dbId, viewId, epoch, getState());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MVMaintenanceJob that = (MVMaintenanceJob) o;
        return jobId == that.jobId
                && viewId == that.viewId
                && Objects.equals(epoch, that.epoch)
                && getState() == that.getState();
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, viewId, epoch, getState());
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (serializedState == null) {
            serializedState = JobState.INIT;
        }
        setState(serializedState);
    }

    @Override
    public void gsonPreProcess() throws IOException {
        serializedState = getState();
    }

    private void setState(JobState jobState) {
        if (state == null) {
            state = new AtomicReference<>();
        }
        state.set(jobState);
    }

    /*
     *                            BuildMeta
     *                  ┌─────────┐      ┌─────────┐
     *                  │ CREATED ├─────►│ STARTED ├─────────────┐
     *                  └─────────┘      └────┬────┘             │
     *                                        │                  │
     *                                        │    OnSchedule    │
     *             Stop          ReSchedule   ▼                  ▼
     *  ┌────────┐      ┌─────────┐      ┌─────────┐         ┌────────┐
     *  │STOPPED │◄─────┤ PAUSED  ├─────►│PREPARING├────────►│ FAILED │
     *  └────────┘      └─────────┘      └────┬────┘         └────────┘
     *                      ▲                 │                  ▲
     *                      │                 │    Deploy        │
     *                      │                 ▼                  │
     *                      │            ┌─────────┐             │
     *                      └────────────┤RUN_EPOCH├─────────────┘
     *                                   └─────────┘
     */
    public enum JobState {
        // Just initialized
        INIT,

        // Wait for scheduling
        STARTED,

        // Preparing for the job
        PREPARING,

        // Pause the job, waiting for reschedule
        PAUSED,

        // Running the epoch
        RUN_EPOCH,

        // Stopped, no tasks on executors
        STOPPED,

        // Failed the whole job, needs to be destroyed
        FAILED
    }

}
