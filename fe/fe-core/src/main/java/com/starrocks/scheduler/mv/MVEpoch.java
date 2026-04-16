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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MvId;
import com.starrocks.common.io.Writable;

import java.util.Objects;

/**
 * Compatibility metadata for a legacy incremental MV maintenance epoch.
 */
public class MVEpoch implements Writable {
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("mvId")
    private long mvId;
    @SerializedName("epochState")
    private EpochState state;
    @SerializedName("binlogState")
    private BinlogConsumeStateVO binlogState;
    @SerializedName("startTimeMilli")
    private long startTimeMilli;
    @SerializedName("commitTimeMilli")
    private long commitTimeMilli;

    public MVEpoch(MvId mv) {
        this.dbId = mv.getDbId();
        this.mvId = mv.getId();
        this.startTimeMilli = System.currentTimeMillis();
        this.state = EpochState.INIT;
        this.binlogState = new BinlogConsumeStateVO();
    }

    public enum EpochState {
        // Wait for data
        INIT,
        // Ready for scheduling
        READY,
        // Scheduled and under execution
        RUNNING,
        // Execution finished and start committing
        COMMITTING,
        // Committed epoch
        COMMITTED,
        // Failed for any reason
        FAILED;

        public boolean isFailed() {
            return this.equals(FAILED);
        }

    }

    public long getDbId() {
        return dbId;
    }

    public long getMvId() {
        return mvId;
    }

    public void setMvId(long mvId) {
        this.mvId = mvId;
    }

    public EpochState getState() {
        return state;
    }

    public void setState(EpochState state) {
        this.state = state;
    }

    public BinlogConsumeStateVO getBinlogState() {
        return binlogState;
    }

    public void setBinlogState(BinlogConsumeStateVO binlogState) {
        this.binlogState = binlogState;
    }

    public long getStartTimeMilli() {
        return startTimeMilli;
    }

    public void setStartTimeMilli(long startTimeMilli) {
        this.startTimeMilli = startTimeMilli;
    }

    public long getCommitTimeMilli() {
        return commitTimeMilli;
    }

    public void setCommitTimeMilli(long commitTimeMilli) {
        this.commitTimeMilli = commitTimeMilli;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MVEpoch mvEpoch = (MVEpoch) o;
        return dbId == mvEpoch.dbId
                && mvId == mvEpoch.mvId
                && startTimeMilli == mvEpoch.startTimeMilli
                && commitTimeMilli == mvEpoch.commitTimeMilli
                && state == mvEpoch.state &&
                Objects.equals(binlogState, mvEpoch.binlogState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, mvId, state, binlogState, startTimeMilli, commitTimeMilli);
    }

    @Override
    public String toString() {
        return "MVEpoch{" +
                "dbId=" + dbId +
                ", mvId=" + mvId +
                ", state=" + state +
                ", binlogState=" + binlogState +
                ", startTimeMilli=" + startTimeMilli +
                ", commitTimeMilli=" + commitTimeMilli +
                '}';
    }
}
