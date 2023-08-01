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

package com.starrocks.warehouse;

import com.google.gson.annotations.SerializedName;
import com.starrocks.thrift.TWarehouseInfo;

import java.util.Objects;

public class WarehouseInfo {
    public static final long ABSENT_ID = -1L;

    @SerializedName(value = "warehouse")
    String warehouse;

    @SerializedName(value = "id")
    private long id = ABSENT_ID;

    @SerializedName(value = "num_unfinished_query_jobs")
    private long numUnfinishedQueryJobs = 0L;
    @SerializedName(value = "num_unfinished_load_jobs")
    private long numUnfinishedLoadJobs = 0L;
    @SerializedName(value = "num_unfinished_backup_jobs")
    private long numUnfinishedBackupJobs = 0L;
    @SerializedName(value = "num_unfinished_restore_jobs")
    private long numUnfinishedRestoreJobs = 0L;
    @SerializedName(value = "last_finished_job_timestamp_ms")
    private long lastFinishedJobTimestampMs = 0L;

    public WarehouseInfo() {
    }

    public WarehouseInfo(String warehouse) {
        this.warehouse = warehouse;
    }

    public WarehouseInfo(String warehouse, long id) {
        this.warehouse = warehouse;
        this.id = id;
    }

    public WarehouseInfo(String warehouse, long id, long numUnfinishedQueryJobs, long numUnfinishedLoadJobs,
                         long numUnfinishedBackupJobs, long numUnfinishedRestoreJobs, long lastFinishedJobTimestampMs) {
        this.warehouse = warehouse;
        this.id = id;
        this.numUnfinishedQueryJobs = numUnfinishedQueryJobs;
        this.numUnfinishedLoadJobs = numUnfinishedLoadJobs;
        this.numUnfinishedBackupJobs = numUnfinishedBackupJobs;
        this.numUnfinishedRestoreJobs = numUnfinishedRestoreJobs;
        this.lastFinishedJobTimestampMs = lastFinishedJobTimestampMs;
    }

    public void increaseNumUnfinishedQueryJobs(long delta) {
        numUnfinishedQueryJobs += delta;
    }

    public void increaseNumUnfinishedLoadJobs(long delta) {
        numUnfinishedLoadJobs += delta;
    }

    public void increaseNumUnfinishedBackupJobs(long delta) {
        numUnfinishedBackupJobs += delta;
    }

    public void increaseNumUnfinishedRestoreJobs(long delta) {
        numUnfinishedRestoreJobs += delta;
    }

    public void updateLastFinishedJobTimeMs(long timestampMs) {
        lastFinishedJobTimestampMs = Math.max(lastFinishedJobTimestampMs, timestampMs);
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public long getNumUnfinishedQueryJobs() {
        return numUnfinishedQueryJobs;
    }

    public long getNumUnfinishedLoadJobs() {
        return numUnfinishedLoadJobs;
    }

    public long getNumUnfinishedBackupJobs() {
        return numUnfinishedBackupJobs;
    }

    public long getNumUnfinishedRestoreJobs() {
        return numUnfinishedRestoreJobs;
    }

    public long getLastFinishedJobTimestampMs() {
        return lastFinishedJobTimestampMs;
    }

    public TWarehouseInfo toThrift() {
        return new TWarehouseInfo()
                .setWarehouse(warehouse)
                .setId(id)
                .setNum_unfinished_query_jobs(numUnfinishedQueryJobs)
                .setNum_unfinished_load_jobs(numUnfinishedLoadJobs)
                .setNum_unfinished_backup_jobs(numUnfinishedBackupJobs)
                .setNum_unfinished_restore_jobs(numUnfinishedRestoreJobs)
                .setLast_finished_job_timestamp_ms(lastFinishedJobTimestampMs);
    }

    public static WarehouseInfo fromThrift(TWarehouseInfo tinfo) {
        WarehouseInfo info = new WarehouseInfo(tinfo.getWarehouse());
        info.id = tinfo.getId();
        info.numUnfinishedQueryJobs = tinfo.getNum_unfinished_query_jobs();
        info.numUnfinishedLoadJobs = tinfo.getNum_unfinished_load_jobs();
        info.numUnfinishedBackupJobs = tinfo.getNum_unfinished_backup_jobs();
        info.numUnfinishedRestoreJobs = tinfo.getNum_unfinished_restore_jobs();
        info.lastFinishedJobTimestampMs = tinfo.getLast_finished_job_timestamp_ms();
        return info;
    }

    public static WarehouseInfo fromWarehouse(Warehouse wh) {
        return new WarehouseInfo(wh.getName(), wh.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WarehouseInfo that = (WarehouseInfo) o;
        return id == that.id && numUnfinishedQueryJobs == that.numUnfinishedQueryJobs &&
                numUnfinishedLoadJobs == that.numUnfinishedLoadJobs &&
                numUnfinishedBackupJobs == that.numUnfinishedBackupJobs &&
                numUnfinishedRestoreJobs == that.numUnfinishedRestoreJobs &&
                lastFinishedJobTimestampMs == that.lastFinishedJobTimestampMs &&
                Objects.equals(warehouse, that.warehouse);
    }

    @Override
    public int hashCode() {
        return Objects.hash(warehouse, id, numUnfinishedQueryJobs, numUnfinishedLoadJobs, numUnfinishedBackupJobs,
                numUnfinishedRestoreJobs, lastFinishedJobTimestampMs);
    }

    @Override
    public String toString() {
        return "WarehouseInfo{" +
                "warehouse='" + warehouse + '\'' +
                ", id=" + id +
                ", numUnfinishedQueryJobs=" + numUnfinishedQueryJobs +
                ", numUnfinishedLoadJobs=" + numUnfinishedLoadJobs +
                ", numUnfinishedBackupJobs=" + numUnfinishedBackupJobs +
                ", numUnfinishedRestoreJobs=" + numUnfinishedRestoreJobs +
                ", lastFinishedJobTimestampMs=" + lastFinishedJobTimestampMs +
                '}';
    }
}
