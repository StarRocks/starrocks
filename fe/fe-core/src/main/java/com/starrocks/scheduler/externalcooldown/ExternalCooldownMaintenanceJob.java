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

package com.starrocks.scheduler.externalcooldown;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.externalcooldown.ExternalCooldownConfig;
import com.starrocks.externalcooldown.ExternalCooldownPartitionSelector;
import com.starrocks.externalcooldown.ExternalCooldownSchedule;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


/**
 * Long-running job responsible for external cooldown maintenance.
 */
public class ExternalCooldownMaintenanceJob implements Writable, GsonPreProcessable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalCooldownMaintenanceJob.class);

    // Persisted state
    @SerializedName("jobId")
    private final long jobId;
    @SerializedName("dbId")
    private final long dbId;
    @SerializedName("tableId")
    private final long tableId;

    // Runtime ephemeral state
    // At most one thread could execute this job, this flag indicates is someone scheduling this job
    private transient OlapTable olapTable;
    private transient ExternalCooldownPartitionSelector partitionSelector;
    private transient ExternalCooldownSchedule schedule;
    private transient String lastRunTaskName = null;

    public ExternalCooldownMaintenanceJob(OlapTable olapTable, long dbId) {
        this.jobId = olapTable.getId();
        this.tableId = olapTable.getId();
        this.olapTable = olapTable;
        this.dbId = dbId;
    }

    public static ExternalCooldownMaintenanceJob read(DataInput input) throws IOException {
        ExternalCooldownMaintenanceJob job = GsonUtils.GSON.fromJson(
                Text.readString(input), ExternalCooldownMaintenanceJob.class);
        return job;
    }

    public void restore() {
        if (olapTable == null) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
            Preconditions.checkState(table != null && table.isOlapTable());
            this.olapTable = (OlapTable) table;
        }
        if (partitionSelector == null) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            Preconditions.checkState(db != null);
            partitionSelector = new ExternalCooldownPartitionSelector(db, olapTable);
            partitionSelector.init();
        }
        if (schedule == null) {
            schedule = ExternalCooldownSchedule.fromString(olapTable.getExternalCoolDownSchedule());
        } else {
            ExternalCooldownSchedule tmpSchedule = ExternalCooldownSchedule.fromString(olapTable.getExternalCoolDownSchedule());
            if (tmpSchedule != null) {
                tmpSchedule.setLastScheduleMs(schedule.getLastScheduleMs());
                schedule = tmpSchedule;
            }
        }
    }

    public void stopJob() {
        // stopTasks();
    }

    public void onSchedule() throws Exception {
        if (lastRunTaskName != null) {
            Task task = GlobalStateMgr.getCurrentState().getTaskManager().getTask(lastRunTaskName);
            if (task != null && task.getState() != Constants.TaskState.ACTIVE) {
                return;
            }
        }
        if (schedule == null || !schedule.trySchedule()) {
            LOG.debug("current time not match external cooldown schedule, skip");
            return;
        }
        if (!partitionSelector.hasPartitionSatisfied()) {
            partitionSelector.reloadSatisfiedPartitions();
            if (!partitionSelector.hasPartitionSatisfied()) {
                return;
            }
        }
        Partition partition = partitionSelector.getOneSatisfiedPartition();
        if (partition == null) {
            return;
        }
        LOG.info("create external cooldown task for partition {}", partition);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(dbId);
        Task task = TaskBuilder.buildExternalCooldownTask(db, olapTable, partition);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        taskManager.killTask(task.getName(), false);
        taskManager.createTask(task, false);
        ExecuteOption executeOption = TaskBuilder.getCooldownExecuteOption(olapTable, partition);
        taskManager.executeTask(task.getName(), executeOption);
        lastRunTaskName = task.getName();
    }

    public boolean isRunnable() {
        if (olapTable == null) {
            return false;
        }
        ExternalCooldownConfig config = olapTable.getCurExternalCoolDownConfig();
        if (config == null) {
            return false;
        }
        return config.isReadyForAutoCooldown();
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    public long getJobId() {
        return jobId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getDbId() {
        return dbId;
    }

    @Override
    public String toString() {
        return String.format("ExternalCooldownJob id=%s,dbId=%s,tableId=%d", jobId, dbId, tableId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExternalCooldownMaintenanceJob that = (ExternalCooldownMaintenanceJob) o;
        return jobId == that.jobId && tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, tableId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }

    @Override
    public void gsonPreProcess() throws IOException {
    }
}
