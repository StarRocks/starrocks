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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TMVMaintenanceStartTask;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TNetworkAddress;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO(murphy) implement the Coordinator to compute task correctly
 * <p>
 * Runnable maintenance task on each executor
 * 1. After maintenance job started, generated tasks are deployed on executors
 * 2. The execution of task is coordinated by EpochCoordinator on FE
 */
public class MVMaintenanceTask {

    // Job information of the job
    private MVMaintenanceJob job;
    private String dbName;

    // Task specific information
    private long beId;
    private TNetworkAddress beHost;
    private long taskId;
    private List<TExecPlanFragmentParams> fragmentInstances = new ArrayList<>();

    public static MVMaintenanceTask build(MVMaintenanceJob job, long taskId, long beId, TNetworkAddress beHost,
                                          List<TExecPlanFragmentParams> fragmentInstances) {
        MVMaintenanceTask task = new MVMaintenanceTask();
        task.dbName = GlobalStateMgr.getCurrentState().getDb(job.getView().getDbId()).getFullName();
        task.job = job;
        task.beId = beId;
        task.beHost = beHost;
        task.taskId = taskId;
        task.fragmentInstances = fragmentInstances;

        return task;
    }

    public TMVMaintenanceTasks toThrift() {
        TMVMaintenanceTasks request = new TMVMaintenanceTasks();
        request.setTask_type(MVTaskType.START_MAINTENANCE);
        TMVMaintenanceStartTask task = new TMVMaintenanceStartTask();
        request.setJob_id(job.getJobId());
        request.setTask_id(taskId);
        request.setStart_maintenance(task);
        request.setDb_name(dbName);
        request.setMv_name(job.getView().getName());
        task.setFragments(fragmentInstances);

        return request;
    }

    public MVMaintenanceJob getJob() {
        return job;
    }

    public void setJob(MVMaintenanceJob job) {
        this.job = job;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public long getBeId() {
        return beId;
    }

    public void setBeId(long beId) {
        this.beId = beId;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public void addFragmentInstance(TExecPlanFragmentParams instance) {
        fragmentInstances.add(instance);
    }

    public List<TExecPlanFragmentParams> getFragmentInstances() {
        return fragmentInstances;
    }

    public void setFragmentInstances(List<TExecPlanFragmentParams> fragmentInstances) {
        this.fragmentInstances = fragmentInstances;
    }

    @Override
    public String toString() {
        return "MVMaintenanceTask{" +
                "job=" + job +
                ", dbName='" + dbName + '\'' +
                ", beId=" + beId +
                ", host=" + beHost +
                ", taskId=" + taskId +
                '}';
    }
}
