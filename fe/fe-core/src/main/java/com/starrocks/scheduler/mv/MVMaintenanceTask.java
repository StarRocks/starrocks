// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.starrocks.planner.PlanFragmentId;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TMVMaintenanceStartTask;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TUniqueId;

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
    private long taskId;
    private PlanFragmentId fragmentId;
    private TUniqueId instanceId;
    private TExecPlanFragmentParams fragmentInstance;

    public static MVMaintenanceTask build(MVMaintenanceJob job, long taskId, long beId, PlanFragmentId fragmentId,
                                          TUniqueId instanceId,
                                          TExecPlanFragmentParams fragmentInstance) {
        MVMaintenanceTask task = new MVMaintenanceTask();
        task.dbName = GlobalStateMgr.getCurrentState().getDb(job.getView().getDbId()).getFullName();
        task.job = job;
        task.beId = beId;
        task.taskId = taskId;
        task.fragmentId = fragmentId;
        task.instanceId = instanceId;
        task.fragmentInstance = fragmentInstance;

        return task;
    }

    public TMVMaintenanceTasks toThrift() {
        TMVMaintenanceTasks request = new TMVMaintenanceTasks();
        request.setTask_type(MVTaskType.START_MAINTENANCE);
        TMVMaintenanceStartTask task = new TMVMaintenanceStartTask();
        request.setJob_id(job.getJobId());
        request.setTask_id(taskId);
        request.setStart_maintenance(task);
        task.setDb_name(dbName);
        task.setMv_name(job.getView().getName());
        task.setPlan_params(fragmentInstance);

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

    public PlanFragmentId getFragmentId() {
        return fragmentId;
    }

    public void setFragmentId(PlanFragmentId fragmentId) {
        this.fragmentId = fragmentId;
    }

    public TUniqueId getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(TUniqueId instanceId) {
        this.instanceId = instanceId;
    }

    public TExecPlanFragmentParams getFragmentInstance() {
        return fragmentInstance;
    }

    public void setFragmentInstance(TExecPlanFragmentParams fragmentInstance) {
        this.fragmentInstance = fragmentInstance;
    }

    @Override
    public String toString() {
        return "MVMaintenanceTask{" +
                "job=" + job +
                ", dbName='" + dbName + '\'' +
                ", beId=" + beId +
                ", taskId=" + taskId +
                '}';
    }
}
