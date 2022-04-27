// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.starrocks.statistic.Constants;

public interface IMaterializedViewRefreshTask {
    Constants.MaterializedViewTaskStatus getStatus();
    void setStatus(Constants.MaterializedViewTaskStatus status);
    // beginTask is a HOOK that handles things that the task framework can do before running the task is executed,
    // such as setting the start time.
    void beginTask();
    // The task logic that this task actually runs.
    void runTask() throws Exception;
    // finishTask is a HOOK that handles things that the task framework can do after running the task is executed,
    // such as setting the end time.
    void finishTask();
    void setErrMsg(String errMsg);
    IMaterializedViewRefreshTask cloneTask();
}
