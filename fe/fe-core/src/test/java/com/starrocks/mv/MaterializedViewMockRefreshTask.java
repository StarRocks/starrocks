// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.starrocks.statistic.Constants;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.hadoop.util.ThreadUtil;

import java.time.LocalDateTime;

public class MaterializedViewMockRefreshTask implements IMaterializedViewRefreshTask {

    Constants.MaterializedViewTaskStatus status = Constants.MaterializedViewTaskStatus.PENDING;
    private long executeMillis;
    private String mvTable;

    public MaterializedViewMockRefreshTask(Long executeMillis, String mvTable) {
        this.executeMillis = executeMillis;
        this.mvTable = mvTable;
    }

    @Override
    public Constants.MaterializedViewTaskStatus getStatus() {
        return status;
    }

    @Override
    public void runTask() {
        status = Constants.MaterializedViewTaskStatus.RUNNING;
        ThreadUtil.sleepAtLeastIgnoreInterrupts(executeMillis);
        System.out.println("running a " + mvTable + " task:" + LocalDateTime.now());
        status = Constants.MaterializedViewTaskStatus.SUCCESS;
    }

    @Override
    public IMaterializedViewRefreshTask cloneTask() {
        return new MaterializedViewMockRefreshTask(executeMillis, mvTable);
    }

    @Override
    public String toString() {
        return "MaterializedViewMockRefreshTask{" +
                "status=" + status +
                '}';
    }
}
