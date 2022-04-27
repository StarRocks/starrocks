// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.starrocks.statistic.Constants;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;

public class MaterializedViewMockRefreshTask extends MaterializedViewRefreshTask {

    private static final Logger LOG = LogManager.getLogger(MaterializedViewMockRefreshTask.class);
    Constants.MaterializedViewTaskStatus status = Constants.MaterializedViewTaskStatus.PENDING;
    private long executeMillis;
    private String mvTable;
    private boolean mockFailed = false;

    public MaterializedViewMockRefreshTask(Long executeMillis, String mvTable) {
        this.executeMillis = executeMillis;
        this.mvTable = mvTable;
    }

    @Override
    public Constants.MaterializedViewTaskStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(Constants.MaterializedViewTaskStatus status) {
        this.status = status;
    }

    public void setMockFailed(boolean mockFailed) {
        this.mockFailed = mockFailed;
    }

    @Override
    public void runTask() {
        status = Constants.MaterializedViewTaskStatus.RUNNING;
        ThreadUtil.sleepAtLeastIgnoreInterrupts(executeMillis);
        if (mockFailed) {
            throw new MaterializedViewTaskException("mock task execute failed.");
        }
        LOG.info("running a " + mvTable + " task:" + LocalDateTime.now());
        status = Constants.MaterializedViewTaskStatus.SUCCESS;
    }

    @Override
    public IMaterializedViewRefreshTask cloneTask() {
        MaterializedViewMockRefreshTask task = new MaterializedViewMockRefreshTask(executeMillis, mvTable);
        task.setMockFailed(mockFailed);
        return task;
    }

    @Override
    public String toString() {
        return "MaterializedViewMockRefreshTask{" +
                "status=" + status +
                '}';
    }
}
