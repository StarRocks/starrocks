// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mv;

import com.starrocks.statistic.Constants;

public interface IMaterializedViewRefreshTask {
    Constants.MaterializedViewTaskStatus getStatus();
    void runTask();
    IMaterializedViewRefreshTask cloneTask();
}
